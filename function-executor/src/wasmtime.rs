// TODO:

//! Use wasmtime as executor's runtime.

#![warn(missing_docs)]

use crate::{get_args, get_module, kvs_get, kvs_put, EssaResult, FunctionExecutor};
use anna::{
    lattice::LastWriterWinsLattice, lattice::Lattice, nodes::ClientNode, store::LatticeValue,
    ClientKey,
};
use anyhow::{bail, Context};
use essa_common::{scheduler_function_call_topic, scheduler_run_r_function_call_topic};
use flume::Receiver;
use r_polars::polars::prelude::*;
use std::{collections::HashMap, sync::{Arc, Mutex}};
use uuid::Uuid;
use wasmtime::{Caller, Engine, Extern, Linker, Module, Store, ValType};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder};
use zenoh::{prelude::r#async::*, query::Reply, queryable::Query};

use std::sync::atomic::{AtomicU32, Ordering};
use lazy_static::lazy_static;

impl FunctionExecutor {
    /// Runs the given WASM module.
    pub fn run_module(mut self, wasm_bytes: Vec<u8>) -> anyhow::Result<()> {
        log::info!("Start running WASM module");

        // compile the WASM module
        let engine = Engine::default();
        let module = Module::new(&engine, &wasm_bytes).context("failed to load wasm module")?;

        // store the compiled module in the key-value store under an
        // unique key (to avoid recompiling the module on future function
        // calls)
        log::info!("Storing compiled wasm module in anna KVS");
        let module_key: ClientKey = Uuid::new_v4().to_string().into();
        let value = LastWriterWinsLattice::new_now(
            module.serialize().context("failed to serialize module")?,
        )
        .into();
        kvs_put(module_key.clone(), value, &mut self.anna)?;

        let (mut store, linker) = set_up_module(
            engine,
            &module,
            module_key,
            vec![],
            self.zenoh.clone(),
            self.zenoh_prefix.clone(),
            self.anna,
        )?;

        // get the default function (i.e. the `main` function)
        let default_func = linker
            .get_default(&mut store, "")
            .context("module has no default function")?
            .typed::<(), ()>(&store)
            .context("default function has invalid type")?;

        log::info!("Starting default function of wasm module");

        // call the function
        //
        // TODO: wasmtime allows fine-grained control of the execution, e.g.
        // periodic interrupts. We might want to use these features to guard
        // against malicious or buggy user programs. For example, an infinite
        // loop should not waste CPU time forever.
        default_func
            .call(&mut store, ())
            .context("default function failed")?;

        Ok(())
    }

    /// Runs the given function of a already compiled WASM module.
    pub async fn handle_function_call(mut self, query: Query) -> anyhow::Result<()> {
        let mut topic_split = query.key_expr().as_str().split('/');
        let args_key = ClientKey::from(topic_split.next_back().context("no args key in topic")?);
        let function_name = topic_split
            .next_back()
            .context("no function key in topic")?;
        let module_key =
            ClientKey::from(topic_split.next_back().context("no module key in topic")?);

        let module = get_module(module_key.clone(), &mut self.anna)?;

        let args = get_args(args_key, &mut self.anna)?;
        let args_len = u32::try_from(args.len())?;

        // deserialize and set up WASM module
        let engine = Engine::default();
        let module =
            unsafe { Module::deserialize(&engine, module).expect("failed to deserialize module") };

        let (mut store, linker) = set_up_module(
            engine,
            &module,
            module_key,
            args,
            self.zenoh.clone(),
            self.zenoh_prefix.clone(),
            self.anna,
        )?;

        // get the function that we've been requested to call
        let func = linker
            .get(&mut store, "", function_name)
            .and_then(|e| e.into_func())
            .with_context(|| format!("module has no function `{}`", function_name))?
            .typed::<(i32,), ()>(&store)
            .context("default function has invalid type")?;

        func.call(&mut store, (args_len as i32,))
            .context("function trapped")?;

        // store the function's result into the key value store under
        // the requested key
        //
        // TODO: The result is needed only once, so it might make more sense to
        // send it as a message instead of storing it in the KVS. This would
        // also improve performance since the receiver would no longer need to
        // busy-wait on the result key in the KVS anymore.
        let mut host_state = store.into_data();
        if let Some(result_value) = host_state.function_result.take() {
            log::info!("`handle_function_call` result value is: {result_value:?}");

            // TODO: this is questionable, can I do it without a `new_anna_client`?
            let mut anna = new_anna_client(self.zenoh).await?;
            // store args in kvs
            let key: ClientKey = Uuid::new_v4().to_string().into();
            kvs_put(
                key.clone(),
                LastWriterWinsLattice::new_now(result_value.clone().into()).into(),
                &mut anna,
            )?;

            let key_serialized = bincode::serialize(&key)?;
            log::info!("key serialized: {:?}", key_serialized);

            let selector = query.key_expr().clone();
            query
                .reply(Ok(Sample::new(selector, key_serialized)))
                .res()
                .await
                .map_err(|e| {
                    println!("Error reply function run");
                    let err = Box::<dyn std::error::Error + 'static + Send + Sync>::from(e);
                    anyhow::anyhow!(err)
                })?;

            Ok(())
        } else {
            Err(anyhow::anyhow!("no result"))
        }
    }
}

/// Link the host functions and WASI abstractions into the WASM module.
fn set_up_module(
    engine: Engine,
    module: &Module,
    module_key: ClientKey,
    args: Vec<u8>,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
    anna: ClientNode,
) -> Result<(Store<HostState>, Linker<HostState>), anyhow::Error> {
    let wasi = WasiCtxBuilder::new()
        // TODO: we probably want to write to a log file or an anna key instead
        .inherit_stdout()
        .inherit_stderr()
        .build();
    let mut store = Store::new(
        &engine,
        HostState {
            wasi,
            module: module.clone(),
            module_key,
            function_result: None,
            next_result_handle: 1,
            results: HashMap::new(),
            result_receivers: HashMap::new(),
            zenoh,
            zenoh_prefix,
            anna,
        },
    );
    let mut linker = Linker::new(&engine);

    // link in the essa host functions
    linker
        .func_wrap(
            "host",
            "essa_get_args",
            move |mut caller: Caller<'_, HostState>, buf_ptr: u32, buf_len: u32| {
                // the given buffer must be large enough to hold `args`
                if buf_len < u32::try_from(args.len()).unwrap() {
                    return Ok(EssaResult::BufferTooSmall as i32);
                }

                // write `args` to the given memory region in the sandbox
                let mem = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => bail!("failed to find host memory"),
                };
                mem.write(&mut caller, buf_ptr as usize, args.as_slice())
                    .context("val ptr/len out of bounds")?;

                Ok(EssaResult::Ok as i32)
            },
        )
        .context("failed to create essa_get_args")?;
    linker
        .func_wrap(
            "host",
            "essa_set_result",
            |mut caller: Caller<'_, HostState>, buf_ptr: u32, buf_len: u32| {
                // copy the given memory region out of the sandbox
                let mem = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => bail!("failed to find host memory"),
                };
                let mut buf = vec![0; buf_len as usize];
                mem.read(&mut caller, buf_ptr as usize, &mut buf)
                    .context("ptr/len out of bounds")?;

                // set the function result in the host state
                caller.data_mut().function_result = Some(buf);

                Ok(EssaResult::Ok as i32)
            },
        )
        .context("failed to create essa_set_result")?;
    linker
        .func_wrap(
            "host",
            "essa_call",
            |caller: Caller<'_, HostState>,
             function_name_ptr: u32,
             function_name_len: u32,
             serialized_args_ptr: u32,
             serialized_arg_len: u32,
             result_handle_ptr: u32| {
                essa_call_wrapper(
                    caller,
                    function_name_ptr,
                    function_name_len,
                    serialized_args_ptr,
                    serialized_arg_len,
                    result_handle_ptr,
                )
                .map(|r| r as i32)
            },
        )
        .context("failed to create essa_call host function")?;
    linker
        .func_wrap(
            "host",
            "essa_run_r",
            |caller: Caller<'_, HostState>,
             function_ptr: u32,
             function_len: u32,
             serialized_args_ptr: u32,
             serialized_arg_len: u32,
             result_handle_ptr: u32| {
                essa_run_r_wrapper(
                    caller,
                    function_ptr,
                    function_len,
                    serialized_args_ptr,
                    serialized_arg_len,
                    result_handle_ptr,
                )
                .map(|r| r as i32)
            },
        )
        .context("failed to create essa_run_r host function")?;
    linker
        .func_wrap(
            "host",
            "essa_series_new",
            |caller: Caller<'_, HostState>,
             col_name_ptr: u32,
             col_name_len: u32,
             vec_values_ptr: u32,
             vec_values_len: u32,
             result_handle_ptr: u32| {
                essa_series_new_wrapper(
                    caller,
                    col_name_ptr,
                    col_name_len,
                    vec_values_ptr,
                    vec_values_len,
                    result_handle_ptr,
                )
                .map(|r| r as i32)
            },
        )
        .context("failed to create essa_series_new host function")?;
    linker
        .func_wrap(
            "host",
            "essa_dataframe_new",
            |caller: Caller<'_, HostState>,
             vec_key_series_ptr: u32,
             vec_key_series_len: u32,
             result_handle_ptr: u32| {
                essa_dataframe_new_wrapper(
                    caller,
                    vec_key_series_ptr,
                    vec_key_series_len,
                    result_handle_ptr,
                )
                .map(|r| r as i32)
            },
        )
        .context("failed to create essa_dataframe_new host function")?;
    linker
        .func_wrap(
            "host",
            "essa_datafusion_run",
            |caller: Caller<'_, HostState>,
             sql_query_ptr: u32,
             sql_query_len: u32,
             delta_table_ptr: u32,
             delta_table_len: u32,
             result_handle_ptr: u32| {
                essa_datafusion_run_wrapper(
                    caller,
                    sql_query_ptr,
                    sql_query_len,
                    delta_table_ptr,
                    delta_table_len,
                    result_handle_ptr,
                )
                .map(|r| r as i32)
            },
        )
        .context("failed to create essa_run_r host function")?;
    linker
        .func_wrap(
            "host",
            "essa_deltalake_save",
            |caller: Caller<'_, HostState>,
             table_path_ptr: u32,
             table_path_len: u32,
             dataframe_handler_ptr: u32,
             dataframe_handler_len: u32,
             result_handle_ptr: u32| {
                essa_deltalake_save_wrapper(
                    caller,
                    table_path_ptr,
                    table_path_len,
                    dataframe_handler_ptr,
                    dataframe_handler_len,
                    result_handle_ptr,
                )
                .map(|r| r as i32)
            },
        )
        .context("failed to create essa_run_r host function")?;
    linker
        .func_wrap(
            "host",
            "essa_get_result_len",
            |caller: Caller<'_, HostState>, handle: u32, value_len_ptr: u32| {
                essa_get_result_len_wrapper(caller, handle, value_len_ptr).map(|r| r as i32)
            },
        )
        .context("failed to create essa_get_result_len host function")?;
    linker
        .func_wrap(
            "host",
            "essa_get_result",
            |caller: Caller<'_, HostState>,
             handle: u32,
             value_ptr: u32,
             value_capacity: u32,
             value_len_ptr: u32| {
                essa_get_result_wrapper(caller, handle, value_ptr, value_capacity, value_len_ptr)
                    .map(|r| r as i32)
            },
        )
        .context("failed to create essa_get_result host function")?;
    linker
        .func_wrap(
            "host",
            "essa_put_lattice",
            |caller: Caller<'_, HostState>,
             key_ptr: u32,
             key_len: u32,
             value_ptr: u32,
             value_len: u32| {
                essa_put_lattice_wrapper(caller, key_ptr, key_len, value_ptr, value_len)
                    .map(|r| r as i32)
            },
        )
        .context("failed to create essa_put_lattice host function")?;
    linker
        .func_wrap(
            "host",
            "essa_get_lattice_len",
            |caller: Caller<'_, HostState>, key_ptr: u32, key_len: u32, value_len_ptr: u32| {
                essa_get_lattice_len_wrapper(caller, key_ptr, key_len, value_len_ptr)
                    .map(|r| r as i32)
            },
        )
        .context("failed to create essa_get_lattice host function")?;
    linker
        .func_wrap(
            "host",
            "essa_get_lattice_data",
            |caller: Caller<'_, HostState>,
             key_ptr: u32,
             key_len: u32,
             value_ptr: u32,
             value_capacity: u32,
             value_len_ptr: u32| {
                essa_get_lattice_data_wrapper(
                    caller,
                    key_ptr,
                    key_len,
                    value_ptr,
                    value_capacity,
                    value_len_ptr,
                )
                .map(|r| r as i32)
            },
        )
        .context("failed to create essa_get_lattice_data host function")?;

    // add WASI functionality (e.g. stdin/stdout access)
    wasmtime_wasi::add_to_linker(&mut linker, |state: &mut HostState| &mut state.wasi)
        .context("failed to add wasi functionality to linker")?;

    // link the host and WASI functions with the user-supplied WASM module
    linker
        .module(&mut store, "", module)
        .context("failed to add module to linker")?;

    Ok((store, linker))
}

/// Host function for calling the specified function on a remote node.
fn essa_call_wrapper (
    mut caller: Caller<HostState>,
    function_name_ptr: u32,
    function_name_len: u32,
    serialized_args_ptr: u32,
    serialized_args_len: u32,
    result_handle_ptr: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to get the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };

    // read the function name from the WASM sandbox
    let function_name = {
        let mut data = vec![0u8; function_name_len as usize];
        mem.read(&caller, function_name_ptr as usize, &mut data)
            .context("function name ptr/len out of bounds")?;
        String::from_utf8(data).context("function name not valid utf8")?
    };
    // read the serialized function arguments from the WASM sandbox
    let args = {
        let mut data = vec![0u8; serialized_args_len as usize];
        mem.read(&caller, serialized_args_ptr as usize, &mut data)
            .context("function name ptr/len out of bounds")?;
        data
    };

    // trigger the external function call
    match caller.data_mut().essa_call(function_name.clone(), args) {
        Ok(reply) => {
            let host_state = caller.data_mut();
            let handle = host_state.next_result_handle;
            host_state.next_result_handle += 1;
            host_state.result_receivers.insert(handle, reply);

            // write handle
            mem.write(
                &mut caller,
                result_handle_ptr as usize,
                &handle.to_le_bytes(),
            )
            .context("result_handle_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

/// Host function for calling the specified R function on a remote node.
fn essa_run_r_wrapper(
    mut caller: Caller<HostState>,
    function_ptr: u32,
    function_len: u32,
    serialized_args_ptr: u32,
    serialized_args_len: u32,
    result_handle_ptr: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to get the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };
    // read the function name from the WASM sandbox
    let function = {
        let mut data = vec![0u8; function_len as usize];
        mem.read(&caller, function_ptr as usize, &mut data)
            .context("function ptr/len out of bounds")?;
        String::from_utf8(data).context("function name not valid utf8")?
    };
    // read the serialized function arguments from the WASM sandbox
    let args = {
        let mut data = vec![0u8; serialized_args_len as usize];
        mem.read(&caller, serialized_args_ptr as usize, &mut data)
            .context("function args ptr/len out of bounds")?;
        data
    };

    match caller.data_mut().essa_run_r(function, args) {
        Ok(reply) => {
            let host_state = caller.data_mut();
            let handle = host_state.next_result_handle;
            host_state.next_result_handle += 1;
            host_state.result_receivers.insert(handle, reply);

            // write handle
            mem.write(
                &mut caller,
                result_handle_ptr as usize,
                &handle.to_le_bytes(),
            )
            .context("result_handle_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

/// Host function for calling the specified R function on a remote node.
fn essa_series_new_wrapper(
    mut caller: Caller<HostState>,
    col_name_ptr: u32,
    col_name_len: u32,
    vec_values_ptr: u32,
    vec_values_len: u32,
    result_handle_ptr: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to get the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };
    // read the function name from the WASM sandbox
    let col_name = {
        let mut data = vec![0u8; col_name_len as usize];
        mem.read(&caller, col_name_ptr as usize, &mut data)
            .context("function ptr/len out of bounds")?;
        String::from_utf8(data).context("col name not valid utf8")?
    };
    // read the serialized function arguments from the WASM sandbox
    let vec_values_serialized = {
        let mut data = vec![0u8; vec_values_len as usize];
        mem.read(&caller, vec_values_ptr as usize, &mut data)
            .context("vec values ptr/len out of bounds")?;
        data
    };

    let vec_values = split_args_to_f64(&vec_values_serialized);

    match caller.data_mut().essa_series_new(col_name, vec_values) {
        Ok(key_client) => {
            let host_state = caller.data_mut();
            let handle = host_state.next_result_handle;
            host_state.next_result_handle += 1;
            host_state.results.insert(handle, key_client);

            // write handle
            mem.write(
                &mut caller,
                result_handle_ptr as usize,
                &handle.to_le_bytes(),
            )
            .context("result_handle_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

/// Host function for calling the specified R function on a remote node.
fn essa_dataframe_new_wrapper(
    mut caller: Caller<HostState>,
    vec_client_key_series_ptr: u32,
    vec_client_key_series_len: u32,
    result_handle_ptr: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to get the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };

    // read the serialized function arguments from the WASM sandbox
    let handle_vec_series = {
        let mut data = vec![0u8; vec_client_key_series_len as usize];
        mem.read(&caller, vec_client_key_series_ptr as usize, &mut data)
            .context("vec values ptr/len out of bounds")?;
        data
    };

    println!("handle vec series: {:?}", handle_vec_series);

    let vec_key_series_handle = split_args_to_handles(&handle_vec_series);

    println!("key vec series: {:?}", vec_key_series_handle);

    // TODO: this should be better
    let vec_key_series: Vec<ClientKey> = vec_key_series_handle
        .iter()
        .map(|handle: &u32| {
            // TODO: remove unwrap()
            smol::block_on(caller.data_mut().get_result(*handle))
                .map_err(|_| EssaResult::UnknownError)
                .unwrap()
        })
        .collect();

    println!("vec key series: {:?}", vec_key_series);

    match caller.data_mut().essa_dataframe_new(vec_key_series) {
        Ok(key_client) => {
            let host_state = caller.data_mut();
            let handle = host_state.next_result_handle;
            host_state.next_result_handle += 1;
            host_state.results.insert(handle, key_client);

            // write handle
            mem.write(
                &mut caller,
                result_handle_ptr as usize,
                &handle.to_le_bytes(),
            )
            .context("result_handle_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

/// Host function for running a `Datafusion` query to a `Deltalake`.
fn essa_datafusion_run_wrapper(
    mut caller: Caller<HostState>,
    sql_query_ptr: u32,
    sql_query_len: u32,
    delta_table_ptr: u32,
    delta_table_len: u32,
    result_handle_ptr: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to get the memory export of the
    // module which called this host sql_query.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };
    // read the sql_query name from the WASM sandbox
    let sql_query: String = {
        let mut data = vec![0u8; sql_query_len as usize];
        mem.read(&caller, sql_query_ptr as usize, &mut data)
            .context("sql_query ptr/len out of bounds")?;
        String::from_utf8(data).context("sql_query name not valid utf8")?
    };
    // read the serialized sql_query arguments from the WASM sandbox
    let delta_table: String = {
        let mut data = vec![0u8; delta_table_len as usize];
        mem.read(&caller, delta_table_ptr as usize, &mut data)
            .context("sql_query args ptr/len out of bounds")?;
        String::from_utf8(data).context("delta_table name not valid utf8")?
    };

    match caller
        .data_mut()
        .essa_datafusion_run(sql_query, delta_table)
    {
        Ok(reply) => {
            let host_state = caller.data_mut();
            let handle = host_state.next_result_handle;
            host_state.next_result_handle += 1;
            host_state.result_receivers.insert(handle, reply);

            // write handle
            mem.write(
                &mut caller,
                result_handle_ptr as usize,
                &handle.to_le_bytes(),
            )
            .context("result_handle_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

/// Host function for running a `Datafusion` query to a `Deltalake`.
fn essa_deltalake_save_wrapper(
    mut caller: Caller<HostState>,
    table_path_ptr: u32,
    table_path_len: u32,
    dataframe_handler_ptr: u32,
    dataframe_handler_len: u32,
    result_handle_ptr: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to get the memory export of the
    // module which called this host sql_query.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };
    // read the sql_query name from the WASM sandbox
    let table_path = {
        let mut data = vec![0u8; table_path_len as usize];
        mem.read(&caller, table_path_ptr as usize, &mut data)
            .context("sql_query ptr/len out of bounds")?;
        String::from_utf8(data).context("sql_query name not valid utf8")?
    };

    // read the serialized function arguments from the WASM sandbox
    let args = {
        let mut data = vec![0u8; dataframe_handler_len as usize];
        mem.read(&caller, dataframe_handler_ptr as usize, &mut data)
            .context("function args ptr/len out of bounds")?;
        data
    };

    match caller.data_mut().essa_deltalake_save(table_path, args) {
        Ok(reply) => {
            let host_state = caller.data_mut();
            let handle = host_state.next_result_handle;
            host_state.next_result_handle += 1;
            host_state.result_receivers.insert(handle, reply);

            // write handle
            mem.write(
                &mut caller,
                result_handle_ptr as usize,
                &handle.to_le_bytes(),
            )
            .context("result_handle_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

fn essa_get_result_len_wrapper(
    mut caller: Caller<HostState>,
    handle: u32,
    val_len_ptr: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to learn about the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };

    // get the corresponding value from the KVS
    match smol::block_on(caller.data_mut().get_result(handle)) {
        Ok(key) => {
            println!("key: {:?}", key);
            let value_lattice: LatticeValue = kvs_get(key, &mut caller.data_mut().anna)?;

            let value_serialized = value_lattice
                .into_lww()
                .map_err(crate::eyre_to_anyhow)
                .unwrap()
                .into_revealed()
                .into_value();

            let len = value_serialized.len();
            println!("+++++++++len no get len: {len}");
            // write the length of the value into the sandbox
            //
            // We cannot write the value directly because the WASM module
            // needs to allocate some space for the (dynamically-sized) value
            // first.
            mem.write(
                &mut caller,
                val_len_ptr as usize,
                &u32::try_from(len).unwrap().to_le_bytes(),
            )
            .context("val_len_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

fn essa_get_result_wrapper(
    mut caller: Caller<HostState>,
    handle: u32,
    val_ptr: u32,
    val_capacity: u32,
    val_len_ptr: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to learn about the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };

    // get the corresponding value from the KVS
    match smol::block_on(caller.data_mut().get_result(handle)) {
        Ok(key) => {
            let value_lattice: LatticeValue = kvs_get(key, &mut caller.data_mut().anna)?;
            log::info!(
                "`essa_get_result` lattice from handle = {handle} is {:?}",
                value_lattice
            );

            let value_serialized = value_lattice
                .into_lww()
                .map_err(crate::eyre_to_anyhow)
                .unwrap()
                .into_revealed()
                .into_value();

            if value_serialized.len() > val_capacity as usize {
                Ok(EssaResult::BufferTooSmall)
            } else {
                // write the value into the sandbox
                mem.write(&mut caller, val_ptr as usize, &value_serialized)
                    .context("val ptr/len out of bounds")?;
                // write the length of the value
                mem.write(
                    &mut caller,
                    val_len_ptr as usize,
                    &u32::try_from(value_serialized.len()).unwrap().to_le_bytes(),
                )
                .context("val_len_ptr out of bounds")?;

                // TODO: Should/could I make this work?
                //caller.data_mut().remove_result(handle);

                Ok(EssaResult::Ok)
            }
        }
        Err(err) => Ok(err),
    }
}

/// Host function for storing a given lattice value into the KVS.
fn essa_put_lattice_wrapper(
    mut caller: Caller<HostState>,
    key_ptr: u32,
    key_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to learn about the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };
    // read out and parse the KVS key
    let key = {
        let mut data = vec![0u8; key_len as usize];
        mem.read(&caller, key_ptr as usize, &mut data)
            .context("key ptr/len out of bounds")?;
        String::from_utf8(data)
            .context("key is not valid utf8")?
            .into()
    };
    // read out the value that should be stored
    let value = {
        let mut data = vec![0u8; value_len as usize];
        mem.read(&caller, value_ptr as usize, &mut data)
            .context("value ptr/len out of bounds")?;
        data
    };

    match caller.data_mut().put_lattice(&key, &value) {
        Ok(()) => Ok(EssaResult::Ok),
        Err(other) => Ok(other),
    }
}

/// Host function for reading the length of value stored under a specific key
/// in the KVS.
fn essa_get_lattice_len_wrapper(
    mut caller: Caller<HostState>,
    key_ptr: u32,
    key_len: u32,
    val_len_ptr: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to learn about the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };
    // read out and parse the KVS key
    let key = {
        let mut data = vec![0u8; key_len as usize];
        mem.read(&caller, key_ptr as usize, &mut data)
            .context("key ptr/len out of bounds")?;
        String::from_utf8(data)
            .context("key is not valid utf8")?
            .into()
    };

    // get the corresponding value from the KVS
    match caller.data_mut().get_lattice(&key) {
        Ok(value) => {
            // write the length of the value into the sandbox
            //
            // We cannot write the value directly because the WASM module
            // needs to allocate some space for the (dynamically-sized) value
            // first.
            mem.write(
                &mut caller,
                val_len_ptr as usize,
                &u32::try_from(value.len()).unwrap().to_le_bytes(),
            )
            .context("val_len_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

/// Host function for reading a specific value from the KVS.
fn essa_get_lattice_data_wrapper(
    mut caller: Caller<HostState>,
    key_ptr: u32,
    key_len: u32,
    val_ptr: u32,
    val_capacity: u32,
    val_len_ptr: u32,
) -> anyhow::Result<EssaResult> {
    // Use our `caller` context to learn about the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => bail!("failed to find host memory"),
    };
    // read out and parse the KVS key
    let key = {
        let mut data = vec![0u8; key_len as usize];
        mem.read(&caller, key_ptr as usize, &mut data)
            .context("key ptr/len out of bounds")?;
        String::from_utf8(data)
            .context("key is not valid utf8")?
            .into()
    };
    // get the corresponding value from the KVS
    match caller.data_mut().get_lattice(&key) {
        Ok(value) => {
            if value.len() > val_capacity as usize {
                Ok(EssaResult::BufferTooSmall)
            } else {
                // write the value into the sandbox
                mem.write(&mut caller, val_ptr as usize, value.as_slice())
                    .context("val ptr/len out of bounds")?;
                // write the length of the value
                mem.write(
                    &mut caller,
                    val_len_ptr as usize,
                    &u32::try_from(value.len()).unwrap().to_le_bytes(),
                )
                .context("val_len_ptr out of bounds")?;

                Ok(EssaResult::Ok)
            }
        }
        Err(err) => Ok(err),
    }
}

/// Stores all the information needed during execution.
///
/// The `wasmtime` crate gives this struct as an additional argument to all
/// host functions, which makes it possible to keep state between across
/// host function invocations.
pub struct HostState {
    wasi: WasiCtx,
    /// The compiled WASM module.
    module: Module,
    /// The KVS key under which a serialized version of the compiled WASM
    /// module is stored.
    module_key: ClientKey,
    /// The result value of this function, set through the `essa_set_result`
    /// host function.
    function_result: Option<Vec<u8>>,

    next_result_handle: u32,
    result_receivers: HashMap<u32, flume::Receiver<Reply>>,
    results: HashMap<u32, ClientKey>,

    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
    anna: ClientNode,
}

impl HostState {
    /// Calls the given function on a node and returns the reply receiver for
    /// the corresponding result.
    fn essa_call(
        &mut self,
        function_name: String,
        args: Vec<u8>,
    ) -> Result<flume::Receiver<Reply>, EssaResult> {
        // get the requested function and check its signature
        let func = self
            .module
            .get_export(&function_name)
            .and_then(|e| e.func().cloned())
            .ok_or(EssaResult::NoSuchFunction)?;
        if func.params().collect::<Vec<_>>().as_slice() != [ValType::I32]
            || !func.results().collect::<Vec<_>>().as_slice().is_empty()
        {
            return Err(EssaResult::InvalidFunctionSignature);
        }

        // store args in kvs
        let args_key: ClientKey = Uuid::new_v4().to_string().into();
        kvs_put(
            args_key.clone(),
            LastWriterWinsLattice::new_now(args).into(),
            &mut self.anna,
        )
        .map_err(|_| EssaResult::FailToSaveKVS)?;

        // trigger the function call on a remote node
        let reply = smol::block_on(call_function_extern(
            self.module_key.clone(),
            function_name,
            args_key,
            self.zenoh.clone(),
            &self.zenoh_prefix,
        ))
        .map_err(|_| EssaResult::FailToCallExternal)?;

        Ok(reply)
    }

    /// Calls the given function on a node and returns the reply receiver for
    /// the corresponding result.
    fn essa_run_r(
        &mut self,
        func: String,
        args: Vec<u8>,
    ) -> Result<flume::Receiver<Reply>, EssaResult> {
        let func_key: ClientKey = Uuid::new_v4().to_string().into();
        kvs_put(
            func_key.clone(),
            LastWriterWinsLattice::new_now(func.as_bytes().into()).into(),
            &mut self.anna,
        )
        .map_err(|_| EssaResult::FailToSaveKVS)?;

        let args_handles: Vec<u32> = split_args_to_handles(&args);

        // TODO: this should be better
        let args_key_vec: Vec<ClientKey> = args_handles
            .iter()
            .map(|handle: &u32| {
                let result = smol::block_on(self.get_result(*handle));
                println!("**********{handle} result = {result:?}");
                result.map_err(|_| EssaResult::UnknownError).unwrap()
            })
            .collect();

        let args_vec_key: ClientKey = Uuid::new_v4().to_string().into();
        let serialized_args_key_vec =
            bincode::serialize(&args_key_vec).map_err(|_| EssaResult::UnknownError)?;
        kvs_put(
            args_vec_key.clone(),
            LastWriterWinsLattice::new_now(serialized_args_key_vec).into(),
            &mut self.anna,
        )
        .map_err(|_| EssaResult::FailToSaveKVS)?;

        // trigger the function call on a remote node
        let reply = smol::block_on(run_r_extern(
            func_key,
            args_vec_key,
            self.zenoh.clone(),
            &self.zenoh_prefix,
        ))
        .map_err(|_| EssaResult::FailToCallExternal)?;

        Ok(reply)
    }

    /// Creates a `Series::new`
    fn essa_series_new(
        &mut self,
        col_name: String,
        vec_values: Vec<f64>,
    ) -> Result<ClientKey, EssaResult> {
        let vec_series = Series::new(&col_name, vec_values);
        let serialized_vec_series =
            bincode::serialize(&vec_series).map_err(|_| EssaResult::UnknownError)?;

        let key_vec_series: ClientKey = Uuid::new_v4().to_string().into();
        // TODO: add series as a supported values inside anna.
        let lattice: anna::store::LatticeValue =
            LastWriterWinsLattice::new_now(serialized_vec_series).into();
        kvs_put(key_vec_series.clone(), lattice, &mut self.anna)
            .map_err(|_| EssaResult::FailToSaveKVS)?;

        Ok(key_vec_series)
    }

    /// Creates a `Dataframe::new`
    fn essa_dataframe_new(
        &mut self,
        vec_key_series: Vec<ClientKey>,
    ) -> Result<ClientKey, EssaResult> {
        // TODO: shouldn't `unwrap` this
        let vec_series_serialized: Vec<LatticeValue> = vec_key_series
            .into_iter()
            .map(|key| kvs_get(key, &mut self.anna).unwrap())
            .collect();
        println!("vec series serialized: {:?}", vec_series_serialized);

        // TODO: better error handling is needed here.
        let vec_series: Vec<Series> = vec_series_serialized
            .into_iter()
            .map(|serie_lattice| {
                let value = serie_lattice
                    .into_lww()
                    .map_err(crate::eyre_to_anyhow)
                    .unwrap()
                    .into_revealed()
                    .into_value();

                bincode::deserialize::<Series>(&value).unwrap()
            })
            .collect();
        println!("vec series: {:?}", vec_series);

        let dataframe = DataFrame::new(vec_series).map_err(|_| EssaResult::UnknownError)?;
        let serialized_dataframe =
            bincode::serialize(&dataframe).map_err(|_| EssaResult::UnknownError)?;

        let key_dataframe: ClientKey = Uuid::new_v4().to_string().into();
        println!("key dataframe: {:?}", key_dataframe);
        // TODO: add series as a supported values inside anna.
        let lattice: anna::store::LatticeValue =
            LastWriterWinsLattice::new_now(serialized_dataframe).into();
        kvs_put(key_dataframe.clone(), lattice, &mut self.anna)
            .map_err(|_| EssaResult::FailToSaveKVS)?;

        Ok(key_dataframe)
    }

    /// Calls the given function on a node and returns the reply receiver for
    /// the corresponding result.
    fn essa_datafusion_run(
        &mut self,
        sql_query: String,
        delta_table: String,
    ) -> Result<flume::Receiver<Reply>, EssaResult> {
        let sql_query_key: ClientKey = Uuid::new_v4().to_string().into();
        kvs_put(
            sql_query_key.clone(),
            LastWriterWinsLattice::new_now(sql_query.as_bytes().into()).into(),
            &mut self.anna,
        )
        .map_err(|_| EssaResult::FailToSaveKVS)?;

        // store args in kvs
        let delta_table_key: ClientKey = Uuid::new_v4().to_string().into();
        kvs_put(
            delta_table_key.clone(),
            LastWriterWinsLattice::new_now(delta_table.as_bytes().into()).into(),
            &mut self.anna,
        )
        .map_err(|_| EssaResult::FailToSaveKVS)?;

        // trigger the function call on a remote node
        let reply = smol::block_on(datafusion_run_extern(
            sql_query_key,
            delta_table_key,
            self.zenoh.clone(),
            &self.zenoh_prefix,
        ))
        .map_err(|_| EssaResult::FailToCallExternal)?;

        Ok(reply)
    }

    /// Calls the given function on a node and returns the reply receiver for
    /// the corresponding result.
    fn essa_deltalake_save(
        &mut self,
        table_path: String,
        vec_handle: Vec<u8>,
    ) -> Result<flume::Receiver<Reply>, EssaResult> {
        let table_path_key: ClientKey = Uuid::new_v4().to_string().into();
        kvs_put(
            table_path_key.clone(),
            LastWriterWinsLattice::new_now(table_path.as_bytes().into()).into(),
            &mut self.anna,
        )
        .map_err(|_| EssaResult::FailToSaveKVS)?;

        let args_handles: Vec<u32> = split_args_to_handles(&vec_handle);

        println!("args handles: {:?}", args_handles);

        // TODO: this should be better
        let args_key_vec: Vec<ClientKey> = args_handles
            .iter()
            .map(|handle: &u32| {
                // TODO: remove unwrap()
                println!(
                    "meu get result do deltalake sabe: {:?}",
                    smol::block_on(self.get_result(*handle))
                );
                smol::block_on(self.get_result(*handle))
                    .map_err(|_| EssaResult::UnknownError)
                    .unwrap()
            })
            .collect();

        let args_vec_key: ClientKey = Uuid::new_v4().to_string().into();
        let serialized_args_key_vec =
            bincode::serialize(&args_key_vec).map_err(|_| EssaResult::UnknownError)?;
        kvs_put(
            args_vec_key.clone(),
            LastWriterWinsLattice::new_now(serialized_args_key_vec).into(),
            &mut self.anna,
        )
        .map_err(|_| EssaResult::FailToSaveKVS)?;

        // trigger the function call on a remote node
        let reply = smol::block_on(deltalake_save_extern(
            table_path_key,
            args_vec_key,
            self.zenoh.clone(),
            &self.zenoh_prefix,
        ))
        .map_err(|_| EssaResult::FailToCallExternal)?;

        Ok(reply)
    }

    /// Stores the given serialized `LattiveValue` in the KVS.
    fn put_lattice(&mut self, key: &ClientKey, value: &[u8]) -> Result<(), EssaResult> {
        let value = bincode::deserialize(value).map_err(|_| EssaResult::UnknownError)?;
        kvs_put(self.with_prefix(key), value, &mut self.anna).map_err(|_| EssaResult::FailToSaveKVS)
    }

    /// Reads the `LattiveValue` at the specified key from the KVS serializes it.
    fn get_lattice(&mut self, key: &ClientKey) -> Result<Vec<u8>, EssaResult> {
        kvs_get(self.with_prefix(key), &mut self.anna)
            .map_err(|_| EssaResult::NotFound)
            .and_then(|v| bincode::serialize(&v).map_err(|_| EssaResult::UnknownError))
    }

    fn with_prefix(&self, key: &ClientKey) -> ClientKey {
        format!("{}/data/{}", self.module_key, key).into()
    }

    async fn get_result(&mut self, handle: u32) -> Result<ClientKey, EssaResult> {
        println!(
            "dentro do get_result onlhando hashmap: {:?}\n\n",
            self.results.entry(handle)
        );
        match self.results.entry(handle) {
            std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                if let Some(result) = self.result_receivers.get(&handle) {
                    let reply = result.recv_async().await.map_err(|e| {
                        log::debug!("Error get_result, recv_async: {e:?}");
                        EssaResult::UnknownError
                    })?;

                    log::info!("reply: {:?}", reply.sample);
                    let value = reply
                        .sample
                        .map_err(|_e| EssaResult::UnknownError)?
                        .value
                        .payload
                        .contiguous()
                        .into_owned();

                    let a = bincode::deserialize::<usize>(&value);
                    log::info!("Value from `get_result` {handle}: {value:?}, a = {a:?}");

                    let key_value: ClientKey =
                        bincode::deserialize(&value).map_err(|_| EssaResult::FailToCallExternal)?;
                    log::info!("ClientKey from `get_result` {handle}: {key_value:?}");

                    let _value = entry.insert(key_value.clone());

                    Ok(key_value)
                } else {
                    log::error!("result_receivers for {handle} is None");
                    Err(EssaResult::NotFound)
                }
            }
        }
    }

    // fn _remove_result(&mut self, handle: u32) {
    //     self.results.remove(&handle);
    // }
}

/// Call the specfied function on a remote node.
async fn call_function_extern(
    module_key: ClientKey,
    function_name: String,
    args_key: ClientKey,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: &str,
) -> anyhow::Result<flume::Receiver<Reply>> {
    let topic = scheduler_function_call_topic(zenoh_prefix, &module_key, &function_name, &args_key);

    // send the request to the scheduler node
    let reply = zenoh
        .get(topic)
        .timeout(std::time::Duration::from_secs(20))
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to send function call request to scheduler")?;

    Ok(reply)
}

/// Call the specfied function on a remote node.
async fn run_r_extern(
    func_key: ClientKey,
    args_key: ClientKey,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: &str,
) -> anyhow::Result<flume::Receiver<Reply>> {
    let topic = scheduler_run_r_function_call_topic(zenoh_prefix, &func_key, &args_key);

    // send the request to the scheduler node
    let reply = zenoh
        .get(topic)
        .timeout(std::time::Duration::from_secs(20))
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to send function call request to scheduler")?;

    Ok(reply)
}

async fn datafusion_run_extern(
    sql_query_key: ClientKey,
    delta_table_key: ClientKey,
    zenoh: Arc<zenoh::Session>,
    _zenoh_prefix: &str,
) -> anyhow::Result<Receiver<Reply>> {
    println!("Datafusion query_expr: essa/datafusion/{sql_query_key}/{delta_table_key}");
    let topic = format!("essa/datafusion/{sql_query_key}/{delta_table_key}");

    let reply = zenoh
        .get(topic)
        .timeout(std::time::Duration::from_secs(20))
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed datafusion")?;

    Ok(reply)
}

async fn deltalake_save_extern(
    table_path_key: ClientKey,
    dataframe_key: ClientKey,
    zenoh: Arc<zenoh::Session>,
    _zenoh_prefix: &str,
) -> anyhow::Result<Receiver<Reply>> {
    let topic = format!("essa/save-deltalake/{table_path_key}/{dataframe_key}");

    let reply = zenoh
        .get(topic)
        .timeout(std::time::Duration::from_secs(20))
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed save deltalake")?;

    Ok(reply)
}

// TODO: make those `split_args_to_*` a macro.
fn split_args_to_handles(args: &[u8]) -> Vec<u32> {
    let mut ptr_args = args;
    let mut handles = vec![];

    loop {
        let (int_bytes, rest) = ptr_args.split_at(std::mem::size_of::<u32>());

        handles.push(u32::from_ne_bytes(int_bytes.try_into().unwrap()));
        ptr_args = rest;

        if rest.is_empty() {
            return handles;
        }
    }
}

fn split_args_to_f64(args: &[u8]) -> Vec<f64> {
    let mut ptr_args = args;
    let mut handles = vec![];

    loop {
        let (int_bytes, rest) = ptr_args.split_at(std::mem::size_of::<f64>());

        handles.push(f64::from_ne_bytes(int_bytes.try_into().unwrap()));
        ptr_args = rest;

        if rest.is_empty() {
            return handles;
        }
    }
}

// TODO: Remove this duplication.
/// Crate a new anna connection.
async fn new_anna_client(zenoh: Arc<zenoh::Session>) -> anyhow::Result<ClientNode> {
    let zenoh_prefix = anna::anna_default_zenoh_prefix().to_owned();

    let cluster_info = anna::nodes::request_cluster_info(&zenoh, &zenoh_prefix)
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to request cluster info from seed node")?;

    let routing_threads: Vec<_> = cluster_info
        .routing_node_ids
        .into_iter()
        .map(|node_id| anna::topics::RoutingThread {
            node_id,
            // TODO: use anna config file to get number of threads per
            // routing node
            thread_id: 0,
        })
        .collect();

    // connect to anna as a new client node
    let mut anna = ClientNode::new(
        Uuid::new_v4().to_string(),
        0,
        routing_threads,
        std::time::Duration::from_secs(10),
        zenoh,
        zenoh_prefix,
    )
    .map_err(eyre_to_anyhow)
    .context("failed to connect to anna")?;

    anna.init_tcp_connections()
        .await
        .map_err(eyre_to_anyhow)
        .context("Failed to init TCP connections in anna client")?;

    Ok(anna)
}

/// Transforms an [`eyre::Report`] to an [`anyhow::Error`].
fn eyre_to_anyhow(err: eyre::Report) -> anyhow::Error {
    let err = Box::<dyn std::error::Error + 'static + Send + Sync>::from(err);
    anyhow::anyhow!(err)
}
