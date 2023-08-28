#![warn(missing_docs)]
use anna::{
    anna_default_zenoh_prefix,
    lattice::LastWriterWinsLattice,
    lattice::Lattice,
    nodes::{request_cluster_info, ClientNode},
    store::LatticeValue,
    topics::RoutingThread,
    ClientKey,
};
use anyhow::{anyhow, Context};
use essa_common::{essa_default_zenoh_prefix, executor_run_r_subscribe_topic};
use extendr_api::prelude::*;
use extendr_api::Robj;
use r_polars::conversion_r_to_s::par_read_robjs;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;
use zenoh::prelude::r#async::*;

/// Starts a essa R executor.
///
/// The given ID must be an unique identifier for this executor instance, i.e.
/// there must not be another active executor instance with the same id.
#[derive(argh::FromArgs, Debug, Clone)]
struct Args {
    #[argh(positional)]
    id: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Err(err) = set_up_logger() {
        eprintln!(
            "ERROR: {:?}",
            anyhow::anyhow!(err).context("Failed to set up logget")
        );
    }

    let args: Args = argh::from_env();

    let zenoh = Arc::new(
        zenoh::open(zenoh::config::Config::default())
            .res()
            .await
            .map_err(|e| anyhow::anyhow!(e))
            .context("Failed to connect to zenoh")?,
    );

    let zenoh_prefix = essa_default_zenoh_prefix().to_owned();
    extendr_engine::start_r();
    r_executor(args, zenoh, zenoh_prefix).await?;
    extendr_engine::end_r();

    Ok(())
}

/// R-executor.
async fn r_executor(
    args: Args,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
) -> anyhow::Result<()> {
    // This should not be hardcoded.
    let executor = args.id;

    let topic = executor_run_r_subscribe_topic(&zenoh_prefix, executor);
    log::info!("Starting R executor!");
    let reply = zenoh
        .declare_queryable(topic)
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to declare subscriber r-executor")?;

    let mut anna_client = new_anna_client(zenoh).await.unwrap();

    loop {
        match reply.recv_async().await {
            Ok(query) => {
                let mut topic_split = query.key_expr().as_str().split('/');
                let key_to_args = topic_split
                    .next_back()
                    .context("no args key in topic")?
                    .to_owned();
                let func = topic_split
                    .next_back()
                    .context("no func in topic")?
                    .to_owned();

                let serialized_args_vec_key = kvs_get(key_to_args.into(), &mut anna_client)?;

                let serialized_args_vec_key: Vec<u8> = serialized_args_vec_key
                    .into_lww()
                    .map_err(eyre_to_anyhow)
                    .context("func is not a LWW lattice")?
                    .into_revealed()
                    .into_value();

                let args_vec_key: Vec<ClientKey> = bincode::deserialize(&serialized_args_vec_key)?;

                let func = kvs_get(func.into(), &mut anna_client)?;
                let mut args_vec = vec![];
                for args_key in args_vec_key {
                    args_vec.push(kvs_get(args_key.into(), &mut anna_client)?);
                }

                let mut final_result: Vec<r_polars::polars::prelude::Series> = Vec::new();

                {
                    let func = func
                        .into_lww()
                        .map_err(eyre_to_anyhow)
                        .context("func is not a LWW lattice")?
                        .into_revealed()
                        .into_value();
                    let args_from_anna_vec: Vec<Vec<u8>> = args_vec
                        .into_iter()
                        .map(|arg| {
                            arg.into_lww()
                                .map_err(eyre_to_anyhow)
                                .context("args is not a LWW lattice")
                                .unwrap()
                                .into_revealed()
                                .into_value()
                        })
                        .collect();

                    let dataframes: Vec<r_polars::polars::prelude::DataFrame> = args_from_anna_vec
                        .iter()
                        .map(|args_from_anna| bincode::deserialize(&args_from_anna).unwrap())
                        .collect();

                    let r_dataframes: Vec<r_polars::rdataframe::DataFrame> = dataframes
                        .into_iter()
                        .map(|dataframe| (dataframe).into())
                        .collect();
                    println!("dataframes: {:?}", r_dataframes);

                    let r_lists: Vec<r_polars::Robj> = r_dataframes
                        .iter()
                        .map(|r_dataframe| r_dataframe.to_list_result().unwrap().into())
                        .collect();
                    //log::info!("r list: {:?}", r_lists);

                    let function = String::from_utf8(func)?;
                    // TODO: this error handling and semantics should be better.
                    let func: Robj = eval_string(&function).map_err(|e| {
                        log::error!("Failed with: {:?}", e);
                        anyhow!("Failed to `eval` function")
                    })?;
                    let arguments = func
                        .as_function()
                        .expect("The given function is not an `R function`")
                        .formals();

                    let mut arguments_pairlist: Vec<(&str, Robj)> = vec![];
                    match arguments {
                        Some(args) => {
                            for (name, r_list) in args.names().zip(r_lists) {
                                arguments_pairlist.push((name, r_list));
                            }
                        }
                        None => {
                            log::info!("R function has no args");
                        }
                    }

                    let result: Robj = func
                        .call(Pairlist::from_pairs(arguments_pairlist))
                        .map_err(|e| {
                            log::error!("Failed with: {:?}", e);
                            anyhow!("Failed to call R function")
                        })?;

                    // this not enough if the response is a dataframe, because it will be the
                    // most generic type possible.
                    println!("result rtype: {:?}", result.rtype());
                    final_result = match result.rtype() {
                        Rtype::Doubles => convert_from_robj_real_to_polars(result)?,
                        Rtype::Integers => convert_from_robj_integer_to_polars(result)?,
                        // TODO: error handling
                        _ => {
                            println!("Return type not suported");
                            convert_from_robj_integer_to_polars(r!(vec![1, 2]))?
                        }
                    };
                }

                let serialized = bincode::serialize(&final_result)?;
                let key: ClientKey = Uuid::new_v4().to_string().into();
                kvs_put(
                    key.clone(),
                    LastWriterWinsLattice::new_now(serialized.into()).into(),
                    &mut anna_client,
                )?;

                let key_serialized = bincode::serialize(&key)?;

                query
                    .reply(Ok(Sample::new(query.key_expr().clone(), key_serialized)))
                    .res()
                    .await
                    .expect("failed to send sample back");
            }
            Err(e) => {
                log::debug!("zenoh error {e:?}");
                break;
            }
        }
    }

    Ok(())
}

macro_rules! create_convert_function {
    ($name:ident, $as_type_slice:ident) => {
        fn $name(result: Robj) -> anyhow::Result<Vec<r_polars::polars::prelude::Series>> {
            let mut results = result
                .$as_type_slice()
                .ok_or(anyhow!("Couldn't get a `real slice`"))?
                .chunks_exact(result.len());
            if let Some(dim) = result.dim() {
                match dim.iter().collect::<Vec<_>>().as_slice() {
                    &[_line, col] => {
                        results = result
                            .$as_type_slice()
                            .ok_or(anyhow!("Couldn't get a `real slice`"))?
                            .chunks_exact(col.0 as usize)
                    }
                    _ => (),
                }
            };

            let mut par_objs = vec![];

            for (i, result) in results.enumerate() {
                par_objs.push((
                    r_polars::utils::extendr_concurrent::ParRObj(result.into()),
                    i.to_string(),
                ));
            }

            Ok(par_read_robjs(par_objs)?)
        }
    };
}

create_convert_function!(convert_from_robj_real_to_polars, as_real_slice);
create_convert_function!(convert_from_robj_integer_to_polars, as_integer_slice);

/// Get the given value in the key-value store.
///
/// Returns an error if the requested key does not exist in the KVS.
fn kvs_get(key: ClientKey, anna: &mut ClientNode) -> anyhow::Result<LatticeValue> {
    smol::block_on(anna.get(key))
        .map_err(eyre_to_anyhow)
        .context("get failed")
}

/// Store the given value in the key-value store.
fn kvs_put(key: ClientKey, value: LatticeValue, anna: &mut ClientNode) -> anyhow::Result<()> {
    let start = Instant::now();

    smol::block_on(anna.put(key, value))
        .map_err(eyre_to_anyhow)
        .context("put failed")?;

    let put_latency = (Instant::now() - start).as_millis();
    if put_latency >= 100 {
        log::trace!("high kvs_put latency: {}ms", put_latency);
    }

    Ok(())
}

async fn new_anna_client(zenoh: Arc<zenoh::Session>) -> anyhow::Result<ClientNode> {
    let zenoh_prefix = anna_default_zenoh_prefix().to_owned();

    let cluster_info = request_cluster_info(&zenoh, &zenoh_prefix)
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to request cluster info from seed node")?;

    let routing_threads: Vec<_> = cluster_info
        .routing_node_ids
        .into_iter()
        .map(|node_id| RoutingThread {
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
        Duration::from_secs(10),
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

/// Set up the `log` crate.
fn set_up_logger() -> std::result::Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .level_for("zenoh", log::LevelFilter::Warn)
        .level_for("essa_function_executor", log::LevelFilter::Trace)
        .chain(std::io::stdout())
        .chain(fern::log_file("function-executor.log")?)
        .apply()?;
    Ok(())
}
