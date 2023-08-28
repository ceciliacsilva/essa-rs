use anna::{
    anna_default_zenoh_prefix,
    lattice::LastWriterWinsLattice,
    nodes::{request_cluster_info, ClientNode},
    store::LatticeValue,
    topics::RoutingThread,
    ClientKey,
};
use anyhow::Context;
use arrow_array::GenericListArray;
use essa_common::essa_default_zenoh_prefix;
use std::sync::Arc;
use uuid::Uuid;

use zenoh::prelude::r#async::*;

use anna::lattice::Lattice;

use deltalake::{
    arrow::{
        datatypes::{DataType, Field, Schema as ArrowSchema},
        record_batch::RecordBatch,
    },
    DeltaOps, DeltaTable, SchemaDataType, SchemaField, SchemaTypeArray,
};
use parquet::file::properties::WriterProperties;
use polars_sql::SQLContext;
use r_polars::polars::prelude::*;
use sqlparser::parser::Parser;
use sqlparser::{ast::TableWithJoins, dialect::GenericDialect};
use std::path::Path;
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Err(err) = set_up_logger() {
        eprintln!(
            "ERROR: {:?}",
            anyhow::anyhow!(err).context("Failed to set up logget")
        );
    }

    let zenoh = Arc::new(
        zenoh::open(zenoh::config::Config::default())
            .res()
            .await
            .map_err(|e| anyhow::anyhow!(e))
            .context("Failed to connect to zenoh")?,
    );

    let zenoh_prefix = essa_default_zenoh_prefix().to_owned();
    // listen for function call requests in a separate thread
    {
        let zenoh = zenoh.clone();
        let zenoh_prefix = zenoh_prefix.clone();

        tokio::spawn(async move {
            if let Err(err) = save_to_deltalake_loop(zenoh, &zenoh_prefix).await {
                log::error!("{:?}", err)
            }
        });
    }

    loop {
        if let Err(err) = polars_loop(zenoh.clone(), &zenoh_prefix).await {
            log::error!("Error while reading table: {:?}", err);
        }
    }

    Ok(())
}

pub async fn polars_loop(zenoh: Arc<zenoh::Session>, _zenoh_prefix: &str) -> anyhow::Result<()> {
    log::info!("Stating `polars` deltalake executor");
    let topic = "essa/datafusion/*/*";

    let reply = zenoh
        .declare_queryable(topic)
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to send function call request to scheduler")?;

    let mut anna_client = new_anna_client(zenoh).await?;

    loop {
        match reply.recv_async().await {
            Ok(query) => {
                let mut topic_split = query.key_expr().as_str().split('/');
                let delta_table_key: String = topic_split
                    .next_back()
                    .context("no args key in topic")?
                    .to_owned();
                let sql_query_key: String = topic_split
                    .next_back()
                    .context("no func in topic")?
                    .to_owned();

                let delta_table = kvs_get(delta_table_key.into(), &mut anna_client)?;
                let sql_query = kvs_get(sql_query_key.into(), &mut anna_client)?;
                let delta_table_path = delta_table
                    .into_lww()
                    .map_err(crate::eyre_to_anyhow)
                    .context("delta_table is not a LWW lattice")?
                    .into_revealed()
                    .into_value();
                let sql_query = sql_query
                    .into_lww()
                    .map_err(crate::eyre_to_anyhow)
                    .context("sql_query is not a LWW lattice")?
                    .into_revealed()
                    .into_value();

                let delta_table_path = String::from_utf8(delta_table_path)?;

                println!("delta table path: {:?}", delta_table_path);
                let mut ctx = SQLContext::new();
                let table = deltalake::open_table(delta_table_path.clone()).await?;

                println!("{:?}", table.get_files());

                let mut dfs = Vec::new();
                for file in table.get_files() {
                    let df = LazyFrame::scan_parquet(
                        format!("{}/{}", delta_table_path, file),
                        Default::default(),
                    )
                    .unwrap();

                    let _ = dfs.push(df);
                }

                let lazy_frame = diag_concat_lf(&dfs, false, false)?;

                let sql_query = String::from_utf8(sql_query)?;
                let dialect = &GenericDialect;
                let select = Parser::new(dialect)
                    .try_with_sql(&sql_query)?
                    .parse_select()?;
                //println!("select from get {:?}", select.from);
                // TODO: figure out why this only works for `SELECT <cols> FROM X`
                // and not for `SELECT * FROM X`.
                log::info!("query: {:?}", sql_query);
                log::info!("select: {:?}", select);
                let table_name = match select.from.get(0) {
                    Some(TableWithJoins {
                        relation,
                        joins: _j,
                    }) => relation.to_string(),
                    _ => {
                        log::info!("Could not a Table name from sql. Table will be filename");
                        let filename = delta_table_path.split("/").last().unwrap();
                        filename.to_string()
                    }
                };

                ctx.register(&table_name, lazy_frame);

                let table: r_polars::polars::prelude::DataFrame =
                    ctx.execute(&sql_query).unwrap().collect()?;

                log::info!("Table: {:?}", table);

                let table_serialized = bincode::serialize(&table)?;
                // store args in kvs
                let key: ClientKey = Uuid::new_v4().to_string().into();
                kvs_put(
                    key.clone(),
                    LastWriterWinsLattice::new_now(table_serialized.into()).into(),
                    &mut anna_client,
                )?;

                let key_serialized = bincode::serialize(&key)?;
                log::info!("key serialized: {:?}", key_serialized);

                query
                    .reply(Ok(Sample::new(query.key_expr().clone(), key_serialized)))
                    .res()
                    .await
                    .expect("Failed to send the `reply` back");

                log::info!("Sending Result back to {:?}", query.key_expr());
            }
            Err(e) => {
                log::info!("zenoh error {e:?}");
                break;
            }
        }
    }

    Ok(())
}

pub async fn save_to_deltalake_loop(
    zenoh: Arc<zenoh::Session>,
    _zenoh_prefix: &str,
) -> anyhow::Result<()> {
    log::info!("Stating `polars` deltalake save executor");
    let topic = "essa/save-deltalake/*/*";

    let reply = zenoh
        .declare_queryable(topic)
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to get `save to deltalake` calls")?;

    let mut anna_client = new_anna_client(zenoh).await?;

    loop {
        match reply.recv_async().await {
            Ok(query) => {
                println!("Got a `toSave` message");
                let mut topic_split = query.key_expr().as_str().split('/');
                let key_to_args: String = topic_split
                    .next_back()
                    .context("no args key in topic")?
                    .to_owned();
                let delta_table_key: String = topic_split
                    .next_back()
                    .context("no args in topic")?
                    .to_owned();

                let serialized_args_vec_key = kvs_get(key_to_args.into(), &mut anna_client)?;

                let serialized_args_vec_key: Vec<u8> = serialized_args_vec_key
                    .into_lww()
                    .map_err(eyre_to_anyhow)
                    .context("func is not a LWW lattice")?
                    .into_revealed()
                    .into_value();

                let args_vec_key: Vec<ClientKey> = bincode::deserialize(&serialized_args_vec_key)?;
                println!("vec of client keys: {:?}", args_vec_key);

                let mut args_vec = vec![];
                for args_key in args_vec_key {
                    println!("args key: {:?}", args_key);
                    println!(
                        "kvs get: {:?}",
                        kvs_get(args_key.clone().into(), &mut anna_client)
                    );
                    args_vec.push(kvs_get(args_key.into(), &mut anna_client)?);
                }

                println!("args vec: {:?}", args_vec);

                let delta_table = kvs_get(delta_table_key.into(), &mut anna_client)?;

                let delta_table_path = delta_table
                    .into_lww()
                    .map_err(crate::eyre_to_anyhow)
                    .context("delta_table is not a LWW lattice")?
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
                    .map(|args_from_anna| {
                        bincode::deserialize(&args_from_anna)
                            .expect("Failed to deserialize dataframes")
                    })
                    .collect();

                let mut schema_vec = vec![];

                for (i, dataframe) in dataframes.iter().enumerate() {
                    let (col, _line) = dataframe.shape();
                    if col == 1 {
                        for field in dataframe.fields() {
                            let schema = field.to_arrow();
                            let data_type =
                                // TODO: missing other conversions.
                                match schema.data_type {
                                    ArrowDataType::Float64 => DataType::Float64,
                                    ArrowDataType::Int32 => DataType::Int32,
                                    _ => DataType::Float64,
                                };

                            schema_vec.push(Field::new(
                                format!("col{}{}", schema.name, i),
                                data_type,
                                schema.is_nullable,
                            ));
                        }
                    } else {
                        for field in dataframe.fields() {
                            let schema = field.to_arrow();
                            let data_type =
                                // TODO: missing other conversions.
                                match schema.data_type {
                                    ArrowDataType::Float64 => DataType::Float64,
                                    ArrowDataType::Int32 => DataType::Int32,
                                    _ => DataType::Float64,
                                };

                            schema_vec.push(Field::new(
                                format!("col{}{}", schema.name, i),
                                DataType::List(
                                    Field::new("item", data_type, schema.is_nullable).into(),
                                ),
                                schema.is_nullable,
                            ));
                        }
                    }
                }

                let arrow_schema = ArrowSchema::new(schema_vec.clone());
                let mut array_ref_columns = vec![];
                for dataframe in &dataframes {
                    for column in dataframe.get_columns() {
                        let ctype = column.dtype();

                        if ctype.is_float() {
                            if column.len() == 1 {
                                array_ref_columns.push(convert_between_arrow_formats_float(
                                    &dataframe,
                                    column.name(),
                                ));
                            } else {
                                array_ref_columns.push(convert_between_arrow_formats_list_float(
                                    &dataframe,
                                    column.name(),
                                ));
                            }
                        } else if ctype.is_integer() {
                            if column.len() == 1 {
                                array_ref_columns.push(convert_between_arrow_formats_int(
                                    &dataframe,
                                    column.name(),
                                ));
                            } else {
                                array_ref_columns.push(convert_between_arrow_formats_list_int(
                                    &dataframe,
                                    column.name(),
                                ));
                            }
                        } else {
                            array_ref_columns.push(convert_between_arrow_formats_list_float(
                                &dataframe,
                                column.name(),
                            ));
                        }
                    }
                }

                let batch = RecordBatch::try_new(Arc::new(arrow_schema), array_ref_columns)?;

                let delta_table_path = String::from_utf8(delta_table_path)?;

                let table_dir = Path::new(&delta_table_path);
                println!("table_dir: {:?}", table_dir);
                let table_name = table_dir.file_name().unwrap().to_str().unwrap();
                println!("table_name: {:?}", table_name);
                let comment = "A table";
                let table =
                    create_or_open_delta_table(table_dir, table_name, comment, schema_vec).await?;

                // TODO: missing writer properties config.
                let writer_properties = WriterProperties::builder().set_dictionary_enabled(true);
                let table = DeltaOps(table)
                    .write(vec![batch])
                    .with_writer_properties(writer_properties.build())
                    .await?;

                log::info!("table: {:?}", table);

                let success_msg = "Table stored";
                let key: ClientKey = Uuid::new_v4().to_string().into();
                kvs_put(
                    key.clone(),
                    LastWriterWinsLattice::new_now(success_msg.as_bytes().into()).into(),
                    &mut anna_client,
                )?;

                let key_serialized = bincode::serialize(success_msg)?;

                query
                    .reply(Ok(Sample::new(query.key_expr().clone(), key_serialized)))
                    .res()
                    .await
                    .expect("failed to send sample back");
            }
            Err(e) => {
                log::info!("zenoh error {e:?}");
                break;
            }
        }
    }
    Ok(())
}

macro_rules! convert_arrow_primitive {
    ($name:ident, $arrow_type:ty, $deltalake_type:path) => {
        fn $name(from_polars: &DataFrame, column_name: &str) -> deltalake::arrow::array::ArrayRef {
            let chunked_array = from_polars
                .column(column_name)
                .expect(&format!("{:?} not found in {:?}", column_name, from_polars))
                .chunks();
            let mut data = vec![];
            for chunk in chunked_array {
                let array = chunk.as_any().downcast_ref::<$arrow_type>();
                for a in array.unwrap() {
                    data.push(a.unwrap().clone());
                }
            }

            Arc::new($deltalake_type(data))
        }
    };
}

convert_arrow_primitive!(
    convert_between_arrow_formats_int,
    r_polars::polars::export::arrow::array::Int32Array,
    deltalake::arrow::array::Int32Array::from
);
convert_arrow_primitive!(
    convert_between_arrow_formats_float,
    r_polars::polars::export::arrow::array::Float64Array,
    deltalake::arrow::array::Float64Array::from
);

macro_rules! convert_between_arrow_formats_list {
    ($name:ident, $rust_type:ty, $arrow_type:ty, $arrow_type_array:ty) => {
        fn $name(from_polars: &DataFrame, column_name: &str) -> deltalake::arrow::array::ArrayRef {
            let chunked_array = from_polars
                .column(column_name)
                .expect(&format!("{:?} not found in {:?}", column_name, from_polars))
                .chunks();
            let mut data: Vec<Option<$rust_type>> = vec![];
            for chunk in chunked_array {
                println!("chunk: {:?}", chunk);
                let array = chunk.as_any().downcast_ref::<$arrow_type>();
                println!("array: {:?}", array);
                for a in array.unwrap() {
                    // XXX: sadly I need to unwrap a and then make it optional again because
                    // a = `Option<&f64>`.
                    data.push(Some(a.unwrap().clone()));
                }
            }

            let list_array: GenericListArray<i32> =
                deltalake::arrow::array::ListArray::from_iter_primitive::<$arrow_type_array, _, _>(
                    [Some(data)],
                );
            Arc::new(list_array)
        }
    };
}

convert_between_arrow_formats_list!(
    convert_between_arrow_formats_list_float,
    f64,
    r_polars::polars::export::arrow::array::Float64Array,
    arrow_array::types::Float64Type
);
convert_between_arrow_formats_list!(
    convert_between_arrow_formats_list_int,
    i32,
    r_polars::polars::export::arrow::array::Int32Array,
    arrow_array::types::Int32Type
);

async fn create_or_open_delta_table(
    table_dir: &Path,
    table_name: &str,
    comment: &str,
    schema_vec: Vec<Field>,
) -> Result<DeltaTable, anyhow::Error> {
    let ops = DeltaOps::try_from_uri(table_dir.to_str().expect("Not a valid OS Path")).await?;

    // The operations module uses a builder pattern that allows specifying several options
    // on how the command behaves. The builders implement `Into<Future>`, so once
    // options are set you can run the command using `.await`.
    let table = match ops
        .create()
        .with_columns(get_table_columns(schema_vec))
        .with_table_name(table_name)
        .with_comment(comment)
        .await
    {
        Ok(table) => table,
        Err(_) => {
            let ops =
                DeltaOps::try_from_uri(table_dir.to_str().expect("Not a valid OS Path")).await?;
            let (table, _) = ops.load().await?;
            table
        }
    };

    Ok(table)
}

fn get_table_columns(schema_vec: Vec<Field>) -> Vec<SchemaField> {
    let mut table_columns = vec![];
    for field in schema_vec {
        let datatype =
            // TODO: missing other conversions
            match field.data_type() {
                DataType::Float64 => SchemaDataType::primitive(String::from("double")),
                DataType::Int32 => SchemaDataType::primitive(String::from("integer")),
                DataType::List(f) if f.data_type() == &DataType::Float64 => SchemaDataType::array(SchemaTypeArray::new(
                    Box::new(SchemaDataType::primitive(String::from("double"))),
                    true,
                )),
                DataType::List(f) if f.data_type() == &DataType::Int32 => SchemaDataType::array(SchemaTypeArray::new(
                    Box::new(SchemaDataType::primitive(String::from("integer"))),
                    true,
                )),
                _ => SchemaDataType::primitive(String::from("integer")),
            };
        table_columns.push(SchemaField::new(
            field.name().to_owned(),
            datatype,
            field.is_nullable(),
            Default::default(),
        ));
    }

    table_columns
}

/// Creates a new client connected to the `anna-rs` key-value store.
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

/// Set up the `log` crate.
fn set_up_logger() -> Result<(), fern::InitError> {
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
