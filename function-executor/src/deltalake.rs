use anna::{
    anna_default_zenoh_prefix,
    nodes::{request_cluster_info, ClientNode},
    store::LatticeValue,
    topics::RoutingThread,
    ClientKey,
};
use anyhow::Context;
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
    DeltaOps, DeltaTable, SchemaDataType, SchemaField,
};
use parquet::file::properties::WriterProperties;
use polars_sql::SQLContext;
use r_polars::polars::prelude::*;
use std::path::Path;

use sqlparser::{dialect::GenericDialect, ast::TableWithJoins};
use sqlparser::parser::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    let _ = polars_loop(zenoh, &zenoh_prefix).await;

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

    let mut anna_client = new_anna_client(zenoh).await.unwrap();

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

                let mut ctx = SQLContext::new();
                let table = deltalake::open_table(delta_table_path.clone())
                    .await
                    .unwrap();

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
                let table_name =
                    match select.from.get(0) {
                        Some(TableWithJoins { relation, joins: _j }) => relation.to_string(),
                        _ => {
                            log::info!("Could not a Table name");
                            "demo".to_string()
                        },
                    };

                // TODO: has to be `table` name, comming from query.
                ctx.register(&table_name, lazy_frame);

                let result: r_polars::polars::prelude::DataFrame =
                    ctx.execute(&sql_query).unwrap().collect().unwrap();

                log::debug!("SQL execute result: {:?}", result);

                let serialized_result = bincode::serialize(&result)?;

                query
                    .reply(Ok(Sample::new(query.key_expr().clone(), serialized_result)))
                    .res()
                    .await
                    .expect("failed to send sample back");

                log::info!("Sending result back");
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

    let mut anna_client = new_anna_client(zenoh).await.unwrap();

    loop {
        match reply.recv_async().await {
            Ok(query) => {
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

                let mut args_vec = vec![];
                for args_key in args_vec_key {
                    args_vec.push(kvs_get(args_key.into(), &mut anna_client)?);
                }

                let delta_table = kvs_get(delta_table_key.into(), &mut anna_client)?;

                let delta_table_path = delta_table
                    .into_lww()
                    .map_err(crate::eyre_to_anyhow)
                    .context("delta_table is not a LWW lattice")?
                    .into_revealed()
                    .into_value();

                let args_from_anna_vec: Vec<Vec<u8>> = args_vec
                    .iter()
                    .map(|arg| {
                        arg.clone().into_lww()
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

                let mut schema_vec = vec![];

                for (i, dataframe) in dataframes.iter().enumerate() {
                    for schema in dataframe.fields() {
                        let schema = schema.to_arrow();
                        let data_type =
                            // TODO: missing other conversions.
                            match schema.data_type {
                                ArrowDataType::Float64 => DataType::Float64,
                                ArrowDataType::Int32 => DataType::Int32,
                                _ => DataType::Float64,
                            };

                        schema_vec.push(Field::new(format!("{}-{}", schema.name, i), data_type, schema.is_nullable));
                    }
                }

                let arrow_schema = ArrowSchema::new(schema_vec.clone());
                let mut array_ref_columns = vec![];
                for dataframe in &dataframes {
                    for column_name in dataframe.get_column_names() {
                        array_ref_columns.push(convert_between_arrow_formats(&dataframe, column_name));
                    }
                }

                let batch = RecordBatch::try_new(Arc::new(arrow_schema), array_ref_columns).unwrap();

                let delta_table_path = String::from_utf8(delta_table_path)?;

                let table_dir = Path::new(&delta_table_path);
                log::info!("table_dir: {:?}", table_dir);
                let table_name = table_dir.file_name().unwrap().to_str().unwrap();
                log::info!("table_name: {:?}", table_name);
                let comment = "A table with median resistence shm";
                let table = create_or_open_delta_table(table_dir, table_name, comment, schema_vec)
                    .await
                    .unwrap();
                // TODO: missing writer properties config.
                let writer_properties = WriterProperties::builder().set_dictionary_enabled(true);
                let table = DeltaOps(table)
                    .write(vec![batch])
                    .with_writer_properties(writer_properties.build())
                    .await
                    .unwrap();

                log::info!("table: {:?}", table);

                let serialized_result = bincode::serialize("oi").unwrap();

                query
                    .reply(Ok(Sample::new(query.key_expr().clone(), serialized_result)))
                    .res()
                    .await
                    .expect("failed to send sample back");
                log::info!("Sending result back");
            }
            Err(e) => {
                log::info!("zenoh error {e:?}");
                break;
            }
        }
    }
    Ok(())
}

fn convert_between_arrow_formats(
    from_polars: &DataFrame,
    column_name: &str,
) -> deltalake::arrow::array::ArrayRef {
    let chunked_array = from_polars
        .column(column_name)
        .expect(&format!("{:?} not found in {:?}", column_name, from_polars))
        .chunks();
    //println!("{:?}", chunked_array);
    let mut data = vec![];
    for chunk in chunked_array {
        let array = chunk
            .as_any()
            .downcast_ref::<r_polars::polars::export::arrow::array::Float64Array>();
        for a in array.unwrap() {
            data.push(a.unwrap().clone());
        }
    }

    Arc::new(deltalake::arrow::array::Float64Array::from(data))
}

async fn create_or_open_delta_table(
    table_dir: &Path,
    table_name: &str,
    comment: &str,
    schema_vec: Vec<Field>,
) -> Result<DeltaTable, Box<dyn std::error::Error>> {
    let ops = DeltaOps::try_from_uri(table_dir.to_str().expect("Not a valid OS Path"))
        .await
        .unwrap();

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
            let ops = DeltaOps::try_from_uri(table_dir.to_str().expect("Not a valid OS Path"))
                .await
                .unwrap();
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
                _ => SchemaDataType::primitive(String::from("double")),
            };
        table_columns.push(
            SchemaField::new(
                field.name().to_owned(),
                datatype,
                field.is_nullable(),
                Default::default(),
            )
        );
    }

    table_columns
}

// fn get_table_columns() -> Vec<SchemaField> {
//     vec![
//         SchemaField::new(
//             String::from("id"),
//             SchemaDataType::primitive(String::from("float")),
//             false,
//             Default::default(),
//         ),
//         // SchemaField::new(
//         //      String::from("frequencia"),
//         //      SchemaDataType::array(SchemaTypeArray::new(
//         //          Box::new(SchemaDataType::primitive(String::from("double"))),
//         //          true,
//         //      )),
//         //      true,
//         //      Default::default(),
//         //  ),
//         //  SchemaField::new(
//         //      String::from("sensor_id"),
//         //      SchemaDataType::primitive(String::from("long")),
//         //      false,
//         //      Default::default(),
//         //  ),
//         //  SchemaField::new(
//         //      String::from("ciclo"),
//         //      SchemaDataType::primitive(String::from("long")),
//         //      false,
//         //      Default::default(),
//         //  ),
//         //  SchemaField::new(
//         //      String::from("execucao_ciclo_id"),
//         //      SchemaDataType::primitive(String::from("long")),
//         //      false,
//         //      Default::default(),
//         //  ),
//         //  SchemaField::new(
//         //      String::from("repeticao"),
//         //      SchemaDataType::primitive(String::from("long")),
//         //      false,
//         //      Default::default(),
//         //  ),
//         //  SchemaField::new(
//         //      String::from("data_hora"),
//         //      SchemaDataType::primitive(String::from("timestamp")),
//         //      false,
//         //      Default::default(),
//         //  ),
//     ]
// }

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
