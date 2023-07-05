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

use polars_sql::SQLContext;
use r_polars::polars::prelude::*;

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

                let out = diag_concat_lf(&dfs, false, false)?;

                // XXX: has to be `table` name
                ctx.register("demo", out);

                let sql_query = String::from_utf8(sql_query)?;
                let result: r_polars::polars::prelude::DataFrame =
                    ctx.execute(&sql_query).unwrap().collect().unwrap();

                log::debug!("SQL execute result: {:?}", result);

                let serialized_result = bincode::serialize(&result)?;

                query
                    .reply(Ok(Sample::new(query.key_expr().clone(), serialized_result)))
                    .res()
                    .await
                    .expect("failed to send sample back");
                log::info("Sending result back");
            }
            Err(e) => {
                log::info!("zenoh error {e:?}");
                break;
            }
        }
    }

    Ok(())
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
