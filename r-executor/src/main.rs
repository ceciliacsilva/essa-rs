#![warn(missing_docs)]
use anna::{
    anna_default_zenoh_prefix,
    lattice::Lattice,
    nodes::{request_cluster_info, ClientNode},
    store::LatticeValue,
    topics::RoutingThread,
    ClientKey,
};
use anyhow::Context;
use essa_common::{essa_default_zenoh_prefix, executor_run_r_subscribe_topic};
use extendr_api::deserializer::from_robj;
use extendr_api::prelude::*;
use extendr_api::Robj;
use r_polars::conversion_r_to_s::par_read_robjs;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;
use zenoh::prelude::r#async::*;

use std::io::Write;

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
                let args = topic_split
                    .next_back()
                    .context("no args key in topic")?
                    .to_owned();
                let func = topic_split
                    .next_back()
                    .context("no func in topic")?
                    .to_owned();

                let func = kvs_get(func.into(), &mut anna_client)?;
                let args = kvs_get(args.into(), &mut anna_client)?;
                let mut final_result: Vec<r_polars::polars::prelude::Series> = Vec::new();

                {
                    let func = func
                        .into_lww()
                        .map_err(eyre_to_anyhow)
                        .context("func is not a LWW lattice")?
                        .into_revealed()
                        .into_value();
                    let args_from_anna = args
                        .into_lww()
                        .map_err(eyre_to_anyhow)
                        .context("args is not a LWW lattice")?
                        .into_revealed()
                        .into_value();

                    println!("aqui");

                    let result: r_polars::polars::prelude::DataFrame =
                        bincode::deserialize(&args_from_anna)?;

                    let rdf: r_polars::rdataframe::DataFrame = result.into();
                    println!("{:?}", rdf);

                    let rlist: r_polars::Robj = rdf.to_list_result().unwrap().into();
                    println!("{:?}", rlist);

                    let function = String::from_utf8(func)?;
                    let func = eval_string(&function).unwrap();
                    let result = func.call(Pairlist::from_pairs([("x", rlist)])).unwrap();

                    println!("result: {:?}, type: {:?}", result, result.rtype());
                    final_result = par_read_robjs(vec![(
                        r_polars::utils::extendr_concurrent::ParRObj(result),
                        "result".to_string(),
                    )])?;
                }

                let serialize = bincode::serialize(&final_result)?;
                query
                    .reply(Ok(Sample::new(query.key_expr().clone(), serialize)))
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

/// Get the given value in the key-value store.
///
/// Returns an error if the requested key does not exist in the KVS.
fn kvs_get(key: ClientKey, anna: &mut ClientNode) -> anyhow::Result<LatticeValue> {
    smol::block_on(anna.get(key))
        .map_err(eyre_to_anyhow)
        .context("get failed")
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
