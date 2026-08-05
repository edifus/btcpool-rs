/// bitcoin/zmq.rs
///
/// Listens on the Bitcoin Knots ZMQ `hashblock` socket.
/// On new block notification, triggers a GBT refresh via the template engine.
/// Falls back to RPC polling when ZMQ is unavailable or misconfigured.
use crate::config::ZmqConfig;
use crate::metrics;
use std::sync::Arc;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

/// Sends a unit signal every time a new block is detected.
pub type NewBlockSender = watch::Sender<u64>;
pub type NewBlockReceiver = watch::Receiver<u64>;

/// Start the ZMQ listener (or polling fallback).
/// Returns a `watch::Receiver` that fires whenever the chain tip advances.
pub async fn start(cfg: &ZmqConfig, rpc: Arc<crate::bitcoin::rpc::RpcClient>) -> NewBlockReceiver {
    let (tx, rx) = watch::channel(0u64);
    let endpoint = cfg.hashblock_endpoint.clone();
    let poll_fallback = cfg.poll_fallback;
    let poll_interval_ms = cfg.poll_interval_ms;

    tokio::spawn(async move {
        // Try ZMQ first
        match run_zmq_listener(&endpoint, tx.clone()).await {
            Ok(_) => {}
            Err(e) => {
                warn!("ZMQ listener failed ({e}), switching to RPC poll fallback");
                if poll_fallback {
                    metrics::rpc_fallback_used();
                    run_poll_fallback(rpc, poll_interval_ms, tx).await;
                }
            }
        }
    });

    rx
}

async fn run_zmq_listener(endpoint: &str, tx: NewBlockSender) -> anyhow::Result<()> {
    let ctx = tmq::Context::new();
    let mut sub = tmq::subscribe(&ctx)
        .connect(endpoint)?
        .subscribe(b"hashblock")?;

    info!("ZMQ listener connected to {endpoint}");
    let mut seq: u64 = 0;

    loop {
        match sub.next().await {
            Some(Ok(_multipart)) => {
                seq += 1;
                debug!("ZMQ: hashblock notification #{seq}");
                let _ = tx.send(seq);
            }
            Some(Err(e)) => {
                return Err(anyhow::anyhow!("ZMQ receive error: {e}"));
            }
            None => {
                return Err(anyhow::anyhow!("ZMQ stream closed"));
            }
        }
    }
}

async fn run_poll_fallback(
    rpc: Arc<crate::bitcoin::rpc::RpcClient>,
    poll_interval_ms: u64,
    tx: NewBlockSender,
) {
    info!(
        "Starting RPC poll fallback ({}ms interval)",
        poll_interval_ms
    );
    let mut last_hash = String::new();
    let mut seq: u64 = 0;
    let interval = tokio::time::Duration::from_millis(poll_interval_ms);

    loop {
        match rpc.best_block_hash() {
            Ok(hash) => {
                if hash != last_hash {
                    debug!("Poll: new block hash {hash}");
                    last_hash = hash;
                    seq += 1;
                    let _ = tx.send(seq);
                }
            }
            Err(e) => warn!("Poll RPC error: {e}"),
        }
        tokio::time::sleep(interval).await;
    }
}
