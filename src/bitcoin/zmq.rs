/// bitcoin/zmq.rs
///
/// Listens on the Bitcoin Knots ZMQ `hashblock` socket.
/// On new block notification, triggers a GBT refresh via the template engine.
///
/// The listener is supervised forever with capped exponential backoff, so the
/// `watch::Sender` it shares with `run_poll_fallback` is never dropped — see
/// `start`. When `poll_fallback` is enabled, RPC polling runs as a permanent
/// concurrent backstop alongside ZMQ rather than only after ZMQ fails: a
/// socket that connects but never publishes (wrong port, node still in IBD)
/// never surfaces as an error, so the supervisor has nothing to react to.
use crate::config::ZmqConfig;
use crate::metrics;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

/// Sends a unit signal every time a new block is detected.
pub type NewBlockSender = watch::Sender<u64>;
pub type NewBlockReceiver = watch::Receiver<u64>;

// ─────────────────────────────────────────────────────────────────────────────
// Backoff constants
// ─────────────────────────────────────────────────────────────────────────────

/// First retry delay after the ZMQ listener dies. Short enough that a blip
/// (bitcoind restarting inside the same second) is invisible to the pool — the
/// poll backstop, when enabled, is covering the gap in the meantime regardless.
const ZMQ_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Retry ceiling. High enough that a genuinely down node doesn't turn into a
/// reconnect storm in the logs; low enough that a node coming back up is
/// noticed within a minute even in the worst case.
const ZMQ_MAX_BACKOFF: Duration = Duration::from_secs(60);

/// How long a connection must stay up before a later failure is treated as a
/// fresh problem (backoff resets to `ZMQ_INITIAL_BACKOFF`) rather than a
/// continuation of the same flapping episode (backoff keeps doubling).
/// Deliberately *not* reset on connect: an endpoint that accepts and
/// immediately drops would otherwise pin the retry rate at 1s forever. Set to
/// the same value as `ZMQ_MAX_BACKOFF` so a connection is credited as stable
/// only once it has outlasted the worst-case retry gap.
const ZMQ_STABLE_AFTER: Duration = Duration::from_secs(60);

/// Start the ZMQ listener (supervised, reconnecting forever) and, if
/// configured, a concurrent RPC poll backstop. Returns a `watch::Receiver`
/// that fires whenever the chain tip advances.
pub async fn start(cfg: &ZmqConfig, rpc: Arc<crate::bitcoin::rpc::RpcClient>) -> NewBlockReceiver {
    let (tx, rx) = watch::channel(0u64);
    let endpoint = cfg.hashblock_endpoint.clone();

    tokio::spawn(supervise_zmq_listener(endpoint, tx.clone()));

    if cfg.poll_fallback {
        // Permanent backstop, not a post-failure stopgap: this is what covers a
        // ZMQ socket that connects fine but never publishes (defect described
        // in the module doc above), which by construction never produces an
        // error for the supervisor above to react to.
        metrics::rpc_fallback_used();
        tokio::spawn(run_poll_fallback(rpc, cfg.poll_interval_ms, tx));
    }

    rx
}

/// Keep `run_zmq_listener` running forever. `tx` is cloned into every attempt
/// so the shared `watch::Sender` state stays alive for the lifetime of this
/// task — which never returns — regardless of how many times the underlying
/// socket dies and is recreated.
async fn supervise_zmq_listener(endpoint: String, tx: NewBlockSender) {
    let mut backoff = ZMQ_INITIAL_BACKOFF;

    loop {
        let attempt_started = Instant::now();
        match run_zmq_listener(&endpoint, tx.clone()).await {
            // Not reachable today (`run_zmq_listener`'s loop only exits via
            // `Err`), but matched explicitly rather than assumed so a future
            // clean-exit path is still supervised correctly.
            Ok(()) => warn!("ZMQ listener exited; reconnecting in {backoff:?}"),
            Err(e) => warn!("ZMQ listener failed ({e}); reconnecting in {backoff:?}"),
        }
        metrics::zmq_reconnect();

        // Sampled before the sleep below, not after: the retry delay is not
        // uptime. Measuring across it would let a permanently dead endpoint
        // clear ZMQ_STABLE_AFTER on the strength of its own backoff — once
        // backoff reaches ZMQ_MAX_BACKOFF the sleep alone satisfies the
        // threshold — and reset, cycling 1s→60s→1s forever instead of settling
        // at the ceiling.
        let connection_lasted = attempt_started.elapsed();

        tokio::time::sleep(backoff).await;

        backoff = if connection_lasted >= ZMQ_STABLE_AFTER {
            ZMQ_INITIAL_BACKOFF
        } else {
            next_backoff(backoff)
        };
    }
}

/// Double the backoff, capped at `ZMQ_MAX_BACKOFF`. Pure so the doubling/cap
/// logic is unit-testable without a clock.
fn next_backoff(current: Duration) -> Duration {
    (current * 2).min(ZMQ_MAX_BACKOFF)
}

async fn run_zmq_listener(endpoint: &str, tx: NewBlockSender) -> anyhow::Result<()> {
    let ctx = tmq::Context::new();
    let mut sub = tmq::subscribe(&ctx)
        .connect(endpoint)?
        .subscribe(b"hashblock")?;

    info!("ZMQ listener connected to {endpoint}");

    loop {
        match sub.next().await {
            Some(Ok(_multipart)) => {
                // `watch` notifies on this version counter advancing, not on
                // value equality, so wrapping is harmless — but the counter is
                // also shared with `run_poll_fallback`'s sends, and keeping it
                // strictly monotonic here keeps that contract honest.
                let mut seq = 0;
                tx.send_modify(|n| {
                    *n += 1;
                    seq = *n;
                });
                debug!(seq, "ZMQ: hashblock notification");
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
    let interval = Duration::from_millis(poll_interval_ms);

    loop {
        match rpc.best_block_hash().await {
            Ok(hash) => {
                if hash != last_hash {
                    debug!("Poll: new block hash {hash}");
                    last_hash = hash;
                    tx.send_modify(|n| *n += 1);
                }
            }
            Err(e) => warn!("Poll RPC error: {e}"),
        }
        tokio::time::sleep(interval).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_backoff_doubles() {
        assert_eq!(next_backoff(Duration::from_secs(1)), Duration::from_secs(2));
        assert_eq!(next_backoff(Duration::from_secs(4)), Duration::from_secs(8));
    }

    #[test]
    fn next_backoff_caps_at_max() {
        assert_eq!(next_backoff(ZMQ_MAX_BACKOFF), ZMQ_MAX_BACKOFF);
        // Doubling from just under the cap must clamp, not merely land close.
        assert_eq!(next_backoff(Duration::from_secs(59)), ZMQ_MAX_BACKOFF);
        assert_eq!(next_backoff(Duration::from_secs(45)), ZMQ_MAX_BACKOFF);
    }
}
