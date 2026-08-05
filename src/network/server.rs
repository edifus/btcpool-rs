/// network/server.rs
///
/// TCP accept loop.
///
/// Responsibilities:
///  - Bind the listener socket
///  - Enforce per-IP connection rate limits before spawning a session
///  - Check the ban list on accept
///  - Track total active connections (bounded by max_connections)
///  - Spawn one tokio task per miner connection
use crate::{
    config::Config,
    mining::engine::TemplateEngine,
    security::{BanList, ConnectionRateLimiter},
    stats::PoolStats,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Deadline for a new connection to make protocol progress before it has
/// authorized a worker: the protocol auto-detect peek here, the SV2 Noise
/// handshake, and each pre-auth message in both session loops. Separate from
/// (and much shorter than) the per-session idle timeout, so silent connections
/// cannot pin the bounded global connection slots.
pub(crate) const HANDSHAKE_TIMEOUT_SECS: u64 = 10;

/// TCP keepalive tuning for accepted miner connections. A powered-off miner
/// sends no FIN, so without probes a dead-but-quiet connection lives until the
/// kernel exhausts its retransmission budget on the next write (15-20 minutes
/// with Linux defaults). Probe after 60s of silence, every 10s, give up after
/// 3 failed probes: a dead peer is detected within ~90 seconds.
const TCP_KEEPALIVE_IDLE_SECS: u64 = 60;
const TCP_KEEPALIVE_INTERVAL_SECS: u64 = 10;
const TCP_KEEPALIVE_RETRIES: u32 = 3;

/// Upper bound on how long written data (job broadcasts) may sit unacked
/// before the kernel declares the connection dead. Covers the write-to-dead-
/// peer path the same way keepalive covers the idle path. On Linux this also
/// overrides keepalive's retry accounting, so keep the two in the same
/// ballpark.
const TCP_USER_TIMEOUT_SECS: u64 = 90;

pub async fn run(
    config: Arc<Config>,
    engine: Arc<TemplateEngine>,
    ban_list: Arc<BanList>,
    stats: Arc<PoolStats>,
    network: bitcoin::Network,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&config.pool.listen_addr).await?;
    if config.sv2.enabled {
        info!(
            "Stratum listener on {} (SV1 + SV2 auto-detected)",
            config.pool.listen_addr
        );
    } else {
        info!("Stratum V1 listener on {}", config.pool.listen_addr);
    }

    let conn_limiter = ConnectionRateLimiter::new(config.security.max_connections_per_ip);
    let active_count = Arc::new(AtomicUsize::new(0));
    let max_connections = config.pool.max_connections;

    // Background ban-list + rate-limiter + idle-worker pruner
    {
        let bl = ban_list.clone();
        let rl = conn_limiter.clone();
        let st = stats.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300));
            loop {
                interval.tick().await;
                bl.prune();
                rl.prune();
                st.prune_idle_workers();
            }
        });
    }

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                // accept() returns transient, per-connection errors (peer RST
                // mid-handshake → ECONNABORTED) and resource errors under fd
                // exhaustion (EMFILE/ENFILE); none of them may kill the
                // listener. Back off briefly so the loop doesn't spin while
                // the process is out of descriptors.
                warn!("accept() failed: {e}");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }
        };

        // ── Global connection cap ────────────────────────────────────────────
        let reserved = loop {
            let current = active_count.load(Ordering::Relaxed);
            if current >= max_connections {
                warn!("Connection limit reached ({max_connections}), dropping {peer}");
                break false;
            }
            match active_count.compare_exchange(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break true,
                Err(_) => continue,
            }
        };
        if !reserved {
            continue;
        }

        // ── IP ban check ─────────────────────────────────────────────────────
        if ban_list.is_banned(&peer.ip()) {
            warn!("Rejected banned IP: {peer}");
            active_count.fetch_sub(1, Ordering::Relaxed);
            continue;
        }

        // ── Per-IP rate limit ────────────────────────────────────────────────
        if !conn_limiter.check_and_record(peer.ip()) {
            warn!("Connection rate limit exceeded for {}", peer.ip());
            ban_list.ban(peer.ip(), "connection rate limit exceeded");
            active_count.fetch_sub(1, Ordering::Relaxed);
            continue;
        }

        // ── TCP tuning ───────────────────────────────────────────────────────
        if let Err(e) = stream.set_nodelay(true) {
            warn!("TCP_NODELAY failed for {peer}: {e}");
        }
        let sock = socket2::SockRef::from(&stream);
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(std::time::Duration::from_secs(TCP_KEEPALIVE_IDLE_SECS))
            .with_interval(std::time::Duration::from_secs(TCP_KEEPALIVE_INTERVAL_SECS))
            .with_retries(TCP_KEEPALIVE_RETRIES);
        if let Err(e) = sock.set_tcp_keepalive(&keepalive) {
            warn!("TCP keepalive setup failed for {peer}: {e}");
        }
        #[cfg(target_os = "linux")]
        if let Err(e) =
            sock.set_tcp_user_timeout(Some(std::time::Duration::from_secs(TCP_USER_TIMEOUT_SECS)))
        {
            warn!("TCP_USER_TIMEOUT failed for {peer}: {e}");
        }

        // ── Spawn session task ───────────────────────────────────────────────
        let config = config.clone();
        let engine = engine.clone();
        let ban_list = ban_list.clone();
        let stats = stats.clone();
        let active_count = active_count.clone();

        tokio::spawn(async move {
            // Auto-detect the protocol from the first byte without consuming it:
            // SV1 is JSON ('{'); SV2 frames start with a binary extension_type
            // (0x00 for the standard mining protocol's SetupConnection).
            // Force the protocol-detect byte to arrive promptly so a peer cannot
            // hold the connection (and its slot) open by sending nothing. This
            // deadline is deliberately much shorter than idle_timeout_secs:
            // every real miner sends its first message immediately on connect,
            // and a silent connection pinning a global slot for the full idle
            // timeout (default 300 s) lets a handful of IPs exhaust all slots.
            let handshake_timeout = tokio::time::Duration::from_secs(HANDSHAKE_TIMEOUT_SECS);
            let mut first = [0u8; 1];
            let peek = tokio::time::timeout(handshake_timeout, stream.peek(&mut first)).await;
            let is_sv1 = match peek {
                Err(_) => {
                    warn!("Protocol-detect timeout for {peer}");
                    active_count.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
                Ok(Ok(0)) => {
                    // Connection closed before sending anything.
                    active_count.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
                Ok(Ok(_)) => first[0] == b'{',
                Ok(Err(e)) => {
                    warn!("Peek failed for {peer}: {e}");
                    active_count.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
            };

            if is_sv1 {
                crate::network::session::run(
                    stream, peer, config, engine, ban_list, stats, network,
                )
                .await;
            } else if config.sv2.enabled {
                crate::protocol::sv2::run(stream, peer, config, engine, ban_list, stats, network)
                    .await;
            } else {
                warn!("SV2 connection from {peer} rejected (sv2.enabled = false)");
            }
            active_count.fetch_sub(1, Ordering::Relaxed);
        });
    }
}
