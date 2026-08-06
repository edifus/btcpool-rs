/// btcpool-rs — Solo BTC mining pool (Stratum V1 + V2, auto-detected on one port)
///
/// Startup sequence:
///   1. Load config.toml
///   2. Initialise tracing (structured or plain)
///   3. Start Prometheus metrics endpoint
///   4. Connect to Bitcoin Knots RPC (cookie auth)
///   5. Start ZMQ block-notification listener (or RPC poll fallback)
///   6. Bootstrap the template engine and build first job
///   7. Start the TCP accept loop
mod bitcoin;
mod config;
mod error;
mod metrics;
mod mining;
mod network;
mod protocol;
mod security;
mod settings;
mod stats;

use crate::{
    bitcoin::{rpc::RpcClient, zmq},
    mining::engine::TemplateEngine,
    security::BanList,
    stats::PoolStats,
};
use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // ── Config ────────────────────────────────────────────────────────────────
    let mut args = std::env::args().skip(1);
    let cfg_path = match args.next() {
        Some(a) if a == "--config" => args.next().unwrap_or_else(|| "config.toml".to_string()),
        Some(a) => a,
        None => "config.toml".to_string(),
    };

    let config = Arc::new(
        config::load(&cfg_path).with_context(|| format!("Loading config from '{cfg_path}'"))?,
    );

    // ── Logging ───────────────────────────────────────────────────────────────
    init_tracing(&config.logging);

    info!(
        version = env!("CARGO_PKG_VERSION"),
        listen  = %config.pool.listen_addr,
        "btcpool-rs starting"
    );

    // ── Metrics ───────────────────────────────────────────────────────────────
    let prometheus_handle = metrics::init(&config.metrics.prometheus_addr);

    // ── Pool stats (HTTP dashboard snapshot + in-memory state)
    // Supports optional SQLite persistence for all-time best values.
    let stats = PoolStats::new_with_store(config.metrics.stats_db_path.clone());

    // ── Bitcoin RPC ───────────────────────────────────────────────────────────
    let rpc =
        Arc::new(RpcClient::new(&config.bitcoin_rpc).context("Connecting to Bitcoin Knots RPC")?);

    // ── Runtime network (detected from the node) ─────────────────────────────
    // Miner payout addresses are validated against this chain at authorization.
    let node_chain = rpc
        .chain()
        .await
        .context("Querying node chain (getblockchaininfo)")?;
    info!(chain = %node_chain, "Connected node chain detected");
    let runtime_settings = settings::RuntimeSettings::new(&config.pool, &node_chain)?;

    // ── Hashrate decay ticker ────────────────────────────────────────────────
    // Folds each session's accumulated shares into its decaying averages on a
    // fixed cadence, which is also what makes an idle miner fade out instead of
    // freezing at its last reading. Mirrors ckpool's statsupdate thread.
    {
        let stats = stats.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(
                mining::hashrate::TICK_SECS,
            ));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                stats.tick_hashrates();
            }
        });
    }

    // ── Hashrate history recorder ────────────────────────────────────────────
    // Ticks before the first write so a restart doesn't stamp a zero sample at
    // the left edge of every chart.
    {
        let stats = stats.clone();
        tokio::spawn(async move {
            let period = tokio::time::Duration::from_secs(stats::SNAPSHOT_INTERVAL_SECS);
            // `interval` yields its first tick immediately; start one period out
            // so the first sample is a real measurement rather than a zero.
            let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                stats.record_hashrate_snapshot();
            }
        });
    }

    // ── Network hash rate poll ───────────────────────────────────────────────
    {
        let stats = stats.clone();
        let rpc = rpc.clone();
        tokio::spawn(async move {
            let interval = tokio::time::Duration::from_secs(30);
            loop {
                match rpc.network_hashrate(None, None).await {
                    Ok(network_hps) => stats.set_network_hashrate(network_hps),
                    Err(e) => tracing::warn!("Failed to poll network hash rate: {e}"),
                }
                match rpc.estimate_difficulty_change_pct().await {
                    Ok(pct) => stats.set_est_difficulty_change_pct(pct),
                    Err(e) => tracing::warn!("Failed to estimate difficulty change: {e}"),
                }
                tokio::time::sleep(interval).await;
            }
        });
    }

    // ── ZMQ / poll ────────────────────────────────────────────────────────────
    let new_block_rx = zmq::start(&config.zmq, rpc.clone()).await;

    // ── Template engine ───────────────────────────────────────────────────────
    let engine = TemplateEngine::new(rpc.clone(), config.pool.clone(), stats.clone());

    // Spawn the template refresh loop
    {
        let engine = engine.clone();
        tokio::spawn(engine.run(new_block_rx));
    }

    // ── SV2 Noise authority (before the dashboard, which shows the pubkey) ────
    let sv2_authority_pubkey = if config.sv2.enabled {
        let pubkey = protocol::sv2::init_noise_authority(&config.sv2)
            .context("Initialising SV2 Noise authority key")?;
        if config.sv2.persist_authority_key {
            info!("SV2 authority public key: {pubkey} (pin this on the miner to verify pool identity)");
        } else {
            info!("SV2 authority public key: {pubkey} (ephemeral: persist_authority_key = false, changes every restart)");
        }
        Some(pubkey)
    } else {
        None
    };

    // ── Dashboard ─────────────────────────────────────────────────────────────
    network::dashboard::start(
        &config.metrics.prometheus_addr,
        stats.clone(),
        engine.clone(),
        prometheus_handle,
        runtime_settings.clone(),
        &config.pool.listen_addr,
        config.sv2.enabled,
        sv2_authority_pubkey,
    )
    .await;

    // ── Security ──────────────────────────────────────────────────────────────
    let ban_list = BanList::new(config.security.ban_duration_secs);

    // ── TCP server ────────────────────────────────────────────────────────────
    network::server::run(
        config,
        engine,
        ban_list,
        stats,
        runtime_settings.bitcoin_network(),
    )
    .await?;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Tracing initialisation
// ─────────────────────────────────────────────────────────────────────────────

fn init_tracing(cfg: &config::LoggingConfig) {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_new(&cfg.level).unwrap_or_else(|_| EnvFilter::new("info"));

    if let Some(log_dir) = &cfg.log_dir {
        use std::path::PathBuf;
        use tracing_appender::rolling::{RollingFileAppender, Rotation};

        // Expand ~ if present
        let log_dir_path = if let Some(stripped) = log_dir.strip_prefix("~/") {
            if let Some(home) = std::env::var_os("HOME") {
                PathBuf::from(home).join(stripped)
            } else {
                PathBuf::from(log_dir)
            }
        } else {
            PathBuf::from(log_dir)
        };

        // Create directory if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(&log_dir_path) {
            eprintln!(
                "Failed to create log directory {}: {}",
                log_dir_path.display(),
                e
            );
            std::process::exit(1);
        }

        let file_appender =
            RollingFileAppender::new(Rotation::DAILY, log_dir_path, "btcpool-rs.log");

        if cfg.json {
            fmt()
                .json()
                .with_env_filter(filter)
                .with_current_span(true)
                .with_writer(file_appender)
                .with_ansi(false)
                .init();
        } else {
            fmt()
                .with_env_filter(filter)
                .with_target(true)
                .with_writer(file_appender)
                .with_ansi(false)
                .init();
        }
    } else if cfg.json {
        fmt()
            .json()
            .with_env_filter(filter)
            .with_current_span(true)
            .with_ansi(false)
            .init();
    } else {
        fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_ansi(false)
            .init();
    }
}
