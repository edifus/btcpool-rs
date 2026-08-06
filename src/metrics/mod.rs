/// metrics/mod.rs
///
/// Prometheus-compatible metrics.
///
/// `init()` installs the global recorder and returns a handle that the
/// dashboard uses to render the /metrics endpoint.  The HTTP listener is
/// managed by network::dashboard, not here.
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use tracing::{info, warn};

/// Drop metric series untouched for this long. Worker-labeled series are minted
/// from untrusted names, so without an idle timeout each distinct name leaks a
/// permanent label set in the exporter. 24 h matches the stats-map eviction.
/// Note `pool_worker_hashrate_hps` is additionally multiplied by the number of
/// averaging windows; the pool-wide `pool_hashrate_hps` carries no worker label
/// and so stays at a fixed series count.
const METRIC_IDLE_TIMEOUT_SECS: u64 = 86_400;

pub fn init(addr: &str) -> Option<PrometheusHandle> {
    if addr.is_empty() {
        info!("Prometheus metrics disabled (empty prometheus_addr)");
        return None;
    }
    match PrometheusBuilder::new()
        .idle_timeout(
            MetricKindMask::ALL,
            Some(std::time::Duration::from_secs(METRIC_IDLE_TIMEOUT_SECS)),
        )
        .install_recorder()
    {
        Ok(handle) => Some(handle),
        Err(e) => {
            warn!("Failed to install Prometheus recorder: {e}");
            None
        }
    }
}

// ── Counters & gauges ─────────────────────────────────────────────────────────

pub fn miner_connected() {
    gauge!("pool_connected_miners").increment(1.0);
}

pub fn miner_disconnected() {
    gauge!("pool_connected_miners").decrement(1.0);
}

pub fn share_accepted(difficulty: u64, worker: &str) {
    counter!("pool_shares_accepted_total", "worker" => worker.to_string()).increment(1);
    histogram!("pool_share_difficulty", "worker" => worker.to_string()).record(difficulty as f64);
}

pub fn share_rejected(reason: &str, worker: &str) {
    counter!(
        "pool_shares_rejected_total",
        "reason" => reason.to_string(),
        "worker" => worker.to_string()
    )
    .increment(1);
    // Also track per-worker rejected shares for efficiency monitoring
    counter!(
        "pool_worker_shares_rejected_total",
        "worker" => worker.to_string()
    )
    .increment(1);
}

pub fn share_validation_time(duration_ms: f64) {
    histogram!("pool_share_validation_duration_ms").record(duration_ms);
}

pub fn connection_duration(worker: &str, duration_secs: f64) {
    histogram!("pool_connection_duration_secs", "worker" => worker.to_string())
        .record(duration_secs);
}

pub fn miner_disconnect(reason: &str, worker: &str) {
    counter!(
        "pool_miner_disconnects_total",
        "reason" => reason.to_string(),
        "worker" => worker.to_string()
    )
    .increment(1);
}

/// The node's verdict on a block we submitted, one series per outcome.
///
/// `outcome` is a closed set (`BlockSubmitOutcome::label`) — same contract as
/// the `reason` label below, and for the same reason.
///
/// This is the breakdown; `block_found` below is the headline. Only outcomes
/// that won their height increment both.
pub fn block_submission_outcome(outcome: &'static str) {
    counter!(
        "pool_block_submissions_total",
        "outcome" => outcome
    )
    .increment(1);
}

pub fn block_submission_failure(reason: &'static str) {
    counter!(
        "pool_block_submissions_failed_total",
        "reason" => reason
    )
    .increment(1);
}

pub fn job_broadcast(miners_count: usize) {
    gauge!("pool_job_broadcast_miners").set(miners_count as f64);
    counter!("pool_job_broadcasts_total").increment(1);
}

pub fn zmq_reconnect() {
    counter!("pool_zmq_reconnects_total").increment(1);
}

pub fn rpc_fallback_used() {
    counter!("pool_rpc_fallback_used_total").increment(1);
}

pub fn vardiff_retarget(worker: &str, old_diff: u64, new_diff: u64) {
    gauge!("pool_worker_difficulty", "worker" => worker.to_string()).set(new_diff as f64);
    histogram!("pool_vardiff_change_ratio").record(new_diff as f64 / old_diff as f64);
}

/// A block that won its height. Deliberately unlabelled: this is the number an
/// operator puts on a wall, and reading it should not need a label matcher.
///
/// Call this only when `BlockSubmitOutcome::is_win` — a valid block that lost a
/// same-height race is not a find.
pub fn block_found() {
    counter!("pool_blocks_found_total").increment(1);
}

/// A block that won its height and was later reorged off the chain — the
/// correction to `pool_blocks_found_total`, which cannot itself go down.
///
/// Unlabelled for the same reason as `block_found`: subtracting one from the
/// other should not need a label matcher.
pub fn block_orphaned() {
    counter!("pool_blocks_orphaned_total").increment(1);
}

/// How the deferred confirmation pass decided a block, one series per outcome.
///
/// `result` is a closed set (`stats::BlockResolution::label`); this is the
/// breakdown that makes `found = confirmed + orphaned + abandoned + pending`
/// checkable against the gauge below.
pub fn block_confirmation(result: &'static str) {
    counter!("pool_block_confirmations_total", "result" => result).increment(1);
}

/// Found blocks the confirmation pass has not yet decided. Normally zero; a
/// value stuck above zero for hours means the pass is not making progress.
pub fn update_blocks_pending_confirmation(pending: u64) {
    gauge!("pool_blocks_pending_confirmation").set(pending as f64);
}

/// `window` label values for the hashrate gauges, in the order
/// `stats::HashrateWindows::to_windows` produces (the `mining::hashrate` `W_*`
/// indices). Keep the two in step or every series is mislabelled.
pub const HASHRATE_WINDOW_LABELS: [&str; 7] = ["1m", "5m", "10m", "1h", "3h", "6h", "24h"];

/// Pool-wide hashrate, one series per averaging window. No worker label, so
/// this is the series to alert on — it does not fan out with the fleet.
pub fn update_pool_hashrate(windows: [f64; 7]) {
    for (hps, window) in windows.iter().zip(HASHRATE_WINDOW_LABELS) {
        gauge!("pool_hashrate_hps", "window" => window).set(*hps);
    }
}

/// Per-worker hashrate, one series per worker per averaging window. A worker
/// that has gone away is exported as zero for one tick before its series are
/// left to the idle timeout, so dashboards fall to zero rather than flatlining
/// at the last reading.
pub fn update_worker_hashrate(worker: &str, windows: [f64; 7]) {
    for (hps, window) in windows.iter().zip(HASHRATE_WINDOW_LABELS) {
        gauge!(
            "pool_worker_hashrate_hps",
            "worker" => worker.to_string(),
            "window" => window
        )
        .set(*hps);
    }
}

pub fn update_job_height(height: u64) {
    gauge!("pool_job_height").set(height as f64);
}

/// Raw unix timestamp of the last fully successful template refresh — not an
/// age, so PromQL derives freshness with
/// `time() - pool_template_last_refresh_timestamp_seconds`, the same idiom as
/// `process_start_time_seconds`. Lets operators pick an alerting threshold
/// independent of `/health`'s fixed `TEMPLATE_STALE_AFTER` cutoff.
pub fn update_template_last_refresh(unix_secs: u64) {
    gauge!("pool_template_last_refresh_timestamp_seconds").set(unix_secs as f64);
}

/// A refresh derived `clean=true` that its caller did not request — the ntime
/// timer, not the block-notification path, is what noticed the tip moved. One
/// is a coincidence (the block landed between the notification firing and the
/// timer tick); a handful in a row is evidence that path stopped delivering.
pub fn tip_change_discovered_by_timer() {
    counter!("pool_tip_changes_discovered_by_timer_total").increment(1);
}
