use crate::metrics;
/// mining/engine.rs
///
/// The TemplateEngine:
///  - Holds the current address-independent template
///  - Broadcasts template updates to connected miner sessions
///  - Is the single writer to shared template state
///
/// Sessions materialize payout-specific jobs and retain their own bounded job
/// history so a submitted job can only pay the identity that received it.
use crate::{
    bitcoin::{
        rpc::{BlockSubmitOutcome, RpcClient},
        template,
        zmq::NewBlockReceiver,
    },
    config::PoolConfig,
    error::PoolError,
    mining::accounting,
};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{broadcast, RwLock},
    task,
};
use tracing::{debug, error, info, warn};

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Channel capacity for new-job broadcasts
const JOB_BROADCAST_CAP: usize = 64;

/// Broadcast payload: the new job plus whether miners should discard current work.
#[derive(Clone, Debug)]
pub struct JobBroadcast {
    pub template: Arc<template::JobTemplate>,
    /// true  = new block, miners MUST abandon old work (clean_jobs=true in notify)
    /// false = ntime refresh only, miners MAY continue current work
    pub clean: bool,
}

/// How often to push a new job with refreshed ntime even without a new block.
/// This keeps Avalon/ASIC hardware fed — at 5 TH/s the 32-bit nonce space
/// exhausts in <1ms, so miners need periodic work updates to stay active.
const NTIME_REFRESH_SECS: u64 = 30;

/// How stale the template may get before /health reports unhealthy. Six
/// nominal ntime cycles: comfortably survives a bitcoind restart or a
/// needrestart sweep without flapping, and still fires inside 3 min — short
/// against the ~600 s mean block interval, so a freeze is normally caught
/// before it costs anything. Well clear of bitcoin_rpc.timeout_secs (10 s).
const TEMPLATE_STALE_AFTER: Duration = Duration::from_secs(180);

/// In-line submitblock attempts before handing off to the background retrier.
/// Kept small so the miner still gets a timely share response.
const SUBMIT_INLINE_ATTEMPTS: u32 = 3;

/// Background retrier cadence and give-up horizon. Past the deadline the block
/// is almost certainly orphaned, but its hex stays archived on disk either way.
const SUBMIT_RETRY_INTERVAL: Duration = Duration::from_secs(10);
const SUBMIT_RETRY_DEADLINE: Duration = Duration::from_secs(2 * 60 * 60);

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// TemplateEngine
// ─────────────────────────────────────────────────────────────────────────────

pub struct TemplateEngine {
    rpc: Arc<RpcClient>,
    pool_cfg: PoolConfig,

    /// Current address-independent template.
    current_template: RwLock<Option<Arc<template::JobTemplate>>>,

    /// Broadcast channel — sessions subscribe on connect
    job_tx: broadcast::Sender<JobBroadcast>,

    /// Chain figures for the dashboard are published from here rather than from
    /// the per-session broadcast handler: with no miners connected there is no
    /// session to run that code, and the dashboard would sit on a stale height.
    stats: Arc<crate::stats::PoolStats>,

    /// Fixed at construction. `last_refresh_offset_secs` is measured against
    /// this monotonic clock rather than the wall clock so an NTP step can
    /// never corrupt the staleness decision (see `template_age_from`).
    engine_started: Instant,

    /// `engine_started.elapsed().as_secs()` as of the last fully successful
    /// `refresh` (successful GBT *and* successful `build_job_template`). Left
    /// at 0 until the first one lands, so an engine that has never refreshed
    /// reads as exactly as stale as it is old, rather than falsely fresh.
    last_refresh_offset_secs: AtomicU64,

    /// Set by the first fully successful `refresh`. Needed because a 0 offset
    /// is ambiguous: the startup refresh normally lands inside the engine's
    /// first second, which stores 0 — indistinguishable from the initial value
    /// meaning "never refreshed". Only `is_template_fresh` needs to tell those
    /// apart; `template_age` reads correctly either way.
    has_refreshed: AtomicBool,

    /// Wall-clock unix seconds of the same event, for the
    /// `pool_template_last_refresh_timestamp_seconds` gauge only — never
    /// branched on internally, so an NTP step can't affect a health decision.
    last_refresh_unix_secs: AtomicU64,

    /// The `!`-prefixed template rules this build does not implement, as of the
    /// last refresh. Empty is the healthy case. Retained by name, not as a
    /// count, so `/health` and the dashboard can say *which* rule rather than
    /// just report unhealthy.
    ///
    /// Populated regardless of `strict_gbt_rules`, because the operator wants to
    /// see it either way; only the strict flag decides whether it also stops
    /// work (see [`Self::is_blocked_on_rules`]).
    unsupported_rules: RwLock<Vec<String>>,
}

impl TemplateEngine {
    pub fn new(
        rpc: Arc<RpcClient>,
        pool_cfg: PoolConfig,
        stats: Arc<crate::stats::PoolStats>,
    ) -> Arc<Self> {
        let (job_tx, _) = broadcast::channel(JOB_BROADCAST_CAP);
        Arc::new(Self {
            rpc,
            pool_cfg,
            current_template: RwLock::new(None),
            job_tx,
            stats,
            engine_started: Instant::now(),
            last_refresh_offset_secs: AtomicU64::new(0),
            last_refresh_unix_secs: AtomicU64::new(0),
            has_refreshed: AtomicBool::new(false),
            unsupported_rules: RwLock::new(Vec::new()),
        })
    }

    /// `!`-prefixed template rules this build does not implement, as of the last
    /// refresh. Empty is the healthy case. Reported whether or not
    /// `strict_gbt_rules` is on.
    pub async fn unsupported_rules(&self) -> Vec<String> {
        self.unsupported_rules.read().await.clone()
    }

    /// Whether those rules are actually stopping work — that is, whether the
    /// operator left `strict_gbt_rules` on. With it off the pool keeps mining
    /// by explicit instruction, so this stays false and `/health` stays 200:
    /// a probe that failed anyway would just restart the pool in a loop.
    pub async fn is_blocked_on_rules(&self) -> bool {
        self.pool_cfg.strict_gbt_rules && !self.unsupported_rules.read().await.is_empty()
    }

    /// Subscribe to new-job broadcasts. Call this when a miner session connects.
    pub fn subscribe(&self) -> broadcast::Receiver<JobBroadcast> {
        self.job_tx.subscribe()
    }

    /// Return the current best job, if any.
    pub async fn current_template(&self) -> Option<Arc<template::JobTemplate>> {
        self.current_template.read().await.clone()
    }

    /// How long since the last fully successful refresh. Reads as full process
    /// uptime if `refresh` has never once succeeded.
    pub fn template_age(&self) -> Duration {
        template_age_from(
            self.engine_started,
            self.last_refresh_offset_secs.load(Ordering::Relaxed),
            Instant::now(),
        )
    }

    /// Whether the template is fresh enough to serve. Backs `GET /health`.
    ///
    /// A refresh must have actually landed: before the first one the pool has
    /// no template to hand out at all, and reporting healthy then would hide a
    /// node that was already unreachable at startup for the whole
    /// `TEMPLATE_STALE_AFTER` window — precisely the blindness this endpoint
    /// exists to remove. The cost is a 503 for the one RPC round trip between
    /// binding the listener and the startup refresh landing, which is the
    /// honest answer during that window.
    pub fn is_template_fresh(&self) -> bool {
        is_fresh(
            self.has_refreshed.load(Ordering::Relaxed),
            self.template_age(),
        )
    }

    /// Main loop: refresh the template whenever a new block arrives,
    /// and periodically push a ntime-updated job to keep ASIC hardware active.
    ///
    /// A thin wrapper around `drive_refresh_loop`: the control flow lives there
    /// so it can be driven by a fake `on_refresh` in tests without a real RPC
    /// client, while this method supplies the real one (`Self::refresh`).
    pub async fn run(self: Arc<Self>, new_block: NewBlockReceiver) {
        // Do an immediate fetch on startup
        self.refresh(true).await;

        let mut ntime_tick = tokio::time::interval(Duration::from_secs(NTIME_REFRESH_SECS));
        ntime_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ntime_tick.tick().await; // discard the immediate first tick

        drive_refresh_loop(new_block, ntime_tick, |clean| self.refresh(clean)).await;
    }

    /// Fetch a fresh GBT and push it out to all connected sessions.
    async fn refresh(&self, clean_jobs: bool) {
        match self.rpc.get_block_template().await {
            Ok(gbt) => {
                if !self.check_template_rules(&gbt).await {
                    return;
                }
                match template::build_job_template(&gbt) {
                    Ok(template) => {
                        let template = Arc::new(template);
                        debug!(
                            height = template.height,
                            bits = %template.bits,
                            "New shared template built"
                        );

                        // Capture the outgoing template's prev_hash before it is
                        // overwritten below: `should_force_clean` compares
                        // against it, and it is unrecoverable the instant the
                        // new template replaces it in `current_template`.
                        let previous_prev_hash = self
                            .current_template
                            .read()
                            .await
                            .as_ref()
                            .map(|t| t.prev_hash.clone());
                        let clean = should_force_clean(
                            clean_jobs,
                            previous_prev_hash.as_deref(),
                            &template.prev_hash,
                        );
                        if clean && !clean_jobs {
                            // The ntime timer noticed a tip change the caller
                            // didn't already know about — proof the block path
                            // (normally ZMQ) missed it.
                            metrics::tip_change_discovered_by_timer();
                        }

                        *self.current_template.write().await = Some(template.clone());

                        self.stats.update_height(
                            template.height,
                            template.coinbase_value,
                            template.transactions.len() as u64,
                        );
                        match template::bits_to_difficulty(&template.bits) {
                            Ok(net_diff) => self.stats.set_network_difficulty(net_diff),
                            Err(e) => warn!("Unusable nbits {}: {e}", template.bits),
                        }
                        metrics::update_job_height(template.height);

                        // Freshness bookkeeping — only on a fully successful
                        // refresh, so a GBT or job-build failure lets the
                        // template correctly age instead of resetting the clock
                        // on a template that never actually changed.
                        let offset_secs = self.engine_started.elapsed().as_secs();
                        self.last_refresh_offset_secs
                            .store(offset_secs, Ordering::Relaxed);
                        let unix_secs = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        self.last_refresh_unix_secs
                            .store(unix_secs, Ordering::Relaxed);
                        self.has_refreshed.store(true, Ordering::Relaxed);
                        metrics::update_template_last_refresh(unix_secs);

                        // Broadcast — ignore "no receivers" errors (normal before first miner)
                        let receiver_count = self.job_tx.receiver_count();
                        let _ = self.job_tx.send(JobBroadcast { template, clean });
                        metrics::job_broadcast(receiver_count);
                    }
                    Err(e) => error!("Failed to build job: {e}"),
                }
            }
            Err(e) => error!("getblocktemplate failed: {e}"),
        }
    }

    /// Gate a fresh template on the `!`-prefixed rules it announces.
    ///
    /// Returns whether the caller may go on to build a job. Under
    /// `strict_gbt_rules` an unimplemented rule also *withdraws* the previous
    /// template: leaving it in place would keep every connected miner grinding
    /// a prev-hash that has since moved on, which does not orphan the resulting
    /// block so much as make it worthless — the node stores it on a side branch
    /// and `submitblock` says `inconclusive`. Better to serve nothing and say
    /// so loudly than to burn hashrate producing blocks that cannot win.
    async fn check_template_rules(&self, gbt: &crate::bitcoin::rpc::GbtResult) -> bool {
        let unsupported = template::unsupported_gbt_rules(gbt);
        metrics::update_unsupported_gbt_rules(unsupported.len());

        // Edge-triggered: this runs on every tip change and every 30 s tick, and
        // an activation does not resolve itself. One line per transition, not
        // one per refresh.
        let changed = {
            let mut current = self.unsupported_rules.write().await;
            let changed = *current != unsupported;
            if changed {
                current.clone_from(&unsupported);
            }
            changed
        };

        if unsupported.is_empty() {
            if changed {
                info!("template rules are supported again; resuming work");
            }
            return true;
        }

        if !self.pool_cfg.strict_gbt_rules {
            if changed {
                error!(
                    rules = %unsupported.join(", "),
                    "getblocktemplate announces rules this build does not implement. \
                     strict_gbt_rules is off, so the pool keeps mining — verify by hand \
                     that the coinbase it builds still satisfies them"
                );
            }
            return true;
        }

        if changed {
            error!(
                rules = %unsupported.join(", "),
                "getblocktemplate announces rules this build does not implement; \
                 refusing to issue work because the coinbase this pool builds may no \
                 longer be consensus-valid. Upgrade btcpool-rs, or set \
                 strict_gbt_rules = false once you have verified the new rules by hand"
            );
        }
        // Withdraw the stale template so sessions stop being handed work.
        *self.current_template.write().await = None;
        false
    }

    /// Submit a found block, guaranteeing it cannot be silently lost: the raw
    /// hex is archived to disk in parallel with the first attempt, transient
    /// RPC failures are retried in-line a few times, and if those fail a
    /// detached background task keeps retrying while the caller reports the
    /// failure.
    ///
    /// `Ok` is not the same as a win — check
    /// [`BlockSubmitOutcome::is_win`](crate::bitcoin::rpc::BlockSubmitOutcome::is_win)
    /// before reporting one. A block that lost a same-height race comes back as
    /// `Ok(Inconclusive)`: the node took it and stored it, and it earned
    /// nothing.
    pub async fn submit_found_block(
        self: &Arc<Self>,
        height: u64,
        hash_hex: &str,
        block_hex: String,
        worker: &str,
        payout: &str,
        stats: Arc<crate::stats::PoolStats>,
    ) -> Result<BlockSubmitOutcome, PoolError> {
        let block_hex = Arc::new(block_hex);

        // Archive concurrently on a blocking thread — submission is in a race
        // against the rest of the network and must not wait on disk; the
        // archive only matters if submission fails, and it still lands within
        // milliseconds of the submit going out.
        {
            let dir = self.pool_cfg.found_block_dir.clone();
            let hash = hash_hex.to_owned();
            let hex = block_hex.clone();
            task::spawn_blocking(move || archive_found_block(&dir, height, &hash, &hex));
        }

        let mut last_err = None;
        for attempt in 1..=SUBMIT_INLINE_ATTEMPTS {
            match self.rpc.submit_block(block_hex.clone()).await {
                // Terminal for every outcome, including `Inconclusive`:
                // resubmitting a block the node already stored on a side branch
                // cannot promote it to the tip.
                Ok(outcome) => return Ok(outcome),
                Err(e) if is_permanent_reject(&e) => return Err(e),
                Err(e) => {
                    warn!(
                        "submitblock attempt {attempt}/{SUBMIT_INLINE_ATTEMPTS} \
                         failed for block {hash_hex} (height {height}): {e}"
                    );
                    last_err = Some(e);
                }
            }
            if attempt < SUBMIT_INLINE_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(500 << attempt)).await;
            }
        }

        self.spawn_resubmit_task(
            height,
            hash_hex.to_owned(),
            block_hex,
            worker.to_owned(),
            payout.to_owned(),
            stats,
        );
        Err(last_err
            .unwrap_or_else(|| PoolError::Other(anyhow::anyhow!("submitblock never attempted"))))
    }

    /// Keep resubmitting a found block in the background after the in-line
    /// attempts failed — e.g. while bitcoind restarts. `submit_block` treats
    /// "duplicate" as success, so racing an earlier attempt is harmless.
    fn spawn_resubmit_task(
        self: &Arc<Self>,
        height: u64,
        hash_hex: String,
        block_hex: Arc<String>,
        worker: String,
        payout: String,
        stats: Arc<crate::stats::PoolStats>,
    ) {
        let engine = self.clone();
        tokio::spawn(async move {
            let deadline = Instant::now() + SUBMIT_RETRY_DEADLINE;
            let mut attempt = SUBMIT_INLINE_ATTEMPTS;
            while Instant::now() < deadline {
                tokio::time::sleep(SUBMIT_RETRY_INTERVAL).await;
                attempt += 1;
                match engine.rpc.submit_block(block_hex.clone()).await {
                    Ok(outcome) => {
                        // Mirror the inline-success path so the dashboard's
                        // block count agrees with Prometheus.
                        accounting::record_block_outcome(
                            &stats, outcome, height, &worker, &payout, &hash_hex,
                        );
                        if outcome.is_win() {
                            info!(
                                "🏆 Block {hash_hex} (height {height}) accepted on \
                                 retry attempt {attempt}"
                            );
                        } else {
                            warn!(
                                "Block {hash_hex} (height {height}) was valid but lost \
                                 its height race on retry attempt {attempt}; it is stored \
                                 on a side branch and earned nothing"
                            );
                        }
                        return;
                    }
                    Err(e) if is_permanent_reject(&e) => {
                        error!(
                            "Block {hash_hex} (height {height}) permanently \
                             rejected on retry attempt {attempt}: {e}"
                        );
                        return;
                    }
                    Err(e) => {
                        warn!(
                            "submitblock retry attempt {attempt} for block \
                             {hash_hex} (height {height}) failed: {e}"
                        );
                    }
                }
            }
            error!(
                "Giving up resubmitting block {hash_hex} (height {height}) after {:?}; \
                 its hex remains archived in {} — submit manually with \
                 `bitcoin-cli submitblock`",
                SUBMIT_RETRY_DEADLINE, engine.pool_cfg.found_block_dir
            );
        });
    }
}

/// The `run` loop's control flow, extracted so it is testable without a real
/// `TemplateEngine` (which needs a live RPC client): `on_refresh` stands in for
/// `TemplateEngine::refresh`, called with the same `clean` flag it would be.
///
/// Runs forever. `tip_signal_alive` starts `true` and is latched `false` the
/// first time `new_block` closes, which drops that branch from the `select!`'s
/// poll set — a bare `continue` without the guard would hot-spin, since
/// `changed()` on a closed channel resolves `Err` immediately and forever.
/// `select!` evaluates the guard before polling the branch, and both
/// `changed()` and `tick()` are cancellation-safe, so nothing is lost to
/// whichever branch does *not* win a given iteration.
async fn drive_refresh_loop<F, Fut>(
    mut new_block: NewBlockReceiver,
    mut ntime_tick: tokio::time::Interval,
    mut on_refresh: F,
) where
    F: FnMut(bool) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut tip_signal_alive = true;
    loop {
        tokio::select! {
            result = new_block.changed(), if tip_signal_alive => {
                if result.is_err() {
                    error!(
                        "New-block channel closed; template engine now relies on \
                         the {NTIME_REFRESH_SECS}s ntime timer only"
                    );
                    tip_signal_alive = false;
                    continue;
                }
                // New block: full GBT refresh, miners must abandon old work
                on_refresh(true).await;
                // Reset the ntime timer so we don't send a redundant notify
                // right after the block notify
                ntime_tick.reset();
            }
            _ = ntime_tick.tick() => {
                // Periodic ntime refresh: new job_id + current wall-clock time,
                // but miners may keep working on current nonce ranges (clean=false)
                on_refresh(false).await;
            }
        }
    }
}

/// Whether a refresh should tell miners to abandon in-flight work. `true`
/// whenever the caller already knows it must be (a ZMQ-driven block refresh),
/// and *also* whenever `prev_hash` moved since the last template regardless of
/// what the caller asked for — which is what turns a timer-discovered tip into
/// a forced clean job instead of a silently stale one.
///
/// Compared on `prev_hash`, not `height`: `hashPrevBlock` is the only field in
/// the 80-byte header that binds work to a specific chain (height lives only in
/// the BIP34 coinbase push), so it is a strict superset of a height comparison
/// — in particular it also catches a same-height reorg, which height alone
/// would miss.
fn should_force_clean(caller_requested: bool, previous: Option<&str>, new_prev_hash: &str) -> bool {
    caller_requested || previous != Some(new_prev_hash)
}

/// The freshness verdict behind `TemplateEngine::is_template_fresh`, split out
/// so both halves of it — the never-refreshed case and the threshold — are
/// testable without a live `RpcClient`.
fn is_fresh(has_refreshed: bool, age: Duration) -> bool {
    has_refreshed && age < TEMPLATE_STALE_AFTER
}

/// How stale a template is, from an `engine_started` epoch, the offset (in
/// seconds since `engine_started`) of the last successful refresh, and the
/// current time. Pure — and takes `now` as a parameter rather than sampling
/// the clock itself — so it is testable without waiting on a real clock.
fn template_age_from(
    engine_started: Instant,
    last_refresh_offset_secs: u64,
    now: Instant,
) -> Duration {
    let now_offset_secs = now.saturating_duration_since(engine_started).as_secs();
    Duration::from_secs(now_offset_secs.saturating_sub(last_refresh_offset_secs))
}

/// A consensus-level rejection: the block itself is invalid or outdated, so
/// resubmitting the same bytes can never succeed. Everything else (transport
/// errors, node restarting, unexpected responses) is worth retrying.
fn is_permanent_reject(e: &PoolError) -> bool {
    matches!(e, PoolError::SubmitBlockRejected(_))
}

/// Write the block hex to `<found_block_dir>/block_<height>_<hash>.hex` so the
/// block survives a crash or node outage and can be replayed by hand. Runs on
/// a blocking thread in parallel with submission. Failure is loud but
/// non-fatal — submission proceeds regardless.
fn archive_found_block(dir: &str, height: u64, hash_hex: &str, block_hex: &str) -> Option<PathBuf> {
    let dir = Path::new(dir);
    let path = dir.join(format!("block_{height}_{hash_hex}.hex"));
    let res = std::fs::create_dir_all(dir).and_then(|_| std::fs::write(&path, block_hex));
    match res {
        Ok(()) => {
            info!("Archived found block to {}", path.display());
            Some(path)
        }
        Err(e) => {
            error!(
                "Failed to archive found block {hash_hex} to {}: {e}",
                path.display()
            );
            None
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── should_force_clean ──────────────────────────────────────────────────

    #[test]
    fn should_force_clean_when_caller_requested_it() {
        // Caller already knows (ZMQ-driven block refresh); the prev_hash
        // comparison is irrelevant.
        assert!(should_force_clean(true, Some("aaaa"), "aaaa"));
    }

    #[test]
    fn should_force_clean_when_prev_hash_changed_under_an_ntime_refresh() {
        // The timer, not the caller, is the one that noticed the tip moved.
        assert!(should_force_clean(false, Some("aaaa"), "bbbb"));
    }

    #[test]
    fn should_not_force_clean_on_same_block_ntime_refresh() {
        // Routine ntime refresh: same tip, miners may keep their nonce range.
        assert!(!should_force_clean(false, Some("aaaa"), "aaaa"));
    }

    #[test]
    fn should_force_clean_for_first_template() {
        // No previous template to compare against — treat it like a new tip.
        assert!(should_force_clean(false, None, "aaaa"));
    }

    // ── template_age_from ───────────────────────────────────────────────────

    #[test]
    fn template_age_from_computes_elapsed_since_last_refresh() {
        let start = Instant::now();
        let age = template_age_from(start, 10, start + Duration::from_secs(30));
        assert_eq!(age, Duration::from_secs(20));
    }

    #[test]
    fn template_age_from_never_refreshed_reads_as_full_uptime() {
        let start = Instant::now();
        // last_refresh_offset_secs = 0, the constructor's initial value.
        let age = template_age_from(start, 0, start + Duration::from_secs(45));
        assert_eq!(age, Duration::from_secs(45));
    }

    // ── is_fresh ────────────────────────────────────────────────────────────

    #[test]
    fn never_refreshed_is_never_fresh() {
        // The startup refresh normally lands within the engine's first second,
        // so a young engine looks identical to one whose node was unreachable
        // from the start. Only the has_refreshed flag separates them, and
        // /health must report the second case as stale immediately rather than
        // for TEMPLATE_STALE_AFTER pretending the pool has work to hand out.
        assert!(!is_fresh(false, Duration::ZERO));
        assert!(!is_fresh(false, TEMPLATE_STALE_AFTER / 2));
    }

    #[test]
    fn refreshed_is_fresh_until_the_threshold() {
        assert!(is_fresh(true, Duration::ZERO));
        assert!(is_fresh(
            true,
            TEMPLATE_STALE_AFTER - Duration::from_secs(1)
        ));
        // Boundary is exclusive: exactly at the threshold is already stale.
        assert!(!is_fresh(true, TEMPLATE_STALE_AFTER));
        assert!(!is_fresh(true, TEMPLATE_STALE_AFTER * 2));
    }

    // ── drive_refresh_loop ──────────────────────────────────────────────────

    /// Proves the ntime timer keeps firing after the new-block channel closes
    /// — a `break` on channel close would silently stop refreshes for the life
    /// of the process. Drives the real `drive_refresh_loop`, not a copy of its
    /// control flow.
    #[tokio::test(start_paused = true)]
    async fn ntime_timer_keeps_firing_after_new_block_channel_closes() {
        let (tx, rx) = tokio::sync::watch::channel(0u64);
        drop(tx); // channel closed before drive_refresh_loop ever polls it

        let mut ntime_tick = tokio::time::interval(Duration::from_secs(NTIME_REFRESH_SECS));
        ntime_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ntime_tick.tick().await; // mirrors `run` discarding the immediate first tick

        let clean_calls = Arc::new(AtomicU64::new(0));
        let dirty_calls = Arc::new(AtomicU64::new(0));
        let (clean_counter, dirty_counter) = (clean_calls.clone(), dirty_calls.clone());

        tokio::spawn(drive_refresh_loop(rx, ntime_tick, move |clean: bool| {
            if clean {
                clean_counter.fetch_add(1, Ordering::Relaxed);
            } else {
                dirty_counter.fetch_add(1, Ordering::Relaxed);
            }
            std::future::ready(())
        }));

        // With time paused, the runtime auto-advances the clock to a task's
        // timer deadline whenever there is nothing else runnable — so sleeping
        // here (the only other live task is the spawned loop, parked on its
        // own timer) walks the clock through each of its three ntime
        // deadlines in turn, letting it actually run `on_refresh` and re-arm
        // between them, rather than asserting anything about how a single
        // `advance()` call batches multiple elapsed timers. The `+ 1` breaks
        // an exact tie with the loop's own third deadline: at the same virtual
        // instant, this task's wake could otherwise be ordered before the
        // spawned task gets to process its own, undercounting by one.
        tokio::time::sleep(Duration::from_secs(NTIME_REFRESH_SECS * 3 + 1)).await;

        assert_eq!(
            clean_calls.load(Ordering::Relaxed),
            0,
            "the channel closed before any block notification; a clean refresh \
             should never fire"
        );
        let dirty = dirty_calls.load(Ordering::Relaxed);
        assert!(
            dirty >= 3,
            "expected at least 3 ntime refreshes after the channel closed, got {dirty}"
        );
    }

    // ── check_template_rules ────────────────────────────────────────────────

    /// A real engine, which needs no live backend: `RpcClient::new` only builds
    /// a lazy JSON-RPC client, and `PoolStats` runs storeless. That lets these
    /// exercise the actual method — including withdrawing the template — rather
    /// than a restatement of its control flow.
    fn test_engine(strict_gbt_rules: bool) -> Arc<TemplateEngine> {
        let rpc = Arc::new(
            crate::bitcoin::rpc::RpcClient::new(&crate::config::RpcConfig {
                url: "http://127.0.0.1:1".into(),
                cookie_path: None,
                user: Some("u".into()),
                password: Some("p".into()),
                timeout_secs: 1,
            })
            .expect("building a lazy RPC client cannot fail"),
        );
        let pool_cfg = PoolConfig {
            listen_addr: "127.0.0.1:0".into(),
            coinbase_address: None,
            coinbase_tag: "/test/".into(),
            initial_difficulty: 1,
            extranonce1_size: 4,
            extranonce2_size: 4,
            max_connections: 8,
            idle_timeout_secs: 300,
            found_block_dir: "found-blocks".into(),
            confirmation_depth: 6,
            network: None,
            strict_gbt_rules,
        };
        TemplateEngine::new(rpc, pool_cfg, crate::stats::PoolStats::new_with_store(None))
    }

    fn gbt_with_rules(rules: &[&str]) -> crate::bitcoin::rpc::GbtResult {
        crate::bitcoin::rpc::GbtResult {
            version: 0x2000_0000,
            prev_hash: "00".repeat(32),
            bits: "1d00ffff".into(),
            cur_time: 1_700_000_000,
            height: 900_000,
            coinbase_value: 312_500_000,
            transactions: Vec::new(),
            longpoll_id: None,
            default_witness_commitment: Some("6a24aa21a9ed".to_string() + &"11".repeat(32)),
            rules: rules.iter().map(|r| r.to_string()).collect(),
            vbrequired: 0,
        }
    }

    #[tokio::test]
    async fn supported_rules_leave_the_engine_working() {
        let engine = test_engine(true);
        assert!(
            engine
                .check_template_rules(&gbt_with_rules(&["csv", "!segwit", "taproot"]))
                .await
        );
        assert!(engine.unsupported_rules().await.is_empty());
        assert!(!engine.is_blocked_on_rules().await);
    }

    /// The point of the whole gate: an activation this build predates must stop
    /// work *and* withdraw the template already in hand. Leaving the old one up
    /// would keep every miner grinding a prev-hash the chain has moved past, so
    /// the blocks it produced could not win even if they were valid.
    #[tokio::test]
    async fn an_unimplemented_rule_stops_work_and_withdraws_the_template() {
        let engine = test_engine(true);

        // Seed a template, as a successful refresh would have.
        *engine.current_template.write().await = Some(Arc::new(
            template::build_job_template(&gbt_with_rules(&["!segwit"])).unwrap(),
        ));
        assert!(engine.current_template().await.is_some());

        let proceed = engine
            .check_template_rules(&gbt_with_rules(&["!segwit", "!greatfork"]))
            .await;

        assert!(!proceed, "the caller must not go on to build a job");
        assert!(
            engine.current_template().await.is_none(),
            "the stale template must be withdrawn, not left serving work"
        );
        assert_eq!(
            engine.unsupported_rules().await,
            vec!["greatfork".to_string()]
        );
        assert!(engine.is_blocked_on_rules().await);
    }

    /// With the escape hatch open the operator has said to keep mining, so the
    /// pool reports the rule but does not stop — and `/health` stays 200, since
    /// a probe failing here would just restart the pool in a loop.
    #[tokio::test]
    async fn strict_gbt_rules_off_reports_but_keeps_mining() {
        let engine = test_engine(false);
        let proceed = engine
            .check_template_rules(&gbt_with_rules(&["!segwit", "!greatfork"]))
            .await;

        assert!(proceed);
        assert_eq!(
            engine.unsupported_rules().await,
            vec!["greatfork".to_string()]
        );
        assert!(!engine.is_blocked_on_rules().await);
    }

    /// The condition clears on its own once the operator upgrades, so the gate
    /// has to lift without a restart.
    #[tokio::test]
    async fn work_resumes_once_the_rule_becomes_supported() {
        let engine = test_engine(true);
        assert!(
            !engine
                .check_template_rules(&gbt_with_rules(&["!greatfork"]))
                .await
        );
        assert!(engine.is_blocked_on_rules().await);

        assert!(
            engine
                .check_template_rules(&gbt_with_rules(&["!segwit"]))
                .await
        );
        assert!(engine.unsupported_rules().await.is_empty());
        assert!(!engine.is_blocked_on_rules().await);
    }
}
