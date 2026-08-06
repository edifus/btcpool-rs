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
    bitcoin::{rpc::RpcClient, template, zmq::NewBlockReceiver},
    config::PoolConfig,
    error::PoolError,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
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
        })
    }

    /// Subscribe to new-job broadcasts. Call this when a miner session connects.
    pub fn subscribe(&self) -> broadcast::Receiver<JobBroadcast> {
        self.job_tx.subscribe()
    }

    /// Return the current best job, if any.
    pub async fn current_template(&self) -> Option<Arc<template::JobTemplate>> {
        self.current_template.read().await.clone()
    }

    /// Main loop: refresh the template whenever a new block arrives,
    /// and periodically push a ntime-updated job to keep ASIC hardware active.
    pub async fn run(self: Arc<Self>, mut new_block: NewBlockReceiver) {
        // Do an immediate fetch on startup
        self.refresh(true).await;

        let mut ntime_tick = tokio::time::interval(Duration::from_secs(NTIME_REFRESH_SECS));
        ntime_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ntime_tick.tick().await; // discard the immediate first tick

        loop {
            tokio::select! {
                result = new_block.changed() => {
                    if result.is_err() {
                        warn!("New-block channel closed; stopping template engine");
                        break;
                    }
                    // New block: full GBT refresh, miners must abandon old work
                    self.refresh(true).await;
                    // Reset the ntime timer so we don't send a redundant notify
                    // right after the block notify
                    ntime_tick.reset();
                }
                _ = ntime_tick.tick() => {
                    // Periodic ntime refresh: new job_id + current wall-clock time,
                    // but miners may keep working on current nonce ranges (clean=false)
                    self.refresh(false).await;
                }
            }
        }
    }

    /// Fetch a fresh GBT and push it out to all connected sessions.
    async fn refresh(&self, clean_jobs: bool) {
        match self.rpc.get_block_template().await {
            Ok(gbt) => {
                match template::build_job_template(&gbt) {
                    Ok(template) => {
                        let template = Arc::new(template);
                        debug!(
                            height = template.height,
                            bits = %template.bits,
                            "New shared template built"
                        );

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

                        // Broadcast — ignore "no receivers" errors (normal before first miner)
                        let receiver_count = self.job_tx.receiver_count();
                        let _ = self.job_tx.send(JobBroadcast {
                            template,
                            clean: clean_jobs,
                        });
                        metrics::job_broadcast(receiver_count);
                    }
                    Err(e) => error!("Failed to build job: {e}"),
                }
            }
            Err(e) => error!("getblocktemplate failed: {e}"),
        }
    }

    /// Submit a found block, guaranteeing it cannot be silently lost: the raw
    /// hex is archived to disk in parallel with the first attempt, transient
    /// RPC failures are retried in-line a few times, and if those fail a
    /// detached background task keeps retrying while the caller reports the
    /// failure.
    pub async fn submit_found_block(
        self: &Arc<Self>,
        height: u64,
        hash_hex: &str,
        block_hex: String,
        worker: &str,
        payout: &str,
        stats: Arc<crate::stats::PoolStats>,
    ) -> Result<(), PoolError> {
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
                Ok(()) => return Ok(()),
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
                    Ok(()) => {
                        metrics::block_found();
                        metrics::block_submission_success();
                        // Mirror the inline-success path so the dashboard's
                        // block count / last-block panel agree with Prometheus.
                        stats.block_found(&worker, &payout, &hash_hex);
                        info!(
                            "🏆 Block {hash_hex} (height {height}) accepted on \
                             retry attempt {attempt}"
                        );
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
