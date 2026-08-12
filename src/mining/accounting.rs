/// mining/accounting.rs
///
/// The stats and metrics bookkeeping for one submitted share, shared by the
/// SV1 and SV2 session loops.
///
/// Both protocols validate shares through the same `validator` and must report
/// them identically; keeping two hand-written copies of this sequence let them
/// drift (the Prometheus share-difficulty histogram and the dashboard hashrate
/// were recording different difficulties). The session keeps its own local
/// counters and vardiff — only the pool-wide bookkeeping lives here.
use crate::{
    bitcoin::rpc::BlockSubmitOutcome,
    error::PoolError,
    metrics,
    security::SessionGuard,
    stats::{BlockResolution, PendingBlock, PoolStats},
};

/// Why a share was not accepted. A closed set, so the `reason` Prometheus label
/// cannot be minted from untrusted input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Submitted before authorizing, or under a different worker name.
    Unauthorized,
    /// Job id is not in this session's history (rotated out, or never issued).
    JobNotFound,
    /// This exact share was already accepted.
    Duplicate,
    /// The job was retired by a clean-jobs broadcast.
    Stale,
    /// Below the acceptance threshold.
    LowDifficulty,
    /// SV2 extranonce width did not match the channel.
    BadExtranonce,
    /// Anything else the validator refused (bad ntime, version bits, …).
    Invalid,
}

impl RejectReason {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::JobNotFound => "job_not_found",
            Self::Duplicate => "duplicate",
            Self::Stale => "stale",
            Self::LowDifficulty => "low_difficulty",
            Self::BadExtranonce => "bad_extranonce",
            Self::Invalid => "invalid",
        }
    }

    /// Whether this rejection should count toward the invalid-share ban.
    ///
    /// Stale and low-difficulty shares are the normal consequence of pool-side
    /// timing — a new block, or a vardiff raise the miner has not applied to
    /// its queued work yet — so counting them would disconnect honest miners.
    /// `Unauthorized` is a miner-side misconfiguration (submitting under a name
    /// it never authorized) rather than an attack: the per-message rate limiter
    /// and the pre-auth handshake deadline already bound what it can cost us,
    /// and dropping the connection would only hide the mistake.
    pub const fn counts_as_invalid(self) -> bool {
        !matches!(self, Self::LowDifficulty | Self::Stale | Self::Unauthorized)
    }

    pub fn from_error(e: &PoolError) -> Self {
        match e {
            PoolError::StaleJob(_) => Self::Stale,
            PoolError::DuplicateShare => Self::Duplicate,
            PoolError::LowDifficulty => Self::LowDifficulty,
            _ => Self::Invalid,
        }
    }
}

/// Record an accepted share.
///
/// `credit` is what the share is worth to the hashrate estimator (see
/// [`crate::mining::credit::ShareCredit`]); `hash_difficulty` is the actual
/// difficulty of the hash, which drives the best-share watermarks. The
/// Prometheus difficulty histogram records `credit` so it agrees with the
/// dashboard's hashrate.
pub fn record_accepted(
    stats: &PoolStats,
    session_id: &str,
    worker: &str,
    credit: u64,
    hash_difficulty: u64,
) {
    stats.add_share_diff(session_id, worker, credit as f64);
    stats.share_accepted(credit, hash_difficulty);
    stats.worker_share_accepted(worker, hash_difficulty);
    stats.mark_worker_submit(worker);
    stats.ledger_share_accepted(worker, credit);
    metrics::share_accepted(credit, worker);
}

/// Record a rejected share. Returns `true` when the session has exceeded its
/// invalid-share budget and should be disconnected.
pub fn record_rejected(
    stats: &PoolStats,
    guard: &mut SessionGuard,
    worker: &str,
    reason: RejectReason,
) -> bool {
    let label = reason.label();
    stats.share_rejected(label);
    stats.worker_share_rejected(worker, label);
    stats.ledger_share_rejected(worker);
    metrics::share_rejected(label, worker);
    reason.counts_as_invalid() && guard.invalid_shares.record_invalid()
}

/// Record a message dropped by the per-connection rate limiter.
///
/// Not a share reject in any form: the limiter fires on any inbound message —
/// authorize floods, subscribe floods — and a dropped `mining.configure` is
/// not a bad share. Counting these as rejects poisoned every reject ratio a
/// fleet operator would read, so they get their own counter and their own
/// metric, and never touch the ledger. It also takes no `SessionGuard` — a
/// pool-side defence cannot feed the invalid-share ban budget.
pub fn record_rate_limited(stats: &PoolStats, worker: Option<&str>) {
    stats.message_rate_limited();
    metrics::rate_limited(worker.unwrap_or("?"));
}

/// Record what the node did with a block we submitted.
///
/// Shared by the SV1 and SV2 session loops *and* the engine's background
/// resubmit task — three hand-written copies of this sequence is exactly how
/// the stale-tip miscount got in. Only a block that won its height is a find:
/// an `Inconclusive` block is consensus-valid but sits on a side branch and
/// earned nothing, so it must not move `pool_blocks_found_total` or the
/// dashboard's block count.
///
/// Callers log their own line — they carry protocol and retry-attempt context
/// this does not.
///
/// The verdict recorded here is provisional either way: it is what the node
/// believed at one instant, and a reorg can overturn it in both directions. The
/// block is enrolled in the confirmation ledger so `mining::confirm` can
/// reconcile it once the chain has settled.
pub fn record_block_outcome(
    stats: &PoolStats,
    outcome: BlockSubmitOutcome,
    height: u64,
    worker: &str,
    payout: &str,
    hash_hex: &str,
) {
    metrics::block_submission_outcome(outcome.label());
    if outcome.is_win() {
        metrics::block_found();
        stats.block_found();
    } else {
        stats.block_inconclusive();
    }
    stats.enroll_pending_block(height, hash_hex, worker, payout, outcome.is_win());
}

/// Record how the deferred confirmation pass decided a block.
///
/// Lives next to `record_block_outcome` for the same reason that one exists:
/// the metric and the dashboard must move together, and the way they came apart
/// last time was two hand-written copies of the sequence.
pub fn record_block_resolution(
    stats: &PoolStats,
    block: &PendingBlock,
    resolution: BlockResolution,
) -> bool {
    if !stats.resolve_block(block, resolution) {
        return false;
    }
    metrics::block_confirmation(resolution.label());
    match (block.won_at_submit, resolution) {
        (true, BlockResolution::Orphaned) => metrics::block_orphaned(),
        // A block that lost its height race and was then promoted by a reorg is
        // a find the submit-time verdict never counted.
        (false, BlockResolution::Confirmed) => metrics::block_found(),
        _ => {}
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timing_rejections_do_not_count_against_the_invalid_budget() {
        assert!(!RejectReason::LowDifficulty.counts_as_invalid());
        assert!(!RejectReason::Stale.counts_as_invalid());
        assert!(!RejectReason::Unauthorized.counts_as_invalid());
        assert!(RejectReason::Duplicate.counts_as_invalid());
        assert!(RejectReason::JobNotFound.counts_as_invalid());
        assert!(RejectReason::Invalid.counts_as_invalid());
    }

    #[test]
    fn validator_errors_map_to_their_labels() {
        assert_eq!(
            RejectReason::from_error(&PoolError::LowDifficulty).label(),
            "low_difficulty"
        );
        assert_eq!(
            RejectReason::from_error(&PoolError::StaleJob("j".into())).label(),
            "stale"
        );
        assert_eq!(
            RejectReason::from_error(&PoolError::DuplicateShare).label(),
            "duplicate"
        );
        assert_eq!(
            RejectReason::from_error(&PoolError::NotAuthorized).label(),
            "invalid"
        );
    }

    #[test]
    fn a_superseded_block_is_not_counted_as_found() {
        let stats = PoolStats::new_with_store(None);
        record_block_outcome(
            &stats,
            BlockSubmitOutcome::Inconclusive,
            800_000,
            "worker1",
            "bc1qpayout",
            "00000000000000000000deadbeef",
        );

        let snap = stats.snapshot();
        assert_eq!(snap.blocks_found, 0);
        assert_eq!(snap.blocks_inconclusive, 1);
    }

    #[test]
    fn a_winning_block_updates_the_count() {
        for outcome in [BlockSubmitOutcome::Accepted, BlockSubmitOutcome::Duplicate] {
            let stats = PoolStats::new_with_store(None);
            record_block_outcome(
                &stats,
                outcome,
                800_000,
                "worker1",
                "bc1qpayout",
                "0000cafe",
            );

            let snap = stats.snapshot();
            assert_eq!(snap.blocks_found, 1, "{outcome:?}");
            assert_eq!(snap.blocks_inconclusive, 0, "{outcome:?}");
        }
    }

    /// Every block the node stored is enrolled, whichever way the verdict went:
    /// both can be overturned by a later reorg.
    #[test]
    fn every_stored_block_is_enrolled_for_confirmation() {
        for outcome in [
            BlockSubmitOutcome::Accepted,
            BlockSubmitOutcome::Duplicate,
            BlockSubmitOutcome::Inconclusive,
        ] {
            let stats = PoolStats::new_with_store(None);
            record_block_outcome(
                &stats,
                outcome,
                800_000,
                "worker1",
                "bc1qpayout",
                "0000cafe",
            );

            let pending = stats.pending_blocks();
            assert_eq!(pending.len(), 1, "{outcome:?}");
            assert_eq!(pending[0].height, 800_000, "{outcome:?}");
            assert_eq!(pending[0].won_at_submit, outcome.is_win(), "{outcome:?}");
            assert_eq!(
                stats.snapshot().blocks_pending_confirmation,
                1,
                "{outcome:?}"
            );
        }
    }

    /// The retry ladder and the background resubmitter can both report the same
    /// block; it must not be swept — or counted — twice.
    #[test]
    fn the_same_block_is_only_enrolled_once() {
        let stats = PoolStats::new_with_store(None);
        for _ in 0..3 {
            record_block_outcome(
                &stats,
                BlockSubmitOutcome::Duplicate,
                800_000,
                "worker1",
                "bc1qpayout",
                "0000cafe",
            );
        }
        assert_eq!(stats.pending_blocks().len(), 1);
    }

    #[test]
    fn a_win_that_is_reorged_out_stops_counting_as_a_block_found() {
        let stats = PoolStats::new_with_store(None);
        record_block_outcome(
            &stats,
            BlockSubmitOutcome::Accepted,
            800_000,
            "worker1",
            "bc1qpayout",
            "0000cafe",
        );
        let block = stats.pending_blocks().pop().unwrap();

        assert!(record_block_resolution(
            &stats,
            &block,
            BlockResolution::Orphaned
        ));

        let snap = stats.snapshot();
        assert_eq!(snap.blocks_found, 0);
        assert_eq!(snap.blocks_orphaned, 1);
        assert_eq!(snap.blocks_pending_confirmation, 0);

        // The sweep snapshots the pending set, so a slow tick can overlap the
        // next one. The second report must not double-count.
        assert!(!record_block_resolution(
            &stats,
            &block,
            BlockResolution::Orphaned
        ));
        assert_eq!(stats.snapshot().blocks_orphaned, 1);
    }

    #[test]
    fn a_reorg_that_promotes_a_side_branch_block_counts_it_as_found() {
        let stats = PoolStats::new_with_store(None);
        record_block_outcome(
            &stats,
            BlockSubmitOutcome::Inconclusive,
            800_000,
            "worker1",
            "bc1qpayout",
            "0000cafe",
        );
        let block = stats.pending_blocks().pop().unwrap();

        assert!(record_block_resolution(
            &stats,
            &block,
            BlockResolution::Confirmed
        ));

        let snap = stats.snapshot();
        assert_eq!(snap.blocks_found, 1);
        assert_eq!(snap.blocks_inconclusive, 0);
        assert_eq!(snap.blocks_orphaned, 0);
    }

    /// Nothing was proved against an abandoned block, so the submit-time
    /// verdict has to stand rather than quietly reverse.
    #[test]
    fn abandoning_a_block_leaves_its_submit_time_verdict_alone() {
        let stats = PoolStats::new_with_store(None);
        record_block_outcome(
            &stats,
            BlockSubmitOutcome::Accepted,
            800_000,
            "worker1",
            "bc1qpayout",
            "0000cafe",
        );
        let block = stats.pending_blocks().pop().unwrap();

        assert!(record_block_resolution(
            &stats,
            &block,
            BlockResolution::Abandoned
        ));

        let snap = stats.snapshot();
        assert_eq!(snap.blocks_found, 1);
        assert_eq!(snap.blocks_orphaned, 0);
        assert_eq!(snap.blocks_pending_confirmation, 0);
    }

    /// The share that wins a block is the last share of the round it ended.
    /// Recorded in the frontends' order — accepted first, outcome second — the
    /// reset sweeps it away with the rest of the round. The reverse order
    /// seeded every new round with one share and an unbeatable block-level
    /// best.
    #[test]
    fn the_winning_share_belongs_to_the_round_it_ended() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("worker1", 1_000);

        record_accepted(&stats, "s1", "worker1", 1_000, 2_000);
        // The block share, credited before its outcome is recorded.
        record_accepted(&stats, "s1", "worker1", 1_000, 120_000_000_000_000);
        record_block_outcome(
            &stats,
            BlockSubmitOutcome::Accepted,
            800_000,
            "worker1",
            "bc1qpayout",
            "0000cafe",
        );

        let snap = stats.snapshot();
        assert_eq!(snap.blocks_found, 1);
        assert_eq!(snap.round_shares_accepted, 0, "round restarts empty");
        assert_eq!(snap.best_share_difficulty, 0, "round best restarts empty");
        let worker = snap
            .worker_states
            .iter()
            .find(|w| w.worker == "worker1")
            .unwrap();
        assert_eq!(worker.best_share_difficulty, 0);

        drop(stats);
    }

    /// A rate-limited message is not a share, so it must move its own counter
    /// and leave every share statistic untouched — the limiter fires on any
    /// message type, and a dropped `mining.configure` is not a bad share.
    #[test]
    fn rate_limited_messages_do_not_touch_share_accounting() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("w", 1_000);

        record_rate_limited(&stats, Some("w"));
        record_rate_limited(&stats, None);

        let snap = stats.snapshot();
        assert_eq!(snap.rate_limited_drops, 2);
        assert_eq!(snap.shares_rejected, 0);
        assert!(snap.reject_reasons.is_empty());
        let worker = snap.worker_states.iter().find(|w| w.worker == "w").unwrap();
        assert_eq!(worker.shares_rejected, 0);
        assert!(worker.reject_reasons.is_empty());
    }
}
