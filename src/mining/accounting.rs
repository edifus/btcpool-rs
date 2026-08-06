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
use crate::{error::PoolError, metrics, security::SessionGuard, stats::PoolStats};

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
    stats.share_accepted(hash_difficulty);
    stats.worker_share_accepted(worker, hash_difficulty);
    stats.mark_worker_submit(worker);
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
    stats.share_rejected();
    stats.worker_share_rejected(worker, label);
    metrics::share_rejected(label, worker);
    reason.counts_as_invalid() && guard.invalid_shares.record_invalid()
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
}
