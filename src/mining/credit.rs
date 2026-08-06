/// mining/credit.rs
///
/// How much difficulty one accepted share is worth to the hashrate estimator.
///
/// The pool validates shares against the *floor* (`vardiff.min_difficulty`)
/// rather than the session's current vardiff, because cgminer/Avalon firmware
/// pins its hardware submission threshold to the `minimum-difficulty` sent at
/// `mining.configure` time and never raises it. Crediting such a session at its
/// vardiff level would inflate its hashrate by `vardiff / floor`: shares arrive
/// at the rate the *miner's* threshold implies, so that threshold — not the one
/// we asked for — is the unbiased credit.
///
/// Firmware that honours `mining.set_difficulty` (ESP-Miner, SV2 devices, most
/// cgminer builds) must still be credited at its assigned difficulty, or its
/// hashrate reads low by the same ratio. The pool cannot know which kind it is
/// talking to up front, so it watches: a session that keeps submitting shares
/// below the difficulty it was told is demonstrably ignoring `set_difficulty`,
/// and from then on is credited at the floor.
use std::time::{Duration, Instant};

/// Shares below the assigned difficulty needed before a session is judged to be
/// ignoring `set_difficulty`. More than one, so a single straggler cannot flip
/// the verdict.
const EVIDENCE_SHARES: u32 = 3;

/// Grace period after a difficulty change during which low shares prove
/// nothing: a well-behaved miner still has work queued at the old target.
const RETARGET_GRACE: Duration = Duration::from_secs(30);

/// A share must fall this far below the assigned difficulty to count as
/// evidence. `hash_to_difficulty` truncates, so a share landing exactly on the
/// target can read a hair under it.
const BELOW_TOLERANCE: f64 = 0.99;

/// Per-session decision of what an accepted share is worth.
#[derive(Debug, Clone)]
pub struct ShareCredit {
    /// Acceptance threshold every share is validated against.
    floor: u64,
    /// Difficulty most recently advertised to the miner.
    assigned: u64,
    /// When `assigned` last changed.
    assigned_at: Instant,
    /// Shares seen below `assigned` since that change.
    below_assigned: u32,
    /// False once the session has proven it ignores `set_difficulty`. Sticky:
    /// firmware does not start honouring it mid-connection.
    honors_assigned: bool,
}

impl ShareCredit {
    pub fn new(floor: u64, now: Instant) -> Self {
        Self {
            floor: floor.max(1),
            // Zero so the first credited share registers as a change and starts
            // the grace period, rather than judging a session on its opening
            // shares.
            assigned: 0,
            assigned_at: now,
            below_assigned: 0,
            honors_assigned: true,
        }
    }

    /// Difficulty to credit for one accepted share of `hash_difficulty` actual
    /// work, where `assigned` is the session's current vardiff. Changes to
    /// `assigned` are detected here, so callers do not have to notify every
    /// `set_difficulty` / `SetTarget` send. The first share of a session
    /// therefore establishes the assigned difficulty and opens the first grace
    /// window, which is why a session is never judged on its opening share.
    pub fn credit(&mut self, assigned: u64, hash_difficulty: u64, now: Instant) -> u64 {
        if assigned != self.assigned {
            self.assigned = assigned;
            self.assigned_at = now;
            self.below_assigned = 0;
        }

        if self.honors_assigned
            && now.saturating_duration_since(self.assigned_at) > RETARGET_GRACE
            && (hash_difficulty as f64) < self.assigned as f64 * BELOW_TOLERANCE
        {
            self.below_assigned += 1;
            if self.below_assigned >= EVIDENCE_SHARES {
                self.honors_assigned = false;
            }
        }

        if self.honors_assigned {
            self.assigned.max(self.floor)
        } else {
            self.floor
        }
    }

    /// Whether this session is still believed to honour `set_difficulty`.
    /// Exposed for logging and the disconnect summary.
    pub fn honors_assigned(&self) -> bool {
        self.honors_assigned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLOOR: u64 = 4_096;
    const ASSIGNED: u64 = 65_536;

    fn past_grace(from: Instant) -> Instant {
        from + RETARGET_GRACE + Duration::from_secs(1)
    }

    /// Take a fresh tracker past the point where evidence starts counting: the
    /// first credited share establishes the assigned difficulty and opens the
    /// grace window, exactly as a real session's first share does.
    fn settled(start: Instant) -> (ShareCredit, Instant) {
        let mut credit = ShareCredit::new(FLOOR, start);
        assert_eq!(credit.credit(ASSIGNED, ASSIGNED, start), ASSIGNED);
        (credit, past_grace(start))
    }

    #[test]
    fn miner_honouring_set_difficulty_is_credited_its_assigned_difficulty() {
        let (mut credit, now) = settled(Instant::now());
        for _ in 0..10 {
            assert_eq!(credit.credit(ASSIGNED, 70_000, now), ASSIGNED);
        }
        assert!(credit.honors_assigned());
    }

    #[test]
    fn miner_pinned_to_the_floor_falls_back_to_it_after_enough_evidence() {
        let (mut credit, now) = settled(Instant::now());

        // Shares keep arriving at the floor while vardiff sits far above it.
        for _ in 0..(EVIDENCE_SHARES - 1) {
            assert_eq!(credit.credit(ASSIGNED, FLOOR, now), ASSIGNED);
        }
        assert_eq!(credit.credit(ASSIGNED, FLOOR, now), FLOOR);
        assert!(!credit.honors_assigned());
        // Sticky: a later share that happens to be large does not undo it.
        assert_eq!(credit.credit(ASSIGNED, 1_000_000, now), FLOOR);
    }

    #[test]
    fn shares_inside_the_grace_window_are_not_evidence() {
        let start = Instant::now();
        let mut credit = ShareCredit::new(FLOOR, start);
        // In-flight work at the old target, arriving right after the raise.
        for _ in 0..(EVIDENCE_SHARES * 3) {
            assert_eq!(credit.credit(ASSIGNED, FLOOR, start), ASSIGNED);
        }
        assert!(credit.honors_assigned());
    }

    #[test]
    fn a_retarget_restarts_the_grace_period() {
        let (mut credit, now) = settled(Instant::now());

        // Low shares, one short of the verdict.
        for _ in 0..(EVIDENCE_SHARES - 1) {
            assert_eq!(credit.credit(ASSIGNED, FLOOR, now), ASSIGNED);
        }
        // Vardiff moves: the counter resets and the grace window reopens, so
        // stragglers from before the change cannot combine with new ones.
        for _ in 0..(EVIDENCE_SHARES * 2) {
            assert_eq!(credit.credit(32_768, FLOOR, now), 32_768);
        }
        assert!(credit.honors_assigned());

        // Past the new grace window the count starts again from zero.
        let later = past_grace(now);
        for _ in 0..(EVIDENCE_SHARES - 1) {
            assert_eq!(credit.credit(32_768, FLOOR, later), 32_768);
        }
        assert_eq!(credit.credit(32_768, FLOOR, later), FLOOR);
    }

    #[test]
    fn a_share_landing_exactly_on_target_is_not_read_as_below_it() {
        let (mut credit, now) = settled(Instant::now());
        // hash_to_difficulty truncates: a share on the nose can read one under.
        for _ in 0..(EVIDENCE_SHARES * 2) {
            assert_eq!(credit.credit(ASSIGNED, ASSIGNED - 1, now), ASSIGNED);
        }
        assert!(credit.honors_assigned());
    }

    #[test]
    fn credit_never_drops_below_the_floor() {
        let start = Instant::now();
        let mut credit = ShareCredit::new(FLOOR, start);
        // A vardiff below the configured floor should never under-credit.
        assert_eq!(credit.credit(1, 10_000, start), FLOOR);
    }
}
