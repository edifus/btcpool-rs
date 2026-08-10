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
/// and while that holds is credited at the floor.
///
/// A share is judged against the smaller of the session's current difficulty
/// and the difficulty stamped on the job it solves: after a raise, work in
/// flight legitimately sits below the new value; after a cut a miner applies
/// mid-job, below the job's stamp. Neither is evidence of anything.
///
/// The verdict is not permanent. A flagged session that produces a run of
/// shares at or above its effective difficulty is re-credited in full — a
/// genuinely floor-pinned miner cannot produce such a run once vardiff sits
/// above the floor, while a session flagged in error produces nothing else.
use std::time::{Duration, Instant};

/// Shares below the effective difficulty needed before a session is judged to
/// be ignoring `set_difficulty`. More than one, so a single straggler cannot
/// flip the verdict.
const EVIDENCE_SHARES: u32 = 3;

/// Consecutive shares at or above the effective difficulty that clear the
/// verdict again. Per share the chance a floor-pinned miner clears an assigned
/// difficulty `a` is `floor/a`, so a run of this length is unreachable for one
/// while being the only thing an honouring session produces.
const REDEEM_SHARES: u32 = 20;

/// Grace period after a difficulty change during which low shares prove
/// nothing: a well-behaved miner still has work queued at the old target.
const RETARGET_GRACE: Duration = Duration::from_secs(30);

/// A share must fall this far below the effective difficulty to count as
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
    /// Shares seen below the effective difficulty since that change.
    below_assigned: u32,
    /// Consecutive shares at or above the effective difficulty while flagged.
    redeeming: u32,
    /// False while the session is judged to be ignoring `set_difficulty`.
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
            redeeming: 0,
            honors_assigned: true,
        }
    }

    /// Difficulty to credit for one accepted share of `hash_difficulty` actual
    /// work, where `assigned` is the session's current vardiff and
    /// `job_difficulty` the vardiff stamped on the job the share solves.
    /// Changes to `assigned` are detected here, so callers do not have to
    /// notify every `set_difficulty` / `SetTarget` send. The first share of a
    /// session therefore establishes the assigned difficulty and opens the
    /// first grace window, which is why a session is never judged on its
    /// opening share.
    pub fn credit(
        &mut self,
        assigned: u64,
        job_difficulty: u64,
        hash_difficulty: u64,
        now: Instant,
    ) -> u64 {
        if assigned != self.assigned {
            self.assigned = assigned;
            self.assigned_at = now;
            self.below_assigned = 0;
        }

        // The difficulty this share actually had to clear: the job's stamp
        // bounds it after a raise the miner has not applied yet, the current
        // assignment after a cut it applied mid-job.
        let effective = self.assigned.min(job_difficulty).max(self.floor);
        let meets_effective = hash_difficulty as f64 >= effective as f64 * BELOW_TOLERANCE;

        if self.honors_assigned {
            if !meets_effective && now.saturating_duration_since(self.assigned_at) > RETARGET_GRACE
            {
                self.below_assigned += 1;
                if self.below_assigned >= EVIDENCE_SHARES {
                    self.honors_assigned = false;
                    self.redeeming = 0;
                }
            }
        } else if meets_effective {
            self.redeeming += 1;
            if self.redeeming >= REDEEM_SHARES {
                self.honors_assigned = true;
                self.below_assigned = 0;
                // The redeeming run says nothing about work still queued from
                // before it, so it reopens the grace window too.
                self.assigned_at = now;
            }
        } else {
            self.redeeming = 0;
        }

        if self.honors_assigned {
            effective
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
        assert_eq!(credit.credit(ASSIGNED, ASSIGNED, ASSIGNED, start), ASSIGNED);
        (credit, past_grace(start))
    }

    /// Flag a settled tracker with floor shares on current jobs.
    fn flagged(start: Instant) -> (ShareCredit, Instant) {
        let (mut credit, now) = settled(start);
        for _ in 0..EVIDENCE_SHARES {
            credit.credit(ASSIGNED, ASSIGNED, FLOOR, now);
        }
        assert!(!credit.honors_assigned());
        (credit, now)
    }

    #[test]
    fn miner_honouring_set_difficulty_is_credited_its_assigned_difficulty() {
        let (mut credit, now) = settled(Instant::now());
        for _ in 0..10 {
            assert_eq!(credit.credit(ASSIGNED, ASSIGNED, 70_000, now), ASSIGNED);
        }
        assert!(credit.honors_assigned());
    }

    #[test]
    fn miner_pinned_to_the_floor_falls_back_to_it_after_enough_evidence() {
        let (mut credit, now) = settled(Instant::now());

        // Shares keep arriving at the floor while vardiff sits far above it.
        for _ in 0..(EVIDENCE_SHARES - 1) {
            assert_eq!(credit.credit(ASSIGNED, ASSIGNED, FLOOR, now), ASSIGNED);
        }
        assert_eq!(credit.credit(ASSIGNED, ASSIGNED, FLOOR, now), FLOOR);
        assert!(!credit.honors_assigned());
        // One large share does not undo the verdict — that takes a run.
        assert_eq!(credit.credit(ASSIGNED, ASSIGNED, 1_000_000, now), FLOOR);
    }

    #[test]
    fn shares_inside_the_grace_window_are_not_evidence() {
        let start = Instant::now();
        let mut credit = ShareCredit::new(FLOOR, start);
        // In-flight work at the old target, arriving right after the raise.
        for _ in 0..(EVIDENCE_SHARES * 3) {
            assert_eq!(credit.credit(ASSIGNED, ASSIGNED, FLOOR, start), ASSIGNED);
        }
        assert!(credit.honors_assigned());
    }

    #[test]
    fn a_retarget_restarts_the_grace_period() {
        let (mut credit, now) = settled(Instant::now());

        // Low shares, one short of the verdict.
        for _ in 0..(EVIDENCE_SHARES - 1) {
            assert_eq!(credit.credit(ASSIGNED, ASSIGNED, FLOOR, now), ASSIGNED);
        }
        // Vardiff moves: the counter resets and the grace window reopens, so
        // stragglers from before the change cannot combine with new ones.
        for _ in 0..(EVIDENCE_SHARES * 2) {
            assert_eq!(credit.credit(32_768, 32_768, FLOOR, now), 32_768);
        }
        assert!(credit.honors_assigned());

        // Past the new grace window the count starts again from zero.
        let later = past_grace(now);
        for _ in 0..(EVIDENCE_SHARES - 1) {
            assert_eq!(credit.credit(32_768, 32_768, FLOOR, later), 32_768);
        }
        assert_eq!(credit.credit(32_768, 32_768, FLOOR, later), FLOOR);
    }

    #[test]
    fn a_share_landing_exactly_on_target_is_not_read_as_below_it() {
        let (mut credit, now) = settled(Instant::now());
        // hash_to_difficulty truncates: a share on the nose can read one under.
        for _ in 0..(EVIDENCE_SHARES * 2) {
            assert_eq!(
                credit.credit(ASSIGNED, ASSIGNED, ASSIGNED - 1, now),
                ASSIGNED
            );
        }
        assert!(credit.honors_assigned());
    }

    #[test]
    fn credit_never_drops_below_the_floor() {
        let start = Instant::now();
        let mut credit = ShareCredit::new(FLOOR, start);
        // A vardiff below the configured floor should never under-credit.
        assert_eq!(credit.credit(1, 1, 10_000, start), FLOOR);
    }

    /// After a raise the miner has not applied yet, shares from jobs stamped
    /// at the old difficulty are judged against that stamp — however long they
    /// keep coming — and credited at it, not at the raised value.
    #[test]
    fn in_flight_work_after_a_raise_is_judged_against_its_job() {
        let (mut credit, now) = settled(Instant::now());
        let raised = ASSIGNED * 2;
        // First share registers the change and opens the grace window…
        assert_eq!(credit.credit(raised, ASSIGNED, ASSIGNED, now), ASSIGNED);
        // …and shares far past it still prove nothing.
        let later = past_grace(now);
        for _ in 0..(EVIDENCE_SHARES * 5) {
            assert_eq!(credit.credit(raised, ASSIGNED, ASSIGNED, later), ASSIGNED);
        }
        assert!(credit.honors_assigned());
    }

    /// ESP-Miner applies a cut to the job it is already working: shares then
    /// land below the job's stamp but at the new assignment, which is exactly
    /// what honouring the cut looks like.
    #[test]
    fn a_cut_applied_mid_job_is_not_evidence() {
        let (mut credit, now) = settled(Instant::now());
        let cut = ASSIGNED / 2;
        assert_eq!(credit.credit(cut, ASSIGNED, cut, now), cut);
        let later = past_grace(now);
        for _ in 0..(EVIDENCE_SHARES * 5) {
            assert_eq!(credit.credit(cut, ASSIGNED, cut, later), cut);
        }
        assert!(credit.honors_assigned());
    }

    /// A session flagged in error produces nothing but shares at its assigned
    /// difficulty; a run of them restores full credit instead of leaving the
    /// session at the floor for the rest of the connection.
    #[test]
    fn a_flagged_session_redeems_itself_with_a_run_of_high_shares() {
        let (mut credit, now) = flagged(Instant::now());

        for _ in 0..REDEEM_SHARES {
            credit.credit(ASSIGNED, ASSIGNED, ASSIGNED, now);
        }
        assert!(credit.honors_assigned());
        assert_eq!(credit.credit(ASSIGNED, ASSIGNED, ASSIGNED, now), ASSIGNED);
    }

    #[test]
    fn a_low_share_breaks_the_redeeming_run() {
        let (mut credit, now) = flagged(Instant::now());

        for _ in 0..(REDEEM_SHARES - 1) {
            assert_eq!(credit.credit(ASSIGNED, ASSIGNED, ASSIGNED, now), FLOOR);
        }
        credit.credit(ASSIGNED, ASSIGNED, FLOOR, now);
        for _ in 0..(REDEEM_SHARES - 1) {
            assert_eq!(credit.credit(ASSIGNED, ASSIGNED, ASSIGNED, now), FLOOR);
        }
        assert!(!credit.honors_assigned());
    }

    /// A floor-pinned miner's occasional lucky high share cannot assemble the
    /// consecutive run redemption requires.
    #[test]
    fn a_floor_pinned_miner_stays_at_the_floor_through_lucky_shares() {
        let (mut credit, now) = flagged(Instant::now());

        for _ in 0..100 {
            for _ in 0..4 {
                assert_eq!(credit.credit(ASSIGNED, ASSIGNED, FLOOR, now), FLOOR);
            }
            assert_eq!(credit.credit(ASSIGNED, ASSIGNED, ASSIGNED * 2, now), FLOOR);
        }
        assert!(!credit.honors_assigned());
    }
}
