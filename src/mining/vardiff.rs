/// mining/vardiff.rs
///
/// Per-session variable difficulty (vardiff).
///
/// Algorithm:
///   - Count accepted shares since the last retarget
///   - At each retarget interval, compute actual share rate vs target
///   - Scale difficulty proportionally, clamped by min/max and max_factor
///   - Return the new difficulty so the caller can send `set_difficulty`
use crate::config::VardiffConfig;
use std::time::Instant;

pub struct Vardiff {
    cfg: VardiffConfig,
    last_retarget: Instant,
    /// Current difficulty assigned to this session
    pub current: u64,
    /// Number of valid shares since last retarget
    shares_since_retarget: u64,
}

impl Vardiff {
    pub fn new(cfg: VardiffConfig, initial_difficulty: u64) -> Self {
        Self {
            current: initial_difficulty,
            cfg,
            last_retarget: Instant::now(),
            shares_since_retarget: 0,
        }
    }

    /// Seed the working difficulty from a miner's `mining.suggest_difficulty`
    /// hint, clamped to the configured floor/ceiling, and give it a fresh
    /// retarget window. Returns the applied (clamped) value. Vardiff retains
    /// full authority afterwards — this only sets the starting point.
    pub fn suggest(&mut self, difficulty: u64) -> u64 {
        let clamped = difficulty.clamp(self.cfg.min_difficulty, self.cfg.max_difficulty);
        self.current = clamped;
        self.last_retarget = Instant::now();
        self.shares_since_retarget = 0;
        clamped
    }

    /// Record a valid share submission. Only the count matters here — hashrate
    /// estimation lives in `PoolStats`, which decays the assigned difficulty
    /// through `mining::hashrate::HashrateDecay`.
    pub fn record_share(&mut self) {
        self.shares_since_retarget += 1;
    }

    /// Check if a retarget is due. Returns `Some(new_difficulty)` when the
    /// difficulty should change.
    pub fn check_retarget(&mut self) -> Option<u64> {
        let elapsed = self.last_retarget.elapsed().as_secs_f64();
        let interval = self.cfg.retarget_interval_secs as f64;

        if elapsed < interval {
            return None;
        }

        let shares = self.shares_since_retarget;
        self.shares_since_retarget = 0;
        self.last_retarget = Instant::now();

        if shares == 0 {
            // No shares in this window — halve difficulty so a slow/paused miner
            // gets an easier target on reconnect, flooring at min_difficulty.
            let new_diff = (self.current / 2).max(self.cfg.min_difficulty);
            if new_diff != self.current {
                self.current = new_diff;
                return Some(new_diff);
            }
            return None;
        }

        // Actual seconds per share during this window
        let actual_sps = elapsed / shares as f64;
        let target_sps = self.cfg.target_share_time_secs as f64;

        // Scale: if shares came in too fast (actual_sps < target_sps), raise difficulty
        let ratio = target_sps / actual_sps;

        // Clamp ratio to ±max_retarget_factor
        let factor = self.cfg.max_retarget_factor;
        let clamped_ratio = ratio.clamp(1.0 / factor, factor);

        let new_diff_f = self.current as f64 * clamped_ratio;
        let new_diff = (new_diff_f as u64).clamp(self.cfg.min_difficulty, self.cfg.max_difficulty);

        // Only emit if meaningfully different (>5% change)
        let pct_change = ((new_diff as f64 - self.current as f64) / self.current as f64).abs();
        if pct_change > 0.05 && new_diff != self.current {
            tracing::debug!(
                old = self.current,
                new = new_diff,
                actual_sps = format!("{:.1}", actual_sps),
                "vardiff retarget"
            );
            self.current = new_diff;
            Some(new_diff)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn cfg() -> VardiffConfig {
        VardiffConfig {
            target_share_time_secs: 15,
            retarget_interval_secs: 60,
            min_difficulty: 1024,
            max_difficulty: 1_000_000_000,
            max_retarget_factor: 4.0,
        }
    }

    #[test]
    fn no_retarget_before_interval() {
        let mut vd = Vardiff::new(cfg(), 100_000);
        for _ in 0..10 {
            vd.record_share();
        }
        // No retarget should happen immediately
        assert!(vd.check_retarget().is_none());
    }

    #[test]
    fn zero_shares_halves_difficulty() {
        let mut vd = Vardiff::new(cfg(), 100_000);
        // Force the last retarget to be far in the past
        vd.last_retarget = Instant::now() - Duration::from_secs(120);
        let result = vd.check_retarget();
        assert_eq!(result, Some(50_000));
    }

    #[test]
    fn suggest_clamps_to_floor_and_ceiling() {
        // cfg(): floor 1024, ceiling 1_000_000_000.
        let mut vd = Vardiff::new(cfg(), 100_000);
        // Below floor → clamped up to the floor (a hostile/buggy suggestion can
        // never push a miner below the share-rate floor).
        assert_eq!(vd.suggest(1), 1024);
        assert_eq!(vd.current, 1024);
        // Above ceiling → clamped down.
        assert_eq!(vd.suggest(5_000_000_000), 1_000_000_000);
        // In range → applied verbatim.
        assert_eq!(vd.suggest(50_000), 50_000);
        assert_eq!(vd.current, 50_000);
    }
}
