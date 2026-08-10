/// mining/vardiff.rs
///
/// Per-session variable difficulty (vardiff).
///
/// Ported from ckpool's `add_submit()` in `src/stratifier.c`, which keeps a
/// miner's difficulty still for hours at a time. Three mechanisms do that work:
///
///   - **Difficulty-weighted decaying averages** rather than a share count over
///     a fixed window. A window holding `n` shares carries `1/sqrt(n)` relative
///     noise, and a controller that jumps straight to a noisy estimate emits
///     that noise verbatim. The averages here are fed the credited difficulty
///     of every share and survive across retargets, so a difficulty change does
///     not throw the measurement away.
///   - **A warm-up correction** ([`time_bias`]) so an average seeded at zero
///     still reads true within seconds of a session's first share.
///   - **A multiplicative deadzone**: the newly computed optimum has to differ
///     from the assigned difficulty by a factor, not a few percent, before
///     anything is sent.
///
/// The estimate is built from what a share is *credited*
/// ([`crate::mining::credit::ShareCredit`]), not from how many arrived. Shares
/// are validated against the configured floor rather than the assigned
/// difficulty, so firmware that ignores `set_difficulty` submits at a rate that
/// does not respond to a raise at all; counting those shares would leave the
/// loop open and ratchet such a session to `max_difficulty`. Credit is
/// difficulty per second either way, which makes it a hashrate estimate that
/// the assigned difficulty cannot bias.
use crate::config::VardiffConfig;
use crate::mining::hashrate::{decay_time, sane_tdiff, time_bias};
use std::time::Instant;

/// Time constants of the two rate averages, as multiples of the target share
/// time. The multiple is the invariant, not the number of seconds: estimator
/// noise is `1/sqrt(2 * tau / target)`, so a window pinned to wall-clock seconds
/// gets noisy the moment the target share time is raised. At ckpool's 3.33 s
/// target these reproduce its own 60 s and 300 s windows.
const FAST_WINDOW_MULTIPLE: f64 = 20.0;
const SLOW_WINDOW_MULTIPLE: f64 = 120.0;

pub struct Vardiff {
    cfg: VardiffConfig,
    /// Current difficulty assigned to this session
    pub current: u64,
    /// Difficulty-shares per second over the fast window.
    fast: f64,
    /// Difficulty-shares per second over the slow window.
    slow: f64,
    fast_secs: f64,
    slow_secs: f64,
    /// When the averages were last folded forward.
    last_decay: Instant,
    /// First share of the session — anchors the warm-up correction.
    first_share: Option<Instant>,
    /// When the difficulty last changed.
    last_change: Instant,
    /// Shares credited since that change.
    shares_since_change: u64,
    /// Shares a miner hitting the target would submit in one retarget interval.
    /// Reaching it is the early trigger for a miner running faster than target.
    retarget_shares: u64,
}

impl Vardiff {
    pub fn new(cfg: VardiffConfig, initial_difficulty: u64, now: Instant) -> Self {
        let target = cfg.target_share_time_secs.max(1) as f64;
        let retarget_shares = (cfg.retarget_interval_secs as f64 / target)
            .round()
            .max(4.0) as u64;
        Self {
            current: initial_difficulty,
            fast: 0.0,
            slow: 0.0,
            fast_secs: FAST_WINDOW_MULTIPLE * target,
            slow_secs: SLOW_WINDOW_MULTIPLE * target,
            last_decay: now,
            first_share: None,
            last_change: now,
            shares_since_change: 0,
            retarget_shares,
            cfg,
        }
    }

    /// Seed the working difficulty from a miner's `mining.suggest_difficulty`
    /// hint, clamped to the configured floor/ceiling, and give it a fresh
    /// retarget window. Returns the applied (clamped) value. Vardiff retains
    /// full authority afterwards — this only sets the starting point.
    pub fn suggest(&mut self, difficulty: u64, now: Instant) -> u64 {
        let clamped = difficulty.clamp(self.cfg.min_difficulty, self.cfg.max_difficulty);
        self.current = clamped;
        self.last_change = now;
        self.shares_since_change = 0;
        clamped
    }

    /// Record an accepted share worth `credit` difficulty.
    pub fn record_share(&mut self, credit: u64, now: Instant) {
        self.first_share.get_or_insert(now);
        self.decay(credit as f64, now);
        self.shares_since_change += 1;
    }

    /// Fold `credit` and the time since the last fold into both averages.
    /// Passing zero decays them, which is how a session that stopped submitting
    /// falls back toward an easier target instead of freezing at its last
    /// reading.
    fn decay(&mut self, credit: f64, now: Instant) {
        let tdiff = sane_tdiff(now, self.last_decay);
        self.last_decay = now;
        decay_time(&mut self.fast, credit, tdiff, self.fast_secs);
        decay_time(&mut self.slow, credit, tdiff, self.slow_secs);
    }

    /// Check if a retarget is due. Returns `Some(new_difficulty)` when the
    /// difficulty should change.
    ///
    /// `network_difficulty` is the difficulty of the block currently being
    /// worked on, when one is known. A miner is never asked for a share harder
    /// than a block: past that point the target no longer measures anything.
    pub fn check_retarget(&mut self, now: Instant, network_difficulty: Option<u64>) -> Option<u64> {
        self.decay(0.0, now);

        // A session that has never submitted has nothing to estimate from.
        let first_share = self.first_share?;

        let since_change = now
            .saturating_duration_since(self.last_change)
            .as_secs_f64();
        let hit_share_trigger = self.shares_since_change >= self.retarget_shares;
        if !hit_share_trigger && since_change < self.cfg.retarget_interval_secs as f64 {
            return None;
        }

        // Reaching the share count means the miner is running faster than
        // target, so react on the fast average. Reaching the interval instead
        // means it is slower than target, where stability is worth more than
        // speed.
        let (rate, window) = if hit_share_trigger {
            (self.fast, self.fast_secs)
        } else {
            (self.slow, self.slow_secs)
        };

        let age = sane_tdiff(now, first_share);
        let dsps = rate / time_bias(age, window);
        let optimal = dsps * self.cfg.target_share_time_secs as f64;

        // Deadzone: leave the difficulty alone while the optimum is within a
        // factor of it. This is the whole reason a settled session stops
        // emitting — the estimate still wanders, the output does not.
        let ratio = optimal / self.current as f64;
        if ratio > self.cfg.deadzone_low && ratio < self.cfg.deadzone_high {
            self.open_window(now);
            return None;
        }

        // The floor outranks the network clamp: on regtest, and on testnet
        // during a difficulty reset, a block can be easier to find than the
        // configured minimum share, and an inverted range would panic in
        // `clamp`.
        let ceiling = match network_difficulty {
            Some(network) => self.cfg.max_difficulty.min(network),
            None => self.cfg.max_difficulty,
        }
        .max(self.cfg.min_difficulty);
        let factor = self.cfg.max_retarget_factor;
        let new_diff = optimal
            .clamp(self.current as f64 / factor, self.current as f64 * factor)
            .clamp(self.cfg.min_difficulty as f64, ceiling as f64)
            .round() as u64;

        // A miner returning from a leave of absence has a decayed average that
        // says nothing about its real rate. Let the first share after a change
        // reset the clock instead of pulling the difficulty down under it.
        if new_diff < self.current && self.shares_since_change <= 1 {
            self.last_change = now;
            return None;
        }

        if new_diff == self.current {
            self.open_window(now);
            return None;
        }

        tracing::debug!(
            old = self.current,
            new = new_diff,
            dsps = format!("{dsps:.1}"),
            window_secs = window,
            "vardiff retarget"
        );
        self.current = new_diff;
        self.open_window(now);
        Some(new_diff)
    }

    /// Start a fresh retarget window without touching the difficulty.
    fn open_window(&mut self, now: Instant) {
        self.last_change = now;
        self.shares_since_change = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const NONCES: f64 = 4_294_967_296.0;
    const TARGET: u64 = 5;
    const FLOOR: u64 = 256;
    const CEILING: u64 = 4_000_000;
    const START: u64 = 2_048;

    fn cfg() -> VardiffConfig {
        VardiffConfig {
            target_share_time_secs: TARGET,
            retarget_interval_secs: 100,
            min_difficulty: FLOOR,
            max_difficulty: CEILING,
            max_retarget_factor: 10.0,
            deadzone_low: 0.667,
            deadzone_high: 1.5,
        }
    }

    /// Difficulty a miner of `hashrate` H/s settles at under the target.
    fn optimal_for(hashrate: f64) -> f64 {
        hashrate / NONCES * TARGET as f64
    }

    /// A deterministic stand-in for exponential inter-arrival times. Real share
    /// gaps are Poisson; a fixed seed keeps the tests reproducible while still
    /// feeding the estimator the long/short gaps it has to cope with.
    struct Arrivals(u64);

    impl Arrivals {
        fn new() -> Self {
            Self(0x2545_F491_4F6C_DD1D)
        }

        /// Next gap, exponentially distributed with mean `mean_secs`.
        fn next_gap(&mut self, mean_secs: f64) -> Duration {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            let uniform = (self.0 >> 11) as f64 / (1u64 << 53) as f64;
            let gap = -mean_secs * (1.0 - uniform).max(1e-12).ln();
            Duration::from_secs_f64(gap.clamp(1e-4, mean_secs * 20.0))
        }
    }

    /// Drive `vd` with a miner of `hashrate` H/s for `secs`, honouring every
    /// difficulty it is handed. Returns the difficulties emitted, with the time
    /// each was sent.
    fn mine(
        vd: &mut Vardiff,
        start: Instant,
        hashrate: f64,
        secs: u64,
        honor: bool,
    ) -> Vec<(f64, u64)> {
        let mut arrivals = Arrivals::new();
        let mut now = start;
        let deadline = start + Duration::from_secs(secs);
        let mut changes = Vec::new();
        while now < deadline {
            // The difficulty the hardware actually submits at, and so both the
            // credit and the arrival rate.
            let submitted = if honor { vd.current } else { FLOOR };
            now += arrivals.next_gap(submitted as f64 * NONCES / hashrate);
            vd.record_share(submitted, now);
            if let Some(new_diff) = vd.check_retarget(now, None) {
                changes.push((now.duration_since(start).as_secs_f64(), new_diff));
            }
        }
        changes
    }

    /// A miner starting far from its optimum reaches it within a couple of
    /// minutes, rather than the tens of minutes an uncorrected average takes.
    #[test]
    fn converges_from_the_initial_difficulty() {
        let start = Instant::now();
        let mut vd = Vardiff::new(cfg(), START, start);
        let hashrate = 6.49e12;

        let changes = mine(&mut vd, start, hashrate, 120, true);

        let optimal = optimal_for(hashrate);
        assert!(
            !changes.is_empty(),
            "no retarget at all in two minutes from {START}"
        );
        let ratio = vd.current as f64 / optimal;
        assert!(
            (0.6..1.7).contains(&ratio),
            "settled at {} against an optimum of {optimal:.0} ({ratio:.2}x): {changes:?}",
            vd.current
        );
    }

    /// The regression test for the reported bug: a session at its optimum stops
    /// emitting. The old fixed-window controller changed difficulty roughly 45
    /// times an hour here, spanning a factor of 15.
    #[test]
    fn a_settled_session_stops_retargeting() {
        let start = Instant::now();
        let hashrate = 6.49e12;
        let mut vd = Vardiff::new(cfg(), optimal_for(hashrate).round() as u64, start);

        // Warm the averages up at the optimum, then measure an hour.
        mine(&mut vd, start, hashrate, 600, true);
        let settled = vd.current;
        let changes = mine(
            &mut vd,
            start + Duration::from_secs(600),
            hashrate,
            3_600,
            true,
        );

        assert!(
            changes.len() <= 1,
            "{} retargets in a steady hour: {changes:?}",
            changes.len()
        );
        for (_, diff) in &changes {
            let ratio = *diff as f64 / settled as f64;
            assert!(
                (0.5..2.0).contains(&ratio),
                "retarget to {diff} from a settled {settled}"
            );
        }
    }

    /// An estimate inside the deadzone produces nothing, however many shares
    /// arrive. This is the "a few numbers off" case the old 5% gate let through.
    #[test]
    fn the_deadzone_suppresses_a_small_error() {
        let start = Instant::now();
        // 1.2x the assigned difficulty: inside [0.667, 1.5).
        let hashrate = 1.2 * 8_192.0 * NONCES / TARGET as f64;
        let mut vd = Vardiff::new(cfg(), 8_192, start);

        mine(&mut vd, start, hashrate, 900, true);
        let settled = vd.current;
        let changes = mine(
            &mut vd,
            start + Duration::from_secs(900),
            hashrate,
            1_800,
            true,
        );

        assert!(
            changes.is_empty(),
            "retargeted {} times for a 1.2x error: {changes:?}",
            changes.len()
        );
        assert_eq!(vd.current, settled);
    }

    /// Outside the band it does move, in both directions.
    #[test]
    fn the_deadzone_releases_outside_the_band() {
        for multiple in [2.5_f64, 0.4] {
            let start = Instant::now();
            let hashrate = multiple * 8_192.0 * NONCES / TARGET as f64;
            let mut vd = Vardiff::new(cfg(), 8_192, start);

            let changes = mine(&mut vd, start, hashrate, 900, true);

            assert!(
                !changes.is_empty(),
                "no retarget for a {multiple}x error, stuck at {}",
                vd.current
            );
            if multiple > 1.0 {
                assert!(vd.current > 8_192, "difficulty fell for a fast miner");
            } else {
                assert!(vd.current < 8_192, "difficulty rose for a slow miner");
            }
        }
    }

    /// A session that goes quiet must not be cut repeatedly, and the single
    /// share that ends the silence must not be read as a rate.
    #[test]
    fn a_silent_session_is_not_repeatedly_halved() {
        let start = Instant::now();
        let hashrate = 6.49e12;
        let mut vd = Vardiff::new(cfg(), optimal_for(hashrate).round() as u64, start);
        mine(&mut vd, start, hashrate, 600, true);
        let settled = vd.current;

        // Ten minutes of silence, polled the way the session loop polls.
        let mut now = start + Duration::from_secs(600);
        let mut cuts = 0;
        for _ in 0..20 {
            now += Duration::from_secs(30);
            if vd.check_retarget(now, None).is_some() {
                cuts += 1;
            }
        }
        assert_eq!(cuts, 0, "silence alone moved the difficulty {cuts} times");
        assert_eq!(vd.current, settled);

        // The first share back cannot pull it down on its own.
        now += Duration::from_secs(30);
        vd.record_share(settled, now);
        assert_eq!(vd.check_retarget(now, None), None);
        assert_eq!(vd.current, settled);
    }

    /// Firmware pinned to the acceptance floor submits at a rate that ignores
    /// every raise. Counting those shares leaves the loop open and walks the
    /// difficulty to the ceiling; crediting them at the floor does not.
    #[test]
    fn a_miner_pinned_to_the_floor_does_not_ratchet_to_the_ceiling() {
        let start = Instant::now();
        let hashrate = 6.49e12;
        let mut vd = Vardiff::new(cfg(), START, start);

        mine(&mut vd, start, hashrate, 3_600, false);

        assert!(
            vd.current < CEILING,
            "walked to the ceiling at {}",
            vd.current
        );
        let ratio = vd.current as f64 / optimal_for(hashrate);
        assert!(
            (0.6..1.7).contains(&ratio),
            "settled at {} against an optimum of {:.0}",
            vd.current,
            optimal_for(hashrate)
        );
    }

    /// The smallest Bitaxe must land above the floor, not sit on it — the whole
    /// point of the floor being 256 rather than 4096.
    #[test]
    fn a_small_bitaxe_is_not_pinned_to_the_floor() {
        let start = Instant::now();
        // BM1397-class, ~0.4 TH/s.
        let hashrate = 0.40e12;
        let mut vd = Vardiff::new(cfg(), START, start);

        mine(&mut vd, start, hashrate, 1_800, true);

        assert!(
            vd.current > FLOOR,
            "a {:.2} TH/s miner sat on the floor at {}",
            hashrate / 1e12,
            vd.current
        );
        let ratio = vd.current as f64 / optimal_for(hashrate);
        assert!(
            (0.6..1.7).contains(&ratio),
            "settled at {} against an optimum of {:.0}",
            vd.current,
            optimal_for(hashrate)
        );
    }

    /// A share is never made harder to find than a block.
    #[test]
    fn a_miner_is_never_asked_for_more_than_the_network_difficulty() {
        let start = Instant::now();
        let network = 100_000_u64;
        let mut vd = Vardiff::new(cfg(), START, start);

        // Far more hashrate than the ceiling allows for.
        let mut arrivals = Arrivals::new();
        let mut now = start;
        for _ in 0..2_000 {
            now += arrivals.next_gap(vd.current as f64 * NONCES / 1.0e15);
            vd.record_share(vd.current, now);
            vd.check_retarget(now, Some(network));
            assert!(
                vd.current <= network,
                "assigned {} against a network difficulty of {network}",
                vd.current
            );
        }
        assert_eq!(vd.current, network, "never reached the network clamp");
    }

    /// On regtest, and on testnet during a difficulty reset, a block can be
    /// easier to find than the configured floor. The floor still wins — an
    /// inverted range would panic inside `clamp`.
    #[test]
    fn a_network_difficulty_below_the_floor_does_not_invert_the_clamp() {
        let start = Instant::now();
        let mut vd = Vardiff::new(cfg(), START, start);
        let hashrate = 6.49e12;

        let mut arrivals = Arrivals::new();
        let mut now = start;
        for _ in 0..500 {
            now += arrivals.next_gap(vd.current as f64 * NONCES / hashrate);
            vd.record_share(vd.current, now);
            vd.check_retarget(now, Some(1));
        }
        assert_eq!(vd.current, FLOOR);
    }

    #[test]
    fn suggest_clamps_to_floor_and_ceiling() {
        let now = Instant::now();
        let mut vd = Vardiff::new(cfg(), START, now);
        // Below floor → clamped up to the floor (a hostile/buggy suggestion can
        // never push a miner below the share-rate floor).
        assert_eq!(vd.suggest(1, now), FLOOR);
        assert_eq!(vd.current, FLOOR);
        // Above ceiling → clamped down.
        assert_eq!(vd.suggest(5_000_000_000, now), CEILING);
        // In range → applied verbatim.
        assert_eq!(vd.suggest(50_000, now), 50_000);
        assert_eq!(vd.current, 50_000);
    }

    #[test]
    fn no_retarget_before_the_interval() {
        let start = Instant::now();
        let mut vd = Vardiff::new(cfg(), 100_000, start);
        let mut now = start;
        for _ in 0..10 {
            now += Duration::from_secs(1);
            vd.record_share(100_000, now);
            assert!(vd.check_retarget(now, None).is_none());
        }
    }

    #[test]
    fn a_session_that_never_submitted_is_left_alone() {
        let start = Instant::now();
        let mut vd = Vardiff::new(cfg(), START, start);
        let mut now = start;
        for _ in 0..10 {
            now += Duration::from_secs(60);
            assert_eq!(vd.check_retarget(now, None), None);
        }
        assert_eq!(vd.current, START);
    }
}
