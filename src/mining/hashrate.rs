/// mining/hashrate.rs
///
/// Exponentially decaying hashrate averages.
///
/// The estimator is a direct port of ckpool's `decay_time()`
/// (<https://bitbucket.org/ckolivas/ckpool> — `src/libckpool.c`), together with
/// the coalescing and idle-decay behaviour of its `decay_client()` /
/// `statsupdate()` pair in `src/stratifier.c`.
///
/// State is kept as **dsps** — difficulty-shares per second — and converted to
/// H/s only at read time by multiplying with 2³² (`NONCES`), the expected
/// number of hashes behind one difficulty-1 share.
///
/// Why this form rather than a trailing sliding window: the average is defined
/// by a time constant, not by a sample span, so it is well behaved no matter
/// how the samples are spaced. Feeding a constant rate `R` with spacing `Δt`
/// has fixed point `f·(1+p) = f + R·p` where `p = 1 − e^(−Δt/τ)`, so `f → R`
/// for *any* `Δt`. And as `Δt → 0` the contribution `fadd/fsecs · fprop`
/// converges to `fadd/interval` instead of diverging, so a burst of shares can
/// never produce an unbounded reading.
use std::time::{Duration, Instant};

/// Expected hashes behind one difficulty-1 share (2³²).
pub const NONCES: f64 = 4_294_967_296.0;

/// Scale from the meter's native per-second state to a per-minute reading.
pub const SECS_PER_MINUTE: f64 = 60.0;

/// Indices into the decayed window arrays.
pub const W_1M: usize = 0;
pub const W_5M: usize = 1;
pub const W_10M: usize = 2;
pub const W_1H: usize = 3;
pub const W_3H: usize = 4;
pub const W_6H: usize = 5;
pub const W_24H: usize = 6;
pub const WINDOW_COUNT: usize = 7;

/// Time constant of each window, in seconds.
pub const WINDOW_SECS: [f64; WINDOW_COUNT] = [
    60.0,     // 1m
    300.0,    // 5m
    600.0,    // 10m
    3_600.0,  // 1h
    10_800.0, // 3h
    21_600.0, // 6h
    86_400.0, // 24h
];

/// Seconds between `HashrateDecay::tick` calls. Matches the cadence of
/// ckpool's `statsupdate` thread (1.875s) closely enough; every tracked entry
/// is decayed on the same tick with the same `tdiff`, which is what makes
/// summing dsps across sessions exact.
pub const TICK_SECS: u64 = 2;

/// Create an exponentially decaying average over `interval`.
///
/// Ported from ckpool's `decay_time()` in `src/libckpool.c`.
pub(crate) fn decay_time(f: &mut f64, fadd: f64, fsecs: f64, interval: f64) {
    if fsecs <= 0.0 {
        return;
    }
    // Put a sanity bound on how large the denominator can get.
    let dexp = (fsecs / interval).min(36.0);
    let fprop = 1.0 - 1.0 / dexp.exp();
    let ftotal = 1.0 + fprop;
    *f += fadd / fsecs * fprop;
    *f /= ftotal;
    // Sanity check to prevent meaningless super small numbers that eventually
    // underflow the JSON real number interpretation.
    if *f < 2e-16 {
        *f = 0.0;
    }
}

/// Seconds between two instants, floored so a clock that barely moved (or a
/// pair of events in the same microsecond) cannot become a denominator.
/// Ported from ckpool's `sane_tdiff()`.
pub(crate) fn sane_tdiff(end: Instant, start: Instant) -> f64 {
    end.saturating_duration_since(start)
        .as_secs_f64()
        .max(0.001)
}

/// Warm-up correction for an average that started at zero.
///
/// A [`decay_time`] average fed a constant rate for `age_secs` reads low by
/// exactly this factor, so dividing by it recovers the true rate long before
/// the window has filled. Ported from ckpool's `time_bias()` in
/// `src/stratifier.c`, which uses it to make vardiff usable on a session that
/// has only been submitting for a few seconds.
pub(crate) fn time_bias(age_secs: f64, interval: f64) -> f64 {
    1.0 - 1.0 / (age_secs / interval).min(36.0).exp()
}

/// Decayed difficulty-share rates over [`WINDOW_SECS`].
///
/// Shares are accumulated into `pending` as they arrive and folded into the
/// averages by [`HashrateDecay::tick`] on a fixed cadence — the same split
/// ckpool uses between `add_submit()` and its `statsupdate` thread. Ticking a
/// source with nothing pending decays it toward zero, so a miner that stops
/// hashing (or drops the connection entirely) falls off on its own rather than
/// freezing at its last reading.
///
/// Nothing here is specific to difficulty: the state is "units per second" and
/// the unit is whatever the caller feeds [`Self::add_share`]. `stats` runs two
/// meters off it — per-session difficulty, read back as H/s through
/// [`Self::hashrates`], and pool-wide accepted share counts, read back as
/// shares/min through [`Self::per_minute`]. Each of those readers is paired
/// with the `restored*` constructor that inverts it, so a checkpoint cannot
/// come back in the wrong unit.
#[derive(Debug, Clone)]
pub struct HashrateDecay {
    dsps: [f64; WINDOW_COUNT],
    pending: f64,
    last_decay: Instant,
}

impl HashrateDecay {
    pub fn new(now: Instant) -> Self {
        Self {
            dsps: [0.0; WINDOW_COUNT],
            pending: 0.0,
            last_decay: now,
        }
    }

    /// Restore a checkpointed hashrate and decay it across time spent offline.
    ///
    /// `Instant` cannot be persisted, so callers supply the wall-clock gap from
    /// the checkpoint separately. The gap is applied at the normal ticker
    /// cadence; doing one very large `decay_time` step would only halve an idle
    /// value because ckpool clamps that function's exponent.
    pub(crate) fn restored(
        now: Instant,
        hashrates: [f64; WINDOW_COUNT],
        offline_for: Duration,
    ) -> Self {
        Self::restored_rates(now, hashrates.map(|hps| hps / NONCES), offline_for)
    }

    /// Restore a checkpointed per-minute rate. Inverse of
    /// [`Self::per_minute`], the way [`Self::restored`] is of
    /// [`Self::hashrates`].
    pub(crate) fn restored_per_minute(
        now: Instant,
        per_minute: [f64; WINDOW_COUNT],
        offline_for: Duration,
    ) -> Self {
        Self::restored_rates(now, per_minute.map(|v| v / SECS_PER_MINUTE), offline_for)
    }

    /// As [`Self::restored`], but for a meter whose unit is not difficulty:
    /// `rates` are the raw per-second values [`Self::rates`] returned, with no
    /// scaling.
    pub(crate) fn restored_rates(
        now: Instant,
        rates: [f64; WINDOW_COUNT],
        offline_for: Duration,
    ) -> Self {
        let mut decay = Self {
            dsps: rates.map(|rate| {
                if rate.is_finite() && rate > 0.0 {
                    rate
                } else {
                    0.0
                }
            }),
            pending: 0.0,
            last_decay: now,
        };
        decay.decay_idle_for(offline_for.as_secs_f64());
        decay
    }

    fn decay_idle_for(&mut self, elapsed_secs: f64) {
        if !elapsed_secs.is_finite() || elapsed_secs <= 0.0 {
            return;
        }

        let step = TICK_SECS as f64;
        let whole_ticks = (elapsed_secs / step).floor();
        let remainder = elapsed_secs - whole_ticks * step;
        for (value, interval) in self.dsps.iter_mut().zip(WINDOW_SECS) {
            let step_factor = 1.0 / (2.0 - (-step / interval).exp());
            *value *= step_factor.powf(whole_ticks);
            if remainder > 0.0 {
                decay_time(value, 0.0, remainder, interval);
            }
            if *value < 2e-16 {
                *value = 0.0;
            }
        }
    }

    /// Record `diff` units — assigned difficulty for a hashrate meter, or a
    /// plain share count for a share-rate one. Cheap: it only accumulates,
    /// leaving the arithmetic to the next [`Self::tick`].
    pub fn add_share(&mut self, diff: f64) {
        if diff.is_finite() && diff > 0.0 {
            self.pending += diff;
        }
    }

    /// Fold everything accumulated since the last tick into the averages.
    pub fn tick(&mut self, now: Instant) {
        let tdiff = sane_tdiff(now, self.last_decay);
        self.last_decay = now;
        let fadd = std::mem::take(&mut self.pending);
        for (f, interval) in self.dsps.iter_mut().zip(WINDOW_SECS) {
            decay_time(f, fadd, tdiff, interval);
        }
    }

    /// Decayed units per second, per window, in whatever unit
    /// [`Self::add_share`] was fed.
    pub fn rates(&self) -> [f64; WINDOW_COUNT] {
        self.dsps
    }

    /// Decayed units per minute, per window. What a meter fed plain share
    /// counts reads out as: at pool scale a per-second figure spends its life
    /// in the tenths, where the leading digits carry no information.
    pub fn per_minute(&self) -> [f64; WINDOW_COUNT] {
        self.dsps.map(|d| d * SECS_PER_MINUTE)
    }

    /// Decayed hashrate in H/s, per window. Only meaningful for a meter fed
    /// share difficulties.
    pub fn hashrates(&self) -> [f64; WINDOW_COUNT] {
        self.dsps.map(|d| d * NONCES)
    }

    /// True once every window has decayed to nothing and no share is pending,
    /// i.e. the entry can be evicted without changing any displayed total.
    pub fn is_idle(&self) -> bool {
        self.pending == 0.0 && self.dsps.iter().all(|&d| d == 0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const TH: f64 = 1e12;

    /// Drive `decay` at `rate` H/s for `secs` seconds in `step`-second ticks.
    fn feed(decay: &mut HashrateDecay, start: Instant, rate: f64, secs: u64, step: u64) -> Instant {
        let mut now = start;
        let per_tick = rate * step as f64 / NONCES;
        for _ in 0..(secs / step) {
            decay.add_share(per_tick);
            now += Duration::from_secs(step);
            decay.tick(now);
        }
        now
    }

    /// The steady-state reading equals the true rate regardless of how the
    /// samples are spaced.
    #[test]
    fn converges_to_true_rate_at_any_sample_spacing() {
        for step in [1_u64, 2, 60] {
            let start = Instant::now();
            let mut decay = HashrateDecay::new(start);
            // Five 24h time constants so even the longest window settles.
            feed(&mut decay, start, 10.0 * TH, 5 * 86_400, step);
            let hr = decay.hashrates();
            for (i, &value) in hr.iter().enumerate() {
                let error = (value - 10.0 * TH).abs() / (10.0 * TH);
                assert!(
                    error < 0.01,
                    "step {step}s window {i}: {value:e} H/s is {:.1}% off 10 TH/s",
                    error * 100.0
                );
            }
        }
    }

    /// A lone share folded in with a near-zero elapsed time must not spike the
    /// average: one share of difficulty 4700 can lift the 1m window by at most
    /// 4700/60 × 2³² ≈ 336 GH/s no matter how small the interval is.
    #[test]
    fn single_share_cannot_spike_the_average() {
        let start = Instant::now();
        let mut decay = HashrateDecay::new(start);
        decay.add_share(4_700.0);
        // A tick in the same instant: tdiff floors at 1ms.
        decay.tick(start);
        let one_minute = decay.hashrates()[W_1M];
        let bound = 4_700.0 / 60.0 * NONCES;
        assert!(
            one_minute <= bound,
            "1m read {one_minute:e} H/s, above the {bound:e} H/s ceiling"
        );
        assert!(
            one_minute > bound * 0.9,
            "1m read {one_minute:e} H/s, implausibly low"
        );
    }

    /// A freshly started pool must not report the same number for every
    /// window. After one minute of hashing the 1m average is most of the way
    /// there while the 24h average has barely moved.
    #[test]
    fn windows_diverge_on_a_young_pool() {
        let start = Instant::now();
        let mut decay = HashrateDecay::new(start);
        feed(&mut decay, start, 10.0 * TH, 60, 1);
        let hr = decay.hashrates();

        // One time constant in: ~63% of the true rate.
        assert!(
            hr[W_1M] > 5.5 * TH && hr[W_1M] < 7.0 * TH,
            "1m read {:e} H/s, expected ~6.3 TH/s",
            hr[W_1M]
        );
        // 1/1440th of a time constant in: still essentially nothing.
        assert!(
            hr[W_24H] < hr[W_1M] / 100.0,
            "24h read {:e} H/s against a 1m of {:e} H/s — the windows collapsed",
            hr[W_24H],
            hr[W_1M]
        );
        // Strictly ordered: the longer the window, the further behind it lags.
        for pair in hr.windows(2) {
            assert!(
                pair[0] > pair[1],
                "windows not monotonically decreasing: {hr:?}"
            );
        }
    }

    /// A miner that stops submitting decays instead of freezing at its last
    /// reading, whether or not the session is still connected.
    #[test]
    fn idle_source_decays_toward_zero() {
        let start = Instant::now();
        let mut decay = HashrateDecay::new(start);
        let now = feed(&mut decay, start, 10.0 * TH, 5 * 86_400, 1);
        let before = decay.hashrates();

        // One 1m time constant of silence.
        let mut now = now;
        for _ in 0..60 {
            now += Duration::from_secs(1);
            decay.tick(now);
        }
        let after = decay.hashrates();
        let ratio = after[W_1M] / before[W_1M];
        assert!(
            (0.30..0.45).contains(&ratio),
            "1m decayed to {:.0}% after a minute idle, expected ~37%",
            ratio * 100.0
        );
        // The 24h window barely notices a minute of silence.
        assert!(after[W_24H] > before[W_24H] * 0.99);

        // A full day of silence takes everything down.
        for _ in 0..(86_400 / 10) {
            now += Duration::from_secs(10);
            decay.tick(now);
        }
        let after = decay.hashrates();
        assert!(
            after[W_24H] < before[W_24H] * 0.5,
            "24h read {:e} H/s after a day idle",
            after[W_24H]
        );
    }

    #[test]
    fn dsps_scale_by_nonces() {
        let start = Instant::now();
        let mut decay = HashrateDecay::new(start);
        feed(&mut decay, start, 1.0 * TH, 3_600, 1);
        let dsps = decay.rates();
        let hr = decay.hashrates();
        for i in 0..WINDOW_COUNT {
            assert!((hr[i] - dsps[i] * NONCES).abs() < 1.0);
        }
    }

    #[test]
    fn restored_state_matches_idle_ticks_across_downtime() {
        let start = Instant::now();
        let saved = [10.0 * TH; WINDOW_COUNT];

        let restored = HashrateDecay::restored(start, saved, Duration::from_secs(300));

        let mut ticked = HashrateDecay::restored(start, saved, Duration::ZERO);
        let mut now = start;
        for _ in 0..(300 / TICK_SECS) {
            now += Duration::from_secs(TICK_SECS);
            ticked.tick(now);
        }

        for (actual, expected) in restored.hashrates().into_iter().zip(ticked.hashrates()) {
            let error = (actual - expected).abs() / expected;
            assert!(error < 1e-12, "restored value differed by {error:e}");
        }
    }

    /// Each reader must round-trip through its own `restored*` constructor and
    /// no other. Mixing the pairs is the whole failure mode: a share rate
    /// restored through `restored` comes back 2³² times too small, and one
    /// restored through `restored_rates` comes back 60 times too small.
    #[test]
    fn each_unit_round_trips_through_its_own_constructor() {
        let start = Instant::now();
        let saved = [4.0; WINDOW_COUNT];

        let raw = HashrateDecay::restored_rates(start, saved, Duration::ZERO);
        assert_eq!(raw.rates(), saved);

        let scaled = HashrateDecay::restored(start, saved.map(|r| r * NONCES), Duration::ZERO);
        for (actual, expected) in scaled.rates().into_iter().zip(saved) {
            assert!((actual - expected).abs() < 1e-9);
        }

        let per_min = HashrateDecay::restored_per_minute(start, saved, Duration::ZERO);
        for (actual, expected) in per_min.per_minute().into_iter().zip(saved) {
            assert!((actual - expected).abs() < 1e-9);
        }
        // …and the underlying state really is the per-second value.
        for (actual, expected) in per_min.rates().into_iter().zip(saved) {
            assert!((actual - expected / SECS_PER_MINUTE).abs() < 1e-12);
        }
    }

    #[test]
    fn fresh_source_is_idle() {
        let start = Instant::now();
        let mut decay = HashrateDecay::new(start);
        assert!(decay.is_idle());
        decay.add_share(1024.0);
        assert!(!decay.is_idle());
        decay.tick(start + Duration::from_secs(2));
        assert!(!decay.is_idle());
    }
}
