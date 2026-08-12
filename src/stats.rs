/// stats.rs
///
/// In-process pool statistics — updated by session tasks, read by the dashboard.
///
/// Uses atomics and DashMap so updates are lock-free from any async task.
use crate::mining::hashrate;
use dashmap::DashMap;
use parking_lot::Mutex;
use rusqlite::{params, Connection};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// Offline workers idle longer than this are evicted from the in-memory stats
/// maps (their persisted best share survives, subject to the cap below). Keeps
/// per-message stats work and dashboard payloads bounded against connections
/// that mint many distinct worker names.
const IDLE_WORKER_EVICT_SECS: u64 = 86_400;

/// Maximum rows kept in `worker_best_shares` (in memory and in SQLite), keeping
/// the highest difficulties. Bounds boot-time load and dashboard growth; a solo
/// operator's real fleet is far below this.
const MAX_WORKER_BEST_SHARES: usize = 512;

/// Stamped into `PRAGMA user_version`. Bumped whenever the schema changes, or
/// whenever stored values stop meaning what they used to — a new hashrate
/// estimator counts, since its readings are not comparable with the old ones.
///
/// A file stamped with anything else is refused rather than upgraded: this pool
/// carries no migration path, by choice. The operator deletes the file and the
/// next boot writes a current one. Nothing here is irreplaceable enough to
/// justify code that has to understand every shape the schema ever had.
const SCHEMA_VERSION: i64 = 1;

/// How often the pool hashrate is written to `hashrate_history`. The dashboard
/// polls the chart at the same cadence.
pub const SNAPSHOT_INTERVAL_SECS: u64 = 10;

/// How long the decayed-average history (`hashrate_history`,
/// `share_rate_history`) is kept. Those tables only serve the short chart
/// ranges; everything longer is derived exactly from the share ledger.
const FINE_HISTORY_RETENTION_SECS: u64 = 48 * 3600;

/// Grid of the share ledger: one `share_intervals` row per worker per minute.
pub const LEDGER_INTERVAL_SECS: u64 = 60;

/// How long 1-minute ledger rows are kept before being rolled up into
/// `share_intervals_hourly`. Rollup is exact — the rows are sums — so this
/// bounds resolution, not accuracy.
///
/// This is the one table whose size grows with fleet size *and* calendar time
/// (one row per worker per minute), so the window is set by what actually reads
/// minute grain: `/history`, and nothing else. Every chart range buckets at 5
/// minutes or coarser. Eight days keeps the fine table near 12k rows per
/// worker while the hourly table — 60× smaller per unit time — carries the
/// long record; the extra day past a week keeps the 1w chart range entirely
/// clear of the rollup seam.
const LEDGER_FINE_RETENTION_SECS: u64 = 8 * 86_400;

/// Distinct workers the ledger will create rows for. Worker rows are permanent
/// and authorization is unauthenticated, so without a cap a client rotating
/// identities could grow `users`/`workers` without bound. A real fleet — even
/// the hypothetical 1000-device one — is far below this.
const MAX_LEDGER_WORKERS: usize = 10_000;

// ─────────────────────────────────────────────────────────────────────────────
// Found-block ledger
// ─────────────────────────────────────────────────────────────────────────────

/// A block this pool found, still waiting on the deferred confirmation pass.
///
/// `submitblock`'s verdict is only true at the instant it is read, so every
/// block the node stored is enrolled here and re-checked until the answer is
/// final. See `mining::confirm`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingBlock {
    /// Big-endian display hex, as `mining::validator::block_hash_display`
    /// produces it — the only form `getblockheader` accepts.
    pub hash: String,
    pub height: u64,
    pub worker: String,
    pub payout: String,
    pub found_ts: u64,
    /// What `submitblock` said at the time (`BlockSubmitOutcome::is_win`), and
    /// so what was already counted. The confirmation pass only has work to do
    /// where this disagrees with the chain.
    pub won_at_submit: bool,
}

/// How a found block's confirmation ended.
///
/// Terminal: a block leaves the pending set exactly once. A closed set because
/// it is both a SQL column value and a Prometheus label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockResolution {
    /// Buried `confirmation_depth` deep on the active chain. Final.
    Confirmed,
    /// Buried that deep on a branch that lost. If this block was counted as a
    /// win at submit time, that count was wrong.
    Orphaned,
    /// The node stopped knowing about the hash and never came back — reindexed,
    /// replaced, or restored from a snapshot. Neither claim can be made, so the
    /// submit-time verdict is left standing.
    Abandoned,
}

impl BlockResolution {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Orphaned => "orphaned",
            Self::Abandoned => "abandoned",
        }
    }
}

/// Status a found block carries in the `found_blocks` ledger until the
/// confirmation pass decides it. `BlockResolution::label` supplies the
/// terminal values.
pub const BLOCK_STATUS_PENDING: &str = "pending";

// Generates `HashrateWindows` and everything that has to walk its fields in
// window order, from the single table below the macro.
//
// The four operations (`add`, `from_windows`, `to_windows`, `uniform`) were
// four hand-written lists of the same seven fields in the same order, and
// `to_windows`' order additionally has to match the Prometheus window labels.
// Any one of them could drift and silently mislabel every exported series, so
// the field ↔ window-index correspondence is stated exactly once.
macro_rules! hashrate_windows {
    ($($field:ident => $index:path),+ $(,)?) => {
        /// Hashrate averages over the windows in
        /// `mining::hashrate::WINDOW_SECS`, in H/s. These are decaying averages
        /// with the named window as their time constant, not trailing sliding
        /// windows: a freshly started source reads far below its true rate on
        /// the longer windows until they have had time to fill.
        #[derive(Debug, Clone, Copy, Default)]
        pub struct HashrateWindows {
            $(pub $field: f64,)+
        }

        impl HashrateWindows {
            fn add(&mut self, other: Self) {
                $(self.$field += other.$field;)+
            }

            /// Build from a `mining::hashrate` window array, which is indexed
            /// by the `W_*` constants.
            fn from_windows(hps: [f64; hashrate::WINDOW_COUNT]) -> Self {
                Self { $($field: hps[$index],)+ }
            }

            /// Back to the `W_*`-indexed array, which is the order the
            /// Prometheus window labels are in
            /// (`metrics::HASHRATE_WINDOW_LABELS`).
            pub fn to_windows(self) -> [f64; hashrate::WINDOW_COUNT] {
                let mut out = [0.0; hashrate::WINDOW_COUNT];
                $(out[$index] = self.$field;)+
                out
            }

            #[cfg(test)]
            fn uniform(hps: f64) -> Self {
                Self { $($field: hps,)+ }
            }
        }
    };
}

hashrate_windows! {
    one_minute => hashrate::W_1M,
    five_minutes => hashrate::W_5M,
    ten_minutes => hashrate::W_10M,
    one_hour => hashrate::W_1H,
    three_hours => hashrate::W_3H,
    six_hours => hashrate::W_6H,
    twenty_four_hours => hashrate::W_24H,
}

/// One bucketed sample of a decaying-average series: hashrate in H/s, or
/// accepted shares/min. `None` where the underlying column predates the row.
/// Six windows, not seven — 3h is checkpointed but never plotted.
#[derive(Debug, Clone, Copy)]
pub struct RateHistoryPoint {
    pub ts: u64,
    pub one_minute: Option<f64>,
    pub five_minutes: Option<f64>,
    pub ten_minutes: Option<f64>,
    pub one_hour: Option<f64>,
    pub six_hours: Option<f64>,
    pub twenty_four_hours: Option<f64>,
}

struct PersistedWorkerHashrate {
    worker: String,
    updated_ts: u64,
    rates: HashrateWindows,
}

/// Decaying hashrate state for one miner connection, keyed by session id
/// rather than worker name so that several rigs sharing a name each contribute
/// to the total instead of overwriting one another.
struct SessionHashrate {
    worker: String,
    decay: hashrate::HashrateDecay,
}

// ─────────────────────────────────────────────────────────────────────────────
// Persistent store
// ─────────────────────────────────────────────────────────────────────────────

/// Share counts for the current round, as persisted. A named struct rather
/// than two bare integers because it is the unit a future per-worker totals
/// map will carry through the same snapshot write.
#[derive(Debug, Clone, Copy, Default)]
struct ShareTotals {
    accepted: u64,
    rejected: u64,
}

/// Amounts accumulated for one worker inside the ledger minute currently open.
#[derive(Debug, Clone, Copy, Default)]
struct LedgerPending {
    /// Σ credited difficulty of accepted shares — the work-sum primitive every
    /// long-range hashrate figure is derived from.
    work: u64,
    accepted: u64,
    rejected: u64,
}

/// The share ledger's in-memory stage: everything submitted since the last
/// minute boundary, keyed by worker name. One mutex guards both fields so the
/// flush's swap-and-restamp is atomic against the share path.
struct LedgerAccum {
    /// Start of the minute the pending map is accumulating for.
    minute_ts: u64,
    pending: HashMap<String, LedgerPending>,
}

impl LedgerAccum {
    /// Advance to the minute containing `now`, returning the closed minute's
    /// contents if one just ended with anything in it.
    ///
    /// The stamp moves even when there was nothing to flush, so an idle stretch
    /// cannot leave shares from a later minute filed under an earlier one.
    fn roll_to(&mut self, now: u64) -> Option<(u64, Vec<LedgerEntry>)> {
        let minute = now / LEDGER_INTERVAL_SECS * LEDGER_INTERVAL_SECS;
        if minute == self.minute_ts {
            return None;
        }
        // A clock stepped backwards must not reopen a minute already flushed:
        // keep accumulating into the current one instead.
        if minute < self.minute_ts {
            return None;
        }
        let closed = self.take_open();
        self.minute_ts = minute;
        closed
    }

    /// Take everything staged for the open minute without closing it.
    fn take_open(&mut self) -> Option<(u64, Vec<LedgerEntry>)> {
        if self.pending.is_empty() {
            return None;
        }
        let entries = self
            .pending
            .drain()
            .map(|(worker, amounts)| LedgerEntry {
                worker,
                work: amounts.work,
                accepted: amounts.accepted,
                rejected: amounts.rejected,
            })
            .collect();
        Some((self.minute_ts, entries))
    }
}

/// One flushed worker-minute, applied by the writer thread with
/// accumulate-on-conflict so a restart inside the minute merges rather than
/// overwrites.
#[derive(Debug, Clone)]
struct LedgerEntry {
    worker: String,
    work: u64,
    accepted: u64,
    rejected: u64,
}

/// One bucket of the share ledger: exact sums over `[ts, ts + span_secs)`.
/// Average hashrate over the bucket is `work × 2³² / span_secs`.
///
/// `span_secs` is usually the bucket size the caller asked for, but rows past
/// the rollup horizon can only be bucketed at whole hours — the divisor
/// travels with the point so no caller ever divides an hour's work by a
/// minute's grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkHistoryPoint {
    pub ts: u64,
    pub work: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub span_secs: u64,
}

/// Scope of a ledger history query: the whole pool, one payout address, or one
/// worker (`address` or `address.label`, exactly as authorized).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerScope<'a> {
    Pool,
    User(&'a str),
    Worker(&'a str),
}

/// One sampling tick's worth of persisted state, assembled by
/// `PoolStats::record_hashrate_snapshot_at` and written as a single
/// transaction. Grouped rather than passed as a dozen positional arguments
/// through three layers — `Default` also keeps the test call sites to the
/// fields each one actually cares about.
#[derive(Debug, Clone, Default)]
struct SnapshotWrite {
    /// Timestamp for the history series, snapped to the sampling grid.
    history_ts: u64,
    /// True time of the checkpoint rows, so a restore knows the real gap.
    state_ts: u64,
    rates: HashrateWindows,
    worker_rates: HashMap<String, HashrateWindows>,
    share_totals: ShareTotals,
    round_reject_reasons: BTreeMap<String, u64>,
    /// Accepted-share work accumulated in the current round (see
    /// `PoolStats::round_work`).
    round_work: u64,
    /// The round epoch the share stats above were read under. The writer
    /// discards share stats carrying an epoch older than the last applied
    /// `RoundReset`, so a snapshot raced by a reset cannot resurrect the
    /// previous round's totals.
    round_epoch: u64,
    /// Pool-wide accepted shares/min, `hashrate::W_*`-indexed. A bare array
    /// rather than a named-field struct: there is one pool meter and no
    /// aggregation to do, and indexing by the `W_*` constants leaves no gap for
    /// fields and window indices to drift apart in.
    share_rates: [f64; hashrate::WINDOW_COUNT],
}

/// A persisted update, applied by the writer thread.
///
/// Every write originates on an async task — the share hot path, the snapshot
/// ticker, the pruner — so none of them may touch the disk directly. They hand
/// the work to one thread that owns the write connection instead, which also
/// keeps dashboard queries and share submissions off a shared mutex.
enum StoreWrite {
    BestShare(u64),
    BestHashrate(f64),
    WorkerBestShare {
        worker: String,
        difficulty: u64,
    },
    Snapshot(SnapshotWrite),
    /// One flushed minute of the share ledger. Not epoch-guarded: the ledger
    /// is round-agnostic by design — round figures are queries over it, not
    /// state that a reset has to zero.
    Ledger {
        minute_ts: u64,
        entries: Vec<LedgerEntry>,
    },
    /// The pool found a block: zero every "since the last found block"
    /// quantity and stamp when the new round began. Watermark writes racing
    /// this are not epoch-guarded — the worst case is one share-sized
    /// watermark surviving the reset, re-earned by the next share.
    RoundReset {
        epoch: u64,
        since_ts: u64,
    },
    PruneWorkerBestShares(usize),
    /// Enrol a found block in the confirmation ledger. Unlike a watermark this
    /// cannot be recomputed — it is the only record that a reorg has to be
    /// checked for — which is why the writer retries it on failure instead of
    /// dropping it.
    BlockFound(PendingBlock),
    BlockResolved {
        hash: String,
        resolution: BlockResolution,
        resolved_ts: u64,
    },
    /// Barrier: acknowledged once every write queued before it has been
    /// applied. Bounds data loss on the shutdown path, where the store's
    /// `Drop` never runs because every spawned task holds an `Arc` clone.
    Flush(std::sync::mpsc::Sender<()>),
}

/// How long the writer waits between retry passes over failed durable writes.
/// Long enough for a transient condition (a `sqlite3` session holding the
/// lock past the busy timeout) to clear, short enough that a recovered disk
/// catches up within seconds.
const WRITE_RETRY_SECS: u64 = 5;

/// Failed durable writes held for retry before the oldest is dropped. Ledger
/// minutes arrive once a minute and block events a few times a year, so a
/// backlog this deep means the disk has been gone for days — at which point
/// bounding memory matters more than the writes.
const MAX_WRITE_BACKLOG: usize = 10_000;

struct StatsStore {
    /// Read connection. Only touched from `spawn_blocking` (dashboard queries)
    /// and at boot, so it never blocks a runtime worker thread.
    read: Mutex<Connection>,
    /// `None` only while dropping, which is what closes the channel and lets
    /// the writer thread finish. Unbounded: `enqueue` must never block the
    /// share path and must never drop — memory is bounded by event rate times
    /// however long the writer is stalled, and every message is small.
    writes: Option<std::sync::mpsc::Sender<StoreWrite>>,
    writer: Option<std::thread::JoinHandle<()>>,
}

/// Refuse a stats file this build does not already understand.
///
/// The only two acceptable states are "stamped with our version" and "empty",
/// the second being a file SQLite just created. Anything else was written by a
/// different schema and is rejected outright — the caller degrades to running
/// without persistence, so miners keep hashing while the operator deletes the
/// file. Upgrading it in place is the thing this pool deliberately does not do.
fn check_schema_version(conn: &Connection) -> Result<(), rusqlite::Error> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version == SCHEMA_VERSION {
        return Ok(());
    }
    let tables: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'",
        [],
        |row| row.get(0),
    )?;
    if version == 0 && tables == 0 {
        return Ok(());
    }
    Err(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_NOTADB),
        Some(format!(
            "stats database is schema v{version}, this build writes v{SCHEMA_VERSION}; \
             there is no migration path — delete or move the file and restart"
        )),
    ))
}

impl Drop for StatsStore {
    /// Flush before going away: closing the channel ends `writer_loop` once it
    /// has drained the queue, and the join waits for that. Without this a
    /// process that shuts down promptly — or a test that reopens the same file
    /// — could lose the last few queued updates.
    fn drop(&mut self) {
        self.writes.take();
        if let Some(handle) = self.writer.take() {
            let _ = handle.join();
        }
    }
}

impl StatsStore {
    /// Open a connection with the pragmas this workload wants: WAL so the
    /// dashboard's reads never block the writer (and vice versa), `NORMAL`
    /// syncing so a 10-second snapshot doesn't fsync twice, and a busy timeout
    /// so a concurrent `sqlite3` session on the same file cannot make writes
    /// fail outright.
    fn connect(path: &str) -> Result<Connection, rusqlite::Error> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        // Off by default in SQLite, which would make the ledger's REFERENCES
        // clauses decorative.
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }

    fn open(path: &str) -> Result<Self, rusqlite::Error> {
        let conn = Self::connect(path)?;
        check_schema_version(&conn)?;

        // One transaction, stamp last: a file either carries the whole schema
        // and its version, or none of it — a crash mid-create cannot leave an
        // unstamped half-schema that the next boot would refuse as foreign.
        //
        // The schema separates three kinds of state. Durable facts (`users`,
        // `workers`, `share_intervals*`, `found_blocks`) are records nothing
        // can reconstruct. Round state (`round_*`) is zeroed by every found
        // block. Everything else is a decayed-display cache or checkpoint,
        // pruned to a 48-hour horizon and rebuilt from live meters.
        conn.execute_batch(&format!(
            "BEGIN;

             -- Round state: every column resets when a block is found.
             -- `since_ts` is when the current round began (file creation,
             -- then each found block); `work` is the round's credited sum.
             CREATE TABLE IF NOT EXISTS round_stats (
               id INTEGER PRIMARY KEY CHECK(id = 1),
               shares_accepted INTEGER NOT NULL CHECK(shares_accepted >= 0),
               shares_rejected INTEGER NOT NULL CHECK(shares_rejected >= 0),
               work INTEGER NOT NULL CHECK(work >= 0),
               since_ts INTEGER NOT NULL CHECK(since_ts >= 0),
               best_share_difficulty INTEGER NOT NULL CHECK(best_share_difficulty >= 0),
               best_hashrate_hps REAL NOT NULL CHECK(best_hashrate_hps >= 0.0)
             );
             INSERT OR IGNORE INTO round_stats VALUES
               (1, 0, 0, 0, CAST(strftime('%s','now') AS INTEGER), 0, 0.0);
             CREATE TABLE IF NOT EXISTS round_worker_best_shares (
               worker TEXT PRIMARY KEY,
               best_share_difficulty INTEGER NOT NULL CHECK(best_share_difficulty >= 0)
             );
             -- Round rejects by reason. Keys come from the closed
             -- `RejectReason` label set, so the table is bounded at a handful
             -- of rows and never needs pruning.
             CREATE TABLE IF NOT EXISTS round_reject_reasons (
               reason TEXT PRIMARY KEY,
               count INTEGER NOT NULL CHECK(count >= 0)
             );

             -- Durable: each row is a block this pool found, and `status` is
             -- what the deferred confirmation pass reconciles against the
             -- chain. Rows are never deleted — a solo pool's block count is
             -- the whole point of the exercise.
             CREATE TABLE IF NOT EXISTS found_blocks (
               hash TEXT PRIMARY KEY,
               height INTEGER NOT NULL CHECK(height >= 0),
               worker TEXT NOT NULL,
               payout TEXT NOT NULL,
               found_ts INTEGER NOT NULL CHECK(found_ts >= 0),
               won_at_submit INTEGER NOT NULL CHECK(won_at_submit IN (0, 1)),
               status TEXT NOT NULL
                 CHECK(status IN ('pending', 'confirmed', 'orphaned', 'abandoned')),
               resolved_ts INTEGER,
               CHECK((status = 'pending') = (resolved_ts IS NULL))
             );

             -- Durable: the share ledger. Exact per-interval work sums,
             -- dimensioned by worker and user (payout address) — the record
             -- every long-range figure is derived from. `SUM(work) × 2³² /
             -- span` is the average hashrate over any span, additive across
             -- workers → users → pool, and it means the same thing under any
             -- estimator, which is why these outlive every cache below.
             CREATE TABLE IF NOT EXISTS users (
               id INTEGER PRIMARY KEY,
               payout_address TEXT NOT NULL UNIQUE CHECK(payout_address <> ''),
               first_seen_ts INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS workers (
               id INTEGER PRIMARY KEY,
               user_id INTEGER NOT NULL REFERENCES users(id),
               label TEXT NOT NULL,
               first_seen_ts INTEGER NOT NULL,
               UNIQUE(user_id, label)
             );
             -- WITHOUT ROWID: the composite key *is* the row identity, and
             -- the B-tree on (worker_id, ts) is the per-worker range scan
             -- every filtered query does. The ts indexes serve the pool-wide
             -- scans. Timestamps are grid-aligned by construction; the CHECK
             -- makes a misfiled row an error instead of a silent misplot.
             CREATE TABLE IF NOT EXISTS share_intervals (
               worker_id INTEGER NOT NULL REFERENCES workers(id),
               ts INTEGER NOT NULL CHECK(ts % {minute} = 0),
               work INTEGER NOT NULL CHECK(work >= 0),
               accepted INTEGER NOT NULL CHECK(accepted >= 0),
               rejected INTEGER NOT NULL CHECK(rejected >= 0),
               PRIMARY KEY (worker_id, ts)
             ) WITHOUT ROWID;
             CREATE INDEX IF NOT EXISTS share_intervals_ts ON share_intervals(ts);
             CREATE TABLE IF NOT EXISTS share_intervals_hourly (
               worker_id INTEGER NOT NULL REFERENCES workers(id),
               ts INTEGER NOT NULL CHECK(ts % 3600 = 0),
               work INTEGER NOT NULL CHECK(work >= 0),
               accepted INTEGER NOT NULL CHECK(accepted >= 0),
               rejected INTEGER NOT NULL CHECK(rejected >= 0),
               PRIMARY KEY (worker_id, ts)
             ) WITHOUT ROWID;
             CREATE INDEX IF NOT EXISTS share_intervals_hourly_ts
               ON share_intervals_hourly(ts);

             -- Caches: decayed-average history and live-meter checkpoints,
             -- pruned to a 48-hour horizon and rebuilt from live meters.
             CREATE TABLE IF NOT EXISTS hashrate_history (
               ts INTEGER PRIMARY KEY,
               hashrate_hps REAL NOT NULL,
               hashrate_1m_hps REAL,
               hashrate_5m_hps REAL,
               hashrate_1h_hps REAL,
               hashrate_6h_hps REAL,
               hashrate_24h_hps REAL
             );
             CREATE TABLE IF NOT EXISTS worker_hashrate_state (
               worker TEXT PRIMARY KEY,
               updated_ts INTEGER NOT NULL,
               hashrate_1m_hps REAL NOT NULL,
               hashrate_5m_hps REAL NOT NULL,
               hashrate_10m_hps REAL NOT NULL,
               hashrate_1h_hps REAL NOT NULL,
               hashrate_3h_hps REAL NOT NULL,
               hashrate_6h_hps REAL NOT NULL,
               hashrate_24h_hps REAL NOT NULL
             );
             -- Pool-wide accepted shares/min over the same six windows as
             -- `hashrate_history`, so the two charts plot the same points.
             CREATE TABLE IF NOT EXISTS share_rate_history (
               ts INTEGER PRIMARY KEY,
               spm_1m REAL,
               spm_5m REAL,
               spm_10m REAL,
               spm_1h REAL,
               spm_6h REAL,
               spm_24h REAL
             );
             -- Checkpoint of the live meter, so a restart resumes the
             -- averages decayed across the downtime. All seven windows,
             -- matching `worker_hashrate_state`: restoring 3h as zero would
             -- silently corrupt it the day it gets exposed.
             CREATE TABLE IF NOT EXISTS share_rate_state (
               id INTEGER PRIMARY KEY CHECK (id = 1),
               updated_ts INTEGER NOT NULL,
               spm_1m REAL NOT NULL,
               spm_5m REAL NOT NULL,
               spm_10m REAL NOT NULL,
               spm_1h REAL NOT NULL,
               spm_3h REAL NOT NULL,
               spm_6h REAL NOT NULL,
               spm_24h REAL NOT NULL
             );

             PRAGMA user_version = {version};
             COMMIT;",
            minute = LEDGER_INTERVAL_SECS,
            version = SCHEMA_VERSION,
        ))?;

        // `found_blocks` is the durable authority on when rounds ended. A
        // reset that was parked in the retry backlog when the process died
        // never reached `round_stats`, leaving the finished round's totals
        // standing; reconcile by re-deriving the round boundary from the last
        // submit-time win. Strictly newer only — `since_ts` is stamped by the
        // reset in the same second the block is enrolled, so an applied reset
        // never trips this.
        let last_win: Option<u64> = conn.query_row(
            "SELECT MAX(found_ts) FROM found_blocks WHERE won_at_submit = 1",
            [],
            |row| row.get(0),
        )?;
        let since_ts: u64 =
            conn.query_row("SELECT since_ts FROM round_stats WHERE id = 1", [], |row| {
                row.get(0)
            })?;
        if let Some(win_ts) = last_win {
            if win_ts > since_ts {
                apply_round_reset(&conn, win_ts)?;
                info!(
                    "Recovered a round reset lost to an unclean shutdown: \
                     round restarted at the block found at {win_ts}"
                );
            }
        }

        // Enforce the row cap at boot, synchronously and before the writer
        // thread exists, so an attacker-inflated table from a previous run is
        // trimmed before `load_values` pulls it into RAM.
        prune_worker_best_shares(&conn, MAX_WORKER_BEST_SHARES);

        let writer_conn = Self::connect(path)?;
        let (writes, rx) = std::sync::mpsc::channel();
        let writer = std::thread::Builder::new()
            .name("stats-writer".into())
            .spawn(move || writer_loop(writer_conn, rx))
            .map_err(|e| {
                rusqlite::Error::InvalidParameterName(format!("spawn stats writer thread: {e}"))
            })?;

        Ok(Self {
            read: Mutex::new(conn),
            writes: Some(writes),
            writer: Some(writer),
        })
    }

    /// Queue a write. Never blocks (the channel is unbounded) and never drops:
    /// what happens on a failing disk is the writer's problem, not the share
    /// path's.
    fn enqueue(&self, write: StoreWrite) {
        let Some(writes) = self.writes.as_ref() else {
            return; // shutting down
        };
        if writes.send(write).is_err() {
            static GONE: std::sync::Once = std::sync::Once::new();
            GONE.call_once(|| {
                warn!("Stats writer thread has stopped; stats are no longer persisted")
            });
        }
    }

    /// Block until every write queued so far has been applied. Bounded: a
    /// wedged disk must not be able to hang shutdown, so give up after five
    /// seconds and let whatever is still queued go down with the process.
    fn flush(&self) {
        let (ack, done) = std::sync::mpsc::channel();
        let Some(writes) = self.writes.as_ref() else {
            return;
        };
        if writes.send(StoreWrite::Flush(ack)).is_ok() {
            if done.recv_timeout(Duration::from_secs(5)).is_err() {
                warn!("Timed out draining the stats write queue");
            }
        } else {
            warn!("Stats writer is gone; skipping final flush");
        }
    }

    fn load_values(
        &self,
    ) -> Result<(u64, f64, std::collections::HashMap<String, u64>), rusqlite::Error> {
        let conn = self.read.lock();
        let mut stmt = conn.prepare(
            "SELECT best_share_difficulty, best_hashrate_hps FROM round_stats WHERE id = 1",
        )?;
        let mut rows = stmt.query([])?;
        let best_values = if let Some(row) = rows.next()? {
            let best_share_difficulty = row.get::<_, u64>(0)?;
            let best_hashrate_hps = row.get::<_, f64>(1)?;
            (best_share_difficulty, best_hashrate_hps)
        } else {
            (0, 0.0)
        };

        let mut worker_best_shares = std::collections::HashMap::new();
        let mut stmt =
            conn.prepare("SELECT worker, best_share_difficulty FROM round_worker_best_shares")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let worker = row.get::<_, String>(0)?;
            let difficulty = row.get::<_, u64>(1)?;
            worker_best_shares.insert(worker, difficulty);
        }

        Ok((best_values.0, best_values.1, worker_best_shares))
    }

    fn load_hashrate_state(&self) -> Result<Vec<PersistedWorkerHashrate>, rusqlite::Error> {
        let conn = self.read.lock();
        let mut stmt = conn.prepare(
            "SELECT worker, updated_ts,
                    hashrate_1m_hps, hashrate_5m_hps, hashrate_10m_hps,
                    hashrate_1h_hps, hashrate_3h_hps, hashrate_6h_hps,
                    hashrate_24h_hps
             FROM worker_hashrate_state",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(PersistedWorkerHashrate {
                worker: row.get(0)?,
                updated_ts: row.get(1)?,
                rates: HashrateWindows {
                    one_minute: row.get(2)?,
                    five_minutes: row.get(3)?,
                    ten_minutes: row.get(4)?,
                    one_hour: row.get(5)?,
                    three_hours: row.get(6)?,
                    six_hours: row.get(7)?,
                    twenty_four_hours: row.get(8)?,
                },
            })
        })?;
        rows.collect()
    }

    /// Blocks still awaiting confirmation, so a restart mid-window does not
    /// silently abandon one. Confirmation takes an hour at the default depth;
    /// without this a well-timed restart is all it takes for a reorg to go
    /// unnoticed, which is the bug this ledger exists to close.
    fn load_pending_blocks(&self) -> Result<Vec<PendingBlock>, rusqlite::Error> {
        let conn = self.read.lock();
        let mut stmt = conn.prepare(
            "SELECT hash, height, worker, payout, found_ts, won_at_submit
             FROM found_blocks WHERE status = ?1",
        )?;
        let rows = stmt.query_map(params![BLOCK_STATUS_PENDING], |row| {
            Ok(PendingBlock {
                hash: row.get(0)?,
                height: row.get(1)?,
                worker: row.get(2)?,
                payout: row.get(3)?,
                found_ts: row.get(4)?,
                won_at_submit: row.get::<_, i64>(5)? != 0,
            })
        })?;
        rows.collect()
    }

    /// All-time `(found, orphaned, inconclusive)` from the ledger.
    ///
    /// A row counts as a win if `submitblock` said so and nothing has since
    /// orphaned it, or if it lost its height race at submit time and a later
    /// reorg promoted it onto the active chain — the two directions the
    /// confirmation pass reconciles. `abandoned` leaves the submit-time verdict
    /// standing, since nothing was ever proved against it.
    fn load_block_counts(&self) -> Result<(u64, u64, u64), rusqlite::Error> {
        let conn = self.read.lock();
        // `SUM(CASE ...)` rather than `COUNT(*) FILTER (...)`: rusqlite links
        // the system SQLite, and `FILTER` needs 3.30. `COALESCE` because `SUM`
        // over no rows is NULL, not 0.
        conn.query_row(
            "SELECT
               COALESCE(SUM(CASE WHEN (won_at_submit = 1 AND status != 'orphaned')
                                    OR (won_at_submit = 0 AND status = 'confirmed')
                                 THEN 1 ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN won_at_submit = 1 AND status = 'orphaned'
                                 THEN 1 ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN won_at_submit = 0 AND status != 'confirmed'
                                 THEN 1 ELSE 0 END), 0)
             FROM found_blocks",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
    }

    fn set_best_share_difficulty(&self, difficulty: u64) {
        self.enqueue(StoreWrite::BestShare(difficulty));
    }

    fn set_best_hashrate_hps(&self, hps: f64) {
        self.enqueue(StoreWrite::BestHashrate(hps));
    }

    fn set_worker_best_share(&self, worker: &str, difficulty: u64) {
        self.enqueue(StoreWrite::WorkerBestShare {
            worker: worker.to_string(),
            difficulty,
        });
    }

    fn prune_worker_best_shares(&self, keep: usize) {
        self.enqueue(StoreWrite::PruneWorkerBestShares(keep));
    }

    fn record_found_block(&self, block: PendingBlock) {
        self.enqueue(StoreWrite::BlockFound(block));
    }

    fn record_block_resolution(&self, hash: &str, resolution: BlockResolution, resolved_ts: u64) {
        self.enqueue(StoreWrite::BlockResolved {
            hash: hash.to_string(),
            resolution,
            resolved_ts,
        });
    }

    fn record_ledger_minute(&self, minute_ts: u64, entries: Vec<LedgerEntry>) {
        self.enqueue(StoreWrite::Ledger { minute_ts, entries });
    }

    fn record_hashrate_snapshot(&self, snapshot: SnapshotWrite) {
        self.enqueue(StoreWrite::Snapshot(snapshot));
    }

    fn round_reset(&self, epoch: u64, since_ts: u64) {
        self.enqueue(StoreWrite::RoundReset { epoch, since_ts });
    }

    fn load_round_stats(&self) -> Result<(ShareTotals, u64, u64), rusqlite::Error> {
        let conn = self.read.lock();
        conn.query_row(
            "SELECT shares_accepted, shares_rejected, since_ts, work
             FROM round_stats WHERE id = 1",
            [],
            |row| {
                Ok((
                    ShareTotals {
                        accepted: row.get(0)?,
                        rejected: row.get(1)?,
                    },
                    row.get(2)?,
                    row.get(3)?,
                ))
            },
        )
    }

    fn load_reject_reasons(&self) -> Result<BTreeMap<String, u64>, rusqlite::Error> {
        let conn = self.read.lock();
        let mut stmt = conn.prepare("SELECT reason, count FROM round_reject_reasons")?;
        let mut rows = stmt.query([])?;
        let mut reasons = BTreeMap::new();
        while let Some(row) = rows.next()? {
            reasons.insert(row.get::<_, String>(0)?, row.get::<_, u64>(1)?);
        }
        Ok(reasons)
    }

    /// The pool share-rate checkpoint, or `None` before the first snapshot has
    /// ever been written.
    fn load_share_rate_state(
        &self,
    ) -> Result<Option<(u64, [f64; hashrate::WINDOW_COUNT])>, rusqlite::Error> {
        let conn = self.read.lock();
        conn.query_row(
            "SELECT updated_ts, spm_1m, spm_5m, spm_10m, spm_1h, spm_3h, spm_6h, spm_24h
             FROM share_rate_state WHERE id = 1",
            [],
            |row| {
                let mut rates = [0.0; hashrate::WINDOW_COUNT];
                rates[hashrate::W_1M] = row.get(1)?;
                rates[hashrate::W_5M] = row.get(2)?;
                rates[hashrate::W_10M] = row.get(3)?;
                rates[hashrate::W_1H] = row.get(4)?;
                rates[hashrate::W_3H] = row.get(5)?;
                rates[hashrate::W_6H] = row.get(6)?;
                rates[hashrate::W_24H] = row.get(7)?;
                Ok(Some((row.get(0)?, rates)))
            },
        )
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })
    }

    fn get_share_rate_history(&self, since_ts: u64, bucket_secs: u64) -> Vec<RateHistoryPoint> {
        let conn = self.read.lock();
        let mut stmt = match conn.prepare(
            "SELECT (ts / ?2) * ?2 AS bucket_ts,
                    AVG(spm_1m), AVG(spm_5m), AVG(spm_10m),
                    AVG(spm_1h), AVG(spm_6h), AVG(spm_24h)
             FROM share_rate_history
             WHERE ts >= ?1
             GROUP BY bucket_ts
             ORDER BY bucket_ts ASC",
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        stmt.query_map(params![since_ts, bucket_secs.max(1)], |row| {
            Ok(RateHistoryPoint {
                ts: row.get(0)?,
                one_minute: row.get(1)?,
                five_minutes: row.get(2)?,
                ten_minutes: row.get(3)?,
                one_hour: row.get(4)?,
                six_hours: row.get(5)?,
                twenty_four_hours: row.get(6)?,
            })
        })
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
    }

    /// Exact work sums over `[since_ts, ∞)`, bucketed to `bucket_secs`.
    ///
    /// Reads the 1-minute table where it still has rows and the hourly rollup
    /// before that, so a range crossing the retention boundary is continuous.
    /// The two never overlap: rollup deletes the minute rows it consumed, in
    /// the same transaction that inserts the hours.
    ///
    /// Buckets with no shares are absent rather than zero — a caller plotting a
    /// series decides for itself whether a gap means "idle" or "pool was down",
    /// and inventing zeroes here would foreclose that.
    fn get_work_history(
        &self,
        since_ts: u64,
        bucket_secs: u64,
        now_ts: u64,
        scope: LedgerScope<'_>,
    ) -> Vec<WorkHistoryPoint> {
        let bucket = bucket_secs.max(1);
        // Floor the range start onto the grid: a bucket must either be inside
        // the range whole or not at all, or its leading point under-reads.
        let since = since_ts / bucket * bucket;
        // A bucket smaller than an hour cannot be served from the rollup, so
        // rolled-up rows are bucketed at whole hours instead. Each branch
        // reports the grid it used as `span`, which is the divisor that turns
        // the bucket's work sum back into a rate — without it a caller
        // dividing an hour's work by a minute grid reads 60× high.
        let hourly_bucket = bucket.max(3_600) / 3_600 * 3_600;
        // The bound name is unused by the pool-wide filter, but still bound:
        // `?4` fixes the statement's parameter count either way.
        let (filter, param): (&str, &str) = match scope {
            LedgerScope::Pool => ("", ""),
            LedgerScope::User(address) => (
                "AND worker_id IN (SELECT w.id FROM workers w
                                   JOIN users u ON u.id = w.user_id
                                   WHERE u.payout_address = ?3)",
                address,
            ),
            LedgerScope::Worker(name) => (
                "AND worker_id IN (SELECT w.id FROM workers w
                                   JOIN users u ON u.id = w.user_id
                                   WHERE u.payout_address || CASE w.label WHEN '' THEN ''
                                         ELSE '.' || w.label END = ?3)",
                name,
            ),
        };
        // The outer aggregation is not redundant with the inner ones: a bucket
        // wider than an hour can span the retention seam and collect rows from
        // both tables, which would otherwise emit that bucket twice. MAX(span)
        // is exact for such a bucket — the grids coincide there, since a
        // sub-hour bucket can never straddle the seam.
        let sql = format!(
            "SELECT bucket_ts, SUM(work), SUM(accepted), SUM(rejected), MAX(span) FROM (
               SELECT (ts / ?2) * ?2 AS bucket_ts, SUM(work) AS work,
                      SUM(accepted) AS accepted, SUM(rejected) AS rejected, ?2 AS span
               FROM share_intervals
               WHERE ts >= ?1 {filter}
               GROUP BY bucket_ts
               UNION ALL
               SELECT (ts / ?4) * ?4 AS bucket_ts, SUM(work) AS work,
                      SUM(accepted) AS accepted, SUM(rejected) AS rejected, ?4 AS span
               FROM share_intervals_hourly
               WHERE ts >= ?1 {filter}
               GROUP BY bucket_ts
             )
             GROUP BY bucket_ts
             ORDER BY bucket_ts ASC"
        );
        let conn = self.read.lock();
        let mut stmt = match conn.prepare(&sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                warn!("Failed to prepare ledger query: {e}");
                return vec![];
            }
        };
        stmt.query_map(params![since, bucket, param, hourly_bucket], |row| {
            Ok(WorkHistoryPoint {
                ts: row.get(0)?,
                work: row.get::<_, i64>(1).unwrap_or(0).max(0) as u64,
                accepted: row.get::<_, i64>(2).unwrap_or(0).max(0) as u64,
                rejected: row.get::<_, i64>(3).unwrap_or(0).max(0) as u64,
                span_secs: row.get::<_, i64>(4).unwrap_or(0).max(1) as u64,
            })
        })
        .map(|rows| {
            rows.filter_map(|r| r.ok())
                // A bucket whose span has not fully elapsed holds a fraction
                // of its final work; served as-is it reads as a rate collapse
                // at the right edge of every chart.
                .filter(|p: &WorkHistoryPoint| p.ts + p.span_secs <= now_ts)
                .collect()
        })
        .unwrap_or_default()
    }

    fn get_hashrate_history(&self, since_ts: u64, bucket_secs: u64) -> Vec<RateHistoryPoint> {
        let conn = self.read.lock();
        let mut stmt = match conn.prepare(
            "SELECT (ts / ?2) * ?2 AS bucket_ts,
                    AVG(hashrate_1m_hps), AVG(hashrate_5m_hps), AVG(hashrate_hps),
                    AVG(hashrate_1h_hps), AVG(hashrate_6h_hps), AVG(hashrate_24h_hps)
             FROM hashrate_history
             WHERE ts >= ?1
             GROUP BY bucket_ts
             ORDER BY bucket_ts ASC",
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        stmt.query_map(params![since_ts, bucket_secs.max(1)], |row| {
            Ok(RateHistoryPoint {
                ts: row.get(0)?,
                one_minute: row.get(1)?,
                five_minutes: row.get(2)?,
                ten_minutes: row.get(3)?,
                one_hour: row.get(4)?,
                six_hours: row.get(5)?,
                twenty_four_hours: row.get(6)?,
            })
        })
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Writer thread
// ─────────────────────────────────────────────────────────────────────────────

/// Drain queued writes until the store is dropped. Owns the only write
/// connection, so SQLite never sees two writers and every statement below runs
/// off the async runtime.
///
/// Writes come in two classes. The durable ones — ledger minutes, found
/// blocks, block resolutions, round resets — are records, not derivable from
/// anything else, so a failed apply parks them in a retry backlog instead of
/// dropping them; the loop wakes every `WRITE_RETRY_SECS` while any are
/// pending. Everything else is a watermark or cache the next share or snapshot
/// re-earns, and stays warn-and-drop.
fn writer_loop(conn: Connection, rx: std::sync::mpsc::Receiver<StoreWrite>) {
    use std::sync::mpsc::RecvTimeoutError;

    let mut state = WriterState::default();
    let mut backlog: std::collections::VecDeque<StoreWrite> = std::collections::VecDeque::new();
    loop {
        let next = if backlog.is_empty() {
            match rx.recv() {
                Ok(write) => Some(write),
                Err(_) => break,
            }
        } else {
            match rx.recv_timeout(Duration::from_secs(WRITE_RETRY_SECS)) {
                Ok(write) => Some(write),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        };

        // Oldest first, before newer work, so a recovered disk sees writes in
        // roughly the order they happened.
        retry_backlog(&conn, &mut backlog, &mut state);
        if let Some(write) = next {
            handle_write(&conn, write, &mut state, &mut backlog);
        }
    }

    // The channel only closes with an empty queue, so all that can remain is
    // the backlog: one last attempt, then say plainly what is being lost.
    retry_backlog(&conn, &mut backlog, &mut state);
    if !backlog.is_empty() {
        error!(
            "Exiting with {} durable stats writes unpersisted",
            backlog.len()
        );
    }
}

/// State the writer thread carries between writes.
#[derive(Default)]
struct WriterState {
    /// Epoch of the last RoundReset *successfully applied* this boot. A
    /// snapshot's round-scoped stats are merged only when its epoch equals
    /// this: an older epoch means the snapshot predates a reset, a newer one
    /// means the reset itself has not landed yet (queued or parked in the
    /// retry backlog) — merging in either state would resurrect a finished
    /// round's totals through the MAX guards.
    round_epoch: u64,
    /// `workers.id` by name, saving one SELECT per worker per ledger minute.
    /// Sound because both tables are insert-only — but a failed ledger write
    /// rolls back rows whose ids may already be cached, so the cache is
    /// cleared on that failure rather than left pointing at nothing.
    worker_ids: std::collections::HashMap<String, i64>,
}

/// Whether a failed write must be retried rather than dropped: these are the
/// records nothing can reconstruct.
fn is_durable(write: &StoreWrite) -> bool {
    matches!(
        write,
        StoreWrite::Ledger { .. }
            | StoreWrite::BlockFound(_)
            | StoreWrite::BlockResolved { .. }
            | StoreWrite::RoundReset { .. }
    )
}

fn handle_write(
    conn: &Connection,
    write: StoreWrite,
    state: &mut WriterState,
    backlog: &mut std::collections::VecDeque<StoreWrite>,
) {
    // A flush that acks while durable writes are parked would falsely tell
    // shutdown everything landed.
    if matches!(write, StoreWrite::Flush(_)) && !backlog.is_empty() {
        warn!(
            "{} durable stats writes still unpersisted at flush",
            backlog.len()
        );
    }
    if let Err(e) = apply_write(conn, &write, state) {
        if is_durable(&write) {
            warn!("Failed to persist a durable stats write (will retry): {e}");
            if backlog.len() >= MAX_WRITE_BACKLOG {
                error!("Durable stats backlog overflowed; dropping the oldest write");
                backlog.pop_front();
            }
            backlog.push_back(write);
        } else {
            warn!("Failed to persist stats update: {e}");
        }
    }
}

/// Re-attempt parked durable writes, oldest first. Stops at the first failure:
/// if the disk is still gone there is no point hammering the rest, and order
/// is preserved for the next pass. Retrying is safe — a failed transaction
/// rolled back, and the ledger's upserts accumulate on conflict rather than
/// replace.
fn retry_backlog(
    conn: &Connection,
    backlog: &mut std::collections::VecDeque<StoreWrite>,
    state: &mut WriterState,
) {
    let parked = backlog.len();
    for _ in 0..parked {
        let Some(write) = backlog.pop_front() else {
            break;
        };
        if apply_write(conn, &write, state).is_err() {
            backlog.push_front(write);
            break;
        }
    }
    let recovered = parked - backlog.len();
    if recovered > 0 {
        info!("Persisted {recovered} previously failed durable stats writes");
    }
}

fn apply_write(
    conn: &Connection,
    write: &StoreWrite,
    state: &mut WriterState,
) -> Result<(), rusqlite::Error> {
    match write {
        // The `?1 > ...` guards keep the watermarks monotonic at the SQL level:
        // updates can be queued out of order, and a stale lower value must not
        // overwrite a higher one already persisted.
        StoreWrite::BestShare(difficulty) => {
            conn.execute(
                "UPDATE round_stats SET best_share_difficulty = ?1
                 WHERE id = 1 AND ?1 > best_share_difficulty",
                params![difficulty],
            )?;
        }
        StoreWrite::BestHashrate(hps) => {
            conn.execute(
                "UPDATE round_stats SET best_hashrate_hps = ?1
                 WHERE id = 1 AND ?1 > best_hashrate_hps",
                params![hps],
            )?;
        }
        StoreWrite::WorkerBestShare { worker, difficulty } => {
            conn.execute(
                "INSERT INTO round_worker_best_shares (worker, best_share_difficulty) VALUES (?1, ?2)
                 ON CONFLICT(worker) DO UPDATE SET best_share_difficulty = excluded.best_share_difficulty
                 WHERE excluded.best_share_difficulty > round_worker_best_shares.best_share_difficulty",
                params![worker, difficulty],
            )?;
        }
        StoreWrite::PruneWorkerBestShares(keep) => {
            prune_worker_best_shares(conn, *keep);
        }
        // `OR IGNORE`: the inline retry ladder and the background resubmitter
        // can both report the same block, and the first enrolment is the one
        // with the right `found_ts`.
        StoreWrite::BlockFound(block) => {
            conn.execute(
                "INSERT OR IGNORE INTO found_blocks
                 (hash, height, worker, payout, found_ts, won_at_submit, status, resolved_ts)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)",
                params![
                    block.hash,
                    block.height,
                    block.worker,
                    block.payout,
                    block.found_ts,
                    block.won_at_submit as i64,
                    BLOCK_STATUS_PENDING,
                ],
            )?;
        }
        // Guarded on the current status so a resolution can only ever be
        // written once: the pending set is the authority on what still needs
        // deciding, and a restart replays it.
        StoreWrite::BlockResolved {
            hash,
            resolution,
            resolved_ts,
        } => {
            let updated = conn.execute(
                "UPDATE found_blocks SET status = ?2, resolved_ts = ?3
                 WHERE hash = ?1 AND status = ?4",
                params![hash, resolution.label(), resolved_ts, BLOCK_STATUS_PENDING],
            )?;
            if updated == 0 {
                // Zero rows has two very different meanings. The row exists
                // with a settled status: a replay, safely dropped. The row
                // does not exist at all: this resolution outran its
                // `BlockFound`, which is parked in the retry backlog — treat
                // it as a failure so it parks behind and lands after. Without
                // this, the row would sit `pending` forever while the pool
                // believed it resolved.
                let exists = conn
                    .query_row(
                        "SELECT 1 FROM found_blocks WHERE hash = ?1",
                        params![hash],
                        |_| Ok(()),
                    )
                    .map(|_| true)
                    .or_else(|e| match e {
                        rusqlite::Error::QueryReturnedNoRows => Ok(false),
                        other => Err(other),
                    })?;
                if !exists {
                    return Err(rusqlite::Error::QueryReturnedNoRows);
                }
            }
        }
        StoreWrite::Snapshot(snapshot) => {
            // Equality, not `<`: the round-scoped portion merges with MAX, so
            // it is only valid against a `round_stats` row that the reset for
            // this exact epoch has already zeroed. A snapshot from a *newer*
            // epoch than the last applied reset means that reset is still in
            // the queue or parked in the backlog — merging now would MAX the
            // finished round's totals back in on top of the new round's.
            let stale_round = snapshot.round_epoch != state.round_epoch;
            write_snapshot(conn, snapshot, stale_round)?;
        }
        StoreWrite::Ledger { minute_ts, entries } => {
            if let Err(e) = write_ledger_minute(conn, *minute_ts, entries, &mut state.worker_ids) {
                // The rollback may have taken rows whose ids were just cached.
                state.worker_ids.clear();
                return Err(e);
            }
            // Separate from the minute write, and its failure must not send
            // this write to the backlog: the minute is already committed, and
            // replaying it would double-count. A failed rollup simply runs
            // again on the next minute — the overdue rows are still there.
            if let Err(e) = roll_up_ledger(conn, *minute_ts) {
                warn!("Ledger rollup failed (will run again next minute): {e}");
            }
        }
        StoreWrite::RoundReset { epoch, since_ts } => {
            apply_round_reset(conn, *since_ts)?;
            // Advanced only after the reset committed: this is what the
            // Snapshot arm's equality guard reads, and advancing it on a
            // failed (parked) reset would declare snapshots of the new round
            // mergeable against the old round's still-standing totals.
            state.round_epoch = state.round_epoch.max(*epoch);
        }
        StoreWrite::Flush(ack) => {
            let _ = ack.send(());
        }
    }
    Ok(())
}

/// Resolve a worker name to its `workers.id`, creating the user and worker rows
/// on first sight.
///
/// Names arriving here are `MinerIdentity::canonical_name` — the frontends key
/// every stats call on it — so splitting on the first dot yields the
/// network-checked payout address, already in its one canonical spelling, and
/// the device label. A bare address (no label) is a worker with an empty
/// label, which keeps "one device authorized as just the address" a normal row
/// rather than a special case.
///
/// Returns `None` once `MAX_LEDGER_WORKERS` rows exist and the name is not one
/// of them: worker rows are permanent and authorization is unauthenticated, so
/// the table needs a ceiling. Existing workers keep recording either way.
fn resolve_worker_id(
    conn: &Connection,
    name: &str,
    now: u64,
) -> Result<Option<i64>, rusqlite::Error> {
    let (address, label) = match name.split_once('.') {
        Some((address, label)) => (address, label),
        None => (name, ""),
    };

    let existing: Option<i64> = conn
        .query_row(
            "SELECT w.id FROM workers w
             JOIN users u ON u.id = w.user_id
             WHERE u.payout_address = ?1 AND w.label = ?2",
            params![address, label],
            |row| row.get(0),
        )
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })?;
    if let Some(id) = existing {
        return Ok(Some(id));
    }

    let workers: i64 = conn.query_row("SELECT COUNT(*) FROM workers", [], |row| row.get(0))?;
    if workers as usize >= MAX_LEDGER_WORKERS {
        static CAPPED: std::sync::Once = std::sync::Once::new();
        CAPPED.call_once(|| {
            warn!(
                "Share ledger is at its {MAX_LEDGER_WORKERS}-worker cap; \
                 new worker names are no longer recorded"
            )
        });
        return Ok(None);
    }

    conn.execute(
        "INSERT OR IGNORE INTO users (payout_address, first_seen_ts) VALUES (?1, ?2)",
        params![address, now],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO workers (user_id, label, first_seen_ts)
         VALUES ((SELECT id FROM users WHERE payout_address = ?1), ?2, ?3)",
        params![address, label, now],
    )?;
    conn.query_row(
        "SELECT w.id FROM workers w
         JOIN users u ON u.id = w.user_id
         WHERE u.payout_address = ?1 AND w.label = ?2",
        params![address, label],
        |row| row.get(0),
    )
    .map(Some)
}

/// Apply one flushed minute of the share ledger. One transaction, no side
/// trips: the caller retries this write on failure, which is only sound while
/// failure means nothing was committed.
///
/// Sums accumulate on conflict rather than replacing: a process restarted
/// mid-minute flushes a partial row on the way down and the next run adds to
/// it, so a bounce costs no work at all rather than a whole interval. That is
/// also why the row is keyed by (worker, minute) instead of carrying a
/// sequence — two flushes for the same minute are the normal case, not an
/// error to detect.
fn write_ledger_minute(
    conn: &Connection,
    minute_ts: u64,
    entries: &[LedgerEntry],
    worker_ids: &mut std::collections::HashMap<String, i64>,
) -> Result<(), rusqlite::Error> {
    if entries.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO share_intervals (worker_id, ts, work, accepted, rejected)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(worker_id, ts) DO UPDATE SET
               work = work + excluded.work,
               accepted = accepted + excluded.accepted,
               rejected = rejected + excluded.rejected",
        )?;
        for entry in entries {
            let worker_id = match worker_ids.get(&entry.worker) {
                Some(id) => *id,
                None => {
                    let Some(id) = resolve_worker_id(&tx, &entry.worker, minute_ts)? else {
                        continue;
                    };
                    worker_ids.insert(entry.worker.clone(), id);
                    id
                }
            };
            stmt.execute(params![
                worker_id,
                minute_ts,
                entry.work,
                entry.accepted,
                entry.rejected
            ])?;
        }
    }
    tx.commit()
}

/// Fold minute rows older than `LEDGER_FINE_RETENTION_SECS` into hourly ones.
///
/// Summation, so the rollup is lossless in every quantity the ledger records —
/// only time resolution goes. Insert and delete share one transaction: the two
/// tables are read as a single series (`get_work_history` unions them), and a
/// crash between the halves would either double-count an hour or lose it.
fn roll_up_ledger(conn: &Connection, now: u64) -> Result<(), rusqlite::Error> {
    let cutoff = now.saturating_sub(LEDGER_FINE_RETENTION_SECS);
    // Whole hours only: rolling up a partially-elapsed hour would have the
    // next pass insert the same hour again, and the accumulate-on-conflict
    // below cannot tell that apart from a genuine second contribution.
    let cutoff = cutoff / 3_600 * 3_600;
    let due: i64 = conn.query_row(
        "SELECT COUNT(*) FROM share_intervals WHERE ts < ?1",
        params![cutoff],
        |row| row.get(0),
    )?;
    if due == 0 {
        return Ok(());
    }

    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "INSERT INTO share_intervals_hourly (worker_id, ts, work, accepted, rejected)
         SELECT worker_id, (ts / 3600) * 3600, SUM(work), SUM(accepted), SUM(rejected)
         FROM share_intervals
         WHERE ts < ?1
         GROUP BY worker_id, (ts / 3600) * 3600
         ON CONFLICT(worker_id, ts) DO UPDATE SET
           work = work + excluded.work,
           accepted = accepted + excluded.accepted,
           rejected = rejected + excluded.rejected",
        params![cutoff],
    )?;
    let rolled = tx.execute("DELETE FROM share_intervals WHERE ts < ?1", params![cutoff])?;
    tx.commit()?;
    info!("Rolled up {rolled} ledger minute rows into hourly totals");
    Ok(())
}

fn write_snapshot(
    conn: &Connection,
    snapshot: &SnapshotWrite,
    stale_round: bool,
) -> Result<(), rusqlite::Error> {
    let SnapshotWrite {
        history_ts,
        state_ts,
        rates,
        worker_rates,
        share_totals,
        round_reject_reasons,
        round_work,
        round_epoch: _,
        share_rates,
    } = snapshot;
    {
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO hashrate_history (
               ts, hashrate_hps, hashrate_1m_hps, hashrate_5m_hps,
               hashrate_1h_hps, hashrate_6h_hps, hashrate_24h_hps
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                history_ts,
                rates.ten_minutes,
                rates.one_minute,
                rates.five_minutes,
                rates.one_hour,
                rates.six_hours,
                rates.twenty_four_hours,
            ],
        )?;

        // Replace the whole checkpoint atomically so workers whose tails
        // have fully decayed do not reappear after a later restart.
        tx.execute("DELETE FROM worker_hashrate_state", [])?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO worker_hashrate_state (
                   worker, updated_ts, hashrate_1m_hps, hashrate_5m_hps,
                   hashrate_10m_hps, hashrate_1h_hps, hashrate_3h_hps,
                   hashrate_6h_hps, hashrate_24h_hps
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?;
            for (worker, worker_rate) in worker_rates {
                stmt.execute(params![
                    worker,
                    state_ts,
                    worker_rate.one_minute,
                    worker_rate.five_minutes,
                    worker_rate.ten_minutes,
                    worker_rate.one_hour,
                    worker_rate.three_hours,
                    worker_rate.six_hours,
                    worker_rate.twenty_four_hours,
                ])?;
            }
        }

        // Share stats are round-scoped: a snapshot assembled before a round
        // reset the writer has already applied must not re-persist them.
        if !stale_round {
            // Scalar MAX keeps the persisted totals monotonic within a round,
            // same idea as the watermark guards in `apply_write`: the baseline
            // the next boot restores must never regress, whatever order queued
            // snapshots land in.
            tx.execute(
                "UPDATE round_stats SET
                   shares_accepted = MAX(shares_accepted, ?1),
                   shares_rejected = MAX(shares_rejected, ?2),
                   work = MAX(work, ?3)
                 WHERE id = 1",
                params![share_totals.accepted, share_totals.rejected, round_work],
            )?;

            {
                let mut stmt = tx.prepare(
                    "INSERT INTO round_reject_reasons (reason, count) VALUES (?1, ?2)
                     ON CONFLICT(reason) DO UPDATE SET count = MAX(count, excluded.count)",
                )?;
                for (reason, count) in round_reject_reasons {
                    stmt.execute(params![reason, count])?;
                }
            }
        }

        // Share rate: the plotted series on the history grid, and the single
        // checkpoint row the next boot resumes from. Bound by `W_*` index so
        // the column order cannot silently drift from the window order.
        tx.execute(
            "INSERT OR REPLACE INTO share_rate_history (
               ts, spm_1m, spm_5m, spm_10m, spm_1h, spm_6h, spm_24h
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                history_ts,
                share_rates[hashrate::W_1M],
                share_rates[hashrate::W_5M],
                share_rates[hashrate::W_10M],
                share_rates[hashrate::W_1H],
                share_rates[hashrate::W_6H],
                share_rates[hashrate::W_24H],
            ],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO share_rate_state (
               id, updated_ts, spm_1m, spm_5m, spm_10m, spm_1h, spm_3h, spm_6h, spm_24h
             ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                state_ts,
                share_rates[hashrate::W_1M],
                share_rates[hashrate::W_5M],
                share_rates[hashrate::W_10M],
                share_rates[hashrate::W_1H],
                share_rates[hashrate::W_3H],
                share_rates[hashrate::W_6H],
                share_rates[hashrate::W_24H],
            ],
        )?;

        tx.commit()?;
    }

    // These two tables only back the short chart ranges now — anything longer
    // is summed exactly from the ledger, which keeps its own retention. So they
    // are dropped wholesale past the horizon rather than thinned: a decayed
    // sample surviving at one-a-minute resolution has no reader.
    let cutoff = history_ts.saturating_sub(FINE_HISTORY_RETENTION_SECS);
    conn.execute(
        "DELETE FROM hashrate_history WHERE ts < ?1",
        params![cutoff],
    )?;
    conn.execute(
        "DELETE FROM share_rate_history WHERE ts < ?1",
        params![cutoff],
    )?;
    Ok(())
}

/// Zero every round-scoped table, stamping when the new round began. One
/// transaction: a partially-applied reset retried later is harmless (every
/// statement is idempotent), but the boot reconciliation below reads these
/// tables as a unit and must never see half a reset.
fn apply_round_reset(conn: &Connection, since_ts: u64) -> Result<(), rusqlite::Error> {
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "UPDATE round_stats SET
           shares_accepted = 0,
           shares_rejected = 0,
           work = 0,
           best_share_difficulty = 0,
           best_hashrate_hps = 0.0,
           since_ts = ?1
         WHERE id = 1",
        params![since_ts],
    )?;
    tx.execute("DELETE FROM round_reject_reasons", [])?;
    tx.execute("DELETE FROM round_worker_best_shares", [])?;
    tx.commit()
}

/// Keep only the `keep` highest-difficulty rows in `round_worker_best_shares`.
fn prune_worker_best_shares(conn: &Connection, keep: usize) {
    match conn.execute(
        "DELETE FROM round_worker_best_shares WHERE worker NOT IN (
           SELECT worker FROM round_worker_best_shares
           ORDER BY best_share_difficulty DESC LIMIT ?1
         )",
        params![keep as i64],
    ) {
        Ok(0) => {}
        Ok(n) => info!("Pruned {n} stale round_worker_best_shares rows (cap {keep})"),
        Err(e) => warn!("Failed to prune round_worker_best_shares: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PoolStats
// ─────────────────────────────────────────────────────────────────────────────

pub struct PoolStats {
    shares_accepted: AtomicU64,
    shares_rejected: AtomicU64,
    /// Inbound messages dropped by the per-connection rate limiter, this boot.
    /// Not a share statistic: the limiter fires on any message type, so these
    /// stay out of the reject counts and the ledger entirely.
    rate_limited_drops: AtomicU64,
    /// Accepted/rejected totals carried over from previous runs. The atomics
    /// above count this boot only; anything round-facing goes through
    /// `round_shares_accepted()`/`..rejected()` = base + atomic − round
    /// offset. Immutable after construction, so plain integers.
    round_accepted_base: u64,
    round_rejected_base: u64,
    /// Value of base + atomic captured at the last round reset, so the
    /// displayed totals restart at zero while the boot counters keep serving
    /// the session cards. Zero until a block is found this boot.
    round_offset_accepted: AtomicU64,
    round_offset_rejected: AtomicU64,
    /// Accepted-share work (vardiff credit) accumulated since the pool last
    /// found a block — the "Pool difficulty" KPI. Restored from the stats DB
    /// at boot; zeroed by `round_reset`.
    round_work: AtomicU64,
    /// Round resets this boot. Snapshot writes carry it so the writer thread
    /// can discard share stats assembled before a reset it already applied.
    round_epoch: AtomicU64,
    /// Unix time the current round's accounting began: DB creation, then the
    /// last found block. Boot time when no stats DB is configured, so the
    /// totals above still read coherently.
    round_since_ts: AtomicU64,
    /// This boot's rejects by reason, pool-wide. Keys are the closed reject
    /// label set, so cardinality is bounded; a mutex is fine because rejects
    /// are the exception on the share path.
    reject_reasons: Mutex<BTreeMap<&'static str, u64>>,
    /// Rejects by reason carried over from previous runs; same baseline
    /// arrangement as the share totals above. Immutable after construction.
    round_reject_reasons_base: BTreeMap<String, u64>,
    /// Merged reason counts captured at the last round reset — the per-reason
    /// analogue of `round_offset_accepted`.
    round_offset_reject_reasons: Mutex<BTreeMap<String, u64>>,
    blocks_found: AtomicU64,
    /// Blocks we mined that were consensus-valid but lost a same-height race,
    /// so they sit on a side branch and earned nothing. Tracked apart from
    /// `blocks_found` so the dashboard cannot report them as wins.
    blocks_inconclusive: AtomicU64,
    /// Blocks that won their height and were later reorged out. `blocks_found`
    /// is decremented to match, so the dashboard shows the count that survived;
    /// Prometheus keeps both, since a counter cannot go down.
    blocks_orphaned: AtomicU64,
    connected_miners: AtomicU64,
    current_height: AtomicU64,
    current_coinbase_value: AtomicU64,
    current_block_transaction_count: AtomicU64,
    best_share_difficulty: AtomicU64,
    session_best_share_difficulty: AtomicU64,
    best_hashrate_hps: AtomicU64,
    session_best_hashrate_hps: AtomicU64,
    network_hashrate_hps: AtomicU64,
    network_difficulty: AtomicU64,
    /// Estimated difficulty change (%) at the next retarget, from epoch timestamps.
    /// Stored as f64::to_bits; NaN until first polled / right after a retarget.
    est_difficulty_change_pct: AtomicU64,
    /// Identity of the connected Bitcoin node, display-ready. Polled from
    /// `getnetworkinfo`; empty strings until the first successful poll.
    node_info: Mutex<NodeInfoDisplay>,
    /// Seconds after pool start when the node-info poll last succeeded;
    /// `u64::MAX` until it first does. Uptime-relative rather than wall-clock
    /// so the RPC-status age is immune to clock steps.
    node_info_ok_at_secs: AtomicU64,
    /// Found blocks still awaiting confirmation, keyed by display hash. This is
    /// the working set `mining::confirm` sweeps; it is mirrored to SQLite when
    /// a stats DB is configured and lives here alone when one is not.
    pending_blocks: DashMap<String, PendingBlock>,
    /// Per-connection decaying hashrate state, keyed by session id. Shares are
    /// accumulated here as they arrive and folded into the averages by
    /// `tick_hashrates` on a fixed cadence; an entry that stops receiving
    /// shares decays to zero on its own and is then evicted, so a miner that
    /// goes quiet — or drops its connection — falls off without any special
    /// case.
    session_hashrates: DashMap<String, SessionHashrate>,
    /// Accepted shares not yet folded into `share_rate`. Counting here rather
    /// than locking the meter keeps the share path to one relaxed add; the
    /// hashrate ticker drains it.
    pending_shares: AtomicU64,
    /// Pool-wide accepted share rate, over the same decaying windows as
    /// hashrate. Its state is per second like every `HashrateDecay`; everything
    /// outside reads it per minute through `share_rate_windows`. One meter
    /// rather than one per session: unlike hashrate this is not summed from
    /// parts, and nothing displays it per worker. It is also never evicted when
    /// idle — there is only the one, and a quiet pool should read zero rather
    /// than disappear.
    share_rate: Mutex<hashrate::HashrateDecay>,
    /// Shares accumulated for the ledger minute currently open. The share path
    /// only adds here; whole minutes are handed to the writer thread as they
    /// close. This is the staging area for the pool's durable record, so unlike
    /// every other map here it is not evicted, rebuilt, or reset by a round.
    ledger: Mutex<LedgerAccum>,
    worker_protocol: DashMap<String, String>,
    worker_last_submit_ts: DashMap<String, u64>,
    worker_best_shares: DashMap<String, u64>,
    worker_states: DashMap<String, WorkerState>,
    start_time: Instant,
    store: Option<StatsStore>,
}

/// Identity of the connected Bitcoin node, parsed from its BIP14 user agent.
/// Replaced whole on each poll so a reader never sees a mixed generation.
#[derive(Clone, Default)]
struct NodeInfoDisplay {
    /// e.g. "Bitcoin Core", "Bitcoin Knots".
    implementation: String,
    /// e.g. "29.0.0".
    version: String,
    /// The raw user agent, e.g. "/Satoshi:28.1.0/Knots:20250305/".
    subversion: String,
}

#[derive(Clone, Serialize)]
pub struct WorkerState {
    pub worker: String,
    /// Connection protocol: "sv1" or "sv2".
    pub protocol: String,
    pub online: bool,
    pub current_vardiff: u64,
    pub shares_accepted: u64,
    pub shares_rejected: u64,
    pub shares_stale: u64,
    /// Rejected shares broken down by reason ("stale", "duplicate",
    /// "low_difficulty", ...). Keys come from the fixed reason strings at the
    /// reject sites, so cardinality is bounded. Session-lifetime, not persisted.
    pub reject_reasons: BTreeMap<String, u64>,
    pub best_share_difficulty: u64,
    pub active_sessions: u64,
    pub connected_ts: u64,
    pub last_submit_ts: u64,
    pub hashrate_60s_hps: f64,
    pub hashrate_5m_hps: f64,
    pub hashrate_10m_hps: f64,
    pub hashrate_1h_hps: f64,
    pub hashrate_3h_hps: f64,
    pub hashrate_6h_hps: f64,
    pub hashrate_24h_hps: f64,
}

/// Everything read back from the stats DB at boot.
///
/// `load` refuses a file it cannot open or read the core values from — the
/// production boot (`open_recording`) propagates that so the operator learns
/// immediately, instead of the pool running for months while recording
/// nothing. The remaining loaders degrade per-table with a warning: their
/// store is demonstrably usable, so partial state loss beats no persistence.
#[derive(Default)]
struct Persisted {
    store: Option<StatsStore>,
    best_share_difficulty: u64,
    best_hashrate_hps: f64,
    worker_best_shares: HashMap<String, u64>,
    hashrates: Vec<PersistedWorkerHashrate>,
    pending_blocks: Vec<PendingBlock>,
    blocks_found: u64,
    blocks_orphaned: u64,
    blocks_inconclusive: u64,
    round_share_totals: ShareTotals,
    round_since_ts: u64,
    round_work: u64,
    round_reject_reasons: BTreeMap<String, u64>,
    /// Checkpointed pool share rate: when it was written, and the per-window
    /// shares/min at that moment. `None` when the table has no row yet, which
    /// is every boot before this feature existed.
    share_rate: Option<(u64, [f64; hashrate::WINDOW_COUNT])>,
}

impl Persisted {
    fn load(path: &str) -> Result<Self, rusqlite::Error> {
        let store = StatsStore::open(path)?;
        let (best_share_difficulty, best_hashrate_hps, worker_best_shares) = store.load_values()?;
        let hashrates = store.load_hashrate_state().unwrap_or_else(|e| {
            warn!("Failed to restore hashrates from DB {path}: {e}");
            Vec::new()
        });
        let pending_blocks = store.load_pending_blocks().unwrap_or_else(|e| {
            warn!("Failed to restore pending blocks from DB {path}: {e}");
            Vec::new()
        });
        let (blocks_found, blocks_orphaned, blocks_inconclusive) =
            store.load_block_counts().unwrap_or_else(|e| {
                warn!("Failed to restore block counts from DB {path}: {e}");
                (0, 0, 0)
            });
        let (round_share_totals, round_since_ts, round_work) =
            store.load_round_stats().unwrap_or_else(|e| {
                warn!("Failed to restore share totals from DB {path}: {e}");
                (ShareTotals::default(), 0, 0)
            });
        let round_reject_reasons = store.load_reject_reasons().unwrap_or_else(|e| {
            warn!("Failed to restore reject reasons from DB {path}: {e}");
            BTreeMap::new()
        });
        let share_rate = store.load_share_rate_state().unwrap_or_else(|e| {
            warn!("Failed to restore share rate from DB {path}: {e}");
            None
        });
        if !pending_blocks.is_empty() {
            info!(
                "Restored {} block(s) awaiting confirmation from {path}",
                pending_blocks.len()
            );
        }
        Ok(Self {
            store: Some(store),
            best_share_difficulty,
            best_hashrate_hps,
            worker_best_shares,
            hashrates,
            pending_blocks,
            blocks_found,
            blocks_orphaned,
            blocks_inconclusive,
            round_share_totals,
            round_since_ts,
            round_work,
            round_reject_reasons,
            share_rate,
        })
    }
}

impl PoolStats {
    /// The production constructor: a configured path must yield a working
    /// store, or the error goes to the caller and boot stops. A pool that
    /// silently runs without the ledger it was told to keep looks healthy
    /// right up until the operator asks where their history went. Deleting or
    /// moving the refused file is the recovery — there is no migration path.
    pub fn open_recording(stats_db_path: &str) -> Result<Arc<Self>, rusqlite::Error> {
        let persisted = Persisted::load(stats_db_path)?;
        Ok(Self::from_persisted(
            persisted,
            Self::now_secs(),
            Instant::now(),
        ))
    }

    /// Construct with best-effort persistence: an unusable path degrades to
    /// running without a store, with a warning. For tests and tools; the
    /// production boot uses `open_recording`.
    pub fn new_with_store(stats_db_path: Option<String>) -> Arc<Self> {
        Self::new_with_store_at(stats_db_path, Self::now_secs(), Instant::now())
    }

    fn new_with_store_at(
        stats_db_path: Option<String>,
        wall_now: u64,
        instant_now: Instant,
    ) -> Arc<Self> {
        let persisted = stats_db_path
            .filter(|p| !p.is_empty())
            .and_then(|path| match Persisted::load(&path) {
                Ok(persisted) => Some(persisted),
                Err(e) => {
                    warn!("Failed to open stats DB {path}: {e}");
                    None
                }
            })
            .unwrap_or_default();
        Self::from_persisted(persisted, wall_now, instant_now)
    }

    fn from_persisted(persisted: Persisted, wall_now: u64, instant_now: Instant) -> Arc<Self> {
        let Persisted {
            store,
            best_share_difficulty,
            best_hashrate_hps,
            worker_best_shares: worker_best_shares_map,
            hashrates: persisted_hashrates,
            pending_blocks: persisted_pending_blocks,
            blocks_found,
            blocks_orphaned,
            blocks_inconclusive,
            round_share_totals,
            round_since_ts,
            round_work,
            round_reject_reasons,
            share_rate: persisted_share_rate,
        } = persisted;

        let pending_blocks = DashMap::new();
        for block in persisted_pending_blocks {
            pending_blocks.insert(block.hash.clone(), block);
        }

        let worker_best_shares = DashMap::new();
        for (worker, best_share) in worker_best_shares_map {
            worker_best_shares.insert(worker, best_share);
        }

        let session_hashrates = DashMap::new();
        for persisted in persisted_hashrates {
            let offline_for = Duration::from_secs(wall_now.saturating_sub(persisted.updated_ts));
            let decay = hashrate::HashrateDecay::restored(
                instant_now,
                persisted.rates.to_windows(),
                offline_for,
            );
            if !decay.is_idle() {
                session_hashrates.insert(
                    format!("restored:{}", persisted.worker),
                    SessionHashrate {
                        worker: persisted.worker,
                        decay,
                    },
                );
            }
        }

        // Resume the pool share rate from its checkpoint, decayed across the
        // downtime, exactly as the per-worker hashrates above are. Unlike them
        // an idle result is kept rather than dropped: there is only one meter,
        // and it has to exist for the ticker to fold into.
        let share_rate = match persisted_share_rate {
            Some((updated_ts, rates)) => hashrate::HashrateDecay::restored_per_minute(
                instant_now,
                rates,
                Duration::from_secs(wall_now.saturating_sub(updated_ts)),
            ),
            None => hashrate::HashrateDecay::new(instant_now),
        };

        Arc::new(Self {
            shares_accepted: AtomicU64::new(0),
            shares_rejected: AtomicU64::new(0),
            rate_limited_drops: AtomicU64::new(0),
            round_accepted_base: round_share_totals.accepted,
            round_rejected_base: round_share_totals.rejected,
            round_offset_accepted: AtomicU64::new(0),
            round_offset_rejected: AtomicU64::new(0),
            round_work: AtomicU64::new(round_work),
            round_epoch: AtomicU64::new(0),
            // Zero covers every no-persistence path: `Persisted::default()`
            // and a DB the migration has not stamped yet.
            round_since_ts: AtomicU64::new(if round_since_ts == 0 {
                wall_now
            } else {
                round_since_ts
            }),
            reject_reasons: Mutex::new(BTreeMap::new()),
            round_reject_reasons_base: round_reject_reasons,
            round_offset_reject_reasons: Mutex::new(BTreeMap::new()),
            blocks_found: AtomicU64::new(blocks_found),
            blocks_inconclusive: AtomicU64::new(blocks_inconclusive),
            blocks_orphaned: AtomicU64::new(blocks_orphaned),
            connected_miners: AtomicU64::new(0),
            current_height: AtomicU64::new(0),
            current_coinbase_value: AtomicU64::new(0),
            current_block_transaction_count: AtomicU64::new(0),
            best_share_difficulty: AtomicU64::new(best_share_difficulty),
            session_best_share_difficulty: AtomicU64::new(0),
            best_hashrate_hps: AtomicU64::new(best_hashrate_hps.to_bits()),
            session_best_hashrate_hps: AtomicU64::new(0),
            network_hashrate_hps: AtomicU64::new(0),
            network_difficulty: AtomicU64::new(f64::to_bits(0.0)),
            est_difficulty_change_pct: AtomicU64::new(f64::to_bits(f64::NAN)),
            node_info: Mutex::new(NodeInfoDisplay::default()),
            node_info_ok_at_secs: AtomicU64::new(u64::MAX),
            session_hashrates,
            pending_shares: AtomicU64::new(0),
            share_rate: Mutex::new(share_rate),
            ledger: Mutex::new(LedgerAccum {
                minute_ts: wall_now / LEDGER_INTERVAL_SECS * LEDGER_INTERVAL_SECS,
                pending: HashMap::new(),
            }),
            worker_protocol: DashMap::new(),
            worker_last_submit_ts: DashMap::new(),
            worker_best_shares,
            worker_states: DashMap::new(),
            pending_blocks,
            start_time: instant_now,
            store,
        })
    }

    fn persist_best_share_difficulty(&self, difficulty: u64) {
        if let Some(store) = &self.store {
            store.set_best_share_difficulty(difficulty);
        }
    }

    fn persist_best_hashrate_hps(&self, hps: f64) {
        if let Some(store) = &self.store {
            store.set_best_hashrate_hps(hps);
        }
    }

    pub fn miner_connected(&self) {
        self.connected_miners.fetch_add(1, Ordering::Relaxed);
    }

    pub fn miner_disconnected(&self) {
        self.connected_miners.fetch_sub(1, Ordering::Relaxed);
    }

    /// `credit` is the share's vardiff work value and feeds the round-work
    /// accumulator; `hash_difficulty` is the actual difficulty of the hash
    /// and drives the best-share watermarks.
    pub fn share_accepted(&self, credit: u64, hash_difficulty: u64) {
        self.shares_accepted.fetch_add(1, Ordering::Relaxed);
        // Counted here, not weighted by difficulty: this is share throughput,
        // which is exactly what the hashrate estimate divides back out.
        self.pending_shares.fetch_add(1, Ordering::Relaxed);
        self.round_work.fetch_add(credit, Ordering::Relaxed);

        // Round-best share. `fetch_max` is the whole CAS loop: it only ever
        // raises the watermark, so two racing writers cannot lose the higher
        // value. Persist only when this call is the one that raised it.
        if self
            .best_share_difficulty
            .fetch_max(hash_difficulty, Ordering::Relaxed)
            < hash_difficulty
        {
            self.persist_best_share_difficulty(hash_difficulty);
        }
        self.session_best_share_difficulty
            .fetch_max(hash_difficulty, Ordering::Relaxed);
    }

    /// Add an accepted share to the ledger: `credit` difficulty of proven work
    /// against `worker`.
    ///
    /// `credit` is the same value the hashrate estimator is fed
    /// (`mining::credit`), never the share's actual hash difficulty — hash
    /// difficulty is heavy-tailed (a share can land at 100× its target by
    /// luck), so summing it would make the ledger's work totals a lottery
    /// rather than a measurement. Summed credit is what converges to real work.
    pub fn ledger_share_accepted(&self, worker: &str, credit: u64) {
        self.record_ledger(worker, credit, 1, 0, Self::now_secs());
    }

    /// Add a rejected share to the ledger. Rejects carry no work — they are
    /// counted so a device's reject ratio is recoverable over any past range,
    /// which is the number that tells a fleet operator a rig is failing.
    pub fn ledger_share_rejected(&self, worker: &str) {
        self.record_ledger(worker, 0, 0, 1, Self::now_secs());
    }

    fn record_ledger(&self, worker: &str, work: u64, accepted: u64, rejected: u64, now: u64) {
        if self.store.is_none() {
            return;
        }
        // Only authorized identities get ledger rows. Rejects can arrive from a
        // session that never authorized, carrying a placeholder name; a worker
        // row is permanent, and its user dimension is a payout address, so a
        // name that was never parsed as one must not create either.
        if !self.worker_states.contains_key(worker) {
            return;
        }
        let closed = {
            let mut accum = self.ledger.lock();
            let closed = accum.roll_to(now);
            // The staging map needs no cap of its own: it only ever holds names
            // already present in `worker_states`, which is the pool's ceiling on
            // live identities, and it is emptied every minute. Capping it here
            // instead would let a burst of new names crowd out an established
            // worker whose first share of the minute happened to arrive later.
            let entry = accum.pending.entry(worker.to_string()).or_default();
            entry.work = entry.work.saturating_add(work);
            entry.accepted = entry.accepted.saturating_add(accepted);
            entry.rejected = entry.rejected.saturating_add(rejected);
            closed
        };
        self.persist_ledger_minute(closed);
    }

    /// Close the open ledger minute if `now` has moved past it, and hand
    /// whatever it holds to the writer thread. Called from the snapshot ticker
    /// so a pool that goes quiet still persists its last minute promptly
    /// instead of holding it until the next share arrives.
    fn flush_ledger_at(&self, now: u64) {
        if self.store.is_none() {
            return;
        }
        let closed = self.ledger.lock().roll_to(now);
        self.persist_ledger_minute(closed);
    }

    /// Persist the open minute even though it has not closed, without
    /// disturbing the accumulator's boundary. Only for shutdown: the writer
    /// accumulates on conflict, so the next run's flush of the same minute adds
    /// to this partial row rather than replacing it.
    fn flush_ledger_partial(&self) {
        if self.store.is_none() {
            return;
        }
        let closed = self.ledger.lock().take_open();
        self.persist_ledger_minute(closed);
    }

    fn persist_ledger_minute(&self, closed: Option<(u64, Vec<LedgerEntry>)>) {
        let (Some(store), Some((minute_ts, entries))) = (&self.store, closed) else {
            return;
        };
        store.record_ledger_minute(minute_ts, entries);
    }

    /// Exact work/share totals per bucket over `[since, ∞)` for one scope.
    /// Empty when no stats DB is configured.
    pub fn work_history(
        &self,
        since_ts: u64,
        bucket_secs: u64,
        scope: LedgerScope<'_>,
    ) -> Vec<WorkHistoryPoint> {
        self.work_history_at(since_ts, bucket_secs, Self::now_secs(), scope)
    }

    /// `work_history` against an explicit clock. `now_ts` bounds the result to
    /// buckets whose span has fully elapsed — a partial bucket under-reads.
    pub fn work_history_at(
        &self,
        since_ts: u64,
        bucket_secs: u64,
        now_ts: u64,
        scope: LedgerScope<'_>,
    ) -> Vec<WorkHistoryPoint> {
        self.store
            .as_ref()
            .map(|s| s.get_work_history(since_ts, bucket_secs, now_ts, scope))
            .unwrap_or_default()
    }

    /// `reason` must come from the closed reject label set
    /// (`RejectReason::label()`) — it becomes a persisted map key, so it must
    /// not be mintable from miner input.
    pub fn share_rejected(&self, reason: &'static str) {
        self.shares_rejected.fetch_add(1, Ordering::Relaxed);
        *self.reject_reasons.lock().entry(reason).or_insert(0) += 1;
    }

    /// Count a message dropped by the rate limiter. Not a share reject — the
    /// limiter fires on any inbound message type.
    pub fn message_rate_limited(&self) {
        self.rate_limited_drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Accepted shares since the last found block (pool lifetime until the
    /// first win): persisted base plus this boot, minus the round offset.
    fn round_shares_accepted(&self) -> u64 {
        self.round_accepted_base
            .saturating_add(self.shares_accepted.load(Ordering::Relaxed))
            .saturating_sub(self.round_offset_accepted.load(Ordering::Relaxed))
    }

    fn round_shares_rejected(&self) -> u64 {
        self.round_rejected_base
            .saturating_add(self.shares_rejected.load(Ordering::Relaxed))
            .saturating_sub(self.round_offset_rejected.load(Ordering::Relaxed))
    }

    /// This boot's pool-wide rejects by reason.
    fn session_reject_reasons(&self) -> BTreeMap<String, u64> {
        self.reject_reasons
            .lock()
            .iter()
            .map(|(reason, count)| ((*reason).to_string(), *count))
            .collect()
    }

    /// Rejects by reason since the last found block: the persisted baseline
    /// plus this boot, minus the counts captured at the last round reset.
    fn round_reject_reasons(&self) -> BTreeMap<String, u64> {
        let mut merged = self.round_reject_reasons_base.clone();
        for (reason, count) in self.reject_reasons.lock().iter() {
            *merged.entry((*reason).to_string()).or_insert(0) += count;
        }
        let offsets = self.round_offset_reject_reasons.lock();
        merged.retain(|reason, count| {
            *count = count.saturating_sub(offsets.get(reason).copied().unwrap_or(0));
            *count > 0
        });
        merged
    }

    /// Count a block the pool won and start a new round. The block's identity
    /// goes to the ledger via `enroll_pending_block`, not here.
    pub fn block_found(&self) {
        self.blocks_found.fetch_add(1, Ordering::Relaxed);
        self.round_reset();
    }

    /// Zero every "since the last found block" quantity, in memory and in the
    /// store. Session counters (this boot's cards) and the Prometheus
    /// counters, which must stay monotonic, are left alone.
    fn round_reset(&self) {
        let now = Self::now_secs();
        self.round_offset_accepted.store(
            self.round_accepted_base
                .saturating_add(self.shares_accepted.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
        self.round_offset_rejected.store(
            self.round_rejected_base
                .saturating_add(self.shares_rejected.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
        {
            let mut offsets = self.round_offset_reject_reasons.lock();
            *offsets = self.round_reject_reasons_base.clone();
            for (reason, count) in self.reject_reasons.lock().iter() {
                *offsets.entry((*reason).to_string()).or_insert(0) += count;
            }
        }
        self.round_work.store(0, Ordering::Relaxed);
        self.best_share_difficulty.store(0, Ordering::Relaxed);
        self.best_hashrate_hps.store(0, Ordering::Relaxed);
        self.round_since_ts.store(now, Ordering::Relaxed);
        self.worker_best_shares.clear();
        for mut state in self.worker_states.iter_mut() {
            state.best_share_difficulty = 0;
        }
        // Bumped only after the offsets above are in place. A snapshot task
        // interleaving reads its epoch first and its share totals second; were
        // the epoch bumped first, a snapshot could carry the new epoch with
        // pre-reset totals, pass the writer's staleness guard, and resurrect
        // the finished round on disk. This order makes the failure mode the
        // benign one — old epoch on already-zeroed stats, which the guard
        // discards.
        let epoch = self.round_epoch.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(store) = &self.store {
            store.round_reset(epoch, now);
        }
    }

    /// A block that was valid but did not become the chain tip. It must not
    /// count as a find.
    pub fn block_inconclusive(&self) {
        self.blocks_inconclusive.fetch_add(1, Ordering::Relaxed);
    }

    /// Enrol a block the node stored in the confirmation ledger, whatever
    /// `submitblock` said about it.
    ///
    /// Both verdicts are enrolled because both can be overturned: a win can be
    /// reorged out, and a block that lost its height race can be promoted onto
    /// the active chain by the reorg that follows.
    pub fn enroll_pending_block(
        &self,
        height: u64,
        hash: &str,
        worker: &str,
        payout: &str,
        won_at_submit: bool,
    ) {
        let block = PendingBlock {
            hash: hash.to_string(),
            height,
            worker: worker.to_string(),
            payout: payout.to_string(),
            found_ts: Self::now_secs(),
            won_at_submit,
        };
        // The retry ladder can report the same block twice; the first
        // enrolment wins, matching the `INSERT OR IGNORE` below it.
        if self.pending_blocks.contains_key(hash) {
            return;
        }
        self.pending_blocks.insert(hash.to_string(), block.clone());
        if let Some(store) = &self.store {
            store.record_found_block(block);
        }
    }

    /// Blocks still awaiting confirmation, for `mining::confirm` to sweep.
    pub fn pending_blocks(&self) -> Vec<PendingBlock> {
        self.pending_blocks
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Retire a pending block and reconcile the counts the submit-time verdict
    /// got wrong.
    ///
    /// Returns `false` if the block was already resolved — the sweep snapshots
    /// the pending set, so a slow tick can overlap the next one.
    pub fn resolve_block(&self, block: &PendingBlock, resolution: BlockResolution) -> bool {
        if self.pending_blocks.remove(&block.hash).is_none() {
            return false;
        }

        match (block.won_at_submit, resolution) {
            // Counted as a win, and the chain disagrees. This is the case the
            // ledger exists for.
            (true, BlockResolution::Orphaned) => {
                self.blocks_orphaned.fetch_add(1, Ordering::Relaxed);
                // Unlike the Prometheus counter this may go down: the dashboard
                // shows what the pool actually kept.
                let _ = self
                    .blocks_found
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                        Some(n.saturating_sub(1))
                    });
            }
            // Lost its height race, then a reorg put it on the active chain
            // after all: count the find the submit-time verdict missed.
            (false, BlockResolution::Confirmed) => {
                let _ = self.blocks_inconclusive.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |n| Some(n.saturating_sub(1)),
                );
                self.block_found();
            }
            _ => {}
        }

        if let Some(store) = &self.store {
            store.record_block_resolution(&block.hash, resolution, Self::now_secs());
        }
        true
    }

    pub fn update_height(&self, height: u64, coinbase_value: u64, transaction_count: u64) {
        self.current_height.store(height, Ordering::Relaxed);
        self.current_coinbase_value
            .store(coinbase_value, Ordering::Relaxed);
        self.current_block_transaction_count
            .store(transaction_count, Ordering::Relaxed);
    }

    /// Record an accepted share against a session's decaying hashrate. Called
    /// on the share path, so it only accumulates — `tick_hashrates` does the
    /// arithmetic.
    pub fn add_share_diff(&self, session_id: &str, worker: &str, difficulty: f64) {
        self.add_share_diff_at(session_id, worker, difficulty, Instant::now());
    }

    fn add_share_diff_at(&self, session_id: &str, worker: &str, difficulty: f64, now: Instant) {
        if let Some(mut entry) = self.session_hashrates.get_mut(session_id) {
            // One connection may re-authorize under a different identity
            // (`max_authorizations_per_session`) while keeping its session id.
            // Follow the rename, or every later share keeps landing on the
            // previous worker name. The decaying tail moves with it: it is the
            // same physical rig either way.
            if entry.worker != worker {
                entry.worker = worker.to_string();
            }
            entry.decay.add_share(difficulty);
            return;
        }
        let mut decay = hashrate::HashrateDecay::new(now);
        decay.add_share(difficulty);
        self.session_hashrates.insert(
            session_id.to_string(),
            SessionHashrate {
                worker: worker.to_string(),
                decay,
            },
        );
    }

    /// Fold the shares accumulated since the last call into every session's
    /// decaying averages, then refresh the derived pool figures. Driven by a
    /// fixed-cadence background task (`hashrate::TICK_SECS`), mirroring
    /// ckpool's `statsupdate` thread.
    ///
    /// Decaying every session on the same tick with the same elapsed time is
    /// what makes summing their rates across a worker — or across the pool —
    /// exact rather than approximate.
    pub fn tick_hashrates(&self) {
        self.tick_hashrates_at(Instant::now());
    }

    fn tick_hashrates_at(&self, now: Instant) {
        let mut idle: Vec<String> = Vec::new();
        for mut entry in self.session_hashrates.iter_mut() {
            entry.decay.tick(now);
            if entry.decay.is_idle() {
                idle.push(entry.key().clone());
            }
        }
        // Sessions that have decayed to nothing no longer affect any total.
        for session_id in idle {
            self.session_hashrates.remove(&session_id);
        }

        let by_worker = self.hashrates_by_worker();

        let mut total = HashrateWindows::default();
        for (worker, rates) in &by_worker {
            total.add(*rates);
            crate::metrics::update_worker_hashrate(worker, rates.to_windows());
        }
        // A worker whose last session just decayed away is gone from `by_worker`
        // and would otherwise keep exporting its final non-zero reading until
        // the exporter's idle timeout. Publish an explicit zero for every known
        // worker that has no live session.
        for entry in self.worker_states.iter() {
            if !by_worker.contains_key(entry.key()) {
                crate::metrics::update_worker_hashrate(
                    entry.key(),
                    HashrateWindows::default().to_windows(),
                );
            }
        }
        crate::metrics::update_pool_hashrate(total.to_windows());

        // The pool share-rate meter rides the same tick, so its windows decay
        // in step with the hashrate ones and the two charts stay comparable.
        // `add_share` ignores a zero, so an idle tick still decays correctly.
        let pending = self.pending_shares.swap(0, Ordering::Relaxed);
        let share_rates = {
            let mut meter = self.share_rate.lock();
            meter.add_share(pending as f64);
            meter.tick(now);
            meter.rates()
        };
        crate::metrics::update_pool_share_rate(share_rates);

        self.record_best_hashrate(total.ten_minutes);
    }

    /// Pool-wide accepted shares per minute, `hashrate::W_*`-indexed.
    fn share_rate_windows(&self) -> [f64; hashrate::WINDOW_COUNT] {
        self.share_rate.lock().per_minute()
    }

    /// Track round-best (persistent, zeroed by a found block) and session-best
    /// (since boot).
    ///
    /// `fetch_max` works directly on the bit patterns: for non-negative finite
    /// f64 the IEEE-754 encoding is monotonic, so comparing the bits as `u64`
    /// orders the values identically. The `is_finite` guard keeps NaN (whose
    /// bits exceed every real value) out of the watermark; a hashrate is never
    /// negative.
    fn record_best_hashrate(&self, total_10m: f64) {
        if !total_10m.is_finite() || total_10m < 0.0 {
            return;
        }
        let bits = total_10m.to_bits();

        if self.best_hashrate_hps.fetch_max(bits, Ordering::Relaxed) < bits {
            self.persist_best_hashrate_hps(total_10m);
        }
        self.session_best_hashrate_hps
            .fetch_max(bits, Ordering::Relaxed);
    }

    pub fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Sum every session's decaying rates into its worker name, in one pass.
    /// A worker running two rigs under the same name reads as the sum of both,
    /// and an unplugged rig keeps contributing a decaying tail until it has
    /// faded out of even the 24h window.
    fn hashrates_by_worker(&self) -> HashMap<String, HashrateWindows> {
        let mut by_worker: HashMap<String, HashrateWindows> = HashMap::new();
        for entry in self.session_hashrates.iter() {
            let rates = HashrateWindows::from_windows(entry.decay.hashrates());
            by_worker
                .entry(entry.worker.clone())
                .or_default()
                .add(rates);
        }
        by_worker
    }

    /// Record the connection protocol ("sv1" / "sv2") for a worker.
    pub fn set_worker_protocol(&self, worker: &str, protocol: &str) {
        self.worker_protocol
            .insert(worker.to_string(), protocol.to_string());
        if let Some(mut state) = self.worker_states.get_mut(worker) {
            state.protocol = protocol.to_string();
        }
    }

    pub fn mark_worker_submit(&self, worker: &str) {
        let now = Self::now_secs();
        self.worker_last_submit_ts.insert(worker.to_string(), now);
        if let Some(mut state) = self.worker_states.get_mut(worker) {
            state.last_submit_ts = now;
        }
    }

    pub fn mark_worker_online(&self, worker: &str, current_vardiff: u64) {
        let now = Self::now_secs();
        if let Some(mut state) = self.worker_states.get_mut(worker) {
            state.online = true;
            state.current_vardiff = current_vardiff;
            state.connected_ts = now;
            state.active_sessions = state.active_sessions.saturating_add(1);
        } else {
            let best_share_difficulty = self
                .worker_best_shares
                .get(worker)
                .map(|v| *v.value())
                .unwrap_or(0);

            let protocol = self
                .worker_protocol
                .get(worker)
                .map(|p| p.value().clone())
                .unwrap_or_else(|| "sv1".to_string());

            self.worker_states.insert(
                worker.to_string(),
                WorkerState {
                    worker: worker.to_string(),
                    protocol,
                    online: true,
                    current_vardiff,
                    shares_accepted: 0,
                    shares_rejected: 0,
                    shares_stale: 0,
                    reject_reasons: BTreeMap::new(),
                    best_share_difficulty,
                    active_sessions: 1,
                    connected_ts: now,
                    last_submit_ts: 0,
                    hashrate_60s_hps: 0.0,
                    hashrate_5m_hps: 0.0,
                    hashrate_10m_hps: 0.0,
                    hashrate_1h_hps: 0.0,
                    hashrate_3h_hps: 0.0,
                    hashrate_6h_hps: 0.0,
                    hashrate_24h_hps: 0.0,
                },
            );
        }
    }

    pub fn mark_worker_offline(&self, worker: &str) {
        if let Some(mut state) = self.worker_states.get_mut(worker) {
            if state.active_sessions > 1 {
                state.active_sessions -= 1;
            } else {
                state.active_sessions = 0;
                state.online = false;
            }
        }
    }

    pub fn update_worker_vardiff(&self, worker: &str, vardiff: u64) {
        if let Some(mut state) = self.worker_states.get_mut(worker) {
            state.current_vardiff = vardiff;
        } else {
            self.mark_worker_online(worker, vardiff);
        }
    }

    pub fn worker_share_accepted(&self, worker: &str, difficulty: u64) {
        if let Some(mut state) = self.worker_states.get_mut(worker) {
            state.shares_accepted += 1;
            if difficulty > state.best_share_difficulty {
                state.best_share_difficulty = difficulty;
            }
        }

        let mut entry = self
            .worker_best_shares
            .entry(worker.to_string())
            .or_insert(0);
        if difficulty > *entry {
            *entry = difficulty;
            if let Some(store) = &self.store {
                store.set_worker_best_share(worker, difficulty);
            }
        }
    }

    pub fn worker_share_rejected(&self, worker: &str, reason: &str) {
        if let Some(mut state) = self.worker_states.get_mut(worker) {
            state.shares_rejected += 1;
            *state.reject_reasons.entry(reason.to_string()).or_insert(0) += 1;
            if reason == "stale" {
                state.shares_stale += 1;
            }
        }
    }

    /// Evict offline workers idle past `IDLE_WORKER_EVICT_SECS` from the
    /// in-memory maps, and bound `worker_best_shares` (memory + SQLite) to the
    /// top `MAX_WORKER_BEST_SHARES` by difficulty. Called from the background
    /// pruner task. A reconnecting evicted worker is recreated on authorize;
    /// only its session counters (accepted/rejected this boot) reset.
    pub fn prune_idle_workers(&self) {
        let cutoff = Self::now_secs().saturating_sub(IDLE_WORKER_EVICT_SECS);

        let stale: Vec<String> = self
            .worker_states
            .iter()
            .filter(|e| {
                let s = e.value();
                if s.online {
                    return false;
                }
                let last_submit = self
                    .worker_last_submit_ts
                    .get(e.key())
                    .map(|v| *v.value())
                    .unwrap_or(s.last_submit_ts);
                last_submit.max(s.connected_ts) < cutoff
            })
            .map(|e| e.key().clone())
            .collect();

        for w in &stale {
            self.worker_states.remove(w);
            self.worker_protocol.remove(w);
            self.worker_last_submit_ts.remove(w);
        }
        if !stale.is_empty() {
            let evicted: std::collections::HashSet<&String> = stale.iter().collect();
            self.session_hashrates
                .retain(|_, s| !evicted.contains(&s.worker));
            info!("Evicted {} idle offline workers from stats", stale.len());
        }

        // Best shares survive eviction (the dashboard still lists the round's
        // bests), bounded by count so they can't grow without limit.
        if self.worker_best_shares.len() > MAX_WORKER_BEST_SHARES {
            let mut all: Vec<(String, u64)> = self
                .worker_best_shares
                .iter()
                .map(|e| (e.key().clone(), *e.value()))
                .collect();
            all.sort_unstable_by_key(|e| std::cmp::Reverse(e.1));
            for (w, _) in all.drain(MAX_WORKER_BEST_SHARES..) {
                self.worker_best_shares.remove(&w);
            }
            if let Some(store) = &self.store {
                store.prune_worker_best_shares(MAX_WORKER_BEST_SHARES);
            }
        }
    }

    pub fn record_hashrate_snapshot(&self) {
        self.record_hashrate_snapshot_at(Self::now_secs());
    }

    fn record_hashrate_snapshot_at(&self, state_ts: u64) {
        // Close the ledger minute if one ended since the last tick. Runs first
        // so a quiet pool's final minute reaches the writer on the same pass
        // that checkpoints the meters.
        self.flush_ledger_at(state_ts);
        if let Some(store) = &self.store {
            // Snap to the sampling grid. The recorder's wall-clock timestamps
            // drift by however long a tick took, and two things downstream need
            // them regular: the chart buckets at exactly this width on the 1h
            // view, and the retention step keeps rows where `ts % 60 == 0` —
            // which unsnapped timestamps would hit only by luck, thinning the
            // long-range history away to nothing.
            let history_ts = state_ts / SNAPSHOT_INTERVAL_SECS * SNAPSHOT_INTERVAL_SECS;
            // Epoch before share stats, and `round_reset` bumps the epoch
            // *after* zeroing them: whichever way a reset interleaves, this
            // snapshot can only pair an old epoch with fresh stats — which the
            // writer treats as stale and skips — never a new epoch with the
            // finished round's totals.
            let round_epoch = self.round_epoch.load(Ordering::Relaxed);
            let by_worker = self.hashrates_by_worker();
            let mut total = HashrateWindows::default();
            for rates in by_worker.values() {
                total.add(*rates);
            }
            store.record_hashrate_snapshot(SnapshotWrite {
                history_ts,
                state_ts,
                rates: total,
                worker_rates: by_worker,
                share_totals: ShareTotals {
                    accepted: self.round_shares_accepted(),
                    rejected: self.round_shares_rejected(),
                },
                round_reject_reasons: self.round_reject_reasons(),
                round_work: self.round_work.load(Ordering::Relaxed),
                round_epoch,
                share_rates: self.share_rate_windows(),
            });
        }
    }

    /// Final persist before process exit: one last snapshot (lifetime totals,
    /// share series, worker-hashrate and share-rate checkpoints), then drain
    /// the write queue.
    /// The explicit call exists because every spawned task holds an `Arc`
    /// clone of this struct, so the store's `Drop` flush never runs in
    /// production.
    pub fn shutdown_persist(&self) {
        self.record_hashrate_snapshot();
        // The minute in progress has not closed, but the work in it is real and
        // the writer merges it with whatever the next run records for the same
        // minute — so a restart costs no work rather than up to a minute of it.
        self.flush_ledger_partial();
        if let Some(store) = &self.store {
            store.flush();
        }
    }

    pub fn get_hashrate_history(&self, since_ts: u64, bucket_secs: u64) -> Vec<RateHistoryPoint> {
        self.store
            .as_ref()
            .map(|s| s.get_hashrate_history(since_ts, bucket_secs))
            .unwrap_or_default()
    }

    pub fn get_share_rate_history(&self, since_ts: u64, bucket_secs: u64) -> Vec<RateHistoryPoint> {
        self.store
            .as_ref()
            .map(|s| s.get_share_rate_history(since_ts, bucket_secs))
            .unwrap_or_default()
    }

    pub fn set_network_hashrate(&self, hps: f64) {
        self.network_hashrate_hps
            .store(hps.to_bits(), Ordering::Relaxed);
    }

    pub fn set_network_difficulty(&self, difficulty: f64) {
        self.network_difficulty
            .store(difficulty.to_bits(), Ordering::Relaxed);
    }

    pub fn set_est_difficulty_change_pct(&self, pct: f64) {
        self.est_difficulty_change_pct
            .store(pct.to_bits(), Ordering::Relaxed);
    }

    pub fn set_node_info(&self, implementation: String, version: String, subversion: String) {
        *self.node_info.lock() = NodeInfoDisplay {
            implementation,
            version,
            subversion,
        };
        self.node_info_ok_at_secs
            .store(self.start_time.elapsed().as_secs(), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        let by_worker = self.hashrates_by_worker();
        let worker_hashrates: Vec<WorkerHashrate> = by_worker
            .iter()
            .map(|(worker, rates)| WorkerHashrate {
                worker: worker.clone(),
                last_submit_ts: self
                    .worker_last_submit_ts
                    .get(worker)
                    .map(|v| *v.value())
                    .unwrap_or(0),
                hashrate_60s_hps: rates.one_minute,
                hashrate_5m_hps: rates.five_minutes,
                hashrate_10m_hps: rates.ten_minutes,
                hashrate_1h_hps: rates.one_hour,
                hashrate_3h_hps: rates.three_hours,
                hashrate_6h_hps: rates.six_hours,
                hashrate_24h_hps: rates.twenty_four_hours,
            })
            .collect();

        let mut totals = HashrateWindows::default();
        for rates in by_worker.values() {
            totals.add(*rates);
        }

        let best_hashrate_hps = f64::from_bits(self.best_hashrate_hps.load(Ordering::Relaxed));
        let share_rates = self.share_rate_windows();

        let mut seen = std::collections::HashSet::new();
        let mut worker_states: Vec<WorkerState> = self
            .worker_states
            .iter()
            .map(|e| {
                let mut state = e.value().clone();
                let worker = e.key();
                seen.insert(worker.clone());
                state.worker = worker.clone();
                let rates = by_worker.get(worker).copied().unwrap_or_default();
                state.hashrate_60s_hps = rates.one_minute;
                state.hashrate_5m_hps = rates.five_minutes;
                state.hashrate_10m_hps = rates.ten_minutes;
                state.hashrate_1h_hps = rates.one_hour;
                state.hashrate_3h_hps = rates.three_hours;
                state.hashrate_6h_hps = rates.six_hours;
                state.hashrate_24h_hps = rates.twenty_four_hours;
                if let Some(p) = self.worker_protocol.get(worker) {
                    state.protocol = p.value().clone();
                }
                state.last_submit_ts = self
                    .worker_last_submit_ts
                    .get(worker)
                    .map(|v| *v.value())
                    .unwrap_or(0);
                state.best_share_difficulty = self
                    .worker_best_shares
                    .get(worker)
                    .map(|v| *v.value())
                    .unwrap_or(state.best_share_difficulty);
                state
            })
            .collect();

        // Workers with no live `WorkerState`: those known only by a round-best
        // share, and those restored from the hashrate checkpoint that have
        // not reconnected yet. The latter still contribute a decaying tail to
        // the pool total, so the table has to show it rather than a zero row.
        let extra_workers = self
            .worker_best_shares
            .iter()
            .map(|e| e.key().clone())
            .chain(by_worker.keys().cloned())
            .filter(|w| !seen.contains(w))
            .collect::<std::collections::BTreeSet<String>>();

        for worker in extra_workers {
            let rates = by_worker.get(&worker).copied().unwrap_or_default();
            worker_states.push(WorkerState {
                protocol: self
                    .worker_protocol
                    .get(&worker)
                    .map(|p| p.value().clone())
                    .unwrap_or_else(|| "sv1".to_string()),
                online: false,
                current_vardiff: 0,
                shares_accepted: 0,
                shares_rejected: 0,
                shares_stale: 0,
                reject_reasons: BTreeMap::new(),
                best_share_difficulty: self
                    .worker_best_shares
                    .get(&worker)
                    .map(|v| *v.value())
                    .unwrap_or(0),
                active_sessions: 0,
                connected_ts: 0,
                last_submit_ts: self
                    .worker_last_submit_ts
                    .get(&worker)
                    .map(|v| *v.value())
                    .unwrap_or(0),
                hashrate_60s_hps: rates.one_minute,
                hashrate_5m_hps: rates.five_minutes,
                hashrate_10m_hps: rates.ten_minutes,
                hashrate_1h_hps: rates.one_hour,
                hashrate_3h_hps: rates.three_hours,
                hashrate_6h_hps: rates.six_hours,
                hashrate_24h_hps: rates.twenty_four_hours,
                worker,
            });
        }

        let node_info = self.node_info.lock().clone();
        let uptime_secs = self.start_time.elapsed().as_secs();
        let node_rpc_last_ok_secs = match self.node_info_ok_at_secs.load(Ordering::Relaxed) {
            u64::MAX => None,
            at => Some(uptime_secs.saturating_sub(at)),
        };

        StatsSnapshot {
            shares_accepted: self.shares_accepted.load(Ordering::Relaxed),
            shares_rejected: self.shares_rejected.load(Ordering::Relaxed),
            round_shares_accepted: self.round_shares_accepted(),
            round_shares_rejected: self.round_shares_rejected(),
            round_since_ts: self.round_since_ts.load(Ordering::Relaxed),
            reject_reasons: self.session_reject_reasons(),
            rate_limited_drops: self.rate_limited_drops.load(Ordering::Relaxed),
            round_reject_reasons: self.round_reject_reasons(),
            blocks_found: self.blocks_found.load(Ordering::Relaxed),
            blocks_inconclusive: self.blocks_inconclusive.load(Ordering::Relaxed),
            blocks_orphaned: self.blocks_orphaned.load(Ordering::Relaxed),
            blocks_pending_confirmation: self.pending_blocks.len() as u64,
            connected_miners: self.connected_miners.load(Ordering::Relaxed),
            current_height: self.current_height.load(Ordering::Relaxed),
            current_coinbase_value: self.current_coinbase_value.load(Ordering::Relaxed),
            current_block_transaction_count: self
                .current_block_transaction_count
                .load(Ordering::Relaxed),
            template_version: 0,
            best_share_difficulty: self.best_share_difficulty.load(Ordering::Relaxed),
            session_best_share_difficulty: self
                .session_best_share_difficulty
                .load(Ordering::Relaxed),
            best_hashrate_hps,
            total_hashrate_60s: totals.one_minute,
            total_hashrate_5m: totals.five_minutes,
            total_hashrate_10m: totals.ten_minutes,
            total_hashrate_1h: totals.one_hour,
            total_hashrate_3h: totals.three_hours,
            total_hashrate_6h: totals.six_hours,
            total_hashrate_24h: totals.twenty_four_hours,
            shares_per_minute_1m: share_rates[hashrate::W_1M],
            shares_per_minute_5m: share_rates[hashrate::W_5M],
            shares_per_minute_10m: share_rates[hashrate::W_10M],
            shares_per_minute_1h: share_rates[hashrate::W_1H],
            shares_per_minute_6h: share_rates[hashrate::W_6H],
            shares_per_minute_24h: share_rates[hashrate::W_24H],
            worker_hashrates,
            worker_states,
            network_hashrate_hps: f64::from_bits(self.network_hashrate_hps.load(Ordering::Relaxed)),
            network_difficulty: f64::from_bits(self.network_difficulty.load(Ordering::Relaxed)),
            est_difficulty_change_pct: f64::from_bits(
                self.est_difficulty_change_pct.load(Ordering::Relaxed),
            ),
            node_implementation: node_info.implementation,
            node_version: node_info.version,
            node_subversion: node_info.subversion,
            node_rpc_last_ok_secs,
            template_fresh: false,
            uptime_secs,
            session_best_hashrate_hps: f64::from_bits(
                self.session_best_hashrate_hps.load(Ordering::Relaxed),
            ),
            pool_difficulty: self.round_work.load(Ordering::Relaxed),
            unsupported_rules: Vec::new(),
            rules_block_work: false,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot (serialised as JSON for /stats)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct StatsSnapshot {
    pub shares_accepted: u64,
    pub shares_rejected: u64,
    /// Round totals: since the pool's last found block (`round_since_ts`
    /// onward) — its whole recorded life until the first win. The pair above
    /// counts this process only; identical when no stats DB is configured.
    pub round_shares_accepted: u64,
    pub round_shares_rejected: u64,
    /// Unix time the current round's accounting began: DB creation, then the
    /// last found block.
    pub round_since_ts: u64,
    /// This process's pool-wide rejects by reason. Unlike the per-worker
    /// breakdowns in `worker_states`, this survives worker eviction.
    pub reject_reasons: BTreeMap<String, u64>,
    /// Messages dropped by the per-connection rate limiter, this boot. Kept
    /// apart from the share rejects: the limiter fires on any message type,
    /// so folding these in would corrupt every reject ratio.
    pub rate_limited_drops: u64,
    /// The round's rejects by reason.
    pub round_reject_reasons: BTreeMap<String, u64>,
    /// Wins that have survived so far: decremented when the confirmation pass
    /// finds one was reorged out. The Prometheus counter of the same name
    /// cannot go down, so the two differ by `blocks_orphaned`.
    pub blocks_found: u64,
    /// Valid blocks that lost a same-height race and earned nothing.
    pub blocks_inconclusive: u64,
    /// Blocks that won their height and were later reorged off the chain.
    pub blocks_orphaned: u64,
    /// Found blocks the confirmation pass has not yet decided.
    pub blocks_pending_confirmation: u64,
    pub connected_miners: u64,
    pub current_height: u64,
    pub current_coinbase_value: u64,
    pub current_block_transaction_count: u64,
    /// Version of the current block template. The dashboard fills this from
    /// the TemplateEngine; other snapshot consumers receive zero.
    pub template_version: u32,
    /// Round-scoped bests, zeroed by a found block; the `session_*` pair is
    /// this boot only.
    pub best_share_difficulty: u64,
    pub session_best_share_difficulty: u64,
    pub best_hashrate_hps: f64,
    pub total_hashrate_60s: f64,
    pub total_hashrate_5m: f64,
    pub total_hashrate_10m: f64,
    pub total_hashrate_1h: f64,
    pub total_hashrate_3h: f64,
    pub total_hashrate_6h: f64,
    pub total_hashrate_24h: f64,
    /// Pool-wide accepted shares per minute, as decaying averages over the same
    /// windows as the hashrate totals above. Share throughput rather than work
    /// done: it moves with vardiff retargets that leave hashrate flat. Per
    /// minute rather than per second because a pool of any realistic size sits
    /// in the tenths otherwise. Six windows only — 3h is checkpointed but not
    /// exposed.
    pub shares_per_minute_1m: f64,
    pub shares_per_minute_5m: f64,
    pub shares_per_minute_10m: f64,
    pub shares_per_minute_1h: f64,
    pub shares_per_minute_6h: f64,
    pub shares_per_minute_24h: f64,
    pub network_hashrate_hps: f64,
    pub network_difficulty: f64,
    pub est_difficulty_change_pct: f64,
    /// Identity of the connected Bitcoin node, parsed from `getnetworkinfo`'s
    /// BIP14 user agent. Empty strings until the first successful poll.
    pub node_implementation: String,
    pub node_version: String,
    /// The raw user agent, e.g. "/Satoshi:28.1.0/Knots:20250305/".
    pub node_subversion: String,
    /// Seconds since the node-info RPC poll last succeeded; `null` before the
    /// first success. Drives the dashboard's RPC-status LED.
    pub node_rpc_last_ok_secs: Option<u64>,
    /// Whether the engine's current template is fresh. Filled by the dashboard
    /// from the TemplateEngine, like `template_version`; other snapshot
    /// consumers receive `false`.
    pub template_fresh: bool,
    pub worker_hashrates: Vec<WorkerHashrate>,
    pub worker_states: Vec<WorkerState>,
    pub uptime_secs: u64,
    pub session_best_hashrate_hps: f64,
    /// Accepted-share work (vardiff credit) accumulated since the pool last
    /// found a block. The dashboard divides it by `network_difficulty` for
    /// the "Pool difficulty" KPI; 100% of the network difficulty is one
    /// expected block's worth of work.
    pub pool_difficulty: u64,
    /// `!`-prefixed `getblocktemplate` rules this build does not implement.
    /// Filled by the dashboard from the TemplateEngine, like `template_version`;
    /// other snapshot consumers receive an empty list.
    #[serde(default)]
    pub unsupported_rules: Vec<String>,
    /// Whether those rules have stopped the pool issuing work
    /// (`strict_gbt_rules`), as opposed to only being reported.
    #[serde(default)]
    pub rules_block_work: bool,
}

#[derive(Serialize)]
pub struct WorkerHashrate {
    pub worker: String,
    pub last_submit_ts: u64,
    pub hashrate_60s_hps: f64,
    pub hashrate_5m_hps: f64,
    pub hashrate_10m_hps: f64,
    pub hashrate_1h_hps: f64,
    pub hashrate_3h_hps: f64,
    pub hashrate_6h_hps: f64,
    pub hashrate_24h_hps: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_temp_db() -> String {
        static TEST_DB_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let mut path = std::env::temp_dir();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let id = TEST_DB_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        path.push(format!("btcpool_rs_stats_test_{}_{}.db", ts, id));
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn best_hashrate_is_persisted_across_instances() {
        let db_path = make_temp_db();

        {
            let stats = PoolStats::new_with_store(Some(db_path.clone()));
            stats.record_best_hashrate(6.0 * TH);
            assert_eq!(stats.snapshot().best_hashrate_hps, 6.0 * TH);
            stats.record_best_hashrate(10.0 * TH);
            assert_eq!(stats.snapshot().best_hashrate_hps, 10.0 * TH);
            // Watermarks only ratchet up.
            stats.record_best_hashrate(4.0 * TH);
            assert_eq!(stats.snapshot().best_hashrate_hps, 10.0 * TH);
        }

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        assert_eq!(stats.snapshot().best_hashrate_hps, 10.0 * TH);

        std::fs::remove_file(db_path).ok();
    }

    #[test]
    fn best_share_is_persisted_across_instances() {
        let db_path = make_temp_db();

        {
            let stats = PoolStats::new_with_store(Some(db_path.clone()));
            stats.share_accepted(1_000_000, 1_000_000);
            assert_eq!(stats.snapshot().best_share_difficulty, 1_000_000);
            stats.share_accepted(1_500_000, 1_500_000);
            assert_eq!(stats.snapshot().best_share_difficulty, 1_500_000);
        }

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        assert_eq!(stats.snapshot().best_share_difficulty, 1_500_000);

        std::fs::remove_file(db_path).ok();
    }

    #[test]
    fn worker_hashrates_resume_after_restart_and_decay_while_offline() {
        let db_path = make_temp_db();
        let saved_at = 1_000_007;
        let restart_at = saved_at + 30;
        let rates = HashrateWindows::uniform(10.0 * TH);

        {
            let now = Instant::now();
            let stats = PoolStats::new_with_store_at(Some(db_path.clone()), saved_at, now);
            stats.session_hashrates.insert(
                "s1".to_string(),
                SessionHashrate {
                    worker: "axe".to_string(),
                    decay: hashrate::HashrateDecay::restored(
                        now,
                        rates.to_windows(),
                        Duration::ZERO,
                    ),
                },
            );
            stats.record_hashrate_snapshot_at(saved_at);
        }

        let now = Instant::now();
        let stats = PoolStats::new_with_store_at(Some(db_path.clone()), restart_at, now);
        let expected = HashrateWindows::from_windows(
            hashrate::HashrateDecay::restored(now, rates.to_windows(), Duration::from_secs(30))
                .hashrates(),
        );
        let snapshot = stats.snapshot();
        assert!((snapshot.total_hashrate_60s - expected.one_minute).abs() < 1.0);
        assert!((snapshot.total_hashrate_24h - expected.twenty_four_hours).abs() < 1.0);
        assert!(snapshot.total_hashrate_60s > 0.0);
        assert!(snapshot.total_hashrate_60s < 10.0 * TH);
        assert!(snapshot.total_hashrate_24h > 9.9 * TH);
        assert_eq!(snapshot.worker_hashrates.len(), 1);
        assert_eq!(snapshot.worker_hashrates[0].worker, "axe");

        // A reconnected session contributes alongside the restored tail. It
        // must not overwrite the checkpoint merely because the worker name is
        // the same.
        stats.session_hashrates.insert(
            "s2".to_string(),
            SessionHashrate {
                worker: "axe".to_string(),
                decay: hashrate::HashrateDecay::restored(
                    now,
                    HashrateWindows::uniform(2.0 * TH).to_windows(),
                    Duration::ZERO,
                ),
            },
        );
        let snapshot = stats.snapshot();
        assert!((snapshot.total_hashrate_60s - expected.one_minute - 2.0 * TH).abs() < 1.0);

        // The restored worker has no `WorkerState` yet (nothing has authorized
        // this boot), but it is carrying real hashrate, so the worker table has
        // to show it rather than a zero row next to a non-zero pool total.
        let row = snapshot
            .worker_states
            .iter()
            .find(|s| s.worker == "axe")
            .expect("restored worker missing from the worker table");
        assert!(!row.online);
        assert!(
            (row.hashrate_60s_hps - snapshot.total_hashrate_60s).abs() < 1.0,
            "restored worker row reads {} against a pool total of {}",
            row.hashrate_60s_hps,
            snapshot.total_hashrate_60s
        );

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// One connection may re-authorize under a different identity. Shares after
    /// the switch belong to the new name, not to the name the connection first
    /// authorized for the rest of its life.
    #[test]
    fn shares_follow_a_session_that_re_authorizes_under_a_new_name() {
        let stats = PoolStats::new_with_store(None);
        let start = Instant::now();

        stats.add_share_diff_at("session-1", "old-name", 4_096.0, start);
        stats.tick_hashrates_at(start + Duration::from_secs(2));
        assert!(stats.hashrates_by_worker().contains_key("old-name"));

        stats.add_share_diff_at("session-1", "new-name", 4_096.0, start);
        stats.tick_hashrates_at(start + Duration::from_secs(4));

        let by_worker = stats.hashrates_by_worker();
        assert!(
            by_worker.contains_key("new-name"),
            "shares still credited to the previous identity: {:?}",
            by_worker.keys().collect::<Vec<_>>()
        );
        assert!(!by_worker.contains_key("old-name"));
        // The decaying tail moved with the session — it is the same rig — so no
        // hashrate was lost or duplicated by the rename.
        assert_eq!(stats.session_hashrates.len(), 1);
    }

    #[test]
    fn restored_hashrate_reaches_zero_after_long_downtime() {
        let db_path = make_temp_db();
        let saved_at = 2_000_000;

        {
            let now = Instant::now();
            let stats = PoolStats::new_with_store_at(Some(db_path.clone()), saved_at, now);
            stats.session_hashrates.insert(
                "s1".to_string(),
                SessionHashrate {
                    worker: "axe".to_string(),
                    decay: hashrate::HashrateDecay::restored(
                        now,
                        HashrateWindows::uniform(TH).to_windows(),
                        Duration::ZERO,
                    ),
                },
            );
            stats.record_hashrate_snapshot_at(saved_at);
        }

        let stats = PoolStats::new_with_store_at(
            Some(db_path.clone()),
            saved_at + 60 * 86_400,
            Instant::now(),
        );
        assert!(stats.session_hashrates.is_empty());
        assert_eq!(stats.snapshot().total_hashrate_24h, 0.0);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// A file from another schema is refused, not upgraded and not silently
    /// written over. The pool then runs without persistence, which is the same
    /// degradation an unreadable file already gets: miners keep hashing, and
    /// the operator is told to move the file aside.
    #[test]
    fn a_foreign_schema_version_is_refused() {
        let db_path = make_temp_db();
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute(
                "CREATE TABLE pool_stats (
                   id INTEGER PRIMARY KEY CHECK(id = 1),
                   best_share_difficulty INTEGER NOT NULL,
                   best_hashrate_hps REAL NOT NULL
                 )",
                [],
            )
            .unwrap();
            conn.execute("INSERT INTO pool_stats VALUES (1, 7, 0.0)", [])
                .unwrap();
        }

        // The production boot path refuses to start on this file.
        assert!(PoolStats::open_recording(&db_path).is_err());
        // Refused means untouched: the old row is still there for whoever wants
        // to read it with sqlite3 before deleting the file.
        let conn = Connection::open(&db_path).unwrap();
        let best: i64 = conn
            .query_row("SELECT best_share_difficulty FROM pool_stats", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(best, 7);
        drop(conn);

        // The best-effort constructor still degrades to no store, for tests.
        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        assert!(stats.store.is_none());

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// A file this build wrote is reopened without complaint, and one SQLite
    /// has only just created is stamped rather than rejected.
    #[test]
    fn a_fresh_file_is_stamped_and_reopens_cleanly() {
        let db_path = make_temp_db();
        {
            let store = StatsStore::open(&db_path).unwrap();
            store.set_best_hashrate_hps(60.0 * TH);
        }
        let conn = Connection::open(&db_path).unwrap();
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        drop(conn);

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        assert_eq!(stats.snapshot().best_hashrate_hps, 60.0 * TH);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    #[test]
    fn hashrate_history_averages_samples_into_time_buckets() {
        let db_path = make_temp_db();
        let store = StatsStore::open(&db_path).unwrap();
        store.record_hashrate_snapshot(SnapshotWrite {
            history_ts: 120,
            state_ts: 120,
            rates: HashrateWindows::uniform(10.0),
            ..Default::default()
        });
        store.record_hashrate_snapshot(SnapshotWrite {
            history_ts: 150,
            state_ts: 150,
            rates: HashrateWindows::uniform(20.0),
            ..Default::default()
        });
        store.flush();

        let history = store.get_hashrate_history(0, 60);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].ts, 120);
        assert_eq!(history[0].one_minute, Some(15.0));
        assert_eq!(history[0].ten_minutes, Some(15.0));
        assert_eq!(history[0].twenty_four_hours, Some(15.0));

        drop(store);
        std::fs::remove_file(db_path).ok();
    }

    /// The decayed series only backs the live chart ranges now, so it is
    /// dropped wholesale past its horizon rather than thinned: everything
    /// longer is summed exactly from the ledger, which keeps its own retention.
    #[test]
    fn decayed_samples_are_dropped_past_their_horizon() {
        let db_path = make_temp_db();
        let store = StatsStore::open(&db_path).unwrap();

        // Two minutes of samples on the grid, at a minute boundary.
        let base = 9_999_960;
        assert_eq!(base % 60, 0);
        for i in 0..12 {
            let ts = base + i * SNAPSHOT_INTERVAL_SECS;
            store.record_hashrate_snapshot(SnapshotWrite {
                history_ts: ts,
                state_ts: ts,
                rates: HashrateWindows::uniform(10.0),
                ..Default::default()
            });
        }
        store.flush();
        assert_eq!(
            store.get_hashrate_history(0, SNAPSHOT_INTERVAL_SECS).len(),
            12
        );

        // A sample far enough ahead pushes them past the horizon.
        let now = base + FINE_HISTORY_RETENTION_SECS + 600;
        store.record_hashrate_snapshot(SnapshotWrite {
            history_ts: now,
            state_ts: now,
            rates: HashrateWindows::uniform(20.0),
            ..Default::default()
        });
        store.flush();

        let kept: Vec<u64> = store
            .get_hashrate_history(0, SNAPSHOT_INTERVAL_SECS)
            .iter()
            .map(|p| p.ts)
            .collect();
        assert_eq!(kept, vec![now]);
        // The share-rate series shares the horizon; a survivor there would mean
        // one of the two tables is growing without a reader.
        assert_eq!(
            store
                .get_share_rate_history(0, SNAPSHOT_INTERVAL_SECS)
                .iter()
                .map(|p| p.ts)
                .collect::<Vec<u64>>(),
            vec![now]
        );

        drop(store);
        std::fs::remove_file(db_path).ok();
    }

    #[test]
    fn round_share_totals_survive_restart() {
        let db_path = make_temp_db();
        {
            let stats = PoolStats::new_with_store(Some(db_path.clone()));
            assert!(stats.snapshot().round_since_ts > 0);
            for _ in 0..3 {
                stats.share_accepted(1_000, 1_000);
            }
            stats.share_rejected("stale");
            stats.share_rejected("duplicate");
            stats.record_hashrate_snapshot_at(120);
            // StatsStore::Drop drains the queue.
        }

        // Pin the inception stamp so the reopens below have to preserve it
        // rather than re-stamp "now".
        Connection::open(&db_path)
            .unwrap()
            .execute("UPDATE round_stats SET since_ts = 111 WHERE id = 1", [])
            .unwrap();

        {
            let stats = PoolStats::new_with_store(Some(db_path.clone()));
            let snap = stats.snapshot();
            // Boot counters restart at zero; the round pair carries on.
            assert_eq!(snap.shares_accepted, 0);
            assert_eq!(snap.shares_rejected, 0);
            assert_eq!(snap.round_shares_accepted, 3);
            assert_eq!(snap.round_shares_rejected, 2);
            assert_eq!(snap.round_since_ts, 111);
            // The reason breakdown restores alongside the totals; this boot
            // has rejected nothing yet.
            assert!(snap.reject_reasons.is_empty());
            assert_eq!(snap.round_reject_reasons.get("stale"), Some(&1));
            assert_eq!(snap.round_reject_reasons.get("duplicate"), Some(&1));

            stats.share_accepted(1_000, 1_000);
            stats.record_hashrate_snapshot_at(130);
        }

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        let snap = stats.snapshot();
        assert_eq!(snap.round_shares_accepted, 4);
        assert_eq!(snap.round_shares_rejected, 2);
        assert_eq!(snap.round_since_ts, 111);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// The lifetime counters start from the moment the file was created, not
    /// from zero-the-epoch, so "shares since" on a fresh pool reads as today
    /// rather than 1970.
    #[test]
    fn a_new_db_stamps_its_own_inception() {
        let db_path = make_temp_db();
        let before = PoolStats::now_secs();
        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        let snap = stats.snapshot();
        assert_eq!(snap.round_shares_accepted, 0);
        assert_eq!(snap.round_shares_rejected, 0);
        assert!(snap.round_since_ts >= before);

        stats.share_accepted(1_000, 1_000);
        stats.record_hashrate_snapshot_at(120);
        drop(stats);

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        assert_eq!(stats.snapshot().round_shares_accepted, 1);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    // ── Share ledger ─────────────────────────────────────────────────────────

    const ADDRESS: &str = "bc1qexampleaddress";

    /// A stats instance whose ledger clock starts at `start_ts`, with `workers`
    /// authorized — which is what gates a name into the ledger.
    ///
    /// The start matters: the accumulator opens the minute containing it, and
    /// a share stamped before that minute is treated as a backwards clock step.
    fn ledger_stats(db_path: &str, start_ts: u64, workers: &[&str]) -> Arc<PoolStats> {
        let stats =
            PoolStats::new_with_store_at(Some(db_path.to_string()), start_ts, Instant::now());
        for worker in workers {
            stats.mark_worker_online(worker, 1_024);
        }
        stats
    }

    /// Feed `count` accepted shares of `credit` each, stamped inside the minute
    /// containing `at`. Deliberately does not flush: several workers share a
    /// minute, and closing it after the first would make the rest look like a
    /// backwards clock step.
    fn ledger_shares(stats: &PoolStats, worker: &str, credit: u64, count: u64, at: u64) {
        for _ in 0..count {
            stats.record_ledger(worker, credit, 1, 0, at);
        }
    }

    /// Close every minute up to `at` and drain the writer queue, so the ledger
    /// is readable.
    fn ledger_flush(stats: &PoolStats, at: u64) {
        stats.flush_ledger_at(at);
        stats.store.as_ref().unwrap().flush();
    }

    /// The ledger's whole purpose: summed credit recovers the true average
    /// hashrate over any span exactly, with no estimator in the path.
    #[test]
    fn ledger_work_sums_to_the_true_average_hashrate() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 600, &[ADDRESS]);

        // 10 TH/s for one minute at difficulty 2048: work = rate × secs / 2³².
        let credit = 2_048;
        let expected_hps = 10.0e12;
        let shares = (expected_hps * 60.0 / hashrate::NONCES / credit as f64).round() as u64;
        ledger_shares(&stats, ADDRESS, credit, shares, 600);
        ledger_flush(&stats, 660);

        let points = stats.work_history(0, 60, LedgerScope::Pool);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].ts, 600);
        assert_eq!(points[0].accepted, shares);

        let hps = points[0].work as f64 * hashrate::NONCES / 60.0;
        let error = (hps - expected_hps).abs() / expected_hps;
        assert!(
            error < 0.01,
            "ledger read {hps:e} H/s against a true {expected_hps:e} H/s"
        );

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// Work is additive across workers → users → pool, which is what makes one
    /// ledger serve every scope. A per-worker query must also not leak its
    /// sibling's rows, the failure that would make a device page silently wrong.
    #[test]
    fn ledger_totals_are_additive_and_scope_filters_are_exact() {
        let db_path = make_temp_db();
        let one = format!("{ADDRESS}.gamma");
        let two = format!("{ADDRESS}.nerdqaxe");
        let other = "bc1qotheraddress.rig";
        let stats = ledger_stats(&db_path, 600, &[&one, &two, other]);

        ledger_shares(&stats, &one, 100, 3, 600);
        ledger_shares(&stats, &two, 100, 5, 600);
        ledger_shares(&stats, other, 100, 7, 600);
        ledger_flush(&stats, 660);

        let work_of = |scope: LedgerScope<'_>| -> u64 {
            stats
                .work_history(0, 60, scope)
                .iter()
                .map(|p| p.work)
                .sum()
        };

        assert_eq!(work_of(LedgerScope::Worker(&one)), 300);
        assert_eq!(work_of(LedgerScope::Worker(&two)), 500);
        // The user is the sum of its workers, and excludes the other address.
        assert_eq!(work_of(LedgerScope::User(ADDRESS)), 800);
        // The pool is the sum of every user.
        assert_eq!(work_of(LedgerScope::Pool), 1_500);

        // A bare address must not act as a prefix match on its own workers.
        assert_eq!(work_of(LedgerScope::Worker(ADDRESS)), 0);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// A miner authorizing as a bare payout address — no worker label — is a
    /// normal row, not a special case, and lands under its own user.
    #[test]
    fn a_bare_address_is_a_worker_with_an_empty_label() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 600, &[ADDRESS]);
        ledger_shares(&stats, ADDRESS, 64, 2, 600);
        ledger_flush(&stats, 660);

        assert_eq!(
            stats
                .work_history(0, 60, LedgerScope::Worker(ADDRESS))
                .iter()
                .map(|p| p.work)
                .sum::<u64>(),
            128
        );
        assert_eq!(
            stats
                .work_history(0, 60, LedgerScope::User(ADDRESS))
                .iter()
                .map(|p| p.work)
                .sum::<u64>(),
            128
        );

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// A resolution can outrun its block when the `BlockFound` insert is
    /// parked in the retry backlog. It must park behind it and land after —
    /// the alternative is a row stuck `pending` forever while the pool
    /// believes it resolved. A genuine replay against a settled row is still
    /// a clean drop.
    #[test]
    fn a_resolution_that_outran_its_block_parks_until_it_lands() {
        let db_path = make_temp_db();
        drop(StatsStore::open(&db_path).unwrap());
        let writer = Connection::open(&db_path).unwrap();

        let mut state = WriterState::default();
        let mut backlog = std::collections::VecDeque::new();
        let resolved = StoreWrite::BlockResolved {
            hash: "00cafe".into(),
            resolution: BlockResolution::Orphaned,
            resolved_ts: 700,
        };
        handle_write(&writer, resolved, &mut state, &mut backlog);
        assert_eq!(backlog.len(), 1, "no row yet: the resolution must park");

        handle_write(
            &writer,
            StoreWrite::BlockFound(PendingBlock {
                hash: "00cafe".into(),
                height: 800_000,
                worker: ADDRESS.to_string(),
                payout: ADDRESS.to_string(),
                found_ts: 600,
                won_at_submit: true,
            }),
            &mut state,
            &mut backlog,
        );
        retry_backlog(&writer, &mut backlog, &mut state);
        assert!(backlog.is_empty());
        let (status, resolved_ts): (String, u64) = writer
            .query_row(
                "SELECT status, resolved_ts FROM found_blocks WHERE hash = '00cafe'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((status.as_str(), resolved_ts), ("orphaned", 700));

        // A replay of the same resolution is a no-op, not a parked retry.
        let replay = StoreWrite::BlockResolved {
            hash: "00cafe".into(),
            resolution: BlockResolution::Confirmed,
            resolved_ts: 900,
        };
        handle_write(&writer, replay, &mut state, &mut backlog);
        assert!(backlog.is_empty());

        drop(writer);
        std::fs::remove_file(db_path).ok();
    }

    /// A snapshot's round totals merge with MAX, so they may only merge once
    /// the reset for their exact epoch has landed. A snapshot from a newer
    /// epoch than the last applied reset would otherwise MAX the finished
    /// round's totals back in on top of the new round's.
    #[test]
    fn a_snapshot_ahead_of_its_rounds_reset_is_not_merged() {
        let db_path = make_temp_db();
        drop(StatsStore::open(&db_path).unwrap());
        let writer = Connection::open(&db_path).unwrap();

        let mut state = WriterState::default();
        let mut backlog = std::collections::VecDeque::new();
        let snapshot = |epoch: u64, accepted: u64| {
            StoreWrite::Snapshot(SnapshotWrite {
                history_ts: 60,
                state_ts: 60,
                share_totals: ShareTotals {
                    accepted,
                    rejected: 0,
                },
                round_epoch: epoch,
                ..Default::default()
            })
        };
        let round_accepted = |conn: &Connection| -> u64 {
            conn.query_row(
                "SELECT shares_accepted FROM round_stats WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap()
        };

        // The reset for epoch 1 has not applied yet: skip the merge.
        handle_write(&writer, snapshot(1, 42), &mut state, &mut backlog);
        assert_eq!(round_accepted(&writer), 0);

        handle_write(
            &writer,
            StoreWrite::RoundReset {
                epoch: 1,
                since_ts: 200,
            },
            &mut state,
            &mut backlog,
        );
        handle_write(&writer, snapshot(1, 42), &mut state, &mut backlog);
        assert_eq!(round_accepted(&writer), 42);

        // And a snapshot from before that reset stays skipped.
        handle_write(&writer, snapshot(0, 9_000), &mut state, &mut backlog);
        assert_eq!(round_accepted(&writer), 42);

        drop(writer);
        std::fs::remove_file(db_path).ok();
    }

    /// A round reset parked in the backlog dies with an unclean shutdown, but
    /// `found_blocks` — written independently — still records the win. Boot
    /// re-derives the round boundary from it, so the finished round's totals
    /// cannot leak into the new round as its baseline.
    #[test]
    fn boot_recovers_a_round_reset_lost_to_a_crash() {
        let db_path = make_temp_db();
        {
            let stats = PoolStats::new_with_store(Some(db_path.clone()));
            for _ in 0..5 {
                stats.share_accepted(1_000, 1_000);
            }
            stats.record_hashrate_snapshot_at(120);
            stats.store.as_ref().unwrap().flush();
        }

        // Pin the round start below the synthetic win time, then record the
        // win in found_blocks without the reset it should have triggered ever
        // reaching round_stats.
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("UPDATE round_stats SET since_ts = 100 WHERE id = 1", [])
            .unwrap();
        conn.execute(
            "INSERT INTO found_blocks VALUES
               ('00feed', 800000, 'w', 'p', 500, 1, 'pending', NULL)",
            [],
        )
        .unwrap();
        drop(conn);

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        let snap = stats.snapshot();
        assert_eq!(snap.round_shares_accepted, 0, "old round must not leak");
        assert_eq!(snap.round_since_ts, 500, "round restarts at the win");

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// A durable write that fails is parked and lands once the disk recovers —
    /// exactly once, since the failed transaction rolled back. A best-effort
    /// write failing the same way is simply dropped.
    #[test]
    fn a_failed_durable_write_is_retried_not_dropped() {
        let db_path = make_temp_db();
        drop(StatsStore::open(&db_path).unwrap());

        // Zero busy timeout, so a held write lock fails immediately instead of
        // stalling the test.
        let writer = Connection::open(&db_path).unwrap();
        writer.busy_timeout(Duration::ZERO).unwrap();
        let blocker = Connection::open(&db_path).unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();

        let mut state = WriterState::default();
        let mut backlog = std::collections::VecDeque::new();
        let minute = StoreWrite::Ledger {
            minute_ts: 600,
            entries: vec![LedgerEntry {
                worker: ADDRESS.to_string(),
                work: 512,
                accepted: 4,
                rejected: 0,
            }],
        };
        handle_write(&writer, minute, &mut state, &mut backlog);
        handle_write(&writer, StoreWrite::BestShare(9), &mut state, &mut backlog);
        assert_eq!(backlog.len(), 1, "only the durable write is parked");

        // Still down: the pass leaves the backlog intact.
        retry_backlog(&writer, &mut backlog, &mut state);
        assert_eq!(backlog.len(), 1);

        blocker.execute_batch("ROLLBACK").unwrap();
        retry_backlog(&writer, &mut backlog, &mut state);
        assert!(backlog.is_empty());

        let work: u64 = blocker
            .query_row("SELECT SUM(work) FROM share_intervals", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(work, 512);

        drop((writer, blocker));
        std::fs::remove_file(db_path).ok();
    }

    /// Restarting inside a minute must not cost the work already done in it:
    /// the shutdown flush writes a partial row and the next run adds to it.
    #[test]
    fn a_restart_inside_a_minute_merges_rather_than_overwrites() {
        let db_path = make_temp_db();
        {
            let stats = ledger_stats(&db_path, 600, &[ADDRESS]);
            for _ in 0..3 {
                stats.record_ledger(ADDRESS, 100, 1, 0, 600);
            }
            stats.shutdown_persist();
        }
        {
            let stats = ledger_stats(&db_path, 600, &[ADDRESS]);
            for _ in 0..2 {
                stats.record_ledger(ADDRESS, 100, 1, 0, 630);
            }
            stats.shutdown_persist();

            // Both runs recorded into the same minute, and neither was lost.
            let points = stats.work_history(0, 60, LedgerScope::Pool);
            assert_eq!(points.len(), 1);
            assert_eq!(points[0].ts, 600);
            assert_eq!(points[0].work, 500);
            assert_eq!(points[0].accepted, 5);

            drop(stats);
        }
        std::fs::remove_file(db_path).ok();
    }

    /// Rolling up to hours is summation, so it must be lossless in every
    /// recorded quantity — and the union across the seam must not double-count
    /// or drop the hour it moved.
    #[test]
    fn the_hourly_rollup_preserves_every_total() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 600, &[ADDRESS]);

        // Three minutes inside one hour, old enough to be rolled up.
        let base = 3_600;
        for minute in 0..3 {
            ledger_shares(&stats, ADDRESS, 100, 2, base + minute * 60);
        }
        ledger_flush(&stats, base + 180);
        let before: u64 = stats
            .work_history(0, 3_600, LedgerScope::Pool)
            .iter()
            .map(|p| p.work)
            .sum();
        assert_eq!(before, 600);

        // A share past the retention horizon triggers the rollup.
        let far = base + LEDGER_FINE_RETENTION_SECS + 7_200;
        ledger_shares(&stats, ADDRESS, 50, 1, far);
        ledger_flush(&stats, far + 60);

        let store = stats.store.as_ref().unwrap();
        let minute_rows: u64 = store
            .read
            .lock()
            .query_row(
                "SELECT COUNT(*) FROM share_intervals WHERE ts < ?1",
                params![far - LEDGER_FINE_RETENTION_SECS],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(minute_rows, 0, "rolled-up minutes must not survive");

        // Totals are unchanged, and the rolled hour is still one bucket.
        let points = stats.work_history(0, 3_600, LedgerScope::Pool);
        let rolled = points.iter().find(|p| p.ts == base).expect("rolled hour");
        assert_eq!(rolled.work, 600);
        assert_eq!(rolled.accepted, 6);
        assert_eq!(
            points.iter().map(|p| p.work).sum::<u64>(),
            650,
            "the seam must not double-count or drop a bucket"
        );

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// A minute-grid query across the rollup seam returns each bucket with the
    /// span it actually covers. Dividing by the requested grid instead read
    /// rolled-up hours 60× high — the exact bug this field exists to prevent.
    #[test]
    fn rolled_up_buckets_carry_their_hourly_span() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 600, &[ADDRESS]);

        let base = 3_600;
        for minute in 0..3 {
            ledger_shares(&stats, ADDRESS, 100, 2, base + minute * 60);
        }
        ledger_flush(&stats, base + 180);
        let far = base + LEDGER_FINE_RETENTION_SECS + 7_200;
        ledger_shares(&stats, ADDRESS, 50, 1, far);
        ledger_flush(&stats, far + 60);

        let points = stats.work_history(0, 60, LedgerScope::Pool);
        let rolled = points.iter().find(|p| p.ts == base).expect("rolled hour");
        assert_eq!((rolled.work, rolled.span_secs), (600, 3_600));
        let fresh = points.iter().find(|p| p.ts == far).expect("fresh minute");
        assert_eq!((fresh.work, fresh.span_secs), (50, 60));

        // The implied rate is flat: the same per-minute work at both grains.
        let rate = |p: &WorkHistoryPoint| p.work as f64 / p.span_secs as f64;
        assert!((rate(rolled) - rate(fresh) * 0.2).abs() < 1e-12);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// The range start snaps down to the bucket grid: a bucket is either in
    /// the range whole or not at all, never silently missing its head.
    #[test]
    fn the_range_start_is_floored_to_the_bucket_grid() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 60, &[ADDRESS]);

        ledger_shares(&stats, ADDRESS, 100, 1, 60);
        ledger_shares(&stats, ADDRESS, 100, 1, 180);
        ledger_flush(&stats, 240);

        // `since` lands mid-bucket; both minutes belong to bucket 0.
        let points = stats.work_history(130, 300, LedgerScope::Pool);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].ts, 0);
        assert_eq!(points[0].work, 200);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// A bucket whose span has not fully elapsed holds a fraction of its final
    /// work; serving it would dent the right edge of every chart.
    #[test]
    fn a_partially_elapsed_bucket_is_not_served() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 600, &[ADDRESS]);

        ledger_shares(&stats, ADDRESS, 100, 1, 600);
        ledger_flush(&stats, 660);

        // Bucket [600, 900) is still open at 700 and closed at 900.
        assert!(stats
            .work_history_at(0, 300, 700, LedgerScope::Pool)
            .is_empty());
        assert_eq!(
            stats.work_history_at(0, 300, 900, LedgerScope::Pool).len(),
            1
        );

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// Rejects carry no work but are counted, so a device's reject ratio stays
    /// recoverable over any past range.
    #[test]
    fn rejects_are_counted_without_contributing_work() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 600, &[ADDRESS]);

        stats.record_ledger(ADDRESS, 500, 1, 0, 600);
        stats.record_ledger(ADDRESS, 0, 0, 1, 600);
        stats.flush_ledger_at(660);
        stats.store.as_ref().unwrap().flush();

        let points = stats.work_history(0, 60, LedgerScope::Pool);
        assert_eq!(points[0].work, 500);
        assert_eq!(points[0].accepted, 1);
        assert_eq!(points[0].rejected, 1);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// Worker rows are permanent and authorization is unauthenticated, so a
    /// name that never authorized must not be able to mint one.
    #[test]
    fn an_unauthorized_name_never_reaches_the_ledger() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 600, &[ADDRESS]);

        // The placeholder a pre-auth reject carries.
        stats.record_ledger("?", 0, 0, 1, 600);
        stats.record_ledger("bc1qneverauthorized", 1_000, 1, 0, 600);
        stats.flush_ledger_at(660);
        stats.store.as_ref().unwrap().flush();

        assert!(stats.work_history(0, 60, LedgerScope::Pool).is_empty());
        let store = stats.store.as_ref().unwrap();
        let users: u64 = store
            .read
            .lock()
            .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
            .unwrap();
        assert_eq!(users, 0);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// Work is summed credit, never the share's actual hash difficulty: hash
    /// difficulty is heavy-tailed, so a lucky share would otherwise show up as
    /// a hashrate spike that never happened.
    #[test]
    fn the_ledger_records_credit_not_hash_difficulty() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 600, &[ADDRESS]);

        // A share worth 1024 that happened to hash 500× harder.
        let later = PoolStats::now_secs() + 2 * LEDGER_INTERVAL_SECS;
        crate::mining::accounting::record_accepted(&stats, "session", ADDRESS, 1_024, 512_000);
        stats.flush_ledger_at(later);
        stats.store.as_ref().unwrap().flush();

        let work: u64 = stats
            .work_history_at(0, 60, later, LedgerScope::Pool)
            .iter()
            .map(|p| p.work)
            .sum();
        assert_eq!(work, 1_024);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// The ledger outlives the decayed caches it sits beside: those are pruned
    /// to a 48-hour horizon, while summed credit is kept whatever its age.
    #[test]
    fn pruning_the_decayed_caches_leaves_the_ledger_alone() {
        let db_path = make_temp_db();
        let base = 9_999_960;
        let stats = ledger_stats(&db_path, base, &[ADDRESS]);
        ledger_shares(&stats, ADDRESS, 100, 4, base);
        ledger_flush(&stats, base + LEDGER_INTERVAL_SECS);
        stats.record_hashrate_snapshot_at(base);

        // A snapshot far enough ahead to prune everything written above.
        stats.record_hashrate_snapshot_at(base + FINE_HISTORY_RETENTION_SECS + 60);
        stats.store.as_ref().unwrap().flush();

        let dropped = |points: Vec<RateHistoryPoint>| points.iter().all(|p| p.ts > base);
        assert!(dropped(stats.get_hashrate_history(0, 60)));
        assert!(dropped(stats.get_share_rate_history(0, 60)));
        assert_eq!(
            stats
                .work_history(0, 60, LedgerScope::Pool)
                .iter()
                .map(|p| p.work)
                .sum::<u64>(),
            400
        );

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// A minute only closes when the clock leaves it, and shares must land in
    /// the minute they arrived in rather than the one the flush happened in.
    #[test]
    fn shares_are_filed_under_the_minute_they_arrived_in() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 600, &[ADDRESS]);

        stats.record_ledger(ADDRESS, 10, 1, 0, 615);
        stats.record_ledger(ADDRESS, 10, 1, 0, 659);
        // Crossing into the next minute closes the first…
        stats.record_ledger(ADDRESS, 10, 1, 0, 661);
        stats.flush_ledger_at(721);
        stats.store.as_ref().unwrap().flush();

        let points = stats.work_history(0, 60, LedgerScope::Pool);
        assert_eq!(points.len(), 2);
        assert_eq!((points[0].ts, points[0].work), (600, 20));
        assert_eq!((points[1].ts, points[1].work), (660, 10));

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// A clock stepping backwards must not reopen a minute already flushed —
    /// the row would be written twice and, being accumulate-on-conflict,
    /// double-counted.
    #[test]
    fn a_backwards_clock_step_does_not_reopen_a_closed_minute() {
        let db_path = make_temp_db();
        let stats = ledger_stats(&db_path, 600, &[ADDRESS]);

        stats.record_ledger(ADDRESS, 10, 1, 0, 660);
        stats.record_ledger(ADDRESS, 10, 1, 0, 720);
        // Back into the already-closed minute.
        stats.record_ledger(ADDRESS, 10, 1, 0, 600);
        stats.flush_ledger_at(780);
        stats.store.as_ref().unwrap().flush();

        let points = stats.work_history(0, 60, LedgerScope::Pool);
        assert_eq!(points.iter().map(|p| p.work).sum::<u64>(), 30);
        assert!(
            points.iter().all(|p| p.ts >= 660),
            "a closed minute was reopened: {points:?}"
        );

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// Column order is hand-written twice (history and checkpoint) against a
    /// `W_*`-indexed array, and a swap there would mislabel every series
    /// silently. Also pins the grid split: history snaps to the sampling grid
    /// so retention's `ts % 60` filter can find it, while the checkpoint keeps
    /// the true time so the restore gap is right.
    #[test]
    fn share_rate_snapshot_maps_windows_to_columns() {
        let db_path = make_temp_db();
        let store = StatsStore::open(&db_path).unwrap();

        // A distinct value per window, so a transposition cannot pass.
        let mut rates = [0.0; hashrate::WINDOW_COUNT];
        rates[hashrate::W_1M] = 1.0;
        rates[hashrate::W_5M] = 2.0;
        rates[hashrate::W_10M] = 3.0;
        rates[hashrate::W_1H] = 4.0;
        rates[hashrate::W_3H] = 5.0;
        rates[hashrate::W_6H] = 6.0;
        rates[hashrate::W_24H] = 7.0;

        store.record_hashrate_snapshot(SnapshotWrite {
            history_ts: 120,
            state_ts: 125,
            share_rates: rates,
            ..Default::default()
        });
        store.flush();

        let history = store.get_share_rate_history(0, SNAPSHOT_INTERVAL_SECS);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].ts, 120);
        assert_eq!(history[0].one_minute, Some(1.0));
        assert_eq!(history[0].five_minutes, Some(2.0));
        assert_eq!(history[0].ten_minutes, Some(3.0));
        assert_eq!(history[0].one_hour, Some(4.0));
        assert_eq!(history[0].six_hours, Some(6.0));
        assert_eq!(history[0].twenty_four_hours, Some(7.0));

        // The checkpoint keeps all seven windows — including the 3h the chart
        // does not plot — at the unsnapped timestamp.
        let (updated_ts, restored) = store.load_share_rate_state().unwrap().expect("checkpoint");
        assert_eq!(updated_ts, 125);
        assert_eq!(restored, rates);

        drop(store);
        std::fs::remove_file(db_path).ok();
    }

    /// Share rate has to come back decayed across the downtime, exactly as the
    /// per-worker hashrates do — the whole point of checkpointing it rather
    /// than letting a restart reset the chart to zero.
    #[test]
    fn share_rate_resumes_after_restart_and_decays_while_offline() {
        let db_path = make_temp_db();
        let saved_at = 1_000_007;
        let restart_at = saved_at + 30;

        // Twenty minutes at a steady 5 shares/sec — 300 a minute — which is
        // long enough for the short windows to converge and leaves the long
        // ones part-filled.
        let saved = {
            let now = Instant::now();
            let stats = PoolStats::new_with_store_at(Some(db_path.clone()), saved_at, now);
            let mut tick = now;
            for _ in 0..600 {
                for _ in 0..(5 * hashrate::TICK_SECS) {
                    stats.share_accepted(1, 1);
                }
                tick += Duration::from_secs(hashrate::TICK_SECS);
                stats.tick_hashrates_at(tick);
            }
            let saved = stats.share_rate_windows();
            stats.record_hashrate_snapshot_at(saved_at);
            saved
        };
        assert!(
            (saved[hashrate::W_1M] - 300.0).abs() < 6.0,
            "1m should have converged on 300/min, got {}",
            saved[hashrate::W_1M]
        );

        let now = Instant::now();
        let stats = PoolStats::new_with_store_at(Some(db_path.clone()), restart_at, now);
        let expected =
            hashrate::HashrateDecay::restored_per_minute(now, saved, Duration::from_secs(30))
                .per_minute();
        let snap = stats.snapshot();

        assert!((snap.shares_per_minute_1m - expected[hashrate::W_1M]).abs() < 1e-9 * 300.0);
        assert!((snap.shares_per_minute_24h - expected[hashrate::W_24H]).abs() < 1e-9 * 300.0);
        // Half a time constant of silence bites the 1m window and barely
        // touches the 24h one.
        assert!(snap.shares_per_minute_1m > 0.0);
        assert!(snap.shares_per_minute_1m < 0.8 * saved[hashrate::W_1M]);
        assert!(snap.shares_per_minute_24h > 0.99 * saved[hashrate::W_24H]);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// "Shares per minute the pool is accepting" — a reject is not throughput,
    /// and counting one would make the chart disagree with the accepted card.
    #[test]
    fn share_rate_counts_accepted_shares_only() {
        let now = Instant::now();
        let stats = PoolStats::new_with_store_at(None, 1_000, now);

        for _ in 0..100 {
            stats.share_rejected("stale");
        }
        stats.tick_hashrates_at(now + Duration::from_secs(hashrate::TICK_SECS));
        assert_eq!(stats.snapshot().shares_per_minute_1m, 0.0);

        stats.share_accepted(1, 1);
        stats.tick_hashrates_at(now + Duration::from_secs(2 * hashrate::TICK_SECS));
        assert!(stats.snapshot().shares_per_minute_1m > 0.0);
    }

    /// Difficulty weights the hashrate estimate, not this one: two shares are
    /// two shares whatever they were worth.
    #[test]
    fn share_rate_ignores_share_difficulty() {
        let now = Instant::now();
        let tick = now + Duration::from_secs(hashrate::TICK_SECS);

        let small = PoolStats::new_with_store_at(None, 1_000, now);
        let large = PoolStats::new_with_store_at(None, 1_000, now);
        for _ in 0..10 {
            small.share_accepted(1, 1);
            large.share_accepted(1_000_000, 1_000_000);
        }
        small.tick_hashrates_at(tick);
        large.tick_hashrates_at(tick);

        assert_eq!(
            small.snapshot().shares_per_minute_1m,
            large.snapshot().shares_per_minute_1m
        );
    }

    /// Shutdown must not rely on `Drop`: in production every task holds an
    /// `Arc<PoolStats>`, so the store is never dropped. `shutdown_persist`
    /// has to leave the totals on disk while the instance is still alive.
    #[test]
    fn shutdown_persist_flushes_without_drop() {
        let db_path = make_temp_db();
        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        stats.share_accepted(1_000, 1_000);
        stats.share_accepted(1_000, 1_000);
        stats.share_rejected("stale");

        stats.shutdown_persist();

        // Read through a second connection while `stats` is still alive.
        let (accepted, rejected): (u64, u64) = Connection::open(&db_path)
            .unwrap()
            .query_row(
                "SELECT shares_accepted, shares_rejected
                 FROM round_stats WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(accepted, 2);
        assert_eq!(rejected, 1);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    #[test]
    fn prune_evicts_idle_offline_workers_but_not_online_or_recent() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("online", 1_000);
        stats.mark_worker_online("idle", 1_000);
        stats.mark_worker_offline("idle");
        stats.mark_worker_online("recent", 1_000);
        stats.mark_worker_offline("recent");

        // Age "idle" past the TTL; "recent" went offline just now.
        stats.worker_states.get_mut("idle").unwrap().connected_ts =
            PoolStats::now_secs() - IDLE_WORKER_EVICT_SECS - 60;

        stats.prune_idle_workers();

        assert!(stats.worker_states.get("online").is_some());
        assert!(stats.worker_states.get("recent").is_some());
        assert!(stats.worker_states.get("idle").is_none());
    }

    #[test]
    fn pool_difficulty_accumulates_credit_and_resets_when_a_block_is_found() {
        let stats = PoolStats::new_with_store(None);
        assert_eq!(stats.snapshot().pool_difficulty, 0);

        stats.share_accepted(1_000, 5_000);
        stats.share_accepted(2_500, 2_500);
        assert_eq!(stats.snapshot().pool_difficulty, 3_500);

        stats.block_found();
        assert_eq!(stats.snapshot().pool_difficulty, 0);

        stats.share_accepted(4_000, 4_000);
        assert_eq!(stats.snapshot().pool_difficulty, 4_000);
    }

    #[test]
    fn finding_a_block_starts_a_new_round_of_kpis() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("w", 1_000);
        stats.share_accepted(1_000, 9_000_000);
        stats.worker_share_accepted("w", 9_000_000);
        stats.share_rejected("stale");
        stats.record_best_hashrate(5.0e12);

        stats.block_found();

        let snap = stats.snapshot();
        assert_eq!(snap.blocks_found, 1);
        assert_eq!(snap.round_shares_accepted, 0);
        assert_eq!(snap.round_shares_rejected, 0);
        assert!(snap.round_reject_reasons.is_empty());
        assert_eq!(snap.best_share_difficulty, 0);
        assert_eq!(snap.best_hashrate_hps, 0.0);
        assert_eq!(snap.pool_difficulty, 0);
        let w = snap.worker_states.iter().find(|w| w.worker == "w").unwrap();
        assert_eq!(w.best_share_difficulty, 0);

        // Session counters describe this boot, not the round: they keep going.
        assert_eq!(snap.shares_accepted, 1);
        assert_eq!(snap.shares_rejected, 1);
        assert_eq!(snap.session_best_share_difficulty, 9_000_000);
        assert_eq!(snap.session_best_hashrate_hps, 5.0e12);

        // The next round accumulates from zero.
        stats.share_accepted(500, 500);
        stats.share_rejected("duplicate");
        let snap = stats.snapshot();
        assert_eq!(snap.round_shares_accepted, 1);
        assert_eq!(snap.round_shares_rejected, 1);
        assert_eq!(snap.round_reject_reasons.get("duplicate"), Some(&1));
        assert_eq!(snap.round_reject_reasons.get("stale"), None);
        assert_eq!(snap.pool_difficulty, 500);
        assert_eq!(snap.best_share_difficulty, 500);
    }

    #[test]
    fn a_round_reset_survives_a_restart() {
        let db_path = make_temp_db();
        {
            let stats = PoolStats::new_with_store(Some(db_path.clone()));
            stats.mark_worker_online("w", 1_000);
            stats.share_accepted(2_000, 8_000);
            stats.worker_share_accepted("w", 8_000);
            stats.share_rejected("stale");
            stats.record_hashrate_snapshot_at(120);
            stats.block_found();
            stats.shutdown_persist();
        }

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        let snap = stats.snapshot();
        assert_eq!(snap.round_shares_accepted, 0);
        assert_eq!(snap.round_shares_rejected, 0);
        assert!(snap.round_reject_reasons.is_empty());
        assert_eq!(snap.best_share_difficulty, 0);
        assert_eq!(snap.pool_difficulty, 0);
        assert!(snap
            .worker_states
            .iter()
            .all(|w| w.best_share_difficulty == 0));

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    #[test]
    fn a_stale_snapshot_cannot_resurrect_a_reset_round() {
        let db_path = make_temp_db();
        let store = StatsStore::open(&db_path).unwrap();
        store.round_reset(1, 1_000);
        // Assembled before the reset (epoch 0), applied after it.
        store.record_hashrate_snapshot(SnapshotWrite {
            history_ts: 60,
            state_ts: 60,
            share_totals: ShareTotals {
                accepted: 999,
                rejected: 9,
            },
            round_work: 12_345,
            round_epoch: 0,
            ..Default::default()
        });
        store.flush();

        let (totals, since_ts, round_work) = store.load_round_stats().unwrap();
        assert_eq!(totals.accepted, 0);
        assert_eq!(totals.rejected, 0);
        assert_eq!(round_work, 0);
        assert_eq!(since_ts, 1_000);

        drop(store);
        std::fs::remove_file(db_path).ok();
    }

    #[test]
    fn worker_rejects_are_counted_per_reason() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("w", 1_000);

        stats.worker_share_rejected("w", "stale");
        stats.worker_share_rejected("w", "stale");
        stats.worker_share_rejected("w", "low_difficulty");
        stats.worker_share_rejected("w", "duplicate");

        let state = stats.worker_states.get("w").unwrap();
        assert_eq!(state.shares_rejected, 4);
        assert_eq!(state.shares_stale, 2);
        assert_eq!(state.reject_reasons.get("stale"), Some(&2));
        assert_eq!(state.reject_reasons.get("low_difficulty"), Some(&1));
        assert_eq!(state.reject_reasons.get("duplicate"), Some(&1));
        assert_eq!(state.reject_reasons.get("invalid"), None);
    }

    #[test]
    fn worker_best_shares_bounded_in_memory_and_on_disk() {
        let db_path = make_temp_db();
        {
            let stats = PoolStats::new_with_store(Some(db_path.clone()));
            for i in 0..MAX_WORKER_BEST_SHARES + 100 {
                stats.worker_share_accepted(&format!("w{i}"), i as u64 + 1);
            }
            stats.prune_idle_workers();
            assert_eq!(stats.worker_best_shares.len(), MAX_WORKER_BEST_SHARES);
            // Highest difficulties survive.
            assert!(stats
                .worker_best_shares
                .get(&format!("w{}", MAX_WORKER_BEST_SHARES + 99))
                .is_some());
            assert!(stats.worker_best_shares.get("w0").is_none());
        }
        // Reopen: boot-time prune + load stay within the cap.
        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        assert!(stats.worker_best_shares.len() <= MAX_WORKER_BEST_SHARES);

        std::fs::remove_file(db_path).ok();
    }

    #[test]
    fn worker_best_share_is_persisted_across_instances() {
        let db_path = make_temp_db();

        {
            let stats = PoolStats::new_with_store(Some(db_path.clone()));
            stats.mark_worker_online("w1", 1_000);
            stats.worker_share_accepted("w1", 1000);
            stats.worker_share_accepted("w1", 4000);

            let ss = stats.snapshot();
            let w1 = ss.worker_states.iter().find(|w| w.worker == "w1").unwrap();
            assert_eq!(w1.best_share_difficulty, 4000);
        }

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        let ss = stats.snapshot();
        let w1 = ss.worker_states.iter().find(|w| w.worker == "w1").unwrap();
        assert_eq!(w1.best_share_difficulty, 4000);

        std::fs::remove_file(db_path).ok();
    }

    /// Confirmation takes an hour at the default depth. A restart inside that
    /// window must not be all it takes for a reorg to go unnoticed.
    #[test]
    fn a_block_awaiting_confirmation_survives_a_restart() {
        let db_path = make_temp_db();

        {
            let stats = PoolStats::new_with_store(Some(db_path.clone()));
            stats.block_found();
            stats.enroll_pending_block(800_000, "0000cafe", "w1", "bc1qpayout", true);
            assert_eq!(stats.snapshot().blocks_pending_confirmation, 1);
        }

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        let pending = stats.pending_blocks();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].hash, "0000cafe");
        assert_eq!(pending[0].height, 800_000);
        assert_eq!(pending[0].worker, "w1");
        assert_eq!(pending[0].payout, "bc1qpayout");
        assert!(pending[0].won_at_submit);
        // The count it was claimed under comes back with it.
        assert_eq!(stats.snapshot().blocks_found, 1);

        std::fs::remove_file(db_path).ok();
    }

    /// The ledger, not the process lifetime, is the authority on how many
    /// blocks the pool has kept — which is what the dashboard's "found blocks
    /// survive restarts" has always claimed.
    #[test]
    fn resolved_block_counts_are_restored_from_the_ledger() {
        let db_path = make_temp_db();

        {
            let stats = PoolStats::new_with_store(Some(db_path.clone()));
            // One win that stands, one win that is reorged out, one block that
            // lost its race and is then promoted by a later reorg.
            for (hash, won) in [("aa", true), ("bb", true), ("cc", false)] {
                stats.enroll_pending_block(800_000, hash, "w1", "bc1qpayout", won);
                if won {
                    stats.block_found();
                } else {
                    stats.block_inconclusive();
                }
            }
            let by_hash = |h: &str| {
                stats
                    .pending_blocks()
                    .into_iter()
                    .find(|b| b.hash == h)
                    .unwrap()
            };
            stats.resolve_block(&by_hash("aa"), BlockResolution::Confirmed);
            stats.resolve_block(&by_hash("bb"), BlockResolution::Orphaned);
            stats.resolve_block(&by_hash("cc"), BlockResolution::Confirmed);

            let snap = stats.snapshot();
            assert_eq!(snap.blocks_found, 2);
            assert_eq!(snap.blocks_orphaned, 1);
            assert_eq!(snap.blocks_inconclusive, 0);
        }

        let snap = PoolStats::new_with_store(Some(db_path.clone())).snapshot();
        assert_eq!(snap.blocks_found, 2);
        assert_eq!(snap.blocks_orphaned, 1);
        assert_eq!(snap.blocks_inconclusive, 0);
        assert_eq!(snap.blocks_pending_confirmation, 0);

        std::fs::remove_file(db_path).ok();
    }

    const TH: f64 = 1e12;

    /// Drive a session at `rate` H/s for `secs` seconds of ticks, returning the
    /// simulated clock. Mirrors what the background tick task does in
    /// production, but on a clock the test controls so nothing has to sleep.
    fn feed(
        stats: &PoolStats,
        session: &str,
        worker: &str,
        rate: f64,
        secs: u64,
        start: Instant,
    ) -> Instant {
        let step = hashrate::TICK_SECS;
        let per_tick = rate * step as f64 / hashrate::NONCES;
        let mut now = start;
        for _ in 0..(secs / step) {
            stats.add_share_diff_at(session, worker, per_tick, now);
            now += Duration::from_secs(step);
            stats.tick_hashrates_at(now);
        }
        now
    }

    /// Tick without any shares for `secs` seconds.
    fn idle(stats: &PoolStats, secs: u64, start: Instant) -> Instant {
        let step = hashrate::TICK_SECS;
        let mut now = start;
        for _ in 0..(secs / step) {
            now += Duration::from_secs(step);
            stats.tick_hashrates_at(now);
        }
        now
    }

    /// `to_windows` feeds the Prometheus `window` label positionally, so a
    /// mismatch between the field order and `HASHRATE_WINDOW_LABELS` would
    /// mislabel every exported series without failing anything else.
    #[test]
    fn window_array_order_matches_the_metric_labels() {
        let rates = HashrateWindows {
            one_minute: 1.0,
            five_minutes: 5.0,
            ten_minutes: 10.0,
            one_hour: 60.0,
            three_hours: 180.0,
            six_hours: 360.0,
            twenty_four_hours: 1440.0,
        };
        // Each value is its window's length in minutes, so the array doubles as
        // an assertion that every label lands on the right figure.
        let expected = [
            ("1m", 1.0),
            ("5m", 5.0),
            ("10m", 10.0),
            ("1h", 60.0),
            ("3h", 180.0),
            ("6h", 360.0),
            ("24h", 1440.0),
        ];
        let values = rates.to_windows();
        assert_eq!(values.len(), crate::metrics::HASHRATE_WINDOW_LABELS.len());
        for (i, (label, minutes)) in expected.iter().enumerate() {
            assert_eq!(crate::metrics::HASHRATE_WINDOW_LABELS[i], *label);
            assert_eq!(values[i], *minutes, "{label} label is on the wrong field");
        }

        // Round-trips, so the read path and the export path agree.
        let back = HashrateWindows::from_windows(values);
        assert_eq!(back.one_minute, rates.one_minute);
        assert_eq!(back.three_hours, rates.three_hours);
        assert_eq!(back.twenty_four_hours, rates.twenty_four_hours);
    }

    /// On a young pool the long windows must lag the short ones. Dividing by
    /// the age of the oldest share in the window rather than by the window
    /// itself would collapse every window to the same number.
    #[test]
    fn windows_do_not_collapse_on_a_young_pool() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("axe", 1024);
        feed(&stats, "s1", "axe", TH, 60, Instant::now());

        let ss = stats.snapshot();
        assert!(ss.total_hashrate_60s > 0.5 * TH);
        assert!(
            ss.total_hashrate_24h < ss.total_hashrate_60s / 100.0,
            "24h ({:e}) should still be far behind 1m ({:e}) after a minute",
            ss.total_hashrate_24h,
            ss.total_hashrate_60s
        );
        assert!(ss.total_hashrate_60s > ss.total_hashrate_5m);
        assert!(ss.total_hashrate_5m > ss.total_hashrate_10m);
        assert!(ss.total_hashrate_10m > ss.total_hashrate_1h);
        assert!(ss.total_hashrate_1h > ss.total_hashrate_6h);
        assert!(ss.total_hashrate_6h > ss.total_hashrate_24h);
    }

    /// A miner that goes quiet decays out of the short windows first while the
    /// long ones barely notice.
    #[test]
    fn idle_worker_decays_short_windows_first() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("axe", 1024);
        // Long enough for every window to settle on 1 TH/s.
        let now = feed(&stats, "s1", "axe", TH, 5 * 86_400, Instant::now());
        stats.mark_worker_offline("axe");

        // 700s of silence: many 1m time constants, a fraction of a 24h one.
        idle(&stats, 700, now);

        // 700s is ~11.7 one-minute time constants but only ~2.3 five-minute
        // ones and 0.19 of an hour, so the windows should be far apart.
        let ss = stats.snapshot();
        assert!(ss.total_hashrate_60s < TH / 10_000.0);
        assert!(
            (0.05 * TH..0.15 * TH).contains(&ss.total_hashrate_5m),
            "5m read {:e} H/s, expected ~0.10 TH/s",
            ss.total_hashrate_5m
        );
        assert!(ss.total_hashrate_1h > TH / 2.0);
        assert!(ss.total_hashrate_24h > TH * 0.98);

        // Per-worker rows decay the same way, in both dashboard shapes.
        let row = &ss.worker_hashrates[0];
        assert!(row.hashrate_60s_hps < TH / 10_000.0);
        let state = ss.worker_states.iter().find(|w| w.worker == "axe").unwrap();
        assert!(state.hashrate_1h_hps > 0.0);
        assert!(state.hashrate_6h_hps > 0.0);
    }

    /// A still-*connected* worker must decay like any other; exempting
    /// connected sessions would leave a rig whose hasher died showing its last
    /// reading forever.
    #[test]
    fn connected_but_silent_worker_still_decays() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("axe", 1024);
        let now = feed(&stats, "s1", "axe", TH, 3_600, Instant::now());
        assert!(stats.snapshot().total_hashrate_60s > 0.9 * TH);

        // Still marked online — no mark_worker_offline call.
        idle(&stats, 600, now);

        let ss = stats.snapshot();
        assert!(
            ss.total_hashrate_60s < TH / 10_000.0,
            "a connected but silent worker still read {:e} H/s",
            ss.total_hashrate_60s
        );
    }

    /// Everything eventually reaches zero and the session is evicted.
    #[test]
    fn long_silence_zeroes_every_window() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("axe", 1024);
        let now = feed(&stats, "s1", "axe", TH, 5 * 86_400, Instant::now());
        stats.mark_worker_offline("axe");

        // Long enough for even the 24h window to cross the underflow floor
        // decay_time clamps to zero.
        idle(&stats, 60 * 86_400, now);

        let ss = stats.snapshot();
        assert_eq!(ss.total_hashrate_60s, 0.0);
        assert_eq!(ss.total_hashrate_24h, 0.0);
        assert!(
            stats.session_hashrates.is_empty(),
            "fully decayed session was not evicted"
        );
    }

    /// Two rigs sharing one worker name must add up. Keying the hashrate map
    /// by worker name with `insert` would let the second session silently
    /// replace the first, reading half of reality into the pool total.
    #[test]
    fn two_sessions_under_one_worker_name_sum() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("axe", 1024);
        stats.mark_worker_online("axe", 1024);

        let start = Instant::now();
        let step = hashrate::TICK_SECS;
        let per_tick = 5.0 * TH * step as f64 / hashrate::NONCES;
        let mut now = start;
        for _ in 0..(3_600 / step) {
            stats.add_share_diff_at("s1", "axe", per_tick, now);
            stats.add_share_diff_at("s2", "axe", per_tick, now);
            now += Duration::from_secs(step);
            stats.tick_hashrates_at(now);
        }

        let ss = stats.snapshot();
        let error = (ss.total_hashrate_60s - 10.0 * TH).abs() / (10.0 * TH);
        assert!(
            error < 0.01,
            "two 5 TH/s sessions summed to {:e} H/s",
            ss.total_hashrate_60s
        );
        // And the worker row reports the combined figure, not one session's.
        let state = ss.worker_states.iter().find(|w| w.worker == "axe").unwrap();
        assert!((state.hashrate_60s_hps - 10.0 * TH).abs() / (10.0 * TH) < 0.01);
    }

    /// A departed worker's decaying tail must not keep the pool total — and so
    /// the persisted best-hashrate watermark — propped up.
    #[test]
    fn faded_worker_does_not_inflate_best_hashrate_watermark() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("a", 1024);
        let now = feed(&stats, "s1", "a", TH, 5 * 86_400, Instant::now());
        assert!(stats.snapshot().session_best_hashrate_hps > 0.9 * TH);
        stats.mark_worker_offline("a");
        let now = idle(&stats, 40 * 86_400, now);

        // Worker "b" now hashes at the same rate. The watermark must stay at
        // one worker's output, not two.
        stats.mark_worker_online("b", 1024);
        feed(&stats, "s2", "b", TH, 5 * 86_400, now);

        let best = stats.snapshot().session_best_hashrate_hps;
        assert!(
            best < 1.1 * TH,
            "watermark reached {best:e} H/s for a pool that never exceeded 1 TH/s"
        );
    }

    /// A burst of shares arriving microseconds before a tick cannot spike the
    /// pool total: the per-share ceiling bounds the sum.
    #[test]
    fn burst_of_shares_cannot_spike_the_pool_total() {
        let stats = PoolStats::new_with_store(None);
        stats.mark_worker_online("axe", 1024);

        let now = Instant::now();
        for _ in 0..10 {
            stats.add_share_diff_at("s1", "axe", 4_700.0, now);
        }
        stats.tick_hashrates_at(now);

        let ss = stats.snapshot();
        let ceiling = 10.0 * 4_700.0 / 60.0 * hashrate::NONCES;
        assert!(
            ss.total_hashrate_60s <= ceiling,
            "1m read {:e} H/s, above the {ceiling:e} H/s ceiling",
            ss.total_hashrate_60s
        );
    }
}
