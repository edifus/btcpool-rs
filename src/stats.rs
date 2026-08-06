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
use tracing::{info, warn};

/// Offline workers idle longer than this are evicted from the in-memory stats
/// maps (their persisted best share survives, subject to the cap below). Keeps
/// per-message stats work and dashboard payloads bounded against connections
/// that mint many distinct worker names.
const IDLE_WORKER_EVICT_SECS: u64 = 86_400;

/// Maximum rows kept in `worker_best_shares` (in memory and in SQLite), keeping
/// the highest difficulties. Bounds boot-time load and dashboard growth; a solo
/// operator's real fleet is far below this.
const MAX_WORKER_BEST_SHARES: usize = 512;

/// Bumped whenever the hashrate estimator changes in a way that makes stored
/// history incomparable with newly recorded values. See `migrate_hashrate_algo`.
/// v1: ckpool-style decaying averages, replacing the sliding-window estimator.
const HASHRATE_ALGO_VERSION: u64 = 1;

/// How often the pool hashrate is written to `hashrate_history`. The dashboard
/// polls the chart at the same cadence.
pub const SNAPSHOT_INTERVAL_SECS: u64 = 10;

/// How long samples are kept at `SNAPSHOT_INTERVAL_SECS` resolution before
/// being thinned to one a minute.
const FINE_HISTORY_RETENTION_SECS: u64 = 48 * 3600;

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

#[derive(Debug, Clone, Copy)]
pub struct HashrateHistoryPoint {
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
// Persistent store for all-time metrics
// ─────────────────────────────────────────────────────────────────────────────

/// A persisted update, applied by the writer thread.
///
/// Every write originates on an async task — the share hot path, the snapshot
/// ticker, the pruner — so none of them may touch the disk directly. They hand
/// the work to one thread that owns the write connection instead, which also
/// removes the lock contention that used to put dashboard queries and share
/// submissions on the same mutex.
enum StoreWrite {
    BestShare(u64),
    BestHashrate(f64),
    WorkerBestShare {
        worker: String,
        difficulty: u64,
    },
    Snapshot {
        history_ts: u64,
        state_ts: u64,
        rates: HashrateWindows,
        worker_rates: HashMap<String, HashrateWindows>,
    },
    PruneWorkerBestShares(usize),
    /// Barrier: acknowledged once every write queued before it has been
    /// applied. Production flushes by dropping the store instead.
    #[cfg(test)]
    Flush(std::sync::mpsc::Sender<()>),
}

/// Pending writes allowed before new ones are dropped. Each is a few hundred
/// bytes and the writer drains them in microseconds; a backlog this deep means
/// the disk is gone, and dropping a best-share update is better than stalling
/// the share path.
const WRITE_QUEUE_DEPTH: usize = 256;

struct StatsStore {
    /// Read connection. Only touched from `spawn_blocking` (dashboard queries)
    /// and at boot, so it never blocks a runtime worker thread.
    read: Mutex<Connection>,
    /// `None` only while dropping, which is what closes the channel and lets
    /// the writer thread finish.
    writes: Option<std::sync::mpsc::SyncSender<StoreWrite>>,
    writer: Option<std::thread::JoinHandle<()>>,
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
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }

    fn open(path: &str) -> Result<Self, rusqlite::Error> {
        let conn = Self::connect(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS pool_stats (
             id INTEGER PRIMARY KEY CHECK(id = 1),
             best_share_difficulty INTEGER NOT NULL,
             best_hashrate_hps REAL NOT NULL
             )",
            [],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO pool_stats (id, best_share_difficulty, best_hashrate_hps)
             VALUES (1, 0, 0.0)",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS worker_best_shares (
             worker TEXT PRIMARY KEY,
             best_share_difficulty INTEGER NOT NULL
             )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS hashrate_history (
             ts INTEGER PRIMARY KEY,
             hashrate_hps REAL NOT NULL,
             hashrate_1m_hps REAL,
             hashrate_5m_hps REAL,
             hashrate_1h_hps REAL,
             hashrate_6h_hps REAL,
             hashrate_24h_hps REAL
             )",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS worker_hashrate_state (
             worker TEXT PRIMARY KEY,
             updated_ts INTEGER NOT NULL,
             hashrate_1m_hps REAL NOT NULL,
             hashrate_5m_hps REAL NOT NULL,
             hashrate_10m_hps REAL NOT NULL,
             hashrate_1h_hps REAL NOT NULL,
             hashrate_3h_hps REAL NOT NULL,
             hashrate_6h_hps REAL NOT NULL,
             hashrate_24h_hps REAL NOT NULL
             )",
            [],
        )?;
        Self::migrate_hashrate_history(&conn)?;
        Self::migrate_hashrate_algo(&conn)?;

        // Enforce the row cap at boot, synchronously and before the writer
        // thread exists, so an attacker-inflated table from a previous run is
        // trimmed before `load_values` pulls it into RAM.
        prune_worker_best_shares(&conn, MAX_WORKER_BEST_SHARES);

        let writer_conn = Self::connect(path)?;
        let (writes, rx) = std::sync::mpsc::sync_channel(WRITE_QUEUE_DEPTH);
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

    /// Queue a write. Never blocks: persistence is best-effort next to serving
    /// miners, so a wedged disk costs a stat, not a share.
    fn enqueue(&self, write: StoreWrite) {
        use std::sync::mpsc::TrySendError;
        let Some(writes) = self.writes.as_ref() else {
            return; // shutting down
        };
        match writes.try_send(write) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                warn!("Stats writer queue full — dropping a persisted stats update")
            }
            Err(TrySendError::Disconnected(_)) => {
                static GONE: std::sync::Once = std::sync::Once::new();
                GONE.call_once(|| {
                    warn!("Stats writer thread has stopped; stats are no longer persisted")
                });
            }
        }
    }

    /// Block until every write queued so far has been applied.
    #[cfg(test)]
    fn flush(&self) {
        let (ack, done) = std::sync::mpsc::channel();
        if let Some(writes) = self.writes.as_ref() {
            if writes.send(StoreWrite::Flush(ack)).is_ok() {
                let _ = done.recv();
            }
        }
    }

    fn load_values(
        &self,
    ) -> Result<(u64, f64, std::collections::HashMap<String, u64>), rusqlite::Error> {
        let conn = self.read.lock();
        let mut stmt = conn.prepare(
            "SELECT best_share_difficulty, best_hashrate_hps FROM pool_stats WHERE id = 1",
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
            conn.prepare("SELECT worker, best_share_difficulty FROM worker_best_shares")?;
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

    fn record_hashrate_snapshot(
        &self,
        history_ts: u64,
        state_ts: u64,
        rates: HashrateWindows,
        worker_rates: &HashMap<String, HashrateWindows>,
    ) {
        self.enqueue(StoreWrite::Snapshot {
            history_ts,
            state_ts,
            rates,
            worker_rates: worker_rates.clone(),
        });
    }

    fn migrate_hashrate_history(conn: &Connection) -> Result<(), rusqlite::Error> {
        let mut stmt = conn.prepare("PRAGMA table_info(hashrate_history)")?;
        let existing: std::collections::HashSet<String> = stmt
            .query_map([], |row| row.get(1))?
            .filter_map(Result::ok)
            .collect();
        drop(stmt);

        for column in [
            "hashrate_1m_hps",
            "hashrate_5m_hps",
            "hashrate_1h_hps",
            "hashrate_6h_hps",
            "hashrate_24h_hps",
        ] {
            if !existing.contains(column) {
                conn.execute(
                    &format!("ALTER TABLE hashrate_history ADD COLUMN {column} REAL"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    /// Discard history written by an earlier hashrate estimator.
    ///
    /// Values from a different estimator are not comparable with the current
    /// one, and the sliding-window estimator this replaced could emit readings
    /// orders of magnitude too high when a lone share landed in a window — a
    /// single such sample pins the chart's y-axis and permanently poisons the
    /// all-time best-hashrate watermark. Rather than try to filter them, drop
    /// the series and start clean. Found blocks, best shares and per-worker
    /// bests are keyed elsewhere and survive.
    fn migrate_hashrate_algo(conn: &Connection) -> Result<(), rusqlite::Error> {
        let mut stmt = conn.prepare("PRAGMA table_info(pool_stats)")?;
        let existing: std::collections::HashSet<String> = stmt
            .query_map([], |row| row.get(1))?
            .filter_map(Result::ok)
            .collect();
        drop(stmt);

        if !existing.contains("hashrate_algo_version") {
            conn.execute(
                "ALTER TABLE pool_stats ADD COLUMN hashrate_algo_version INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

        let stored: u64 = conn
            .query_row(
                "SELECT hashrate_algo_version FROM pool_stats WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        if stored >= HASHRATE_ALGO_VERSION {
            return Ok(());
        }

        let dropped_history = conn.execute("DELETE FROM hashrate_history", [])?;
        let dropped_state = conn.execute("DELETE FROM worker_hashrate_state", [])?;
        conn.execute(
            "UPDATE pool_stats
             SET best_hashrate_hps = 0.0, hashrate_algo_version = ?1
             WHERE id = 1",
            params![HASHRATE_ALGO_VERSION],
        )?;
        if dropped_history > 0 || dropped_state > 0 {
            info!(
                "Hashrate estimator changed (v{stored} → v{HASHRATE_ALGO_VERSION}): \
                 dropped {dropped_history} incomparable history rows and {dropped_state} state rows, \
                 and reset the best-hashrate watermark"
            );
        }
        Ok(())
    }

    fn get_hashrate_history(&self, since_ts: u64, bucket_secs: u64) -> Vec<HashrateHistoryPoint> {
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
            Ok(HashrateHistoryPoint {
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
fn writer_loop(conn: Connection, rx: std::sync::mpsc::Receiver<StoreWrite>) {
    while let Ok(write) = rx.recv() {
        if let Err(e) = apply_write(&conn, write) {
            warn!("Failed to persist stats update: {e}");
        }
    }
}

fn apply_write(conn: &Connection, write: StoreWrite) -> Result<(), rusqlite::Error> {
    match write {
        // The `?1 > ...` guards keep the watermarks monotonic at the SQL level:
        // updates can be queued out of order, and a stale lower value must not
        // overwrite a higher one already persisted.
        StoreWrite::BestShare(difficulty) => {
            conn.execute(
                "UPDATE pool_stats SET best_share_difficulty = ?1
                 WHERE id = 1 AND ?1 > best_share_difficulty",
                params![difficulty],
            )?;
        }
        StoreWrite::BestHashrate(hps) => {
            conn.execute(
                "UPDATE pool_stats SET best_hashrate_hps = ?1
                 WHERE id = 1 AND ?1 > best_hashrate_hps",
                params![hps],
            )?;
        }
        StoreWrite::WorkerBestShare { worker, difficulty } => {
            conn.execute(
                "INSERT INTO worker_best_shares (worker, best_share_difficulty) VALUES (?1, ?2)
                 ON CONFLICT(worker) DO UPDATE SET best_share_difficulty = excluded.best_share_difficulty
                 WHERE excluded.best_share_difficulty > worker_best_shares.best_share_difficulty",
                params![worker, difficulty],
            )?;
        }
        StoreWrite::PruneWorkerBestShares(keep) => {
            prune_worker_best_shares(conn, keep);
        }
        StoreWrite::Snapshot {
            history_ts,
            state_ts,
            rates,
            worker_rates,
        } => write_snapshot(conn, history_ts, state_ts, rates, &worker_rates)?,
        #[cfg(test)]
        StoreWrite::Flush(ack) => {
            let _ = ack.send(());
        }
    }
    Ok(())
}

fn write_snapshot(
    conn: &Connection,
    history_ts: u64,
    state_ts: u64,
    rates: HashrateWindows,
    worker_rates: &HashMap<String, HashrateWindows>,
) -> Result<(), rusqlite::Error> {
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
        tx.commit()?;
    }

    // Thin samples past the fine-grained retention down to one a minute.
    // Charts covering more than a couple of days bucket at 5 minutes or
    // coarser anyway, so the extra resolution buys nothing while the row
    // count grows by SNAPSHOT_INTERVAL_SECS⁻¹ every second.
    let fine_cutoff = history_ts.saturating_sub(FINE_HISTORY_RETENTION_SECS);
    conn.execute(
        "DELETE FROM hashrate_history WHERE ts < ?1 AND ts % 60 != 0",
        params![fine_cutoff],
    )?;
    // Prune entries older than 6 months
    let cutoff = history_ts.saturating_sub(6 * 30 * 24 * 3600);
    conn.execute(
        "DELETE FROM hashrate_history WHERE ts < ?1",
        params![cutoff],
    )?;
    Ok(())
}

/// Keep only the `keep` highest-difficulty rows in `worker_best_shares`.
fn prune_worker_best_shares(conn: &Connection, keep: usize) {
    match conn.execute(
        "DELETE FROM worker_best_shares WHERE worker NOT IN (
           SELECT worker FROM worker_best_shares
           ORDER BY best_share_difficulty DESC LIMIT ?1
         )",
        params![keep as i64],
    ) {
        Ok(0) => {}
        Ok(n) => info!("Pruned {n} stale worker_best_shares rows (cap {keep})"),
        Err(e) => warn!("Failed to prune worker_best_shares: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PoolStats
// ─────────────────────────────────────────────────────────────────────────────

pub struct PoolStats {
    shares_accepted: AtomicU64,
    shares_rejected: AtomicU64,
    blocks_found: AtomicU64,
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
    last_block_worker: Mutex<Option<String>>,
    last_block_payout: Mutex<Option<String>>,
    last_block_hash: Mutex<Option<String>>,
    last_block_ts: AtomicU64,
    /// Per-connection decaying hashrate state, keyed by session id. Shares are
    /// accumulated here as they arrive and folded into the averages by
    /// `tick_hashrates` on a fixed cadence; an entry that stops receiving
    /// shares decays to zero on its own and is then evicted, so a miner that
    /// goes quiet — or drops its connection — falls off without any special
    /// case.
    session_hashrates: DashMap<String, SessionHashrate>,
    worker_protocol: DashMap<String, String>,
    worker_last_submit_ts: DashMap<String, u64>,
    worker_best_shares: DashMap<String, u64>,
    worker_states: DashMap<String, WorkerState>,
    start_time: Instant,
    store: Option<StatsStore>,
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

impl PoolStats {
    pub fn new_with_store(stats_db_path: Option<String>) -> Arc<Self> {
        Self::new_with_store_at(stats_db_path, Self::now_secs(), Instant::now())
    }

    fn new_with_store_at(
        stats_db_path: Option<String>,
        wall_now: u64,
        instant_now: Instant,
    ) -> Arc<Self> {
        let (
            store,
            best_share_difficulty,
            best_hashrate_hps,
            worker_best_shares_map,
            persisted_hashrates,
        ) = match stats_db_path.filter(|p| !p.is_empty()) {
            Some(path) => match StatsStore::open(&path) {
                Ok(store) => match store.load_values() {
                    Ok((best_difficulty, best_hps, worker_best_shares_map)) => {
                        let persisted_hashrates = match store.load_hashrate_state() {
                            Ok(state) => state,
                            Err(e) => {
                                warn!("Failed to restore hashrates from DB {}: {e}", path);
                                Vec::new()
                            }
                        };
                        (
                            Some(store),
                            best_difficulty,
                            best_hps,
                            worker_best_shares_map,
                            persisted_hashrates,
                        )
                    }
                    Err(e) => {
                        warn!("Failed to load stats from DB {}: {e}", path);
                        (None, 0, 0.0, HashMap::new(), Vec::new())
                    }
                },
                Err(e) => {
                    warn!("Failed to open stats DB {}: {e}", path);
                    (None, 0, 0.0, HashMap::new(), Vec::new())
                }
            },
            None => (None, 0, 0.0, HashMap::new(), Vec::new()),
        };

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

        Arc::new(Self {
            shares_accepted: AtomicU64::new(0),
            shares_rejected: AtomicU64::new(0),
            blocks_found: AtomicU64::new(0),
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
            session_hashrates,
            worker_protocol: DashMap::new(),
            worker_last_submit_ts: DashMap::new(),
            worker_best_shares,
            worker_states: DashMap::new(),
            last_block_worker: Mutex::new(None),
            last_block_payout: Mutex::new(None),
            last_block_hash: Mutex::new(None),
            last_block_ts: AtomicU64::new(0),
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

    pub fn share_accepted(&self, difficulty: u64) {
        self.shares_accepted.fetch_add(1, Ordering::Relaxed);

        // CAS loop to track all-time best share
        let mut prev = self.best_share_difficulty.load(Ordering::Relaxed);
        while difficulty > prev {
            match self.best_share_difficulty.compare_exchange_weak(
                prev,
                difficulty,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.persist_best_share_difficulty(difficulty);
                    break;
                }
                Err(x) => prev = x,
            }
        }

        // Session best share
        let mut prev_session_best = self.session_best_share_difficulty.load(Ordering::Relaxed);
        while difficulty > prev_session_best {
            match self.session_best_share_difficulty.compare_exchange_weak(
                prev_session_best,
                difficulty,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => prev_session_best = x,
            }
        }
    }

    pub fn share_rejected(&self) {
        self.shares_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn block_found(&self, worker: &str, payout: &str, hash: &str) {
        self.blocks_found.fetch_add(1, Ordering::Relaxed);
        *self.last_block_worker.lock() = Some(worker.to_string());
        *self.last_block_payout.lock() = Some(payout.to_string());
        *self.last_block_hash.lock() = Some(hash.to_string());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_block_ts.store(now, Ordering::Relaxed);
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

        self.record_best_hashrate(total.ten_minutes);
    }

    /// Track all-time best (persistent) and session-best (since boot).
    /// CAS loops (like share_accepted's best-share tracking) so two racing
    /// updaters cannot let a lower value overwrite a higher one that landed
    /// between the load and the store.
    fn record_best_hashrate(&self, total_10m: f64) {
        if !total_10m.is_finite() {
            return;
        }

        let mut prev = self.best_hashrate_hps.load(Ordering::Relaxed);
        while total_10m > f64::from_bits(prev) {
            match self.best_hashrate_hps.compare_exchange_weak(
                prev,
                total_10m.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.persist_best_hashrate_hps(total_10m);
                    break;
                }
                Err(x) => prev = x,
            }
        }

        let mut prev_session = self.session_best_hashrate_hps.load(Ordering::Relaxed);
        while total_10m > f64::from_bits(prev_session) {
            match self.session_best_hashrate_hps.compare_exchange_weak(
                prev_session,
                total_10m.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => prev_session = x,
            }
        }
    }

    fn now_secs() -> u64 {
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
        }
        if !stale.is_empty() {
            info!("Evicted {} idle offline workers from stats", stale.len());
        }

        // Best shares survive eviction (the dashboard still lists all-time
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
        if let Some(store) = &self.store {
            // Snap to the sampling grid. The recorder's wall-clock timestamps
            // drift by however long a tick took, and two things downstream need
            // them regular: the chart buckets at exactly this width on the 1h
            // view, and the retention step keeps rows where `ts % 60 == 0` —
            // which unsnapped timestamps would hit only by luck, thinning the
            // long-range history away to nothing.
            let history_ts = state_ts / SNAPSHOT_INTERVAL_SECS * SNAPSHOT_INTERVAL_SECS;
            let by_worker = self.hashrates_by_worker();
            let mut total = HashrateWindows::default();
            for rates in by_worker.values() {
                total.add(*rates);
            }
            store.record_hashrate_snapshot(history_ts, state_ts, total, &by_worker);
        }
    }

    pub fn get_hashrate_history(
        &self,
        since_ts: u64,
        bucket_secs: u64,
    ) -> Vec<HashrateHistoryPoint> {
        self.store
            .as_ref()
            .map(|s| s.get_hashrate_history(since_ts, bucket_secs))
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

        // Workers with no live `WorkerState`: those known only by an all-time
        // best share, and those restored from the hashrate checkpoint that have
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

        StatsSnapshot {
            shares_accepted: self.shares_accepted.load(Ordering::Relaxed),
            shares_rejected: self.shares_rejected.load(Ordering::Relaxed),
            blocks_found: self.blocks_found.load(Ordering::Relaxed),
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
            worker_hashrates,
            worker_states,
            network_hashrate_hps: f64::from_bits(self.network_hashrate_hps.load(Ordering::Relaxed)),
            network_difficulty: f64::from_bits(self.network_difficulty.load(Ordering::Relaxed)),
            est_difficulty_change_pct: f64::from_bits(
                self.est_difficulty_change_pct.load(Ordering::Relaxed),
            ),
            uptime_secs: self.start_time.elapsed().as_secs(),
            session_best_hashrate_hps: f64::from_bits(
                self.session_best_hashrate_hps.load(Ordering::Relaxed),
            ),
            last_block_worker: self
                .last_block_worker
                .lock()
                .clone()
                .unwrap_or_else(|| "—".to_string()),
            last_block_payout: self
                .last_block_payout
                .lock()
                .clone()
                .unwrap_or_else(|| "—".to_string()),
            last_block_hash: self
                .last_block_hash
                .lock()
                .clone()
                .unwrap_or_else(|| "—".to_string()),
            last_block_ts: self.last_block_ts.load(Ordering::Relaxed),
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
    pub blocks_found: u64,
    pub connected_miners: u64,
    pub current_height: u64,
    pub current_coinbase_value: u64,
    pub current_block_transaction_count: u64,
    /// Version of the current block template. The dashboard fills this from
    /// the TemplateEngine; other snapshot consumers receive zero.
    pub template_version: u32,
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
    pub network_hashrate_hps: f64,
    pub network_difficulty: f64,
    pub est_difficulty_change_pct: f64,
    pub worker_hashrates: Vec<WorkerHashrate>,
    pub worker_states: Vec<WorkerState>,
    pub uptime_secs: u64,
    pub session_best_hashrate_hps: f64,
    pub last_block_worker: String,
    pub last_block_payout: String,
    pub last_block_hash: String,
    pub last_block_ts: u64,
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
            stats.share_accepted(1_000_000);
            assert_eq!(stats.snapshot().best_share_difficulty, 1_000_000);
            stats.share_accepted(1_500_000);
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
    /// the switch belong to the new name; before this was fixed they kept
    /// landing on the old one for the life of the connection.
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

    #[test]
    fn legacy_hashrate_history_migrates_without_inventing_windows() {
        let db_path = make_temp_db();
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute(
                "CREATE TABLE hashrate_history (
                   ts INTEGER PRIMARY KEY,
                   hashrate_hps REAL NOT NULL
                 )",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO hashrate_history (ts, hashrate_hps) VALUES (120, 42.0)",
                [],
            )
            .unwrap();
            // Already recorded by the current estimator, so the algo migration
            // has no reason to discard these rows — this test is only about the
            // column migration not inventing values for the missing windows.
            conn.execute(
                "CREATE TABLE pool_stats (
                   id INTEGER PRIMARY KEY CHECK(id = 1),
                   best_share_difficulty INTEGER NOT NULL,
                   best_hashrate_hps REAL NOT NULL,
                   hashrate_algo_version INTEGER NOT NULL DEFAULT 0
                 )",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO pool_stats
                   (id, best_share_difficulty, best_hashrate_hps, hashrate_algo_version)
                 VALUES (1, 0, 0.0, ?1)",
                params![HASHRATE_ALGO_VERSION],
            )
            .unwrap();
        }

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        let history = stats.get_hashrate_history(0, 60);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].ten_minutes, Some(42.0));
        assert_eq!(history[0].one_minute, None);
        assert_eq!(history[0].five_minutes, None);
        assert_eq!(history[0].one_hour, None);
        assert_eq!(history[0].six_hours, None);
        assert_eq!(history[0].twenty_four_hours, None);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    /// History written by the old sliding-window estimator is not comparable
    /// with the decaying averages, and its spikes would pin the chart's y-axis
    /// and the all-time watermark forever. Opening such a DB must clear both.
    #[test]
    fn upgrading_the_estimator_discards_incomparable_history() {
        let db_path = make_temp_db();
        {
            let store = StatsStore::open(&db_path).unwrap();
            store.set_best_hashrate_hps(60.0 * TH);
            let worker_rates =
                HashMap::from([("axe".to_string(), HashrateWindows::uniform(949.0e15))]);
            store.record_hashrate_snapshot(
                120,
                120,
                HashrateWindows::uniform(949.0e15),
                &worker_rates,
            );
            // Pretend it was written before the estimator changed.
            store.flush();
            store
                .read
                .lock()
                .execute(
                    "UPDATE pool_stats SET hashrate_algo_version = 0 WHERE id = 1",
                    [],
                )
                .unwrap();
            assert_eq!(store.get_hashrate_history(0, 60).len(), 1);
        }

        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        assert!(stats.get_hashrate_history(0, 60).is_empty());
        assert!(stats.session_hashrates.is_empty());
        assert_eq!(stats.snapshot().best_hashrate_hps, 0.0);

        // Second open is a no-op: the version now matches.
        drop(stats);
        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        stats.record_best_hashrate(3.0 * TH);
        drop(stats);
        let stats = PoolStats::new_with_store(Some(db_path.clone()));
        assert_eq!(stats.snapshot().best_hashrate_hps, 3.0 * TH);

        drop(stats);
        std::fs::remove_file(db_path).ok();
    }

    #[test]
    fn hashrate_history_averages_samples_into_time_buckets() {
        let db_path = make_temp_db();
        let store = StatsStore::open(&db_path).unwrap();
        store.record_hashrate_snapshot(120, 120, HashrateWindows::uniform(10.0), &HashMap::new());
        store.record_hashrate_snapshot(150, 150, HashrateWindows::uniform(20.0), &HashMap::new());
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

    /// Sampling every 10s would put ~1.5M rows in the table over the six-month
    /// retention, so anything past the fine-grained horizon is thinned to one
    /// row a minute. This only works because recorded timestamps are snapped to
    /// the sampling grid — with drifting wall-clock values the `ts % 60` filter
    /// would match almost nothing and delete the entire long-range history.
    #[test]
    fn samples_past_the_fine_horizon_are_thinned_to_one_a_minute() {
        let db_path = make_temp_db();
        let store = StatsStore::open(&db_path).unwrap();

        // Two minutes of samples on the grid, at a minute boundary.
        let base = 9_999_960;
        assert_eq!(base % 60, 0);
        for i in 0..12 {
            let ts = base + i * SNAPSHOT_INTERVAL_SECS;
            store.record_hashrate_snapshot(ts, ts, HashrateWindows::uniform(10.0), &HashMap::new());
        }
        store.flush();
        assert_eq!(
            store.get_hashrate_history(0, SNAPSHOT_INTERVAL_SECS).len(),
            12
        );

        // A sample far enough ahead pushes them past the horizon.
        let now = base + FINE_HISTORY_RETENTION_SECS + 600;
        store.record_hashrate_snapshot(now, now, HashrateWindows::uniform(20.0), &HashMap::new());
        store.flush();

        let kept: Vec<u64> = store
            .get_hashrate_history(0, SNAPSHOT_INTERVAL_SECS)
            .iter()
            .map(|p| p.ts)
            .collect();
        assert_eq!(kept, vec![base, base + 60, now]);

        drop(store);
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

    /// The headline bug: on a young pool every window read the same number,
    /// because the old estimator divided by the age of the oldest share in the
    /// window rather than by the window itself. The long windows must lag.
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

    /// The bug this replaces: a worker whose session was still *connected* was
    /// exempted from decay entirely, so a rig whose hasher died kept showing
    /// its last reading forever.
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

    /// Two rigs sharing one worker name must add up. The old code keyed the
    /// hashrate map by worker name and used `insert`, so the second session
    /// silently replaced the first and the pool total read half of reality.
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

    /// A single share arriving microseconds before a tick cannot produce the
    /// absurd reading the old estimator did (949 PH/s from a 2.7 TH/s Bitaxe).
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
