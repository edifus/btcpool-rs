# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is pre-1.0, breaking changes bump the **minor** version and
everything else bumps the **patch** version.

## [Unreleased]

### Added
- **A shares-per-minute chart on the dashboard**, below the hashrate panel and
  driven by the same range selector, over the same 1m/5m/10m/1h/6h/24h decaying
  averages. Share throughput is a separate signal from work done — it moves with
  vardiff retargets and miner churn while hashrate holds flat — and nothing
  exposed it: the pool counted accepted shares but only ever as a lifetime total.
  Per minute rather than per second because a pool of any realistic size spends
  its life in the tenths otherwise. Persisted like the hashrate series, so a
  restart resumes the averages decayed across the downtime instead of resetting
  the chart to zero. The current rate also appears on the Accepted card, as
  `pool_shares_per_minute{window}` in Prometheus, as `shares_per_minute_*` on
  `GET /stats`, and at `GET /share-chart`.
- **Accepted and rejected share totals now survive restarts** when
  `[metrics] stats_db_path` is configured. The SQLite snapshot records
  monotonic lifetime counters, restores them at boot, and performs a bounded
  final persist on SIGINT or SIGTERM so a clean shutdown does not lose the
  tail between snapshots. The dashboard now gives accepted and rejected shares
  their own cards, with lifetime totals leading and this process's counts below;
  `GET /stats` gains `lifetime_shares_accepted`,
  `lifetime_shares_rejected`, and `stats_since_ts`. Rate-limited messages now
  enter the pool total, and the authenticated worker's total where available,
  so these figures agree with `pool_shares_rejected_total`.
- **Reject reasons are now counted pool-wide for both the current process and
  the pool's recorded lifetime.** The lifetime breakdown is persisted in the
  stats database, while `GET /stats` exposes it as `lifetime_reject_reasons`
  alongside the session-only `reject_reasons`. The dashboard puts each scoped
  breakdown on the corresponding rejected-total tooltip. Reason tracking starts
  with this schema, so its sum can trail the lifetime rejected total restored
  from an older database.
- **The hashrate chart's selected range and legend survive a page reload**,
  joining the theme, chart-collapse and quote-currency preferences. Picking
  `30d` and refreshing snapped back to `1h`, and the legend reverted to the
  default series set: the client defended legend clicks only within a session,
  against its own 10-second poll, and had nothing to restore them from on a
  fresh load.

### Changed
- The dashboard overview drops the redundant Pool uptime card — uptime remains
  in the header — and uses consistent compact labels across the remaining KPI
  cards. Reject details no longer widen the Rejected card as an inline list;
  the lifetime and session totals expose their labelled breakdowns on hover.
- **Logs now always go to stdout**, so the systemd journal and the Docker log
  driver always have them. Configuring `[logging] log_dir` previously *replaced*
  stdout with the log file; it now adds a rotating file alongside stdout rather
  than diverting it.
- **`[logging] json` now formats the log file only**, and has no effect without
  `log_dir`. Structured output exists for log shippers, which read the file.
- `RUST_LOG` is honoured, taking precedence over `[logging] level` when set.
  README and CONTRIBUTING have documented `RUST_LOG=debug cargo run` for a while,
  but the filter was built from the config string alone and ignored the
  environment.
- `BTCPOOL_*` config overrides announce themselves on stderr instead of through
  `tracing`. Overrides are applied while loading the config, before the
  subscriber exists, so those lines were being written to nothing.

### Fixed
- **An empty `[logging] log_dir` no longer writes log files into the working
  directory.** `log_dir` is optional, but `""` deserialized to `Some("")`
  rather than `None` and took the file-logging path anyway. Empty and
  whitespace-only values now mean "stdout only", as every doc already claimed.
- A log directory that cannot be created or opened now exits with a legible
  message on stderr instead of a panic backtrace from inside the appender.

## [0.3.0] - 2026-08-07

A consensus audit of the block-construction path — everything the pool has to
get right once it discards the node's coinbase and builds its own. This class of
bug is invisible until it costs a block: share validation reconstructs the same
header and coinbase the block path will, so a construction error validates
perfectly and only the `submitblock` fails, on the one day it matters.

### Added
- **`[pool] strict_gbt_rules` (default `true`) — the pool stops issuing work when
  `getblocktemplate` announces a `!`-prefixed rule this build does not
  implement.** The `rules` field was parsed and never read. Its `!` prefix is
  BIP22/23's statement that a client which *mutates* the template must
  understand the rule, and this pool mutates it about as far as a client can: it
  throws away Core's coinbase and rebuilds it, so every rule constraining
  coinbase shape is the pool's to satisfy. Core does not error on a `!` rule the
  client failed to declare — it lists the rule and leaves the decision to the
  client, and the pool was declining to make it. At the next soft-fork
  activation this binary predates, it would have kept building coinbases against
  rules it had never heard of, and found out by having a block rejected. The
  supported set is `csv`, `segwit`, `taproot`, which is what Core 31.1 returns on
  mainnet and regtest today. Set `false` to keep mining and rely on the metric
  and banner instead — the right call once you have read the new rule and
  satisfied yourself the coinbase still complies.
- `pool_unsupported_gbt_rules` gauge, a `"status":"unsupported_rules"` 503 from
  `GET /health` naming the blocking rule, and a dashboard banner. `/health`
  reports it ahead of staleness so the cause is named immediately instead of
  surfacing as a generic `"stale"` 180 seconds later; with `strict_gbt_rules =
  false` the probe stays 200, since failing it would only restart a pool the
  operator deliberately left mining.
- `GET /stats` gains `unsupported_rules` and `rules_block_work`.
- Consensus tripwires on every template, all once-per-refresh and all
  previously unguarded: the transaction set must leave Core's 4000 WU coinbase
  reserve free of the 4 MWU limit; the coinbase must fit that reserve;
  `coinbasevalue` must equal the subsidy plus the template's own fees (loud
  rather than fatal — the node's figure is authoritative, so a disagreement
  implicates our sum, and refusing to mine over it would turn a reporting bug
  into an outage), with `pool_coinbase_value_mismatch_total` to alert on; and a
  template announcing segwit must carry a `default_witness_commitment`, since
  its absence silently changes the coinbase's shape into
  `bad-witness-merkle-match`.
- `tests/fixtures/gbt-{mainnet,regtest}.json` — the non-transaction fields of
  real `getblocktemplate` responses, as provenance for the supported-rule set. A
  guessed allow-list behind a fail-closed default is a pool that will not start.

### Changed
- **The extranonce-offset cross-check now runs in release builds.** It was a
  `debug_assert`, so it was compiled out of the binary that actually mines. The
  offset is derived arithmetically from the serialization layout; if it were
  ever wrong — a `bitcoin` crate change, since the 100-byte scriptSig cap pins
  the varint width — the extranonce would overwrite the wrong field, and the
  pool and the miner would agree on the *same* corrupt coinbase. The share
  validates, the block is invalid, and nothing between here and a rejected
  `submitblock` can tell. It costs one byte search per template per payout
  identity, which is not the share path.
- GBT's `vbrequired` is parsed. A required version bit inside BIP320's rolling
  window (bits 13–28) cannot be honoured after the fact — the version is in the
  header the miner already hashed, so forcing it back on at validation time
  would invalidate their proof of work — so it is reported through the same gate
  as an unsupported rule rather than silently cleared. Core hardcodes the field
  to 0, so this only carries information from another template source.
- Failing to attach the BIP141 witness reserved value is now logged instead of
  silently falling back to a witness-free coinbase. The block is still worth
  submitting, because Core's `submitblock` repairs it — but the archived hex is
  not relayable, and that is worth knowing before someone replays it by hand.

### Fixed
- **A malformed transaction id from the node panicked the template refresh
  task.** `build_job_template` decoded each txid with
  `hex::decode(..).unwrap_or_default()` and then `copy_from_slice` into a
  `[u8; 32]`, which panics whenever the lengths differ — so any txid that was
  not exactly 64 valid hex characters, including the empty vector the decode
  failure produced, killed the task. Only that task: the pool kept accepting
  miners and serving the last template forever, which is the same freeze the
  v0.2.0 ZMQ supervision work closed, reached through a different door.
  `/health` would have caught it after 180 seconds, which is the safety net
  working, not a reason to keep the panic. Silently truncating instead would
  have been worse — a wrong merkle leaf produces a root that is wrong but
  self-consistent — so it is now an error carried by the existing `Result`.
- **The accepted ntime window could reach past what consensus allows.** Shares
  were bounded to `curtime ..= curtime + 7200`, but `time-too-new` is measured
  against the *validating node's* clock, not against the template. The two agree
  while `curtime ≈ now`, and Core sets `curtime = max(MTP+1, now)` — so on a host
  running more than about an hour slow, or on regtest under `setmocktime`,
  `curtime` is `MTP+1` and the template's own window reaches past what the node
  will accept. A share at the top of it became a block rejected outright. The
  ceiling is now the tighter of the template-relative drift policy and an
  absolute bound against the pool's own clock; the floor is unchanged and still
  subsumes `time-too-old`.

## [0.2.0] - 2026-08-06

Block accounting that survives reorgs and restarts, a `/health` probe, and the
blocking work moved off the async runtime.

### Added
- `GET /health` on the dashboard/metrics port: 200 with
  `{"status":"ok","template_age_secs":N}` while the template is refreshing, 503
  with `"status":"stale"` once it has not refreshed for 180 seconds — or before
  the first refresh has ever landed, so a node that was already unreachable at
  startup is visible immediately. Deliberately a readiness/alerting probe:
  pointing a container `HEALTHCHECK` at it that restarts the process fixes
  nothing when the freeze is upstream in bitcoind, and drops every connected
  miner for no gain.
- `pool_template_last_refresh_timestamp_seconds` — the raw unix timestamp of the
  last successful template refresh, not an age, so freshness is
  `time() - pool_template_last_refresh_timestamp_seconds` in PromQL and
  operators can pick a threshold independent of `/health`'s fixed one.
- `pool_tip_changes_discovered_by_timer_total` — counts tips the 30-second ntime
  timer noticed before the block-notification path did. One is a coincidence; a
  run of them is direct evidence that ZMQ has stopped delivering, which no
  timeout can detect on a socket that connects but never publishes.
- `pool_zmq_reconnects_total` now has a caller. It was declared and dead since
  it was written, because nothing ever reconnected.
- Boot now fails when `[pool] coinbase_tag` plus the extranonce widths would
  push the coinbase scriptSig past the 100-byte consensus limit. All of it is
  operator-configured, so an over-long tag would otherwise only surface as a
  `bad-cb-length` rejection on the one block the pool ever finds.
- **Deferred confirmation pass for found blocks.** Every block the node stores
  is enrolled in a `found_blocks` ledger in the stats database and re-checked
  with `getblockheader` on a 60-second sweep until it is `confirmation_depth`
  deep on the active chain or that deep on a branch that lost. New config key
  `[pool] confirmation_depth` (default 6, range 1–100); new metrics
  `pool_blocks_orphaned_total`, `pool_block_confirmations_total{result}` and the
  `pool_blocks_pending_confirmation` gauge; new `/stats` fields
  `blocks_orphaned`, `blocks_pending_confirmation` and `last_block_status`.
  The sweep makes no RPC call when nothing is pending, which is nearly always.
- Block counts now survive a restart. `blocks_found`, `blocks_inconclusive` and
  `blocks_orphaned` are recomputed from the `found_blocks` ledger at boot, which
  is what the dashboard's "found blocks survive restarts" label has claimed
  since v0.1.3 without it being true. Requires `[metrics] stats_db_path`; with
  no stats database the ledger lives in memory only and still drives the
  confirmation pass for the life of the process.

### Changed
- **`[zmq] poll_fallback = true` now polls alongside ZMQ instead of only after
  it fails.** It was a one-way switch armed by a ZMQ *error*, which meant it
  never armed for the failure it was most needed for: `zmq_connect` is
  asynchronous, so a wrong port or a node without `zmqpubhashblock` connects
  successfully and then simply never publishes. There is no error to react to.
  The cost is one `getbestblockhash` per `poll_interval_ms` (default 1 s)
  forever, which is cheap and already runs off the async runtime.
  `pool_rpc_fallback_used_total` correspondingly shifts meaning from "we had to
  fall back" to "the backstop is enabled", and still fires exactly once, at
  startup.
- Blocks are submitted with the BIP141 witness reserved value (32 zero bytes)
  already in the coinbase input's witness when the template carries a witness
  commitment. Core's `submitblock` RPC inserts a missing one itself, so blocks
  were being accepted, but the archived `found-blocks/*.hex` was not a block any
  other path would take — P2P relay and Core's IPC mining interface both reject
  it with `bad-witness-nonce-size`. `coinbase1`/`coinbase2` and the merkle root
  keep using the stripped serialization, so the txid is unchanged.
- Share statistics and metrics for SV1 and SV2 now go through one shared
  accounting path, and rejection reasons are a closed enum rather than
  duplicated string literals at each site. Block-submission reporting now goes
  through that same path.
- **Breaking (metrics): `pool_block_submissions_success_total` is removed,**
  replaced by `pool_block_submissions_total{outcome="accepted"|"duplicate"|
  "inconclusive"}`. The old counter was incremented on the line directly after
  every `pool_blocks_found_total` increment and nowhere else, so the two always
  carried identical values under different names; the labelled counter gives the
  node's actual verdict instead. `pool_blocks_found_total` keeps its name and
  stays unlabelled — it is the headline number and should not need a label
  matcher to read — and is now equivalent to
  `sum(pool_block_submissions_total{outcome=~"accepted|duplicate"})`.
  `pool_block_submissions_failed_total{reason}` is unchanged.
- `GET /stats` gains `blocks_inconclusive`, the count of valid blocks that lost
  their height race. Like `blocks_found` it is in-memory only and resets on
  restart.
- SQLite persistence moved off the async runtime: writes are queued to a
  dedicated writer thread, dashboard history queries run on the blocking pool,
  and the database is opened with `journal_mode=WAL`, `synchronous=NORMAL` and a
  5-second busy timeout. Share submissions and dashboard chart queries no longer
  contend on one connection mutex.
- Bitcoin RPC moved off the async runtime as well, finishing that work.
  `getblocktemplate` ran synchronously inside the template loop, parking a tokio
  worker for the whole round trip on every new block and every 30-second ntime
  refresh — and it is the heaviest RPC the node serves, since it re-runs block
  assembly over the mempool. On a 2–4 core home node that stalled a quarter to a
  half of the runtime's capacity while connected miners' share submissions
  queued behind it. Three further synchronous calls were doing the same thing:
  the ZMQ poll fallback's `getbestblockhash` at 1 Hz, and `getnetworkhashps`
  plus the five-round-trip difficulty estimate in the 30-second stats loop.
  `RpcClient` now exposes only `async` methods, which run the call on the
  blocking pool; the synchronous bodies are private, so the mistake cannot
  recur.
- Share validation no longer allocates per submitted share. The coinbase is
  spliced into a scratch buffer held per blocking-pool thread instead of cloning
  `coinbase_template` each time; the hex/format strings above the "Validating
  submitted share" `debug!` moved inside the macro, so `tracing` skips them at
  the default `info` level; and the never-read `worker` field, three throwaway
  strings in `mining.submit` parsing, and a redundant `prev_hash` copy are gone.
  No behaviour or log output changes — this is the last of the hot-path cleanup
  tracked in TODO. As a side effect the prev-hash decode now returns
  `InvalidHeader` for a wrong-length value rather than reaching a
  `copy_from_slice` that would panic inside the validation task; the value is
  pool-generated, so that path was unreachable.

### Fixed
- **A block that won its height and was then reorged out kept counting as a
  win.** `pool_blocks_found_total`, the `/stats` block count and the dashboard's
  "Last block found" card were all decided by the `submitblock` response, which
  is only true at the instant it is read. A deferred pass now re-checks each
  found block and reconciles: the dashboard count drops back and the card is
  marked `reorged out`, while Prometheus — where a counter cannot go down —
  gains `pool_blocks_orphaned_total`, so the blocks the pool kept are
  `pool_blocks_found_total - pool_blocks_orphaned_total`. The correction runs in
  both directions: a block reported `inconclusive` that a later reorg puts on
  the active chain is counted then, which the submit-time verdict could never
  do. Orphaning requires the losing branch to be buried by the full
  `confirmation_depth`, so the routine one-block reorg that resolves in our
  favour does not flap the count.
- **The block hash the pool reported was byte-reversed.** The raw double-SHA256
  is internal (little-endian) order, and it was hex-encoded as-is into the
  dashboard's last-block card, every log line, and the
  `found-blocks/block_<height>_<hash>.hex` filename — putting proof-of-work's
  leading zeros at the wrong end. The string resolved in no block explorer and
  was accepted by no RPC. All of them now carry the conventional big-endian
  display form; already-archived filenames keep their old spelling.
- **Blocks that lost a same-height race were reported as wins.** `submitblock`
  answers `"inconclusive"` when it accepts and stores a consensus-valid block
  that did not become the chain tip — the block sits on a side branch and earns
  nothing. That was mapped to a bare `Ok(())`, indistinguishable from a real
  find, so a superseded block incremented `pool_blocks_found_total`, incremented
  the `/stats` block count, overwrote the dashboard's "Last block found" card
  and logged `🏆 Block submitted!`. `submit_block` now returns a three-way
  `BlockSubmitOutcome`, and only `Accepted`/`Duplicate` count as a find.
  `"duplicate-inconclusive"` was grouped with plain `"duplicate"` and had the
  same problem; it is now `Inconclusive` too. The miner is unaffected: its share
  is still credited and acked, because a valid block-difficulty share losing a
  race is not its fault. Fixed at all three reporting sites — SV1, SV2, and the
  background resubmit task, which is the one most likely to see it, since it
  runs minutes after the block was found.
- **The template engine could stop refreshing without anyone noticing.** The ZMQ
  listener had no reconnect — a single receive error ended it permanently. With
  `poll_fallback = false` that dropped the only `watch::Sender`, and the
  template loop `break`ed out on the closed channel, taking the 30-second ntime
  refresh with it. The pool then served one frozen template until restart, with
  a single `warn!` as the only evidence. The listener is now supervised and
  reconnects forever with capped exponential backoff (1 s → 60 s), so the sender
  is never dropped; the refresh loop additionally survives a closed channel by
  latching that `select!` branch off and running on the ntime timer alone.
- **A tip discovered by the ntime timer was broadcast as `clean=false`,** telling
  miners they could keep grinding a job built on a dead tip. `refresh` trusted
  its caller's flag; it now derives the flag by comparing `prev_hash` against the
  outgoing template, so any tip change forces a clean job no matter which path
  noticed it. `prev_hash` rather than height, because `hashPrevBlock` is the only
  header field that binds work to a chain — height lives only in the BIP34
  coinbase push and misses a same-height reorg. Consequences differed sharply by
  protocol: SV1 miners switched jobs but the stale job stayed live, so a block
  found on it was assembled with the old `prev_hash` and silently earned nothing;
  SV2 was worse, since `clean` gates `SetNewPrevHash` and `min_ntime`, so devices
  were handed an immediate job on a prev-hash they had not been moved to and
  **every** subsequent share failed the target check until the session was
  dropped for invalid shares.
- **`bitcoin_rpc.timeout_secs` was parsed, documented, and then never applied.**
  The client was built with the JSON-RPC transport's hardcoded 15-second
  default, so setting the option did nothing — including on the client rebuilt
  after a cookie rotation. It is now honoured on both construction paths. This
  matters more than it used to: RPC calls run on the blocking pool, where a task
  cannot be cancelled, so the transport timeout is the only bound on how long an
  unresponsive node can hold a thread. Operators whose node needs more than the
  default 10 seconds to answer `getblocktemplate` should raise the value — it is
  now enforced where 15 seconds silently applied before.
- Hashrate averages no longer restart from zero when the service restarts.
  Per-worker decay state is checkpointed with the chart history, restored from
  SQLite at startup, and decayed across the time the service was offline.
- **Reported hashrate was inflated for miners that ignore `set_difficulty`.**
  Shares are validated against the vardiff floor (so cgminer/Avalon firmware
  pinned to its configure-time `minimum-difficulty` isn't rejected) but were
  credited to the estimator at the session's current vardiff. A session that
  keeps submitting below the difficulty it was sent is now detected — three
  such shares outside a 30-second post-retarget grace window — and credited at
  the floor instead, which is the threshold actually governing how often it
  submits. Miners that honour `set_difficulty` are unaffected. The Prometheus
  share-difficulty histogram now records the same credited value the dashboard
  hashrate is built from; the two previously disagreed.
- Shares submitted after a connection re-authorized under a different worker
  name were credited to the previous name for the life of that connection.
- Workers restored from the hashrate checkpoint that have not reconnected yet
  showed a zero row in the dashboard worker table while still contributing to
  the pool total.
- Block height, network difficulty and the coinbase value on the dashboard are
  now published by the template engine on every refresh instead of by each
  miner session, so they stay current when no miner is connected.

## [0.1.3] - 2026-08-05

Upstream v0.6.3 compatibility fixes and live BIP110/RDTS visibility.

### Added
- The dashboard now shows whether the current block template signals
  BIP110/RDTS through version bit 4. This is read directly from the node's live
  template; the pool reports the node's policy and does not choose it.

### Removed
- The unused `[zmq] rawtx_endpoint` setting and the corresponding
  `zmqpubrawtx` setup instructions. The pool consumes `hashblock`
  notifications only.

### Fixed
- The block-acceptance test now abandons a nonce sweep after two minutes and
  resynchronizes with the latest job. Slow CI runners can no longer spend long
  enough on one sweep for the pool to evict that job before submission.
- Hashrate chart lines, legend markers, hover points, and tooltip swatches now
  use the same name-based color mapping. Building ECharts' global palette in
  legend order had rotated all six marker colors away from their plotted lines.

## [0.1.2] - 2026-08-05

Chart readability on small screens, and per-window hashrate metrics.

### Changed
- The hashrate chart lays out responsively: grid padding no longer
  double-counts the space `containLabel` already reserves, x-axis label density
  is derived from the rendered width, and the plot height scales with the
  viewport between 280px and 420px. Previously a fixed 60px left margin and
  ~12 fixed time labels made the chart largely unreadable on a phone.
- Prometheus hashrate metrics are restructured. `pool_hashrate_estimated_hps{worker}`
  is replaced by `pool_hashrate_hps{window}` for the pool total and
  `pool_worker_hashrate_hps{worker,window}` per worker, each carrying every
  averaging window rather than 10m alone. Alert on `pool_hashrate_hps`: it has
  no worker label and so does not fan out with the fleet.

### Fixed
- Departed workers no longer flatline their Prometheus gauges at a final
  non-zero reading until the exporter's idle timeout; an explicit zero is
  published once their last session decays away.

## [0.1.1] - 2026-08-05

Hashrate accuracy. The estimator is replaced with ported `decay_time()` from
ckpool.

### Changed
- Hashrate is now measured with exponentially decaying averages ported from
  ckpool's `decay_time()`, replacing the trailing sliding-window estimator.
  Each figure is defined by a time constant rather than a sample span, so it
  converges on the true rate no matter how share arrivals are spaced. 1m, 5m,
  10m, 1h, 6h and 24h are shown on the chart and the workers table;
  `/stats` additionally carries 3h.
- Hashrate is sampled to SQLite every 10 seconds instead of every minute, and
  the dashboard polls the chart at the same cadence instead of every 60s. The
  1h range buckets at 10s to match. Samples older than 48 hours are thinned to
  one a minute; the six-month retention is unchanged.
- Each session no longer keeps a 24-hour ring buffer of share timestamps, and
  hashrate is no longer recomputed on every inbound message. Shares are
  accumulated as they arrive and folded in by a single 2-second task, removing
  a per-message scan that grew with the share rate.

### Removed
- The `1m` and `6m` chart range aliases for `30d` and `180d`. `1m` also names a
  hashrate series, and the collision was a trap; the UI only ever sent the
  explicit forms.

### Fixed
- Hashrate could read orders of magnitude too high. The estimator divided the
  summed share difficulty by the time since the oldest share still inside the
  window rather than by the window length, and it recomputed on every inbound
  message — so a share that landed alone in a window was divided by its own
  age, microseconds. Related biases in the same formula are gone with it: every
  window collapsing to the same value on a young pool, and an N-shares-over-
  N-1-gaps overestimate worth ~2x at two samples.
- Two rigs authorising under one worker name no longer overwrite each other.
  Hashrate state is keyed by session and summed per worker, so the pool total
  counts both; previously only the last one to submit was counted.
- A worker whose hasher dies while its connection stays up now decays to zero.
  Estimates only refreshed on inbound traffic and connected workers were exempt
  from decay entirely, so such a worker reported its last reading indefinitely.
- The chart no longer records a zero sample at startup, which put a dip at the
  left edge after every restart.
- Toggling a series in the chart legend sticks. The periodic refresh re-applied
  the server's default visibility map, undoing the click within a minute.
- The chart's trailing live point is snapped to the bucket grid.

## [0.1.0] - 2026-08-05

First release under the new name.

### Changed
- The project, binary, deployment artifacts, and NixOS module have been renamed
  from `solo-pool-rs` to `btcpool-rs`. The project version has been reset to
  `0.1.0`.
- Block payouts are now derived from each SV1 worker name or SV2 user identity:
  `BITCOIN_ADDRESS` or `BITCOIN_ADDRESS.worker`. The address is validated for
  the connected node's network before a mining job is issued, and each session
  receives a coinbase paying only its authorized identity. The old global
  `[pool] coinbase_address` and dashboard payout editor have been removed.
- Hashrate reporting now exposes 1m, 5m, 10m, 1h, 6h, and 24h rolling averages
  for the pool and each worker. History is sampled every minute, long chart
  ranges are bounded with server-side time buckets, and the dashboard defaults
  to a 1h range with straight, unfilled lines instead of smoothed area curves.
  Existing SQLite history is migrated in place and retained as 10m data.

### Fixed
- SV2 setup rejections now answer with a spec-compliant `SetupConnection.Error`
  before disconnecting, covering all three spec error codes:
  `unsupported-protocol` for non-mining sub-protocols (such as a Job
  Declarator Client), `protocol-version-mismatch` for version ranges the pool
  cannot serve, and `unsupported-feature-flags` for devices that require
  standard jobs or work selection (the pool serves extended channels only and
  does no job declaration; such a device previously stalled at channel open).
  Previously the connection was closed without a reply, which a conformant
  client could not distinguish from a network fault. Malformed payloads still
  disconnect immediately.

---

# Pre-rename history (solo-pool-rs)

Everything below predates the rename to `btcpool-rs` and the version reset to
`0.1.0` described in the entry above. It used a separate numbering line that
reached 0.6.2 in the [cbyam/solo-pool-rs](https://github.com/cbyam/solo-pool-rs)
repository, so these version numbers overlap the current ones and are kept
unlinked to avoid any ambiguity with a release of the same number here.

## 0.6.2 — 2026-07-10

### Added
- Market card: 24-hour price change shown as a green/red percentage under the
  BTC price, and the price digits pulse green or red on each tick by
  direction (fetched from CoinGecko every 60 seconds, with the 24h-change
  field on the same request). A quote-currency selector (USD, EUR, GBP, CAD,
  AUD, CHF, JPY) persists in the browser like the theme choice.
- Mobile layout: below 880px the nav collapses into a hamburger drawer
  (brand left, menu button right; links, Connect/Settings, Raw metrics,
  Theme, and Uptime inside). The workers table now scrolls horizontally
  inside its panel instead of stretching the whole page, and on phones the
  Mode, Vardiff, 3h/24h hashrate, Best share, and Uptime columns are hidden
  so the essentials fit without scrolling.

### Fixed
- **Offline workers no longer freeze the pool hashrate.** Windowed hashrate
  estimates are only recomputed while a miner's session is delivering traffic,
  so a disconnected worker's last values stayed in the dashboard totals
  unchanged until eviction (up to 24 hours). Offline workers' estimates now
  decay linearly out of each window: the 60s figure reads zero after a minute
  offline, 10m after ten minutes, 3h after three hours, 24h after a day. The
  decayed values feed the dashboard totals, the per-worker rows, the hashrate
  history chart, and the best-hashrate watermark. Online workers and
  reconnects behave as before.
- **Dead miners are detected in about a minute instead of 15-20 minutes.**
  A powered-off miner sends no TCP FIN, and two intended defenses were not
  working: the accept path claimed to enable TCP keepalive but only set
  TCP_NODELAY, and the per-session idle timeout was reset by every job
  broadcast (the timeout future is recreated each `select!` iteration), so it
  never fired for a dead peer still being sent jobs. Detection was left to the
  kernel's retransmission budget on job writes (15-20 minutes with Linux
  defaults). Accepted sockets now get real TCP keepalive (probe after 60s
  idle, every 10s, 3 tries) plus `TCP_USER_TIMEOUT` (90s, Linux) bounding
  unacked job writes, and the idle timeout in both session loops (SV1 and SV2)
  anchors on the last inbound message, so `idle_timeout_secs` now works as
  documented. Note for very small hardware: the broken timeout also never
  disconnected legitimate miners whose share interval exceeds
  `idle_timeout_secs` (default 300). If your vardiff floor puts a miner's
  expected share pace above that, raise `idle_timeout_secs`.

## 0.6.1 — 2026-07-05

### Added
- **Per-reason reject breakdown on the dashboard.** Each worker's rejected
  shares are now counted by reason (stale, duplicate, low difficulty, unknown
  job, bad extranonce, invalid) in the in-memory stats and exposed via
  `/api/stats` (`worker_states[].reject_reasons`). The Rejects tile shows the
  pool-wide breakdown next to the stale rate, and hovering a worker's rejected
  count in the workers table shows that worker's breakdown. Counters reset on
  restart.
- Hide/Show toggle on the hashrate chart panel. The choice persists in the
  browser (localStorage, like the theme), and while hidden the dashboard skips
  the periodic chart fetch; expanding re-fetches immediately.

### Changed
- New-block indication on the Chain tip card: the height now pulses in the
  accent color (two beats), replacing the green background flash.

### Fixed
- Per-worker rejected counts previously missed `job_not_found` rejects (SV1
  and SV2) and `bad_extranonce` rejects (SV2); only the pool-level counter
  recorded them. All reject paths now increment the worker counter, matching
  the Prometheus `pool_shares_rejected_total` metric.

## 0.6.0 — 2026-07-02

### Added
- **SV2 pool identity (authority key pinning).** The Noise authority key now
  persists across restarts (`[sv2] authority_key_file`, created on first start
  with owner-only permissions, same pattern as bitcoind's `.cookie`), so the
  pool keeps a stable identity that miners can pin. The base58check public key
  (SRI `key-utils` format) is logged at startup and shown in the dashboard's
  Connect modal with a copy button, and returned by `GET /api/info` as
  `sv2_authority_pubkey`. Set it as the pool/authority public key on an SV2
  miner to cryptographically verify the pool; miners that do not pin connect
  exactly as before. `[sv2] persist_authority_key = false` opts out (fresh key
  per process, the previous behavior).
- `[sv2] cert_validity_secs` — validity window of the per-connection
  certificate (default one year). Short values are useful for testing how a
  verifying miner handles certificate expiry and clock skew.
- Tests: full Noise handshakes against a pinning SRI initiator (correct key
  accepted, wrong authority key rejected, expired certificate rejected, no-pin
  still connects) plus authority-key-file round-trip/permission checks.

### Changed
- **Upgrade note (breaking for read-only deployments):** with SV2 enabled the
  pool now creates `sv2-authority.key` (relative to its working directory) on
  first start and **fails at boot if it cannot**. Deployments with a read-only
  working directory, such as the shipped systemd unit with
  `ProtectSystem=strict`, must set `[sv2] authority_key_file` to a writable
  path (e.g. `/var/lib/btcpool-rs/sv2-authority.key`) or set
  `persist_authority_key = false`. Docker users who want the pool identity to
  survive container re-creates should point it into the data volume
  (`authority_key_file = "data/sv2-authority.key"`).

### Fixed
- Dashboard: the Connect modal **Copy buttons now actually copy** when the
  dashboard is served over plain HTTP (the usual LAN case).
  `navigator.clipboard` only exists in secure contexts, so the old code
  selected the text and showed "Copied" without copying. Insecure contexts now
  fall back to `document.execCommand('copy')`, and if even that fails the
  button says "Copy manually" and leaves the text selected.
- **Duplicate-share tracking** no longer misreports or permits bounded replay:
  a share is recorded for dedup only after it validates (invalid submissions
  previously occupied slots, so a later identical valid submit was wrongly
  rejected as `duplicate`), and the per-session set is cleared on every
  clean-job broadcast, scoping replay protection to live jobs instead of FIFO
  eviction (evict-then-resubmit could inflate share/hashrate stats).
- Pool **best-share / best-hashrate writes are monotonic end to end**: the
  SQLite `UPDATE`s now carry a `?1 > ...` guard (matching the per-worker
  variant) and the in-memory best-hashrate update is a CAS loop, so racing
  writers can no longer regress a recorded best value.
- A block accepted by the **background submit retrier** (inline attempts
  failed, e.g. while bitcoind restarts) now updates the dashboard block count
  and last-block panel, not just the Prometheus counters.

## 0.5.1 — 2026-06-15

### Added
- Dashboard: **Connect** modal (rail nav) — shows the exact
  `stratum+tcp://<host>:<port>` to point a miner at (host derived from the
  browser, port from the pool config), with a copy button, a note that SV1/SV2
  are auto-detected on the one port, the read-only payout address (link to
  Settings), per-firmware quick-start hints, and build/network/license info.
  Backed by a new read-only `GET /api/info` endpoint. Removes the
  "where do I point my miner?" onboarding gap, especially for Umbrel installs.
- `mining.suggest_difficulty` is now honored as a **starting** difficulty,
  clamped to the configured vardiff floor/ceiling (vardiff takes over from
  there). Lets miners that send it (e.g. AxeOS's "pool difficulty" field) cold-
  start near their settled difficulty instead of bursting low-diff shares from
  the floor. A suggestion can never push a miner below the floor.

### Changed
- Dashboard: the per-worker **degraded (amber) status LED** is now evaluated
  *relative to each worker's own established cadence* rather than a flat
  no-share timeout. Low-hashrate, never-submitted, and just-connected miners
  (whose natural share interval is long or not yet established) are no longer
  falsely flagged; a worker is amber only after going silent well past its own
  expected share interval.

### Documentation
- README: added a "Difficulty and small / large miners" note explaining the
  `min_difficulty` floor, that share difficulty has no payout effect in solo,
  and how to tune for small or large hardware.

## 0.5.0 — 2026-06-14

### Added
- Dashboard: **Settings page** — the payout address can now be changed at
  runtime from the dashboard. The network is **detected from the connected
  node** (`getblockchaininfo`, including testnet4) and shown read-only; a change
  is validated (the address must parse and belong to the node's chain),
  persisted in the stats SQLite (overriding config.toml at next boot), and
  applied immediately via a forced clean-job broadcast so connected miners
  switch payout without waiting for the next block. New config keys:
  `pool.network` (optional assertion — boot fails fast if set and the node
  reports a different chain) and `metrics.allow_runtime_settings` (default true;
  set false on an untrusted LAN, or front the dashboard with an authenticating
  proxy).
- Dashboard: redesigned **console layout** — fixed left rail with scroll-spy
  navigation and status footer, hero hashrate zone with block odds, hairline
  KPI strip, and a non-mainnet network badge. Two color themes — carbon
  (near-black, amber accent; default) and Swiss light (porcelain, cobalt
  accent) — toggleable from the rail, seeded from the OS preference, and
  persisted per browser. The hashrate chart re-skins from the active theme's
  CSS variables without a server round trip. Settings opens in a centered modal.
- Dashboard: **theme-aware brand logo** — a pickaxe-and-Bitcoin-block SVG mark
  with dark and light variants that follow the active theme (replacing the
  earlier glyph).
- Dashboard: **per-worker status LEDs** — an online (green) / offline (grey)
  indicator replaces the Online/Offline text, with a third **degraded (amber)**
  state when a worker is connected but hasn't landed an accepted share in over
  two minutes (the same signal as the "degraded" KPI). The status column is
  centered and the worker columns are reordered to Worker · Status · Mode.
- Dashboard: **rail connectivity LED** — green while `/stats` refreshes are
  landing, grey when they stall, so a frozen dashboard is visible at a glance.

### Changed
- Dashboard: the Market card drops the redundant raw-difficulty readout (the
  same value is already shown on the Network card).

### Security
- Mining is **paused unless the payout address validates against the node's
  chain**. A wrong-network address (e.g. a `tb1…` testnet address while the
  node is on mainnet) still encodes a perfectly valid coinbase script, so the
  pool would previously have mined to a script the operator may not control;
  now no job is built until a valid address is saved (the pool still boots and
  the dashboard stays reachable to fix it — which also gives container
  platforms a clean first-run flow with a placeholder address).
- Stratum: the miner-disconnect metric's `reason` label is now a bounded value
  instead of the raw parser error, which embedded attacker-controlled bytes
  (including line/column). Malformed input could otherwise mint unbounded
  Prometheus time series — a metrics-bloat / amplification vector.

### Fixed
- Stratum: blank / whitespace-only lines (a common firmware keepalive) are now
  ignored instead of being parsed as empty JSON and dropping the connection.

## 0.4.2 — 2026-06-12

### Fixed
- **Critical: every found block was rejected by the node with
  `prev-blk-not-found`.** The coinbase/`mining.notify` prev-hash conversion
  applied a per-4-byte-word swap that `build_header` then undid, leaving the
  block header's `hashPrevBlock` in Bitcoin Core's *display* byte order instead
  of internal order. The header still produced valid proof-of-work, and share
  validation reconstructs the same header, so the bug was invisible at the
  share level — but no node would accept the assembled block, so a real
  block-find would have been archived to disk yet **rejected by the network and
  lost**. `stratum_prev_hash` now reverses the full 32 bytes (display →
  internal) before the per-word swap, yielding the canonical Stratum format;
  `build_header` recovers the correct internal bytes. Verified end to end on
  regtest: the pool now finds a block, submits it, and the node accepts it onto
  the chain (coinbase paying the configured address). The previous round-trip
  unit test only checked that the output changed; it now asserts the recovered
  header bytes equal the block's true internal prev-hash.
- BIP34 coinbase height encoding now matches Bitcoin Core's
  `CScript() << nHeight` for all heights: `OP_0` for 0 and `OP_1..OP_16` for
  heights 1–16, instead of always using the data-push form. The pool only ever
  emits these small-height encodings when mining the first 16 blocks of a fresh
  regtest / signet chain, where the old encoding was rejected with
  `bad-cb-height`; post-BIP34 mainnet heights are all >16 and were unaffected.
  Found while rehearsing the block-submission path on regtest.

## 0.4.1 — 2026-06-11

### Added
- Config: every value can now be overridden by an environment variable named
  `BTCPOOL_<SECTION>__<KEY>` (double underscore between section and key),
  e.g. `BTCPOOL_BITCOIN_RPC__URL`. Overrides are typed after the key in the
  config file (load fails loudly on a type mismatch); for keys absent from the
  file, booleans/numbers are inferred and surrounding double quotes force a
  string. Designed for container platforms (Umbrel, Start9, Compose) that
  configure apps through the environment.
- Packaging: the GHCR image is now multi-arch — `linux/amd64` + `linux/arm64`
  (each built on a native runner and merged into one manifest list), so the
  pool runs on Raspberry Pi–class hosts and arm64 servers. Release tarballs
  now include an `aarch64-unknown-linux-gnu` build alongside x86_64.

### Fixed
- Repeated `mining.authorize` calls on one connection no longer inflate the
  dashboard's per-worker `active_sessions` count (ghost-online accounting);
  re-authorizing the same name is now a no-op, and switching names releases
  the previous one.

### Security
- **Pre-auth DoS hardening** (the two High items from the June 2026 review):
  - A connection now has 10 seconds (was: the full 300 s idle timeout) to make
    protocol progress before authorizing a worker — covering the protocol
    auto-detect peek, the SV2 Noise handshake, and every pre-auth message — so
    silent connections can no longer pin the bounded global connection slots.
  - Worker-identity cardinality is now capped: one connection may authorize at
    most `security.max_authorizations_per_session` distinct names (default 8,
    0 disables; new config key). The per-session token bucket
    (`max_shares_per_sec`) now applies to **all** inbound messages, not just
    share submits, so authorize/configure floods are rate-limited too.
  - Offline workers idle > 24 h are evicted from the in-memory stats maps, and
    Prometheus series untouched for 24 h are dropped from the exporter, so
    attacker-minted worker names no longer leak memory or label series forever.
  - The persisted `worker_best_shares` table is bounded to the top 512 rows by
    difficulty (pruned at boot and periodically); an inflated table from an
    earlier run is trimmed before being loaded into RAM.

## 0.4.0 — 2026-06-11

### Changed
- **Packaging (Docker): the runtime image now runs as a non-root user
  (uid:gid `10001`, `btcpool`) instead of root.** This is a breaking change
  for existing Docker / Compose deployers and needs two one-time migration
  steps:
  - Grant the container read access to bitcoind's cookie via the host node
    group — `docker run --group-add <gid>` or Compose `group_add` — and set
    `rpccookieperms=group` in `bitcoin.conf`.
  - `chown -R 10001:10001` any persisted `./data` volume so the SQLite stats DB
    and found-block archives stay writable.

  The default in-container cookie path also moves from `/root/.bitcoin/.cookie`
  to `/home/btcpool/.bitcoin/.cookie`. The binary is byte-identical and needs
  no root (the stratum and dashboard ports are both >1024); only the image
  contract changes. The systemd unit already ran unprivileged and is unaffected.

### Fixed
- Packaging (Docker): the runtime image was missing `libsqlite3.so.0`, which
  `rusqlite` links — the binary failed at dynamic-link time before `main()`, so
  the published images never actually started. (Bare-metal/systemd installs were
  unaffected, linking the host's library.) The runtime stage now installs
  `libsqlite3-0` alongside `libzmq5`.
- Config: reject `extranonce1_size + extranonce2_size == 0` at startup. A zero
  total extranonce width underflowed the `usize` subtraction in SV2
  `OpenExtendedMiningChannel` channel setup; it now fails fast at load with a
  clear message instead of risking a runtime panic.

### Security
- Network: enforce `max_message_bytes` on the newline-terminated path of the
  SV1 line reader. A line whose terminating newline fell within a single read
  buffer (~8 KiB) was returned without the size check, so the configured limit
  could be exceeded; the cap is now applied on that path too.
- Metrics: bound the `pool_block_submissions_failed_total` `reason` label to a
  small fixed set of values instead of the `Debug`-formatted error. The error
  text carries node-influenced strings (RPC messages, rejection reasons) that
  would otherwise mint unbounded Prometheus label series.

## 0.3.2 — 2026-06-10

### Added
- Config: `pool.found_block_dir` (default `found-blocks`) — directory where the
  raw hex of every found block is archived before submission.

### Fixed
- Mining: a found block can no longer be silently lost when `submitblock`
  fails. The block hex is archived to disk before the first attempt, transient
  RPC failures are retried in-line and then by a background task (for up to two
  hours), and `duplicate`/`inconclusive` node responses are treated as success
  so retries are idempotent. Submission now also runs via `spawn_blocking`
  instead of blocking the session task on the synchronous RPC client.
- Network: a transient `accept()` error (e.g. `ECONNABORTED` from a peer reset
  mid-handshake, or `EMFILE` under fd exhaustion) no longer terminates the
  listener — and with it the whole pool. The accept loop now logs, backs off
  briefly, and continues.
- Security: the connection rate limiter no longer panics on `Instant`
  underflow when the pool starts within 60 seconds of host boot — previously
  this could kill the background pruner task (leaving the per-IP window map
  growing forever) or crash the accept loop on an incoming connection.

## 0.3.1 — 2026-06-09

### Added
- Config: `security.max_worker_name_len` (default 128) caps the accepted worker
  / SV2 user-identity name length.
- Dashboard: footer showing the running build version.

### Changed
- Packaging (systemd): the unit now waits for bitcoind's RPC to be ready before
  starting, via an `ExecStartPre` probe that loops `getblockchaininfo` until it
  succeeds (`After=`/`Wants=bitcoind.service`, `TimeoutStartSec` raised). This
  prevents the pool from exiting on a transient `getblocktemplate` failure when
  it starts against a node that is still warming up (RPC `-28`).
- Packaging (systemd): the readiness probe passes `-datadir` to `bitcoin-cli` so
  it does not read `~/.bitcoin`, which `ProtectHome` blocks (it would otherwise
  abort before reaching the RPC call).
- Packaging (systemd): moved `StartLimitIntervalSec`/`StartLimitBurst` from
  `[Service]` to `[Unit]`, where systemd actually honors them (they were
  silently ignored, producing a recurring journal warning).

### Fixed
- Logging: ANSI color escape sequences are no longer written to file logs
  (`log_dir`). `with_ansi(false)` was applied only to the stdout appenders, so
  on-disk logs at e.g. `/var/log/btcpool-rs/` were cluttered with escape
  codes. Now disabled for both the JSON and plain file appenders.

### Security
- Network: bounded the SV1 line reader so an unauthenticated peer can no longer
  stream an endless newline-free line and exhaust memory before the size check —
  `max_message_bytes` is now enforced *during* the read.
- Network: validate worker / SV2 user-identity names (non-empty, length-capped,
  no control or whitespace characters) at `mining.authorize` /
  `OpenExtendedMiningChannel`. Bounds the per-worker stats maps and Prometheus
  label cardinality against untrusted input and blocks control-character
  injection into logs, metrics, and the dashboard.
- SV2: reject frames whose declared size exceeds the message limit before
  allocating and decrypting the body (previously up to ~16 MB per frame versus
  the 4 KB policy intent).
- Network: prune the per-IP connection-rate-limiter map so it cannot grow
  unbounded across spoofed / IPv6 source addresses.
- Network: bound the SV1 protocol-detect peek and the SV2 Noise handshake with
  the idle timeout, preventing slowloris connection-holding before the session
  loop's idle timeout engages.

## 0.3.0 — 2026-06-06

### Added
- Packaging: CI workflow, Docker image, and a systemd service unit.
- Dashboard: network hash rate and difficulty-change stats.
- Dashboard: embedded favicon, served at `/favicon.ico`.

### Fixed
- Jobs: seed the job-id high bits per process so jobs are not mislabeled as
  stale after a restart.

### Documentation
- Added a real security policy (supported versions, private reporting, scope).
- Clarified the crate description: Stratum V2 is implemented, not just planned.

## 0.2.0 — 2026-05-31

### Added
- Stratum V2 (Noise-encrypted) support via dual-stack SV1 + SV2 auto-detection
  on a single port.
- All-time and session-best hash rate tracking in stats and on the dashboard.
- Link from the dashboard to the raw `/metrics` endpoint.

### Changed
- Dashboard rework: worker rendering and stats mapping fixes; reject rate moved
  into the rejected card; best share keyed by vardiff difficulty.

[Unreleased]: https://github.com/edifus/btcpool-rs/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/edifus/btcpool-rs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/edifus/btcpool-rs/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/edifus/btcpool-rs/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/edifus/btcpool-rs/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/edifus/btcpool-rs/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/edifus/btcpool-rs/releases/tag/v0.1.0
