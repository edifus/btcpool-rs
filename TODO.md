# TODO

Backlog of known work, ordered by priority. Most items come from the June 2026
full-codebase security/performance review; PR #6 fixed the top three findings
(block-loss on `submitblock` failure, fatal `accept()` errors, `Instant`
underflow panic). Line references are as of that review and may drift.

## High — pre-auth DoS hardening

- [x] **Cap attacker-controlled worker-name growth.** Fixed: per-session cap on
  distinct authorized identities (`max_authorizations_per_session`, default 8),
  token bucket extended to all inbound messages, 24 h TTL eviction of offline
  workers from the in-memory maps, `PrometheusBuilder::idle_timeout` (24 h),
  and `worker_best_shares` bounded to the top 512 rows (pruned at boot +
  periodically).
- [x] **Dedicated handshake timeout for protocol auto-detect.** Fixed with a
  10 s pre-auth deadline covering the first-byte peek, the SV2 Noise handshake,
  and both session loops until a worker authorizes / a channel opens.

## Medium

- [x] **Move remaining blocking I/O off the async runtime.** `submit_block` was
  done in PR #6. SQLite followed: writes go to a dedicated `stats-writer` thread
  over a bounded channel, the store opens with WAL + `synchronous=NORMAL` + a
  busy timeout, and the dashboard `/history` + `/chart` scans run under
  `spawn_blocking` against a separate read connection.

  Closed out 2026-08-05 with the Bitcoin RPC. Auditing the surface turned up
  four sync-in-async call sites, not just the `getblocktemplate` in
  `TemplateEngine::refresh` this item originally named — also `best_block_hash`
  in the ZMQ poll fallback (1 Hz), and `network_hashrate` +
  `estimate_difficulty_change_pct` (five sequential round trips) in the 30 s
  stats loop. Rather than wrap each call site, `RpcClient`'s synchronous method
  bodies are now private and the only public surface is `async` wrappers that
  `spawn_blocking` internally, so a future call site cannot reintroduce the bug.
  `bitcoin_rpc.timeout_secs` is applied at last (it was parsed, documented, and
  then dropped on the floor) — it is what bounds how long a wedged node can hold
  a blocking-pool thread, since a `spawn_blocking` task cannot be cancelled.

- [x] **The template engine can stop refreshing without anyone noticing.**
  Fixed 2026-08-06, all three suggested directions plus the observability the
  title asks for. The ZMQ listener is supervised and reconnects forever with
  capped exponential backoff (1 s → 60 s, resetting only after a connection
  survives 60 s), so its `watch::Sender` is never dropped; `run` additionally
  survives a closed channel by latching that `select!` branch off rather than
  breaking, degrading to a 30 s-latency polling pool instead of freezing. The
  poll fallback became a permanent concurrent backstop, because the worst case
  turned out to be a socket that connects and never publishes — `zmq_connect` is
  asynchronous, so a wrong port or a node in IBD produces no error for a
  supervisor to react to. Freshness is exposed via `GET /health` (503 past 180 s,
  and before the first refresh ever lands),
  `pool_template_last_refresh_timestamp_seconds`,
  `pool_tip_changes_discovered_by_timer_total` and an edge-triggered `error!`.

  Two corrections to this entry's original framing, found while fixing it.
  A block mined on a frozen template is not orphaned — it is consensus-valid
  (the frozen template is internally consistent), so the node stores it on a
  side branch and `submitblock` returns `"inconclusive"`. The real cost is
  wasted hashrate with a low-probability catastrophic tail:
  `(h/H) × (D/600)` expected blocks forfeited over a freeze of `D` seconds.
  And the damage was not limited to the frozen case — the ntime timer
  broadcast newly-discovered tips as `clean=false`, which on SV2 meant an
  immediate job on a prev-hash the device had not been moved to, rejecting
  100% of its shares until the session was dropped. `refresh` now derives
  `clean` by comparing `prev_hash`.

- [x] **Stale-tip blocks are reported as wins** (fixed 2026-08-06):
  `submit_block` now returns `BlockSubmitOutcome::{Accepted, Duplicate,
  Inconclusive}`, threaded through `submit_found_block`, and only `is_win()`
  outcomes reach `metrics::block_found()` / `stats.block_found()`.

  Three corrections to this entry's original framing, found while fixing it.
  `"duplicate-inconclusive"` was grouped with plain `"duplicate"` and had the
  identical false-win problem, so it is `Inconclusive` too. There was a third
  reporting site the entry missed — the background resubmit task in
  `engine.rs` — and it is the one most likely to see an inconclusive result,
  because it runs minutes after the block was found. And the reporting sequence
  existed as three hand-written copies, which is how the miscount spread; it now
  lives in `accounting::record_block_outcome`, alongside the share-accounting
  helpers that exist for the same reason.
- [x] **Confirm block wins survive a reorg** (fixed 2026-08-06): every block the
  node stores is enrolled in a `found_blocks` ledger in SQLite and re-checked
  with `getblockheader` on a 60 s sweep until it is `[pool] confirmation_depth`
  (default 6) deep on the active chain or that deep on a branch that lost.
  `pool_blocks_orphaned_total` carries the correction Prometheus cannot make to
  a counter; the dashboard count drops back and the card is marked.

  Four things this entry's framing missed, found while fixing it.
  The reconciliation is bidirectional — an `inconclusive` block that a later
  reorg puts on the active chain is a win the submit-time verdict can never
  count, and it costs two extra match arms. "After N confirmations" is not
  sufficient on its own: the orphan side needs the *losing* branch buried by the
  same depth, or a routine one-block reorg resolves the block as orphaned during
  the very reorg that was about to restore it. There was no durable record of a
  found block to re-check at all — only the archived hex, which is written for
  every attempt including rejects — hence the ledger, which also makes the
  dashboard's "found blocks survive restarts" label true for the first time.
  And the hash the pool recorded was the raw little-endian double-SHA256, so it
  would have been rejected by `getblockheader` outright; it was reversed at the
  source, which incidentally fixes the dashboard card, the log lines and the
  archive filenames, none of which resolved in a block explorer.
- [x] **Harden the duplicate-share set** (shipped in v0.6.0, 2026-07-02):
  shares are recorded for dedup only after validation passes, and the
  per-session set clears on every clean-job broadcast (live-jobs scoping); the
  4096 FIFO cap remains as a memory backstop only.
- [x] **Credit background-retrier block acceptance to dashboard stats**
  (shipped in v0.6.0, 2026-07-02): worker + `PoolStats` are threaded through
  `submit_found_block` into the resubmit task; retry success now mirrors the
  inline-success stats update.

## Low

- [x] Monotonic guard on pool best-share/best-hashrate SQLite `UPDATE`s
  (`WHERE ?1 > ...`), matching the per-worker variant; best-hashrate in-memory
  update is now a CAS. (shipped in v0.6.0, 2026-07-02)
- [x] Fix ghost-online accounting: repeated `mining.authorize` increments
  `active_sessions` per call but disconnect decrements once, for the last name
  only. (Fixed alongside the authorization cap: same-name re-auth is a no-op,
  switching names releases the previous one.)
- [x] Hot-path cleanups: recompute hashrate windows only on accepted shares
  (today: 4 full deque scans per inbound message); move per-share hex/format
  allocations inside `debug!` so they're skipped when disabled; reuse a scratch
  buffer instead of cloning `coinbase_template` per share.

  Status update (2026-08-05): partially addressed.
  - [x] Hashrate no longer performs deque scans per inbound message. Accepted
    shares add to per-session accumulators, and the ckpool-style decay task
    folds them into all seven windows every two seconds.

  Completed (2026-08-06):
  - [x] The three eager `String`s above the "Validating submitted share"
    `debug!` are now field expressions inside it. `tracing` only evaluates
    those when the callsite is enabled, so they cost nothing at the default
    `info` level; log output is unchanged. The below-target `warn!` in the
    validator now reuses `block_hash_display` instead of open-coding a second
    reverse-and-encode.
  - [x] `assemble_coinbase_into` splices into a caller-owned buffer, and the
    validator holds one per blocking-pool thread (`COINBASE_SCRATCH`), so a
    validated share no longer clones `coinbase_template`. Width-mismatch
    behaviour is unchanged: the region stays zeroed and the share fails on its
    merits.
  - [x] Adjacent allocations in the same functions: dropped the never-read
    `ShareParams.worker` (one `String` per share on both SV1 and SV2), stopped
    building three throwaway `String`s in `mining.submit` parsing, and switched
    `build_header`'s prev-hash decode to `hex::decode_to_slice` into a `[u8; 32]`
    — which also turns a would-be `copy_from_slice` panic on a wrong-length
    prev-hash into `PoolError::InvalidHeader`.

  Not done, deliberately: the `metrics`/`stats` per-share label allocations
  (`worker.to_string()` twice per accepted share, three times per rejected one).
  Those need worker-name interning or a label-API change, not a cleanup.

## Planned features

- [x] **v0.4.0: non-root Docker image** (shipped in v0.4.0, 2026-06-11).
- [x] **SV2 identity pinning** (shipped in v0.6.0, 2026-07-02): persistent
  Noise authority key (`[sv2] authority_key_file`, cookie-style
  create-on-first-start), pubkey logged at boot + shown in the dashboard
  Connect modal + `GET /api/info`; `persist_authority_key = false` opts out,
  `cert_validity_secs` configurable. Verified on a NerdQAxe++: pinned key
  verifies and mines, wrong key rejected. Note: the bitaxe/nerdqaxe firmware
  checks only the Schnorr signature, never the validity window (no wall
  clock); upstream enforcement-toggle PRs: bitaxeorg/ESP-Miner#1796,
  shufps/ESP-Miner-NerdQAxePlus#656.
- [ ] **SV1-over-TLS (`stratum+ssl://`) — DEFERRED, build only on request.**
  Decision (2026-06-15): not building it. The target audience is the
  self-hosted *solo* crowd on a trusted LAN, where the value is marginal — solo
  mining has no account password to leak; TLS would only hide the payout address
  and hashrate from a passive on-path observer. SV2 (Noise) already encrypts the
  modern firmware path, so this is purely for legacy SV1 devices over an
  untrusted network (a shrinking niche), and client support for `stratum+ssl` is
  spotty (cgminer/Avalon yes; AxeOS/ESP-Miner version-dependent). Revisit only
  if a real user asks for it.
- [ ] Cookie to save selected chart options for viewing in browser.

  Design notes for when/if that happens, so it doesn't become a support burden:
  - **rustls / `tokio-rustls`, not OpenSSL** — keeps the pure-Rust single-binary
    and arm64/musl cross-compile story intact.
  - **Separate `tls_port`** (e.g. 3334), *not* the auto-detect port. The detector
    is binary (`first[0] == b'{'` → SV1, else → SV2, `server.rs`); a TLS
    ClientHello (`0x16`) lands in the "else → SV2" bucket and collides with the
    Noise handshake's arbitrary first byte, so TLS cannot share that socket.
  - The TLS listener wraps TCP, does the handshake, then feeds the **decrypted**
    stream into the *same* auto-detect + session path — so SV1 and SV2 both work
    over TLS for free. Only refactor needed: make `session::run` generic over
    `impl AsyncRead + AsyncWrite + Unpin + Send` (`tokio::io::split` instead of
    `TcpStream::into_split`).
  - **Cert UX is the real problem, not the crypto.** Default to a pool-generated
    **self-signed cert** (`rcgen`) written to the data dir on first boot, so the
    user manages nothing — Stratum-over-TLS clients generally don't verify the
    cert anyway (opportunistic encryption), which still defeats passive
    eavesdropping. Optional `cert_path`/`key_path` override (+ SIGHUP reload) for
    anyone wanting a CA-signed cert. Skip ACME/Let's Encrypt (needs a public
    domain + inbound reachability — impractical behind home NAT). Ship opt-in,
    off by default; document as "encryption for SV1 miners over untrusted
    networks, not needed on a trusted LAN."
- [ ] **Multi-node Bitcoin RPC failover.** Today a single `bitcoin_rpc.url`; if
  that node restarts (see the near-daily needrestart sweep) or crashes, template
  refresh stalls until it returns. Accept a list of node endpoints and fail over
  on connect error / RPC error / stale tip, preferring the highest-tip healthy
  node. Stays within the single-binary, zero-ops thesis (no external HA layer).
- [ ] Dependency refresh when convenient: `rusqlite` 0.29 and
  `metrics-exporter-prometheus` 0.15 are a few majors behind (no advisories,
  just aging).
