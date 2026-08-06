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

- [ ] **The template engine can stop refreshing without anyone noticing.** If
  the ZMQ/poll task exits, `TemplateEngine::run` breaks out of its loop
  (`engine.rs:119-122`) and the pool serves a frozen template forever — miners
  keep hashing on a dead tip and any block they find is orphaned. The
  `JoinHandle` from `tokio::spawn(engine.run(new_block_rx))` (`main.rs:139`) is
  dropped, so nothing observes or restarts it. At minimum the exit should be
  loud; better would be keeping the ntime loop alive on channel close, or
  supervising the ZMQ task so it restarts.
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
- [ ] Hot-path cleanups: recompute hashrate windows only on accepted shares
  (today: 4 full deque scans per inbound message); move per-share hex/format
  allocations inside `debug!` so they're skipped when disabled; reuse a scratch
  buffer instead of cloning `coinbase_template` per share.

  Status update (2026-08-05): partially addressed.
  - [x] Hashrate no longer performs deque scans per inbound message. Accepted
    shares add to per-session accumulators, and the ckpool-style decay task
    folds them into all seven windows every two seconds.
  - [ ] Move the remaining eager per-share hex/format allocations behind the
    relevant tracing level.
  - [ ] Reuse a scratch buffer instead of cloning `coinbase_template` for each
    validated share.

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
