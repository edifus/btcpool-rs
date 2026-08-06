/// network/session.rs
///
/// Per-miner TCP session state machine.
///
/// Lifecycle:
///   connect → subscribe → authorize → [receive jobs] → [submit shares] → disconnect
///
/// Handles all ASIC extensions:
///   - mining.configure (version-rolling, minimum-difficulty, subscribe-extranonce)
///   - vardiff with async retarget checks
///   - Stale share accounting
///   - Submit latency tracking
///   - Security guards (rate limiting, invalid share counting, message size)
use crate::{
    bitcoin::template::{build_job_for_payout, JobTemplate, StratumJob},
    config::{Config, VardiffConfig},
    error::PoolError,
    metrics,
    mining::{
        accounting::{self, RejectReason},
        credit::ShareCredit,
        engine::{JobBroadcast, TemplateEngine},
        identity::MinerIdentity,
        jobs::IssuedJobs,
        validator::{self, ShareParams, ShareResult, ShareSet},
        vardiff::Vardiff,
    },
    protocol::sv1::{
        AuthorizeParams, ClientMessage, ConfigureParams, ResponseBuilder, StratumRequest,
        SubmitParams, SubscribeParams, SuggestDifficultyParams,
    },
    security::{BanList, SessionGuard},
    stats::PoolStats,
};
use rand::RngCore;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::broadcast,
    task,
};
use tracing::{debug, error, info, warn};

// ─────────────────────────────────────────────────────────────────────────────
// Session state
// ─────────────────────────────────────────────────────────────────────────────

pub struct Session {
    // Identity
    pub peer: SocketAddr,
    pub worker: Option<String>,
    pub identity: Option<Arc<MinerIdentity>>,
    pub user_agent: Option<String>,
    pub session_id: String,

    // Protocol state
    subscribed: bool,
    authorized: bool,
    version_rolling_enabled: bool,
    version_rolling_mask: u32,
    version_rolling_min_bit_count: Option<u32>,

    // Extranonce
    pub extranonce1: Vec<u8>,
    pub extranonce2_size: usize,

    // Job tracking
    pub current_template: Option<Arc<JobTemplate>>,
    issued_jobs: IssuedJobs,
    coinbase_tag: String,
    network: bitcoin::Network,

    // Difficulty & vardiff
    pub difficulty: u64,
    vardiff: Vardiff,
    vardiff_cfg: VardiffConfig,
    /// What one accepted share is worth to the hashrate estimator.
    credit: ShareCredit,

    // Share dedup
    share_set: ShareSet,

    // Security
    guard: SessionGuard,

    // Stats
    shares_accepted: u64,
    shares_rejected: u64,
    connect_time: Instant,
    stats: Arc<PoolStats>,
}

impl Session {
    pub fn new(
        peer: SocketAddr,
        cfg: &Config,
        extranonce1: Vec<u8>,
        stats: Arc<PoolStats>,
        network: bitcoin::Network,
    ) -> Self {
        let initial_diff = cfg.pool.initial_difficulty;
        Self {
            peer,
            worker: None,
            identity: None,
            user_agent: None,
            session_id: format!("{:016x}", random_u64()),
            subscribed: false,
            authorized: false,
            version_rolling_enabled: false,
            version_rolling_mask: crate::mining::validator::VERSION_ROLLING_MASK,
            version_rolling_min_bit_count: None,
            extranonce1,
            extranonce2_size: cfg.pool.extranonce2_size,
            current_template: None,
            issued_jobs: IssuedJobs::new(),
            coinbase_tag: cfg.pool.coinbase_tag.clone(),
            network,
            difficulty: initial_diff,
            vardiff: Vardiff::new(cfg.vardiff.clone(), initial_diff),
            vardiff_cfg: cfg.vardiff.clone(),
            credit: ShareCredit::new(cfg.vardiff.min_difficulty, Instant::now()),
            share_set: ShareSet::new(),
            guard: SessionGuard::new(&cfg.security),
            shares_accepted: 0,
            shares_rejected: 0,
            connect_time: Instant::now(),
            stats,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main session loop
// ─────────────────────────────────────────────────────────────────────────────

pub async fn run(
    stream: TcpStream,
    peer: SocketAddr,
    config: Arc<Config>,
    engine: Arc<TemplateEngine>,
    ban_list: Arc<BanList>,
    stats: Arc<PoolStats>,
    network: bitcoin::Network,
) {
    if ban_list.is_banned(&peer.ip()) {
        debug!("Rejected banned IP: {peer}");
        return;
    }

    metrics::miner_connected();
    stats.miner_connected();
    info!("Miner connected: {peer}");

    let extranonce1 = generate_extranonce1(config.pool.extranonce1_size);
    let mut session = Session::new(peer, &config, extranonce1, stats, network);
    let mut job_rx: tokio::sync::broadcast::Receiver<JobBroadcast> = engine.subscribe();

    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line_buf: Vec<u8> = Vec::new();
    let writer = tokio::sync::Mutex::new(writer);

    // Cache current job for later authorize, but do not notify yet.
    if let Some(template) = engine.current_template().await {
        session.current_template = Some(template);
    }

    let idle_timeout = tokio::time::Duration::from_secs(config.pool.idle_timeout_secs);
    let preauth_timeout =
        tokio::time::Duration::from_secs(crate::network::server::HANDSHAKE_TIMEOUT_SECS);
    // Idle tracking must anchor on the last *inbound* message: every completed
    // select! iteration (job broadcasts arrive far more often than the idle
    // timeout) recreates the read-arm future, so a plain timeout() would reset
    // the idle clock and a dead peer that keeps receiving jobs never times out.
    let mut last_inbound = tokio::time::Instant::now();

    loop {
        // Until a worker authorizes, hold the connection to the short handshake
        // deadline — real miners subscribe/authorize immediately, and a stalled
        // pre-auth connection pins one of the bounded global slots.
        let read_timeout = if session.authorized {
            idle_timeout
        } else {
            preauth_timeout
        };
        tokio::select! {
            // ── Inbound message from miner ──────────────────────────────────
            line_result = tokio::time::timeout_at(
                last_inbound + read_timeout,
                read_line_bounded(&mut reader, &mut line_buf, session.guard.max_message_bytes),
            ) => {
                match line_result {
                    Err(_) => {
                        warn!("Miner {peer} idle timeout — disconnecting");
                        break;
                    }
                    Ok(Err(e)) if e.kind() == std::io::ErrorKind::InvalidData => {
                        // Line exceeded max_message_bytes mid-read (or invalid UTF-8).
                        warn!("{peer} {e}");
                        ban_list.ban(peer.ip(), "message too large");
                        break;
                    }
                    Ok(Err(e)) => {
                        debug!("Read error from {peer}: {e}");
                        break;
                    }
                    Ok(Ok(None)) => {
                        debug!("Miner {peer} disconnected (EOF)");
                        break;
                    }
                    Ok(Ok(Some(_len))) => {
                        last_inbound = tokio::time::Instant::now();
                        let line = match std::str::from_utf8(&line_buf) {
                            Ok(s) => s,
                            Err(_) => {
                                debug!("Non-UTF8 line from {peer} — disconnecting");
                                break;
                            }
                        };

                        tracing::trace!(peer = %peer, raw = %line, "← miner");
                        let response = handle_line(&mut session, line, &engine, &ban_list).await;

                        match response {
                            HandleResult::Messages(msgs) => {
                                if !send_messages(&writer, peer, msgs).await {
                                    break;
                                }
                            }
                            HandleResult::Disconnect(reason) => {
                                if let Some(worker) = &session.worker {
                                    metrics::miner_disconnect(&reason, worker);
                                }
                                warn!("Disconnecting {peer}: {reason}");
                                break;
                            }
                        }

                        if let Some(new_diff) = session.vardiff.check_retarget() {
                            let old_diff = session.difficulty;
                            session.difficulty = new_diff;
                            if let Some(worker) = &session.worker {
                                metrics::vardiff_retarget(worker, old_diff, new_diff);                                session.stats.update_worker_vardiff(worker, new_diff);                            }
                            let msg = ResponseBuilder::set_difficulty(new_diff);
                            debug!(
                                peer = %session.peer,
                                worker = ?session.worker,
                                difficulty = new_diff,
                                "Sending vardiff update"
                            );
                            if !send_messages(&writer, peer, vec![msg]).await {
                                break;
                            }
                        }
                    }
                }
            }

            // ── New job broadcast from template engine ──────────────────────
            job_result = job_rx.recv() => {
                match job_result {
                    Ok(JobBroadcast { template, clean }) => {
                        if clean {
                            // A clean job retires every outstanding job; shares
                            // for them are now rejected as stale before dedup,
                            // so the dedup entries have nothing left to guard.
                            session.share_set.clear();
                        }
                        if session.subscribed && session.authorized {
                            let job = match materialize_job(&session, template.clone()) {
                                Ok(job) => job,
                                Err(e) => {
                                    error!(peer = %session.peer, "Failed to build payout job: {e}");
                                    break;
                                }
                            };
                            let notify = build_notify(&job, clean);
                            session.issued_jobs.issue(job.clone(), clean);
                            session.current_template = Some(template);

                            debug!(
                                peer = %session.peer,
                                worker = ?session.worker,
                                job_id = %job.job_id,
                                difficulty = session.difficulty,
                                clean_jobs = clean,
                                "Sending broadcast mining.notify"
                            );

                            let msgs = vec![
                                ResponseBuilder::set_difficulty(session.difficulty),
                                notify,
                            ];

                            if !send_messages(&writer, peer, msgs).await {
                                break;
                            }
                        } else {
                            session.current_template = Some(template);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("{peer} missed {n} job broadcasts");
                    }
                    Err(_) => break,
                }
            }
        }
    }

    metrics::miner_disconnected();
    session.stats.miner_disconnected();
    let uptime = session.connect_time.elapsed().as_secs() as f64;
    if let Some(worker) = &session.worker {
        session.stats.mark_worker_offline(worker);
        metrics::connection_duration(worker, uptime);
    }
    info!(
        peer = %peer,
        worker = ?session.worker,
        accepted = session.shares_accepted,
        rejected = session.shares_rejected,
        uptime_secs = uptime,
        // False when the miner kept submitting below the difficulty it was
        // sent, so its hashrate was credited at the acceptance floor instead.
        honors_set_difficulty = session.credit.honors_assigned(),
        "Miner session ended"
    );
}

async fn send_messages(
    writer: &tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>,
    peer: SocketAddr,
    msgs: Vec<String>,
) -> bool {
    let mut w = writer.lock().await;

    for msg in msgs {
        tracing::trace!(peer = %peer, raw = %msg, "→ pool");
        let line = format!("{msg}\n");
        if let Err(e) = w.write_all(line.as_bytes()).await {
            warn!("Write error to {peer}: {e}");
            return false;
        }
    }

    if let Err(e) = w.flush().await {
        warn!("Flush error to {peer}: {e}");
        return false;
    }

    true
}

/// Read one line (up to but excluding `\n`) into `buf`, enforcing `max` bytes
/// *during* the read. Unlike `AsyncBufReadExt::next_line`, which grows an
/// unbounded buffer until a newline arrives, this aborts the moment the
/// accumulated bytes exceed `max` — so a peer cannot stream gigabytes with no
/// newline and exhaust memory before the size check runs.
///
/// Returns `Ok(None)` on EOF with no buffered data, `Ok(Some(len))` with the
/// line bytes in `buf` (trailing `\r` stripped), or an `InvalidData` error when
/// the line exceeds `max`.
async fn read_line_bounded<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    max: usize,
) -> std::io::Result<Option<usize>> {
    buf.clear();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(if buf.is_empty() {
                None
            } else {
                Some(buf.len())
            });
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            buf.extend_from_slice(&available[..pos]);
            reader.consume(pos + 1);
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            // Enforce the cap here too: a newline early in a single BufReader
            // chunk reaches this branch without the per-iteration check below,
            // so an oversize line ending within one ~8 KiB read would otherwise
            // slip through.
            if buf.len() > max {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "line exceeds max message size",
                ));
            }
            return Ok(Some(buf.len()));
        }
        buf.extend_from_slice(available);
        let consumed = available.len();
        reader.consume(consumed);
        if buf.len() > max {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "line exceeds max message size",
            ));
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Message dispatch
// ─────────────────────────────────────────────────────────────────────────────

enum HandleResult {
    Messages(Vec<String>),
    Disconnect(String),
}

async fn handle_line(
    session: &mut Session,
    line: &str,
    engine: &Arc<TemplateEngine>,
    ban_list: &Arc<BanList>,
) -> HandleResult {
    // One token per inbound message, not just submits: authorize/configure/
    // subscribe floods are as cheap to send as shares and feed the same
    // per-message stats work, so they share the same bucket.
    if !session.guard.share_rate.try_consume() {
        metrics::share_rejected("rate_limited", session.worker.as_deref().unwrap_or("?"));
        ban_list.ban(session.peer.ip(), "message rate exceeded");
        return HandleResult::Disconnect("rate limited".into());
    }

    // Blank / whitespace-only lines (e.g. firmware keepalive newlines) aren't
    // errors — ignore them instead of letting the empty string fail JSON parsing
    // and drop the connection. They still consumed a rate-limit token above, so a
    // blank-line flood stays bounded.
    if line.trim().is_empty() {
        return HandleResult::Messages(vec![]);
    }

    let req = match StratumRequest::parse(line) {
        Ok(r) => r,
        Err(e) => {
            // Keep the detailed serde message in the log, but emit a *bounded*
            // disconnect reason for the metric label: the serde error embeds the
            // line/column and varies with input, so using it as a Prometheus
            // label lets untrusted bytes mint unbounded time series.
            debug!("Parse error from {}: {e}", session.peer);
            return HandleResult::Disconnect("parse error".into());
        }
    };

    let msg = match ClientMessage::from_request(&req) {
        Ok(m) => m,
        Err(e) => {
            return HandleResult::Messages(vec![ResponseBuilder::err(
                &req.id,
                e.to_stratum_error(),
            )]);
        }
    };

    match msg {
        ClientMessage::Configure(params) => handle_configure(session, &req, params),
        ClientMessage::Subscribe(params) => handle_subscribe(session, &req, params),
        ClientMessage::Authorize(params) => handle_authorize(session, &req, params, engine).await,
        ClientMessage::Submit(params) => handle_submit(session, &req, params, engine).await,
        ClientMessage::SuggestDifficulty(params) => {
            handle_suggest_difficulty(session, &req, params)
        }
        ClientMessage::Unknown(method) => {
            debug!("Unknown method from {}: {method}", session.peer);
            HandleResult::Messages(vec![ResponseBuilder::err(
                &req.id,
                PoolError::UnknownMethod(method).to_stratum_error(),
            )])
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler implementations
// ─────────────────────────────────────────────────────────────────────────────

fn handle_configure(
    session: &mut Session,
    req: &StratumRequest,
    params: ConfigureParams,
) -> HandleResult {
    if params.version_rolling {
        let negotiated = params
            .version_rolling_mask
            .map(|m| m & session.version_rolling_mask)
            .unwrap_or(session.version_rolling_mask);

        let negotiated_bits = negotiated.count_ones();
        let min_bits_ok = params
            .version_rolling_min_bit_count
            .map(|min_bits| negotiated_bits >= min_bits)
            .unwrap_or(true);

        session.version_rolling_enabled = min_bits_ok && negotiated != 0;
        session.version_rolling_mask = negotiated;
        session.version_rolling_min_bit_count = params.version_rolling_min_bit_count;

        if session.version_rolling_enabled {
            debug!(
                "{} version-rolling enabled, mask={:08x}",
                session.peer, negotiated
            );
        } else {
            debug!(
                "{} version-rolling not enabled: negotiated mask {:08x} does not satisfy requested minimum bit count {:?}",
                session.peer, negotiated, params.version_rolling_min_bit_count
            );
        }
    }

    // Always include minimum-difficulty so that Avalon Nano / cgminer-based firmware
    // knows the pool share threshold. The Nano 3 uses this field to configure its
    // hardware submission threshold; without it, it defaults to network difficulty
    // and never submits pool-level shares.
    let configured_min_diff = Some(session.difficulty);

    HandleResult::Messages(vec![ResponseBuilder::configure(
        &req.id,
        session.version_rolling_enabled,
        session.version_rolling_mask,
        configured_min_diff,
        params.subscribe_extranonce,
    )])
}

fn handle_suggest_difficulty(
    session: &mut Session,
    req: &StratumRequest,
    params: SuggestDifficultyParams,
) -> HandleResult {
    // Seed vardiff from the hint, clamped to the configured floor/ceiling so a
    // (buggy or hostile) suggestion can never drop a miner below the share-rate
    // floor. Vardiff owns the difficulty from here.
    let applied = session.vardiff.suggest(params.difficulty);
    session.difficulty = applied;
    debug!(
        peer = %session.peer,
        suggested = params.difficulty,
        applied,
        floor = session.vardiff_cfg.min_difficulty,
        ceiling = session.vardiff_cfg.max_difficulty,
        "Applied mining.suggest_difficulty"
    );

    let mut msgs = vec![ResponseBuilder::ok(&req.id, serde_json::Value::Bool(true))];
    // If the miner is already receiving work, push the new target now; otherwise
    // the set_difficulty sent during subscribe/authorize will carry the seed.
    if session.subscribed {
        msgs.push(ResponseBuilder::set_difficulty(applied));
    }
    HandleResult::Messages(msgs)
}

fn handle_subscribe(
    session: &mut Session,
    req: &StratumRequest,
    params: SubscribeParams,
) -> HandleResult {
    session.subscribed = true;
    session.user_agent = params.user_agent.clone();
    debug!(
        peer = %session.peer,
        user_agent = ?params.user_agent,
        "Subscribed"
    );

    HandleResult::Messages(vec![ResponseBuilder::subscribe(
        &req.id,
        &session.session_id,
        &hex::encode(&session.extranonce1),
        session.extranonce2_size,
    )])
}

async fn handle_authorize(
    session: &mut Session,
    req: &StratumRequest,
    params: AuthorizeParams,
    engine: &Arc<TemplateEngine>,
) -> HandleResult {
    if !session.subscribed {
        return HandleResult::Messages(vec![ResponseBuilder::err(
            &req.id,
            PoolError::NotSubscribed.to_stratum_error(),
        )]);
    }

    if let Err(e) = session.guard.check_worker_name(&params.worker) {
        warn!(peer = %session.peer, "Rejected worker name: {e}");
        return HandleResult::Messages(vec![ResponseBuilder::err(&req.id, e.to_stratum_error())]);
    }

    let identity = match MinerIdentity::parse(
        &params.worker,
        session.network,
        session.guard.max_worker_name_len,
    ) {
        Ok(identity) => Arc::new(identity),
        Err(e) => {
            warn!(peer = %session.peer, worker = %params.worker, "Rejected payout identity: {e}");
            return HandleResult::Messages(vec![ResponseBuilder::ok(
                &req.id,
                serde_json::Value::Bool(false),
            )]);
        }
    };

    // Only a *new* identity counts against the cap or touches the stats maps;
    // re-authorizing the same name (some firmware does on reconnect-in-place)
    // must not inflate active_sessions or the authorization count.
    if session.worker.as_deref() != Some(params.worker.as_str()) {
        if !session.guard.record_new_authorization() {
            return HandleResult::Disconnect("too many worker identities".into());
        }
        if let Some(prev) = session.worker.take() {
            session.stats.mark_worker_offline(&prev);
        }
        session.issued_jobs.clear();
        session.share_set.clear();
        session
            .stats
            .mark_worker_online(&params.worker, session.difficulty);
        session.stats.set_worker_protocol(&params.worker, "sv1");
    }
    session.authorized = true;
    session.worker = Some(params.worker.clone());
    session.identity = Some(identity);

    info!(
        peer = %session.peer,
        worker = %params.worker,
        "Authorized"
    );

    let mut msgs = Vec::new();
    msgs.push(ResponseBuilder::ok(&req.id, serde_json::Value::Bool(true)));

    let diff_msg = ResponseBuilder::set_difficulty(session.difficulty);
    msgs.push(diff_msg);

    let current_template = if let Some(template) = session.current_template.clone() {
        Some(template)
    } else {
        engine.current_template().await
    };

    if let Some(template) = current_template {
        let job = match materialize_job(session, template.clone()) {
            Ok(job) => job,
            Err(e) => {
                error!(peer = %session.peer, "Failed to build initial payout job: {e}");
                return HandleResult::Disconnect("internal payout job error".into());
            }
        };
        session.current_template = Some(template);
        session.issued_jobs.issue(job.clone(), true);

        debug!(
            peer = %session.peer,
            worker = %params.worker,
            job_id = %job.job_id,
            difficulty = session.difficulty,
            clean_jobs = true,
            "Sending initial mining.notify after authorize"
        );

        msgs.push(build_notify(&job, true));
    } else {
        warn!(
            peer = %session.peer,
            worker = %params.worker,
            "Authorized but no current job available"
        );
    }

    HandleResult::Messages(msgs)
}

async fn handle_submit(
    session: &mut Session,
    req: &StratumRequest,
    params: SubmitParams,
    engine: &Arc<TemplateEngine>,
) -> HandleResult {
    let worker_owned = session.worker.clone().unwrap_or_else(|| "?".to_string());
    let worker: &str = &worker_owned;

    if !session.authorized || params.worker != worker {
        reject_share(session, RejectReason::Unauthorized);
        return HandleResult::Messages(vec![ResponseBuilder::err(
            &req.id,
            PoolError::NotAuthorized.to_stratum_error(),
        )]);
    }

    let submit_start = Instant::now();

    let job_entry = match session.issued_jobs.find(&params.job_id) {
        Some(entry) => entry,
        None => {
            if reject_share(session, RejectReason::JobNotFound) {
                return HandleResult::Disconnect("too many invalid shares".into());
            }
            return HandleResult::Messages(vec![ResponseBuilder::err(
                &req.id,
                PoolError::StaleJob(params.job_id.clone()).to_stratum_error(),
            )]);
        }
    };

    let share_params = ShareParams {
        worker: worker.to_string(),
        job_id: params.job_id.clone(),
        extranonce2: params.extranonce2.clone(),
        ntime: params.ntime,
        nonce: params.nonce,
        version_bits: if session.version_rolling_enabled {
            params.version_bits
        } else {
            None
        },
        version_rolling_mask: if session.version_rolling_enabled {
            Some(session.version_rolling_mask)
        } else {
            None
        },
    };

    let extranonce2_hex = hex::encode(&params.extranonce2);
    let ntime_hex = format!("{:08x}", params.ntime);
    let nonce_hex = format!("{:08x}", params.nonce);

    debug!(
        worker = worker,
        job_id = %params.job_id,
        extranonce2 = %extranonce2_hex,
        ntime = %ntime_hex,
        nonce = %nonce_hex,
        version_bits = ?params.version_bits,
        session_version_rolling = session.version_rolling_enabled,
        session_mask = %format!("{:08x}", session.version_rolling_mask),
        "Validating submitted share"
    );

    // Duplicates are only *checked* here; the key is inserted after validation
    // passes, so invalid submissions cannot occupy dedup slots.
    let share_key = validator::ShareKey::new(
        &params.job_id,
        &params.extranonce2,
        params.ntime,
        params.nonce,
        params.version_bits.unwrap_or(0),
    );
    if session.share_set.contains(&share_key) {
        if reject_share(session, RejectReason::Duplicate) {
            return HandleResult::Disconnect("too many invalid shares".into());
        }
        return HandleResult::Messages(vec![ResponseBuilder::err(
            &req.id,
            PoolError::DuplicateShare.to_stratum_error(),
        )]);
    }

    // Accept any share meeting the configured floor (min_difficulty), not the
    // current vardiff level.  For Avalon/cgminer hardware the hardware threshold
    // is set once via minimum-difficulty at configure time and never changes, so
    // subsequent set_difficulty raises cannot reduce its submission rate.
    // Validating against session.difficulty would only produce spurious rejects.
    let accept_difficulty = session.vardiff_cfg.min_difficulty;

    let validation_start = Instant::now();
    let job_height = job_entry.job.height;
    let job_payout = job_entry.job.payout_address.clone();
    let extranonce1 = session.extranonce1.clone();
    let job_entry = job_entry.clone();
    let validation = task::spawn_blocking(move || {
        validator::validate_share_no_dedup(
            &share_params,
            &job_entry.job,
            &job_entry,
            &extranonce1,
            accept_difficulty,
        )
    })
    .await;

    let validation_result = match validation {
        Ok(result) => result,
        Err(e) => {
            error!("Share validation task failed: {e}");
            return HandleResult::Disconnect("internal error".into());
        }
    };
    // Record for dedup only now that the share proved itself (Valid or Block).
    if validation_result.is_ok() {
        session.share_set.insert(share_key);
    }
    match validation_result {
        Ok(ShareResult::Valid {
            assigned_difficulty,
            hash_difficulty,
            hash,
        }) => {
            let validation_duration_ms = validation_start.elapsed().as_millis() as f64;
            metrics::share_validation_time(validation_duration_ms);

            let credit = accept_share(session, worker, hash_difficulty);
            debug!(
                worker = worker,
                job = %params.job_id,
                hash = %hex::encode(hash),
                diff = assigned_difficulty,
                hash_diff = hash_difficulty,
                credit = credit,
                latency_ms = submit_start.elapsed().as_millis(),
                "Share accepted"
            );
            HandleResult::Messages(vec![ResponseBuilder::ok(
                &req.id,
                serde_json::Value::Bool(true),
            )])
        }

        Ok(ShareResult::Block {
            hash_difficulty,
            block_hex,
            hash,
        }) => {
            let validation_duration_ms = validation_start.elapsed().as_millis() as f64;
            metrics::share_validation_time(validation_duration_ms);

            let block_hash_hex = hex::encode(hash);
            let submit_result = engine
                .submit_found_block(
                    job_height,
                    &block_hash_hex,
                    block_hex,
                    worker,
                    &job_payout,
                    session.stats.clone(),
                )
                .await;
            match submit_result {
                Ok(outcome) => {
                    accounting::record_block_outcome(
                        &session.stats,
                        outcome,
                        worker,
                        &job_payout,
                        &block_hash_hex,
                    );
                    // Credited and acked either way: the miner produced a valid
                    // block-difficulty share, and losing a same-height race is
                    // not its fault.
                    accept_share(session, worker, hash_difficulty);
                    if outcome.is_win() {
                        info!("🏆 Block submitted! worker={worker} hash={block_hash_hex}");
                    } else {
                        warn!(
                            "Block from worker={worker} hash={block_hash_hex} was valid but \
                             lost its height race; it is stored on a side branch and earned \
                             nothing"
                        );
                    }
                    HandleResult::Messages(vec![ResponseBuilder::ok(
                        &req.id,
                        serde_json::Value::Bool(true),
                    )])
                }
                Err(e) => {
                    metrics::block_submission_failure(e.submit_failure_label());
                    error!("submitblock failed: {e}");
                    HandleResult::Messages(vec![ResponseBuilder::err(
                        &req.id,
                        e.to_stratum_error(),
                    )])
                }
            }
        }

        Err(e) => {
            let validation_duration_ms = validation_start.elapsed().as_millis() as f64;
            metrics::share_validation_time(validation_duration_ms);

            let reason = RejectReason::from_error(&e);
            warn!(
                worker = worker,
                reason = reason.label(),
                job_id = %params.job_id,
                extranonce2 = %hex::encode(&params.extranonce2),
                ntime = %format!("{:08x}", params.ntime),
                nonce = %format!("{:08x}", params.nonce),
                version_bits = ?params.version_bits,
                "Share rejected: {e}"
            );
            if reject_share(session, reason) {
                return HandleResult::Disconnect("too many invalid shares".into());
            }

            HandleResult::Messages(vec![ResponseBuilder::err(&req.id, e.to_stratum_error())])
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Share bookkeeping
// ─────────────────────────────────────────────────────────────────────────────

/// Book an accepted share: session counters, vardiff, and the pool-wide stats
/// and metrics. Returns the difficulty credited to the hashrate estimator.
fn accept_share(session: &mut Session, worker: &str, hash_difficulty: u64) -> u64 {
    session.shares_accepted += 1;
    session.vardiff.record_share();
    let credit = session
        .credit
        .credit(session.difficulty, hash_difficulty, Instant::now());
    accounting::record_accepted(
        &session.stats,
        &session.session_id,
        worker,
        credit,
        hash_difficulty,
    );
    credit
}

/// Book a rejected share. Returns `true` when the session should be dropped for
/// exceeding its invalid-share budget.
fn reject_share(session: &mut Session, reason: RejectReason) -> bool {
    session.shares_rejected += 1;
    let worker = session.worker.clone().unwrap_or_else(|| "?".to_string());
    accounting::record_rejected(&session.stats, &mut session.guard, &worker, reason)
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

fn build_notify(job: &Arc<StratumJob>, clean: bool) -> String {
    ResponseBuilder::notify(
        &job.job_id,
        &job.prev_hash,
        &hex::encode(&job.coinbase1),
        &hex::encode(&job.coinbase2),
        &job.merkle_branch,
        job.version,
        &job.bits,
        job.cur_time,
        clean,
    )
}

fn materialize_job(
    session: &Session,
    template: Arc<JobTemplate>,
) -> Result<Arc<StratumJob>, PoolError> {
    let identity = session.identity.as_ref().ok_or(PoolError::NotAuthorized)?;
    build_job_for_payout(
        template,
        &identity.payout,
        &session.coinbase_tag,
        session.extranonce1.len(),
        session.extranonce2_size,
    )
    .map(Arc::new)
}

static EXTRANONCE1_COUNTER: AtomicU64 = AtomicU64::new(1);

pub(crate) fn generate_extranonce1(size: usize) -> Vec<u8> {
    let counter = EXTRANONCE1_COUNTER
        .fetch_add(1, Ordering::Relaxed)
        .to_be_bytes();
    if size <= counter.len() {
        return counter[counter.len() - size..].to_vec();
    }

    let mut buf = vec![0u8; size];
    let start = size - counter.len();
    buf[start..].copy_from_slice(&counter);
    buf
}

pub(crate) fn random_u64() -> u64 {
    let mut buf = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut buf);
    u64::from_le_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::read_line_bounded;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn reads_lines_and_strips_crlf() {
        let data = b"hello\r\nworld\n";
        let mut r = BufReader::new(&data[..]);
        let mut buf = Vec::new();

        assert_eq!(
            read_line_bounded(&mut r, &mut buf, 64).await.unwrap(),
            Some(5)
        );
        assert_eq!(&buf, b"hello");
        assert_eq!(
            read_line_bounded(&mut r, &mut buf, 64).await.unwrap(),
            Some(5)
        );
        assert_eq!(&buf, b"world");
        assert_eq!(read_line_bounded(&mut r, &mut buf, 64).await.unwrap(), None);
    }

    #[tokio::test]
    async fn rejects_line_exceeding_max_without_buffering_all() {
        // 10k bytes with no newline; cap is 16. Must error, not buffer everything.
        let data = vec![b'a'; 10_000];
        let mut r = BufReader::new(&data[..]);
        let mut buf = Vec::new();

        let err = read_line_bounded(&mut r, &mut buf, 16).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        // The accumulator never grew far past the cap (bounded by the BufReader
        // chunk size), proving we abort mid-stream rather than reading all 10k.
        assert!(
            buf.len() <= 16 + 8192,
            "buffer grew unbounded: {}",
            buf.len()
        );
    }

    #[tokio::test]
    async fn rejects_oversize_line_ending_within_one_chunk() {
        // A newline-terminated line larger than the cap, delivered in a single
        // BufReader chunk so it hits the newline-found branch. Must still error.
        let mut data = vec![b'a'; 100];
        data.push(b'\n');
        let mut r = BufReader::new(&data[..]);
        let mut buf = Vec::new();

        let err = read_line_bounded(&mut r, &mut buf, 16).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn accepts_line_exactly_at_max() {
        let mut data = vec![b'a'; 16];
        data.push(b'\n');
        let mut r = BufReader::new(&data[..]);
        let mut buf = Vec::new();
        assert_eq!(
            read_line_bounded(&mut r, &mut buf, 16).await.unwrap(),
            Some(16)
        );
    }
}
