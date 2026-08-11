//! protocol/sv2 — Stratum V2 (Extended Channel) frontend.
//!
//! A protocol-agnostic mining core already exists: [`TemplateEngine`] broadcasts
//! shared templates, each session builds identity-bound jobs, and [`validator`]
//! reconstructs/validates the header. This module is a second *frontend* over
//! that core, speaking the SV2 mining protocol to devices such as the
//! NerdQAxe++ (AxeOS ≥ v1.0.37).
//!
//! The transport is Noise-encrypted (see [`noise`]) — the pool is the responder.
//! Devices require this; they will not speak plaintext SV2.
//!
//! Lifecycle: Noise handshake → `SetupConnection` → `OpenExtendedMiningChannel`
//! → jobs pushed via `NewExtendedMiningJob` + `SetNewPrevHash` → shares via
//! `SubmitSharesExtended`.
//!
//! SV1 is served by [`crate::network::session`]; the two are auto-detected on a
//! single port in [`crate::network::server`].
mod job;
mod messages;
mod noise;

pub use noise::init as init_noise_authority;

use crate::{
    bitcoin::template::{bits_to_difficulty, build_job_for_payout, JobTemplate},
    config::{Config, VardiffConfig},
    metrics,
    mining::{
        accounting::{self, RejectReason},
        credit::ShareCredit,
        engine::{JobBroadcast, TemplateEngine},
        identity::MinerIdentity,
        jobs::IssuedJobs,
        validator::{self, ShareParams, ShareResult, ShareSet, VERSION_ROLLING_MASK},
        vardiff::Vardiff,
    },
    security::{BanList, SessionGuard},
    stats::PoolStats,
};
use common_messages_sv2::Protocol;
use const_sv2::{
    MESSAGE_TYPE_MINING_SET_NEW_PREV_HASH, MESSAGE_TYPE_NEW_EXTENDED_MINING_JOB,
    MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL, MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL_SUCCES,
    MESSAGE_TYPE_OPEN_MINING_CHANNEL_ERROR, MESSAGE_TYPE_SETUP_CONNECTION,
    MESSAGE_TYPE_SETUP_CONNECTION_ERROR, MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
    MESSAGE_TYPE_SET_TARGET, MESSAGE_TYPE_SUBMIT_SHARES_ERROR, MESSAGE_TYPE_SUBMIT_SHARES_EXTENDED,
    MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, sync::broadcast, task};
use tracing::{debug, error, info, warn};

use noise::NoiseWriter;

/// Highest SV2 protocol version this pool implements.
const SV2_PROTOCOL_VERSION: u16 = 2;

/// Channel-id allocator (unique per process; channel scope is per-connection).
static CHANNEL_ID: AtomicU32 = AtomicU32::new(1);

// ─────────────────────────────────────────────────────────────────────────────
// Session state
// ─────────────────────────────────────────────────────────────────────────────

struct Sv2Session {
    peer: SocketAddr,
    /// Unique per-connection id, so two rigs authorising under the same worker
    /// name keep separate hashrate state in `PoolStats`.
    session_id: String,
    /// The user identity exactly as the device opened its channel with, for
    /// protocol echoes and logs. Everything recorded keys on `stats_worker()`.
    worker: Option<String>,
    identity: Option<Arc<MinerIdentity>>,

    setup_done: bool,
    channel_open: bool,
    channel_id: u32,

    /// Pool-assigned extranonce prefix (length chosen at channel open).
    extranonce_prefix: Vec<u8>,
    /// Bytes the device contributes (granted at channel open from its request).
    extranonce_size: usize,
    /// Total reserved extranonce width in the coinbase (prefix + device bytes).
    extranonce_total: usize,

    difficulty: u64,
    vardiff: Vardiff,
    vardiff_cfg: VardiffConfig,
    /// What one accepted share is worth to the hashrate estimator.
    credit: ShareCredit,

    share_set: ShareSet,
    guard: SessionGuard,

    /// SV2 job_id (u32) → session `StratumJob.job_id` (String) for stale lookups.
    job_ids: VecDeque<(u32, String)>,
    next_job_id: u32,
    issued_jobs: IssuedJobs,
    /// Most recent shared template (materialized once the channel opens).
    pending_template: Option<Arc<JobTemplate>>,
    coinbase_tag: String,
    network: bitcoin::Network,

    shares_accepted: u64,
    shares_rejected: u64,
    connect_time: Instant,
    stats: Arc<PoolStats>,
}

impl Sv2Session {
    /// The canonical worker identity every statistic, metric label, and ledger
    /// row is keyed on (mirrors the SV1 session). `None` until a channel opens.
    fn stats_worker(&self) -> Option<&str> {
        self.identity.as_deref().map(|i| i.canonical_name.as_str())
    }

    fn new(
        peer: SocketAddr,
        cfg: &Config,
        extranonce_prefix: Vec<u8>,
        stats: Arc<PoolStats>,
        network: bitcoin::Network,
    ) -> Self {
        let initial_diff = cfg.pool.initial_difficulty;
        Self {
            peer,
            session_id: format!("{:016x}", crate::network::session::random_u64()),
            worker: None,
            identity: None,
            setup_done: false,
            channel_open: false,
            channel_id: 0,
            extranonce_prefix,
            extranonce_size: cfg.pool.extranonce2_size,
            extranonce_total: cfg.pool.extranonce1_size + cfg.pool.extranonce2_size,
            difficulty: initial_diff,
            vardiff: Vardiff::new(cfg.vardiff.clone(), initial_diff, Instant::now()),
            vardiff_cfg: cfg.vardiff.clone(),
            credit: ShareCredit::new(cfg.vardiff.min_difficulty, Instant::now()),
            share_set: ShareSet::new(),
            guard: SessionGuard::new(&cfg.security),
            job_ids: VecDeque::new(),
            next_job_id: 1,
            issued_jobs: IssuedJobs::new(),
            pending_template: None,
            coinbase_tag: cfg.pool.coinbase_tag.clone(),
            network,
            shares_accepted: 0,
            shares_rejected: 0,
            connect_time: Instant::now(),
            stats,
        }
    }

    /// Difficulty of the block currently being worked on, when a template is
    /// held. Vardiff clamps to it so a miner is never asked for a share harder
    /// to find than a block.
    fn network_difficulty(&self) -> Option<u64> {
        let template = self.pending_template.as_ref()?;
        let difficulty = bits_to_difficulty(&template.bits).ok()?;
        (difficulty.is_finite() && difficulty >= 1.0).then_some(difficulty as u64)
    }

    /// Allocate an SV2 job_id and remember its engine job-id mapping.
    fn assign_job_id(&mut self, engine_job_id: String) -> u32 {
        let id = self.next_job_id;
        self.next_job_id = self.next_job_id.wrapping_add(1);
        if self.next_job_id == 0 {
            self.next_job_id = 1;
        }
        self.job_ids.push_back((id, engine_job_id));
        while self.job_ids.len() > 16 {
            self.job_ids.pop_front();
        }
        id
    }

    fn engine_job_id(&self, sv2_job_id: u32) -> Option<String> {
        self.job_ids
            .iter()
            .find(|(j, _)| *j == sv2_job_id)
            .map(|(_, s)| s.clone())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Session entry point
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
        warn!("Rejected banned IP: {peer}");
        return;
    }

    info!("SV2 miner connected: {peer}");

    // ── Noise handshake (pool = responder) ───────────────────────────────────
    // Bounded by the short handshake deadline so a peer cannot hold the
    // connection (and its bounded global slot) open mid-handshake — the
    // session-loop idle timeout only starts once we reach transport mode.
    let mut stream = stream;
    let handshake_timeout = Duration::from_secs(crate::network::server::HANDSHAKE_TIMEOUT_SECS);
    let state = match tokio::time::timeout(
        handshake_timeout,
        noise::responder_handshake(&mut stream),
    )
    .await
    {
        Err(_) => {
            info!("SV2 {peer} Noise handshake timed out");
            return;
        }
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            debug!("SV2 {peer} Noise handshake failed: {e}");
            return;
        }
    };

    // Only count the miner once the secure channel is established.
    metrics::miner_connected();
    stats.miner_connected();

    let extranonce_prefix =
        crate::network::session::generate_extranonce1(config.pool.extranonce1_size);
    let mut session = Sv2Session::new(peer, &config, extranonce_prefix, stats, network);
    let mut job_rx: broadcast::Receiver<JobBroadcast> = engine.subscribe();

    // Shared cipher state; reader (own task) decrypts, writer (this task) encrypts.
    let state = Arc::new(tokio::sync::Mutex::new(state));
    let (reader_half, writer_half) = stream.into_split();
    // Allow the configured message size plus SV2 framing + Noise AEAD overhead.
    let max_frame = config.security.max_message_bytes.saturating_add(1024);
    let mut nreader = noise::NoiseReader::new(reader_half, state.clone(), max_frame);
    let mut writer = NoiseWriter::new(writer_half, state, peer);

    // read_exact into the codec buffer is not cancel-safe, so frames are read in
    // a dedicated task and forwarded over a channel the main loop can select on.
    let (inbound_tx, mut inbound_rx) = tokio::sync::mpsc::channel::<(u8, Vec<u8>)>(32);
    let reader_task = tokio::spawn(async move {
        loop {
            match nreader.read().await {
                Ok(msg) => {
                    if inbound_tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    debug!("SV2 reader ended: {e}");
                    break;
                }
            }
        }
    });

    // Cache the current job so we can push it as soon as the channel opens.
    if let Some(template) = engine.current_template().await {
        session.pending_template = Some(template);
    }

    let idle_timeout = Duration::from_secs(config.pool.idle_timeout_secs);
    let preauth_timeout = Duration::from_secs(crate::network::server::HANDSHAKE_TIMEOUT_SECS);
    // Idle tracking must anchor on the last *inbound* message: every completed
    // select! iteration (job broadcasts arrive far more often than the idle
    // timeout) recreates the read-arm future, so a plain timeout() would reset
    // the idle clock and a dead peer that keeps receiving jobs never times out.
    let mut last_inbound = tokio::time::Instant::now();

    loop {
        // Until the channel opens (worker identified), hold the connection to
        // the short handshake deadline — mirrors the SV1 pre-auth timeout.
        let read_timeout = if session.channel_open {
            idle_timeout
        } else {
            preauth_timeout
        };
        tokio::select! {
            // ── Inbound (decrypted) SV2 message ─────────────────────────────
            inbound = tokio::time::timeout_at(last_inbound + read_timeout, inbound_rx.recv()) => {
                let (msg_type, mut payload) = match inbound {
                    Err(_) => { info!("SV2 miner {peer} idle timeout — disconnecting"); break; }
                    Ok(None) => { debug!("SV2 {peer} reader closed"); break; }
                    Ok(Some(m)) => { last_inbound = tokio::time::Instant::now(); m }
                };

                if let Err(e) = session.guard.check_message_size(payload.len()) {
                    warn!("{peer} {e}");
                    ban_list.ban(peer.ip(), "message too large");
                    break;
                }
                tracing::trace!(peer = %peer, msg_type, len = payload.len(), "← sv2 miner");

                match handle_message(&mut session, &mut writer, msg_type, &mut payload, &engine, &ban_list).await {
                    Flow::Continue => {}
                    Flow::Disconnect(reason) => {
                        if let Some(worker) = session.stats_worker() {
                            metrics::miner_disconnect(&reason, worker);
                        }
                        info!("Disconnecting SV2 {peer}: {reason}");
                        break;
                    }
                }

                if !apply_retarget(&mut session, &mut writer).await {
                    break;
                }
            }

            // ── New job broadcast from the template engine ──────────────────
            job_result = job_rx.recv() => {
                match job_result {
                    Ok(JobBroadcast { template, clean }) => {
                        if clean {
                            // A clean job retires every outstanding job; shares
                            // for them are now rejected as stale before dedup,
                            // so the dedup entries have nothing left to guard.
                            session.share_set.clear();
                        }
                        if session.channel_open {
                            // clean (new block): future-job + SetNewPrevHash.
                            // ntime refresh: immediate job on the existing prev-hash.
                            if !send_job(&mut session, &mut writer, template.clone(), clean, peer).await {
                                break;
                            }
                        }
                        session.pending_template = Some(template);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => warn!("SV2 {peer} missed {n} job broadcasts"),
                    Err(_) => break,
                }

                // Job pushes are the only thing that reaches a miner submitting
                // nothing, so they are also what lets vardiff ease the target of
                // one that has stopped.
                if !apply_retarget(&mut session, &mut writer).await {
                    break;
                }
            }
        }
    }

    reader_task.abort();
    metrics::miner_disconnected();
    session.stats.miner_disconnected();
    let uptime = session.connect_time.elapsed().as_secs() as f64;
    if let Some(worker) = session.stats_worker() {
        session.stats.mark_worker_offline(worker);
        metrics::connection_duration(worker, uptime);
    }
    info!(
        peer = %peer,
        worker = ?session.worker,
        accepted = session.shares_accepted,
        rejected = session.shares_rejected,
        uptime_secs = uptime,
        honors_set_difficulty = session.credit.honors_assigned(),
        "SV2 miner session ended"
    );
}

/// Run a vardiff check and push `SetTarget` if the target moved. Returns false
/// when the write failed and the session should end.
async fn apply_retarget(session: &mut Sv2Session, writer: &mut NoiseWriter) -> bool {
    if !session.channel_open {
        return true;
    }
    let network_difficulty = session.network_difficulty();
    let Some(new_diff) = session
        .vardiff
        .check_retarget(Instant::now(), network_difficulty)
    else {
        return true;
    };

    let old_diff = session.difficulty;
    session.difficulty = new_diff;
    if let Some(worker) = session.stats_worker() {
        metrics::vardiff_retarget(worker, old_diff, new_diff);
        session.stats.update_worker_vardiff(worker, new_diff);
    }
    info!(
        peer = %session.peer,
        worker = ?session.worker,
        difficulty = new_diff,
        "Sending SV2 set_target"
    );
    let target = job::difficulty_to_sv2_target(new_diff);
    match messages::set_target(session.channel_id, target) {
        Ok(p) => writer.send(MESSAGE_TYPE_SET_TARGET, true, &p).await,
        Err(e) => {
            error!("encode set_target: {e}");
            false
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Message dispatch
// ─────────────────────────────────────────────────────────────────────────────

enum Flow {
    Continue,
    Disconnect(String),
}

async fn handle_message(
    session: &mut Sv2Session,
    writer: &mut NoiseWriter,
    msg_type: u8,
    payload: &mut [u8],
    engine: &Arc<TemplateEngine>,
    ban_list: &Arc<BanList>,
) -> Flow {
    // One token per inbound frame, not just submits: setup/open-channel floods
    // are as cheap to send as shares and feed the same per-message stats work,
    // so they share the same bucket (mirrors the SV1 dispatch).
    if !session.guard.share_rate.try_consume() {
        accounting::record_rate_limited(&session.stats, session.stats_worker());
        ban_list.ban(session.peer.ip(), "message rate exceeded");
        return Flow::Disconnect("rate limited".into());
    }

    match msg_type {
        MESSAGE_TYPE_SETUP_CONNECTION => handle_setup_connection(session, writer, payload).await,
        MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL => {
            handle_open_extended(session, writer, payload).await
        }
        MESSAGE_TYPE_SUBMIT_SHARES_EXTENDED => {
            handle_submit(session, writer, payload, engine).await
        }
        other => {
            // UpdateChannel / CloseChannel / etc. — not required for plaintext
            // extended-channel mining; log and continue.
            debug!(peer = %session.peer, msg_type = other, "Ignoring unhandled SV2 message");
            Flow::Continue
        }
    }
}

async fn handle_setup_connection(
    session: &mut Sv2Session,
    writer: &mut NoiseWriter,
    payload: &mut [u8],
) -> Flow {
    let setup = match messages::decode_setup_connection(payload) {
        Ok(s) => s,
        // Malformed payload: nothing sane to reply to, just drop.
        Err(e) => return Flow::Disconnect(format!("bad SetupConnection: {e}")),
    };
    if !matches!(setup.protocol, Protocol::MiningProtocol) {
        return setup_error(
            writer,
            "unsupported-protocol",
            0,
            format!("unsupported sub-protocol: {:?}", setup.protocol),
        )
        .await;
    }
    if setup.min_version > SV2_PROTOCOL_VERSION {
        return setup_error(
            writer,
            "protocol-version-mismatch",
            0,
            format!(
                "unsupported SV2 version range {}..{}",
                setup.min_version, setup.max_version
            ),
        )
        .await;
    }
    // Extended channels only, no job declaration: a device that *requires*
    // standard jobs or work selection cannot mine here (today it would stall
    // at channel open, since OpenStandardMiningChannel is ignored).
    if setup.flags & messages::UNSUPPORTED_SETUP_FLAGS != 0 {
        return setup_error(
            writer,
            "unsupported-feature-flags",
            messages::UNSUPPORTED_SETUP_FLAGS,
            format!("unsupported feature flags {:#x}", setup.flags),
        )
        .await;
    }
    let used_version = SV2_PROTOCOL_VERSION.min(setup.max_version);
    session.setup_done = true;
    info!(peer = %session.peer, used_version, "SV2 SetupConnection");

    match messages::setup_connection_success(used_version) {
        Ok(p) => {
            if !writer
                .send(MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS, false, &p)
                .await
            {
                return Flow::Disconnect("write".into());
            }
            Flow::Continue
        }
        Err(e) => Flow::Disconnect(format!("encode SetupConnectionSuccess: {e}")),
    }
}

/// Send `SetupConnection.Error` with the spec error code, then disconnect.
/// `flags` is the full unsupported feature-flag set for
/// `unsupported-feature-flags` and 0 for the other codes.
/// Best-effort: an encode or write failure still disconnects with `reason`,
/// so the client can at most miss the courtesy reply it gets today anyway.
async fn setup_error(writer: &mut NoiseWriter, code: &str, flags: u32, reason: String) -> Flow {
    match messages::setup_connection_error(code, flags) {
        Ok(p) => {
            writer
                .send(MESSAGE_TYPE_SETUP_CONNECTION_ERROR, false, &p)
                .await;
        }
        Err(e) => warn!(code, "encode SetupConnectionError: {e}"),
    }
    Flow::Disconnect(reason)
}

async fn handle_open_extended(
    session: &mut Sv2Session,
    writer: &mut NoiseWriter,
    payload: &mut [u8],
) -> Flow {
    if !session.setup_done {
        return Flow::Disconnect("OpenExtendedMiningChannel before SetupConnection".into());
    }
    let open = match messages::decode_open_extended(payload) {
        Ok(o) => o,
        Err(e) => return Flow::Disconnect(format!("bad OpenExtendedMiningChannel: {e}")),
    };

    if let Err(e) = session.guard.check_worker_name(&open.user_identity) {
        debug!(peer = %session.peer, "Rejected SV2 user_identity: {e}");
        return open_error(writer, open.request_id, "invalid-user-identity").await;
    }
    let identity = match MinerIdentity::parse(
        &open.user_identity,
        session.network,
        session.guard.max_worker_name_len,
    ) {
        Ok(identity) => Arc::new(identity),
        Err(e) => {
            debug!(peer = %session.peer, worker = %open.user_identity, "Rejected SV2 payout identity: {e}");
            return open_error(writer, open.request_id, "invalid-user-identity").await;
        }
    };
    if session.channel_open {
        return open_error(writer, open.request_id, "multiple-channels-unsupported").await;
    }

    // Only a *new* identity counts against the cap or touches the stats maps
    // (mirrors the SV1 authorize path). Compared canonically, so a case
    // variant of the same address cannot burn the authorization budget.
    let is_new_identity = session
        .identity
        .as_deref()
        .map(|i| i.canonical_name.as_str())
        != Some(identity.canonical_name.as_str());
    if is_new_identity {
        if !session.guard.record_new_authorization() {
            return Flow::Disconnect("too many worker identities".into());
        }
        session.worker.take();
        if let Some(prev) = session.identity.take() {
            session.stats.mark_worker_offline(&prev.canonical_name);
        }
    }

    // Grant the device its requested extranonce out of the coinbase's reserved
    // total; the remaining bytes become the pool prefix. This is independent of
    // the SV1 split, so SV1 miners (e.g. the Avalon Nano) keep their smaller
    // extranonce2 while an SV2 device gets the larger size it needs.
    let requested = open.min_extranonce_size as usize;
    if requested > session.extranonce_total {
        warn!(
            peer = %session.peer,
            requested,
            total = session.extranonce_total,
            "SV2 min_extranonce_size exceeds total reserved extranonce — rejecting; \
             raise [pool] extranonce sizes so extranonce1_size + extranonce2_size >= {requested}"
        );
        match messages::open_channel_error_extranonce(open.request_id) {
            Ok(p) => {
                let _ = writer
                    .send(MESSAGE_TYPE_OPEN_MINING_CHANNEL_ERROR, false, &p)
                    .await;
            }
            Err(e) => error!("encode OpenMiningChannelError: {e}"),
        }
        return Flow::Continue;
    }

    // Grant exactly what the device asked (min 1), leaving the rest as prefix.
    let granted = requested.max(1);
    let prefix_len = session.extranonce_total - granted;
    session.extranonce_size = granted;
    session.extranonce_prefix = crate::network::session::generate_extranonce1(prefix_len);
    info!(
        peer = %session.peer,
        granted,
        prefix_len,
        "SV2 extranonce split"
    );

    let channel_id = CHANNEL_ID.fetch_add(1, Ordering::Relaxed);
    session.channel_id = channel_id;
    session.worker = Some(open.user_identity.clone());
    let canonical = identity.canonical_name.clone();
    session.identity = Some(identity);

    // Target from current difficulty, clamped to be no easier than the device's
    // declared `max_target` (SV2: assigned target MUST be ≤ max_target).
    let mut target = job::difficulty_to_sv2_target(session.difficulty);
    if !job::sv2_target_le(&target, &open.max_target) {
        info!(
            peer = %session.peer,
            "SV2 device max_target easier than initial difficulty target; clamping to max_target"
        );
        target = open.max_target;
    }

    if is_new_identity {
        session
            .stats
            .mark_worker_online(&canonical, session.difficulty);
        session.stats.set_worker_protocol(&canonical, "sv2");
    }
    info!(peer = %session.peer, worker = %open.user_identity, channel_id, "SV2 extended channel opened");

    match messages::open_extended_success(
        open.request_id,
        channel_id,
        target,
        session.extranonce_size as u16,
        session.extranonce_prefix.clone(),
    ) {
        Ok(p) => {
            if !writer
                .send(MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL_SUCCES, false, &p)
                .await
            {
                return Flow::Disconnect("write".into());
            }
        }
        Err(e) => return Flow::Disconnect(format!("encode OpenExtendedMiningChannelSuccess: {e}")),
    }

    session.channel_open = true;

    // Push the current job immediately (future-job + SetNewPrevHash).
    if let Some(template) = session.pending_template.clone() {
        if !send_job(session, writer, template, true, session.peer).await {
            return Flow::Disconnect("write".into());
        }
    } else {
        warn!(peer = %session.peer, "SV2 channel opened but no job available yet");
    }

    Flow::Continue
}

async fn open_error(writer: &mut NoiseWriter, request_id: u32, code: &str) -> Flow {
    match messages::open_channel_error(request_id, code) {
        Ok(payload) => {
            if !writer
                .send(MESSAGE_TYPE_OPEN_MINING_CHANNEL_ERROR, false, &payload)
                .await
            {
                return Flow::Disconnect("write".into());
            }
        }
        Err(e) => return Flow::Disconnect(format!("encode OpenMiningChannelError: {e}")),
    }
    Flow::Continue
}

async fn handle_submit(
    session: &mut Sv2Session,
    writer: &mut NoiseWriter,
    payload: &mut [u8],
    engine: &Arc<TemplateEngine>,
) -> Flow {
    if !session.channel_open {
        return Flow::Disconnect("SubmitShares before channel open".into());
    }
    let submit = match messages::decode_submit_extended(payload) {
        Ok(s) => s,
        Err(e) => return Flow::Disconnect(format!("bad SubmitSharesExtended: {e}")),
    };
    let worker = session.stats_worker().unwrap_or("?").to_string();

    // The device must send exactly the granted extranonce size; the coinbase
    // splice depends on it. Guard so a malformed length is a clean reject, not a
    // panic in the validation task.
    if submit.extranonce.len() != session.extranonce_size {
        debug!(
            worker = %worker,
            got = submit.extranonce.len(),
            expected = session.extranonce_size,
            "SV2 submit has wrong extranonce length — rejecting"
        );
        if reject_share(session, RejectReason::BadExtranonce) {
            return Flow::Disconnect("too many invalid shares".into());
        }
        return reject(
            session,
            writer,
            submit.sequence_number,
            "bad-extranonce-size",
        )
        .await;
    }

    let submit_start = Instant::now();

    // Map the protocol job ID to the exact payout-specific job issued here.
    let job_entry = match session.engine_job_id(submit.job_id) {
        Some(engine_id) => session.issued_jobs.find(&engine_id),
        None => None,
    };
    let job_entry = match job_entry {
        Some(e) => e,
        None => {
            if reject_share(session, RejectReason::JobNotFound) {
                return Flow::Disconnect("too many invalid shares".into());
            }
            return reject(session, writer, submit.sequence_number, "stale-job").await;
        }
    };

    let mask = VERSION_ROLLING_MASK;
    let share_params = ShareParams {
        job_id: job_entry.job.job_id.clone(),
        extranonce2: submit.extranonce.clone(),
        ntime: submit.ntime,
        nonce: submit.nonce,
        // The device sends the full version; pass only the masked (rolled) bits.
        version_bits: Some(submit.version & mask),
        version_rolling_mask: Some(mask),
    };

    // Duplicate detection (same key as SV1). Only *checked* here; the key is
    // inserted after validation passes, so invalid submissions cannot occupy
    // dedup slots.
    let share_key = validator::ShareKey::new(
        &share_params.job_id,
        &submit.extranonce,
        submit.ntime,
        submit.nonce,
        submit.version & mask,
    );
    if session.share_set.contains(&share_key) {
        if reject_share(session, RejectReason::Duplicate) {
            return Flow::Disconnect("too many invalid shares".into());
        }
        return reject(session, writer, submit.sequence_number, "duplicate-share").await;
    }

    // Accept any share meeting the configured floor — matches SV1 policy so a
    // fixed hardware submission threshold isn't penalised by vardiff raises.
    let accept_difficulty = session.vardiff_cfg.min_difficulty;
    let job_payout = job_entry.job.payout_address.clone();

    let validation_start = Instant::now();
    let extranonce1 = session.extranonce_prefix.clone();
    let job_entry_cloned = job_entry.clone();
    let validation = task::spawn_blocking(move || {
        validator::validate_share_no_dedup(
            &share_params,
            &job_entry_cloned.job,
            &job_entry_cloned,
            &extranonce1,
            accept_difficulty,
        )
    })
    .await;

    let validation_result = match validation {
        Ok(result) => result,
        Err(e) => {
            error!("SV2 share validation task failed: {e}");
            return Flow::Disconnect("internal error".into());
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
            metrics::share_validation_time(validation_start.elapsed().as_millis() as f64);
            let credit = accept_share(session, &worker, hash_difficulty, job_entry.difficulty);
            debug!(
                worker = %worker, hash = %hex::encode(hash), diff = assigned_difficulty,
                hash_diff = hash_difficulty, credit = credit,
                latency_ms = submit_start.elapsed().as_millis(),
                "SV2 share accepted"
            );
            accept(session, writer, submit.sequence_number).await
        }

        Ok(ShareResult::Block {
            hash_difficulty,
            block_hex,
            hash,
        }) => {
            metrics::share_validation_time(validation_start.elapsed().as_millis() as f64);
            let block_hash_hex = validator::block_hash_display(&hash);
            let submit_result = engine
                .submit_found_block(
                    job_entry.job.height,
                    &block_hash_hex,
                    block_hex,
                    &worker,
                    &job_payout,
                    session.stats.clone(),
                )
                .await;
            // Credited before the outcome is recorded, whatever the node said:
            // the miner produced a valid block-difficulty share, losing a
            // same-height race is not its fault, and a submit error does not
            // unmake the work. Ordering matters — a win resets the round, and
            // the share that ended a round belongs in it, not seeded into the
            // next one as an unbeatable first best.
            accept_share(session, &worker, hash_difficulty, job_entry.difficulty);
            match submit_result {
                Ok(outcome) => {
                    accounting::record_block_outcome(
                        &session.stats,
                        outcome,
                        job_entry.job.height,
                        &worker,
                        &job_payout,
                        &block_hash_hex,
                    );
                    if outcome.is_win() {
                        info!("🏆 Block submitted (SV2)! worker={worker} hash={block_hash_hex}");
                    } else {
                        warn!(
                            "SV2 block from worker={worker} hash={block_hash_hex} was valid but \
                             lost its height race; it is stored on a side branch and earned \
                             nothing"
                        );
                    }
                }
                Err(e) => {
                    metrics::block_submission_failure(e.submit_failure_label());
                    error!("SV2 submitblock failed: {e}");
                }
            }
            accept(session, writer, submit.sequence_number).await
        }

        Err(e) => {
            metrics::share_validation_time(validation_start.elapsed().as_millis() as f64);
            let reason = RejectReason::from_error(&e);
            warn!(worker = %worker, reason = reason.label(), nonce = %format!("{:08x}", submit.nonce), "SV2 share rejected: {e}");
            if reject_share(session, reason) {
                return Flow::Disconnect("too many invalid shares".into());
            }
            reject(session, writer, submit.sequence_number, reason.label()).await
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Share bookkeeping (mirrors the SV1 session; both go through `accounting`)
// ─────────────────────────────────────────────────────────────────────────────

/// Book an accepted share. Returns the difficulty credited to the estimator.
fn accept_share(
    session: &mut Sv2Session,
    worker: &str,
    hash_difficulty: u64,
    job_difficulty: u64,
) -> u64 {
    session.shares_accepted += 1;
    let now = Instant::now();
    let credit = session
        .credit
        .credit(session.difficulty, job_difficulty, hash_difficulty, now);
    session.vardiff.record_share(credit, now);
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
fn reject_share(session: &mut Sv2Session, reason: RejectReason) -> bool {
    session.shares_rejected += 1;
    let worker = session.stats_worker().unwrap_or("?").to_string();
    accounting::record_rejected(&session.stats, &mut session.guard, &worker, reason)
}

// ─────────────────────────────────────────────────────────────────────────────
// Outbound helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Send a job to the device. When `future` (new block / initial), announce a
/// future `NewExtendedMiningJob` then a `SetNewPrevHash` that activates it;
/// otherwise send an immediate job on the current prev-hash (ntime refresh).
async fn send_job(
    session: &mut Sv2Session,
    writer: &mut NoiseWriter,
    template: Arc<JobTemplate>,
    future: bool,
    peer: SocketAddr,
) -> bool {
    let Some(identity) = session.identity.as_ref() else {
        error!(peer = %peer, "Cannot build SV2 job without payout identity");
        return false;
    };
    let job = match build_job_for_payout(
        template,
        &identity.payout,
        &session.coinbase_tag,
        session.extranonce_prefix.len(),
        session.extranonce_size,
    ) {
        Ok(job) => Arc::new(job),
        Err(e) => {
            error!(peer = %peer, "Failed to build SV2 payout job: {e}");
            return false;
        }
    };
    let sv2_job_id = session.assign_job_id(job.job_id.clone());

    let new_job = match job::build_new_extended_job(&job, session.channel_id, sv2_job_id, future) {
        Ok(j) => j,
        Err(e) => {
            error!("build NewExtendedMiningJob: {e}");
            return false;
        }
    };
    let payload = match messages::encode(new_job) {
        Ok(p) => p,
        Err(e) => {
            error!("encode NewExtendedMiningJob: {e}");
            return false;
        }
    };
    debug!(peer = %peer, sv2_job_id, future, clean = future, "Sending SV2 NewExtendedMiningJob");
    if !writer
        .send(MESSAGE_TYPE_NEW_EXTENDED_MINING_JOB, true, &payload)
        .await
    {
        return false;
    }

    if future {
        let snph = match job::build_set_new_prev_hash(&job, session.channel_id, sv2_job_id) {
            Ok(s) => s,
            Err(e) => {
                error!("build SetNewPrevHash: {e}");
                return false;
            }
        };
        let payload = match messages::encode(snph) {
            Ok(p) => p,
            Err(e) => {
                error!("encode SetNewPrevHash: {e}");
                return false;
            }
        };
        if !writer
            .send(MESSAGE_TYPE_MINING_SET_NEW_PREV_HASH, true, &payload)
            .await
        {
            return false;
        }
    }
    session.issued_jobs.issue(job, future, session.difficulty);
    true
}

async fn accept(session: &Sv2Session, writer: &mut NoiseWriter, seq: u32) -> Flow {
    match messages::submit_shares_success(session.channel_id, seq, session.difficulty) {
        Ok(p) => {
            if !writer
                .send(MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS, true, &p)
                .await
            {
                return Flow::Disconnect("write".into());
            }
            Flow::Continue
        }
        Err(e) => Flow::Disconnect(format!("encode SubmitSharesSuccess: {e}")),
    }
}

async fn reject(session: &Sv2Session, writer: &mut NoiseWriter, seq: u32, code: &str) -> Flow {
    match messages::submit_shares_error(session.channel_id, seq, code) {
        Ok(p) => {
            if !writer
                .send(MESSAGE_TYPE_SUBMIT_SHARES_ERROR, true, &p)
                .await
            {
                return Flow::Disconnect("write".into());
            }
            Flow::Continue
        }
        Err(e) => Flow::Disconnect(format!("encode SubmitSharesError: {e}")),
    }
}
