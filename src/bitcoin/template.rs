/// bitcoin/template.rs
///
/// Converts a raw GBT result into a Stratum job, including:
///  - Coinbase transaction construction (BIP34 height, extranonce, tag, reward output)
///  - SegWit witness commitment output
///  - Merkle branch computation for mining.notify
///  - prev_hash byte-reversal (Core → Stratum format)
///  - Job ID management
use super::rpc::GbtResult;
use crate::error::PoolError;
use crate::mining::identity::PayoutDescriptor;
use bitcoin::{
    blockdata::transaction::{OutPoint, Transaction, TxIn, TxOut},
    consensus::encode::serialize,
    Amount, ScriptBuf, Sequence, Witness,
};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

// ─────────────────────────────────────────────────────────────────────────────
// Job ID counter
// ─────────────────────────────────────────────────────────────────────────────

/// Per-job sequence number — the low 32 bits of every job id.
static JOB_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Per-process epoch occupying the high 32 bits of every job id.
///
/// The sequence counter restarts at 1 on each process start, so without this a
/// redeploy re-mints the same ids the previous process used. In-flight shares
/// from before the restart would then resolve against a *different* job that
/// happens to share the id and get booked as `stale`. Seeding the high bits from
/// the wall clock makes post-restart ids disjoint from the previous process's,
/// so such stragglers honestly surface as `job_not_found` instead.
fn job_epoch() -> u64 {
    static EPOCH: OnceLock<u64> = OnceLock::new();
    *EPOCH.get_or_init(|| {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // Low 32 bits of the unix-millis clock, parked in the high half of the id.
        // Distinct per millisecond — two restarts can't collide in practice.
        (millis & 0xFFFF_FFFF) << 32
    })
}

pub fn next_job_id() -> String {
    let seq = JOB_COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFFF_FFFF;
    format!("{:016x}", job_epoch() | seq)
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared template and payout-specific Stratum job
// ─────────────────────────────────────────────────────────────────────────────

/// Address-independent data derived from one getblocktemplate response.
///
/// Transaction data is shared by every payout-specific job issued from this
/// template, avoiding a full block-sized allocation per connected identity.
#[derive(Debug)]
pub struct JobTemplate {
    pub prev_hash: String,
    pub merkle_branch: Vec<String>,
    pub merkle_branch_raw: Vec<[u8; 32]>,
    pub version: u32,
    pub bits: String,
    pub cur_time: u32,
    pub height: u64,
    pub network_target: [u8; 32],
    pub transactions: Arc<Vec<Vec<u8>>>,
    pub coinbase_value: u64,
    pub witness_commitment: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct StratumJob {
    /// Unique job identifier (hex string, sent in mining.notify)
    pub job_id: String,

    /// Previous block hash in Stratum byte order (8 groups of 4 bytes, each reversed)
    pub prev_hash: String,

    /// Serialized coinbase part 1: everything before the extranonce placeholder
    pub coinbase1: Vec<u8>,

    /// Serialized coinbase part 2: everything after the extranonce placeholder
    pub coinbase2: Vec<u8>,

    /// Merkle branch hashes (hex) for mining.notify
    pub merkle_branch: Vec<String>,

    /// Merkle branch hashes as raw bytes for fast merkle root computation
    pub merkle_branch_raw: Vec<[u8; 32]>,

    /// Block version (may include version-rolling mask bits)
    pub version: u32,

    /// Compact target from GBT (nbits)
    pub bits: String,

    /// Template time advertised to miners in mining.notify
    pub cur_time: u32,

    /// Block height (for BIP34 and logging)
    pub height: u64,

    /// Network target as 32 bytes (derived from bits)
    pub network_target: [u8; 32],

    /// Full serialized coinbase (for block assembly after share submit)
    /// extranonce1 + extranonce2 slots are zero until filled by `assemble_coinbase_into`
    pub coinbase_template: Vec<u8>,

    /// Byte offsets of the extranonce field inside `coinbase_template`
    pub extranonce_offset: usize,
    pub extranonce1_len: usize,
    pub extranonce2_len: usize,

    /// All transaction data (for block assembly)
    pub transactions: Arc<Vec<Vec<u8>>>,

    /// Network-checked destination encoded in this job's coinbase.
    pub payout_address: String,

    /// Whether this job's coinbase carries the BIP141 witness commitment
    /// output. When it does, the assembled *block* must also carry the witness
    /// reserved value in the coinbase input (see `assemble_block_hex`).
    pub has_witness_commitment: bool,
}

impl StratumJob {
    /// Assemble the full coinbase by splicing the pool prefix + miner extranonce
    /// into the reserved region.
    ///
    /// The two parts are placed back-to-back and must exactly fill the reserved
    /// extranonce region (`extranonce1_len + extranonce2_len`). The split between
    /// them is chosen per session, not fixed: SV1 uses `extranonce1_len` /
    /// `extranonce2_len`, while an SV2 extended channel may use a different
    /// prefix length (`total - granted`) as long as the two still sum to the
    /// reserved width. A mismatched total leaves the region zeroed so the share
    /// simply fails validation instead of panicking.
    ///
    /// `cb` is overwritten, not appended to: it is a caller-owned scratch buffer
    /// so the share hot path can splice a coinbase per submit without allocating
    /// (see `COINBASE_SCRATCH` in `mining::validator`).
    pub fn assemble_coinbase_into(&self, extranonce1: &[u8], extranonce2: &[u8], cb: &mut Vec<u8>) {
        cb.clear();
        cb.extend_from_slice(&self.coinbase_template);
        let off = self.extranonce_offset;
        let total = self.extranonce1_len + self.extranonce2_len;
        let (p, m) = (extranonce1.len(), extranonce2.len());
        if p + m != total {
            return;
        }
        cb[off..off + p].copy_from_slice(extranonce1);
        cb[off + p..off + p + m].copy_from_slice(extranonce2);
    }

    /// Allocating form of [`Self::assemble_coinbase_into`], for tests only —
    /// production callers pass a reused buffer.
    #[cfg(test)]
    pub fn assemble_coinbase(&self, extranonce1: &[u8], extranonce2: &[u8]) -> Vec<u8> {
        let mut cb = Vec::new();
        self.assemble_coinbase_into(extranonce1, extranonce2, &mut cb);
        cb
    }

    /// Compute the merkle root given a fully assembled coinbase.
    pub fn merkle_root(&self, coinbase: &[u8]) -> [u8; 32] {
        let cb_hash = double_sha256(coinbase);
        let mut hash = cb_hash;
        for branch in &self.merkle_branch_raw {
            let mut combined = [0u8; 64];
            combined[..32].copy_from_slice(&hash);
            combined[32..].copy_from_slice(branch);
            hash = double_sha256(&combined);
        }
        hash
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Template rules and consensus tripwires
// ─────────────────────────────────────────────────────────────────────────────

/// The `getblocktemplate` rules this pool knows how to build a block for.
///
/// GBT returns one entry per soft fork the node is enforcing. A `!` prefix
/// (Core's `gbt_force`) carries BIP22/23's requirement that a client which
/// *mutates* the template must understand the rule — and this pool mutates it
/// about as far as a client can: it discards Core's coinbase and builds its own,
/// so every rule that constrains coinbase shape is the pool's to satisfy.
///
/// Core does not error on a `!` rule the client failed to declare; it lists the
/// rule and leaves the decision to the client. Declining to make that decision
/// is how a pool ends up mining invalid blocks for a whole activation.
///
/// Observed on Bitcoin Core 31.1, 2026-08-07 — captured in
/// `tests/fixtures/gbt-mainnet.json` and `tests/fixtures/gbt-regtest.json`:
///
/// ```text
/// mainnet, height 961441 → ["csv", "!segwit", "taproot"]
/// regtest, height 1      → ["csv", "!segwit", "taproot"]
/// ```
///
/// Of these only segwit changes block *construction*, via the witness
/// commitment output, and that is implemented. `csv` and `taproot` constrain
/// transactions the node selected for us. Anything else is a rule this binary
/// predates, so it cannot know whether its coinbase still satisfies consensus.
pub const SUPPORTED_GBT_RULES: &[&str] = &["csv", "segwit", "taproot"];

/// What in this template the pool cannot honour: `!`-prefixed rules it does not
/// implement, plus any `vbrequired` bit that BIP320 version rolling would let a
/// miner clear.
///
/// Empty is the healthy case. Rules without the `!` prefix are advisory and are
/// ignored: the node is telling us it enforces them, not that we must act.
///
/// `vbrequired` is folded in here rather than narrowing the rolling mask because
/// the pool cannot repair it downstream. The version is part of the header the
/// miner hashed, so forcing a required bit back on at validation time would
/// invalidate their proof of work; the only real fix is to never advertise the
/// bit as rollable, and `mining.configure` is negotiated once per session,
/// before any template is in hand. Reporting it and stopping is the honest
/// behaviour, and it costs nothing while Core hardcodes the field to 0.
pub fn unsupported_gbt_rules(gbt: &GbtResult) -> Vec<String> {
    let mut unsupported: Vec<String> = gbt
        .rules
        .iter()
        .filter_map(|rule| rule.strip_prefix('!'))
        .filter(|name| {
            !SUPPORTED_GBT_RULES
                .iter()
                .any(|known| known.eq_ignore_ascii_case(name))
        })
        .map(str::to_owned)
        .collect();

    let rollable_required = gbt.vbrequired & crate::mining::validator::VERSION_ROLLING_MASK;
    if rollable_required != 0 {
        unsupported.push(format!("vbrequired:{rollable_required:#010x}"));
    }

    unsupported
}

/// Whether the template announces segwit, with or without the `!` prefix.
fn announces_segwit(rules: &[String]) -> bool {
    rules.iter().any(|rule| {
        rule.strip_prefix('!')
            .unwrap_or(rule)
            .eq_ignore_ascii_case("segwit")
    })
}

/// Consensus block weight limit (BIP141).
pub const MAX_BLOCK_WEIGHT: u64 = 4_000_000;

/// Weight Core's block assembler holds back for the coinbase it expected to
/// build. Our coinbase is far smaller (a ~200-byte non-witness transaction, so
/// ~800 WU), which is why the pool can replace it without recomputing the
/// packing — this checks that the assumption still holds.
const COINBASE_WEIGHT_RESERVE: u64 = 4_000;

/// The block subsidy at `height`, in satoshis: 50 BTC halving every 210 000
/// blocks, reaching zero after 64 halvings.
pub fn block_subsidy(height: u64) -> u64 {
    const HALVING_INTERVAL: u64 = 210_000;
    const INITIAL_SUBSIDY: u64 = 50 * 100_000_000;

    let halvings = height / HALVING_INTERVAL;
    if halvings >= 64 {
        return 0;
    }
    INITIAL_SUBSIDY >> halvings
}

/// Tripwire: the node's transaction selection must leave room for our coinbase.
///
/// Core assembles against `MAX_BLOCK_WEIGHT` minus its own coinbase reserve, so
/// this can only fire if that contract changes or the template is corrupt.
/// It is a hard error because the alternative is knowingly mining a block over
/// the weight limit, which is rejected for certain.
fn check_block_weight(gbt: &GbtResult) -> Result<(), PoolError> {
    // The 80-byte header and the transaction-count varint are base bytes, so
    // they weigh 4× each. Three bytes covers any count Core can return.
    const BLOCK_OVERHEAD_WEIGHT: u64 = (80 + 3) * 4;

    let tx_weight: u64 = gbt.transactions.iter().map(|tx| tx.weight).sum();
    let total = BLOCK_OVERHEAD_WEIGHT + tx_weight + COINBASE_WEIGHT_RESERVE;
    if total > MAX_BLOCK_WEIGHT {
        return Err(PoolError::Other(anyhow::anyhow!(
            "template transactions weigh {tx_weight} WU; with block overhead and \
             the {COINBASE_WEIGHT_RESERVE} WU coinbase reserve that is {total} WU, \
             over the {MAX_BLOCK_WEIGHT} WU consensus limit"
        )));
    }
    Ok(())
}

/// Tripwire: `coinbasevalue` must equal the subsidy plus every fee in the
/// template, which is what Core computed it from.
///
/// The pool pays `coinbasevalue` out in full and mines every transaction the
/// template listed. If those two ever drift apart — a truncated transaction
/// list, a changed fee semantic — the block is `bad-cb-amount`.
///
/// Loud rather than fatal, deliberately. `coinbasevalue` comes from the node and
/// is authoritative; if our sum disagrees, our sum is the more likely suspect,
/// and refusing to mine over it would turn a reporting bug into an outage.
fn check_coinbase_value(gbt: &GbtResult) {
    let fees: u64 = gbt.transactions.iter().map(|tx| tx.fee).sum();
    let expected = block_subsidy(gbt.height).saturating_add(fees);
    if expected != gbt.coinbase_value {
        tracing::error!(
            height = gbt.height,
            coinbase_value = gbt.coinbase_value,
            expected,
            subsidy = block_subsidy(gbt.height),
            fees,
            transactions = gbt.transactions.len(),
            "coinbasevalue does not match subsidy + template fees; a block built \
             from this template may be rejected as bad-cb-amount"
        );
        crate::metrics::coinbase_value_mismatch();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Job builder
// ─────────────────────────────────────────────────────────────────────────────

/// Build the address-independent portion of a mining job once per GBT refresh.
pub fn build_job_template(gbt: &GbtResult) -> Result<JobTemplate, PoolError> {
    // Each txid must be exactly 32 bytes: it is a merkle leaf, and a truncated
    // or garbled one would produce a merkle root that is wrong but internally
    // consistent — the exact class of error share validation cannot see,
    // because it recomputes the same root. `decode_to_slice` enforces the
    // width, where the previous `hex::decode(..).unwrap_or_default()` followed
    // by `copy_from_slice` panicked instead, taking the refresh task with it and
    // freezing the template.
    let tx_txids = gbt
        .transactions
        .iter()
        .map(|tx| {
            let mut txid = [0u8; 32];
            hex::decode_to_slice(&tx.txid, &mut txid).map_err(|e| {
                PoolError::Other(anyhow::anyhow!("template txid {:?}: {e}", tx.txid))
            })?;
            txid.reverse();
            Ok(txid)
        })
        .collect::<Result<Vec<[u8; 32]>, PoolError>>()?;

    let merkle_branch_raw = compute_merkle_branch_raw(&tx_txids);
    let merkle_branch = merkle_branch_raw.iter().map(hex::encode).collect();
    let witness_commitment = gbt
        .default_witness_commitment
        .as_deref()
        .map(hex::decode)
        .transpose()
        .map_err(|e| PoolError::Other(anyhow::anyhow!("witness commitment hex: {e}")))?;

    // A segwit template without a commitment would mine `bad-witness-merkle-match`
    // blocks. Core has always sent one, so this is a tripwire on an assumption
    // rather than a known case — but it is the assumption that decides whether
    // the coinbase gets its OP_RETURN output at all.
    if announces_segwit(&gbt.rules) && witness_commitment.is_none() {
        return Err(PoolError::Other(anyhow::anyhow!(
            "template announces the segwit rule but carries no \
             default_witness_commitment; a block built from it would be rejected \
             as bad-witness-merkle-match"
        )));
    }

    check_block_weight(gbt)?;
    check_coinbase_value(gbt);

    Ok(JobTemplate {
        prev_hash: stratum_prev_hash(&gbt.prev_hash)?,
        merkle_branch,
        merkle_branch_raw,
        version: gbt.version,
        bits: gbt.bits.clone(),
        cur_time: gbt.cur_time,
        height: gbt.height,
        network_target: bits_to_target(&gbt.bits)?,
        transactions: Arc::new(gbt.transactions.iter().map(|t| t.data.clone()).collect()),
        coinbase_value: gbt.coinbase_value,
        witness_commitment,
    })
}

/// Materialize the small payout-specific portion of a shared job template.
pub fn build_job_for_payout(
    template: Arc<JobTemplate>,
    payout: &PayoutDescriptor,
    coinbase_tag: &str,
    extranonce1_len: usize,
    extranonce2_len: usize,
) -> Result<StratumJob, PoolError> {
    let job_id = next_job_id();
    let (coinbase_bytes, extranonce_offset) = build_coinbase(
        template.height,
        template.coinbase_value,
        &payout.script_pubkey,
        coinbase_tag,
        extranonce1_len,
        extranonce2_len,
        template.witness_commitment.as_deref(),
    )?;

    // ── 2. Split coinbase around extranonce placeholder ───────────────────────
    let coinbase1 = coinbase_bytes[..extranonce_offset].to_vec();
    let en_total = extranonce1_len + extranonce2_len;
    let coinbase2 = coinbase_bytes[extranonce_offset + en_total..].to_vec();

    Ok(StratumJob {
        job_id,
        prev_hash: template.prev_hash.clone(),
        coinbase1,
        coinbase2,
        merkle_branch: template.merkle_branch.clone(),
        merkle_branch_raw: template.merkle_branch_raw.clone(),
        version: template.version,
        bits: template.bits.clone(),
        cur_time: template.cur_time,
        height: template.height,
        network_target: template.network_target,
        coinbase_template: coinbase_bytes,
        extranonce_offset,
        extranonce1_len,
        extranonce2_len,
        transactions: template.transactions.clone(),
        payout_address: payout.address.clone(),
        has_witness_commitment: template.witness_commitment.is_some(),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Coinbase construction
// ─────────────────────────────────────────────────────────────────────────────

/// Returns `(serialized_coinbase, extranonce_offset)`.
///
/// Coinbase scriptSig layout:
///   [BIP34 height push] [tag bytes] [extranonce1 placeholder] [extranonce2 placeholder]
///
/// The extranonce placeholder region is zeroed; callers fill it at submit time.
fn build_coinbase(
    height: u64,
    reward: u64,
    reward_script: &ScriptBuf,
    tag: &str,
    extranonce1_len: usize,
    extranonce2_len: usize,
    witness_commitment: Option<&[u8]>,
) -> Result<(Vec<u8>, usize), PoolError> {
    // ── scriptSig ─────────────────────────────────────────────────────────────
    let height_script = encode_bip34_height(height);
    let tag_bytes = tag.as_bytes();

    let en_total = extranonce1_len + extranonce2_len;
    if en_total == 0 {
        return Err(PoolError::Other(anyhow::anyhow!(
            "Total extranonce width must be at least one byte"
        )));
    }

    let script_sig_len = height_script.len() + tag_bytes.len() + en_total;
    check_coinbase_script_sig_len(script_sig_len)?;

    let mut script_sig_content = Vec::with_capacity(script_sig_len);
    script_sig_content.extend_from_slice(&height_script);
    script_sig_content.extend_from_slice(tag_bytes);
    script_sig_content.resize(script_sig_len, 0x00); // extranonce placeholder

    let script_sig = ScriptBuf::from_bytes(script_sig_content.clone());

    // ── Inputs ────────────────────────────────────────────────────────────────
    let coinbase_input = TxIn {
        previous_output: OutPoint::null(), // all-zeros (coinbase)
        script_sig,
        sequence: Sequence::MAX,
        witness: Witness::default(),
    };

    // ── Outputs ───────────────────────────────────────────────────────────────
    let reward_output = TxOut {
        value: Amount::from_sat(reward),
        script_pubkey: reward_script.clone(),
    };

    let mut outputs = vec![reward_output];

    // SegWit witness commitment (OP_RETURN)
    if let Some(witness_script) = witness_commitment {
        // GBT's default_witness_commitment is already the full scriptPubKey
        // (OP_RETURN OP_36 0xaa21a9ed <32-byte-hash>), so use it as-is.
        outputs.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(witness_script.to_vec()),
        });
    }

    // ── Assemble transaction (non-segwit serialisation for coinbase) ──────────
    let tx = Transaction {
        version: bitcoin::transaction::Version(1),
        lock_time: bitcoin::locktime::absolute::LockTime::ZERO,
        input: vec![coinbase_input],
        output: outputs,
    };

    let serialized = serialize(&tx);

    // ── Locate the extranonce placeholder inside the serialized bytes ─────────
    //
    // Legacy coinbase layout, which is what `serialize` emits for an input with
    // no witness:
    //   4       version
    //   1       vin count (varint, always 1)
    //   36      outpoint (32-byte null hash + 4-byte index)
    //   1       scriptSig length (varint; one byte because consensus caps the
    //           scriptSig at 100 bytes, enforced above)
    //   N       scriptSig    ← height ‖ tag ‖ extranonce
    //   4       sequence
    //   …       outputs, locktime
    //
    // so the offset is arithmetic. The search below pins that against where the
    // bytes actually landed.
    //
    // This is checked in release too, not just under `debug_assert`. If the
    // offset is ever wrong, the extranonce overwrites the wrong field and the
    // pool and the miner agree on the *same* corrupt coinbase — so the share
    // validates, the block is invalid, and nothing between here and a rejected
    // `submitblock` can tell. That is worth one byte search per template per
    // payout identity, which is not the share path.
    const SCRIPT_SIG_OFFSET: usize = 4 + 1 + 36 + 1;
    if find_bytes(&serialized, &script_sig_content) != Some(SCRIPT_SIG_OFFSET) {
        return Err(PoolError::Other(anyhow::anyhow!(
            "coinbase scriptSig is not at offset {SCRIPT_SIG_OFFSET}; the \
             serialization layout the extranonce splice depends on has changed"
        )));
    }
    let offset = SCRIPT_SIG_OFFSET + height_script.len() + tag_bytes.len();

    // The coinbase has to fit in the room Core's assembler held back for it, or
    // the block exceeds the weight limit. It is witness-free here, so weight is
    // simply four times the size; the witness reserved value added at submit
    // time is 32 bytes of witness data, one weight unit each.
    let coinbase_weight = serialized.len() as u64 * 4 + 36;
    if coinbase_weight > COINBASE_WEIGHT_RESERVE {
        return Err(PoolError::Other(anyhow::anyhow!(
            "coinbase weighs {coinbase_weight} WU, over the \
             {COINBASE_WEIGHT_RESERVE} WU the block template reserved for it"
        )));
    }

    Ok((serialized, offset))
}

/// Consensus bounds on the coinbase scriptSig (`bad-cb-length`): at least 2
/// bytes, at most 100. Everything in it is operator-configured — the BIP34
/// height push, `coinbase_tag`, and the extranonce widths — so an over-long tag
/// would otherwise only surface as a rejected block, on the day one is found.
pub const MAX_COINBASE_SCRIPT_SIG: usize = 100;
pub const MIN_COINBASE_SCRIPT_SIG: usize = 2;

pub fn check_coinbase_script_sig_len(len: usize) -> Result<(), PoolError> {
    if !(MIN_COINBASE_SCRIPT_SIG..=MAX_COINBASE_SCRIPT_SIG).contains(&len) {
        return Err(PoolError::Other(anyhow::anyhow!(
            "coinbase scriptSig would be {len} bytes; consensus requires \
             {MIN_COINBASE_SCRIPT_SIG}..={MAX_COINBASE_SCRIPT_SIG} \
             (BIP34 height push + coinbase_tag + extranonce1_size + extranonce2_size)"
        )));
    }
    Ok(())
}

/// Encode the block height for the BIP34 coinbase scriptSig, matching Bitcoin
/// Core's `CScript() << nHeight` exactly — consensus rejects (`bad-cb-height`)
/// anything else.
///
/// Core's `push_int64` has three cases, and consensus validation does a strict
/// prefix match against them:
/// - height 0 → `OP_0` (single byte 0x00)
/// - height 1..=16 → `OP_1..OP_16` (single byte 0x51..0x60)
/// - height >= 17 → minimal `CScriptNum` data push (a `len` byte then the
///   little-endian bytes, with a 0x00 sign byte appended when the top bit is set)
///
/// Post-BIP34 mainnet heights are all far above 16, so only the data-push case
/// ever runs there — which is why an earlier version that *always* used the
/// data-push form worked on mainnet but produced `bad-cb-height` when mining the
/// first 16 blocks of a fresh regtest / signet chain.
fn encode_bip34_height(height: u64) -> Vec<u8> {
    if height == 0 {
        return vec![0x00]; // OP_0
    }
    if height <= 16 {
        return vec![0x50 + height as u8]; // OP_1 (0x51) ..= OP_16 (0x60)
    }
    let mut n = height;
    let mut bytes = Vec::new();
    while n > 0 {
        bytes.push((n & 0xff) as u8);
        n >>= 8;
    }
    // If high bit set, add 0x00 to avoid sign-bit interpretation
    if bytes.last().is_some_and(|&b| b & 0x80 != 0) {
        bytes.push(0x00);
    }
    let mut result = vec![bytes.len() as u8];
    result.extend_from_slice(&bytes);
    result
}

// ─────────────────────────────────────────────────────────────────────────────
// Merkle branch
// ─────────────────────────────────────────────────────────────────────────────

/// Compute the Stratum merkle branch for the coinbase transaction.
///
/// `txids` must contain every non-coinbase txid in internal byte order.
/// The returned hashes are the coinbase path siblings, from leaf upward.
pub fn compute_merkle_branch_raw(txids: &[[u8; 32]]) -> Vec<[u8; 32]> {
    if txids.is_empty() {
        return vec![];
    }

    let mut branch = Vec::new();
    let mut path_index = 0usize; // coinbase is always leaf 0
    let mut level: Vec<Option<[u8; 32]>> = Vec::with_capacity(txids.len() + 1);
    level.push(None); // placeholder for the unknown coinbase hash
    level.extend(txids.iter().copied().map(Some));

    while level.len() > 1 {
        if level.len() % 2 != 0 {
            let last = *level.last().expect("non-empty merkle level");
            level.push(last);
        }

        let sibling_index = if path_index % 2 == 0 {
            path_index + 1
        } else {
            path_index - 1
        };

        if let Some(sibling) = level[sibling_index] {
            branch.push(sibling);
        }

        let mut next_level = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks(2) {
            match (pair[0], pair[1]) {
                (Some(left), Some(right)) => {
                    let mut buf = [0u8; 64];
                    buf[..32].copy_from_slice(&left);
                    buf[32..].copy_from_slice(&right);
                    next_level.push(Some(double_sha256(&buf)));
                }
                _ => next_level.push(None),
            }
        }

        level = next_level;
        path_index /= 2;
    }

    branch
}

#[allow(dead_code)]
pub fn compute_merkle_branch(txids: &[[u8; 32]]) -> Vec<String> {
    compute_merkle_branch_raw(txids)
        .iter()
        .map(hex::encode)
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

/// Double SHA-256
pub fn double_sha256(data: &[u8]) -> [u8; 32] {
    let first = Sha256::digest(data);
    let second = Sha256::digest(first);
    second.into()
}

/// Convert compact `bits` (hex string) to a 32-byte big-endian target.
pub fn bits_to_target(bits_hex: &str) -> Result<[u8; 32], PoolError> {
    let bits = u32::from_str_radix(bits_hex, 16)
        .map_err(|_| PoolError::Other(anyhow::anyhow!("Invalid bits: {bits_hex}")))?;
    compact_to_target(bits)
}

/// Convert compact `bits` (hex string) to a network difficulty value.
pub fn bits_to_difficulty(bits_hex: &str) -> Result<f64, PoolError> {
    let bits = u32::from_str_radix(bits_hex, 16)
        .map_err(|_| PoolError::Other(anyhow::anyhow!("Invalid bits: {bits_hex}")))?;
    let exponent = ((bits >> 24) & 0xff) as i32;
    let mantissa = (bits & 0x007f_ffff) as u64;
    if mantissa == 0 {
        return Err(PoolError::Other(anyhow::anyhow!(
            "Invalid bits mantissa = 0"
        )));
    }

    // difficulty = diff1_target / current_target
    // diff1_target = 0x00ffff * 2^208
    // current_target  = mantissa * 2^(8*(exponent-3))
    // => difficulty = (0x00ffff / mantissa) * 2^(232 - 8*exponent)
    let diff1_const = 0x00ffffu64 as f64;
    let exponent_factor = 232.0 - (8.0 * exponent as f64);
    let diff = diff1_const / mantissa as f64 * 2f64.powf(exponent_factor);
    Ok(diff)
}

/// Convert a network difficulty to a 32-byte share target using exact integer division.
///
/// difficulty_1_target = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
pub fn difficulty_to_target(difficulty: u64) -> [u8; 32] {
    const DIFF1_TARGET: [u8; 32] = [
        0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];

    if difficulty <= 1 {
        return DIFF1_TARGET;
    }

    div_be_u256_by_u64(&DIFF1_TARGET, difficulty)
}

/// Compute the difficulty of a share from its hash (little-endian SHA256d output).
///
/// difficulty = DIFF1_TARGET / hash, where DIFF1_TARGET is Bitcoin's difficulty-1 target:
/// `0x00000000FFFF0000000000000000000000000000000000000000000000000000`
pub fn hash_to_difficulty(hash_le: &[u8; 32]) -> u64 {
    // Convert LE hash to big-endian for magnitude comparison.
    let mut hash_be = *hash_le;
    hash_be.reverse();

    // Find the first non-zero byte (position of the most significant byte).
    let nz = hash_be.iter().position(|&b| b != 0).unwrap_or(31);

    // Extract up to 8 significant bytes of the hash, left-aligned into a u64.
    let take = 8usize.min(32 - nz);
    let mut hash_sig: u64 = 0;
    for j in 0..take {
        hash_sig = (hash_sig << 8) | (hash_be[nz + j] as u64);
    }
    hash_sig <<= (8 - take) * 8; // left-align to fill the full 8-byte slot

    if hash_sig == 0 {
        return u64::MAX;
    }

    // DIFF1_TARGET (BE): [00 00 00 00 FF FF 00 00 ... 00]
    //   first non-zero byte at index 4; 8 bytes from there = 0xFFFF000000000000
    const DIFF1_NZ: i32 = 4;
    const DIFF1_SIG: u64 = 0xFFFF_0000_0000_0000;

    let ratio = DIFF1_SIG / hash_sig;
    let exp = (nz as i32 - DIFF1_NZ) * 8; // positive → hash has more leading zeros

    if exp >= 64 {
        u64::MAX
    } else if exp >= 0 {
        let exp = exp as u32;
        if ratio > u64::MAX >> exp {
            u64::MAX
        } else {
            ratio << exp
        }
    } else if -exp >= 64 {
        0
    } else {
        ratio >> (-exp) as u32
    }
}

fn compact_to_target(bits: u32) -> Result<[u8; 32], PoolError> {
    let exponent = ((bits >> 24) & 0xff) as usize;
    let mantissa = bits & 0x007f_ffff;

    if mantissa == 0 {
        return Ok([0u8; 32]);
    }

    let mut target = [0u8; 32];
    if exponent <= 3 {
        let value = mantissa >> (8 * (3 - exponent));
        let bytes = value.to_be_bytes();
        target[28..32].copy_from_slice(&bytes);
        return Ok(target);
    }

    let shift = exponent - 3;
    if shift > 29 {
        return Err(PoolError::Other(anyhow::anyhow!(
            "bits overflow target width: {bits:08x}"
        )));
    }

    let mantissa_bytes = [
        ((mantissa >> 16) & 0xff) as u8,
        ((mantissa >> 8) & 0xff) as u8,
        (mantissa & 0xff) as u8,
    ];
    let offset = 32 - 3 - shift;
    target[offset..offset + 3].copy_from_slice(&mantissa_bytes);
    Ok(target)
}

fn div_be_u256_by_u64(value: &[u8; 32], divisor: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    let mut rem: u128 = 0;

    for (i, byte) in value.iter().enumerate() {
        let accum = (rem << 8) | (*byte as u128);
        out[i] = (accum / divisor as u128) as u8;
        rem = accum % divisor as u128;
    }

    out
}

/// Convert `getblocktemplate`'s `previousblockhash` into Stratum
/// `mining.notify` prev-hash format.
///
/// Core returns the hash in **display** (big-endian) order — the same string
/// `getblockhash` prints. A block header's `hashPrevBlock` field, by contrast,
/// holds the hash in **internal** byte order (the full byte-reverse of the
/// display string). The canonical Stratum prev-hash is that internal hash with
/// each 4-byte word byte-swapped; the miner (and our [`build_header`]) recover
/// the internal bytes by swapping each word back.
///
/// So the conversion is two steps: full 32-byte reverse (display → internal),
/// then a per-word swap. Doing only the per-word swap — as an earlier version
/// did — cancels against `build_header`'s swap and leaves the header carrying
/// the *display*-order hash, which no node recognizes (`prev-blk-not-found`),
/// silently invalidating every block this pool finds. Share validation never
/// caught it because it reconstructs the same header and only checks PoW.
pub fn stratum_prev_hash(core_hex: &str) -> Result<String, PoolError> {
    let mut bytes = hex::decode(core_hex)
        .map_err(|_| PoolError::Other(anyhow::anyhow!("Invalid prev_hash hex")))?;
    if bytes.len() != 32 {
        return Err(PoolError::Other(anyhow::anyhow!(
            "prev_hash must be 32 bytes"
        )));
    }
    bytes.reverse(); // display (big-endian) → internal byte order
    for chunk in bytes.chunks_mut(4) {
        chunk.reverse(); // internal → Stratum per-word swap
    }
    Ok(hex::encode(bytes))
}

/// Locate the first occurrence of `needle` in `haystack`.
fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::consensus::deserialize;

    fn payout(address: &str, script: Vec<u8>) -> PayoutDescriptor {
        PayoutDescriptor {
            address: address.to_string(),
            script_pubkey: ScriptBuf::from_bytes(script),
        }
    }

    fn sample_template() -> Arc<JobTemplate> {
        Arc::new(JobTemplate {
            prev_hash: "00".repeat(32),
            merkle_branch: Vec::new(),
            merkle_branch_raw: Vec::new(),
            version: 0x2000_0000,
            bits: "1d00ffff".to_string(),
            cur_time: 1_700_000_000,
            height: 900_000,
            network_target: bits_to_target("1d00ffff").unwrap(),
            transactions: Arc::new(vec![vec![1, 2, 3]]),
            coinbase_value: 312_500_000,
            witness_commitment: None,
        })
    }

    #[test]
    fn payout_jobs_use_distinct_reward_scripts_and_share_transactions() {
        let template = sample_template();
        let payout_a = payout("address-a", vec![0x51]);
        let payout_b = payout("address-b", vec![0x52]);

        let job_a = build_job_for_payout(template.clone(), &payout_a, "/test/", 4, 4).unwrap();
        let job_b = build_job_for_payout(template, &payout_b, "/test/", 4, 4).unwrap();
        let tx_a: Transaction = deserialize(&job_a.coinbase_template).unwrap();
        let tx_b: Transaction = deserialize(&job_b.coinbase_template).unwrap();

        assert_eq!(tx_a.output[0].script_pubkey, payout_a.script_pubkey);
        assert_eq!(tx_b.output[0].script_pubkey, payout_b.script_pubkey);
        assert_ne!(job_a.coinbase_template, job_b.coinbase_template);
        assert!(Arc::ptr_eq(&job_a.transactions, &job_b.transactions));
    }

    #[test]
    fn coinbase_reserves_exactly_the_requested_extranonce_width() {
        let payout = payout("address", vec![0x51]);
        let job = build_job_for_payout(sample_template(), &payout, "/test/", 1, 1).unwrap();
        let assembled = job.assemble_coinbase(&[0x12], &[0x34]);
        assert_eq!(
            &assembled[job.extranonce_offset..job.extranonce_offset + 2],
            &[0x12, 0x34]
        );
        assert_eq!(assembled.len(), job.coinbase_template.len());
    }

    /// `assemble_coinbase_into` avoids a `coinbase_template.clone()` per
    /// validated share. Pin the two properties it must uphold: the bytes are
    /// identical to the cloning form's, and a reused buffer stops allocating.
    #[test]
    fn assemble_coinbase_into_matches_the_cloning_form() {
        let payout = payout("address", vec![0x51]);
        let job = build_job_for_payout(sample_template(), &payout, "/test/", 4, 4).unwrap();

        // The reference: clone the template, then splice the extranonce in at
        // the reserved offset.
        let splice = |en1: &[u8], en2: &[u8]| -> Vec<u8> {
            let mut cb = job.coinbase_template.clone();
            let off = job.extranonce_offset;
            if en1.len() + en2.len() == job.extranonce1_len + job.extranonce2_len {
                cb[off..off + en1.len()].copy_from_slice(en1);
                cb[off + en1.len()..off + en1.len() + en2.len()].copy_from_slice(en2);
            }
            cb
        };

        // Exact width, and the SV2-style split that shifts the boundary but
        // still fills the reserved region.
        for (en1, en2) in [
            (vec![0xAB; 4], vec![0xCD; 4]),
            (vec![0xAB; 6], vec![0xCD; 2]),
        ] {
            let mut buf = Vec::new();
            job.assemble_coinbase_into(&en1, &en2, &mut buf);
            assert_eq!(buf, splice(&en1, &en2));
        }

        // Width mismatch: the region must stay zeroed so the share fails
        // validation on its merits rather than panicking.
        let mut buf = Vec::new();
        job.assemble_coinbase_into(&[0xAB; 4], &[0xCD; 3], &mut buf);
        assert_eq!(buf, job.coinbase_template);
        let off = job.extranonce_offset;
        assert!(buf[off..off + 8].iter().all(|&b| b == 0));
    }

    #[test]
    fn assemble_coinbase_into_reuses_buffer_capacity() {
        let payout = payout("address", vec![0x51]);
        let job = build_job_for_payout(sample_template(), &payout, "/test/", 4, 4).unwrap();

        let mut buf = Vec::new();
        job.assemble_coinbase_into(&[0x01; 4], &[0x02; 4], &mut buf);
        let capacity_after_first = buf.capacity();

        for n in 0..8u8 {
            job.assemble_coinbase_into(&[n; 4], &[n; 4], &mut buf);
            assert_eq!(
                buf.capacity(),
                capacity_after_first,
                "a reused buffer must not reallocate"
            );
            assert_eq!(buf.len(), job.coinbase_template.len());
            let off = job.extranonce_offset;
            assert_eq!(&buf[off..off + 8], &[n; 8]);
        }
    }

    /// The extranonce offset is computed from the serialization layout
    /// rather than found by scanning. Check it against a full deserialization
    /// for both the one-byte and the wide extranonce case.
    #[test]
    fn extranonce_offset_lands_inside_the_serialized_script_sig() {
        for (en1, en2) in [(1usize, 1usize), (4, 4), (8, 8)] {
            let payout = payout("address", vec![0x51]);
            let job =
                build_job_for_payout(sample_template(), &payout, "/btcpool-rs/", en1, en2).unwrap();
            let filled = job.assemble_coinbase(&vec![0xAB; en1], &vec![0xCD; en2]);
            let tx: Transaction = deserialize(&filled).unwrap();
            let script_sig = tx.input[0].script_sig.as_bytes();
            let tail = &script_sig[script_sig.len() - (en1 + en2)..];
            assert_eq!(tail, [vec![0xABu8; en1], vec![0xCDu8; en2]].concat());
        }
    }

    #[test]
    fn an_overlong_coinbase_tag_is_rejected_rather_than_mined() {
        // Consensus caps the coinbase scriptSig at 100 bytes; a tag that pushes
        // it over would only surface as `bad-cb-length` on the day a block is
        // found, so the job build must refuse it here.
        let payout = payout("address", vec![0x51]);
        let tag = "x".repeat(MAX_COINBASE_SCRIPT_SIG);
        let err = build_job_for_payout(sample_template(), &payout, &tag, 4, 4).unwrap_err();
        assert!(
            err.to_string().contains("coinbase scriptSig"),
            "unexpected error: {err}"
        );

        // And the largest tag that still fits is accepted.
        let height_push = encode_bip34_height(900_000).len();
        let fits = "x".repeat(MAX_COINBASE_SCRIPT_SIG - height_push - 8);
        assert!(build_job_for_payout(sample_template(), &payout, &fits, 4, 4).is_ok());
    }

    #[test]
    fn test_bip34_height_encoding() {
        // Must match Bitcoin Core's `CScript() << nHeight` exactly, or the
        // block is rejected with bad-cb-height.
        // 0 → OP_0
        assert_eq!(encode_bip34_height(0), vec![0x00]);
        // 1..=16 → OP_1..OP_16 (0x51..0x60), single byte
        assert_eq!(encode_bip34_height(1), vec![0x51]);
        assert_eq!(encode_bip34_height(16), vec![0x60]);
        // 17 is the first data-push height: push 1 byte 0x11
        assert_eq!(encode_bip34_height(17), vec![0x01, 0x11]);
        // 127 fits in one byte without sign extension
        assert_eq!(encode_bip34_height(127), vec![0x01, 0x7f]);
        // 128 needs a 0x00 sign byte so the top bit isn't read as negative
        assert_eq!(encode_bip34_height(128), vec![0x02, 0x80, 0x00]);
        // A realistic post-BIP34 mainnet height: 0x0DBBA0 = 900_000
        assert_eq!(encode_bip34_height(900_000), vec![0x03, 0xa0, 0xbb, 0x0d]);
    }

    #[test]
    fn test_bits_to_target_mainnet_genesis() {
        // Genesis bits: 0x1d00ffff
        let target = bits_to_target("1d00ffff").unwrap();
        assert_eq!(&target[..6], &[0, 0, 0, 0, 0xff, 0xff]);
    }

    #[test]
    fn test_difficulty_to_target_diff1() {
        let t = difficulty_to_target(1);
        assert!(t[4] > 0, "diff-1 target should be non-zero around byte 4");
    }

    #[test]
    fn test_stratum_prev_hash_yields_correct_header_internal_bytes() {
        // Ground truth: the regtest genesis. `getblockhash 0` prints the
        // DISPLAY hash; a block header's hashPrevBlock must hold the INTERNAL
        // bytes = the full byte-reverse of the display string.
        let display = "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206";
        let mut internal = hex::decode(display).unwrap();
        internal.reverse();

        // stratum_prev_hash() is what we put on the wire in mining.notify.
        let stratum = hex::decode(stratum_prev_hash(display).unwrap()).unwrap();

        // build_header() recovers the header field by swapping each 4-byte word.
        // Replicate that exact step here; the result MUST equal the internal
        // bytes, or the node rejects the block with prev-blk-not-found.
        let mut recovered = stratum.clone();
        for chunk in recovered.chunks_mut(4) {
            chunk.reverse();
        }
        assert_eq!(
            recovered, internal,
            "header hashPrevBlock must equal the genesis internal byte order"
        );
        // And the wire value must NOT be the naive per-word swap of display,
        // which would round-trip back to display order.
        let mut naive = hex::decode(display).unwrap();
        for chunk in naive.chunks_mut(4) {
            chunk.reverse();
        }
        assert_ne!(
            stratum, naive,
            "prev-hash must not be the per-word swap of display order"
        );
    }

    #[test]
    fn test_empty_merkle_branch() {
        let branch = compute_merkle_branch(&[]);
        assert!(branch.is_empty());
    }

    #[test]
    fn test_merkle_branch_for_three_transactions() {
        let tx1 = [0x11u8; 32];
        let tx2 = [0x22u8; 32];
        let branch = compute_merkle_branch(&[tx1, tx2]);
        assert_eq!(branch.len(), 2);
        assert_eq!(branch[0], hex::encode(tx1));

        let mut buf = [0u8; 64];
        buf[..32].copy_from_slice(&tx2);
        buf[32..].copy_from_slice(&tx2);
        assert_eq!(branch[1], hex::encode(double_sha256(&buf)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Template rules and consensus tripwires
    // ─────────────────────────────────────────────────────────────────────────

    use crate::bitcoin::rpc::{GbtResult, GbtTransaction};

    fn sample_gbt() -> GbtResult {
        GbtResult {
            version: 0x2000_0000,
            prev_hash: "00".repeat(32),
            bits: "1d00ffff".to_string(),
            cur_time: 1_700_000_000,
            height: 900_000,
            coinbase_value: block_subsidy(900_000),
            transactions: Vec::new(),
            longpoll_id: None,
            // Shaped like the real thing: OP_RETURN OP_36 aa21a9ed ‖ 32 bytes.
            // Every segwit-active node sends one, so its presence is the default
            // and its absence is what a test has to opt into.
            default_witness_commitment: Some("6a24aa21a9ed".to_string() + &"11".repeat(32)),
            rules: vec!["csv".into(), "!segwit".into(), "taproot".into()],
            vbrequired: 0,
        }
    }

    fn sample_tx(txid: &str, fee: u64, weight: u64) -> GbtTransaction {
        GbtTransaction {
            data: vec![0xde, 0xad],
            txid: txid.to_string(),
            hash: txid.to_string(),
            fee,
            weight,
        }
    }

    /// The fail-closed default is only safe if the rules a real node actually
    /// sends are on the allow-list. Both arrays are what Core 31.1 returned on
    /// 2026-08-07 (`tests/fixtures/gbt-*.json`); a future Core that adds a rule
    /// should fail here, in CI, rather than by refusing to mine in production.
    #[test]
    fn rules_from_a_real_node_are_all_supported() {
        // Identical on both chains today, kept apart because they need not stay
        // that way — a rule buried on mainnet can still be live on regtest.
        let mainnet_961441 = ["csv", "!segwit", "taproot"];
        let regtest_1 = ["csv", "!segwit", "taproot"];

        for (chain, rules) in [("mainnet", mainnet_961441), ("regtest", regtest_1)] {
            let gbt = GbtResult {
                rules: rules.iter().map(|r| r.to_string()).collect(),
                ..sample_gbt()
            };
            assert!(
                unsupported_gbt_rules(&gbt).is_empty(),
                "{chain} sends a rule that is not on SUPPORTED_GBT_RULES"
            );
        }
    }

    /// The `!` prefix is the whole signal: it means a client that rewrites the
    /// coinbase — which this pool does — must understand the rule.
    #[test]
    fn a_forced_rule_this_build_does_not_implement_is_reported() {
        let gbt = GbtResult {
            rules: vec!["!segwit".into(), "!greatfork".into()],
            ..sample_gbt()
        };
        assert_eq!(unsupported_gbt_rules(&gbt), vec!["greatfork".to_string()]);
    }

    /// Without the prefix the node is reporting what it enforces, not demanding
    /// that we act. Stopping the pool over one would be a self-inflicted outage.
    #[test]
    fn an_unforced_rule_this_build_does_not_know_is_ignored() {
        let gbt = GbtResult {
            rules: vec!["csv".into(), "quietfork".into()],
            ..sample_gbt()
        };
        assert!(unsupported_gbt_rules(&gbt).is_empty());
    }

    /// A required version bit that BIP320 rolling would let a miner clear cannot
    /// be honoured after the fact — the version is in the header they hashed —
    /// so it is reported alongside the rules rather than silently dropped. A
    /// required bit *outside* the mask is fine: nothing can rewrite it.
    #[test]
    fn a_required_version_bit_is_reported_only_when_miners_could_clear_it() {
        let inside = GbtResult {
            vbrequired: 1 << 20,
            ..sample_gbt()
        };
        assert_eq!(
            unsupported_gbt_rules(&inside),
            vec!["vbrequired:0x00100000".to_string()]
        );

        let outside = GbtResult {
            vbrequired: 1 << 4,
            ..sample_gbt()
        };
        assert!(unsupported_gbt_rules(&outside).is_empty());
    }

    /// A txid is a merkle leaf. Truncating a short one would produce a root that
    /// is wrong but self-consistent, which share validation cannot see; the
    /// previous code instead panicked inside the refresh task and froze the
    /// template. Neither is acceptable — it has to be an error.
    #[test]
    fn a_malformed_txid_is_an_error_not_a_panic() {
        for bad in ["", "abcd", &"ab".repeat(31), "zz".repeat(32).as_str()] {
            let gbt = GbtResult {
                transactions: vec![sample_tx(bad, 0, 400)],
                ..sample_gbt()
            };
            assert!(
                build_job_template(&gbt).is_err(),
                "txid {bad:?} should be rejected"
            );
        }

        let good = GbtResult {
            transactions: vec![sample_tx(&"ab".repeat(32), 0, 400)],
            ..sample_gbt()
        };
        assert!(build_job_template(&good).is_ok());
    }

    /// Building a segwit block without the commitment output is
    /// `bad-witness-merkle-match`. The commitment is the one thing in the
    /// coinbase the pool copies rather than derives, so its absence has to stop
    /// the template rather than quietly change the coinbase's shape.
    #[test]
    fn a_segwit_template_without_a_commitment_is_rejected() {
        assert!(build_job_template(&sample_gbt()).is_ok());

        let missing = GbtResult {
            default_witness_commitment: None,
            ..sample_gbt()
        };
        assert!(build_job_template(&missing).is_err());

        // No segwit rule announced: the pre-segwit shape is legitimate.
        let no_segwit = GbtResult {
            rules: vec!["csv".into()],
            default_witness_commitment: None,
            ..sample_gbt()
        };
        assert!(build_job_template(&no_segwit).is_ok());
    }

    #[test]
    fn block_subsidy_halves_every_210_000_blocks() {
        assert_eq!(block_subsidy(0), 50 * 100_000_000);
        assert_eq!(block_subsidy(209_999), 50 * 100_000_000);
        assert_eq!(block_subsidy(210_000), 25 * 100_000_000);
        assert_eq!(block_subsidy(630_000), 625_000_000);
        // The height in tests/fixtures/gbt-mainnet.json, whose coinbasevalue of
        // 313_883_546 was exactly this plus 1_383_546 in template fees.
        assert_eq!(block_subsidy(961_441), 312_500_000);
        assert_eq!(block_subsidy(210_000 * 64), 0);
        assert_eq!(block_subsidy(u64::MAX), 0);
    }

    /// Core assembles against the weight limit minus a coinbase reserve, so a
    /// template that leaves us no room means that contract has broken.
    #[test]
    fn a_template_that_leaves_no_room_for_the_coinbase_is_rejected() {
        let fits = GbtResult {
            transactions: vec![sample_tx(&"ab".repeat(32), 0, 3_990_000)],
            ..sample_gbt()
        };
        assert!(build_job_template(&fits).is_ok());

        let overflows = GbtResult {
            transactions: vec![sample_tx(&"ab".repeat(32), 0, 3_999_000)],
            ..sample_gbt()
        };
        assert!(build_job_template(&overflows).is_err());
    }

    /// The coinbase has to fit the room Core held back for it. Reachable here
    /// only through an absurd payout script, which is the point: the check is a
    /// tripwire on the reserve assumption, not on any real address.
    #[test]
    fn a_coinbase_over_the_reserved_weight_is_rejected() {
        let template = sample_template();
        let huge = payout("address", vec![0x51; 1_000]);
        assert!(build_job_for_payout(template, &huge, "/test/", 4, 4).is_err());
    }
}
