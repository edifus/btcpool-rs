/// mining/validator.rs
///
/// Share and block validation logic:
///  - Reconstruct the 80-byte block header from share parameters
///  - Verify the double-SHA256 hash meets the share target
///  - Detect duplicate shares (per-session set)
///  - Detect network-difficulty hits (BLOCK FOUND!)
///  - Stale share detection (job not current)
///  - Version-rolling validation (BIP320 mask enforcement)
use crate::{
    bitcoin::template::{difficulty_to_target, double_sha256, hash_to_difficulty, StratumJob},
    error::PoolError,
    mining::jobs::JobEntry,
};

use std::{
    cell::RefCell,
    collections::{HashSet, VecDeque},
};

thread_local! {
    /// Scratch buffer for the per-share coinbase splice.
    ///
    /// Validation only ever runs inside `spawn_blocking`, so this lives on the
    /// blocking pool's threads and amortises to zero allocations after the first
    /// share each thread handles. `validate_share_no_dedup` is not recursive and
    /// calls nothing that re-enters it, so the `RefCell` cannot be double-borrowed.
    static COINBASE_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

// ─────────────────────────────────────────────────────────────────────────────
// BIP320 version-rolling mask
// ─────────────────────────────────────────────────────────────────────────────

/// Only these bits are allowed to be modified by the miner (BIP320).
/// 0x1FFFE000 = bits 13–28 (16 bits of version space)
pub const VERSION_ROLLING_MASK: u32 = 0x1FFF_E000;

// ─────────────────────────────────────────────────────────────────────────────
// ntime bounds
// ─────────────────────────────────────────────────────────────────────────────

/// Consensus `MAX_FUTURE_BLOCK_TIME`: a block whose time exceeds the validating
/// node's clock by more than this is rejected `time-too-new`. Doubles as the
/// pool's forward ntime-rolling allowance.
const MAX_NTIME_DRIFT_SECS: u32 = 7200;

/// Wall-clock unix seconds, for the ntime ceiling only.
///
/// A clock that predates the epoch reads as `u32::MAX`, which makes the absolute
/// bound saturate out of the way and leaves the template-relative one in force.
/// That is the pre-existing behaviour: a broken clock should cost the extra
/// guard, not reject every share the pool receives.
fn now_unix_secs() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().min(u32::MAX as u64) as u32)
        .unwrap_or(u32::MAX)
}

/// The highest ntime a share may carry: the tighter of the pool's drift policy
/// (measured from the template) and consensus `time-too-new` (measured from the
/// clock). See the call site in `validate_share_no_dedup` for why both are
/// needed.
fn ntime_ceiling(cur_time: u32, now: u32) -> u32 {
    cur_time
        .saturating_add(MAX_NTIME_DRIFT_SECS)
        .min(now.saturating_add(MAX_NTIME_DRIFT_SECS))
}

// ─────────────────────────────────────────────────────────────────────────────
// Share duplicate tracker
// ─────────────────────────────────────────────────────────────────────────────

/// Per-session duplicate-share tracker.
///
/// Only *validated* shares are recorded (callers `contains`-check before
/// validating and `insert` after it succeeds), so invalid submissions cannot
/// occupy slots and later identical valid submits are judged on their own
/// merits. Sessions `clear` the set on every clean-job broadcast — a clean job
/// retires all outstanding jobs, so entries never need to outlive one — which
/// keeps replay protection scoped to live jobs instead of depending on FIFO
/// eviction. The FIFO cap remains as a memory backstop; filling it now takes
/// real proof-of-work at the session floor difficulty within a single job
/// generation, not free invalid submits.
#[derive(Clone, Default)]
pub struct ShareSet {
    seen: HashSet<ShareKey>,
    /// Insertion-order queue for FIFO eviction.
    order: VecDeque<ShareKey>,
    max_size: usize,
}

#[derive(Hash, PartialEq, Eq, Clone)]
pub struct ShareKey {
    job_id: String,
    extranonce2: Vec<u8>,
    ntime: u32,
    nonce: u32,
    version_bits: u32,
}

impl ShareKey {
    pub fn new(
        job_id: &str,
        extranonce2: &[u8],
        ntime: u32,
        nonce: u32,
        version_bits: u32,
    ) -> Self {
        Self {
            job_id: job_id.to_string(),
            extranonce2: extranonce2.to_vec(),
            ntime,
            nonce,
            version_bits,
        }
    }
}

impl ShareSet {
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
            order: VecDeque::new(),
            max_size: 4096,
        }
    }

    /// Whether this share was already accepted this job generation.
    pub fn contains(&self, key: &ShareKey) -> bool {
        self.seen.contains(key)
    }

    /// Record a share that passed validation.
    pub fn insert(&mut self, key: ShareKey) {
        if self.seen.contains(&key) {
            return;
        }
        if self.seen.len() >= self.max_size {
            // Evict the oldest entry rather than clearing the whole set.
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }
        self.order.push_back(key.clone());
        self.seen.insert(key);
    }

    /// Drop all entries. Called on clean-job broadcasts: every outstanding job
    /// is retired, so stale-job rejection takes over from dedup.
    pub fn clear(&mut self) {
        self.seen.clear();
        self.order.clear();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Share submission parameters
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct ShareParams {
    pub job_id: String,
    pub extranonce2: Vec<u8>,
    pub ntime: u32,
    pub nonce: u32,
    /// BIP320: miner-submitted version bits
    pub version_bits: Option<u32>,
    /// Per-session negotiated version-rolling mask
    pub version_rolling_mask: Option<u32>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Validation result
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ShareResult {
    /// Valid share meeting pool difficulty — keep mining
    Valid {
        assigned_difficulty: u64,
        /// Actual difficulty of the hash (≥ assigned_difficulty).
        hash_difficulty: u64,
        hash: [u8; 32],
    },
    /// 🎉 Valid share that ALSO meets network difficulty — submit block!
    Block {
        /// Actual difficulty of the hash.
        hash_difficulty: u64,
        block_hex: String,
        hash: [u8; 32],
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// Core validation function
// ─────────────────────────────────────────────────────────────────────────────

/// Validate a share submission.
///
/// Returns:
///   - `Ok(ShareResult::Valid)` — good share
///   - `Ok(ShareResult::Block)` — block found, submit immediately
///   - `Err(PoolError::*)` — rejected share with reason
pub fn validate_share_no_dedup(
    params: &ShareParams,
    job: &StratumJob,
    job_entry: &JobEntry,
    extranonce1: &[u8],
    session_difficulty: u64,
) -> Result<ShareResult, PoolError> {
    // ── 1. Stale job check ────────────────────────────────────────────────────
    if job_entry.superseded_by_clean {
        return Err(PoolError::StaleJob(params.job_id.clone()));
    }

    // ── 2. ntime validation ───────────────────────────────────────────────────
    //
    // The floor is pool policy that happens to subsume consensus: consensus
    // wants ntime > median-time-past, and Core sets `curtime = max(MTP+1, now)`,
    // so refusing anything below `curtime` can never produce `time-too-old`.
    //
    // The ceiling is consensus, and it is measured against the *validating
    // node's* clock (`time-too-new` at now + MAX_FUTURE_BLOCK_TIME), not against
    // the template. Those coincide while `curtime ≈ now`, but `curtime` is
    // `MTP+1` whenever that exceeds the node's clock — a host running more than
    // ~an hour slow, or regtest under `setmocktime` — and then a share at the
    // top of a template-relative window becomes a block the node rejects
    // outright. So both bounds are applied: the template-relative one as the
    // drift policy, and an absolute one against our own clock.
    let ceiling = ntime_ceiling(job.cur_time, now_unix_secs());
    if params.ntime < job.cur_time || params.ntime > ceiling {
        return Err(PoolError::InvalidParams {
            method: "mining.submit",
            detail: format!(
                "ntime out of range: submitted={} template_curtime={} ceiling={}",
                params.ntime, job.cur_time, ceiling
            ),
        });
    }

    // Steps 3-9 borrow the thread-local coinbase scratch for the whole run so the
    // splice, the merkle root and (on the block path) block assembly all read the
    // same buffer without anyone allocating a fresh coinbase per share.
    COINBASE_SCRATCH.with_borrow_mut(|coinbase| {
        // ── 3. Assemble coinbase ──────────────────────────────────────────────
        job.assemble_coinbase_into(extranonce1, &params.extranonce2, coinbase);

        // ── 4. Compute merkle root ────────────────────────────────────────────
        let merkle_root = job.merkle_root(coinbase);

        // ── 5. Resolve version (with optional BIP320 rolling) ────────────────
        let version = resolve_version(
            job.version,
            params.version_bits,
            params.version_rolling_mask,
        )?;

        // ── 7. Assemble 80-byte block header ─────────────────────────────────
        let header = build_header(
            version,
            &job.prev_hash,
            &merkle_root,
            params.ntime,
            &job.bits,
            params.nonce,
        )?;

        // ── 8. Double-SHA256 of header ────────────────────────────────────────
        let hash = double_sha256(&header);

        // ── 9. Check hash meets pool share target ─────────────────────────────
        let share_target = difficulty_to_target(session_difficulty);
        if !meets_target(&hash, &share_target) {
            tracing::warn!(
                hash_le = %hex::encode(hash),
                hash_be = %block_hash_display(&hash),
                share_target = %hex::encode(share_target),
                session_difficulty = session_difficulty,
                "Share failed target check"
            );
            return Err(PoolError::LowDifficulty);
        }

        let hash_difficulty = hash_to_difficulty(&hash);

        // ── 6. Check if hash also meets network target (BLOCK FOUND!) ────────
        if meets_target(&hash, &job.network_target) {
            let block_hex = assemble_block_hex(
                &header,
                coinbase,
                &job.transactions,
                job.has_witness_commitment,
            );
            tracing::info!(
                "🎉 BLOCK FOUND! height={} hash={}",
                job.height,
                block_hash_display(&hash)
            );
            return Ok(ShareResult::Block {
                hash_difficulty,
                block_hex,
                hash,
            });
        }
        Ok(ShareResult::Valid {
            assigned_difficulty: session_difficulty,
            hash_difficulty,
            hash,
        })
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Apply BIP320 version-rolling: only modify bits allowed by the mask.
fn resolve_version(
    base_version: u32,
    rolling_bits: Option<u32>,
    negotiated_mask: Option<u32>,
) -> Result<u32, PoolError> {
    let mask = negotiated_mask.unwrap_or(VERSION_ROLLING_MASK);
    match rolling_bits {
        Some(bits) => {
            if bits & !mask != 0 {
                return Err(PoolError::InvalidParams {
                    method: "mining.submit",
                    detail: format!(
                        "version bits outside negotiated mask: bits={bits:08x} mask={mask:08x}"
                    ),
                });
            }
            Ok((base_version & !mask) | (bits & mask))
        }
        None => Ok(base_version),
    }
}

/// Build an 80-byte block header.
///
/// Layout (all little-endian):
///   4  version
///   32 prev_block  (Stratum-format → must reverse back to internal order)
///   32 merkle_root
///   4  ntime
///   4  nbits
///   4  nonce
fn build_header(
    version: u32,
    stratum_prev_hash: &str,
    merkle_root: &[u8; 32],
    ntime: u32,
    bits_hex: &str,
    nonce: u32,
) -> Result<[u8; 80], PoolError> {
    let mut header = [0u8; 80];

    // version (LE)
    header[..4].copy_from_slice(&version.to_le_bytes());

    // prev_hash: un-stratum it (reverse each 4-byte word back).
    // `decode_to_slice` also enforces the 64-char length, which the later
    // `copy_from_slice` would otherwise turn into a panic.
    let mut prev_internal = [0u8; 32];
    hex::decode_to_slice(stratum_prev_hash, &mut prev_internal)
        .map_err(|_| PoolError::InvalidHeader)?;
    for chunk in prev_internal.chunks_mut(4) {
        chunk.reverse();
    }
    header[4..36].copy_from_slice(&prev_internal);

    // merkle root (LE — bitcoin's internal byte order)
    header[36..68].copy_from_slice(merkle_root);

    // ntime (LE)
    header[68..72].copy_from_slice(&ntime.to_le_bytes());

    // nbits (from hex, stored LE)
    let bits = u32::from_str_radix(bits_hex, 16).map_err(|_| PoolError::InvalidHeader)?;
    header[72..76].copy_from_slice(&bits.to_le_bytes());

    // nonce (LE)
    header[76..80].copy_from_slice(&nonce.to_le_bytes());

    Ok(header)
}

/// Serialise the complete block as hex for submitblock.
fn assemble_block_hex(
    header: &[u8; 80],
    coinbase: &[u8],
    transactions: &[Vec<u8>],
    has_witness_commitment: bool,
) -> String {
    let with_witness;
    let coinbase = match has_witness_commitment
        .then(|| coinbase_with_witness_reserved_value(coinbase))
        .flatten()
    {
        Some(bytes) => {
            with_witness = bytes;
            &with_witness[..]
        }
        None => {
            if has_witness_commitment {
                // The commitment output is present but the reserved value could
                // not be attached, so this hex is `bad-witness-nonce-size` to
                // anything that does not repair it. `submitblock` does, which is
                // why the block is still worth sending — but the archived hex
                // and any P2P relay of it are not valid, and that is worth
                // knowing before someone replays the archive by hand.
                tracing::error!(
                    "could not attach the BIP141 witness reserved value to the \
                     coinbase; submitting without it (Core's submitblock repairs \
                     this, the archived hex will not be relayable as-is)"
                );
            }
            coinbase
        }
    };

    let mut block = Vec::with_capacity(
        80 + coinbase.len() + transactions.iter().map(|t| t.len()).sum::<usize>() + 16,
    );
    block.extend_from_slice(header);

    // Transaction count varint
    let tx_count = 1 + transactions.len(); // coinbase + rest
    block.extend_from_slice(&encode_varint(tx_count as u64));

    // Coinbase first
    block.extend_from_slice(coinbase);

    // All other transactions
    for tx in transactions {
        block.extend_from_slice(tx);
    }

    hex::encode(block)
}

/// Re-serialise the coinbase with the BIP141 witness reserved value in its
/// input's witness, for block submission only.
///
/// A block whose coinbase carries the witness-commitment output must also carry
/// "a single 32-byte array for the witness reserved value" in the coinbase
/// input's witness, or it is rejected with `bad-witness-nonce-size`. The value
/// is 32 zero bytes: that is what Core's `getblocktemplate` computed
/// `default_witness_commitment` against.
///
/// Core's `submitblock` RPC repairs a missing one for us today
/// (`UpdateUncommittedBlockStructures`), which is why the pool has been mining
/// acceptable blocks without it — but nothing else does. The archived
/// `found-blocks/*.hex`, a block relayed over P2P, and Core's newer IPC mining
/// interface all need the witness to be there already.
///
/// Only the block encoding changes: `coinbase1`/`coinbase2` and the merkle root
/// keep using the stripped serialisation, since the txid is computed over that.
fn coinbase_with_witness_reserved_value(coinbase: &[u8]) -> Option<Vec<u8>> {
    use bitcoin::{
        consensus::encode::{deserialize, serialize},
        Transaction, Witness,
    };

    let mut tx: Transaction = deserialize(coinbase).ok()?;
    tx.input.first_mut()?.witness = Witness::from_slice(&[[0u8; 32]]);
    Some(serialize(&tx))
}

/// Check `hash < target` (both 32-byte big-endian).
pub fn meets_target(hash: &[u8; 32], target: &[u8; 32]) -> bool {
    let mut hash_be = *hash;
    hash_be.reverse();
    hash_be <= *target
}

/// The conventional display form of a block hash: big-endian hex, the string a
/// block explorer, `getblockheader` and `bitcoin-cli` all speak.
///
/// `hash` here is the raw double-SHA256, which is internal (little-endian)
/// order — the reverse. `hex::encode` on it directly produces a string with the
/// leading zeros at the *end*, which resolves nowhere and no RPC will accept.
/// Every hash that leaves this process for a human or a node goes through here.
pub fn block_hash_display(hash: &[u8; 32]) -> String {
    let mut be = *hash;
    be.reverse();
    hex::encode(be)
}

fn encode_varint(n: u64) -> Vec<u8> {
    if n < 0xfd {
        vec![n as u8]
    } else if n <= 0xffff {
        let mut v = vec![0xfd];
        v.extend_from_slice(&(n as u16).to_le_bytes());
        v
    } else if n <= 0xffff_ffff {
        let mut v = vec![0xfe];
        v.extend_from_slice(&(n as u32).to_le_bytes());
        v
    } else {
        let mut v = vec![0xff];
        v.extend_from_slice(&n.to_le_bytes());
        v
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// The mainnet genesis block, against the two forms in the same assertion:
    /// hex-encoding the raw hash directly is what the pool used to report, and
    /// it is the reverse of the string every explorer and RPC speaks.
    #[test]
    fn a_block_hash_is_displayed_the_way_the_rest_of_bitcoin_writes_it() {
        let mut genesis = [0u8; 32];
        genesis.copy_from_slice(
            &hex::decode("6fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000")
                .unwrap(),
        );

        assert_eq!(
            block_hash_display(&genesis),
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
        // The leading zeros of proof-of-work land at the wrong end without it.
        assert_ne!(block_hash_display(&genesis), hex::encode(genesis));
    }

    /// `prev_hash` is pool-generated and always 64 hex chars, so the old
    /// `hex::decode` + `copy_from_slice` pair never tripped — but a wrong length
    /// reached `copy_from_slice` and would have panicked inside the blocking
    /// validation task. It is an error now.
    #[test]
    fn a_wrong_length_prev_hash_is_an_error_not_a_panic() {
        let merkle = [0u8; 32];
        for prev in ["", "abcd", &"00".repeat(31), &"00".repeat(33)] {
            let err = build_header(0x2000_0000, prev, &merkle, 1_700_000_000, "1d00ffff", 0)
                .expect_err("a prev_hash that is not 32 bytes must be rejected");
            assert!(matches!(err, PoolError::InvalidHeader), "got {err:?}");
        }

        // The well-formed case still builds, with the stratum word-reversal applied.
        assert!(build_header(
            0x2000_0000,
            &"00".repeat(32),
            &merkle,
            1_700_000_000,
            "1d00ffff",
            0
        )
        .is_ok());
    }

    #[test]
    fn test_meets_target_lower() {
        // hash is raw SHA256d output (LE, byte[0]=LSB).
        // meets_target reverses it to BE before comparing with the BE target.
        // Significant byte at position 29 → becomes position 2 in BE → 0x01 < target[2]=0x02
        let mut hash = [0u8; 32];
        hash[29] = 0x01;
        let target = [
            0x00, 0x00, 0x02, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ];
        assert!(meets_target(&hash, &target));
    }

    #[test]
    fn test_meets_target_higher() {
        // Significant byte 0x03 at position 29 → BE position 2 → 0x03 > target[2]=0x02
        let mut hash = [0u8; 32];
        hash[29] = 0x03;
        let target = [
            0x00, 0x00, 0x02, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ];
        assert!(!meets_target(&hash, &target));
    }

    #[test]
    fn test_version_rolling_mask() {
        let base: u32 = 0x2000_0000;
        let miner_bits: u32 = 0x0001_E000; // within mask
        let result = (base & !VERSION_ROLLING_MASK) | (miner_bits & VERSION_ROLLING_MASK);
        assert_eq!(result & !VERSION_ROLLING_MASK, base & !VERSION_ROLLING_MASK);
        assert_eq!(
            result & VERSION_ROLLING_MASK,
            miner_bits & VERSION_ROLLING_MASK
        );
    }

    /// Test keys with distinct header nonces. The nonces come from a range
    /// rather than literals: CodeQL's hard-coded-cryptographic-value heuristic
    /// reads the mining header nonce as a cryptographic nonce and flags any
    /// constant flowing into it.
    fn test_keys(count: u32) -> Vec<ShareKey> {
        (0..count)
            .map(|n| ShareKey::new("job1", b"en2", 12345, n, 0))
            .collect()
    }

    #[test]
    fn test_duplicate_share_detection() {
        let mut ss = ShareSet::new();
        let keys = test_keys(2);
        assert!(!ss.contains(&keys[0]));
        ss.insert(keys[0].clone());
        assert!(ss.contains(&keys[0]));
        // Different nonce should not be a duplicate
        assert!(!ss.contains(&keys[1]));
        // Different version bits should also not be a duplicate
        let mut other_bits = keys[0].clone();
        other_bits.version_bits = 0x2000;
        assert!(!ss.contains(&other_bits));
    }

    #[test]
    fn invalid_shares_do_not_occupy_dedup_slots() {
        // The caller only inserts after validation passes, so a rejected
        // submission leaves no trace: an identical later submit that validates
        // is judged fresh, not misreported as `duplicate`.
        let mut ss = ShareSet::new();
        let key = test_keys(1).remove(0);
        assert!(!ss.contains(&key)); // invalid attempt: checked, never inserted
        assert!(!ss.contains(&key)); // same share resubmitted: still fresh
        ss.insert(key.clone()); // now it validates
        assert!(ss.contains(&key)); // and only now is a resubmit a duplicate
    }

    #[test]
    fn clear_retires_all_entries_on_clean_job() {
        let mut ss = ShareSet::new();
        let key = test_keys(1).remove(0);
        ss.insert(key.clone());
        assert!(ss.contains(&key));
        ss.clear();
        assert!(!ss.contains(&key));
        // Re-inserting after a clear works (fresh job generation).
        ss.insert(key.clone());
        assert!(ss.contains(&key));
    }

    #[test]
    fn insert_is_bounded_by_fifo_eviction() {
        let mut ss = ShareSet {
            max_size: 4,
            ..ShareSet::default()
        };
        let keys = test_keys(6);
        for k in &keys {
            ss.insert(k.clone());
        }
        // Oldest two evicted, newest four retained.
        assert!(!ss.contains(&keys[0]));
        assert!(!ss.contains(&keys[1]));
        for k in &keys[2..] {
            assert!(ss.contains(k));
        }
        // Double-insert of a present key must not grow the FIFO or evict.
        ss.insert(keys[5].clone());
        assert!(ss.contains(&keys[2]));
    }

    /// BIP141: a block carrying the witness commitment must carry the 32-byte
    /// witness reserved value in its coinbase input, and adding it must not
    /// disturb the txid the merkle root commits to.
    #[test]
    fn submitted_block_coinbase_carries_the_witness_reserved_value() {
        use crate::bitcoin::template::{bits_to_target, build_job_for_payout, JobTemplate};
        use crate::mining::identity::PayoutDescriptor;
        use bitcoin::{consensus::encode::deserialize, ScriptBuf, Transaction};
        use std::sync::Arc;

        // Shaped like GBT's default_witness_commitment: OP_RETURN OP_36
        // aa21a9ed ‖ 32-byte commitment.
        let mut commitment = vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
        commitment.extend_from_slice(&[0x11; 32]);

        let template = Arc::new(JobTemplate {
            prev_hash: "00".repeat(32),
            merkle_branch: Vec::new(),
            merkle_branch_raw: Vec::new(),
            version: 0x2000_0000,
            bits: "1d00ffff".to_string(),
            cur_time: 1_700_000_000,
            height: 900_000,
            network_target: bits_to_target("1d00ffff").unwrap(),
            transactions: Arc::new(Vec::new()),
            coinbase_value: 312_500_000,
            witness_commitment: Some(commitment),
        });
        let payout = PayoutDescriptor {
            address: "address".to_string(),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        };
        let job = build_job_for_payout(template, &payout, "/test/", 4, 4).unwrap();
        assert!(job.has_witness_commitment);

        let coinbase = job.assemble_coinbase(&[1, 2, 3, 4], &[5, 6, 7, 8]);
        let stripped: Transaction = deserialize(&coinbase).unwrap();

        let block = hex::decode(assemble_block_hex(&[0u8; 80], &coinbase, &[], true)).unwrap();
        // header ‖ tx-count varint (1 byte, value 1) ‖ coinbase
        assert_eq!(block[80], 1);
        let submitted: Transaction = deserialize(&block[81..]).unwrap();

        let witness = &submitted.input[0].witness;
        assert_eq!(witness.len(), 1, "expected exactly one witness item");
        assert_eq!(witness.iter().next().unwrap(), &[0u8; 32][..]);
        assert_eq!(
            submitted.compute_txid(),
            stripped.compute_txid(),
            "adding the witness must not change the txid the merkle root commits to"
        );
    }

    /// Consensus measures `time-too-new` against the *validating node's* clock,
    /// not against the template. The two agree while `curtime ≈ now`, but Core
    /// sets `curtime = max(MTP+1, now)`, so a host running more than about an
    /// hour slow produces a template whose own +2h window reaches past what the
    /// node will accept — and a share at the top of it becomes a rejected block.
    #[test]
    fn the_ntime_ceiling_is_the_tighter_of_the_template_and_the_clock() {
        const DRIFT: u32 = MAX_NTIME_DRIFT_SECS;
        let curtime = 1_700_000_000;

        // Normal case: the template was built roughly now, so the two bounds
        // coincide and the full drift window is available.
        assert_eq!(ntime_ceiling(curtime, curtime), curtime + DRIFT);

        // Stale template — the clock has moved on. The template-relative bound
        // is tighter and stays in force, unchanged from previous behaviour.
        assert_eq!(ntime_ceiling(curtime, curtime + 600), curtime + DRIFT);

        // The failure this guards: curtime is an hour ahead of the clock because
        // MTP+1 exceeded it. The consensus bound is what applies, and it is
        // 3600s below what the template alone would have allowed.
        let now = curtime - 3600;
        assert_eq!(ntime_ceiling(curtime, now), now + DRIFT);
        assert!(ntime_ceiling(curtime, now) < curtime + DRIFT);

        // A clock that cannot be read at all reads as u32::MAX, which saturates
        // the absolute bound away rather than rejecting every share.
        assert_eq!(ntime_ceiling(curtime, u32::MAX), curtime + DRIFT);
    }

    /// Without a commitment output a witness would be `unexpected-witness`.
    #[test]
    fn block_without_a_commitment_keeps_a_bare_coinbase() {
        let coinbase = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0403a0bb0dffffffff0100f2052a01000000015100000000",
        )
        .unwrap();
        let block = hex::decode(assemble_block_hex(&[0u8; 80], &coinbase, &[], false)).unwrap();
        assert_eq!(&block[81..], &coinbase[..]);
    }

    #[test]
    fn test_varint_encoding() {
        assert_eq!(encode_varint(0xfc), vec![0xfc]);
        assert_eq!(encode_varint(0xfd), vec![0xfd, 0xfd, 0x00]);
        assert_eq!(encode_varint(0x1234), vec![0xfd, 0x34, 0x12]);
    }
}
