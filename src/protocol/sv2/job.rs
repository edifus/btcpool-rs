//! protocol/sv2/job.rs
//!
//! Translation of the protocol-agnostic [`StratumJob`] into Stratum V2 mining
//! messages, plus the difficulty/target and prev-hash byte-order conversions
//! that differ between SV1 and SV2.
//!
//! SV1 `mining.notify` bundles coinbase, merkle branch, prev-hash, nbits and
//! ntime in one message. SV2 splits this into [`NewExtendedMiningJob`] (coinbase
//! prefix/suffix, merkle path, version) and [`SetNewPrevHash`] (prev-hash, nbits,
//! min-ntime). The mining core is unchanged — only the wire shape differs.
use crate::bitcoin::template::{difficulty_to_target, StratumJob};
use anyhow::{anyhow, Result};
use binary_sv2::{Seq0255, Sv2Option, B064K, U256};
use mining_sv2::{NewExtendedMiningJob, SetNewPrevHash};

/// Convert a pool share `difficulty` into a Stratum V2 wire target.
///
/// [`difficulty_to_target`] returns a 32-byte **big-endian** target (matching
/// the validator's `meets_target`, which compares the big-endian hash). Stratum
/// V2 encodes targets as a 256-bit **little-endian** integer, so we reverse.
pub fn difficulty_to_sv2_target(difficulty: u64) -> [u8; 32] {
    let mut t = difficulty_to_target(difficulty);
    t.reverse();
    t
}

/// Compare two SV2 (little-endian) U256 targets. Returns true if `a <= b`,
/// i.e. target `a` is at least as hard as target `b`.
pub fn sv2_target_le(a: &[u8; 32], b: &[u8; 32]) -> bool {
    // Reverse to big-endian for magnitude comparison.
    let mut ab = *a;
    let mut bb = *b;
    ab.reverse();
    bb.reverse();
    ab <= bb
}

/// Convert a Stratum V1 "stratum order" prev-hash (hex; eight byte-swapped
/// 32-bit words) into the 32-byte internal byte order used in the block header
/// and by Stratum V2's `SetNewPrevHash.prev_hash`.
///
/// This is the exact transform `validator::build_header` applies when populating
/// header bytes `[4..36]`, kept in sync so SV1 and SV2 hash identical headers.
pub fn stratum_prevhash_to_internal(stratum_hex: &str) -> Result<[u8; 32]> {
    let mut bytes = hex::decode(stratum_hex).map_err(|e| anyhow!("prev_hash hex: {e}"))?;
    if bytes.len() != 32 {
        return Err(anyhow!("prev_hash must be 32 bytes, got {}", bytes.len()));
    }
    for chunk in bytes.chunks_mut(4) {
        chunk.reverse();
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Build a [`NewExtendedMiningJob`] for an extended channel.
///
/// `coinbase1` → `coinbase_tx_prefix`, `coinbase2` → `coinbase_tx_suffix`, and
/// `merkle_branch_raw` → `merkle_path` (all already in SV2's internal byte
/// order). When `future` is true the job is announced ahead of its prev-hash
/// (the new-block flow — activated by a later [`SetNewPrevHash`] with a matching
/// `job_id`); otherwise it carries `min_ntime` and is mineable immediately on
/// the most recently announced prev-hash.
pub fn build_new_extended_job(
    job: &StratumJob,
    channel_id: u32,
    job_id: u32,
    future: bool,
) -> Result<NewExtendedMiningJob<'static>> {
    let merkle: Vec<U256> = job
        .merkle_branch_raw
        .iter()
        .map(|h| U256::from(*h))
        .collect();

    let min_ntime = if future {
        Sv2Option::new(None)
    } else {
        Sv2Option::new(Some(job.cur_time))
    };

    Ok(NewExtendedMiningJob {
        channel_id,
        job_id,
        min_ntime,
        version: job.version,
        // BIP320 general-purpose bits may be rolled by the device.
        version_rolling_allowed: true,
        merkle_path: Seq0255::new(merkle).map_err(|e| anyhow!("merkle_path: {e:?}"))?,
        coinbase_tx_prefix: B064K::try_from(job.coinbase1.clone())
            .map_err(|e| anyhow!("coinbase_tx_prefix: {e:?}"))?,
        coinbase_tx_suffix: B064K::try_from(job.coinbase2.clone())
            .map_err(|e| anyhow!("coinbase_tx_suffix: {e:?}"))?,
    })
}

/// Build a [`SetNewPrevHash`] referencing the given job.
pub fn build_set_new_prev_hash(
    job: &StratumJob,
    channel_id: u32,
    job_id: u32,
) -> Result<SetNewPrevHash<'static>> {
    let prev = stratum_prevhash_to_internal(&job.prev_hash)?;
    let nbits = u32::from_str_radix(&job.bits, 16).map_err(|e| anyhow!("nbits: {e}"))?;
    Ok(SetNewPrevHash {
        channel_id,
        job_id,
        prev_hash: U256::from(prev),
        min_ntime: job.cur_time,
        nbits,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sv2_target_is_byte_reversed_big_endian_target() {
        for diff in [1u64, 2, 4096, 1_000_000, u32::MAX as u64] {
            let be = difficulty_to_target(diff);
            let le = difficulty_to_sv2_target(diff);
            let mut le_rev = le;
            le_rev.reverse();
            assert_eq!(
                be, le_rev,
                "SV2 LE target must be reverse of BE target (diff={diff})"
            );
        }
    }

    #[test]
    fn higher_difficulty_is_a_harder_sv2_target() {
        // Harder difficulty (8192) must compare as <= easier difficulty (4096).
        let hard = difficulty_to_sv2_target(8192);
        let easy = difficulty_to_sv2_target(4096);
        assert!(sv2_target_le(&hard, &easy));
        assert!(!sv2_target_le(&easy, &hard));
    }

    #[test]
    fn prevhash_word_reversal_roundtrips() {
        // 32 distinct bytes; reversing 4-byte words twice is the identity.
        let bytes: Vec<u8> = (0u8..32).collect();
        let hex_in = hex::encode(&bytes);
        let internal = stratum_prevhash_to_internal(&hex_in).unwrap();
        // Each 4-byte word should be reversed relative to the input.
        for w in 0..8 {
            for b in 0..4 {
                assert_eq!(internal[w * 4 + b], bytes[w * 4 + (3 - b)]);
            }
        }
        // Round-trip back through the same transform yields the original.
        let back = stratum_prevhash_to_internal(&hex::encode(internal)).unwrap();
        assert_eq!(&back[..], &bytes[..]);
    }

    #[test]
    fn prevhash_rejects_wrong_length() {
        assert!(stratum_prevhash_to_internal("00").is_err());
    }
}
