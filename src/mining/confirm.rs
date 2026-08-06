/// mining/confirm.rs
///
/// The deferred confirmation pass: does a block we counted still exist on the
/// chain an hour later?
///
/// `submitblock`'s verdict is true at the instant it is read and no longer.
/// A block that wins its height can be reorged out, and a block that lost a
/// same-height race can be promoted onto the active chain by the reorg that
/// follows. Both are corrected here, by re-reading each enrolled block's
/// position with `getblockheader` until the answer is final.
use crate::{
    bitcoin::rpc::{BlockChainPosition, RpcClient},
    metrics,
    mining::accounting,
    stats::{BlockResolution, PendingBlock, PoolStats},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info, warn};

/// How often the pending set is swept. A block is ten minutes, so this is far
/// finer than the state it watches can change; it costs nothing in the usual
/// case because a sweep with nothing pending makes no RPC call at all.
const POLL_INTERVAL: Duration = Duration::from_secs(60);

/// How long a block whose hash the node does not recognise is kept before being
/// written off. Reached only when the node was reindexed, restored from a
/// snapshot, or swapped for a different one — otherwise it has the block, on
/// one branch or another. Without this the pending set would grow forever.
const ABANDON_AFTER: Duration = Duration::from_secs(24 * 3600);

/// What one probe says about a block's fate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Not decided yet — ask again next sweep.
    Pending,
    Final(BlockResolution),
}

/// Decide a block from one `getblockheader` reading.
///
/// Pure and clock-free so the rule is unit-testable: like
/// `classify_submit_response`, getting it wrong is invisible in production
/// until the block counts are already wrong.
///
/// `depth` is required symmetrically in both directions. On the active chain it
/// is the usual finality threshold. On a side branch it is what separates a
/// block that has merely lost a race — a reorg can still restore it, and one
/// that is one block deep routinely is — from one a competing chain has buried
/// for good. Without that condition a block would be declared orphaned during
/// the very reorg that was about to bring it back.
pub fn classify_confirmation(
    position: BlockChainPosition,
    block_height: u64,
    depth: u32,
) -> Verdict {
    match position {
        BlockChainPosition::OnChain { confirmations, .. } if confirmations >= depth => {
            Verdict::Final(BlockResolution::Confirmed)
        }
        BlockChainPosition::OnChain { .. } => Verdict::Pending,
        BlockChainPosition::SideBranch { tip_height }
            if tip_height >= block_height + depth as u64 =>
        {
            Verdict::Final(BlockResolution::Orphaned)
        }
        BlockChainPosition::SideBranch { .. } => Verdict::Pending,
        // The node may yet catch up — or may never. `ABANDON_AFTER` is what
        // decides, and it needs a clock, so it does not belong here.
        BlockChainPosition::Unknown => Verdict::Pending,
    }
}

/// Sweep the pending set forever, reconciling every block that has become
/// decidable.
///
/// Runs alongside the other pool tickers in `main`. A failed probe is logged
/// and retried on the next tick: the pending set is durable, so nothing is lost
/// by being patient, and a node that is down is exactly when this must not give
/// up on a block.
pub async fn run(rpc: Arc<RpcClient>, stats: Arc<PoolStats>, depth: u32) {
    let mut ticker = interval(POLL_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        sweep(&rpc, &stats, depth).await;
    }
}

/// One pass over the pending set.
async fn sweep(rpc: &Arc<RpcClient>, stats: &Arc<PoolStats>, depth: u32) {
    let pending = stats.pending_blocks();
    metrics::update_blocks_pending_confirmation(pending.len() as u64);
    if pending.is_empty() {
        return;
    }

    for block in pending {
        let position = match rpc.block_chain_position(block.hash.clone()).await {
            Ok(position) => position,
            Err(e) => {
                warn!(
                    "Could not check block {} (height {}) for confirmation: {e}",
                    block.hash, block.height
                );
                continue;
            }
        };

        let verdict = classify_confirmation(position, block.height, depth);
        let resolution = match verdict {
            Verdict::Final(resolution) => resolution,
            Verdict::Pending if is_abandoned(&block, position) => BlockResolution::Abandoned,
            Verdict::Pending => continue,
        };

        if accounting::record_block_resolution(stats, &block, resolution) {
            report(&block, resolution);
        }
    }

    metrics::update_blocks_pending_confirmation(stats.pending_blocks().len() as u64);
}

/// Whether a block the node no longer recognises has been unrecognised long
/// enough to write off. Only `Unknown` ages out: a block sitting on a side
/// branch or partway to `depth` is being answered, just not finally.
fn is_abandoned(block: &PendingBlock, position: BlockChainPosition) -> bool {
    if position != BlockChainPosition::Unknown {
        return false;
    }
    let age = PoolStats::now_secs().saturating_sub(block.found_ts);
    age >= ABANDON_AFTER.as_secs()
}

/// One line per resolved block, at the severity an operator would want to be
/// paged on. A block being reorged out is the whole reason this pass exists,
/// so it is an `error!` even though nothing malfunctioned.
fn report(block: &PendingBlock, resolution: BlockResolution) {
    let PendingBlock {
        hash,
        height,
        worker,
        ..
    } = block;
    match (block.won_at_submit, resolution) {
        (true, BlockResolution::Confirmed) => info!(
            "✅ Block {hash} (height {height}, worker {worker}) confirmed on the active chain"
        ),
        (true, BlockResolution::Orphaned) => error!(
            "Block {hash} (height {height}, worker {worker}) won its height but has been \
             reorged out; it earned nothing and no longer counts as a block found"
        ),
        (false, BlockResolution::Confirmed) => info!(
            "🏆 Block {hash} (height {height}, worker {worker}) lost its height race but a \
             reorg has since put it on the active chain; counting it as a block found"
        ),
        (false, BlockResolution::Orphaned) => info!(
            "Block {hash} (height {height}) stayed on the side branch it landed on, as expected"
        ),
        (_, BlockResolution::Abandoned) => error!(
            "Giving up confirming block {hash} (height {height}): the node has not \
             recognised the hash for {:?}. Its submit-time verdict stands.",
            ABANDON_AFTER
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEPTH: u32 = 6;

    fn on_chain(confirmations: u32) -> BlockChainPosition {
        BlockChainPosition::OnChain {
            confirmations,
            tip_height: 900_000,
        }
    }

    #[test]
    fn a_block_is_confirmed_at_exactly_the_configured_depth() {
        assert_eq!(
            classify_confirmation(on_chain(DEPTH), 100, DEPTH),
            Verdict::Final(BlockResolution::Confirmed)
        );
        assert_eq!(
            classify_confirmation(on_chain(DEPTH - 1), 100, DEPTH),
            Verdict::Pending
        );
        assert_eq!(
            classify_confirmation(on_chain(1), 100, DEPTH),
            Verdict::Pending
        );
    }

    #[test]
    fn a_side_branch_block_is_orphaned_only_once_it_is_buried() {
        // The reorg that would restore it is still possible until a competing
        // chain has buried it by the same margin we call final.
        assert_eq!(
            classify_confirmation(
                BlockChainPosition::SideBranch {
                    tip_height: 100 + DEPTH as u64 - 1
                },
                100,
                DEPTH
            ),
            Verdict::Pending
        );
        assert_eq!(
            classify_confirmation(
                BlockChainPosition::SideBranch {
                    tip_height: 100 + DEPTH as u64
                },
                100,
                DEPTH
            ),
            Verdict::Final(BlockResolution::Orphaned)
        );
    }

    #[test]
    fn a_one_block_reorg_does_not_orphan_anything_at_the_default_depth() {
        // The common case on mainnet: our block is momentarily off the tip.
        // Deciding on that reading alone is exactly the flapping this guards.
        assert_eq!(
            classify_confirmation(
                BlockChainPosition::SideBranch { tip_height: 101 },
                100,
                DEPTH
            ),
            Verdict::Pending
        );
    }

    #[test]
    fn an_unrecognised_hash_is_never_final_on_one_reading() {
        assert_eq!(
            classify_confirmation(BlockChainPosition::Unknown, 100, DEPTH),
            Verdict::Pending
        );
    }

    #[test]
    fn only_an_unrecognised_hash_can_age_out() {
        let old = PendingBlock {
            hash: "00".repeat(32),
            height: 100,
            worker: "w".into(),
            payout: "p".into(),
            found_ts: PoolStats::now_secs() - ABANDON_AFTER.as_secs() - 1,
            won_at_submit: true,
        };
        assert!(is_abandoned(&old, BlockChainPosition::Unknown));
        // Still being answered, just not finally — it must keep its place.
        assert!(!is_abandoned(
            &old,
            BlockChainPosition::SideBranch { tip_height: 101 }
        ));
        assert!(!is_abandoned(&old, on_chain(1)));

        let fresh = PendingBlock {
            found_ts: PoolStats::now_secs(),
            ..old
        };
        assert!(!is_abandoned(&fresh, BlockChainPosition::Unknown));
    }
}
