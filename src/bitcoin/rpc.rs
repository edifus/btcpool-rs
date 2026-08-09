/// bitcoin/rpc.rs
///
/// Thin wrapper around `bitcoincore-rpc` providing:
///  - Cookie-file authentication (Bitcoin Knots compatible)
///  - Transparent recovery from cookie rotation (bitcoind restart)
///  - `getblocktemplate`
///  - `submitblock`
///  - Best-block-hash polling (ZMQ fallback)
///
/// `bitcoincore-rpc` is synchronous, so every method that talks to the node is
/// exposed only in `async` form and runs the round trip on the blocking pool.
/// The sync bodies are private on purpose: that is what keeps a future caller
/// from parking a runtime worker on a node round trip.
use crate::{config::RpcConfig, error::PoolError};
use anyhow::{anyhow, Result};
use bitcoin::BlockHash;
use bitcoincore_rpc::{jsonrpc, Auth, Client, RpcApi};
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct GbtResult {
    pub version: u32,
    pub prev_hash: String,
    pub bits: String,
    pub cur_time: u32,
    pub height: u64,
    pub coinbase_value: u64,
    pub transactions: Vec<GbtTransaction>,
    pub longpoll_id: Option<String>,
    pub default_witness_commitment: Option<String>,
    pub rules: Vec<String>,
    /// BIP23 `vbrequired`: version bits the server requires set in submissions.
    ///
    /// Core hardcodes this to 0 and has never implemented it, so in practice it
    /// only carries information from a non-Core template source. It matters
    /// because BIP320 version rolling lets miners rewrite bits 13..28, and a
    /// required bit inside that window would be cleared by the miner rather
    /// than by anything the pool could fix after the fact — the version is in
    /// the header the miner already hashed.
    pub vbrequired: u32,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct GbtTransaction {
    pub data: Vec<u8>,
    pub txid: String,
    pub hash: String,
    pub fee: u64,
    pub weight: u64,
}

/// What the node did with a block we submitted.
///
/// A closed set, so the `outcome` Prometheus label cannot be minted from the
/// node's response text — same contract as `PoolError::submit_failure_label`.
///
/// The distinction that matters is [`is_win`](Self::is_win): only a block that
/// became the chain tip earned anything. `submitblock` reports a valid block
/// that lost a same-height race as `"inconclusive"`, and the node stores it on
/// a side branch — consensus-valid, worth nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockSubmitOutcome {
    /// `null` — accepted and connected as the new chain tip.
    Accepted,
    /// `duplicate` — the node already had this block, from an earlier attempt
    /// of ours that landed despite the RPC round trip appearing to fail.
    Duplicate,
    /// `inconclusive` / `duplicate-inconclusive` — valid and stored, but not on
    /// the best chain. Resubmitting cannot promote it.
    Inconclusive,
}

impl BlockSubmitOutcome {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Duplicate => "duplicate",
            Self::Inconclusive => "inconclusive",
        }
    }

    /// Whether this block won its height, and so counts as a block found.
    ///
    /// `Duplicate` counts: it is our own earlier submission, seen again by the
    /// retry ladder, and that first attempt is never counted anywhere else.
    pub const fn is_win(self) -> bool {
        !matches!(self, Self::Inconclusive)
    }
}

/// Where a block we already submitted sits relative to the active chain, as of
/// one `getblockheader`.
///
/// `submitblock`'s verdict is only true at the instant it is read: a block that
/// won its height can still be reorged out an hour later. This is what the
/// deferred confirmation pass re-reads, and like [`BlockSubmitOutcome`] it is a
/// closed set so the decision can live in a pure function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockChainPosition {
    /// On the active chain, buried under `confirmations` blocks (the block
    /// itself counts as one).
    OnChain { confirmations: u32, tip_height: u64 },
    /// The node has the block but it is not on the active chain
    /// (`confirmations == -1`). Not yet final in either direction — a reorg can
    /// still restore it, which is why the caller also weighs `tip_height`.
    SideBranch { tip_height: u64 },
    /// The node does not have this block at all: reindexed, pruned onto a
    /// different chain, or replaced entirely.
    Unknown,
}

struct Inner {
    client: Client,
    /// Cookie contents (user, password) we built `client` with, if cookie auth is in use.
    /// `None` means we're using explicit creds from config and rotation recovery is a no-op.
    cookie: Option<(String, String)>,
}

pub struct RpcClient {
    cfg: RpcConfig,
    state: RwLock<Inner>,
}

impl RpcClient {
    pub fn new(cfg: &RpcConfig) -> Result<Self> {
        let cookie = cfg.read_cookie().ok();
        let auth = cfg.rpc_auth()?;
        let client = build_client(cfg, auth)?;
        info!(
            "Bitcoin RPC connected to {} (timeout {}s)",
            cfg.url, cfg.timeout_secs
        );
        Ok(Self {
            cfg: cfg.clone(),
            state: RwLock::new(Inner { client, cookie }),
        })
    }

    /// Run `f` against the current client. If it fails and the cookie file on
    /// disk has changed since we built the client, rebuild and retry once —
    /// this recovers from a bitcoind restart without operator intervention.
    fn call_with_refresh<F, T>(&self, f: F) -> Result<T, PoolError>
    where
        F: Fn(&Client) -> Result<T, PoolError>,
    {
        let first_err = {
            let guard = self.state.read().expect("rpc state lock poisoned");
            match f(&guard.client) {
                Ok(v) => return Ok(v),
                Err(e) => e,
            }
        };

        // Only rebuild on cookie rotation — unreadable cookie or unchanged cookie
        // means this failure isn't something we can fix by reconnecting.
        let fresh_cookie = match self.cfg.read_cookie() {
            Ok(c) => c,
            Err(_) => return Err(first_err),
        };

        {
            let guard = self.state.read().expect("rpc state lock poisoned");
            if guard.cookie.as_ref() == Some(&fresh_cookie) {
                return Err(first_err);
            }
        }

        warn!("Bitcoin RPC cookie changed on disk; rebuilding client and retrying");
        let new_client = build_client(
            &self.cfg,
            Auth::UserPass(fresh_cookie.0.clone(), fresh_cookie.1.clone()),
        )
        .map_err(PoolError::Other)?;

        {
            let mut guard = self.state.write().expect("rpc state lock poisoned");
            guard.client = new_client;
            guard.cookie = Some(fresh_cookie);
        }

        let guard = self.state.read().expect("rpc state lock poisoned");
        f(&guard.client)
    }

    /// The chain the connected node is on, per `getblockchaininfo`:
    /// "main" | "test" | "signet" | "regtest". Queried once at boot — it is
    /// the source of truth for payout-address network validation.
    pub async fn chain(self: &Arc<Self>) -> Result<String, PoolError> {
        let this = self.clone();
        spawn_rpc("getblockchaininfo", move || this.chain_blocking()).await
    }

    fn chain_blocking(&self) -> Result<String, PoolError> {
        let info: Value =
            self.call_with_refresh(|c| c.call("getblockchaininfo", &[]).map_err(PoolError::Rpc))?;
        info.get("chain")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .ok_or_else(|| {
                PoolError::Other(anyhow::anyhow!(
                    "getblockchaininfo response missing 'chain'"
                ))
            })
    }

    /// Fetch a fresh block template. The heaviest RPC the node serves — it
    /// re-runs block assembly over the mempool — so it never touches the
    /// runtime.
    pub async fn get_block_template(self: &Arc<Self>) -> Result<GbtResult, PoolError> {
        let this = self.clone();
        spawn_rpc("getblocktemplate", move || {
            this.get_block_template_blocking()
        })
        .await
    }

    fn get_block_template_blocking(&self) -> Result<GbtResult, PoolError> {
        let result: Value = self.call_with_refresh(|c| {
            let request = json!({
                "rules": ["segwit"],
                "capabilities": ["coinbasetxn", "workid"]
            });
            c.call("getblocktemplate", &[request])
                .map_err(PoolError::Rpc)
        })?;

        let transactions = result
            .get("transactions")
            .and_then(Value::as_array)
            .map(|txs| txs.iter().map(parse_gbt_transaction).collect())
            .transpose()?
            .unwrap_or_default();

        Ok(GbtResult {
            version: value_as_u32(&result, "version")?,
            prev_hash: value_as_string(&result, "previousblockhash")?,
            bits: value_as_string(&result, "bits")?,
            cur_time: value_as_u32(&result, "curtime")?,
            height: value_as_u64(&result, "height")?,
            coinbase_value: value_as_u64(&result, "coinbasevalue")?,
            transactions,
            longpoll_id: result
                .get("longpollid")
                .and_then(Value::as_str)
                .map(str::to_owned),
            default_witness_commitment: result
                .get("default_witness_commitment")
                .and_then(Value::as_str)
                .map(str::to_owned),
            rules: result
                .get("rules")
                .and_then(Value::as_array)
                .map(|rules| {
                    rules
                        .iter()
                        .filter_map(|r| r.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
            // Absent or unparseable reads as 0, which is both Core's value and
            // the permissive one. A template source that means to require a bit
            // has to say so.
            vbrequired: result
                .get("vbrequired")
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32,
        })
    }

    /// Submit a found block. Takes an `Arc<String>` so the retry ladder in
    /// `TemplateEngine` can hand the same bytes to successive attempts without
    /// re-allocating them for each blocking task.
    pub async fn submit_block(
        self: &Arc<Self>,
        block_hex: Arc<String>,
    ) -> Result<BlockSubmitOutcome, PoolError> {
        let this = self.clone();
        spawn_rpc("submitblock", move || {
            this.submit_block_blocking(&block_hex)
        })
        .await
    }

    fn submit_block_blocking(&self, block_hex: &str) -> Result<BlockSubmitOutcome, PoolError> {
        let result: Value = self.call_with_refresh(|c| {
            c.call("submitblock", &[json!(block_hex)])
                .map_err(PoolError::Rpc)
        })?;

        let outcome = classify_submit_response(&result);
        match &outcome {
            Ok(BlockSubmitOutcome::Accepted) => info!("🎉 Block accepted by network!"),
            Ok(BlockSubmitOutcome::Duplicate) => {
                info!("submitblock: node already has this block (duplicate)")
            }
            Ok(BlockSubmitOutcome::Inconclusive) => {
                warn!("submitblock: block valid but not on the best chain (inconclusive)")
            }
            Err(e) => warn!("submitblock did not accept the block: {e}"),
        }
        outcome
    }

    pub async fn best_block_hash(self: &Arc<Self>) -> Result<String, PoolError> {
        let this = self.clone();
        spawn_rpc("getbestblockhash", move || this.best_block_hash_blocking()).await
    }

    fn best_block_hash_blocking(&self) -> Result<String, PoolError> {
        self.call_with_refresh(|c| {
            c.get_best_block_hash()
                .map(|h| h.to_string())
                .map_err(PoolError::Rpc)
        })
    }

    /// Where `hash_hex` sits relative to the active chain right now.
    ///
    /// `hash_hex` is the big-endian display form produced by
    /// [`crate::mining::validator::block_hash_display`] — the only form
    /// `getblockheader` accepts.
    ///
    /// Two round trips in one blocking task, so they share a single hop on and
    /// off the runtime: the header carries `confirmations`, and the tip height
    /// is what tells a side-branch block that has merely lost a race apart from
    /// one a competing chain has buried for good.
    pub async fn block_chain_position(
        self: &Arc<Self>,
        hash_hex: String,
    ) -> Result<BlockChainPosition, PoolError> {
        let this = self.clone();
        spawn_rpc("getblockheader", move || {
            this.block_chain_position_blocking(&hash_hex)
        })
        .await
    }

    fn block_chain_position_blocking(
        &self,
        hash_hex: &str,
    ) -> Result<BlockChainPosition, PoolError> {
        let hash = BlockHash::from_str(hash_hex)
            .map_err(|e| PoolError::Other(anyhow!("not a block hash: {hash_hex}: {e}")))?;

        self.call_with_refresh(|c| {
            let header = match c.get_block_header_info(&hash) {
                Ok(header) => header,
                // A hash the node has never seen is an answer, not a failure —
                // and not one a cookie refresh could fix, so it must not go
                // back as `Err` or `call_with_refresh` would rebuild the client
                // over it.
                Err(e) if is_block_not_found(&e) => return Ok(BlockChainPosition::Unknown),
                Err(e) => return Err(PoolError::Rpc(e)),
            };
            let tip_height = c.get_block_count().map_err(PoolError::Rpc)?;

            Ok(if header.confirmations < 0 {
                BlockChainPosition::SideBranch { tip_height }
            } else {
                BlockChainPosition::OnChain {
                    confirmations: header.confirmations as u32,
                    tip_height,
                }
            })
        })
    }

    pub async fn network_hashrate(
        self: &Arc<Self>,
        blocks: Option<u64>,
        height: Option<u64>,
    ) -> Result<f64, PoolError> {
        let this = self.clone();
        spawn_rpc("getnetworkhashps", move || {
            this.network_hashrate_blocking(blocks, height)
        })
        .await
    }

    fn network_hashrate_blocking(
        &self,
        blocks: Option<u64>,
        height: Option<u64>,
    ) -> Result<f64, PoolError> {
        self.call_with_refresh(|c| {
            c.get_network_hash_ps(blocks, height)
                .map_err(PoolError::Rpc)
        })
    }

    /// Estimate the difficulty change (percent, e.g. `+1.85`) at the next
    /// 2016-block retarget using accurate on-chain timestamps for the current
    /// epoch — not a hashrate proxy.
    ///
    /// The retarget keeps 2016 blocks at ~10 min each, so
    ///   new / old = target_timespan / projected_actual_timespan
    ///             = 600 × blocks_into_epoch / elapsed_seconds
    /// where `elapsed_seconds` is measured between the first block of the epoch
    /// and the chain tip. Clamped to the protocol's [-75%, +300%] limit.
    /// Returns `NaN` right after a retarget (no interval to measure yet).
    ///
    /// Five sequential round trips, all inside one blocking task so they share
    /// a single hop on and off the runtime.
    pub async fn estimate_difficulty_change_pct(self: &Arc<Self>) -> Result<f64, PoolError> {
        let this = self.clone();
        spawn_rpc("difficulty estimate", move || {
            this.estimate_difficulty_change_pct_blocking()
        })
        .await
    }

    fn estimate_difficulty_change_pct_blocking(&self) -> Result<f64, PoolError> {
        self.call_with_refresh(|c| {
            let height = c.get_block_count().map_err(PoolError::Rpc)?;
            let into_epoch = height % 2016;
            if into_epoch == 0 {
                return Ok(f64::NAN);
            }
            let epoch_start = height - into_epoch;

            let tip_hash = c.get_block_hash(height).map_err(PoolError::Rpc)?;
            let start_hash = c.get_block_hash(epoch_start).map_err(PoolError::Rpc)?;
            let tip_time = c.get_block_header(&tip_hash).map_err(PoolError::Rpc)?.time as i64;
            let start_time = c
                .get_block_header(&start_hash)
                .map_err(PoolError::Rpc)?
                .time as i64;

            let elapsed = (tip_time - start_time) as f64;
            if elapsed <= 0.0 {
                return Ok(f64::NAN);
            }
            let expected = into_epoch as f64 * 600.0;
            Ok(((expected / elapsed - 1.0) * 100.0).clamp(-75.0, 300.0))
        })
    }
}

/// Map a `submitblock` response to what the node actually did with the block.
///
/// Pure and side-effect free so the mapping is unit-testable: getting it wrong
/// is invisible in production until the block counts are already wrong.
///
/// Per BIP22, a `null` response means the block was accepted onto the tip.
/// Everything else is a status string; only `duplicate` is a success we can
/// claim, because it is our own earlier submission coming back. `inconclusive`
/// and `duplicate-inconclusive` both mean "valid, stored, not on the best
/// chain" — the block lost a same-height race and earned nothing.
fn classify_submit_response(result: &Value) -> Result<BlockSubmitOutcome, PoolError> {
    if result.is_null() {
        return Ok(BlockSubmitOutcome::Accepted);
    }

    match result.as_str() {
        Some("duplicate") => Ok(BlockSubmitOutcome::Duplicate),
        Some("inconclusive") | Some("duplicate-inconclusive") => {
            Ok(BlockSubmitOutcome::Inconclusive)
        }
        Some(reason) => Err(PoolError::SubmitBlockRejected(reason.to_owned())),
        None => Err(PoolError::Other(anyhow!(
            "unexpected submitblock response: {result}"
        ))),
    }
}

/// Whether an RPC failure is Core's `RPC_INVALID_ADDRESS_OR_KEY` (-5), which
/// `getblockheader` returns as "Block not found" for a hash the node does not
/// have. Matched on the code rather than the message, which is not stable.
fn is_block_not_found(e: &bitcoincore_rpc::Error) -> bool {
    matches!(
        e,
        bitcoincore_rpc::Error::JsonRpc(jsonrpc::Error::Rpc(rpc)) if rpc.code == -5
    )
}

/// Run one blocking RPC on the blocking pool. `what` names the call so a
/// panicking task is attributable in the log.
async fn spawn_rpc<T, F>(what: &'static str, f: F) -> Result<T, PoolError>
where
    F: FnOnce() -> Result<T, PoolError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| PoolError::Other(anyhow!("{what} RPC task panicked: {e}")))?
}

/// Build a client that honours `timeout_secs`.
///
/// `Client::new` builds the transport with jsonrpc's hardcoded 15 s default and
/// offers no way to override it, so the configured value went unused. It matters
/// now: a `spawn_blocking` task cannot be cancelled, so the transport timeout is
/// the only bound on how long a wedged node can hold a blocking-pool thread.
/// This mirrors what `Client::new` does — URL, then basic auth when a user is
/// present — with the timeout applied.
fn build_client(cfg: &RpcConfig, auth: Auth) -> Result<Client> {
    let (user, pass) = auth.get_user_pass()?;

    let mut builder = jsonrpc::simple_http::Builder::new()
        .url(&cfg.url)?
        .timeout(Duration::from_secs(cfg.timeout_secs));
    if let Some(user) = user {
        builder = builder.auth(user, pass);
    }

    Ok(Client::from_jsonrpc(jsonrpc::Client::with_transport(
        builder.build(),
    )))
}

fn parse_gbt_transaction(tx: &Value) -> Result<GbtTransaction, PoolError> {
    let data_hex = tx
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| PoolError::Other(anyhow!("GBT transaction missing data field")))?;

    let data = hex::decode(data_hex)
        .map_err(|e| PoolError::Other(anyhow!("GBT transaction data hex decode: {e}")))?;

    Ok(GbtTransaction {
        data,
        txid: value_as_string(tx, "txid")?,
        hash: tx
            .get("hash")
            .and_then(Value::as_str)
            .or_else(|| tx.get("wtxid").and_then(Value::as_str))
            .unwrap_or_default()
            .to_owned(),
        fee: value_as_u64(tx, "fee")?,
        weight: value_as_u64(tx, "weight")?,
    })
}

fn value_as_string(v: &Value, key: &str) -> Result<String, PoolError> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            PoolError::Other(anyhow!("missing or invalid getblocktemplate field: {key}"))
        })
}

fn value_as_u64(v: &Value, key: &str) -> Result<u64, PoolError> {
    v.get(key).and_then(Value::as_u64).ok_or_else(|| {
        PoolError::Other(anyhow!("missing or invalid getblocktemplate field: {key}"))
    })
}

fn value_as_u32(v: &Value, key: &str) -> Result<u32, PoolError> {
    let n = value_as_u64(v, key)?;
    u32::try_from(n).map_err(|_| {
        PoolError::Other(anyhow!(
            "getblocktemplate field out of range for u32: {key}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_responses_map_to_their_outcomes() {
        for (response, expected) in [
            (Value::Null, BlockSubmitOutcome::Accepted),
            (json!("duplicate"), BlockSubmitOutcome::Duplicate),
            (json!("inconclusive"), BlockSubmitOutcome::Inconclusive),
            (
                json!("duplicate-inconclusive"),
                BlockSubmitOutcome::Inconclusive,
            ),
        ] {
            assert_eq!(
                classify_submit_response(&response).unwrap(),
                expected,
                "submitblock response {response:?}"
            );
        }
    }

    #[test]
    fn a_rejection_reason_is_an_error_carrying_that_reason() {
        let err = classify_submit_response(&json!("bad-txns-inputs-missingorspent")).unwrap_err();
        assert!(matches!(
            err,
            PoolError::SubmitBlockRejected(ref r) if r == "bad-txns-inputs-missingorspent"
        ));
        // `duplicate-invalid` is a rejection, not a duplicate: the node has the
        // block and has decided it is bad.
        assert!(matches!(
            classify_submit_response(&json!("duplicate-invalid")).unwrap_err(),
            PoolError::SubmitBlockRejected(_)
        ));
    }

    #[test]
    fn a_non_string_response_is_not_silently_treated_as_success() {
        assert!(matches!(
            classify_submit_response(&json!(42)).unwrap_err(),
            PoolError::Other(_)
        ));
    }

    /// A block that lost a same-height race is consensus-valid but earned
    /// nothing, so it must not move `pool_blocks_found_total`.
    #[test]
    fn only_a_block_on_the_best_chain_counts_as_a_find() {
        assert!(BlockSubmitOutcome::Accepted.is_win());
        assert!(BlockSubmitOutcome::Duplicate.is_win());
        assert!(!BlockSubmitOutcome::Inconclusive.is_win());
    }

    #[test]
    fn outcome_labels_are_stable() {
        assert_eq!(BlockSubmitOutcome::Accepted.label(), "accepted");
        assert_eq!(BlockSubmitOutcome::Duplicate.label(), "duplicate");
        assert_eq!(BlockSubmitOutcome::Inconclusive.label(), "inconclusive");
    }
}
