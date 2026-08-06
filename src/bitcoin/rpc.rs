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
use bitcoincore_rpc::{Client, RpcApi};
use serde_json::{json, Value};
use std::sync::{Arc, RwLock};
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
        let client = Client::new(&cfg.url, auth)?;
        info!("Bitcoin RPC connected to {}", cfg.url);
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
        let new_client = Client::new(
            &self.cfg.url,
            bitcoincore_rpc::Auth::UserPass(fresh_cookie.0.clone(), fresh_cookie.1.clone()),
        )
        .map_err(PoolError::Rpc)?;

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
        })
    }

    /// Submit a found block. Takes an `Arc<String>` so the retry ladder in
    /// `TemplateEngine` can hand the same bytes to successive attempts without
    /// re-allocating them for each blocking task.
    pub async fn submit_block(self: &Arc<Self>, block_hex: Arc<String>) -> Result<(), PoolError> {
        let this = self.clone();
        spawn_rpc("submitblock", move || {
            this.submit_block_blocking(&block_hex)
        })
        .await
    }

    fn submit_block_blocking(&self, block_hex: &str) -> Result<(), PoolError> {
        let result: Value = self.call_with_refresh(|c| {
            c.call("submitblock", &[json!(block_hex)])
                .map_err(PoolError::Rpc)
        })?;

        if result.is_null() {
            info!("🎉 Block accepted by network!");
            return Ok(());
        }

        match result.as_str() {
            // The node already has this block — an earlier (possibly retried)
            // submission went through. Success, not an error.
            Some("duplicate") | Some("duplicate-inconclusive") => {
                info!("submitblock: node already has this block (duplicate)");
                Ok(())
            }
            // Valid block that did not become the chain tip (lost a same-height
            // race). It was accepted and stored — resubmitting cannot help.
            Some("inconclusive") => {
                warn!("submitblock: block valid but not on the best chain (inconclusive)");
                Ok(())
            }
            Some(reason) => {
                warn!("submitblock rejected: {reason}");
                Err(PoolError::SubmitBlockRejected(reason.to_owned()))
            }
            None => Err(PoolError::Other(anyhow!(
                "unexpected submitblock response: {result}"
            ))),
        }
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
