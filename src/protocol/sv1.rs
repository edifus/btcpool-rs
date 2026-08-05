/// protocol/sv1.rs
///
/// Stratum V1 message types, parser, and response builders.
///
/// Supports all ASIC-relevant extensions:
///   - mining.subscribe (with optional client/version)
///   - mining.authorize
///   - mining.notify
///   - mining.set_difficulty
///   - mining.set_extranonce  (subscribe-extranonce extension)
///   - mining.configure       (stratum-extensions: version-rolling, minimum-difficulty)
///   - mining.submit          (with version_bits for version-rolling)
use crate::error::PoolError;
use serde::Deserialize;
use serde_json::Value;

// ─────────────────────────────────────────────────────────────────────────────
// Inbound message
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct StratumRequest {
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

impl StratumRequest {
    pub fn parse(line: &str) -> Result<Self, PoolError> {
        serde_json::from_str(line).map_err(|e| PoolError::Parse(e.to_string()))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Typed inbound methods
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ClientMessage {
    Subscribe(SubscribeParams),
    Authorize(AuthorizeParams),
    Submit(SubmitParams),
    Configure(ConfigureParams),
    SuggestDifficulty(SuggestDifficultyParams),
    /// Any unrecognised method — we return an error
    Unknown(String),
}

impl ClientMessage {
    pub fn from_request(req: &StratumRequest) -> Result<Self, PoolError> {
        match req.method.as_str() {
            "mining.subscribe" => Ok(Self::Subscribe(SubscribeParams::parse(&req.params)?)),
            "mining.authorize" => Ok(Self::Authorize(AuthorizeParams::parse(&req.params)?)),
            "mining.submit" => Ok(Self::Submit(SubmitParams::parse(&req.params)?)),
            "mining.configure" => Ok(Self::Configure(ConfigureParams::parse(&req.params)?)),
            "mining.suggest_difficulty" => Ok(Self::SuggestDifficulty(
                SuggestDifficultyParams::parse(&req.params)?,
            )),
            other => Ok(Self::Unknown(other.to_string())),
        }
    }
}

// ── mining.subscribe ─────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct SubscribeParams {
    /// Mining software user agent, e.g. "cgminer/4.10.0"
    pub user_agent: Option<String>,
    /// Existing session ID (used by subscribe-extranonce extension)
    #[allow(dead_code)]
    pub session_id: Option<String>,
}

impl SubscribeParams {
    fn parse(params: &Value) -> Result<Self, PoolError> {
        let arr = params.as_array();
        Ok(Self {
            user_agent: arr
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .map(String::from),
            session_id: arr
                .and_then(|a| a.get(1))
                .and_then(|v| v.as_str())
                .map(String::from),
        })
    }
}

// ── mining.authorize ─────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct AuthorizeParams {
    pub worker: String,
    #[allow(dead_code)]
    pub password: Option<String>,
}

impl AuthorizeParams {
    fn parse(params: &Value) -> Result<Self, PoolError> {
        let arr = params.as_array().ok_or_else(|| PoolError::InvalidParams {
            method: "mining.authorize",
            detail: "params must be an array".into(),
        })?;
        let worker = arr
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| PoolError::InvalidParams {
                method: "mining.authorize",
                detail: "missing worker name".into(),
            })?
            .to_string();
        let password = arr.get(1).and_then(|v| v.as_str()).map(String::from);
        Ok(Self { worker, password })
    }
}

// ── mining.submit ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct SubmitParams {
    #[allow(dead_code)]
    pub worker: String,
    pub job_id: String,
    pub extranonce2: Vec<u8>,
    pub ntime: u32,
    pub nonce: u32,
    /// BIP320 version bits (only present when version-rolling is active)
    pub version_bits: Option<u32>,
}

impl SubmitParams {
    fn parse(params: &Value) -> Result<Self, PoolError> {
        let arr = params.as_array().ok_or_else(|| PoolError::InvalidParams {
            method: "mining.submit",
            detail: "params must be an array".into(),
        })?;

        let worker = str_at(arr, 0, "mining.submit", "worker")?;
        let job_id = str_at(arr, 1, "mining.submit", "job_id")?;
        let en2_hex = str_at(arr, 2, "mining.submit", "extranonce2")?;
        let ntime_hex = str_at(arr, 3, "mining.submit", "ntime")?;
        let nonce_hex = str_at(arr, 4, "mining.submit", "nonce")?;

        let extranonce2 = hex::decode(&en2_hex).map_err(|_| PoolError::InvalidParams {
            method: "mining.submit",
            detail: "extranonce2 is not valid hex".into(),
        })?;

        let ntime = u32::from_str_radix(&ntime_hex, 16).map_err(|_| PoolError::InvalidParams {
            method: "mining.submit",
            detail: "ntime is not valid hex u32".into(),
        })?;

        let nonce = u32::from_str_radix(&nonce_hex, 16).map_err(|_| PoolError::InvalidParams {
            method: "mining.submit",
            detail: "nonce is not valid hex u32".into(),
        })?;

        // Optional 6th param: version bits (BIP320)
        let version_bits = arr
            .get(5)
            .and_then(|v| v.as_str())
            .and_then(|s| u32::from_str_radix(s, 16).ok());

        Ok(Self {
            worker,
            job_id,
            extranonce2,
            ntime,
            nonce,
            version_bits,
        })
    }
}

// ── mining.configure ─────────────────────────────────────────────────────────

/// The `mining.configure` method carries a list of extensions the miner wants
/// to enable, plus per-extension parameters.
#[derive(Debug, Default)]
pub struct ConfigureParams {
    pub version_rolling: bool,
    /// Miner's preferred mask — we AND this with our own mask
    pub version_rolling_mask: Option<u32>,
    pub version_rolling_min_bit_count: Option<u32>,
    pub minimum_difficulty: Option<u64>,
    pub subscribe_extranonce: bool,
}

impl ConfigureParams {
    fn parse(params: &Value) -> Result<Self, PoolError> {
        let mut result = Self::default();

        let arr = params.as_array();
        let extensions = arr
            .and_then(|a| a.first())
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let ext_params = arr
            .and_then(|a| a.get(1))
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();

        for ext in &extensions {
            match ext.as_str() {
                Some("version-rolling") => {
                    result.version_rolling = true;
                    if let Some(mask_str) = ext_params
                        .get("version-rolling.mask")
                        .and_then(|v| v.as_str())
                    {
                        result.version_rolling_mask = u32::from_str_radix(mask_str, 16).ok();
                    }
                    if let Some(min_bits) = ext_params
                        .get("version-rolling.min-bit-count")
                        .and_then(|v| v.as_u64())
                    {
                        result.version_rolling_min_bit_count = Some(min_bits as u32);
                    }
                }
                Some("minimum-difficulty") => {
                    result.minimum_difficulty = ext_params
                        .get("minimum-difficulty.value")
                        .and_then(|v| v.as_u64());
                }
                Some("subscribe-extranonce") => {
                    result.subscribe_extranonce = true;
                }
                _ => {}
            }
        }

        Ok(result)
    }
}

// ── mining.suggest_difficulty ─────────────────────────────────────────────────

/// The miner's requested *starting* share difficulty. Advisory — the pool
/// applies it clamped to the configured vardiff floor/ceiling and lets vardiff
/// take over from there.
#[derive(Debug)]
pub struct SuggestDifficultyParams {
    pub difficulty: u64,
}

impl SuggestDifficultyParams {
    fn parse(params: &Value) -> Result<Self, PoolError> {
        // Accept both `[difficulty]` and a bare number; difficulty may be sent
        // as an integer or float depending on firmware.
        let d = params
            .as_array()
            .and_then(|a| a.first())
            .or(Some(params))
            .and_then(|v| v.as_f64())
            .ok_or_else(|| PoolError::InvalidParams {
                method: "mining.suggest_difficulty",
                detail: "expected a numeric difficulty".into(),
            })?;
        if !d.is_finite() || d < 1.0 {
            return Err(PoolError::InvalidParams {
                method: "mining.suggest_difficulty",
                detail: "difficulty must be a finite number >= 1".into(),
            });
        }
        Ok(Self {
            difficulty: d as u64,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Outbound message builders
// ─────────────────────────────────────────────────────────────────────────────

pub struct ResponseBuilder;

impl ResponseBuilder {
    pub fn ok(id: &Value, result: Value) -> String {
        serde_json::json!({ "id": id, "result": result, "error": null }).to_string()
    }

    pub fn err(id: &Value, error: Value) -> String {
        serde_json::json!({ "id": id, "result": null, "error": error }).to_string()
    }

    /// mining.subscribe response
    pub fn subscribe(
        id: &Value,
        session_id: &str,
        extranonce1_hex: &str,
        extranonce2_size: usize,
    ) -> String {
        Self::ok(
            id,
            serde_json::json!([
                [
                    ["mining.set_difficulty", session_id],
                    ["mining.notify", session_id]
                ],
                extranonce1_hex,
                extranonce2_size
            ]),
        )
    }

    /// mining.configure response
    ///
    /// We advertise support for version-rolling with our mask, and echo back
    /// the negotiated minimum difficulty.
    pub fn configure(
        id: &Value,
        version_rolling: bool,
        our_mask: u32,
        min_difficulty: Option<u64>,
        subscribe_extranonce: bool,
    ) -> String {
        let mut result = serde_json::Map::new();

        if version_rolling {
            result.insert("version-rolling".into(), Value::Bool(true));
            result.insert(
                "version-rolling.mask".into(),
                Value::String(format!("{:08x}", our_mask)),
            );
        }

        if let Some(md) = min_difficulty {
            result.insert("minimum-difficulty".into(), Value::Bool(true));
            result.insert("minimum-difficulty.value".into(), Value::Number(md.into()));
        }

        if subscribe_extranonce {
            result.insert("subscribe-extranonce".into(), Value::Bool(true));
        }

        Self::ok(id, Value::Object(result))
    }

    /// mining.set_difficulty notification (server → miner, no id)
    pub fn set_difficulty(difficulty: u64) -> String {
        serde_json::json!({
            "id": null,
            "method": "mining.set_difficulty",
            "params": [difficulty]
        })
        .to_string()
    }

    /// mining.notify
    #[allow(clippy::too_many_arguments)]
    pub fn notify(
        job_id: &str,
        prev_hash: &str,
        coinbase1: &str,
        coinbase2: &str,
        merkle_branch: &[String],
        version: u32,
        bits: &str,
        ntime: u32,
        clean_jobs: bool,
    ) -> String {
        serde_json::json!({
            "id": null,
            "method": "mining.notify",
            "params": [
                job_id,
                prev_hash,
                coinbase1,
                coinbase2,
                merkle_branch,
                format!("{:08x}", version),
                bits,
                format!("{:08x}", ntime),
                clean_jobs
            ]
        })
        .to_string()
    }

    /// mining.set_extranonce (subscribe-extranonce extension)
    #[allow(dead_code)]
    pub fn set_extranonce(extranonce1_hex: &str, extranonce2_size: usize) -> String {
        serde_json::json!({
            "id": null,
            "method": "mining.set_extranonce",
            "params": [extranonce1_hex, extranonce2_size]
        })
        .to_string()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

fn str_at(
    arr: &[Value],
    idx: usize,
    method: &'static str,
    field: &'static str,
) -> Result<String, PoolError> {
    arr.get(idx)
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| PoolError::InvalidParams {
            method,
            detail: format!("missing or non-string field `{field}` at index {idx}"),
        })
}

#[cfg(test)]
mod tests {
    use super::{ClientMessage, StratumRequest};

    fn parse_msg(line: &str) -> ClientMessage {
        let req = StratumRequest::parse(line).expect("valid json-rpc");
        ClientMessage::from_request(&req).expect("known method")
    }

    #[test]
    fn suggest_difficulty_parses_array_int_and_float() {
        for line in [
            r#"{"id":1,"method":"mining.suggest_difficulty","params":[8192]}"#,
            r#"{"id":1,"method":"mining.suggest_difficulty","params":[8192.0]}"#,
        ] {
            match parse_msg(line) {
                ClientMessage::SuggestDifficulty(p) => assert_eq!(p.difficulty, 8192),
                other => panic!("expected SuggestDifficulty, got {other:?}"),
            }
        }
    }

    #[test]
    fn suggest_difficulty_rejects_garbage() {
        for line in [
            r#"{"id":1,"method":"mining.suggest_difficulty","params":[0]}"#,
            r#"{"id":1,"method":"mining.suggest_difficulty","params":["nope"]}"#,
            r#"{"id":1,"method":"mining.suggest_difficulty","params":[]}"#,
        ] {
            let req = StratumRequest::parse(line).expect("valid json-rpc");
            assert!(
                ClientMessage::from_request(&req).is_err(),
                "should reject: {line}"
            );
        }
    }
}
