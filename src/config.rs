use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

// ─────────────────────────────────────────────────────────────────────────────
// Top-level config
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub pool: PoolConfig,
    pub bitcoin_rpc: RpcConfig,
    pub zmq: ZmqConfig,
    pub vardiff: VardiffConfig,
    pub security: SecurityConfig,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
    /// Stratum V2 settings. Optional — defaults to enabled if the section is absent.
    #[serde(default)]
    pub sv2: Sv2Config,
}

// ─────────────────────────────────────────────────────────────────────────────
// Pool
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct PoolConfig {
    pub listen_addr: String,
    /// Retained only to produce a clear migration error for pre-username configs.
    #[serde(default)]
    pub coinbase_address: Option<String>,
    pub coinbase_tag: String,
    pub initial_difficulty: u64,
    pub extranonce1_size: usize,
    pub extranonce2_size: usize,
    pub max_connections: usize,
    pub idle_timeout_secs: u64,
    /// Directory where the raw hex of every block this pool finds is archived
    /// before submission, so a failed `submitblock` can be retried or replayed
    /// by hand (`bitcoin-cli submitblock "$(cat <file>)"`). Relative paths
    /// resolve against the service working directory, like `stats_db_path`.
    #[serde(default = "default_found_block_dir")]
    pub found_block_dir: String,
    /// Confirmations before a found block is treated as final.
    ///
    /// `submitblock`'s verdict is only true at the instant it is read, so every
    /// block is re-checked with `getblockheader` until it is this deep on the
    /// active chain (confirmed) or this deep on a branch that lost (reorged
    /// out). The same number is used in both directions, and the second is why
    /// it should not be 1: a block one deep on a side branch is a routine reorg
    /// that usually resolves in our favour.
    ///
    /// 6 is the conventional finality threshold and takes about an hour. 100 is
    /// coinbase maturity — the depth at which the reward is actually spendable
    /// — at the cost of holding each block pending for ~17 hours.
    #[serde(default = "default_confirmation_depth")]
    pub confirmation_depth: u32,
    /// Optional safety assertion. The pool always detects the actual network
    /// from the connected node (`getblockchaininfo`) and validates the payout
    /// address against it. When this is set ("mainnet" | "testnet" | "testnet4" |
    /// "signet" | "regtest"), boot additionally fails fast if the node reports a
    /// different chain — catching a config pointed at the wrong node.
    #[serde(default)]
    pub network: Option<String>,
    /// Stop issuing work when `getblocktemplate` announces a `!`-prefixed rule
    /// this build does not implement.
    ///
    /// The pool does not mine the node's block — it discards Core's coinbase and
    /// builds its own. BIP22/23 marks a rule with `!` precisely to say that a
    /// client which does that must understand the rule, and Core reports such a
    /// rule without erroring, leaving the call to the client. So at a soft-fork
    /// activation this binary predates, the choice is: stop, or keep building
    /// coinbases against rules it has never heard of.
    ///
    /// Default `true` — stop. For a solo pool a silently-invalid block is the
    /// worst available outcome: it costs the whole reward and looks exactly like
    /// bad luck. Set `false` to keep mining and rely on the
    /// `pool_unsupported_gbt_rules` metric and the dashboard banner instead,
    /// which is the right call once you have read the new rule and satisfied
    /// yourself the coinbase this pool builds still complies.
    #[serde(default = "default_strict_gbt_rules")]
    pub strict_gbt_rules: bool,
}

fn default_found_block_dir() -> String {
    "found-blocks".into()
}

fn default_confirmation_depth() -> u32 {
    6
}

fn default_strict_gbt_rules() -> bool {
    true
}

// ─────────────────────────────────────────────────────────────────────────────
// Stratum V2
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct Sv2Config {
    /// Accept Stratum V2 (Extended Channel, Noise-encrypted) connections on the
    /// same listen port as SV1. The protocol is auto-detected from the first
    /// byte of each connection ('{' → SV1 JSON, otherwise → SV2 Noise
    /// handshake). When false, the pool rejects SV2 and only serves SV1.
    pub enabled: bool,
    /// Persist the Noise authority key across restarts so miners can pin the
    /// pool's identity (configure the pool's authority public key on the
    /// miner). The key file is created on first start. When false, a fresh
    /// authority key is generated each start; miners that pin the pool
    /// identity will refuse to connect after every restart.
    #[serde(default = "default_persist_authority_key")]
    pub persist_authority_key: bool,
    /// Path of the authority secret key file (base58check, one line). Created
    /// with owner-only permissions on first start when `persist_authority_key`
    /// is true. Relative paths resolve against the service working directory,
    /// like `stats_db_path`. Supports `~` expansion.
    #[serde(default = "default_authority_key_file")]
    pub authority_key_file: String,
    /// Validity window (seconds) of the certificate signed per handshake:
    /// `valid_from = now`, `not_valid_after = now + cert_validity_secs`.
    /// Miners that verify pool identity check this window against their own
    /// clock, so short values expose device clock skew. Defaults to one year.
    #[serde(default = "default_cert_validity_secs")]
    pub cert_validity_secs: u32,
}

fn default_persist_authority_key() -> bool {
    true
}

fn default_authority_key_file() -> String {
    "sv2-authority.key".into()
}

fn default_cert_validity_secs() -> u32 {
    365 * 24 * 60 * 60
}

impl Default for Sv2Config {
    fn default() -> Self {
        Self {
            enabled: true,
            persist_authority_key: default_persist_authority_key(),
            authority_key_file: default_authority_key_file(),
            cert_validity_secs: default_cert_validity_secs(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Bitcoin RPC — cookie-file auth
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct RpcConfig {
    pub url: String,
    /// Path to the .cookie file generated by Bitcoin Knots / Core.
    /// Supports `~` expansion.
    pub cookie_path: Option<String>,
    /// Explicit credentials (used only when cookie_path is absent / unreadable)
    pub user: Option<String>,
    pub password: Option<String>,
    /// Transport timeout for every RPC round trip. Because the calls run on the
    /// blocking pool, where a task cannot be cancelled, this is what bounds how
    /// long an unresponsive node holds a thread.
    pub timeout_secs: u64,
}

impl RpcConfig {
    /// Resolve the cookie path (expanding `~`) and read its contents.
    /// Returns `(user, password)` parsed from the `user:password` file.
    pub fn read_cookie(&self) -> Result<(String, String)> {
        let raw = self.cookie_path.as_deref().unwrap_or("~/.bitcoin/.cookie");

        let expanded = expand_tilde(raw);
        let contents = std::fs::read_to_string(&expanded)
            .with_context(|| format!("Reading cookie file: {}", expanded.display()))?;

        let (u, p) = contents
            .trim()
            .split_once(':')
            .context("Cookie file format should be `user:password`")?;

        Ok((u.to_owned(), p.to_owned()))
    }

    /// Return bitcoincore_rpc::Auth, preferring cookie file over explicit creds.
    pub fn rpc_auth(&self) -> Result<bitcoincore_rpc::Auth> {
        // Try cookie first
        if let Ok((u, p)) = self.read_cookie() {
            return Ok(bitcoincore_rpc::Auth::UserPass(u, p));
        }
        // Fall back to explicit credentials
        match (&self.user, &self.password) {
            (Some(u), Some(p)) => Ok(bitcoincore_rpc::Auth::UserPass(u.clone(), p.clone())),
            _ => anyhow::bail!(
                "No RPC credentials available. \
                 Provide a readable cookie_path or explicit user/password."
            ),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ZMQ
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct ZmqConfig {
    pub hashblock_endpoint: String,
    pub poll_fallback: bool,
    pub poll_interval_ms: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Vardiff
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct VardiffConfig {
    pub target_share_time_secs: u64,
    pub retarget_interval_secs: u64,
    pub min_difficulty: u64,
    pub max_difficulty: u64,
    pub max_retarget_factor: f64,
    /// Lower edge of the deadzone, as a multiple of the assigned difficulty:
    /// nothing is sent until the computed optimum falls to or below it.
    #[serde(default = "default_deadzone_low")]
    pub deadzone_low: f64,
    /// Upper edge of the deadzone. Widening the band leaves difficulty still
    /// for longer at the cost of sitting further from the target share time.
    #[serde(default = "default_deadzone_high")]
    pub deadzone_high: f64,
}

fn default_deadzone_low() -> f64 {
    0.667
}

fn default_deadzone_high() -> f64 {
    1.5
}

// ─────────────────────────────────────────────────────────────────────────────
// Security
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct SecurityConfig {
    pub max_connections_per_ip: u32,
    pub max_shares_per_sec: u32,
    pub ban_duration_secs: u64,
    pub max_invalid_shares: u32,
    pub max_message_bytes: usize,
    /// Maximum byte length of an accepted worker/user-identity name. Bounds the
    /// global stats maps and Prometheus label cardinality against untrusted
    /// names. Defaults to 128 (fits a bech32m address + `.worker` suffix).
    #[serde(default = "default_max_worker_name_len")]
    pub max_worker_name_len: usize,
    /// Maximum number of *distinct* worker identities one connection may
    /// authorize (SV1 `mining.authorize` / SV2 `OpenExtendedMiningChannel`).
    /// Each distinct name creates entries in the global stats maps and mints
    /// Prometheus label series, so an unbounded count lets a single connection
    /// grow them without limit. Real miners authorize once. Defaults to 8.
    #[serde(default = "default_max_authorizations_per_session")]
    pub max_authorizations_per_session: u32,
}

fn default_max_worker_name_len() -> usize {
    128
}

fn default_max_authorizations_per_session() -> u32 {
    8
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    pub prometheus_addr: String,
    /// Optional SQLite path to persist all-time stats between restarts.
    /// If omitted or empty, persistence is disabled.
    pub stats_db_path: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Logging
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    pub level: String,
    pub json: bool,
    /// Directory for rotating log files (e.g., "/var/log/btcpool-rs/"). Logs
    /// always go to stdout; this adds a file copy alongside them. Empty or
    /// absent disables file logging. Supports `~` expansion.
    pub log_dir: Option<String>,
}

impl LoggingConfig {
    /// Resolved log directory, or `None` when file logging is off. Empty and
    /// whitespace-only values are off, the same as an absent key — `Option`
    /// alone does not cover that, since `""` deserializes to `Some("")`.
    pub fn log_dir_path(&self) -> Option<PathBuf> {
        self.log_dir
            .as_deref()
            .map(str::trim)
            .filter(|dir| !dir.is_empty())
            .map(expand_tilde)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Load helpers
// ─────────────────────────────────────────────────────────────────────────────

impl Config {
    /// Reject configurations that would later panic or misbehave at runtime.
    fn validate(&self) -> Result<()> {
        if self.pool.coinbase_address.is_some() {
            anyhow::bail!(
                "[pool] coinbase_address is no longer used. Remove it and configure miners as \
                 BITCOIN_ADDRESS.worker"
            );
        }
        // SV2 `OpenExtendedMiningChannel` derives `prefix_len` as
        // `extranonce_total - granted` (granted >= 1). A zero total underflows
        // that `usize` subtraction, so refuse it at boot rather than per-channel.
        if self.pool.extranonce1_size + self.pool.extranonce2_size == 0 {
            anyhow::bail!(
                "[pool] extranonce1_size + extranonce2_size must be >= 1 (got 0): \
                 a zero total extranonce width underflows SV2 channel setup"
            );
        }
        // Consensus caps the coinbase scriptSig at 100 bytes (`bad-cb-length`),
        // and everything in it is configured here. Check the worst case — the
        // widest BIP34 height push — so the pool cannot run for months and then
        // have its one found block rejected.
        let widest_height_push = 5; // 1 length byte + up to 4 bytes of height
        let script_sig_len = widest_height_push
            + self.pool.coinbase_tag.len()
            + self.pool.extranonce1_size
            + self.pool.extranonce2_size;
        crate::bitcoin::template::check_coinbase_script_sig_len(script_sig_len).map_err(|e| {
            anyhow::anyhow!(
                "{e}; shorten [pool] coinbase_tag ({} bytes) or the extranonce sizes ({} + {})",
                self.pool.coinbase_tag.len(),
                self.pool.extranonce1_size,
                self.pool.extranonce2_size,
            )
        })?;

        // Zero would resolve every block the instant it was probed, including
        // one sitting on a side branch mid-reorg; above coinbase maturity there
        // is nothing left to learn, since the reward is spendable by then.
        if !(1..=100).contains(&self.pool.confirmation_depth) {
            anyhow::bail!(
                "[pool] confirmation_depth must be between 1 and 100 (got {}); \
                 6 is the conventional finality threshold",
                self.pool.confirmation_depth
            );
        }

        // Vardiff divides by the target and clamps between reciprocals of the
        // retarget factor, so these are the values that would panic or drive
        // every session to the floor rather than merely tune it badly.
        let vardiff = &self.vardiff;
        if vardiff.target_share_time_secs == 0 {
            anyhow::bail!("[vardiff] target_share_time_secs must be >= 1 (got 0)");
        }
        if vardiff.min_difficulty == 0 {
            anyhow::bail!("[vardiff] min_difficulty must be >= 1 (got 0)");
        }
        if vardiff.min_difficulty > vardiff.max_difficulty {
            anyhow::bail!(
                "[vardiff] min_difficulty ({}) must not exceed max_difficulty ({})",
                vardiff.min_difficulty,
                vardiff.max_difficulty
            );
        }
        if !vardiff.max_retarget_factor.is_finite() || vardiff.max_retarget_factor <= 1.0 {
            anyhow::bail!(
                "[vardiff] max_retarget_factor must be > 1.0 (got {}); \
                 it bounds a difficulty change to that ratio in either direction",
                vardiff.max_retarget_factor
            );
        }
        if !vardiff.deadzone_low.is_finite()
            || vardiff.deadzone_low <= 0.0
            || vardiff.deadzone_low >= 1.0
        {
            anyhow::bail!(
                "[vardiff] deadzone_low must be between 0.0 and 1.0 (got {}); \
                 it is the fraction of the assigned difficulty the optimum has to \
                 fall to before the difficulty is lowered",
                vardiff.deadzone_low
            );
        }
        if !vardiff.deadzone_high.is_finite() || vardiff.deadzone_high <= 1.0 {
            anyhow::bail!(
                "[vardiff] deadzone_high must be > 1.0 (got {}); \
                 it is the multiple of the assigned difficulty the optimum has to \
                 reach before the difficulty is raised",
                vardiff.deadzone_high
            );
        }

        if self.sv2.enabled
            && self.sv2.persist_authority_key
            && self.sv2.authority_key_file.trim().is_empty()
        {
            anyhow::bail!(
                "[sv2] authority_key_file must not be empty while \
                 persist_authority_key = true"
            );
        }
        Ok(())
    }
}

pub fn load(path: &str) -> Result<Config> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("Opening config file: {path}"))?;
    let mut value: toml::Value = toml::from_str(&raw).context("Parsing config TOML")?;
    apply_env_overrides(&mut value, std::env::vars())?;
    let config: Config = value.try_into().context("Interpreting config")?;
    config.validate()?;
    Ok(config)
}

/// Environment-variable prefix for config overrides.
const ENV_PREFIX: &str = "BTCPOOL_";

/// Apply `BTCPOOL_<SECTION>__<KEY>=value` environment overrides onto the
/// parsed TOML before it is deserialized into [`Config`]. The double
/// underscore separates section from key (both contain single underscores),
/// e.g. `BTCPOOL_BITCOIN_RPC__URL` → `[bitcoin_rpc] url`.
///
/// Container platforms (Umbrel, Start9, plain Compose) configure apps through
/// environment variables; this lets a stock config file ship in the image with
/// the deployment-specific values injected at runtime.
///
/// Typing: when the key exists in the file, the override is parsed as that
/// value's type (and load fails loudly if it cannot be). When the key is
/// absent, `true`/`false` become booleans and numbers become numbers — wrap
/// the value in double quotes to force a string (e.g. an all-numeric RPC
/// password).
fn apply_env_overrides(
    value: &mut toml::Value,
    vars: impl Iterator<Item = (String, String)>,
) -> Result<()> {
    let root = value
        .as_table_mut()
        .context("Config root is not a TOML table")?;

    for (name, raw) in vars {
        let Some(rest) = name.strip_prefix(ENV_PREFIX) else {
            continue;
        };
        let Some((section, key)) = rest.split_once("__") else {
            anyhow::bail!(
                "{name}: expected {ENV_PREFIX}<SECTION>__<KEY> \
                 (double underscore between section and key)"
            );
        };
        let (section, key) = (section.to_ascii_lowercase(), key.to_ascii_lowercase());
        if section.is_empty() || key.is_empty() {
            anyhow::bail!("{name}: empty section or key");
        }

        let table = root
            .entry(section.clone())
            .or_insert_with(|| toml::Value::Table(Default::default()))
            .as_table_mut()
            .with_context(|| format!("[{section}] is not a table"))?;

        let parse_err =
            || format!("{name}: value does not parse as the type of {section}.{key} in the file");
        let parsed = match table.get(&key) {
            Some(toml::Value::Integer(_)) => {
                toml::Value::Integer(raw.parse().with_context(parse_err)?)
            }
            Some(toml::Value::Float(_)) => toml::Value::Float(raw.parse().with_context(parse_err)?),
            Some(toml::Value::Boolean(_)) => {
                toml::Value::Boolean(raw.parse().with_context(parse_err)?)
            }
            Some(toml::Value::String(_)) => toml::Value::String(raw),
            Some(_) => anyhow::bail!("{name}: cannot override non-scalar {section}.{key}"),
            None => infer_toml_scalar(raw),
        };
        // Names only — values may be credentials. stderr because config loads
        // before the tracing subscriber exists; systemd and Docker capture it.
        eprintln!("Config override from environment: {section}.{key}");
        table.insert(key, parsed);
    }
    Ok(())
}

/// Best-effort scalar typing for keys not present in the config file.
/// Surrounding double quotes force a string.
fn infer_toml_scalar(raw: String) -> toml::Value {
    if let Some(quoted) = raw
        .strip_prefix('"')
        .and_then(|r| r.strip_suffix('"'))
        .filter(|_| raw.len() >= 2)
    {
        return toml::Value::String(quoted.to_string());
    }
    if raw == "true" || raw == "false" {
        return toml::Value::Boolean(raw == "true");
    }
    if let Ok(i) = raw.parse::<i64>() {
        return toml::Value::Integer(i);
    }
    if let Ok(f) = raw.parse::<f64>() {
        return toml::Value::Float(f);
    }
    toml::Value::String(raw)
}

/// Expand a leading `~` to the home directory.
pub(crate) fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return Path::new(&home).join(rest);
        }
    }
    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::{apply_env_overrides, Config, PoolConfig};
    use std::path::PathBuf;

    /// A complete, valid config; tests override single values on top of it.
    fn config_toml(coinbase_tag: &str, extranonce1: usize, extranonce2: usize) -> String {
        format!(
            r#"
[pool]
listen_addr = "127.0.0.1:3333"
coinbase_tag = "{coinbase_tag}"
initial_difficulty = 2048
extranonce1_size = {extranonce1}
extranonce2_size = {extranonce2}
max_connections = 16
idle_timeout_secs = 300

[bitcoin_rpc]
url = "http://127.0.0.1:8332"
timeout_secs = 10

[zmq]
hashblock_endpoint = "tcp://127.0.0.1:28332"
poll_fallback = true
poll_interval_ms = 1000

[vardiff]
target_share_time_secs = 5
retarget_interval_secs = 100
min_difficulty = 256
max_difficulty = 4000000
max_retarget_factor = 10.0

[security]
max_connections_per_ip = 5
max_shares_per_sec = 500
ban_duration_secs = 600
max_invalid_shares = 5
max_message_bytes = 4096

[metrics]
prometheus_addr = "127.0.0.1:9090"

[logging]
level = "info"
json = false
"#
        )
    }

    fn validate(coinbase_tag: &str, extranonce1: usize, extranonce2: usize) -> anyhow::Result<()> {
        let config: Config =
            toml::from_str(&config_toml(coinbase_tag, extranonce1, extranonce2)).unwrap();
        config.validate()
    }

    /// Consensus caps the coinbase scriptSig at 100 bytes. Everything in it is
    /// configured here, so an over-long tag has to fail at boot — the
    /// alternative is discovering it as `bad-cb-length` on the one block the
    /// pool ever finds.
    #[test]
    fn coinbase_script_sig_length_is_enforced_at_boot() {
        assert!(validate("/btcpool-rs/", 4, 4).is_ok());

        let err = validate(&"x".repeat(96), 4, 4).unwrap_err().to_string();
        assert!(
            err.contains("coinbase scriptSig"),
            "unexpected error: {err}"
        );
        assert!(err.contains("coinbase_tag"), "unexpected error: {err}");

        // Wide extranonces count against the same budget.
        let err = validate("/btcpool-rs/", 64, 32).unwrap_err().to_string();
        assert!(
            err.contains("coinbase scriptSig"),
            "unexpected error: {err}"
        );
    }

    /// A deployed config.toml predates this key, so it has to default rather
    /// than fail the whole parse — and an out-of-range value has to fail at
    /// boot, not on the one block the pool ever finds.
    #[test]
    fn confirmation_depth_defaults_and_is_range_checked() {
        let base = config_toml("/btcpool-rs/", 4, 4);

        let config: Config = toml::from_str(&base).unwrap();
        assert_eq!(config.pool.confirmation_depth, 6);
        assert!(config.validate().is_ok());

        for (depth, hint) in [(0, "must be between"), (101, "must be between")] {
            let src = base.replace(
                "idle_timeout_secs = 300",
                &format!("idle_timeout_secs = 300\nconfirmation_depth = {depth}"),
            );
            let config: Config = toml::from_str(&src).unwrap();
            let err = config.validate().unwrap_err().to_string();
            assert!(err.contains(hint), "unexpected error for {depth}: {err}");
        }
    }

    /// `""` deserializes to `Some("")`, not `None`, so an empty `log_dir` would
    /// otherwise enable file logging and resolve relative to the process
    /// working directory.
    #[test]
    fn empty_log_dir_disables_file_logging() {
        let base = config_toml("/btcpool-rs/", 4, 4);
        let parse = |src: &str| {
            toml::from_str::<Config>(src)
                .unwrap()
                .logging
                .log_dir_path()
        };

        // Absent, empty, and whitespace-only all mean "stdout only".
        assert_eq!(parse(&base), None);
        for value in ["\"\"", "\"   \""] {
            let src = base.replace("json = false", &format!("json = false\nlog_dir = {value}"));
            assert_eq!(parse(&src), None, "log_dir = {value} should disable files");
        }

        let src = base.replace(
            "json = false",
            "json = false\nlog_dir = \"/var/log/btcpool-rs\"",
        );
        assert_eq!(parse(&src), Some(PathBuf::from("/var/log/btcpool-rs")));
    }

    // Fixtures pass vars directly instead of mutating the process environment,
    // so tests stay parallel-safe.
    fn apply(toml_src: &str, vars: &[(&str, &str)]) -> anyhow::Result<toml::Value> {
        let mut value: toml::Value = toml::from_str(toml_src).unwrap();
        apply_env_overrides(
            &mut value,
            vars.iter().map(|(k, v)| (k.to_string(), v.to_string())),
        )?;
        Ok(value)
    }

    #[test]
    fn legacy_pool_section_deserializes_for_migration_validation() {
        let pool: PoolConfig = toml::from_str(
            r#"
listen_addr = "127.0.0.1:3333"
coinbase_address = "old-address"
coinbase_tag = "/test/"
initial_difficulty = 1
extranonce1_size = 4
extranonce2_size = 4
max_connections = 1
idle_timeout_secs = 60
"#,
        )
        .unwrap();

        assert_eq!(pool.coinbase_address.as_deref(), Some("old-address"));
    }

    #[test]
    fn overrides_use_the_type_of_the_existing_key() {
        let v = apply(
            "[pool]\nlisten_addr = \"0.0.0.0:3333\"\ninitial_difficulty = 512\n[sv2]\nenabled = true",
            &[
                ("BTCPOOL_POOL__LISTEN_ADDR", "0.0.0.0:3335"),
                ("BTCPOOL_POOL__INITIAL_DIFFICULTY", "1024"),
                ("BTCPOOL_SV2__ENABLED", "false"),
            ],
        )
        .unwrap();
        assert_eq!(v["pool"]["listen_addr"].as_str().unwrap(), "0.0.0.0:3335");
        assert_eq!(v["pool"]["initial_difficulty"].as_integer().unwrap(), 1024);
        assert!(!v["sv2"]["enabled"].as_bool().unwrap());
    }

    #[test]
    fn unparseable_override_for_typed_key_fails_loudly() {
        let err = apply(
            "[pool]\ninitial_difficulty = 512",
            &[("BTCPOOL_POOL__INITIAL_DIFFICULTY", "not-a-number")],
        )
        .unwrap_err();
        assert!(err.to_string().contains("pool.initial_difficulty"));
    }

    #[test]
    fn absent_keys_and_sections_are_created_with_inferred_types() {
        let v = apply(
            "[bitcoin_rpc]\nurl = \"http://127.0.0.1:8332\"",
            &[
                ("BTCPOOL_BITCOIN_RPC__USER", "umbrel"),
                ("BTCPOOL_SV2__ENABLED", "false"),
            ],
        )
        .unwrap();
        assert_eq!(v["bitcoin_rpc"]["user"].as_str().unwrap(), "umbrel");
        assert!(!v["sv2"]["enabled"].as_bool().unwrap());
    }

    #[test]
    fn quotes_force_string_for_numeric_looking_absent_values() {
        let v = apply(
            "[bitcoin_rpc]\nurl = \"http://127.0.0.1:8332\"",
            &[
                ("BTCPOOL_BITCOIN_RPC__PASSWORD", "\"123456\""),
                ("BTCPOOL_BITCOIN_RPC__TIMEOUT_SECS", "30"),
            ],
        )
        .unwrap();
        assert_eq!(v["bitcoin_rpc"]["password"].as_str().unwrap(), "123456");
        assert_eq!(v["bitcoin_rpc"]["timeout_secs"].as_integer().unwrap(), 30);
    }

    #[test]
    fn unrelated_and_malformed_prefixed_vars() {
        // Unrelated vars are ignored entirely.
        let v = apply("[pool]\nlisten_addr = \"a\"", &[("PATH", "/usr/bin")]).unwrap();
        assert_eq!(v["pool"]["listen_addr"].as_str().unwrap(), "a");
        // Prefixed vars without the section/key separator are an error, not
        // silently dropped — a typo should not boot with stale config.
        assert!(apply(
            "[pool]\nlisten_addr = \"a\"",
            &[("BTCPOOL_LISTEN_ADDR", "b")]
        )
        .is_err());
    }
}
