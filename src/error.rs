use thiserror::Error;

#[derive(Debug, Error)]
pub enum PoolError {
    // ── Protocol ────────────────────────────────────────────────────────────
    #[error("Stratum parse error: {0}")]
    Parse(String),

    #[error("Unknown Stratum method: {0}")]
    UnknownMethod(String),

    #[error("Miner not subscribed")]
    NotSubscribed,

    #[error("Miner not authorized")]
    NotAuthorized,

    #[error("Invalid params for {method}: {detail}")]
    InvalidParams {
        method: &'static str,
        detail: String,
    },

    // ── Share validation ─────────────────────────────────────────────────────
    #[error("Stale job id: {0}")]
    StaleJob(String),

    #[error("Duplicate share")]
    DuplicateShare,

    #[error("Low difficulty share")]
    LowDifficulty,

    #[error("Invalid block header")]
    InvalidHeader,

    // ── Bitcoin RPC ──────────────────────────────────────────────────────────
    #[error("RPC error: {0}")]
    Rpc(#[from] bitcoincore_rpc::Error),

    #[error("Template unavailable — node not ready")]
    #[allow(dead_code)]
    TemplateUnavailable,

    #[error("submitblock rejected: {0}")]
    SubmitBlockRejected(String),

    // ── Security ─────────────────────────────────────────────────────────────
    #[error("Connection banned: {reason}")]
    #[allow(dead_code)]
    Banned { reason: String },

    #[error("Rate limit exceeded")]
    #[allow(dead_code)]
    RateLimited,

    #[error("Message too large ({bytes} bytes)")]
    MessageTooLarge { bytes: usize },

    // ── I/O ──────────────────────────────────────────────────────────────────
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Connection closed")]
    #[allow(dead_code)]
    ConnectionClosed,

    // ── Generic ──────────────────────────────────────────────────────────────
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Stratum error codes (returned in the `error` field of JSON responses)
#[derive(Debug, Clone, Copy)]
pub enum StratumErrCode {
    Unknown = 20,
    JobNotFound = 21,
    DuplicateShare = 22,
    LowDifficultyShare = 23,
    Unauthorized = 24,
    NotSubscribed = 25,
}

impl StratumErrCode {
    pub fn message(self) -> &'static str {
        match self {
            Self::Unknown => "Unknown problem",
            Self::JobNotFound => "Job not found",
            Self::DuplicateShare => "Duplicate share",
            Self::LowDifficultyShare => "Low difficulty share",
            Self::Unauthorized => "Unauthorized worker",
            Self::NotSubscribed => "Not subscribed",
        }
    }
}

impl PoolError {
    /// A small, fixed label for the `pool_block_submissions_failed_total` metric.
    /// Never derive this from the error's `Display`/`Debug` text: those carry
    /// node-influenced strings (RPC messages, rejection reasons) that would mint
    /// unbounded Prometheus label series.
    pub fn submit_failure_label(&self) -> &'static str {
        match self {
            PoolError::Rpc(_) => "rpc_error",
            PoolError::SubmitBlockRejected(_) => "rejected",
            PoolError::TemplateUnavailable => "template_unavailable",
            PoolError::Io(_) => "io",
            _ => "other",
        }
    }

    /// Map a PoolError to the appropriate Stratum error tuple `[code, message, null]`.
    pub fn to_stratum_error(&self) -> serde_json::Value {
        let (code, msg) = match self {
            PoolError::StaleJob(_) => (
                StratumErrCode::JobNotFound,
                StratumErrCode::JobNotFound.message(),
            ),
            PoolError::DuplicateShare => (
                StratumErrCode::DuplicateShare,
                StratumErrCode::DuplicateShare.message(),
            ),
            PoolError::LowDifficulty => (
                StratumErrCode::LowDifficultyShare,
                StratumErrCode::LowDifficultyShare.message(),
            ),
            PoolError::NotAuthorized => (
                StratumErrCode::Unauthorized,
                StratumErrCode::Unauthorized.message(),
            ),
            PoolError::NotSubscribed => (
                StratumErrCode::NotSubscribed,
                StratumErrCode::NotSubscribed.message(),
            ),
            _ => (StratumErrCode::Unknown, StratumErrCode::Unknown.message()),
        };
        serde_json::json!([code as u32, msg, null])
    }
}
