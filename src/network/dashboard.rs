/// network/dashboard.rs
///
/// Visual HTTP dashboard served on the configured prometheus_addr.
///
/// Routes:
///   GET /            → HTML dashboard (ECharts, auto-refreshes every 10 s)
///   GET /favicon.ico → embedded site icon
///   GET /stats       → JSON snapshot of PoolStats
///   GET /metrics  → Prometheus text (via PrometheusHandle::render)
use crate::{
    mining::engine::TemplateEngine,
    settings::RuntimeSettings,
    stats::{HashrateHistoryPoint, PoolStats},
};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{net::SocketAddr, sync::Arc};
use tracing::{info, warn};

// ─────────────────────────────────────────────────────────────────────────────
// State
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct DashState {
    pub stats: Arc<PoolStats>,
    pub engine: Arc<TemplateEngine>,
    pub prometheus: Option<PrometheusHandle>,
    pub settings: Arc<RuntimeSettings>,
    /// Port miners connect to (Stratum). Shown on the Connect page; the host is
    /// derived client-side from the browser's own location.
    pub stratum_port: u16,
    pub sv2_enabled: bool,
    /// Base58check SV2 Noise authority public key (None when SV2 is disabled).
    /// Shown on the Connect page so miners can pin the pool identity.
    pub sv2_authority_pubkey: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Startup
// ─────────────────────────────────────────────────────────────────────────────

// Startup wiring pulls together independently-constructed runtime pieces; a
// parameter bundle would just move the same fields around for no clarity gain.
#[allow(clippy::too_many_arguments)]
pub async fn start(
    addr: &str,
    stats: Arc<PoolStats>,
    engine: Arc<TemplateEngine>,
    prometheus: Option<PrometheusHandle>,
    settings: Arc<RuntimeSettings>,
    stratum_listen_addr: &str,
    sv2_enabled: bool,
    sv2_authority_pubkey: Option<String>,
) {
    if addr.is_empty() {
        return;
    }

    let socket_addr: SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => {
            warn!("Invalid dashboard addr '{addr}': {e}");
            return;
        }
    };

    // Just the port — the Connect page builds the URL from the browser's host.
    let stratum_port = stratum_listen_addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(0);

    let state = DashState {
        stats,
        engine,
        prometheus,
        settings,
        stratum_port,
        sv2_enabled,
        sv2_authority_pubkey,
    };
    let app = Router::new()
        .route("/", get(dashboard_html))
        .route("/favicon.ico", get(favicon))
        .route("/logo-dark.svg", get(logo_dark))
        .route("/logo-light.svg", get(logo_light))
        .route("/stats", get(stats_json))
        .route("/history", get(history_json))
        .route("/chart", get(chart_json))
        .route("/api/info", get(info_get))
        .route("/metrics", get(metrics_text))
        .with_state(state);

    match tokio::net::TcpListener::bind(socket_addr).await {
        Ok(listener) => {
            info!("Dashboard at http://{addr}/  metrics at http://{addr}/metrics");
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
        }
        Err(e) => warn!("Failed to bind dashboard on {addr}: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Route handlers
// ─────────────────────────────────────────────────────────────────────────────

async fn dashboard_html() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn favicon() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "image/x-icon")],
        include_bytes!("favicon.ico").as_slice(),
    )
}

async fn logo_dark() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "image/svg+xml")],
        LOGO_DARK_SVG,
    )
}

async fn logo_light() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "image/svg+xml")],
        LOGO_LIGHT_SVG,
    )
}

// Brand mark: a Bitcoin "block" (isometric cube) crossed by a miner's pickaxe.
// Two theme-tuned variants — the orange block is shared, only the badge tile and
// the steel of the pickaxe change so the mark reads on each theme's rail.
//   carbon: dark tile, light-steel pick.  light: porcelain tile, slate pick.
const LOGO_DARK_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" role="img" aria-label="btcpool-rs">
<rect x="3" y="3" width="58" height="58" rx="13" fill="#1a1a1f" stroke="#232328" stroke-width="1.5"/>
<polygon points="32,29 48,37.5 32,46 16,37.5" fill="#f7931a"/>
<polygon points="16,37.5 32,46 32,58 16,49.5" fill="#b8650a"/>
<polygon points="32,46 48,37.5 48,49.5 32,58" fill="#d97b10"/>
<path d="M33 19 L31 42" fill="none" stroke="#9aa0ac" stroke-width="5.5" stroke-linecap="round"/>
<path d="M13 30 Q20 17 33 16 Q47 17 55 30 Q47 23 33 23 Q20 23 13 30 Z" fill="#cdd2db"/>
</svg>"##;

const LOGO_LIGHT_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" role="img" aria-label="btcpool-rs">
<rect x="3" y="3" width="58" height="58" rx="13" fill="#f4f4f5" stroke="#e4e4e7" stroke-width="1.5"/>
<polygon points="32,29 48,37.5 32,46 16,37.5" fill="#f7931a"/>
<polygon points="16,37.5 32,46 32,58 16,49.5" fill="#b8650a"/>
<polygon points="32,46 48,37.5 48,49.5 32,58" fill="#d97b10"/>
<path d="M33 19 L31 42" fill="none" stroke="#3f4651" stroke-width="5.5" stroke-linecap="round"/>
<path d="M13 30 Q20 17 33 16 Q47 17 55 30 Q47 23 33 23 Q20 23 13 30 Z" fill="#555b66"/>
</svg>"##;

async fn stats_json(State(state): State<DashState>) -> Json<crate::stats::StatsSnapshot> {
    let mut snapshot = state.stats.snapshot();
    // Template version comes from the engine so the signal remains live even
    // when no miners are connected. Bit 4 drives the BIP110/RDTS card.
    if let Some(template) = state.engine.current_template().await {
        snapshot.template_version = template.version;
    }
    Json(snapshot)
}

/// Static-ish pool info for the Connect page: how to point a miner here, plus
/// build/network/payout context. The host is added client-side from the URL.
#[derive(Serialize)]
struct InfoView {
    version: &'static str,
    stratum_port: u16,
    sv2_enabled: bool,
    /// SV2 Noise authority public key (base58check) for identity pinning;
    /// null when SV2 is disabled.
    sv2_authority_pubkey: Option<String>,
    network: String,
    username_format: &'static str,
}

async fn info_get(State(state): State<DashState>) -> Json<InfoView> {
    Json(InfoView {
        version: env!("CARGO_PKG_VERSION"),
        stratum_port: state.stratum_port,
        sv2_enabled: state.sv2_enabled,
        sv2_authority_pubkey: state.sv2_authority_pubkey.clone(),
        network: state.settings.network().to_string(),
        username_format: "YOUR_BTC_ADDRESS.worker",
    })
}

async fn metrics_text(State(state): State<DashState>) -> Response {
    match &state.prometheus {
        Some(handle) => {
            let body = handle.render();
            (
                [(
                    axum::http::header::CONTENT_TYPE,
                    "text/plain; version=0.0.4",
                )],
                body,
            )
                .into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Prometheus metrics not enabled",
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct HistoryParams {
    since: Option<u64>,
}

#[derive(Serialize)]
struct HistoryPoint {
    ts: u64,
    hps: f64,
}

async fn history_json(
    State(state): State<DashState>,
    Query(params): Query<HistoryParams>,
) -> Json<Vec<HistoryPoint>> {
    let since = params.since.unwrap_or(0);
    let points = state
        .stats
        .get_hashrate_history(since, 60)
        .into_iter()
        .filter_map(|point| {
            point
                .ten_minutes
                .map(|hps| HistoryPoint { ts: point.ts, hps })
        })
        .collect();
    Json(points)
}

#[derive(Deserialize)]
struct ChartParams {
    window: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChartWindow {
    duration_secs: Option<u64>,
    bucket_secs: u64,
}

/// Bucket sizes are picked to land near 360 points per view. The two shortest
/// ranges bucket at the sampling interval, so the chart moves as often as the
/// pool records — anything coarser would average away the 1m line's detail.
///
/// Note there is deliberately no `"1m"` or `"6m"` alias here: `1m` is a *series*
/// name on this chart, and having it also mean "30 days" as a range made for a
/// nasty ambiguity.
fn chart_window(value: Option<&str>) -> ChartWindow {
    match value.unwrap_or("1h") {
        "1h" => ChartWindow {
            duration_secs: Some(3_600),
            bucket_secs: crate::stats::SNAPSHOT_INTERVAL_SECS,
        },
        "6h" => ChartWindow {
            duration_secs: Some(6 * 3_600),
            bucket_secs: 60,
        },
        "24h" => ChartWindow {
            duration_secs: Some(24 * 3_600),
            bucket_secs: 5 * 60,
        },
        "1w" => ChartWindow {
            duration_secs: Some(7 * 24 * 3_600),
            bucket_secs: 30 * 60,
        },
        "30d" => ChartWindow {
            duration_secs: Some(30 * 24 * 3_600),
            bucket_secs: 2 * 3_600,
        },
        "180d" => ChartWindow {
            duration_secs: Some(6 * 30 * 24 * 3_600),
            bucket_secs: 12 * 3_600,
        },
        "all" => ChartWindow {
            duration_secs: None,
            bucket_secs: 12 * 3_600,
        },
        _ => chart_window(None),
    }
}

fn chart_series_data(
    history: &[HashrateHistoryPoint],
    value: fn(&HashrateHistoryPoint) -> Option<f64>,
) -> Vec<serde_json::Value> {
    history
        .iter()
        .map(|point| json!([point.ts.saturating_mul(1_000), value(point)]))
        .collect()
}

async fn chart_json(
    State(state): State<DashState>,
    Query(params): Query<ChartParams>,
) -> impl IntoResponse {
    let window = chart_window(params.window.as_deref());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let since = window
        .duration_secs
        .map(|duration| now.saturating_sub(duration))
        .unwrap_or(0);

    let mut history = state.stats.get_hashrate_history(since, window.bucket_secs);

    // Append the current live value as the trailing edge of the chart, snapped
    // to the bucket grid. Every other point is a bucket mean, so plotting a raw
    // instantaneous sample at `now` put a different statistic at a different x
    // offset and visibly kinked the right-hand end on coarse ranges. If the
    // query already returned the bucket we are inside, it holds the same
    // samples and there is nothing to add.
    let live_bucket = now / window.bucket_secs.max(1) * window.bucket_secs.max(1);
    if history.last().map(|p| p.ts) != Some(live_bucket) {
        let live = state.stats.snapshot();
        history.push(HashrateHistoryPoint {
            ts: live_bucket,
            one_minute: Some(live.total_hashrate_60s),
            five_minutes: Some(live.total_hashrate_5m),
            ten_minutes: Some(live.total_hashrate_10m),
            one_hour: Some(live.total_hashrate_1h),
            six_hours: Some(live.total_hashrate_6h),
            twenty_four_hours: Some(live.total_hashrate_24h),
        });
    }

    let chart = build_chart_option(&history);

    let body = serde_json::to_string(&chart).unwrap_or_else(|_| "{}".to_string());
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
}

/// Build the ECharts option object the browser renders. The client only skins
/// it (theme colours, JS formatter callbacks); everything structural is decided
/// here.
fn build_chart_option(history: &[HashrateHistoryPoint]) -> serde_json::Value {
    let make_series = |name: &str, data: Vec<serde_json::Value>, width: f64| {
        json!({
            "name": name,
            "type": "line",
            "data": data,
            "showSymbol": false,
            "smooth": false,
            "connectNulls": false,
            "animation": false,
            "lineStyle": { "width": width }
        })
    };

    json!({
        "backgroundColor": "transparent",
        "tooltip": { "trigger": "axis" },
        "legend": {
            "data": ["1m", "5m", "10m", "1h", "6h", "24h"],
            "selected": { "1m": true, "5m": true, "10m": true, "1h": true, "6h": false, "24h": false }
        },
        // `containLabel` already reserves whatever the axis labels need inside
        // the grid box, so these are pure breathing room — anything larger is
        // counted twice and shows up as dead space before the y-axis labels.
        // `top` clears the legend; the client raises it if the legend wraps.
        "grid": { "left": 4, "right": 12, "top": 44, "bottom": 4, "containLabel": true },
        "xAxis": {
            "type": "time",
            "boundaryGap": false,
            "splitLine": { "show": true },
            // The client sets `splitNumber` from the rendered width. hideOverlap
            // is the backstop: whatever tick interval ECharts settles on, it must
            // not paint two labels on top of each other.
            "axisLabel": { "fontSize": 10, "hideOverlap": true }
        },
        "yAxis": {
            "type": "value",
            "min": 0,
            "splitLine": { "show": true },
            "axisLabel": { "fontSize": 10, "hideOverlap": true }
        },
        // Longest window first: ECharts paints series in array order, so this
        // puts the short, fast-moving lines on top of the slow ones instead of
        // burying them. The legend reads the other way round (see `legend.data`
        // above) — it takes its order from that list, not from this one — and
        // the client keys line colours by series *name* so that the two
        // orderings can differ without shuffling the palette.
        "series": [
            make_series("24h", chart_series_data(history, |p| p.twenty_four_hours), 1.2),
            make_series("6h", chart_series_data(history, |p| p.six_hours), 1.2),
            make_series("1h", chart_series_data(history, |p| p.one_hour), 1.4),
            make_series("10m", chart_series_data(history, |p| p.ten_minutes), 1.8),
            make_series("5m", chart_series_data(history, |p| p.five_minutes), 1.2),
            make_series("1m", chart_series_data(history, |p| p.one_minute), 1.0)
        ]
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Dashboard HTML
// ─────────────────────────────────────────────────────────────────────────────

// Console layout: fixed left rail (nav + status) with the content in sections.
// Two themes via CSS custom properties on <html data-theme="...">:
//   carbon (default dark) — near-black neutrals, single amber accent
//   light                 — porcelain/Swiss, single cobalt accent
// The choice persists in localStorage and seeds from prefers-color-scheme.
//
// The build version is baked into the rail footer at compile time from
// CARGO_PKG_VERSION (sourced from Cargo.toml), so it can never drift from the
// crate version. `concat!` keeps the whole page a single `&'static str`.
const DASHBOARD_HTML: &str = concat!(
    r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="icon" href="/favicon.ico" type="image/x-icon">
<title>btcpool-rs</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js"></script>
<style>
:root {
  --bg: #0a0a0b;
  --surface: #131316;
  --surface2: #1a1a1f;
  --border: #232328;
  --text: #ededef;
  --muted: #8b8b93;
  --accent: #f7931a;
  --ok: #3ecf8e;
  --warn: #e3b341;
  --bad: #f0564a;
  --grid: rgba(255,255,255,0.06);
}
:root[data-theme="light"] {
  --bg: #fafafa;
  --surface: #ffffff;
  --surface2: #f4f4f5;
  --border: #e4e4e7;
  --text: #111113;
  --muted: #71717a;
  --accent: #2456e6;
  --ok: #16a34a;
  --warn: #ca8a04;
  --bad: #dc2626;
  --grid: rgba(0,0,0,0.07);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html { scroll-behavior: smooth; }
body {
  background: var(--bg); color: var(--text); min-height: 100vh;
  font-family: 'Inter', ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif;
  font-size: 15px;
}
.shell { display: flex; min-height: 100vh; }

/* ── Left rail ── */
.rail {
  width: 208px; flex: none; position: sticky; top: 0; height: 100vh;
  display: flex; flex-direction: column; gap: 1.4rem;
  padding: 1.2rem 0.85rem; background: var(--surface);
  border-right: 1px solid var(--border);
}
.brand { display: flex; align-items: center; gap: 0.5rem; padding: 0 0.6rem; }
.brand img.mark { height: 1.9rem; width: auto; border-radius: 5px; display: block; }
.brand .name { font-weight: 700; font-size: 0.92rem; letter-spacing: -0.02em; white-space: nowrap; }
#nav-burger { display: none; }
nav { display: flex; flex-direction: column; gap: 2px; }
nav a, nav .nav-btn {
  color: var(--muted); text-decoration: none; font-size: 0.8rem; font-weight: 500;
  padding: 0.42rem 0.6rem; border-radius: 5px; border-left: 2px solid transparent;
  font-family: inherit; text-align: left; background: none; border-top: none;
  border-right: none; border-bottom: none; cursor: pointer; width: 100%;
}
nav a:hover, nav .nav-btn:hover { color: var(--text); background: var(--surface2); }
nav a.active { color: var(--text); background: var(--surface2); border-left-color: var(--accent); }
.rail-foot {
  margin-top: auto; display: flex; flex-direction: column; gap: 0.5rem;
  font-size: 0.7rem; color: var(--muted); padding: 0 0.6rem;
  font-variant-numeric: tabular-nums;
}
#theme-toggle {
  align-self: flex-start; cursor: pointer; font: inherit; color: var(--muted);
  background: none; border: 1px solid var(--border); border-radius: 5px;
  padding: 0.3rem 0.6rem;
}
#theme-toggle:hover { color: var(--text); border-color: var(--muted); }
.rail-led { margin-right: 0.4rem; }
.rail-foot a { color: var(--muted); text-decoration: none; }
.rail-foot a:hover { color: var(--text); }

/* ── Main column ── */
main { flex: 1; min-width: 0; max-width: 1240px; padding: 1.7rem 2.1rem 2.5rem; }
section { margin-bottom: 2.4rem; scroll-margin-top: 1.2rem; }
.sec-title {
  font-size: 0.66rem; font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.13em; color: var(--muted); margin-bottom: 0.9rem;
}

/* ── Hero ── */
.hero {
  display: flex; flex-wrap: wrap; gap: 1.6rem 2.4rem; align-items: stretch;
  background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  padding: 1.4rem 1.7rem; margin-bottom: 1.4rem;
}
.hero .label, .kpi .label {
  font-size: 0.62rem; font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.11em; color: var(--muted); margin-bottom: 0.4rem;
}
.hero-value {
  font-size: 3.1rem; font-weight: 740; line-height: 1.04; letter-spacing: -0.045em;
  color: var(--accent); font-variant-numeric: tabular-nums;
}
.hero-sub { display: flex; flex-wrap: wrap; gap: 0.35rem 1.2rem; margin-top: 0.5rem; font-size: 0.76rem; color: var(--muted); font-variant-numeric: tabular-nums; }
.hero-side {
  margin-left: auto; display: flex; flex-direction: column; justify-content: center;
  gap: 0.32rem; padding-left: 2.2rem; border-left: 1px solid var(--border);
  font-size: 0.78rem; font-variant-numeric: tabular-nums;
}
.hero-side .label { margin-bottom: 0.2rem; }

/* ── KPI strip ── */
.kpis {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(158px, 1fr));
  gap: 1.1rem 1.5rem; margin-bottom: 1.4rem;
}
.kpi { border-left: 1px solid var(--border); padding-left: 0.9rem; min-width: 0; }
.kpi .val { font-size: 1.06rem; font-weight: 650; letter-spacing: -0.01em; font-variant-numeric: tabular-nums; }
.kpi .sub { font-size: 0.72rem; color: var(--muted); margin-top: 0.15rem; font-variant-numeric: tabular-nums; }
.kpi .sub.trunc { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ok  { color: var(--ok); }
.bad { color: var(--bad); }
.accent { color: var(--accent); }

/* ── Panels / chart / table ── */
.panel { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 1.15rem 1.3rem; }
/* A too-wide table scrolls inside its panel instead of stretching the page
   (which pushed every other card off-balance on narrow screens). */
#workers .panel { overflow-x: auto; }
.panel-head { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 0.65rem; margin-bottom: 0.7rem; }
.panel-controls { display: flex; flex-wrap: wrap; align-items: center; gap: 0.7rem; }
#chart-toggle {
  cursor: pointer; font: inherit; font-size: 0.72rem; color: var(--muted);
  background: none; border: 1px solid var(--border); border-radius: 5px;
  padding: 0.22rem 0.45rem;
}
#chart-toggle:hover { color: var(--text); border-color: var(--muted); }
.panel-title { font-size: 0.66rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.13em; color: var(--muted); }
.timeframe-tabs { display: flex; flex-wrap: wrap; align-items: center; }
.timeframe-btn {
  cursor: pointer; font: inherit; font-size: 0.7rem; color: var(--muted);
  background: var(--surface2); border: 1px solid var(--border); border-right: none;
  border-radius: 0; padding: 0.23rem 0.45rem;
}
.timeframe-btn:first-child { border-radius: 5px 0 0 5px; }
.timeframe-btn:last-child { border-right: 1px solid var(--border); border-radius: 0 5px 5px 0; }
.timeframe-btn:hover { color: var(--text); }
.timeframe-btn.active { color: var(--bg); background: var(--accent); border-color: var(--accent); }
/* Viewport-relative so a phone gets a usable plot and a tall desktop doesn't
   push the workers table below the fold. ECharts does not track CSS size on
   its own — the debounced resize handler is what makes this take effect.
   vh, not dvh: dvh follows the mobile URL bar and would re-lay-out on scroll. */
#hashrate-chart { height: clamp(280px, 40vh, 420px); width: 100%; }
table { width: 100%; border-collapse: collapse; font-size: 0.84rem; font-variant-numeric: tabular-nums; }
th {
  text-align: left; color: var(--muted); font-weight: 500; padding: 0.34rem 0.55rem;
  border-bottom: 1px solid var(--border); font-size: 0.66rem;
  text-transform: uppercase; letter-spacing: 0.09em; white-space: nowrap;
}
td { padding: 0.5rem 0.55rem; border-bottom: 1px solid var(--grid); white-space: nowrap; }
tr:last-child td { border-bottom: none; }
.empty-row { color: var(--muted); text-align: center; padding: 1.2rem; font-size: 0.84rem; }
/* Worker status LED — green when online, grey when offline. */
.led { width: 9px; height: 9px; border-radius: 50%; display: inline-block; vertical-align: middle; }
.led-on { background: var(--ok); box-shadow: 0 0 5px var(--ok); }
.led-warn { background: var(--warn); box-shadow: 0 0 5px var(--warn); }
.led-off { background: var(--muted); opacity: 0.45; }
.col-led { text-align: center; }
/* New chain tip: pulse the number itself in the accent color (two beats),
   matching the other highlighted values instead of flashing the background. */
@keyframes blockPulse {
  0%   { color: var(--text);   transform: scale(1); }
  15%  { color: var(--accent); transform: scale(1.14); }
  40%  { color: var(--accent); transform: scale(1); }
  55%  { color: var(--accent); transform: scale(1.08); }
  75%  { color: var(--accent); transform: scale(1); }
  100% { color: var(--text);   transform: scale(1); }
}
#v-height.block-new { animation: blockPulse 1.6s ease-in-out; transform-origin: left center; }

/* BTC price tick: pulse just the price digits green/red by direction. */
@keyframes pricePulse { 0% { color: var(--pulse); } 70% { color: var(--pulse); } 100% { color: inherit; } }
#v-btc-price-num.price-up   { --pulse: var(--ok);  animation: pricePulse 1.4s ease-out; }
#v-btc-price-num.price-down { --pulse: var(--bad); animation: pricePulse 1.4s ease-out; }
#pair-select {
  font: inherit; font-size: 0.62rem; color: var(--muted); text-transform: none;
  letter-spacing: normal; background: var(--surface2);
  border: 1px solid var(--border); border-radius: 4px; padding: 0.08rem 0.25rem;
}
/* .kpi .sub sets the muted color at higher specificity; win it back for the
   24h change line. */
.kpi .sub.ok  { color: var(--ok); }
.kpi .sub.bad { color: var(--bad); }

/* ── Connect form / network badge ── */
#net-badge {
  font-size: 0.58rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em;
  color: var(--accent); border: 1px solid var(--accent); border-radius: 4px;
  padding: 0.1rem 0.35rem; margin-left: 0.5rem; align-self: center;
}
.field { margin-bottom: 0.95rem; }
.field label {
  display: block; font-size: 0.62rem; font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.11em; color: var(--muted); margin-bottom: 0.35rem;
}
.field input, .field select {
  font: inherit; font-size: 0.85rem; color: var(--text); background: var(--surface2);
  border: 1px solid var(--border); border-radius: 5px; padding: 0.45rem 0.6rem;
  width: 100%; max-width: 520px; font-variant-numeric: tabular-nums;
}
.field input:focus, .field select:focus { outline: none; border-color: var(--accent); }
.settings-note { font-size: 0.72rem; color: var(--muted); margin-top: 0.9rem; line-height: 1.5; }

/* ── Connect modal ── */
#connect-modal {
  /* Pin to the viewport center — the UA default doesn't reliably vertically
     center a modal <dialog> across browsers. */
  position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); margin: 0;
  width: min(640px, calc(100vw - 2rem)); max-height: calc(100vh - 2rem); overflow: auto;
  border: 1px solid var(--border);
  border-radius: 10px; background: var(--surface); color: var(--text);
  padding: 1.3rem 1.5rem; box-shadow: 0 24px 60px rgba(0,0,0,0.45);
}
#connect-modal::backdrop { background: rgba(0,0,0,0.55); backdrop-filter: blur(2px); }
.copy-btn {
  font: inherit; font-size: 0.8rem; font-weight: 600; cursor: pointer; flex: none;
  color: var(--bg); background: var(--accent); border: none; border-radius: 5px; padding: 0 0.9rem;
}
.connect-ro {
  font-size: 0.85rem; font-variant-numeric: tabular-nums; word-break: break-all;
  background: var(--surface2); border: 1px solid var(--border); border-radius: 5px; padding: 0.45rem 0.6rem;
}
.connect-hints { list-style: none; display: flex; flex-direction: column; gap: 0.5rem; font-size: 0.78rem; color: var(--muted); line-height: 1.5; }
.connect-hints code { background: var(--surface2); padding: 0.05rem 0.3rem; border-radius: 4px; }
.modal-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; }
.modal-x {
  font: inherit; font-size: 1.3rem; line-height: 1; cursor: pointer; color: var(--muted);
  background: none; border: none; padding: 0 0.2rem;
}
.modal-x:hover { color: var(--text); }
.modal-actions { display: flex; align-items: center; margin-top: 0.4rem; }

/* ── Narrow screens: rail becomes a top bar ── */
@media (max-width: 880px) {
  .shell { flex-direction: column; }
  /* Top bar: brand left, burger right; nav + foot live in a drawer that
     expands below the bar (pushing content down — no overlay to manage). */
  .rail {
    width: 100%; height: auto; position: static; flex-direction: row;
    flex-wrap: wrap; align-items: center; gap: 0.9rem; padding: 0.7rem 1rem;
    border-right: none; border-bottom: 1px solid var(--border);
  }
  #nav-burger {
    display: block; margin-left: auto; cursor: pointer;
    font-size: 1.15rem; line-height: 1; color: var(--muted);
    background: none; border: 1px solid var(--border); border-radius: 5px;
    padding: 0.25rem 0.55rem;
  }
  #nav-burger:hover { color: var(--text); border-color: var(--muted); }
  .rail.nav-open #nav-burger { color: var(--text); border-color: var(--accent); }
  nav, .rail-foot { display: none; }
  .rail.nav-open nav {
    display: flex; flex-direction: column; width: 100%;
    padding-top: 0.5rem; border-top: 1px solid var(--border);
  }
  .rail.nav-open .rail-foot {
    display: flex; flex-direction: row; flex-wrap: wrap; align-items: center;
    width: 100%; margin-top: 0; gap: 0.8rem;
  }
  .rail-foot .hide-sm { display: none; }
  main { padding: 1.2rem 1rem 2rem; }
  .hero-side { margin-left: 0; padding-left: 0; border-left: none; }
}

/* Phone: the 15-column workers table can't fit; retain the 10m operational
   rate and counters while dropping secondary averages and metadata
   and let anything still wider scroll inside its panel. */
@media (max-width: 660px) {
  #workers th:nth-child(3), #workers td:nth-child(3),
  #workers th:nth-child(4), #workers td:nth-child(4),
  #workers th:nth-child(5), #workers td:nth-child(5),
  #workers th:nth-child(6), #workers td:nth-child(6),
  #workers th:nth-child(8), #workers td:nth-child(8),
  #workers th:nth-child(9), #workers td:nth-child(9),
  #workers th:nth-child(10), #workers td:nth-child(10),
  #workers th:nth-child(13), #workers td:nth-child(13),
  #workers th:nth-child(15), #workers td:nth-child(15) { display: none; }
}
</style>
</head>
<body>
<div class="shell">

<aside class="rail">
  <div class="brand"><img id="brand-logo" class="mark" src="/logo-dark.svg" alt="btcpool-rs logo" width="64" height="64"><span class="name">btcpool-rs</span><span id="net-badge" hidden></span></div>
  <button type="button" id="nav-burger" aria-label="Toggle menu" aria-expanded="false">&#9776;</button>
  <nav id="rail-nav">
    <a href="#overview" data-section="overview" class="active">Overview</a>
    <a href="#workers" data-section="workers">Workers</a>
    <a href="#network" data-section="network">Network</a>
    <button type="button" class="nav-btn" id="open-connect">Connect</button>
    <a href="/metrics">Raw metrics &#8599;</a>
  </nav>
  <div class="rail-foot">
    <button id="theme-toggle" title="Toggle light/dark theme">&#9681; Theme</button>
    <span class="hide-sm"><span id="conn-led" class="led led-off rail-led" title="Connecting&hellip;"></span>Block <span id="rail-height">&mdash;</span></span>
    <span id="server-uptime" title="How long this pool process has been running">Uptime &mdash;</span>
    <span id="last-updated" class="hide-sm">Loading&hellip;</span>
    <span class="hide-sm">v"##,
    env!("CARGO_PKG_VERSION"),
    r##" &middot; <a href="https://github.com/edifus/btcpool-rs">source</a></span>
  </div>
</aside>

<main>

<section id="overview">
  <div class="hero">
    <div>
      <div class="label">Pool hashrate &middot; 10m</div>
      <div class="hero-value" id="v-reported-current">&mdash;</div>
      <div class="hero-sub"><span id="v-reported-1m">1m: &mdash;</span><span id="v-reported-1h">1h: &mdash;</span><span id="v-reported-6h">6h: &mdash;</span><span id="v-reported-24h">24h: &mdash;</span></div>
    </div>
    <div class="hero-side">
      <div class="label">Block odds</div>
      <span id="v-prob-daily">Daily: &mdash;</span>
      <span id="v-prob-monthly">Monthly: &mdash;</span>
      <span id="v-prob-yearly">Yearly: &mdash;</span>
      <span id="v-prob-powerball" style="color:var(--muted);">vs Powerball: &mdash;</span>
    </div>
  </div>

  <div class="kpis">
    <div class="kpi">
      <div class="label">Miners</div>
      <div class="val" id="v-miners">&mdash;</div>
      <div class="sub"><span id="v-workers-online">Online: &mdash;</span> &middot; <span id="v-workers-degraded">Degraded: &mdash;</span></div>
      <div class="sub" id="v-workers-offline">Offline: &mdash;</div>
    </div>
    <div class="kpi">
      <div class="label">Rejects</div>
      <div class="val" id="v-reject-rate">&mdash;</div>
      <div class="sub" id="v-stale-rate">Stale: &mdash;</div>
    </div>
    <div class="kpi">
      <div class="label">Best share</div>
      <div class="val accent" id="v-best-share">&mdash;</div>
      <div class="sub">session: <span id="v-session-best-share">&mdash;</span> &middot; <span id="v-best-over-network" title="Has the all-time best share met current network difficulty?">&mdash;</span> vs net</div>
    </div>
    <div class="kpi">
      <div class="label">Best hashrate</div>
      <div class="val" id="v-best-hashrate">&mdash;</div>
      <div class="sub">session: <span id="v-session-best-hashrate">&mdash;</span></div>
    </div>
    <div class="kpi">
      <div class="label">Pool uptime</div>
      <div class="val" id="v-uptime">&mdash;</div>
      <div class="sub">found blocks survive restarts</div>
    </div>
    <div class="kpi">
      <div class="label">Last block found</div>
      <div class="val" id="v-last-block-worker">&mdash;</div>
      <div class="sub trunc" id="v-last-block-payout" title="Payout address encoded in the found block">&mdash;</div>
      <div class="sub" id="v-last-block-time">&mdash;</div>
      <div class="sub trunc" id="v-last-block-hash" title="Hash of the last block this pool found">&mdash;</div>
    </div>
  </div>

  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">Hashrate averages <span title="Rolling hashrate averages sampled every minute; long ranges use time-bucket averages" style="cursor:help;">&#9432;</span></div>
      <div class="panel-controls">
        <div id="chart-window-label" class="timeframe-tabs" role="group" aria-label="Chart range">
          <button type="button" class="timeframe-btn active" data-window="1h">1h</button>
          <button type="button" class="timeframe-btn" data-window="6h">6h</button>
          <button type="button" class="timeframe-btn" data-window="24h">24h</button>
          <button type="button" class="timeframe-btn" data-window="1w">1w</button>
          <button type="button" class="timeframe-btn" data-window="30d">30d</button>
          <button type="button" class="timeframe-btn" data-window="180d">180d</button>
          <button type="button" class="timeframe-btn" data-window="all">All</button>
        </div>
        <button id="chart-toggle" title="Hide or show the hashrate chart">Hide</button>
      </div>
    </div>
    <div id="hashrate-chart"></div>
  </div>
</section>

<section id="workers">
  <div class="sec-title">Workers</div>
  <div class="panel">
  <table>
    <thead>
      <tr>
        <th>Worker</th>
        <th class="col-led">Status</th>
        <th>Mode</th>
        <th>Vardiff</th>
        <th>Hashrate (1m)</th>
        <th>Hashrate (5m)</th>
        <th>Hashrate (10m)</th>
        <th>Hashrate (1h)</th>
        <th>Hashrate (6h)</th>
        <th>Hashrate (24h)</th>
        <th>Accepted</th>
        <th>Rejected</th>
        <th>Best Share</th>
        <th>Last Share</th>
        <th>Uptime</th>
      </tr>
    </thead>
    <tbody id="workers-tbody">
      <tr><td colspan="15" class="empty-row">Loading workers&hellip;</td></tr>
    </tbody>
  </table>
  </div>
</section>

<section id="network">
  <div class="sec-title">Network</div>
  <div class="kpis">
    <div class="kpi">
      <div class="label">Network hashrate</div>
      <div class="val" id="v-net-hashrate">&mdash;</div>
      <div class="sub" id="v-net-diff">Diff: &mdash;</div>
    </div>
    <div class="kpi">
      <div class="label">Next adjustment</div>
      <div class="val" id="v-net-next-adj" style="font-size:0.92rem;" title="Estimated time until the next difficulty adjustment (2016-block epochs, ~10 min/block)">&mdash;</div>
      <div class="sub" id="v-net-adj-pct" title="Estimated difficulty change at the next retarget, from actual block timestamps in the current 2016-block epoch. Clamped to the protocol's [-75%, +300%] range.">Est. move: &mdash;</div>
    </div>
    <div class="kpi">
      <div class="label">Chain tip</div>
      <div class="val" id="v-height" title="Height of current best chain tip">&mdash;</div>
      <div class="sub"><span id="v-block-transaction-count">Txs: &mdash;</span></div>
      <div class="sub" id="v-block-reward">Reward: &mdash;</div>
    </div>
    <div class="kpi">
      <div class="label">BIP110 / RDTS</div>
      <div class="val" id="v-bip110" style="font-size:0.92rem;" title="Whether block templates from your Bitcoin node signal the BIP110 (RDTS) soft fork proposal by setting version bit 4. The pool copies the block version from your node, so this is decided by your node software, not by the pool.">&mdash;</div>
      <div class="sub">signal &middot; version bit 4, from your node</div>
    </div>
    <div class="kpi">
      <div class="label" style="display:flex; justify-content:space-between; align-items:center;">Market
        <select id="pair-select" title="Quote currency">
          <option selected>USD</option>
          <option>EUR</option>
          <option>GBP</option>
          <option>CAD</option>
          <option>AUD</option>
          <option>CHF</option>
          <option>JPY</option>
        </select>
      </div>
      <div class="val" id="v-btc-price" style="font-size:0.92rem;">BTC <span id="v-btc-price-num">&mdash;</span></div>
      <div class="sub" id="v-btc-change">24h: &mdash;</div>
    </div>
  </div>
</section>

<dialog id="connect-modal">
  <div class="modal-head">
    <span class="sec-title" style="margin:0;">Connect a miner</span>
    <button type="button" class="modal-x" id="close-connect" aria-label="Close">&times;</button>
  </div>
  <div class="field">
    <label for="connect-url">Point your miner at this address</label>
    <div style="display:flex; gap:0.5rem;">
      <input id="connect-url" type="text" readonly spellcheck="false" value="&mdash;">
      <button type="button" id="connect-copy" class="copy-btn">Copy</button>
    </div>
    <p class="settings-note" id="connect-proto" style="margin-top:0.5rem;"></p>
  </div>
  <div class="field">
    <label>Username / worker identity</label>
    <div class="connect-ro" id="connect-address">&mdash;</div>
    <p class="settings-note" style="margin-top:0.35rem;">Use your Bitcoin address, optionally followed by a worker label. A found block pays the address before the first dot.</p>
  </div>
  <div class="field" id="connect-authority-field" hidden>
    <label for="connect-authority">Pool identity (SV2 authority public key)</label>
    <div style="display:flex; gap:0.5rem;">
      <input id="connect-authority" type="text" readonly spellcheck="false" value="&mdash;">
      <button type="button" id="connect-authority-copy" class="copy-btn">Copy</button>
    </div>
    <p class="settings-note" style="margin-top:0.35rem;">Optional: set this as the pool/authority public key on an SV2 miner to verify it is talking to this pool. Miners connect fine without it.</p>
  </div>
  <div class="field">
    <label>Firmware quick start</label>
    <ul class="connect-hints">
      <li><strong>Bitaxe / AxeOS</strong> &amp; multi-chip (NerdQAxe++, Nexus): set the Stratum URL + port above and use <code>BITCOIN_ADDRESS.worker</code>.</li>
      <li><strong>Avalon Nano / Q</strong>: in the Avalon Family app, add a pool using the host and port above, then set the worker identity to <code>BITCOIN_ADDRESS.worker</code>.</li>
      <li><strong>cgminer / generic ASIC</strong>: <code>--url stratum+tcp://HOST:PORT --userpass BITCOIN_ADDRESS.worker:x</code>.</li>
    </ul>
  </div>
  <p class="settings-note">
    btcpool-rs <span id="connect-version">&mdash;</span> &middot; network <span id="connect-network">&mdash;</span> &middot;
    <a href="https://github.com/edifus/btcpool-rs">source</a> &middot;
    <a href="https://github.com/edifus/btcpool-rs/issues">support</a> &middot; MIT/Apache-2.0
  </p>
</dialog>

</main>
</div>

<script>
// ── Theme ────────────────────────────────────────────────────────────────────
const THEME_KEY = 'btcpool-theme';
function currentTheme() { return document.documentElement.dataset.theme === 'light' ? 'light' : 'carbon'; }
function applyTheme(t) {
  if (t === 'light') document.documentElement.dataset.theme = 'light';
  else delete document.documentElement.dataset.theme;
  try { localStorage.setItem(THEME_KEY, t); } catch (_) {}
  const logo = document.getElementById('brand-logo');
  if (logo) logo.src = t === 'light' ? '/logo-light.svg' : '/logo-dark.svg';
}
(function initTheme() {
  let t = null;
  try { t = localStorage.getItem(THEME_KEY); } catch (_) {}
  if (t !== 'light' && t !== 'carbon') {
    t = window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'carbon';
  }
  applyTheme(t);
})();
function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

const DEFAULT_WINDOW = '1h';
let selectedWindow = DEFAULT_WINDOW;
let lastBlockHeight = 0;
// Degraded detection is *relative to each worker's own baseline*, not an absolute
// timeout — so low-hashrate / never-submitted / just-connected miners (whose
// natural share interval is long, or who haven't established one yet) are never
// falsely flagged. A worker is degraded only if it has an established cadence and
// has since gone silent for well beyond it.
const DEGRADED_SECS = 120;        // floor on the silence threshold (fast miners)
const DEGRADED_INTERVALS = 5;     // missed *expected* shares before flagging

// Has the worker established a share cadence we can reason about? Needs to be
// online, to have submitted at least once, and to have a measurable baseline
// hashrate (the 3h window retains the rate even after a recent stall).
function workerBaselineHps(w) {
  return (w.online && w.last_submit_ts > 0) ? (w.hashrate_3h_hps || 0) : 0;
}

function isDegraded(w, nowSec) {
  const baseHps = workerBaselineHps(w);
  if (baseHps <= 0) return false;                     // no baseline → never alarm
  // Expected seconds/share at the worker's own difficulty + baseline hashrate.
  const expected = (w.current_vardiff * 4294967296) / baseHps;
  const silentFor = nowSec - w.last_submit_ts;
  return silentFor > Math.max(DEGRADED_SECS, DEGRADED_INTERVALS * expected);
}

// Classify a worker's status LED: grey (offline) > yellow (online but degraded)
// > green (healthy). The reason rides in the tooltip, not the colour.
function workerLed(w, nowSec) {
  if (!w.online) return { cls: 'led-off', title: 'Offline' };
  if (isDegraded(w, nowSec)) {
    return { cls: 'led-warn', title: 'Degraded — no share in ' + fmtUptime(nowSec - w.last_submit_ts) + ' (well past its usual cadence)' };
  }
  return { cls: 'led-on', title: 'Online' };
}
// Timestamp (ms) of the last successful /stats refresh. Drives the rail
// connectivity LED: green while updates are landing, grey once they go stale.
let lastStatsOk = 0;

function updateConnLed() {
  const led = document.getElementById('conn-led');
  if (!led) return;
  const ageMs = lastStatsOk ? Date.now() - lastStatsOk : Infinity;
  // Refresh runs every 10s; tolerate one missed beat before flagging stale.
  if (ageMs < 25000) {
    led.classList.add('led-on'); led.classList.remove('led-off');
    led.title = 'Live — updated ' + Math.round(ageMs / 1000) + 's ago';
  } else {
    led.classList.add('led-off'); led.classList.remove('led-on');
    led.title = lastStatsOk
      ? 'Connection lost — no update for ' + Math.round(ageMs / 1000) + 's'
      : 'Connecting…';
  }
}

const myChart = echarts.init(document.getElementById('hashrate-chart'), null, { renderer: 'canvas' });

// Last option object handed to the chart, kept so a resize can recompute the
// width-dependent bits without refetching.
let chartOptions = null;

// Anything about the chart that depends on how wide it actually rendered. The
// server can't know the viewport, so it ships sensible defaults and these get
// patched in on top — on every load, on resize, and when the panel is expanded.
function applyResponsiveLayout(options) {
  if (!options) return options;
  const width = myChart.getWidth() || 0;

  // Roughly one x-axis label per 90px, which fits 'HH:mm' at fontSize 10 with
  // clear air between. A phone lands on 4; a wide desktop caps at 10.
  const xAxis = Array.isArray(options.xAxis) ? options.xAxis[0] : options.xAxis;
  if (xAxis) {
    xAxis.splitNumber = Math.min(10, Math.max(3, Math.round(width / 90)));
  }

  // Six legend entries need ~340px on one row. Below that ECharts wraps to a
  // second row, which would sit on top of the plot at the default grid top.
  const legendRows = width > 0 && width < 340 ? 2 : 1;
  const grid = Array.isArray(options.grid) ? options.grid[0] : options.grid;
  if (grid) {
    grid.top = 44 + 22 * (legendRows - 1);
  }

  // Two decimals ('25.00 T') is a lot of y-axis label on a narrow screen, and
  // containLabel turns every pixel saved there into plot width.
  const yAxis = Array.isArray(options.yAxis) ? options.yAxis[0] : options.yAxis;
  if (yAxis) {
    const digits = width > 0 && width < 420 ? 0 : 2;
    yAxis.axisLabel = Object.assign(yAxis.axisLabel || {}, {
      formatter: v => fmtHr(v, true, digits)
    });
  }
  return options;
}

let resizeTimer = null;
window.addEventListener('resize', () => {
  // Debounced: a drag-resize fires continuously, and each pass re-lays out the
  // whole chart. The CSS height is viewport-relative, so this is also what
  // makes the canvas follow it — ECharts does not track CSS size on its own.
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    myChart.resize();
    if (!chartOptions) return;
    // Carry the user's current legend toggles across the merge, or this would
    // re-apply whichever ones were live when the chart was last fetched.
    const shown = chartLegendSelected();
    if (shown && chartOptions.legend) chartOptions.legend.selected = shown;
    myChart.setOption(applyResponsiveLayout(chartOptions));
  }, 150);
});

document.getElementById('theme-toggle').addEventListener('click', () => {
  applyTheme(currentTheme() === 'light' ? 'carbon' : 'light');
  if (!chartCollapsed()) loadChart(selectedWindow); // re-skin chart from the new theme's CSS vars
});

// ── Mobile nav drawer ────────────────────────────────────────────────────────
const railEl = document.querySelector('.rail');
const burgerEl = document.getElementById('nav-burger');
burgerEl.addEventListener('click', () => {
  const open = railEl.classList.toggle('nav-open');
  burgerEl.setAttribute('aria-expanded', open ? 'true' : 'false');
});
// Any tap inside the drawer that navigates or opens a modal also closes it.
document.getElementById('rail-nav').addEventListener('click', () => {
  railEl.classList.remove('nav-open');
  burgerEl.setAttribute('aria-expanded', 'false');
});

// ── Chart collapse toggle ────────────────────────────────────────────────────
// Persisted like the theme choice; while collapsed the periodic chart fetch
// is skipped, and expanding re-fetches so the chart is current immediately.
const CHART_COLLAPSED_KEY = 'chartCollapsed';
function chartCollapsed() {
  try { return localStorage.getItem(CHART_COLLAPSED_KEY) === '1'; } catch (_) { return false; }
}
function applyChartCollapsed(collapsed) {
  try { localStorage.setItem(CHART_COLLAPSED_KEY, collapsed ? '1' : '0'); } catch (_) {}
  document.getElementById('hashrate-chart').style.display = collapsed ? 'none' : '';
  document.getElementById('chart-window-label').style.display = collapsed ? 'none' : '';
  document.getElementById('chart-toggle').textContent = collapsed ? 'Show' : 'Hide';
  if (!collapsed) {
    myChart.resize(); // container was display:none; ECharts needs a re-measure
    // loadChart re-runs applyResponsiveLayout against the width we just measured.
    loadChart(selectedWindow);
  }
}
document.getElementById('chart-toggle').addEventListener('click', () => {
  applyChartCollapsed(!chartCollapsed());
});

// ── Formatters ───────────────────────────────────────────────────────────────
// `digits` defaults to 2; the chart's y-axis drops to 0 on narrow screens so a
// tick reads '25 T' rather than '25.00 T'.
function fmtHr(hps, short, digits) {
  const d = digits === undefined ? 2 : digits;
  if (hps >= 1e21) return (hps / 1e21).toFixed(d) + (short ? ' Z'  : ' ZH/s');
  if (hps >= 1e18) return (hps / 1e18).toFixed(d) + (short ? ' E'  : ' EH/s');
  if (hps >= 1e15) return (hps / 1e15).toFixed(d) + (short ? ' P'  : ' PH/s');
  if (hps >= 1e12) return (hps / 1e12).toFixed(d) + (short ? ' T'  : ' TH/s');
  if (hps >= 1e9)  return (hps / 1e9 ).toFixed(d) + (short ? ' G'  : ' GH/s');
  if (hps >= 1e6)  return (hps / 1e6 ).toFixed(d) + (short ? ' M'  : ' MH/s');
  if (hps >= 1e3)  return (hps / 1e3 ).toFixed(d) + (short ? ' K'  : ' KH/s');
  return hps.toFixed(0) + (short ? ''    : ' H/s');
}

function fmtDiff(d) {
  if (d >= 1e12) return (d / 1e12).toFixed(2) + 'T';
  if (d >= 1e9)  return (d / 1e9 ).toFixed(2) + 'G';
  if (d >= 1e6)  return (d / 1e6 ).toFixed(2) + 'M';
  if (d >= 1e3)  return (d / 1e3 ).toFixed(1) + 'K';
  return d.toString();
}

function fmtNextAdjustment(height) {
  if (!height || height <= 0) return '—';
  // Difficulty retargets every 2016 blocks; estimate ~10 min/block.
  const into = height % 2016;
  const blocksLeft = 2016 - into;
  const secs = blocksLeft * 600;
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const eta = d > 0 ? (d + 'd ' + h + 'h') : (h + 'h');
  return '~' + eta + ' (' + blocksLeft + ' blk)';
}

// Estimated difficulty change at the next retarget. Computed on the backend from
// accurate epoch block timestamps (rpc.estimate_difficulty_change_pct); null
// until first polled or right after a retarget.
function fmtAdjustmentPct(pct) {
  if (pct === null || pct === undefined || !isFinite(pct)) {
    return { text: '—', color: 'var(--muted)' };
  }
  const sign = pct >= 0 ? '+' : '';
  // Difficulty up = harder for miners (red), down = easier (green).
  const color = pct > 0.05 ? 'var(--bad)' : (pct < -0.05 ? 'var(--ok)' : 'var(--muted)');
  return { text: sign + pct.toFixed(2) + '%', color };
}

function fmtUptime(secs) {
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  if (d) return d + 'd ' + h + 'h';
  if (h) return h + 'h ' + m + 'm';
  if (m) return m + 'm ' + s + 's';
  return s + 's';
}

function fmtTimestamp(ts) {
  if (!ts || ts === 0) return '—';
  return new Date(ts * 1000).toLocaleString();
}

// ── Chart ────────────────────────────────────────────────────────────────────

// Which series the user currently has toggled on, or null before the chart has
// ever been drawn. Read back off the live chart so legend clicks survive the
// notMerge setOption that each poll performs.
function chartLegendSelected() {
  const opt = myChart.getOption();
  if (!opt || !opt.legend || !opt.legend[0]) return null;
  return opt.legend[0].selected || null;
}

async function loadChart(window) {
  try {
    const resp = await fetch('/chart?window=' + window);
    if (!resp.ok) return;
    const options = await resp.json();
    // Skin the server-built option object from the active theme's CSS vars,
    // and patch in JS formatter callbacks that cannot be serialised from Rust.
    const muted = cssVar('--muted'), grid = cssVar('--grid'), surface = cssVar('--surface2'),
          border = cssVar('--border'), text = cssVar('--text');
    const yAxis = Array.isArray(options.yAxis) ? options.yAxis[0] : options.yAxis;
    if (yAxis) {
      // The formatter is applyResponsiveLayout's — its precision depends on width.
      yAxis.axisLabel = Object.assign(yAxis.axisLabel || {}, { color: muted });
      yAxis.splitLine = { lineStyle: { color: grid } };
    }
    const xAxis = Array.isArray(options.xAxis) ? options.xAxis[0] : options.xAxis;
    if (xAxis) {
      xAxis.splitLine = { lineStyle: { color: grid } };
      xAxis.axisLabel = Object.assign(xAxis.axisLabel || {}, {
        color: muted,
        formatter: v => {
          const d = new Date(v);
          if (d.getHours() === 0 && d.getMinutes() === 0) {
            return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
          }
          return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });
        }
      });
    }
    // Keyed by series name, not by array position: the server draws longest
    // window first so the 1m line lands on top, while the legend lists them
    // shortest first. Indexing by position would have swapped every colour.
    const light = currentTheme() === 'light';
    const palette = {
      '1m':  light ? '#2563eb' : '#60a5fa',
      '5m':  light ? '#047857' : '#34d399',
      '10m': light ? '#b45309' : '#f59e0b',
      '1h':  light ? '#dc2626' : '#f87171',
      '6h':  light ? '#7c3aed' : '#a78bfa',
      '24h': light ? '#0e7490' : '#22d3ee'
    };
    const series = Array.isArray(options.series) ? options.series : [options.series].filter(Boolean);
    const legendOrder = (options.legend && options.legend.data) || series.map(s => s.name);
    options.color = legendOrder.map(name => palette[name]).filter(Boolean);
    series.forEach(line => {
      line.smooth = false;
      line.lineStyle = Object.assign(line.lineStyle || {}, { color: palette[line.name] });
    });
    if (options.legend) {
      options.legend.textStyle = { color: muted, fontSize: 11 };
      // The server sends a default show/hide map on every poll. Without this
      // the chart would undo the user's legend clicks once per refresh.
      const shown = chartLegendSelected();
      if (shown) options.legend.selected = shown;
    }
    if (options.tooltip) {
      options.tooltip.backgroundColor = surface;
      options.tooltip.borderColor = border;
      options.tooltip.textStyle = { color: text, fontSize: 12 };
      options.tooltip.formatter = params => {
        if (!params || !params.length) return '';
        const pt = params[0];
        const ts = Array.isArray(pt.value) ? pt.value[0] : pt.value;
        const date = new Date(ts).toLocaleString([], { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
        // Series order is longest-window-first for drawing; list the rows the
        // way the legend reads instead.
        const order = ['1m', '5m', '10m', '1h', '6h', '24h'];
        const rows = params
          .filter(p => Array.isArray(p.value) && p.value[1] !== null && p.value[1] !== undefined)
          .sort((a, b) => order.indexOf(a.seriesName) - order.indexOf(b.seriesName))
          .map(p => '<span style="color:' + p.color + '">&#9632;</span> ' + p.seriesName + ': ' + fmtHr(p.value[1], false));
        return date + '<br/>' + rows.join('<br/>');
      };
    }
    chartOptions = applyResponsiveLayout(options);
    myChart.setOption(chartOptions, true);
  } catch (e) {
    console.error('Chart fetch error:', e);
  }
}

// ── Stats refresh ────────────────────────────────────────────────────────────
async function refresh() {
  try {
    const resp = await fetch('/stats');
    if (!resp.ok) return;
    const d = await resp.json();

    const reported10m = d.total_hashrate_10m || 0;

    document.getElementById('v-reported-current').textContent = fmtHr(reported10m, false);
    document.getElementById('v-reported-1m').textContent = '1m: ' + fmtHr(d.total_hashrate_60s || 0, false);
    document.getElementById('v-reported-1h').textContent = '1h: ' + fmtHr(d.total_hashrate_1h || 0, false);
    document.getElementById('v-reported-6h').textContent = '6h: ' + fmtHr(d.total_hashrate_6h || 0, false);
    document.getElementById('v-reported-24h').textContent = '24h: ' + fmtHr(d.total_hashrate_24h || 0, false);

    updateProbability(d.total_hashrate_10m || 0, d.network_hashrate_hps || 0);

    document.getElementById('v-miners').textContent = d.connected_miners;

    // Flash on new block height
    if (d.current_height !== lastBlockHeight) {
      const heightEl = document.getElementById('v-height');
      heightEl.classList.remove('block-new');
      // Trigger reflow to restart animation
      void heightEl.offsetWidth;
      heightEl.classList.add('block-new');
      lastBlockHeight = d.current_height;
    }
    document.getElementById('v-height').textContent = d.current_height.toLocaleString();
    document.getElementById('rail-height').textContent = d.current_height.toLocaleString();
    if (d.current_block_transaction_count != null) {
      document.getElementById('v-block-transaction-count').textContent = 'Txs: ' + d.current_block_transaction_count.toLocaleString();
    }
    if (d.current_coinbase_value) {
      const btc = d.current_coinbase_value / 1e8;
      document.getElementById('v-block-reward').textContent = 'Reward: ' + btc.toFixed(8) + ' BTC';
    }
    document.getElementById('v-last-block-worker').textContent = d.last_block_worker || '—';
    document.getElementById('v-last-block-payout').textContent = d.last_block_payout || '—';
    document.getElementById('v-last-block-hash').textContent = d.last_block_hash || '—';
    document.getElementById('v-last-block-time').textContent = fmtTimestamp(d.last_block_ts);
    document.getElementById('v-best-share').textContent = fmtDiff(d.best_share_difficulty);
    document.getElementById('v-session-best-share').textContent = fmtDiff(d.session_best_share_difficulty);
    document.getElementById('v-best-over-network').textContent = d.best_share_difficulty >= Math.ceil(d.network_difficulty) ? 'YES' : 'no';

    // Network section (human-readable hashrate + difficulty + next-adjustment ETA)
    document.getElementById('v-net-hashrate').textContent = fmtHr(d.network_hashrate_hps || 0, false);
    document.getElementById('v-net-diff').textContent = 'Diff: ' + fmtDiff(d.network_difficulty || 0);
    document.getElementById('v-net-next-adj').textContent = fmtNextAdjustment(d.current_height || 0);
    const adj = fmtAdjustmentPct(d.est_difficulty_change_pct);
    const adjEl = document.getElementById('v-net-adj-pct');
    adjEl.textContent = 'Est. move: ' + adj.text;
    adjEl.style.color = adj.color;
    // BIP110/RDTS: bit 4 of the template version. Zero means no template yet.
    const bipEl = document.getElementById('v-bip110');
    if (d.template_version) {
      const signaling = (d.template_version & (1 << 4)) !== 0;
      bipEl.textContent = signaling ? 'Signaling' : 'Not signaling';
      bipEl.style.color = signaling ? 'var(--accent)' : '';
    } else {
      bipEl.textContent = '—';
      bipEl.style.color = '';
    }
    document.getElementById('v-session-best-hashrate').textContent = fmtHr(d.session_best_hashrate_hps, false);
    document.getElementById('v-best-hashrate').textContent = fmtHr(d.best_hashrate_hps, false);
    document.getElementById('v-uptime').textContent = fmtUptime(d.uptime_secs);
    document.getElementById('server-uptime').textContent = 'Uptime ' + fmtUptime(d.uptime_secs);

    const total = d.shares_accepted + d.shares_rejected;
    const rejectPct = total > 0 ? (d.shares_rejected / total * 100).toFixed(1) : '0.0';
    const staleTotal = Array.isArray(d.worker_states) ? d.worker_states.reduce((sum, w) => sum + (w.shares_stale || 0), 0) : 0;
    const stalePct = total > 0 ? (staleTotal / total * 100).toFixed(1) : '0.0';
    const reasonTotals = {};
    (Array.isArray(d.worker_states) ? d.worker_states : []).forEach(w => {
      Object.entries(w.reject_reasons || {}).forEach(([r, n]) => {
        reasonTotals[r] = (reasonTotals[r] || 0) + n;
      });
    });
    const otherReasons = Object.entries(reasonTotals)
      .filter(([r, n]) => r !== 'stale' && n > 0)
      .sort((a, b) => b[1] - a[1])
      .map(([r, n]) => `${rejectLabel(r)}: ${n.toLocaleString()}`)
      .join(' · ');

    document.getElementById('v-reject-rate').textContent = `${d.shares_rejected.toLocaleString()} (${rejectPct}%)`;
    document.getElementById('v-stale-rate').textContent =
      `Stale: ${staleTotal.toLocaleString()} (${stalePct}%)` + (otherReasons ? ` · ${otherReasons}` : '');

    const workers = Array.isArray(d.worker_states) ? d.worker_states : [];
    const onlineCount = workers.filter(w => w.online).length;
    const offlineCount = workers.filter(w => !w.online).length;
    const nowSecKpi = Math.floor(Date.now() / 1000);
    const degradedCount = workers.filter(w => isDegraded(w, nowSecKpi)).length;

    document.getElementById('v-workers-online').textContent = 'Online: ' + onlineCount;
    document.getElementById('v-workers-offline').textContent = 'Offline: ' + offlineCount;
    document.getElementById('v-workers-degraded').textContent = 'Degraded: ' + degradedCount;

    // Workers table
    const tbody = document.getElementById('workers-tbody');
    if (workers.length === 0) {
      tbody.innerHTML = '<tr><td colspan="15" class="empty-row">No connected workers</td></tr>';
    } else {
      tbody.innerHTML = workers
        .sort((a, b) => b.hashrate_60s_hps - a.hashrate_60s_hps)
        .map(w => {
          const workerName = w.worker.includes('.') ? w.worker.split('.')[1] : w.worker;
          const nowSec = Math.floor(Date.now() / 1000);
          const lastShareAgo = w.last_submit_ts > 0 ? fmtUptime(nowSec - w.last_submit_ts) : '—';
          const uptime = w.connected_ts > 0 ? fmtUptime(nowSec - w.connected_ts) : '—';
          const mode = (w.protocol || 'sv1').toUpperCase();
          const led = workerLed(w, nowSec);
          return `<tr>
            <td>${escHtml(workerName)}</td>
            <td class="col-led"><span class="led ${led.cls}" title="${led.title}"></span></td>
            <td>${mode}</td>
            <td>${fmtDiff(w.current_vardiff)}</td>
            <td>${fmtHr(w.hashrate_60s_hps, false)}</td>
            <td>${fmtHr(w.hashrate_5m_hps, false)}</td>
            <td>${fmtHr(w.hashrate_10m_hps, false)}</td>
            <td>${fmtHr(w.hashrate_1h_hps, false)}</td>
            <td>${fmtHr(w.hashrate_6h_hps, false)}</td>
            <td>${fmtHr(w.hashrate_24h_hps, false)}</td>
            <td>${w.shares_accepted.toLocaleString()}</td>
            <td title="${rejectBreakdown(w)}">${w.shares_rejected.toLocaleString()}</td>
            <td>${fmtDiff(w.best_share_difficulty)}</td>
            <td>${lastShareAgo}</td>
            <td>${uptime}</td>
          </tr>`;
        })
        .join('');
    }

    document.getElementById('last-updated').textContent = 'Updated ' + new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' });
    lastStatsOk = Date.now();
  } catch (e) {
    console.error('Dashboard refresh error:', e);
  }
  updateConnLed();
}

const REJECT_LABELS = {
  stale: 'Stale',
  duplicate: 'Duplicate',
  low_difficulty: 'Low diff',
  job_not_found: 'Unknown job',
  bad_extranonce: 'Bad extranonce',
  invalid: 'Invalid',
};

function rejectLabel(reason) {
  return REJECT_LABELS[reason] || reason;
}

function rejectBreakdown(w) {
  const parts = Object.entries(w.reject_reasons || {})
    .filter(([, n]) => n > 0)
    .sort((a, b) => b[1] - a[1])
    .map(([r, n]) => `${rejectLabel(r)}: ${n.toLocaleString()}`);
  return parts.length ? parts.join(', ') : 'No rejects';
}

function fmtOdds(p) {
  if (p <= 0) return '—';
  const inv = Math.round(1 / p);
  if (inv >= 1e9)  return '1 in ' + (inv / 1e9).toFixed(1) + 'B';
  if (inv >= 1e6)  return '1 in ' + (inv / 1e6).toFixed(2) + 'M';
  if (inv >= 1e3)  return '1 in ' + (inv / 1e3).toFixed(1) + 'K';
  return '1 in ' + inv.toLocaleString();
}

function updateProbability(ourHps, netHps) {
  const el = id => document.getElementById(id);
  if (!ourHps || !netHps || netHps === 0) {
    el('v-prob-daily').textContent   = 'Daily: —';
    el('v-prob-monthly').textContent = 'Monthly: —';
    el('v-prob-yearly').textContent  = 'Yearly: —';
    el('v-prob-powerball').textContent = 'vs Powerball: —';
    return;
  }
  // Probability of finding a block per block (~10 min)
  const pBlock = ourHps / netHps;
  // Blocks per period
  const blocksPerDay   = 144;
  const blocksPerMonth = blocksPerDay * 30;
  const blocksPerYear  = blocksPerDay * 365;
  // P(at least one block in N blocks) = 1 - (1 - pBlock)^N
  const pDaily   = 1 - Math.pow(1 - pBlock, blocksPerDay);
  const pMonthly = 1 - Math.pow(1 - pBlock, blocksPerMonth);
  const pYearly  = 1 - Math.pow(1 - pBlock, blocksPerYear);
  // Powerball jackpot: 1 in 292,201,338 per ticket
  const pPowerball = 1 / 292201338;
  const ratio = pDaily / pPowerball;
  const vsText = ratio >= 1
    ? (ratio.toFixed(1) + '× better than Powerball')
    : ((1 / ratio).toFixed(1) + '× worse than Powerball');

  el('v-prob-daily').textContent   = 'Daily: '   + fmtOdds(pDaily);
  el('v-prob-monthly').textContent = 'Monthly: ' + fmtOdds(pMonthly);
  el('v-prob-yearly').textContent  = 'Yearly: '  + fmtOdds(pYearly);
  el('v-prob-powerball').textContent = vsText;
}

function attachTimeframeSelector() {
  const group = document.getElementById('chart-window-label');
  group.addEventListener('click', event => {
    const button = event.target.closest('.timeframe-btn');
    if (!button) return;
    selectedWindow = button.dataset.window;
    group.querySelectorAll('.timeframe-btn').forEach(item => {
      item.classList.toggle('active', item === button);
    });
    loadChart(selectedWindow);
  });
}

function escHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

const PAIR_KEY = 'btcPair';
const PAIRS = ['USD', 'EUR', 'GBP', 'CAD', 'AUD', 'CHF', 'JPY'];
function currentPair() {
  try {
    const p = localStorage.getItem(PAIR_KEY);
    return PAIRS.includes(p) ? p : 'USD';
  } catch (_) { return 'USD'; }
}

let lastBtcPrice = null;
async function fetchBtcPrice() {
  const pair = currentPair();
  try {
    const vs = pair.toLowerCase();
    const resp = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=' + vs + '&include_24hr_change=true');
    if (!resp.ok) return;
    const data = await resp.json();
    if (pair !== currentPair()) return; // pair switched while the fetch was in flight
    const price = data?.bitcoin?.[vs];
    const change = data?.bitcoin?.[vs + '_24h_change'];
    if (price != null) {
      const el = document.getElementById('v-btc-price-num');
      el.textContent = new Intl.NumberFormat(undefined, {
        style: 'currency', currency: pair, maximumFractionDigits: 0,
      }).format(price);
      if (lastBtcPrice != null && price !== lastBtcPrice) {
        el.classList.remove('price-up', 'price-down');
        void el.offsetWidth; // restart the animation
        el.classList.add(price > lastBtcPrice ? 'price-up' : 'price-down');
      }
      lastBtcPrice = price;
    }
    if (change != null) {
      const chEl = document.getElementById('v-btc-change');
      chEl.textContent = (change >= 0 ? '+' : '') + change.toFixed(2) + '% (24h)';
      chEl.classList.remove('ok', 'bad');
      chEl.classList.add(change >= 0 ? 'ok' : 'bad');
    }
  } catch (_) {}
}

const pairSelect = document.getElementById('pair-select');
pairSelect.value = currentPair();
pairSelect.addEventListener('change', () => {
  try { localStorage.setItem(PAIR_KEY, pairSelect.value); } catch (_) {}
  lastBtcPrice = null; // a currency switch is not a price move; don't pulse
  fetchBtcPrice();
});

// ── Network badge ────────────────────────────────────────────────────────────
function updateNetBadge(network) {
  const badge = document.getElementById('net-badge');
  if (network && network !== 'mainnet') {
    badge.textContent = network;
    badge.hidden = false;
  } else {
    badge.hidden = true;
  }
}

// ── Scroll spy for the rail nav ──────────────────────────────────────────────
// Position-based: the active link is the last section whose top has scrolled
// above a threshold near the top of the viewport. Robust for short sections and
// the final section (which an IntersectionObserver band can miss).
const navLinks = Array.from(document.querySelectorAll('#rail-nav a[data-section]'));
function updateActiveNav() {
  let current = navLinks[0];
  for (const link of navLinks) {
    const sec = document.getElementById(link.dataset.section);
    if (sec && sec.getBoundingClientRect().top <= 140) current = link;
  }
  navLinks.forEach(l => l.classList.toggle('active', l === current));
}
window.addEventListener('scroll', updateActiveNav, { passive: true });
window.addEventListener('resize', updateActiveNav);
// Immediate feedback on click (before the smooth-scroll settles).
navLinks.forEach(l => l.addEventListener('click', () => {
  navLinks.forEach(x => x.classList.remove('active'));
  l.classList.add('active');
}));
updateActiveNav();

// ── Connect modal ─────────────────────────────────────────────────────────────
const connectModal = document.getElementById('connect-modal');
async function openConnect() {
  try {
    const resp = await fetch('/api/info');
    if (resp.ok) {
      const i = await resp.json();
      const host = window.location.hostname || 'your-pool-host';
      document.getElementById('connect-url').value = 'stratum+tcp://' + host + ':' + i.stratum_port;
      document.getElementById('connect-proto').textContent = i.sv2_enabled
        ? 'Stratum V1 and V2 (Noise-encrypted) are auto-detected on this one port — point any miner here.'
        : 'Stratum V1 on this port (SV2 is disabled in this pool’s config).';
      document.getElementById('connect-address').textContent = i.username_format;
      document.getElementById('connect-authority-field').hidden = !i.sv2_authority_pubkey;
      document.getElementById('connect-authority').value = i.sv2_authority_pubkey || '—';
      document.getElementById('connect-version').textContent = 'v' + i.version;
      document.getElementById('connect-network').textContent = i.network;
      updateNetBadge(i.network);
    }
  } catch (e) { console.error('Info fetch error:', e); }
  connectModal.showModal();
}
document.getElementById('open-connect').addEventListener('click', openConnect);
document.getElementById('close-connect').addEventListener('click', () => connectModal.close());
connectModal.addEventListener('click', e => { if (e.target === connectModal) connectModal.close(); });
function wireCopy(inputId, btnId) {
  document.getElementById(btnId).addEventListener('click', () => {
    const el = document.getElementById(inputId);
    const btn = document.getElementById(btnId);
    const done = ok => { const p = btn.textContent; btn.textContent = ok ? 'Copied' : 'Copy manually'; setTimeout(() => btn.textContent = p, 1500); };
    // navigator.clipboard only exists in secure contexts (HTTPS/localhost);
    // this dashboard is usually plain HTTP on the LAN, so fall back to
    // selecting the text and execCommand('copy'). The selection is left in
    // place so a manual Ctrl/Cmd+C works if even that fails.
    const legacy = () => {
      el.focus(); el.select(); el.setSelectionRange(0, el.value.length);
      let ok = false;
      try { ok = document.execCommand('copy'); } catch (e) { ok = false; }
      done(ok);
    };
    if (navigator.clipboard && window.isSecureContext) navigator.clipboard.writeText(el.value).then(() => done(true)).catch(legacy);
    else legacy();
  });
}
wireCopy('connect-url', 'connect-copy');
wireCopy('connect-authority', 'connect-authority-copy');

attachTimeframeSelector();
if (chartCollapsed()) {
  applyChartCollapsed(true);
} else {
  loadChart(DEFAULT_WINDOW);
}
updateConnLed();
refresh();
fetchBtcPrice();
setInterval(refresh, 10000);
// Re-evaluate the connectivity LED between refreshes so it goes stale on its
// own even if refresh() stops landing (server down, tab throttled, etc.).
setInterval(updateConnLed, 5000);
// Matches the pool's snapshot interval, so the chart gains a point as soon as
// one exists rather than up to a minute later.
setInterval(() => { if (!chartCollapsed()) loadChart(selectedWindow); }, 10000);
setInterval(fetchBtcPrice, 60000);
</script>
</body>
</html>"##
);

#[cfg(test)]
mod tests {
    use super::{build_chart_option, chart_window, ChartWindow, DASHBOARD_HTML};
    use std::collections::HashSet;

    /// Every element id the embedded JS looks up must exist in the markup —
    /// a missing one throws inside refresh() and kills the whole update loop.
    #[test]
    fn all_ids_referenced_by_js_exist_in_markup() {
        let mut wanted = HashSet::new();
        // Direct lookups, plus updateProbability's `el('…')` helper shorthand.
        for pat in ["getElementById('", "el('"] {
            for (idx, _) in DASHBOARD_HTML.match_indices(pat) {
                let rest = &DASHBOARD_HTML[idx + pat.len()..];
                if let Some(end) = rest.find('\'') {
                    wanted.insert(&rest[..end]);
                }
            }
        }
        // Sanity: the scrape itself worked.
        assert!(wanted.len() > 20, "id scrape found too few: {wanted:?}");

        let missing: Vec<&&str> = wanted
            .iter()
            .filter(|id| !DASHBOARD_HTML.contains(&format!("id=\"{id}\"")))
            .collect();
        assert!(
            missing.is_empty(),
            "ids referenced by JS but absent from markup: {missing:?}"
        );
    }

    #[test]
    fn chart_defaults_to_one_hour_with_bounded_range_buckets() {
        assert_eq!(
            chart_window(None),
            ChartWindow {
                duration_secs: Some(3_600),
                bucket_secs: crate::stats::SNAPSHOT_INTERVAL_SECS,
            }
        );
        assert_eq!(chart_window(Some("6h")).bucket_secs, 60);
        assert_eq!(chart_window(Some("24h")).bucket_secs, 300);
        assert_eq!(chart_window(Some("30d")).bucket_secs, 7_200);
        assert_eq!(chart_window(Some("unknown")), chart_window(None));
        assert!(DASHBOARD_HTML.contains("const DEFAULT_WINDOW = '1h'"));

        // "1m" and "6m" used to alias 30d/180d, colliding with the 1m series
        // name. They must fall through to the default now.
        assert_eq!(chart_window(Some("1m")), chart_window(None));
        assert_eq!(chart_window(Some("6m")), chart_window(None));
    }

    /// The legend lists the windows shortest-first while the series are drawn
    /// longest-first, so the fast-moving 1m line paints on top of the slow ones
    /// instead of being buried under them. The two orders are deliberately
    /// opposite; this pins that down so a future tidy-up doesn't "fix" one of
    /// them into agreement.
    #[test]
    fn chart_draws_short_windows_over_long_ones() {
        let chart = build_chart_option(&[]);

        let drawn: Vec<&str> = chart["series"]
            .as_array()
            .expect("series array")
            .iter()
            .map(|s| s["name"].as_str().expect("series name"))
            .collect();
        assert_eq!(drawn, ["24h", "6h", "1h", "10m", "5m", "1m"]);

        let legend: Vec<&str> = chart["legend"]["data"]
            .as_array()
            .expect("legend data")
            .iter()
            .map(|n| n.as_str().expect("legend entry"))
            .collect();
        assert_eq!(legend, ["1m", "5m", "10m", "1h", "6h", "24h"]);

        // Every drawn series must be in the legend, or it renders with no way
        // to toggle it and no palette entry on the client.
        for name in &drawn {
            assert!(legend.contains(name), "series {name} missing from legend");
        }

        // The client keys colours by series name; a positional palette would
        // silently invert now that the two orders differ.
        assert!(
            DASHBOARD_HTML.contains("palette[line.name]"),
            "chart colours must be keyed by series name, not array index"
        );
    }

    /// `containLabel` already reserves room for the axis labels inside the grid
    /// box, so a large `left` is counted twice and renders as fixed-width dead
    /// space to the left of the y-axis — worst on a phone, where it never
    /// shrinks. The two settings are only correct together.
    #[test]
    fn chart_grid_does_not_double_count_axis_label_space() {
        let chart = build_chart_option(&[]);
        let grid = &chart["grid"];

        assert_eq!(
            grid["containLabel"], true,
            "label space must be reserved automatically"
        );
        for edge in ["left", "right", "top", "bottom"] {
            let value = &grid[edge];
            assert!(
                value.is_number(),
                "grid.{edge} must be a plain number, not a css string: {value}"
            );
        }
        // Padding only. `top` is the exception: it clears the legend.
        for edge in ["left", "right", "bottom"] {
            let px = grid[edge].as_f64().expect("numeric grid inset");
            assert!(
                px <= 16.0,
                "grid.{edge} is {px}px — too large to be padding on top of containLabel"
            );
        }
    }

    /// The x-axis rendered ~12 colliding time labels on a phone. Density is set
    /// from the rendered width client-side (the server cannot know it), with
    /// `hideOverlap` as the backstop.
    #[test]
    fn chart_axis_labels_adapt_to_the_rendered_width() {
        let chart = build_chart_option(&[]);
        assert_eq!(chart["xAxis"]["axisLabel"]["hideOverlap"], true);
        assert_eq!(chart["yAxis"]["axisLabel"]["hideOverlap"], true);

        // Density and the wrapped-legend grid offset are recomputed from the
        // measured width, not baked in server-side.
        assert!(DASHBOARD_HTML.contains("xAxis.splitNumber = Math.min"));
        assert!(DASHBOARD_HTML.contains("function applyResponsiveLayout(options)"));

        // A bare `myChart.resize()` on the resize event would leave the label
        // density — and the viewport-relative height — stale until the next poll.
        assert!(
            DASHBOARD_HTML.contains("myChart.setOption(applyResponsiveLayout(chartOptions));"),
            "resize must re-run applyResponsiveLayout, not just resize the canvas"
        );

        // The height only follows the viewport because of that resize path.
        assert!(
            DASHBOARD_HTML.contains("#hashrate-chart { height: clamp("),
            "chart height must be viewport-relative, not a fixed pixel value"
        );
    }
}
