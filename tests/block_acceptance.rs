//! End-to-end block-acceptance test — the one test that exercises the path no
//! unit test can reach: assembling a real block and having a real `bitcoind`
//! accept it onto the chain.
//!
//! It spins up `bitcoind -regtest`, launches the actual pool binary against it,
//! connects over the live Stratum V1 socket as a miner would, grinds a
//! difficulty-1 share (which on regtest is also a valid block), submits it, and
//! asserts the node accepted it: the chain advances and the new block's coinbase
//! pays the address supplied as the miner identity. Because it mines block **1** of a fresh
//! chain it also covers the BIP34 small-height coinbase encoding.
//!
//! This guards the bugs that are otherwise invisible until a real block-find:
//! prev-hash byte order, BIP34 height, merkle root, witness commitment, coinbase
//! structure, and the submit path itself.
//!
//! Ignored by default (needs `bitcoind` + `bitcoin-cli`, and the diff-1 grind
//! wants release codegen). Run it explicitly:
//!
//! ```text
//! cargo test --release --test block_acceptance -- --ignored --nocapture
//! ```
//!
//! Binaries are found via `$BITCOIND` / `$BITCOIN_CLI`, else `bitcoind` /
//! `bitcoin-cli` on `PATH`.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ─────────────────────────────────────────────────────────────────────────────
// External binaries
// ─────────────────────────────────────────────────────────────────────────────

fn bitcoind_bin() -> String {
    std::env::var("BITCOIND").unwrap_or_else(|_| "bitcoind".into())
}
fn bitcoin_cli_bin() -> String {
    std::env::var("BITCOIN_CLI").unwrap_or_else(|_| "bitcoin-cli".into())
}
fn have_binaries() -> bool {
    let probe = |b: &str| {
        Command::new(b)
            .arg("-version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    };
    probe(&bitcoind_bin()) && probe(&bitcoin_cli_bin())
}

// ─────────────────────────────────────────────────────────────────────────────
// Regtest node, controlled via bitcoin-cli
// ─────────────────────────────────────────────────────────────────────────────

struct Regtest {
    datadir: PathBuf,
    rpc_port: u16,
    child: Child,
}
impl Regtest {
    fn start(datadir: &Path, rpc_port: u16, p2p_port: u16) -> Self {
        std::fs::create_dir_all(datadir).unwrap();
        let child = Command::new(bitcoind_bin())
            .arg("-regtest")
            .arg(format!("-datadir={}", datadir.display()))
            .arg(format!("-rpcport={rpc_port}"))
            .arg(format!("-port={p2p_port}"))
            .arg("-rpcbind=127.0.0.1")
            .arg("-rpcallowip=127.0.0.1")
            .arg("-fallbackfee=0.0001")
            .arg("-server=1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn bitcoind");
        let rt = Regtest {
            datadir: datadir.to_path_buf(),
            rpc_port,
            child,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if rt.cli(&["getblockchaininfo"]).is_ok() {
                return rt;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!("bitcoind RPC did not become ready");
    }
    fn cli(&self, args: &[&str]) -> Result<String, String> {
        let out = Command::new(bitcoin_cli_bin())
            .arg("-regtest")
            .arg(format!("-datadir={}", self.datadir.display()))
            .arg(format!("-rpcport={}", self.rpc_port))
            .args(args)
            .output()
            .map_err(|e| e.to_string())?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
        } else {
            Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
        }
    }
    fn cookie_path(&self) -> PathBuf {
        self.datadir.join("regtest").join(".cookie")
    }
}
impl Drop for Regtest {
    fn drop(&mut self) {
        let _ = self.cli(&["stop"]);
        std::thread::sleep(Duration::from_millis(500));
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pool process under test
// ─────────────────────────────────────────────────────────────────────────────

struct Pool {
    child: Child,
}
impl Drop for Pool {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

// ─────────────────────────────────────────────────────────────────────────────
// SHA-256 (hardware SHA-NI via sha2::compress256, with midstate) for the grind
// ─────────────────────────────────────────────────────────────────────────────

use sha2::compress256;
use sha2::digest::generic_array::{typenum::U64, GenericArray};
use sha2::{Digest, Sha256};

const H0: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

/// One 64-byte SHA-256 block compression. `sha2` dispatches to hardware SHA
/// extensions at runtime, so this is ~10–40× the scalar path.
#[inline(always)]
fn compress(state: &mut [u32; 8], block: &[u8; 64]) {
    let ga = GenericArray::<u8, U64>::clone_from_slice(block);
    compress256(state, &[ga]);
}

/// Full SHA-256d of an arbitrary message (cold paths: coinbase, merkle).
fn sha256d(msg: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(&Sha256::digest(Sha256::digest(msg)));
    out
}

/// SHA-256d of an 80-byte block header given its 76-byte prefix and nonce, via
/// the midstate trick. Shared by the grind's inner loop logic and a correctness
/// test that pins it against the library's full hash.
fn header_hash(prefix: &[u8; 76], nonce: u32) -> [u8; 32] {
    let mut mid = H0;
    let mut blk1 = [0u8; 64];
    blk1.copy_from_slice(&prefix[..64]);
    compress(&mut mid, &blk1);

    let mut blk2 = [0u8; 64];
    blk2[..12].copy_from_slice(&prefix[64..76]);
    blk2[12..16].copy_from_slice(&nonce.to_le_bytes());
    blk2[16] = 0x80;
    blk2[62] = 0x02; // 640-bit length
    blk2[63] = 0x80;
    let mut st = mid;
    compress(&mut st, &blk2);

    let mut blk2b = [0u8; 64];
    for i in 0..8 {
        blk2b[i * 4..i * 4 + 4].copy_from_slice(&st[i].to_be_bytes());
    }
    blk2b[32] = 0x80;
    blk2b[62] = 0x01; // 256-bit length
    let mut st2 = H0;
    compress(&mut st2, &blk2b);

    let mut d = [0u8; 32];
    for i in 0..8 {
        d[i * 4..i * 4 + 4].copy_from_slice(&st2[i].to_be_bytes());
    }
    d
}

/// Exactly mirror the pool's `meets_target` against the diff-1 target: reverse
/// the 32-byte hash and require it `<= 0x00000000ffff0000…0000`. Using the same
/// predicate guarantees the miner never submits a share the pool would reject.
fn meets_diff1(d: &[u8; 32]) -> bool {
    #[rustfmt::skip]
    const T: [u8; 32] = [
        0,0,0,0,0xff,0xff,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
    ];
    for i in 0..32 {
        let hb = d[31 - i];
        if hb < T[i] {
            return true;
        }
        if hb > T[i] {
            return false;
        }
    }
    true
}

// ─────────────────────────────────────────────────────────────────────────────
// Stratum miner
// ─────────────────────────────────────────────────────────────────────────────

struct Notify {
    job_id: String,
    prev_internal: [u8; 32],
    coinb1: Vec<u8>,
    coinb2: Vec<u8>,
    merkle_branch: Vec<[u8; 32]>,
    version: u32,
    nbits: u32,
    ntime: u32,
}

fn parse_notify(v: &serde_json::Value) -> Notify {
    let p = v.get("params").unwrap().as_array().unwrap();
    let mut prev_internal = [0u8; 32];
    prev_internal.copy_from_slice(&hex::decode(p[1].as_str().unwrap()).unwrap());
    // Stratum prev-hash → header internal bytes: swap each 4-byte word.
    for chunk in prev_internal.chunks_mut(4) {
        chunk.reverse();
    }
    let merkle_branch = p[4]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| {
            let mut a = [0u8; 32];
            a.copy_from_slice(&hex::decode(b.as_str().unwrap()).unwrap());
            a
        })
        .collect();
    Notify {
        job_id: p[0].as_str().unwrap().to_string(),
        prev_internal,
        coinb1: hex::decode(p[2].as_str().unwrap()).unwrap(),
        coinb2: hex::decode(p[3].as_str().unwrap()).unwrap(),
        merkle_branch,
        version: u32::from_str_radix(p[5].as_str().unwrap(), 16).unwrap(),
        nbits: u32::from_str_radix(p[6].as_str().unwrap(), 16).unwrap(),
        ntime: u32::from_str_radix(p[7].as_str().unwrap(), 16).unwrap(),
    }
}

/// Block until a JSON line satisfying `pred` arrives; return it.
fn read_until(
    reader: &mut impl BufRead,
    pred: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(Instant::now() < deadline, "timed out reading from pool");
        let mut line = String::new();
        let n = reader.read_line(&mut line).expect("read line");
        assert!(n > 0, "pool closed connection");
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) {
            if pred(&v) {
                return v;
            }
        }
    }
}

/// Non-blocking drain of any buffered `mining.notify` lines, returning the most
/// recent job (or `current` if none arrived). Keeps the mined job fresh so a
/// slow grind doesn't submit against a job that rotated out of pool history.
fn refresh_job(reader: &mut BufReader<TcpStream>, current: Notify) -> Notify {
    // Set the timeout on the very fd the BufReader reads from.
    reader
        .get_ref()
        .set_read_timeout(Some(Duration::from_millis(150)))
        .unwrap();
    let mut latest = current;
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) {
                    if v.get("method").and_then(|m| m.as_str()) == Some("mining.notify") {
                        latest = parse_notify(&v);
                    }
                }
            }
            Err(_) => break, // WouldBlock / timeout
        }
    }
    reader.get_ref().set_read_timeout(None).unwrap();
    latest
}

/// Build the 76-byte header prefix (everything but the nonce) for a given
/// extranonce2, using the standard Stratum conventions a real ASIC uses.
fn header_prefix(job: &Notify, extranonce1: &[u8], extranonce2: &[u8]) -> [u8; 76] {
    let mut coinbase = job.coinb1.clone();
    coinbase.extend_from_slice(extranonce1);
    coinbase.extend_from_slice(extranonce2);
    coinbase.extend_from_slice(&job.coinb2);
    let mut root = sha256d(&coinbase);
    for branch in &job.merkle_branch {
        let mut buf = [0u8; 64];
        buf[..32].copy_from_slice(&root);
        buf[32..].copy_from_slice(branch);
        root = sha256d(&buf);
    }
    let mut hdr = [0u8; 76];
    hdr[0..4].copy_from_slice(&job.version.to_le_bytes());
    hdr[4..36].copy_from_slice(&job.prev_internal);
    hdr[36..68].copy_from_slice(&root);
    hdr[68..72].copy_from_slice(&job.ntime.to_le_bytes());
    hdr[72..76].copy_from_slice(&job.nbits.to_le_bytes());
    hdr
}

/// Cap on one nonce sweep. Jobs refresh every 30 seconds and each session keeps
/// 16 of them, so an id is forgotten after roughly eight minutes. Resyncing
/// every two minutes keeps slow CI runners comfortably inside that window.
const GRIND_CHUNK: Duration = Duration::from_secs(120);

/// Multi-threaded grind for a nonce whose sha256d(header) meets diff-1. Returns
/// the nonce, or None if the deadline passes or the whole 2^32 space holds no
/// solution (~37%).
fn grind_diff1(prefix: &[u8; 76], deadline: Instant) -> Option<u32> {
    let nthreads = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(4);
    let found = Arc::new(AtomicI64::new(-1));
    let stop = Arc::new(AtomicBool::new(false));

    // Midstate over header block 1 (constant across nonces), shared by threads.
    let mut midstate = H0;
    let mut blk1 = [0u8; 64];
    blk1.copy_from_slice(&prefix[..64]);
    compress(&mut midstate, &blk1);
    let tail12: [u8; 12] = prefix[64..76].try_into().unwrap();

    std::thread::scope(|s| {
        for tid in 0..nthreads {
            let found = found.clone();
            let stop = stop.clone();
            s.spawn(move || {
                let mut blk2 = [0u8; 64];
                blk2[..12].copy_from_slice(&tail12);
                blk2[16] = 0x80;
                blk2[62] = 0x02; // 640-bit length
                blk2[63] = 0x80;
                let mut blk2b = [0u8; 64];
                blk2b[32] = 0x80;
                blk2b[62] = 0x01; // 256-bit length
                let mut n = tid;
                let mut counter = 0u32;
                loop {
                    // Check the stop flag and deadline occasionally, not every
                    // iteration.
                    counter = counter.wrapping_add(1);
                    if counter % 8192 == 0
                        && (stop.load(Ordering::Relaxed) || Instant::now() >= deadline)
                    {
                        return;
                    }
                    blk2[12..16].copy_from_slice(&n.to_le_bytes());
                    let mut st = midstate;
                    compress(&mut st, &blk2);
                    for i in 0..8 {
                        blk2b[i * 4..i * 4 + 4].copy_from_slice(&st[i].to_be_bytes());
                    }
                    let mut st2 = H0;
                    compress(&mut st2, &blk2b);
                    if st2[7] == 0 {
                        let mut d = [0u8; 32];
                        for i in 0..8 {
                            d[i * 4..i * 4 + 4].copy_from_slice(&st2[i].to_be_bytes());
                        }
                        if meets_diff1(&d) {
                            found.store(n as i64, Ordering::SeqCst);
                            stop.store(true, Ordering::SeqCst);
                            return;
                        }
                    }
                    match n.checked_add(nthreads) {
                        Some(next) => n = next,
                        None => return,
                    }
                }
            });
        }
    });
    let f = found.load(Ordering::SeqCst);
    (f >= 0).then_some(f as u32)
}

#[test]
fn grind_respects_deadline() {
    let prefix = [0u8; 76];
    let started = Instant::now();
    let result = grind_diff1(&prefix, Instant::now());
    assert!(result.is_none(), "expired deadline still returned a nonce");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "grind ran {:?} past an expired deadline",
        started.elapsed()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// The test
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "needs bitcoind/bitcoin-cli; run with: cargo test --release --test block_acceptance -- --ignored --nocapture"]
fn pool_block_is_accepted_by_node() {
    if !have_binaries() {
        eprintln!("SKIP: bitcoind/bitcoin-cli not found (set $BITCOIND / $BITCOIN_CLI)");
        return;
    }

    let tmp = std::env::temp_dir().join(format!("btcpool-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let node = Regtest::start(&tmp.join("node"), free_port(), free_port());
    let rpc_port = node.rpc_port;

    node.cli(&["createwallet", "e2e"]).expect("createwallet");
    let payout = node.cli(&["getnewaddress"]).expect("getnewaddress");
    assert_eq!(node.cli(&["getblockcount"]).unwrap(), "0", "fresh chain");

    let stratum_port = free_port();
    let dash_port = free_port();
    let cookie = node.cookie_path();
    let cdl = Instant::now() + Duration::from_secs(10);
    while !cookie.exists() && Instant::now() < cdl {
        std::thread::sleep(Duration::from_millis(100));
    }
    let cfg_path = tmp.join("pool.toml");
    std::fs::write(
        &cfg_path,
        format!(
            r#"
[pool]
listen_addr = "127.0.0.1:{stratum_port}"
coinbase_tag = "/btcpool-rs-e2e/"
initial_difficulty = 1
extranonce1_size = 4
extranonce2_size = 4
max_connections = 16
idle_timeout_secs = 1800
found_block_dir = "{found}"

[bitcoin_rpc]
url = "http://127.0.0.1:{rpc_port}"
cookie_path = "{cookie}"
timeout_secs = 10

[zmq]
hashblock_endpoint = "tcp://127.0.0.1:1"
poll_fallback = true
poll_interval_ms = 500

[vardiff]
target_share_time_secs = 15
retarget_interval_secs = 60
min_difficulty = 1
max_difficulty = 65536
max_retarget_factor = 4.0

[security]
max_connections_per_ip = 64
max_shares_per_sec = 500
ban_duration_secs = 0
max_invalid_shares = 50
max_message_bytes = 8192

[metrics]
prometheus_addr = "127.0.0.1:{dash_port}"
stats_db_path = ""

[logging]
level = "warn"
json = false
"#,
            found = tmp.join("found-blocks").display(),
            cookie = cookie.display(),
        ),
    )
    .unwrap();

    let pool = Pool {
        child: Command::new(env!("CARGO_BIN_EXE_btcpool-rs"))
            .arg(&cfg_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn pool"),
    };
    assert!(
        wait_for_port(stratum_port, Duration::from_secs(20)),
        "pool stratum port never opened"
    );

    // ── Connect as an SV1 miner ───────────────────────────────────────────────
    let stream = TcpStream::connect(("127.0.0.1", stratum_port)).unwrap();
    stream.set_nodelay(true).unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let send = |w: &mut TcpStream, s: &str| {
        w.write_all(s.as_bytes()).unwrap();
        w.write_all(b"\n").unwrap();
    };

    send(
        &mut writer,
        r#"{"id":1,"method":"mining.subscribe","params":["e2e/1.0"]}"#,
    );
    let sub = read_until(&mut reader, |v| {
        v.get("id").and_then(|i| i.as_u64()) == Some(1)
    });
    let subres = sub.get("result").unwrap().as_array().unwrap();
    let extranonce1 = hex::decode(subres[1].as_str().unwrap()).unwrap();
    let en2_size = subres[2].as_u64().unwrap() as usize;
    let worker = format!("{payout}.e2e");

    send(
        &mut writer,
        &format!(r#"{{"id":2,"method":"mining.authorize","params":["{worker}","x"]}}"#),
    );
    read_until(&mut reader, |v| {
        v.get("id").and_then(|i| i.as_u64()) == Some(2)
    });
    let first = read_until(&mut reader, |v| {
        v.get("method").and_then(|m| m.as_str()) == Some("mining.notify")
    });
    let mut job = parse_notify(&first);

    // ── Grind + submit, refreshing the job per sweep, until accepted ──────────
    let t0 = Instant::now();
    let mut en2: u32 = 0;
    let mut accepted = false;
    while t0.elapsed() < Duration::from_secs(900) {
        job = refresh_job(&mut reader, job);
        en2 += 1;
        let e2_bytes = en2.to_be_bytes()[4 - en2_size..].to_vec();
        let prefix = header_prefix(&job, &extranonce1, &e2_bytes);
        let Some(nonce) = grind_diff1(&prefix, Instant::now() + GRIND_CHUNK) else {
            continue; // sweep exhausted or deadline hit; re-sync and roll
        };
        eprintln!(
            "found diff-1 nonce {nonce} in {:.0}s (job {}, en2 #{en2})",
            t0.elapsed().as_secs_f64(),
            job.job_id
        );
        send(
            &mut writer,
            &format!(
                r#"{{"id":100,"method":"mining.submit","params":["{worker}","{}","{}","{:08x}","{:08x}"]}}"#,
                job.job_id,
                hex::encode(&e2_bytes),
                job.ntime,
                nonce,
            ),
        );
        let resp = read_until(&mut reader, |v| {
            v.get("id").and_then(|i| i.as_u64()) == Some(100)
        });
        if resp.get("result").and_then(|r| r.as_bool()) == Some(true) {
            accepted = true;
            break;
        }
        eprintln!("submit not accepted ({resp}); re-syncing and retrying");
    }
    assert!(
        accepted,
        "pool never accepted a submitted block within 15 min"
    );

    // ── Verify on-chain ───────────────────────────────────────────────────────
    let mut height = String::new();
    let hdl = Instant::now() + Duration::from_secs(5);
    while Instant::now() < hdl {
        height = node.cli(&["getblockcount"]).unwrap();
        if height == "1" {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(height, "1", "node did not accept the block onto the chain");

    let hash = node.cli(&["getblockhash", "1"]).unwrap();
    let block: serde_json::Value =
        serde_json::from_str(&node.cli(&["getblock", &hash, "2"]).unwrap()).unwrap();
    let coinbase = &block["tx"][0];
    let paid = coinbase["vout"][0]["scriptPubKey"]["address"]
        .as_str()
        .unwrap();
    assert_eq!(
        paid, payout,
        "coinbase did not pay the miner identity address"
    );

    // BIP34 small-height path: block 1's coinbase scriptSig begins with OP_1.
    let script_sig = coinbase["vin"][0]["coinbase"].as_str().unwrap();
    assert!(
        script_sig.starts_with("51"),
        "block-1 coinbase scriptSig should start with OP_1 (0x51): {script_sig}"
    );

    let archived = std::fs::read_dir(tmp.join("found-blocks"))
        .map(|d| d.count())
        .unwrap_or(0);
    assert!(archived >= 1, "found-block hex was not archived");

    eprintln!("✅ block 1 accepted by node; coinbase pays {paid}");

    drop(pool);
    drop(node);
    let _ = std::fs::remove_dir_all(&tmp);
}

// ─────────────────────────────────────────────────────────────────────────────
// Fast correctness checks (no bitcoind) — these run in normal `cargo test` and
// guard the miner's midstate hashing so the heavy grind can be trusted.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn header_hash_matches_full_sha256d() {
    let mut hdr = [0u8; 80];
    for (i, b) in hdr.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add(7);
    }
    let nonce = u32::from_le_bytes([hdr[76], hdr[77], hdr[78], hdr[79]]);
    let mut prefix = [0u8; 76];
    prefix.copy_from_slice(&hdr[..76]);
    // The midstate path must equal a plain double-SHA-256 of the 80-byte header.
    assert_eq!(header_hash(&prefix, nonce), sha256d(&hdr));
}

#[test]
fn meets_diff1_extremes() {
    assert!(meets_diff1(&[0x00; 32]), "zero hash is below any target");
    assert!(!meets_diff1(&[0xff; 32]), "max hash exceeds diff-1");
}
