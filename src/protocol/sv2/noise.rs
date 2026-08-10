//! protocol/sv2/noise.rs
//!
//! Stratum V2 Noise transport (pool = responder).
//!
//! SV2 secures the connection with the `Noise_NX_Secp256k1+EllSwift_ChaChaPoly_SHA256`
//! handshake: the initiator (miner) sends a 64-byte ElligatorSwift ephemeral key,
//! the responder (pool) replies with its ephemeral + encrypted static key + a
//! signed certificate, after which all frames are AEAD-encrypted.
//!
//! Devices such as the NerdQAxe++ require this — they will not speak plaintext
//! SV2. We use the SRI `noise_sv2` responder for the handshake and `codec_sv2`'s
//! noise codec for the encrypted transport.
//!
//! The certificate is signed by the pool's authority key. By default that key
//! persists in `[sv2] authority_key_file` so the base58check-encoded public
//! key (logged at startup, shown on the dashboard Settings page) can be pinned
//! in the miner's configuration and survives restarts. Setting
//! `persist_authority_key = false` reverts to a fresh key per process, which
//! any pinning miner will reject after a restart.
use anyhow::{anyhow, Context, Result};
use codec_sv2::{NoiseEncoder, StandardNoiseDecoder, State};
use framing_sv2::framing::{Frame, Sv2Frame};
use key_utils::{Secp256k1PublicKey, Secp256k1SecretKey};
use noise_sv2::{Responder, ELLSWIFT_ENCODING_SIZE};
use secp256k1::{Keypair, Secp256k1, SecretKey};
use std::sync::{Arc, OnceLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    net::TcpStream,
    sync::Mutex,
};
use tracing::{debug, warn};

use super::messages;
use crate::config::Sv2Config;

/// Phantom message type for the codec generics. The decoder never deserializes
/// into it (we read raw payload bytes) and the encoder is fed already-serialized
/// frames, so any no-lifetime SV2 message that implements the codec bounds works.
type Marker = mining_sv2::SubmitSharesSuccess;
type Decoder = StandardNoiseDecoder<Marker>;
type Encoder = NoiseEncoder<Marker>;

/// Process-wide authority key + certificate validity, set once by [`init`] at
/// boot. Falls back to an ephemeral key with the default validity if [`init`]
/// was never called (unit tests, defensive).
struct Authority {
    secret: [u8; 32],
    cert_validity_secs: u32,
}

static AUTHORITY: OnceLock<Authority> = OnceLock::new();

/// Initialise the process-wide Noise authority from config: load the secret
/// key from `authority_key_file` (creating it on first start) when
/// `persist_authority_key` is true, otherwise generate an ephemeral key.
/// Returns the base58check-encoded authority public key (the string a miner
/// pins to verify pool identity).
pub fn init(cfg: &Sv2Config) -> Result<String> {
    let secret = if cfg.persist_authority_key {
        load_or_generate_secret(&crate::config::expand_tilde(&cfg.authority_key_file))?
    } else {
        generate_secret()
    };
    let authority = Authority {
        secret,
        cert_validity_secs: cfg.cert_validity_secs,
    };
    let authority = AUTHORITY.get_or_init(|| authority);
    Ok(encode_public_key(&authority.secret))
}

fn authority() -> &'static Authority {
    AUTHORITY.get_or_init(|| Authority {
        secret: generate_secret(),
        cert_validity_secs: 365 * 24 * 60 * 60,
    })
}

fn generate_secret() -> [u8; 32] {
    let secp = Secp256k1::new();
    let (sk, _pk) = secp.generate_keypair(&mut rand::thread_rng());
    sk.secret_bytes()
}

/// Base58check encoding of the authority public key, in the SRI `key-utils`
/// format miners expect (2-byte version prefix + 32-byte x-only key).
fn encode_public_key(secret: &[u8; 32]) -> String {
    let sk = SecretKey::from_slice(secret).expect("valid authority secret");
    Secp256k1PublicKey::from(Secp256k1SecretKey(sk)).to_string()
}

/// Read the base58check secret key from `path`, or generate one and write it
/// there (owner-only permissions) if the file does not exist yet — the same
/// create-on-first-use pattern as bitcoind's `.cookie`.
fn load_or_generate_secret(path: &std::path::Path) -> Result<[u8; 32]> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let key: Secp256k1SecretKey = contents
                .trim()
                .parse()
                .map_err(|e| anyhow!("{e:?}"))
                .with_context(|| {
                    format!(
                        "Parsing SV2 authority key file {} (base58check secret key). \
                         Delete the file to generate a fresh key.",
                        path.display()
                    )
                })?;
            Ok(key.into_bytes())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let secret = generate_secret();
            let sk = SecretKey::from_slice(&secret).expect("valid generated secret");
            let encoded = Secp256k1SecretKey(sk).to_string();
            if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
                std::fs::create_dir_all(dir)
                    .with_context(|| format!("Creating directory {}", dir.display()))?;
            }
            write_owner_only(path, &encoded)
                .with_context(|| format!("Writing SV2 authority key file {}", path.display()))?;
            tracing::info!("Generated new SV2 authority key: {}", path.display());
            Ok(secret)
        }
        Err(e) => {
            Err(e).with_context(|| format!("Reading SV2 authority key file {}", path.display()))
        }
    }
}

#[cfg(unix)]
fn write_owner_only(path: &std::path::Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(contents.as_bytes())?;
    f.write_all(b"\n")
}

#[cfg(not(unix))]
fn write_owner_only(path: &std::path::Path, contents: &str) -> std::io::Result<()> {
    std::fs::write(path, format!("{contents}\n"))
}

fn io_err(msg: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string())
}

/// Run the responder side of the Noise handshake on the raw stream, returning a
/// transport-mode codec [`State`] ready for encrypted framing.
///
/// Wire sequence (no length prefixes — fixed sizes):
///   initiator → 64-byte ElligatorSwift ephemeral key
///   responder → `INITIATOR_EXPECTED_HANDSHAKE_MESSAGE_SIZE`-byte reply
pub async fn responder_handshake(stream: &mut TcpStream) -> Result<State> {
    let auth = authority();
    responder_handshake_with(stream, &auth.secret, auth.cert_validity_secs).await
}

async fn responder_handshake_with(
    stream: &mut TcpStream,
    secret: &[u8; 32],
    cert_validity_secs: u32,
) -> Result<State> {
    let secp = Secp256k1::new();
    let sk = SecretKey::from_slice(secret).expect("valid authority secret");
    let kp = Keypair::from_secret_key(&secp, &sk);
    let mut responder = Responder::new(kp, cert_validity_secs);

    let mut re_pub = [0u8; ELLSWIFT_ENCODING_SIZE];
    stream.read_exact(&mut re_pub).await?;

    let (response, codec) = responder
        .step_1(re_pub)
        .map_err(|e| anyhow!("noise handshake step_1 failed: {e:?}"))?;

    stream.write_all(&response).await?;
    stream.flush().await?;

    Ok(State::with_transport_mode(codec))
}

/// Encrypted SV2 frame reader (owns the read half + decoder; shares cipher
/// state with the writer via `state`).
pub struct NoiseReader {
    reader: OwnedReadHalf,
    decoder: Decoder,
    state: Arc<Mutex<State>>,
    /// Upper bound on the bytes we will read for a single frame chunk before
    /// rejecting the connection. The SV2 frame header carries an attacker-chosen
    /// u24 length (up to ~16 MB); without this cap the decoder would allocate and
    /// `read_exact` that whole frame (and AEAD-decrypt it) before the post-decode
    /// `check_message_size` ever runs. Set from `security.max_message_bytes` plus
    /// framing/AEAD overhead.
    max_frame: usize,
}

impl NoiseReader {
    pub fn new(reader: OwnedReadHalf, state: Arc<Mutex<State>>, max_frame: usize) -> Self {
        Self {
            reader,
            decoder: Decoder::new(),
            state,
            max_frame,
        }
    }

    /// Read, decrypt and frame one SV2 message, returning `(msg_type, payload)`.
    pub async fn read(&mut self) -> std::io::Result<(u8, Vec<u8>)> {
        loop {
            let decoded = {
                let mut st = self.state.lock().await;
                self.decoder.next_frame(&mut st)
            };
            match decoded {
                Ok(Frame::Sv2(mut frame)) => {
                    let header = frame
                        .get_header()
                        .ok_or_else(|| io_err("SV2 frame missing header"))?;
                    let payload = frame.payload().to_vec();
                    return Ok((header.msg_type(), payload));
                }
                Ok(Frame::HandShake(_)) => {
                    return Err(io_err("unexpected handshake frame in transport mode"))
                }
                Err(codec_sv2::Error::MissingBytes(_)) => {
                    let writable = self.decoder.writable();
                    if writable.len() > self.max_frame {
                        return Err(io_err(format!(
                            "SV2 frame too large: {} > {} bytes",
                            writable.len(),
                            self.max_frame
                        )));
                    }
                    self.reader.read_exact(writable).await?;
                }
                Err(e) => return Err(io_err(format!("noise decode error: {e:?}"))),
            }
        }
    }
}

/// Encrypted SV2 frame writer (owns the write half + encoder; shares cipher
/// state with the reader via `state`).
pub struct NoiseWriter {
    writer: OwnedWriteHalf,
    encoder: Encoder,
    state: Arc<Mutex<State>>,
    peer: std::net::SocketAddr,
}

impl NoiseWriter {
    pub fn new(
        writer: OwnedWriteHalf,
        state: Arc<Mutex<State>>,
        peer: std::net::SocketAddr,
    ) -> Self {
        Self {
            writer,
            encoder: Encoder::new(),
            state,
            peer,
        }
    }

    /// Frame, encrypt and write one SV2 message. Returns false on any error so
    /// the session can disconnect (mirrors the SV1 `send_messages` contract).
    pub async fn send(&mut self, msg_type: u8, channel_msg: bool, payload: &[u8]) -> bool {
        tracing::trace!(peer = %self.peer, msg_type, len = payload.len(), "→ sv2 pool (noise)");

        let full = messages::frame_bytes(msg_type, channel_msg, payload);
        let frame: Sv2Frame<Marker, Vec<u8>> = Sv2Frame::from_bytes_unchecked(full);
        let item = Frame::Sv2(frame);

        let encrypted = {
            let mut st = self.state.lock().await;
            match self.encoder.encode(item, &mut st) {
                Ok(b) => b,
                Err(e) => {
                    warn!("SV2 noise encode error to {}: {e:?}", self.peer);
                    return false;
                }
            }
        };

        if let Err(e) = self.writer.write_all(encrypted.as_ref()).await {
            debug!("SV2 write error to {}: {e}", self.peer);
            return false;
        }
        if let Err(e) = self.writer.flush().await {
            debug!("SV2 flush error to {}: {e}", self.peer);
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noise_sv2::{Initiator, INITIATOR_EXPECTED_HANDSHAKE_MESSAGE_SIZE};
    use tokio::net::TcpListener;

    fn authority_pubkey_bytes(secret: &[u8; 32]) -> [u8; 32] {
        let sk = SecretKey::from_slice(secret).unwrap();
        Secp256k1PublicKey::from(Secp256k1SecretKey(sk)).into_bytes()
    }

    /// Drive a full handshake against `responder_handshake_with`, acting as a
    /// miner. `now_offset_secs` shifts the initiator's clock when it checks the
    /// certificate validity window (a stand-in for device clock skew).
    async fn initiator_handshake(
        mut initiator: Box<Initiator>,
        responder_secret: [u8; 32],
        cert_validity_secs: u32,
        now_offset_secs: i64,
    ) -> Result<(), noise_sv2::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            responder_handshake_with(&mut sock, &responder_secret, cert_validity_secs)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let first = initiator.step_0().unwrap();
        client.write_all(&first).await.unwrap();
        let mut reply = [0u8; INITIATOR_EXPECTED_HANDSHAKE_MESSAGE_SIZE];
        client.read_exact(&mut reply).await.unwrap();

        let now = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + now_offset_secs) as u32;
        let result = initiator.step_2_with_now(reply, now).map(|_| ());
        server.await.unwrap().unwrap();
        result
    }

    #[tokio::test]
    async fn pinning_initiator_accepts_the_pool_certificate() {
        let secret = generate_secret();
        let initiator = Initiator::from_raw_k(authority_pubkey_bytes(&secret)).unwrap();
        initiator_handshake(initiator, secret, 3600, 0)
            .await
            .expect("handshake with the correct pinned authority key");
    }

    #[tokio::test]
    async fn pinning_initiator_rejects_a_wrong_authority_key() {
        let secret = generate_secret();
        let other = generate_secret();
        let initiator = Initiator::from_raw_k(authority_pubkey_bytes(&other)).unwrap();
        let err = initiator_handshake(initiator, secret, 3600, 0)
            .await
            .expect_err("certificate signed by a different authority must be rejected");
        assert!(matches!(err, noise_sv2::Error::InvalidCertificate(_)));
    }

    #[tokio::test]
    async fn non_pinning_initiator_connects_without_the_key() {
        let secret = generate_secret();
        let initiator = Initiator::without_pk().unwrap();
        initiator_handshake(initiator, secret, 3600, 0)
            .await
            .expect("handshake without identity pinning");
    }

    #[tokio::test]
    async fn certificate_outside_its_validity_window_is_rejected() {
        // Initiator clock 60 s past a 1 s validity window — well outside the
        // 10 s clock-drift leeway SRI's verifier grants (stratum issue #2015).
        let secret = generate_secret();
        let initiator = Initiator::from_raw_k(authority_pubkey_bytes(&secret)).unwrap();
        let err = initiator_handshake(initiator, secret, 1, 60)
            .await
            .expect_err("expired certificate must be rejected");
        assert!(matches!(err, noise_sv2::Error::InvalidCertificate(_)));
    }

    fn temp_key_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("btcpool-noise-tests-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn authority_key_file_round_trips() {
        let path = temp_key_path("roundtrip.key");
        std::fs::remove_file(&path).ok();

        let first = load_or_generate_secret(&path).unwrap();
        let second = load_or_generate_secret(&path).unwrap();
        assert_eq!(first, second, "second load must return the persisted key");

        // The file holds one base58check line in the SRI key-utils format.
        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: Secp256k1SecretKey = contents.trim().parse().unwrap();
        assert_eq!(parsed.into_bytes(), first);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "key file must be owner-only");
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn corrupt_authority_key_file_fails_loudly() {
        let path = temp_key_path("corrupt.key");
        std::fs::write(&path, "not-a-key\n").unwrap();
        let err = load_or_generate_secret(&path).unwrap_err();
        assert!(err.to_string().contains("Parsing SV2 authority key file"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn encoded_public_key_parses_in_the_key_utils_format() {
        let secret = generate_secret();
        let encoded = encode_public_key(&secret);
        let parsed: Secp256k1PublicKey = encoded.parse().unwrap();
        assert_eq!(parsed.into_bytes(), authority_pubkey_bytes(&secret));
    }
}
