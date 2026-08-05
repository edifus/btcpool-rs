//! Throwaway SV2 **Noise** smoke client — verifies the pool's encrypted Stratum
//! V2 handshake and job delivery end-to-end against a real `getblocktemplate`.
//! Mirrors what a NerdQAxe++ does (initiator that does not verify pool identity).
//!
//!   cargo run --example sv2_smoke_client -- 127.0.0.1:13335 BITCOIN_ADDRESS.smoke
//!
//! Drives: Noise handshake → SetupConnection → OpenExtendedMiningChannel, then
//! reads the pushed OpenExtendedMiningChannelSuccess / NewExtendedMiningJob /
//! SetNewPrevHash and prints a summary. Not part of the shipping pool.
use binary_sv2::{Str0255, U256};
use codec_sv2::{NoiseEncoder, StandardNoiseDecoder, State};
use common_messages_sv2::{Protocol, SetupConnection};
use framing_sv2::framing::{Frame, Sv2Frame};
use mining_sv2::{
    NewExtendedMiningJob, OpenExtendedMiningChannel, SetNewPrevHash, SubmitSharesSuccess,
};
use noise_sv2::{Initiator, ELLSWIFT_ENCODING_SIZE, INITIATOR_EXPECTED_HANDSHAKE_MESSAGE_SIZE};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

type Marker = SubmitSharesSuccess;
const HDR: usize = 6;

fn frame_bytes(msg_type: u8, channel_msg: bool, payload: &[u8]) -> Vec<u8> {
    let ext: u16 = if channel_msg { 0x8000 } else { 0 };
    let len = (payload.len() as u32).to_le_bytes();
    let mut out = Vec::with_capacity(HDR + payload.len());
    out.extend_from_slice(&ext.to_le_bytes());
    out.push(msg_type);
    out.extend_from_slice(&len[0..3]);
    out.extend_from_slice(payload);
    out
}

async fn send(
    s: &mut TcpStream,
    enc: &mut NoiseEncoder<Marker>,
    st: &mut State,
    mt: u8,
    ch: bool,
    payload: &[u8],
) {
    let frame: Sv2Frame<Marker, Vec<u8>> =
        Sv2Frame::from_bytes_unchecked(frame_bytes(mt, ch, payload));
    let bytes = enc.encode(Frame::Sv2(frame), st).expect("encode");
    s.write_all(bytes.as_ref()).await.unwrap();
    s.flush().await.unwrap();
}

async fn recv(
    s: &mut TcpStream,
    dec: &mut StandardNoiseDecoder<Marker>,
    st: &mut State,
) -> (u8, Vec<u8>) {
    loop {
        match dec.next_frame(st) {
            Ok(Frame::Sv2(mut f)) => {
                let mt = f.get_header().unwrap().msg_type();
                return (mt, f.payload().to_vec());
            }
            Ok(Frame::HandShake(_)) => panic!("unexpected handshake frame"),
            Err(codec_sv2::Error::MissingBytes(_)) => {
                let w = dec.writable();
                s.read_exact(w).await.unwrap();
            }
            Err(e) => panic!("decode error: {e:?}"),
        }
    }
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:13335".to_string());
    let user_identity = args
        .next()
        .expect("usage: sv2_smoke_client [HOST:PORT] BITCOIN_ADDRESS.worker");
    let mut s = TcpStream::connect(&addr).await.expect("connect");
    println!("connected to {addr}");

    // ── Noise handshake (initiator, no identity verification) ────────────────
    let mut initiator = Initiator::without_pk().expect("initiator");
    let first = initiator.step_0().expect("step_0");
    assert_eq!(first.len(), ELLSWIFT_ENCODING_SIZE);
    s.write_all(&first).await.unwrap();
    s.flush().await.unwrap();

    let mut reply = [0u8; INITIATOR_EXPECTED_HANDSHAKE_MESSAGE_SIZE];
    s.read_exact(&mut reply).await.unwrap();
    let codec = initiator.step_2(reply).expect("step_2");
    println!("Noise handshake complete ✅ (encrypted transport established)");

    let mut state = State::with_transport_mode(codec);
    let mut enc = NoiseEncoder::<Marker>::new();
    let mut dec = StandardNoiseDecoder::<Marker>::new();

    // ── SetupConnection ──────────────────────────────────────────────────────
    let setup = SetupConnection {
        protocol: Protocol::MiningProtocol,
        min_version: 2,
        max_version: 2,
        flags: 0,
        endpoint_host: Str0255::try_from(String::new()).unwrap(),
        endpoint_port: 0,
        vendor: Str0255::try_from("smoke".to_string()).unwrap(),
        hardware_version: Str0255::try_from(String::new()).unwrap(),
        firmware: Str0255::try_from(String::new()).unwrap(),
        device_id: Str0255::try_from(String::new()).unwrap(),
    };
    send(
        &mut s,
        &mut enc,
        &mut state,
        0x00,
        false,
        &binary_sv2::to_bytes(setup).unwrap(),
    )
    .await;
    let (mt, _) = recv(&mut s, &mut dec, &mut state).await;
    println!("← msg_type 0x{mt:02x} (expect 0x01 SetupConnectionSuccess)");
    assert_eq!(mt, 0x01);

    // ── OpenExtendedMiningChannel ────────────────────────────────────────────
    let open = OpenExtendedMiningChannel {
        request_id: 1,
        user_identity: Str0255::try_from(user_identity).unwrap(),
        nominal_hash_rate: 1.0e12,
        max_target: U256::from([0xffu8; 32]),
        min_extranonce_size: 8, // NerdQAxe++ asks for 8; pool grants >= this
    };
    send(
        &mut s,
        &mut enc,
        &mut state,
        0x13,
        false,
        &binary_sv2::to_bytes(open).unwrap(),
    )
    .await;

    let mut saw_success = false;
    let mut saw_job = false;
    let mut saw_prevhash = false;
    for _ in 0..6 {
        let (mt, mut payload) =
            match tokio::time::timeout(Duration::from_secs(8), recv(&mut s, &mut dec, &mut state))
                .await
            {
                Ok(v) => v,
                Err(_) => break,
            };
        match mt {
            0x14 => {
                saw_success = true;
                println!("← 0x14 OpenExtendedMiningChannelSuccess");
            }
            0x1f => {
                saw_job = true;
                let j: NewExtendedMiningJob = binary_sv2::from_bytes(&mut payload).unwrap();
                println!(
                    "← 0x1f NewExtendedMiningJob: job_id={} version=0x{:08x} vr_allowed={} merkle_path_len={} cb_prefix={}B cb_suffix={}B",
                    j.job_id, j.version, j.version_rolling_allowed, j.merkle_path.0.len(),
                    j.coinbase_tx_prefix.inner_as_ref().len(), j.coinbase_tx_suffix.inner_as_ref().len(),
                );
            }
            0x20 => {
                saw_prevhash = true;
                let p: SetNewPrevHash = binary_sv2::from_bytes(&mut payload).unwrap();
                println!(
                    "← 0x20 SetNewPrevHash: job_id={} nbits=0x{:08x} prev_hash={}",
                    p.job_id,
                    p.nbits,
                    hex::encode(p.prev_hash.inner_as_ref()),
                );
            }
            other => println!("← 0x{other:02x} (other)"),
        }
        if saw_success && saw_job && saw_prevhash {
            break;
        }
    }

    assert!(saw_success, "no OpenExtendedMiningChannelSuccess");
    assert!(saw_job, "no NewExtendedMiningJob");
    assert!(saw_prevhash, "no SetNewPrevHash");
    println!("\nSV2 Noise smoke test PASSED ✅");
}
