//! Two-endpoint loopback test for `cdn/probe/v1`.
//!
//! Spawns a server endpoint running the probe handler, connects a client
//! endpoint over iroh on localhost, sends a `ProbeRequest`, and verifies the
//! response echoes the nonce and reports the server's node id and rate.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use decdn_node::handlers::probe::ProbeHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::{
    ALPN_PROBE, MAX_MESSAGE_SIZE, ProbeMessage, decode_message, encode_message,
    message::{ProbeRequest, ProbeResponse},
    read_frame, write_frame,
};
use iroh::endpoint::{
    ApplicationClose, Connection, ConnectionError, ReadError, ReadToEndError, VarInt,
};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};
use rand::Rng;
use tokio::task::JoinHandle;

/// Build a fresh `SecretKey` by drawing 32 random bytes and feeding them
/// to `SecretKey::from_bytes`. We don't use
/// `fresh_key()` because iroh 0.97 pins
/// `rand_core 0.9` while this crate uses rand 0.10, so the two
/// `CryptoRng` traits don't match.
fn fresh_key() -> SecretKey {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    SecretKey::from_bytes(&bytes)
}

/// Build an endpoint bound to 127.0.0.1 with relays disabled and no discovery.
/// Returns the endpoint plus its local socket address.
async fn local_endpoint(
    secret_key: SecretKey,
    alpns: Vec<Vec<u8>>,
) -> anyhow::Result<(Endpoint, SocketAddr)> {
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let ep = Endpoint::empty_builder()
        .secret_key(secret_key)
        .alpns(alpns)
        .relay_mode(RelayMode::Disabled)
        .bind_addr(bind)
        .map_err(|e| anyhow::anyhow!("bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("bind: {e}"))?;
    let addr = ep
        .bound_sockets()
        .into_iter()
        .find(SocketAddr::is_ipv4)
        .ok_or_else(|| anyhow::anyhow!("no IPv4 bound socket"))?;
    let addr = match addr {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, v4.port()))
        }
        other => other,
    };
    Ok((ep, addr))
}

#[tokio::test(flavor = "multi_thread")]
async fn probe_roundtrip() -> anyhow::Result<()> {
    let rate_per_mb: u64 = 42;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let handler = Arc::new(ProbeHandler::new(
        server_id,
        rate_per_mb,
        Arc::clone(&metrics),
    ));

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_PROBE.to_vec()]).await?;

    let server_ep_bg = server_ep.clone();
    let accept_task = tokio::spawn(async move {
        if let Some(incoming) = server_ep_bg.accept().await {
            let connecting = incoming
                .accept()
                .map_err(|e| anyhow::anyhow!("accept: {e}"))?;
            let conn = connecting
                .await
                .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
            handler
                .accept(conn)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        Ok::<_, anyhow::Error>(())
    });

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;

    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let conn = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    let req = ProbeRequest { nonce: 0x00c0_ffee };
    let payload = encode_message(&ProbeMessage::Request(req))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| anyhow::anyhow!("read: {e}"))?;
    let (msg, _rest) = decode_message::<ProbeMessage>(&frame)?;
    let resp: ProbeResponse = match msg {
        ProbeMessage::Response(r) => r,
        ProbeMessage::Request(_) => anyhow::bail!("unexpected request variant on client"),
    };

    assert_eq!(resp.nonce, req.nonce);
    assert_eq!(resp.rate_per_mb, rate_per_mb);
    assert_eq!(resp.node_id, *server_id.as_bytes());
    assert!(resp.measured_at_unix_ms > 0);

    conn.close(0u32.into(), b"bye");
    client_ep.close().await;

    // Allow the server task to finish handling before closing its endpoint.
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;
    Ok(())
}

/// Spin up a probe server and return the client's connected [`Connection`]
/// plus the background accept task and endpoints. The accept task is expected
/// to return `Err` once the server handler rejects the client's input — that
/// is the signal the correct app error code was emitted.
struct Harness {
    client_conn: Connection,
    accept_task: JoinHandle<anyhow::Result<()>>,
    client_ep: Endpoint,
    server_ep: Endpoint,
}

async fn spin_up_probe_harness() -> anyhow::Result<Harness> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let handler = Arc::new(ProbeHandler::new(server_id, 1, metrics));
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_PROBE.to_vec()]).await?;

    let server_ep_bg = server_ep.clone();
    let accept_task = tokio::spawn(async move {
        let incoming = server_ep_bg
            .accept()
            .await
            .ok_or_else(|| anyhow::anyhow!("no incoming connection"))?;
        let connecting = incoming
            .accept()
            .map_err(|e| anyhow::anyhow!("accept: {e}"))?;
        let conn = connecting
            .await
            .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
        handler
            .accept(conn)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    });

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let client_conn = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    Ok(Harness {
        client_conn,
        accept_task,
        client_ep,
        server_ep,
    })
}

/// Expect `recv.read_to_end` to fail with `expected_code` delivered either as a
/// stream `RESET_STREAM` or as a connection-level `CONNECTION_CLOSE` carrying
/// an application error code. The probe handler closes the connection with
/// the same app code it resets the stream with (probe is 1:1
/// connection:stream), so either form is a correct observation of the ADR 013
/// mapping.
async fn assert_reset_with_code(
    recv: &mut iroh::endpoint::RecvStream,
    expected_code: u32,
) -> anyhow::Result<()> {
    let expected = VarInt::from_u32(expected_code);
    match recv.read_to_end(4096).await {
        Err(ReadToEndError::Read(ReadError::Reset(code))) if code == expected => Ok(()),
        Err(ReadToEndError::Read(ReadError::ConnectionLost(
            ConnectionError::ApplicationClosed(ApplicationClose { error_code, .. }),
        ))) if error_code == expected => Ok(()),
        other => {
            anyhow::bail!("expected error carrying app code {expected_code:#x}, got {other:?}")
        }
    }
}

async fn tear_down(h: Harness) -> anyhow::Result<()> {
    h.client_conn.close(0u32.into(), b"bye");
    h.client_ep.close().await;
    // Handler is expected to return Err on these error-path tests; we only
    // need to confirm the task joined, not that it succeeded.
    let _ = h.accept_task.await;
    h.server_ep.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn probe_oversized_frame_returns_too_large_code() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    let (mut send, mut recv) = h
        .client_conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Write a varint length-prefix that exceeds MAX_MESSAGE_SIZE with no payload.
    let mut bogus = Vec::new();
    let mut v = MAX_MESSAGE_SIZE + 1;
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            bogus.push(byte);
            break;
        }
        bogus.push(byte | 0x80);
    }
    send.write_all(&bogus)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    assert_reset_with_code(&mut recv, 0x02).await?;
    tear_down(h).await
}

#[tokio::test(flavor = "multi_thread")]
async fn probe_garbage_postcard_returns_malformed_code() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    let (mut send, mut recv) = h
        .client_conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Valid length-prefixed frame, but payload has unknown ProbeMessage discriminant.
    write_frame(&mut send, &[99u8, 0])
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    assert_reset_with_code(&mut recv, 0x03).await?;
    tear_down(h).await
}

#[tokio::test(flavor = "multi_thread")]
async fn probe_response_on_server_stream_returns_unsupported_code() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    let (mut send, mut recv) = h
        .client_conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Well-formed frame, but the wrong variant (server expects Request).
    let payload = encode_message(&ProbeMessage::Response(ProbeResponse {
        nonce: 0,
        measured_at_unix_ms: 0,
        node_id: [0u8; 32],
        rate_per_mb: 0,
    }))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    assert_reset_with_code(&mut recv, 0x01).await?;
    tear_down(h).await
}

// Closes #241. ProbeHandler has two phase-level timeouts that had no test
// coverage: ACCEPT_BI_TIMEOUT (client connected but never opened a
// bi-stream) and PROBE_READ_TIMEOUT (client opened a stream but never
// wrote a frame). Both are 5s in production; gating tests on the real
// deadline would slow every CI run, so instead each test uses
// `tokio::time::pause()` + `advance()` to fast-forward the handler's
// inner `tokio::time::timeout` future by 6s of virtual time. iroh's
// network I/O sits on tokio-mio (not tokio::time), so only the timeout
// futures we care about are affected.

#[tokio::test(start_paused = true)]
async fn probe_read_timeout_resets_stream_with_zero_code() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    let (mut send, mut recv) = h
        .client_conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Deliberately don't write a frame. Virtual-advance past the handler's
    // PROBE_READ_TIMEOUT so the timeout fires without the test blocking
    // on the real 5-second deadline. `start_paused = true` requires the
    // current_thread runtime; iroh doesn't insist on multi-thread.
    tokio::time::advance(Duration::from_secs(6)).await;

    // Handler resets the stream AND closes the connection with app code 0
    // (ADR 013 defines no timeout-specific code; the handler uses 0 for
    // "no app error"). `assert_reset_with_code` tolerates either
    // observation form.
    assert_reset_with_code(&mut recv, 0x00).await?;
    let _ = send.finish();
    tear_down(h).await
}

#[tokio::test(start_paused = true)]
async fn probe_accept_bi_timeout_errors_handler() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    // Deliberately do NOT call open_bi. The handler's first await is
    // `tokio::time::timeout(ACCEPT_BI_TIMEOUT, conn.accept_bi())`, which
    // must time out and return Err.
    tokio::time::advance(Duration::from_secs(6)).await;

    // Confirm the server task returned Err with the expected message.
    // Wrap in a real-time timeout as a safety net: if virtual-advance
    // didn't do its job, we don't want the test hanging forever.
    let joined = tokio::time::timeout(Duration::from_secs(2), h.accept_task)
        .await
        .map_err(|_| anyhow::anyhow!("server task did not complete after virtual-advance"))?
        .map_err(|e| anyhow::anyhow!("join: {e}"))?;
    let Err(err) = joined else {
        anyhow::bail!("handler should have returned Err on ACCEPT_BI_TIMEOUT, got Ok");
    };
    let msg = err.to_string();
    anyhow::ensure!(
        msg.contains("accept_bi timed out"),
        "expected accept_bi timeout error, got: {msg}"
    );

    // Manual teardown — `h.accept_task` was consumed above, so the shared
    // `tear_down` helper can't run as-is.
    h.client_conn.close(0u32.into(), b"bye");
    h.client_ep.close().await;
    h.server_ep.close().await;
    Ok(())
}
