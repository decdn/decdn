//! Two-endpoint loopback test for `cdn/probe/v1`.
//!
//! Spawns a server endpoint running the probe handler, connects a client
//! endpoint over iroh on localhost, sends a `ProbeRequest`, and verifies the
//! response echoes the nonce and reports the server's node id and rate.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use decdn_node::handlers::{Handler, probe::ProbeHandler};
use decdn_node::metrics::Metrics;
use decdn_protocol::{
    ALPN_PROBE, MAX_MESSAGE_SIZE, ProbeMessage, decode_message, encode_message,
    message::{ProbeRequest, ProbeResponse},
    read_frame, write_frame,
};
use iroh::endpoint::{Connection, ReadError, ReadToEndError, VarInt};
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};
use tokio::task::JoinHandle;

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

    let server_sk = SecretKey::generate(&mut rand::rng());
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let handler: Arc<dyn Handler> = Arc::new(ProbeHandler::new(
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
            handler.handle(conn).await?;
        }
        Ok::<_, anyhow::Error>(())
    });

    let (client_ep, _) = local_endpoint(SecretKey::generate(&mut rand::rng()), vec![]).await?;

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
    let server_sk = SecretKey::generate(&mut rand::rng());
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let handler: Arc<dyn Handler> = Arc::new(ProbeHandler::new(server_id, 1, metrics));
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
        handler.handle(conn).await
    });

    let (client_ep, _) = local_endpoint(SecretKey::generate(&mut rand::rng()), vec![]).await?;
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

/// Expect `recv.read_to_end` to fail with a stream reset carrying `expected_code`.
async fn assert_stream_reset_with_code(
    recv: &mut iroh::endpoint::RecvStream,
    expected_code: u32,
) -> anyhow::Result<()> {
    match recv.read_to_end(4096).await {
        Err(ReadToEndError::Read(ReadError::Reset(code))) => {
            assert_eq!(code, VarInt::from_u32(expected_code));
            Ok(())
        }
        other => anyhow::bail!("expected stream Reset({expected_code:#x}), got {other:?}"),
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

    assert_stream_reset_with_code(&mut recv, 0x02).await?;
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

    assert_stream_reset_with_code(&mut recv, 0x03).await?;
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

    assert_stream_reset_with_code(&mut recv, 0x01).await?;
    tear_down(h).await
}
