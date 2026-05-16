//! Two-endpoint loopback test for `cdn/probe/v1`.
//!
//! Spawns a server endpoint running the probe handler, connects a client
//! endpoint over iroh on localhost, sends a `ProbeRequest`, and verifies the
//! response echoes the nonce and reports the server's node id and rate.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use decdn_common::config::ResolvedSecurity;
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::probe::ProbeHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::{
    ALPN_PROBE, APP_ERR_RATE_LIMITED, MAX_MESSAGE_SIZE, ProbeMessage, decode_message,
    encode_message,
    message::{ProbeRequest, ProbeResponse},
    read_frame, write_frame,
};
use iroh::endpoint::{
    ApplicationClose, Connection, ConnectionError, IdleTimeout, QuicTransportConfig, ReadError,
    ReadToEndError, VarInt, presets,
};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};
use tokio::task::JoinHandle;

/// Build a permissive `ConnectionLimiter` suitable for tests that don't
/// exercise rate-limiting behaviour.
fn permissive_limiter(metrics: &Arc<Metrics>) -> Arc<ConnectionLimiter> {
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 1_000_000.0,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
    };
    Arc::new(ConnectionLimiter::new(&cfg, Arc::clone(metrics)))
}

fn fresh_key() -> SecretKey {
    SecretKey::generate()
}

/// Build an endpoint bound to 127.0.0.1 with relays disabled and no discovery.
/// Returns the endpoint plus its local socket address.
async fn local_endpoint(
    secret_key: SecretKey,
    alpns: Vec<Vec<u8>>,
) -> anyhow::Result<(Endpoint, SocketAddr)> {
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let ep = Endpoint::builder(presets::Minimal)
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
    let limiter = permissive_limiter(&metrics);
    let handler = Arc::new(ProbeHandler::new(
        server_id,
        Arc::new(AtomicU64::new(rate_per_mb)),
        Arc::clone(&metrics),
        limiter,
        // 1-RTT path: this suite is the pre-ADR-015 loopback coverage.
        // 0-RTT acceptance has its own dedicated test (probe_0rtt.rs).
        false,
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
    let limiter = permissive_limiter(&metrics);
    let handler = Arc::new(ProbeHandler::new(
        server_id,
        Arc::new(AtomicU64::new(1)),
        Arc::clone(&metrics),
        limiter,
        // 1-RTT path: this suite is the pre-ADR-015 loopback coverage.
        // 0-RTT acceptance has its own dedicated test (probe_0rtt.rs).
        false,
    ));
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
    // Just `.await` — no wrapper timeout, because under `start_paused`
    // `tokio::time::timeout` itself runs on the virtual clock and would
    // not trip on a non-timer deadlock. Cargo's test-harness global
    // timeout covers that pathological case.
    let joined = h
        .accept_task
        .await
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

/// End-to-end check that the rate limiter rejects with `APP_ERR_RATE_LIMITED`
/// (`0x10`) on the wire. Without this, the dispatch reject path is dead code
/// under tests — every other test in this file uses `permissive_limiter`.
///
/// Strict per-IP burst=1 limiter; the server runs `ProbeHandler::accept` in a
/// loop so two back-to-back client connections both reach the handler. The
/// first one drains the bucket and serves a normal probe (close code 0); the
/// second one is rejected by `ConnectionLimiter::acquire` and observes
/// `APP_ERR_RATE_LIMITED` on its `CONNECTION_CLOSE`.
#[tokio::test(flavor = "multi_thread")]
async fn probe_rate_limit_returns_rate_limited_close_code() -> anyhow::Result<()> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());

    // burst=1 per-source so the second connection from the same client
    // IP unconditionally rejects. Global is loose so it doesn't
    // interfere.
    let strict = ResolvedSecurity {
        max_concurrent_handlers: 64,
        per_source_rate_per_sec: 0.001, // negligible refill within the test window
        per_source_burst: 1,
        max_tracked_sources: 32,
    };
    let limiter = Arc::new(ConnectionLimiter::new(&strict, Arc::clone(&metrics)));
    let handler = Arc::new(ProbeHandler::new(
        server_id,
        Arc::new(AtomicU64::new(1)),
        Arc::clone(&metrics),
        limiter,
        // 1-RTT path: this suite is the pre-ADR-015 loopback coverage.
        // 0-RTT acceptance has its own dedicated test (probe_0rtt.rs).
        false,
    ));

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_PROBE.to_vec()]).await?;
    let server_ep_bg = server_ep.clone();
    let handler_bg = Arc::clone(&handler);
    let accept_loop = tokio::spawn(async move {
        // Accept up to two connections — the test only drives two clients.
        for _ in 0..2 {
            let Some(incoming) = server_ep_bg.accept().await else {
                break;
            };
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else {
                continue;
            };
            let h = Arc::clone(&handler_bg);
            // Spawn so a slow first probe doesn't block the second accept.
            tokio::spawn(async move {
                let _ = h.accept(conn).await;
            });
        }
    });

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // First connection: completes a normal probe round-trip so the per-IP
    // bucket is drained when the second client arrives.
    let conn1 = client_ep
        .connect(target.clone(), ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect 1: {e}"))?;
    let (mut s, mut r) = conn1
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi 1: {e}"))?;
    let req = ProbeRequest { nonce: 1 };
    write_frame(&mut s, &encode_message(&ProbeMessage::Request(req))?)
        .await
        .map_err(|e| anyhow::anyhow!("write 1: {e}"))?;
    s.finish().map_err(|e| anyhow::anyhow!("finish 1: {e}"))?;
    let _ = read_frame(&mut r)
        .await
        .map_err(|e| anyhow::anyhow!("read 1: {e}"))?;
    conn1.close(0u32.into(), b"bye");

    // Second connection from the same client (same NodeID + IP). The
    // limiter rejects on accept and the server closes with
    // APP_ERR_RATE_LIMITED.
    let conn2 = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect 2: {e}"))?;
    // Wait for the connection to be closed by the server. `closed()`
    // resolves with the application close code, which iroh exposes as
    // ConnectionError.
    let close_err = conn2.closed().await;
    let expected = VarInt::from_u32(APP_ERR_RATE_LIMITED);
    match close_err {
        ConnectionError::ApplicationClosed(ApplicationClose { error_code, .. })
            if error_code == expected => {}
        other => anyhow::bail!("expected ApplicationClosed({expected:?}), got {other:?}"),
    }

    client_ep.close().await;
    let _ = accept_loop.await;
    server_ep.close().await;
    Ok(())
}

/// Verify that `QuicTransportConfig::max_idle_timeout` actually closes a
/// silent connection (the wiring `production_transport_config` relies on).
/// The production value is 30s per ADR 005; we shorten it to 300ms here
/// so the test runs in well under a second. `keep_alive_interval` is
/// parked at 60s on both ends so the path stays silent across the idle
/// window — otherwise the keep-alive PINGs the runtime sends in
/// production would refresh the timer and the test could never observe
/// the close.
#[tokio::test(flavor = "multi_thread")]
async fn idle_timeout_closes_quiet_connection() -> anyhow::Result<()> {
    let idle = Duration::from_millis(300);
    let build_cfg = || -> anyhow::Result<QuicTransportConfig> {
        let it: IdleTimeout = idle
            .try_into()
            .map_err(|e| anyhow::anyhow!("idle timeout: {e}"))?;
        Ok(QuicTransportConfig::builder()
            .max_idle_timeout(Some(it))
            .keep_alive_interval(Duration::from_mins(1))
            .max_concurrent_bidi_streams(VarInt::from_u32(100))
            .build())
    };

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let server_ep = Endpoint::builder(presets::Minimal)
        .secret_key(server_sk)
        .transport_config(build_cfg()?)
        .alpns(vec![ALPN_PROBE.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind_addr(server_bind)
        .map_err(|e| anyhow::anyhow!("server bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("server bind: {e}"))?;
    let server_addr = server_ep
        .bound_sockets()
        .into_iter()
        .find(SocketAddr::is_ipv4)
        .ok_or_else(|| anyhow::anyhow!("no IPv4 bound socket"))?;
    let server_addr = match server_addr {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, v4.port()))
        }
        other => other,
    };

    // Server task: accept the connection but don't drive any application
    // handler. We're testing transport-level idle close, not protocol
    // behaviour — the only thing that should close the connection is the
    // idle timer.
    let server_ep_bg = server_ep.clone();
    let accept_task = tokio::spawn(async move {
        if let Some(incoming) = server_ep_bg.accept().await {
            let connecting = incoming
                .accept()
                .map_err(|e| anyhow::anyhow!("accept: {e}"))?;
            let conn = connecting
                .await
                .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
            let _ = conn.closed().await;
        }
        Ok::<_, anyhow::Error>(())
    });

    let client_bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let client_ep = Endpoint::builder(presets::Minimal)
        .secret_key(fresh_key())
        .transport_config(build_cfg()?)
        .relay_mode(RelayMode::Disabled)
        .bind_addr(client_bind)
        .map_err(|e| anyhow::anyhow!("client bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("client bind: {e}"))?;

    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let client_conn = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    // Wait for the idle timer to fire. The bound is generous compared to
    // `idle` so a slow CI doesn't flake the test, but tight enough that a
    // wiring regression (no `transport_config(...)` call, default 30s
    // timeout) fails fast instead of stalling for the full default.
    let close_err = tokio::time::timeout(Duration::from_secs(3), client_conn.closed())
        .await
        .map_err(|_| {
            anyhow::anyhow!("connection did not idle-close within 3s (idle window: {idle:?})")
        })?;

    match close_err {
        ConnectionError::TimedOut => {}
        other => anyhow::bail!("expected ConnectionError::TimedOut, got {other:?}"),
    }

    client_ep.close().await;
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;
    Ok(())
}
