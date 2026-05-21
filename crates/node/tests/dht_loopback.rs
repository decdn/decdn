//! Two-endpoint loopback test for `cdn/dht/v1` (ADR 022, #320).
//!
//! Spawns a server endpoint running the DHT handler, connects a client
//! over iroh on localhost, sends a `FindNodeRequest`, and verifies that
//! the response echoes the target and returns the K-closest seeded peers.
//!
//! Only `FindNode` is exercised here — `Store` / `FindValue` get
//! placeholder responses in this PR slice (see [`crate::handlers::dht`]
//! doc comment); their loopback coverage lands with PR 3 of #320.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::Mutex;

use decdn_common::config::ResolvedSecurity;
use decdn_node::dht::routing::RoutingTable;
use decdn_node::dht::{DhtRateLimiter, rate_limit::DhtRateLimitConfig};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::dht::DhtHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::{
    ALPN_DHT, decode_message, dht as wire, encode_message, read_frame, write_frame,
};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey, endpoint::presets};

fn fresh_key() -> SecretKey {
    SecretKey::generate()
}

fn permissive_limiter(metrics: &Arc<Metrics>) -> Arc<ConnectionLimiter> {
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 1_000_000.0,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
    };
    Arc::new(ConnectionLimiter::new(&cfg, Arc::clone(metrics)))
}

fn permissive_dht_rate_limiter(metrics: &Arc<Metrics>) -> Arc<DhtRateLimiter> {
    let cfg = DhtRateLimitConfig {
        per_peer_rate_per_sec: 1e6,
        per_peer_burst: u32::MAX,
        per_ip_rate_per_sec: 1e6,
        per_ip_burst: u32::MAX,
        global_rate_per_sec: 1e6,
        global_burst: u32::MAX,
        trusted_ips: std::collections::HashSet::new(),
    };
    Arc::new(DhtRateLimiter::new(&cfg, Arc::clone(metrics)))
}

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
async fn find_node_returns_closer_peers_from_routing_table() -> anyhow::Result<()> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let rate_limiter = permissive_dht_rate_limiter(&metrics);

    // Seed the server's routing table with three fake peers so the
    // response carries something verifiable. The handler is constructed
    // via `with_routing` so we get a shared `Arc<Mutex<RoutingTable>>`
    // for the seeding step.
    let routing = Arc::new(Mutex::new(RoutingTable::new(*server_id.as_bytes())));
    let seeded_peers: Vec<[u8; 32]> = (1u8..=3).map(|i| [i; 32]).collect();
    {
        let mut t = routing.lock().expect("routing lock poisoned");
        for p in &seeded_peers {
            t.insert(*p);
        }
    }

    let handler = Arc::new(DhtHandler::with_routing(
        server_id,
        Arc::clone(&routing),
        rate_limiter,
        limiter,
        Arc::clone(&metrics),
    ));

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_DHT.to_vec()]).await?;

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
    let client_id = client_ep.id();
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let conn = client_ep
        .connect(target, ALPN_DHT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Target = one of the seeded peers, so it should appear in the
    // response along with the rest of the table.
    let req = wire::FindNodeRequest {
        target: [1u8; 32],
        requester: *client_id.as_bytes(),
    };
    let payload = encode_message(&wire::DhtMessage::FindNode(req))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| anyhow::anyhow!("read: {e}"))?;
    let (msg, _rest) = decode_message::<wire::DhtMessage>(&frame)?;
    let resp = match msg {
        wire::DhtMessage::FindNodeResponse(r) => r,
        other => anyhow::bail!("expected FindNodeResponse, got {other:?}"),
    };

    assert_eq!(resp.target, req.target, "target echoed");
    assert!(
        !resp.closer_nodes.is_empty(),
        "responder seeded with 3 peers must return at least one"
    );
    assert!(
        resp.closer_nodes.contains(&[1u8; 32]),
        "the exact target NodeId is one of the seeded peers and should appear"
    );
    // Routing table should not include the server's own id.
    assert!(
        !resp.closer_nodes.iter().any(|n| n == server_id.as_bytes()),
        "responder must not list itself"
    );

    // The handler should have refreshed the requester (the client) into
    // its routing table after admitting the request. Verify by reading
    // the table directly (we have the Arc).
    {
        let t = routing.lock().expect("routing lock poisoned");
        assert!(
            t.contains(client_id.as_bytes()),
            "client's NodeId must be in the responder's routing table after a successful FindNode"
        );
    }

    conn.close(0u32.into(), b"bye");
    client_ep.close().await;
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn find_value_returns_placeholder_with_closer_nodes() -> anyhow::Result<()> {
    // PR-2 placeholder behaviour: FindValue returns an empty providers
    // list plus the closest seeded routing-table peers. Pinned now so an
    // accidental real-handling regression while the record store lands
    // can't pass under the same loopback name.
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let rate_limiter = permissive_dht_rate_limiter(&metrics);

    let routing = Arc::new(Mutex::new(RoutingTable::new(*server_id.as_bytes())));
    {
        let mut t = routing.lock().expect("routing lock poisoned");
        t.insert([0x42u8; 32]);
    }

    let handler = Arc::new(DhtHandler::with_routing(
        server_id,
        routing,
        rate_limiter,
        limiter,
        Arc::clone(&metrics),
    ));
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_DHT.to_vec()]).await?;
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
        .connect(target, ALPN_DHT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    let req = wire::FindValueRequest {
        hash: [0x99u8; 32],
        requester: *client_ep.id().as_bytes(),
    };
    let payload = encode_message(&wire::DhtMessage::FindValue(req))?;
    write_frame(&mut send, &payload).await?;
    send.finish()?;
    let frame = read_frame(&mut recv).await?;
    let (msg, _) = decode_message::<wire::DhtMessage>(&frame)?;
    let resp = match msg {
        wire::DhtMessage::FindValueResponse(r) => r,
        other => anyhow::bail!("expected FindValueResponse, got {other:?}"),
    };
    assert_eq!(resp.hash, req.hash);
    assert!(
        resp.providers.is_empty(),
        "PR-2 FindValue returns no providers (record store lands in PR 3)"
    );
    assert!(
        resp.closer_nodes.contains(&[0x42u8; 32]),
        "seeded peer must appear in closer_nodes"
    );

    conn.close(0u32.into(), b"bye");
    client_ep.close().await;
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn find_node_does_not_insert_attacker_supplied_requester() -> anyhow::Result<()> {
    // Regression guard for the routing-table poisoning hole flagged in
    // PR #643 review: the handler must NOT trust the unauthenticated
    // `req.requester` wire field. An authenticated peer A sending
    // `FindNode { requester: B }` for an arbitrary B must NOT cause B to
    // be inserted into the responder's routing table — only A's
    // QUIC-authenticated NodeId is honest enough to insert.
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let rate_limiter = permissive_dht_rate_limiter(&metrics);
    let routing = Arc::new(Mutex::new(RoutingTable::new(*server_id.as_bytes())));
    let handler = Arc::new(DhtHandler::with_routing(
        server_id,
        Arc::clone(&routing),
        rate_limiter,
        limiter,
        Arc::clone(&metrics),
    ));

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_DHT.to_vec()]).await?;
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
    let client_id = client_ep.id();
    let attacker_id: [u8; 32] = [0xEE; 32];

    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let conn = client_ep
        .connect(target, ALPN_DHT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Authenticated client A sends a FindNode whose `requester` claims to
    // be the unrelated `attacker_id`.
    let req = wire::FindNodeRequest {
        target: [0x12; 32],
        requester: attacker_id,
    };
    let payload = encode_message(&wire::DhtMessage::FindNode(req))?;
    write_frame(&mut send, &payload).await?;
    send.finish()?;
    let frame = read_frame(&mut recv).await?;
    let (_msg, _) = decode_message::<wire::DhtMessage>(&frame)?;

    conn.close(0u32.into(), b"bye");
    client_ep.close().await;
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;

    let t = routing.lock().expect("routing lock poisoned");
    assert!(
        t.contains(client_id.as_bytes()),
        "authenticated client must be inserted by the connection-level refresh"
    );
    assert!(
        !t.contains(&attacker_id),
        "attacker-supplied req.requester must NOT be inserted (routing-table poisoning guard)"
    );
    Ok(())
}

// ADR 013 error-code mapping verification — three loopback tests that send
// a deliberately malformed/unsupported request and assert the server
// closes the stream with the expected QUIC application error code. The
// codes themselves are protocol-wide constants in
// `crates/protocol/src/lib.rs` and `crates/node/src/handlers/probe.rs`;
// the mapping for `cdn/dht/v1` lives in `handlers/dht.rs::frame_err_code`
// + `read_dht_request`.

mod adr_013_error_codes {
    use super::*;
    use decdn_protocol::APP_ERR_RATE_LIMITED;
    use iroh::endpoint::{ConnectionError, ReadError, ReadToEndError, VarInt};

    const APP_ERR_UNSUPPORTED_MESSAGE: u32 = 0x01;
    // `APP_ERR_MESSAGE_TOO_LARGE = 0x02` is mapped by `frame_err_code`
    // when the framing layer reports `FrameError::TooLarge` — exercising
    // it requires sending a length-prefix above `MAX_MESSAGE_SIZE`
    // without going through `write_frame` (which caps before writing).
    // Left for a future low-level framing test once a raw-bytes helper
    // is available.
    const APP_ERR_MALFORMED_MESSAGE: u32 = 0x03;

    /// Spin up a permissive DHT server, return its address + a join handle
    /// for the accept task. The handle is expected to `Err` once the
    /// server rejects the client's input — that signals the right app
    /// error code was emitted at the server side.
    async fn spin_up_dht_server() -> anyhow::Result<(
        Endpoint,
        std::net::SocketAddr,
        iroh::PublicKey,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    )> {
        let server_sk = fresh_key();
        let server_id = server_sk.public();
        let metrics = Arc::new(Metrics::new());
        let limiter = permissive_limiter(&metrics);
        let rate_limiter = permissive_dht_rate_limiter(&metrics);
        let routing = Arc::new(Mutex::new(RoutingTable::new(*server_id.as_bytes())));
        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            metrics,
        ));
        let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_DHT.to_vec()]).await?;
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
        Ok((server_ep, server_addr, server_id, accept_task))
    }

    /// Assert that a `read_to_end` on the client's `recv` stream surfaces
    /// `expected_code` as either a stream reset or a connection close
    /// with that ADR 013 app error code.
    fn assert_close_code(result: Result<Vec<u8>, ReadToEndError>, expected_code: u32) {
        let expected = VarInt::from_u32(expected_code);
        match result {
            Err(ReadToEndError::Read(ReadError::Reset(code))) if code == expected => {}
            Err(ReadToEndError::Read(ReadError::ConnectionLost(
                ConnectionError::ApplicationClosed(close),
            ))) if close.error_code == expected => {}
            other => panic!(
                "expected close code {expected_code:#x} (RESET or ApplicationClosed), got {other:?}"
            ),
        }
    }

    /// Malformed postcard body → `APP_ERR_MALFORMED_MESSAGE` (0x03).
    #[tokio::test(flavor = "multi_thread")]
    async fn malformed_frame_returns_malformed_message_code() -> anyhow::Result<()> {
        let (server_ep, server_addr, server_id, accept_task) = spin_up_dht_server().await?;
        let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
        let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
        let conn = client_ep
            .connect(target, ALPN_DHT)
            .await
            .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
        // Write a valid frame header but garbage postcard body — the
        // discriminant byte 0x55 has no matching DhtMessage variant.
        let garbage = [0x55u8, 0x00, 0x00, 0x00];
        write_frame(&mut send, &garbage).await?;
        send.finish()?;
        let r = recv.read_to_end(64).await;
        assert_close_code(r, APP_ERR_MALFORMED_MESSAGE);
        conn.close(0u32.into(), b"bye");
        client_ep.close().await;
        // The server task is expected to surface the rejection as an Err.
        let _ = accept_task.await;
        server_ep.close().await;
        Ok(())
    }

    /// Response variant on the server stream → `APP_ERR_UNSUPPORTED_MESSAGE` (0x01).
    #[tokio::test(flavor = "multi_thread")]
    async fn response_on_server_stream_returns_unsupported_code() -> anyhow::Result<()> {
        let (server_ep, server_addr, server_id, accept_task) = spin_up_dht_server().await?;
        let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
        let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
        let conn = client_ep
            .connect(target, ALPN_DHT)
            .await
            .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
        // FindNodeResponse is a response variant; the handler rejects it.
        let resp = wire::DhtMessage::FindNodeResponse(wire::FindNodeResponse {
            target: [0u8; 32],
            closer_nodes: Vec::new(),
        });
        let payload = encode_message(&resp)?;
        write_frame(&mut send, &payload).await?;
        send.finish()?;
        let r = recv.read_to_end(64).await;
        assert_close_code(r, APP_ERR_UNSUPPORTED_MESSAGE);
        conn.close(0u32.into(), b"bye");
        client_ep.close().await;
        let _ = accept_task.await;
        server_ep.close().await;
        Ok(())
    }

    /// `BatchStore` request → `APP_ERR_UNSUPPORTED_MESSAGE` (0x01). Per
    /// ADR 022 §STORE Flow line 138 + §Schema Evolution the unsupported
    /// signal MUST be stream-close-without-ack — an all-`false`
    /// `BatchStoreAck` would NOT trigger publisher fallback. This test
    /// guards against a regression that returns a well-formed ack.
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_store_returns_unsupported_code_not_ack() -> anyhow::Result<()> {
        let (server_ep, server_addr, server_id, accept_task) = spin_up_dht_server().await?;
        let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
        let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
        let conn = client_ep
            .connect(target, ALPN_DHT)
            .await
            .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
        let req = wire::DhtMessage::BatchStore(wire::BatchStoreRequest {
            hashes: vec![[0x01u8; 32], [0x02u8; 32], [0x03u8; 32]],
            holder: *client_ep.id().as_bytes(),
        });
        let payload = encode_message(&req)?;
        write_frame(&mut send, &payload).await?;
        send.finish()?;
        let r = recv.read_to_end(64).await;
        assert_close_code(r, APP_ERR_UNSUPPORTED_MESSAGE);
        conn.close(0u32.into(), b"bye");
        client_ep.close().await;
        let _ = accept_task.await;
        server_ep.close().await;
        Ok(())
    }

    /// Rate-limit rejection → `APP_ERR_RATE_LIMITED` (0x10). Build a
    /// burst=1 limiter, send two requests on the same connection, assert
    /// the second stream is reset with the rate-limit code.
    #[tokio::test(flavor = "multi_thread")]
    async fn rate_limit_rejection_returns_rate_limited_code() -> anyhow::Result<()> {
        let server_sk = fresh_key();
        let server_id = server_sk.public();
        let metrics = Arc::new(Metrics::new());
        let limiter = permissive_limiter(&metrics);
        // burst=1 on global so the second request on the same connection
        // hits the cap; per-peer and per-IP loose so we test the global
        // path. trusted_ips empty.
        let rate_cfg = DhtRateLimitConfig {
            per_peer_rate_per_sec: 1e6,
            per_peer_burst: u32::MAX,
            per_ip_rate_per_sec: 1e6,
            per_ip_burst: u32::MAX,
            global_rate_per_sec: 1.0,
            global_burst: 1,
            trusted_ips: std::collections::HashSet::new(),
        };
        let rate_limiter = Arc::new(DhtRateLimiter::new(&rate_cfg, Arc::clone(&metrics)));
        let routing = Arc::new(Mutex::new(RoutingTable::new(*server_id.as_bytes())));
        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            metrics,
        ));
        let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_DHT.to_vec()]).await?;
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
            .connect(target, ALPN_DHT)
            .await
            .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

        // First request consumes the global=1 bucket and must succeed.
        let (mut s1, mut r1) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi #1: {e}"))?;
        let req = wire::DhtMessage::FindNode(wire::FindNodeRequest {
            target: [0u8; 32],
            requester: *client_ep.id().as_bytes(),
        });
        let payload = encode_message(&req)?;
        write_frame(&mut s1, &payload).await?;
        s1.finish()?;
        let frame = read_frame(&mut r1).await?;
        let _ = decode_message::<wire::DhtMessage>(&frame)?;

        // Second request on the same connection must be rate-limited.
        let (mut s2, mut r2) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi #2: {e}"))?;
        write_frame(&mut s2, &payload).await?;
        s2.finish()?;
        let rr = r2.read_to_end(2048).await;
        assert_close_code(rr, APP_ERR_RATE_LIMITED);
        conn.close(0u32.into(), b"bye");
        client_ep.close().await;
        let _ = accept_task.await;
        server_ep.close().await;
        Ok(())
    }
}
