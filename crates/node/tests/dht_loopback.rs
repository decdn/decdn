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
