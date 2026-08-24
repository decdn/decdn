//! Two-endpoint loopback tests for `cdn/dht/v1` (ADR 022, #320).
//!
//! Spawns a server endpoint running the DHT handler, connects a client
//! over iroh on localhost, exchanges messages, and verifies the
//! responses. Covers:
//!
//! - `FindNode` round-trip + routing-table refresh of the authenticated
//!   peer + the routing-table poisoning regression guard.
//! - `FindValue` against an empty store (no providers + closer-nodes
//!   only).
//! - `Store` admission: holder/peer mismatch rejection, non-staked
//!   rejection, and the full Store → `FindValue` round-trip for a staked
//!   publisher.
//! - ADR 013 application error codes on malformed / unsupported /
//!   rate-limited streams (the `adr_013_error_codes` submodule).

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
use decdn_node::dht::{
    DhtRateLimiter, RecordStore, RecordStoreConfig, StakerSet, rate_limit::DhtRateLimitConfig,
};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::dht::DhtHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::{
    ALPN_DHT, ContentHash, NodeId, decode_message, dht as wire, encode_message, read_frame,
    write_frame,
};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey, endpoint::presets};
mod support;
use support::shutdown;

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

/// `StakerSet` test stub: every `is_active` returns `true`. The
/// `active_nodes` / `len` overrides are intentionally NOT enumerable —
/// no real test consumer of this helper iterates the set, and faking
/// an infinite or empty list would lie either way. Tests that *do*
/// need a concrete active set use the production `ConfigStakerSet`
/// with an explicit `HashSet`.
#[derive(Debug)]
struct AllStaked;

impl AllStaked {
    const fn new() -> Self {
        Self
    }
}

impl StakerSet for AllStaked {
    fn is_active(&self, _: &NodeId) -> bool {
        true
    }
    /// Returns empty — this helper is not enumerable. Any code that
    /// reads `active_nodes()` from `AllStaked` is using the wrong
    /// helper for its test; switch to `ConfigStakerSet` with a real
    /// `HashSet` of peers instead.
    fn active_nodes(&self) -> Vec<NodeId> {
        Vec::new()
    }
    /// Returns 0 to stay consistent with [`Self::active_nodes`]. A
    /// previous version returned `usize::MAX` to convey "this
    /// effectively admits everyone", but the two views (set length vs.
    /// set contents) MUST agree per the [`StakerSet`] trait contract.
    fn len(&self) -> usize {
        0
    }
}

fn empty_record_store() -> Arc<Mutex<RecordStore>> {
    Arc::new(Mutex::new(RecordStore::new(RecordStoreConfig::default())))
}

fn permissive_dht_rate_limiter(metrics: &Arc<Metrics>) -> Arc<DhtRateLimiter> {
    let cfg = DhtRateLimitConfig {
        per_peer_rate_per_sec: 1e6,
        per_peer_burst: u32::MAX,
        per_ip_rate_per_sec: 1e6,
        per_ip_burst: u32::MAX,
        global_rate_per_sec: 1e6,
        global_burst: u32::MAX,
        max_tracked_per_ip: 4096,
        max_tracked_per_peer: 4096,
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
    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *server_id.as_bytes(),
    ))));
    let seeded_peers: Vec<[u8; 32]> = (1u8..=3).map(|i| [i; 32]).collect();
    {
        let mut t = routing.lock().expect("routing lock poisoned");
        for p in &seeded_peers {
            t.insert(NodeId::from_bytes(*p));
        }
    }

    let handler = Arc::new(DhtHandler::with_routing(
        server_id,
        Arc::clone(&routing),
        rate_limiter,
        limiter,
        Arc::clone(&metrics),
        Arc::new(AllStaked::new()),
        empty_record_store(),
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
        target: NodeId::from_bytes([1u8; 32]),
        requester: NodeId::from_bytes(*client_id.as_bytes()),
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
        resp.closer_nodes
            .as_slice()
            .contains(&NodeId::from_bytes([1u8; 32])),
        "the exact target NodeId is one of the seeded peers and should appear"
    );
    // Routing table should not include the server's own id.
    let server_node_id = NodeId::from_bytes(*server_id.as_bytes());
    assert!(
        !resp.closer_nodes.as_slice().contains(&server_node_id),
        "responder must not list itself"
    );

    // The handler should have refreshed the requester (the client) into
    // its routing table after admitting the request. Verify by reading
    // the table directly (we have the Arc).
    {
        let t = routing.lock().expect("routing lock poisoned");
        assert!(
            t.contains(&NodeId::from_bytes(*client_id.as_bytes())),
            "client's NodeId must be in the responder's routing table after a successful FindNode"
        );
    }

    conn.close(0u32.into(), b"bye");
    shutdown([], [&client_ep]).await?;
    support::reap("accept", accept_task).await??;
    shutdown([], [&server_ep]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn find_value_with_empty_store_returns_no_providers_but_closer_nodes() -> anyhow::Result<()> {
    // Empty record store ⇒ no providers in the response, but the
    // responder still surfaces its K-closest peers from the routing
    // table so the requester can continue iterative lookup (ADR 022
    // §FIND_VALUE Flow). This is the steady-state behaviour when a
    // hash hasn't been stored at this responder yet — distinct from
    // the previous PR's hand-coded placeholder behaviour, which
    // returned the same shape but bypassed the record store entirely.
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let rate_limiter = permissive_dht_rate_limiter(&metrics);

    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *server_id.as_bytes(),
    ))));
    {
        let mut t = routing.lock().expect("routing lock poisoned");
        t.insert(NodeId::from_bytes([0x42u8; 32]));
    }

    let handler = Arc::new(DhtHandler::with_routing(
        server_id,
        routing,
        rate_limiter,
        limiter,
        Arc::clone(&metrics),
        Arc::new(AllStaked::new()),
        empty_record_store(),
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
        hash: ContentHash::from_bytes([0x99u8; 32]),
        requester: NodeId::from_bytes(*client_ep.id().as_bytes()),
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
        resp.closer_nodes
            .iter()
            .any(|n| n == &NodeId::from_bytes([0x42u8; 32])),
        "seeded peer must appear in closer_nodes"
    );

    conn.close(0u32.into(), b"bye");
    shutdown([], [&client_ep]).await?;
    support::reap("accept", accept_task).await??;
    shutdown([], [&server_ep]).await?;
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
    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *server_id.as_bytes(),
    ))));
    let handler = Arc::new(DhtHandler::with_routing(
        server_id,
        Arc::clone(&routing),
        rate_limiter,
        limiter,
        Arc::clone(&metrics),
        Arc::new(AllStaked::new()),
        empty_record_store(),
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
        target: NodeId::from_bytes([0x12; 32]),
        requester: NodeId::from_bytes(attacker_id),
    };
    let payload = encode_message(&wire::DhtMessage::FindNode(req))?;
    write_frame(&mut send, &payload).await?;
    send.finish()?;
    let frame = read_frame(&mut recv).await?;
    let (_msg, _) = decode_message::<wire::DhtMessage>(&frame)?;

    conn.close(0u32.into(), b"bye");
    shutdown([], [&client_ep]).await?;
    support::reap("accept", accept_task).await??;
    shutdown([], [&server_ep]).await?;

    let t = routing.lock().expect("routing lock poisoned");
    assert!(
        t.contains(&NodeId::from_bytes(*client_id.as_bytes())),
        "authenticated client must be inserted by the connection-level refresh"
    );
    assert!(
        !t.contains(&NodeId::from_bytes(attacker_id)),
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
        let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *server_id.as_bytes(),
        ))));
        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            metrics,
            Arc::new(AllStaked::new()),
            empty_record_store(),
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

    /// Malformed postcard body under a *known* discriminant →
    /// `APP_ERR_MALFORMED_MESSAGE` (0x03). Distinct from an unknown
    /// discriminant, which is `UNSUPPORTED_MESSAGE` (see
    /// `unknown_discriminant_returns_unsupported_message_code`).
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
        // Discriminant 0x00 is `FindValue`, a known variant, but its body
        // (`hash` + `requester`, 64 fixed bytes) is truncated to 3 bytes —
        // postcard hits end-of-input mid-struct. A genuine parse fault, so
        // the ADR 013 code is MALFORMED, not UNSUPPORTED.
        let truncated = [0x00u8, 0x01, 0x02];
        write_frame(&mut send, &truncated).await?;
        send.finish()?;
        let r = recv.read_to_end(64).await;
        assert_close_code(r, APP_ERR_MALFORMED_MESSAGE);
        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        // The server task is expected to surface the rejection as an Err.
        let _ = support::reap("accept", accept_task).await;
        shutdown([], [&server_ep]).await?;
        Ok(())
    }

    /// Unknown enum discriminant → `APP_ERR_UNSUPPORTED_MESSAGE` (0x01), NOT
    /// `MALFORMED_MESSAGE`. ADR 013 §Application Error Codes: a frame naming a
    /// variant this build does not know is the Tier-2 graceful-evolution
    /// signal, distinct from a genuine parse fault.
    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_discriminant_returns_unsupported_message_code() -> anyhow::Result<()> {
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
        // Discriminant 0x55 (85) is far past DhtMessage's 8 declared variants —
        // an unknown variant. The trailing bytes are irrelevant.
        let unknown = [0x55u8, 0x00, 0x00, 0x00];
        write_frame(&mut send, &unknown).await?;
        send.finish()?;
        let r = recv.read_to_end(64).await;
        assert_close_code(r, APP_ERR_UNSUPPORTED_MESSAGE);
        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        // The server task is expected to surface the rejection as an Err, and it
        // ends on its own once the client's connection closes — so it is joined
        // here rather than aborted by `shutdown`.
        let _ = support::reap("accept", accept_task).await;
        shutdown([], [&server_ep]).await?;
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
            target: NodeId::from_bytes([0u8; 32]),
            closer_nodes: wire::CloserNodes::default(),
        });
        let payload = encode_message(&resp)?;
        write_frame(&mut send, &payload).await?;
        send.finish()?;
        let r = recv.read_to_end(64).await;
        assert_close_code(r, APP_ERR_UNSUPPORTED_MESSAGE);
        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        // The server task is expected to surface the rejection as an Err, and it
        // ends on its own once the client's connection closes — so it is joined
        // here rather than aborted by `shutdown`.
        let _ = support::reap("accept", accept_task).await;
        shutdown([], [&server_ep]).await?;
        Ok(())
    }

    /// `BatchStore` whose batch-level `holder` does not equal the
    /// authenticated QUIC `NodeId` → `APP_ERR_MALFORMED_MESSAGE` (0x03),
    /// stream-close-without-ack. Per ADR 022 §Batch token accounting
    /// step 2 the receiver rejects the **entire** batch with
    /// `MALFORMED_MESSAGE` on a holder mismatch (distinct from the
    /// per-hash `Store` path, which acks `accepted: false`): a batch
    /// claiming an identity the connection cannot prove is malformed.
    /// `spin_up_dht_server` uses `AllStaked`, so the staker filter would
    /// pass — the holder check is what fires.
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_store_holder_mismatch_returns_malformed_code() -> anyhow::Result<()> {
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
            hashes: vec![
                ContentHash::from_bytes([0x01u8; 32]),
                ContentHash::from_bytes([0x02u8; 32]),
                ContentHash::from_bytes([0x03u8; 32]),
            ],
            // Claim a holder that is NOT the authenticated client id.
            holder: NodeId::from_bytes([0xAB; 32]),
        });
        let payload = encode_message(&req)?;
        write_frame(&mut send, &payload).await?;
        send.finish()?;
        let r = recv.read_to_end(64).await;
        assert_close_code(r, APP_ERR_MALFORMED_MESSAGE);
        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        // The server task is expected to surface the rejection as an Err, and it
        // ends on its own once the client's connection closes — so it is joined
        // here rather than aborted by `shutdown`.
        let _ = support::reap("accept", accept_task).await;
        shutdown([], [&server_ep]).await?;
        Ok(())
    }

    /// A `BatchStore` carrying more than `MAX_BATCH_STORE_HASHES` (256)
    /// hashes → `APP_ERR_MALFORMED_MESSAGE` (0x03), stream-close-without-
    /// ack (ADR 022 §STORE Flow / AC 19). The oversize `hashes` vec is
    /// rejected at the wire-decode boundary
    /// (`deserialize_batch_hashes`), which `frame_err_code` maps to
    /// `MALFORMED_MESSAGE`. The publisher is expected to split the set,
    /// not back off.
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_store_over_cap_returns_malformed_code() -> anyhow::Result<()> {
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
        // 257 hashes > the 256 wire cap.
        let req = wire::DhtMessage::BatchStore(wire::BatchStoreRequest {
            hashes: vec![
                ContentHash::from_bytes([0x07u8; 32]);
                decdn_protocol::dht::MAX_BATCH_STORE_HASHES + 1
            ],
            holder: NodeId::from_bytes(*client_ep.id().as_bytes()),
        });
        let payload = encode_message(&req)?;
        write_frame(&mut send, &payload).await?;
        send.finish()?;
        let r = recv.read_to_end(64).await;
        assert_close_code(r, APP_ERR_MALFORMED_MESSAGE);
        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        // The server task is expected to surface the rejection as an Err, and it
        // ends on its own once the client's connection closes — so it is joined
        // here rather than aborted by `shutdown`.
        let _ = support::reap("accept", accept_task).await;
        shutdown([], [&server_ep]).await?;
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
        // path.
        let rate_cfg = DhtRateLimitConfig {
            per_peer_rate_per_sec: 1e6,
            per_peer_burst: u32::MAX,
            per_ip_rate_per_sec: 1e6,
            per_ip_burst: u32::MAX,
            global_rate_per_sec: 1.0,
            global_burst: 1,
            max_tracked_per_ip: 4096,
            max_tracked_per_peer: 4096,
        };
        let rate_limiter = Arc::new(DhtRateLimiter::new(&rate_cfg, Arc::clone(&metrics)));
        let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *server_id.as_bytes(),
        ))));
        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            metrics,
            Arc::new(AllStaked::new()),
            empty_record_store(),
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
            target: NodeId::from_bytes([0u8; 32]),
            requester: NodeId::from_bytes(*client_ep.id().as_bytes()),
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
        shutdown([], [&client_ep]).await?;
        // The server task is expected to surface the rejection as an Err, and it
        // ends on its own once the client's connection closes — so it is joined
        // here rather than aborted by `shutdown`.
        let _ = support::reap("accept", accept_task).await;
        shutdown([], [&server_ep]).await?;
        Ok(())
    }
}

// ADR 022 §STORE Flow + §FIND_VALUE Flow — real record-store admission.
// PR 3 of #320 replaces the previous "always reject" placeholder with
// the spec-conformant `(holder == authenticated NodeId) + active-staker
// filter + per-publisher quota + global LRU + receiver-anchored TTL`
// pipeline. These tests pin the boundary conditions a future refactor
// could regress.

mod store_admission {
    use super::*;

    /// Spin up a DHT server where the client is in the active-staker
    /// set, then issue one `Store` and one `FindValue` and verify the
    /// stored holder comes back in `providers`.
    #[tokio::test(flavor = "multi_thread")]
    #[allow(clippy::too_many_lines)] // Linear setup → Store → FindValue → assert flow; splitting into helpers loses the readable narrative.
    async fn store_then_find_value_roundtrip_for_staked_publisher() -> anyhow::Result<()> {
        let server_sk = fresh_key();
        let server_id = server_sk.public();
        let metrics = Arc::new(Metrics::new());
        let limiter = permissive_limiter(&metrics);
        let rate_limiter = permissive_dht_rate_limiter(&metrics);
        let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *server_id.as_bytes(),
        ))));
        let records = empty_record_store();

        // Need the client NodeId in the active-staker set BEFORE we
        // build the handler — capture the client key first.
        let client_sk = fresh_key();
        let client_id = client_sk.public();
        let mut active = std::collections::HashSet::new();
        active.insert(NodeId::from_bytes(*client_id.as_bytes()));
        let staker_set: Arc<dyn StakerSet> =
            Arc::new(decdn_node::dht::staker_set::ConfigStakerSet::new(active));

        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            Arc::clone(&metrics),
            staker_set,
            Arc::clone(&records),
        ));
        let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_DHT.to_vec()]).await?;
        let server_ep_bg = server_ep.clone();
        let accept_task = tokio::spawn(async move {
            // Two streams on one connection: Store, then FindValue.
            // Both call `handler.accept` which loops over `accept_bi`.
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
        let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
        let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
        let conn = client_ep
            .connect(target, ALPN_DHT)
            .await
            .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

        let target_hash = ContentHash::from_bytes([0xAAu8; 32]);

        // Store the record.
        {
            let (mut s, mut r) = conn
                .open_bi()
                .await
                .map_err(|e| anyhow::anyhow!("open_bi store: {e}"))?;
            let req = wire::DhtMessage::Store(wire::StoreRequest {
                hash: target_hash,
                holder: NodeId::from_bytes(*client_id.as_bytes()),
            });
            let payload = encode_message(&req)?;
            write_frame(&mut s, &payload).await?;
            s.finish()?;
            let frame = read_frame(&mut r).await?;
            let (msg, _) = decode_message::<wire::DhtMessage>(&frame)?;
            let ack = match msg {
                wire::DhtMessage::StoreAck(a) => a,
                other => anyhow::bail!("expected StoreAck, got {other:?}"),
            };
            assert!(ack.accepted, "staked publisher's Store must be accepted");
            assert_eq!(ack.hash, target_hash);
        }

        // FindValue should now surface the stored holder.
        {
            let (mut s, mut r) = conn
                .open_bi()
                .await
                .map_err(|e| anyhow::anyhow!("open_bi find: {e}"))?;
            let req = wire::DhtMessage::FindValue(wire::FindValueRequest {
                hash: target_hash,
                requester: NodeId::from_bytes(*client_id.as_bytes()),
            });
            let payload = encode_message(&req)?;
            write_frame(&mut s, &payload).await?;
            s.finish()?;
            let frame = read_frame(&mut r).await?;
            let (msg, _) = decode_message::<wire::DhtMessage>(&frame)?;
            let resp = match msg {
                wire::DhtMessage::FindValueResponse(r) => r,
                other => anyhow::bail!("expected FindValueResponse, got {other:?}"),
            };
            assert_eq!(resp.hash, target_hash);
            assert_eq!(
                resp.providers,
                vec![NodeId::from_bytes(*client_id.as_bytes())],
                "stored holder must appear in providers"
            );
        }

        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        support::reap("accept", accept_task).await??;
        shutdown([], [&server_ep]).await?;
        Ok(())
    }

    /// `Store` from a non-staked publisher must be rejected — the
    /// active-staker filter is the load-bearing admission check that
    /// stops every randomly-rotating attacker `NodeId` from publishing
    /// records.
    #[tokio::test(flavor = "multi_thread")]
    async fn store_from_non_staked_publisher_is_rejected() -> anyhow::Result<()> {
        let server_sk = fresh_key();
        let server_id = server_sk.public();
        let metrics = Arc::new(Metrics::new());
        let limiter = permissive_limiter(&metrics);
        let rate_limiter = permissive_dht_rate_limiter(&metrics);
        let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *server_id.as_bytes(),
        ))));

        // Empty staker set ⇒ EVERY publisher is non-staked ⇒ every
        // Store rejects. This is the "no operator opt-in" default.
        let staker_set: Arc<dyn StakerSet> =
            Arc::new(decdn_node::dht::staker_set::ConfigStakerSet::empty());
        let records = empty_record_store();

        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            Arc::clone(&metrics),
            staker_set,
            Arc::clone(&records),
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
        let (mut s, mut r) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
        let req = wire::DhtMessage::Store(wire::StoreRequest {
            hash: ContentHash::from_bytes([0xCCu8; 32]),
            holder: NodeId::from_bytes(*client_ep.id().as_bytes()),
        });
        let payload = encode_message(&req)?;
        write_frame(&mut s, &payload).await?;
        s.finish()?;
        let frame = read_frame(&mut r).await?;
        let (msg, _) = decode_message::<wire::DhtMessage>(&frame)?;
        let ack = match msg {
            wire::DhtMessage::StoreAck(a) => a,
            other => anyhow::bail!("expected StoreAck, got {other:?}"),
        };
        assert!(
            !ack.accepted,
            "Store from non-staked publisher must NOT be accepted"
        );
        // Record store is still empty post-rejection.
        assert!(records.lock().expect("records lock").is_empty());

        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        support::reap("accept", accept_task).await??;
        shutdown([], [&server_ep]).await?;
        Ok(())
    }

    /// Lying-`holder` attack: an authenticated peer A sends
    /// `Store { holder: B }` claiming to be a different staked node B.
    /// The handler MUST reject this even when B is in the staker set,
    /// because the QUIC handshake proves only A's identity, not B's.
    /// Without this check a single staked Sybil could publish records
    /// on behalf of every other staker.
    #[tokio::test(flavor = "multi_thread")]
    async fn store_with_holder_not_authenticated_is_rejected() -> anyhow::Result<()> {
        let server_sk = fresh_key();
        let server_id = server_sk.public();
        let metrics = Arc::new(Metrics::new());
        let limiter = permissive_limiter(&metrics);
        let rate_limiter = permissive_dht_rate_limiter(&metrics);
        let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *server_id.as_bytes(),
        ))));

        // Both the client AND the fake `holder` (B) are staked, so the
        // staker filter alone would let this through — the
        // authenticated-NodeId check is what catches it.
        let client_sk = fresh_key();
        let client_id = client_sk.public();
        let fake_holder: [u8; 32] = [0xBB; 32];
        let mut active = std::collections::HashSet::new();
        active.insert(NodeId::from_bytes(*client_id.as_bytes()));
        active.insert(NodeId::from_bytes(fake_holder));
        let staker_set: Arc<dyn StakerSet> =
            Arc::new(decdn_node::dht::staker_set::ConfigStakerSet::new(active));
        let records = empty_record_store();

        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            Arc::clone(&metrics),
            staker_set,
            Arc::clone(&records),
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
        let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
        let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
        let conn = client_ep
            .connect(target, ALPN_DHT)
            .await
            .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
        let (mut s, mut r) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
        let req = wire::DhtMessage::Store(wire::StoreRequest {
            hash: ContentHash::from_bytes([0xDDu8; 32]),
            holder: NodeId::from_bytes(fake_holder), // != client_id
        });
        let payload = encode_message(&req)?;
        write_frame(&mut s, &payload).await?;
        s.finish()?;
        let frame = read_frame(&mut r).await?;
        let (msg, _) = decode_message::<wire::DhtMessage>(&frame)?;
        let ack = match msg {
            wire::DhtMessage::StoreAck(a) => a,
            other => anyhow::bail!("expected StoreAck, got {other:?}"),
        };
        assert!(
            !ack.accepted,
            "Store with holder != authenticated NodeId must be rejected"
        );
        // Record store still empty — the rejection happened pre-insert.
        assert!(records.lock().expect("records lock").is_empty());

        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        support::reap("accept", accept_task).await??;
        shutdown([], [&server_ep]).await?;
        Ok(())
    }
}

// ADR 022 §STORE Flow Batched STORE + §Batch token accounting (#648,
// ACs 17-18). The handler admits `BatchStore`: a single batch-level
// holder check, then per-hash admission (rate limit + staker filter +
// record-store insert) matching what `n` separate `Store`s would do,
// with one `bool` per hash in request order.
mod batch_store_admission {
    use super::*;

    /// Drive one `BatchStore` from `client_sk` against a freshly spun-up
    /// server whose active-staker set is exactly `{client}` and whose
    /// rate limiter is `rate_cfg`. Returns the decoded ack plus the
    /// server's record store so the caller can assert on inserted state.
    #[allow(clippy::too_many_lines)]
    async fn run_batch_store(
        client_sk: SecretKey,
        hashes: Vec<[u8; 32]>,
        rate_cfg: DhtRateLimitConfig,
    ) -> anyhow::Result<(wire::BatchStoreAck, Arc<Mutex<RecordStore>>, Arc<Metrics>)> {
        let server_sk = fresh_key();
        let server_id = server_sk.public();
        let metrics = Arc::new(Metrics::new());
        let limiter = permissive_limiter(&metrics);
        let rate_limiter = Arc::new(DhtRateLimiter::new(&rate_cfg, Arc::clone(&metrics)));
        let metrics_handle = Arc::clone(&metrics);
        let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *server_id.as_bytes(),
        ))));
        let records = empty_record_store();

        let client_id = client_sk.public();
        let mut active = std::collections::HashSet::new();
        active.insert(NodeId::from_bytes(*client_id.as_bytes()));
        let staker_set: Arc<dyn StakerSet> =
            Arc::new(decdn_node::dht::staker_set::ConfigStakerSet::new(active));

        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            Arc::clone(&metrics),
            staker_set,
            Arc::clone(&records),
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
        let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
        let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
        let conn = client_ep
            .connect(target, ALPN_DHT)
            .await
            .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
        let (mut s, mut r) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
        let req = wire::DhtMessage::BatchStore(wire::BatchStoreRequest {
            hashes: hashes.into_iter().map(ContentHash::from_bytes).collect(),
            holder: NodeId::from_bytes(*client_id.as_bytes()),
        });
        let payload = encode_message(&req)?;
        write_frame(&mut s, &payload).await?;
        s.finish()?;
        let frame = read_frame(&mut r).await?;
        let (msg, _) = decode_message::<wire::DhtMessage>(&frame)?;
        let ack = match msg {
            wire::DhtMessage::BatchStoreAck(a) => a,
            other => anyhow::bail!("expected BatchStoreAck, got {other:?}"),
        };

        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        support::reap("accept", accept_task).await??;
        shutdown([], [&server_ep]).await?;
        Ok((ack, records, metrics_handle))
    }

    const fn permissive_rate_cfg() -> DhtRateLimitConfig {
        DhtRateLimitConfig {
            per_peer_rate_per_sec: 1e6,
            per_peer_burst: u32::MAX,
            per_ip_rate_per_sec: 1e6,
            per_ip_burst: u32::MAX,
            global_rate_per_sec: 1e6,
            global_burst: u32::MAX,
            max_tracked_per_ip: 4096,
            max_tracked_per_peer: 4096,
        }
    }

    /// AC 17: a `BatchStore` of `n` hashes from a staked publisher gets a
    /// `BatchStoreAck` with one `true` per hash, in request order, and
    /// every hash lands in the record store (a subsequent `FindValue`
    /// would find the holder).
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_store_all_admitted_for_staked_publisher() -> anyhow::Result<()> {
        let client_sk = fresh_key();
        let holder = NodeId::from_bytes(*client_sk.public().as_bytes());
        let hashes: Vec<[u8; 32]> = (1u8..=5).map(|i| [i; 32]).collect();
        let (ack, records, _metrics) =
            run_batch_store(client_sk, hashes.clone(), permissive_rate_cfg()).await?;
        assert_eq!(
            ack.results,
            vec![true; 5],
            "every hash from a staked publisher under permissive limits must be admitted, in order"
        );
        let mut store = records.lock().expect("records lock");
        for h in &hashes {
            assert!(
                store
                    .providers_at(&ContentHash::from_bytes(*h), 0)
                    .contains(&holder),
                "hash {h:?} must be in the record store after a batch admit"
            );
        }
        Ok(())
    }

    /// AC 17/18 boundary: a 1-hash batch (`extra = n - 1 = 0`, no stage-2
    /// tokens) admits exactly the single hash — the `n == 1` edge of the
    /// `k = 1 + min(b, n-1)` arithmetic.
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_store_single_hash_admitted() -> anyhow::Result<()> {
        let client_sk = fresh_key();
        let holder = NodeId::from_bytes(*client_sk.public().as_bytes());
        let (ack, records, _metrics) =
            run_batch_store(client_sk, vec![[0x42u8; 32]], permissive_rate_cfg()).await?;
        assert_eq!(ack.results, vec![true]);
        assert!(
            records
                .lock()
                .expect("records lock")
                .providers_at(&ContentHash::from_bytes([0x42u8; 32]), 0)
                .contains(&holder)
        );
        Ok(())
    }

    /// A holder-mismatch batch is a deliberate protocol rejection: it
    /// bumps the specific `dht_store_rejected_holder_mismatch` counter and
    /// closes the stream (MALFORMED), but must NOT inflate
    /// `dht_requests_failed`, which is reserved for post-admission internal
    /// failures (decode/write/timeout). Guards the dispatch-rejection →
    /// `Ok(())` path in `handle_one`.
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_store_holder_mismatch_does_not_bump_requests_failed() -> anyhow::Result<()> {
        let server_sk = fresh_key();
        let server_id = server_sk.public();
        let metrics = Arc::new(Metrics::new());
        let limiter = permissive_limiter(&metrics);
        let rate_limiter = permissive_dht_rate_limiter(&metrics);
        let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *server_id.as_bytes(),
        ))));
        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            Arc::clone(&metrics),
            Arc::new(AllStaked::new()),
            empty_record_store(),
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
        let (mut s, mut r) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
        let req = wire::DhtMessage::BatchStore(wire::BatchStoreRequest {
            hashes: vec![
                ContentHash::from_bytes([0x01u8; 32]),
                ContentHash::from_bytes([0x02u8; 32]),
            ],
            holder: NodeId::from_bytes([0xAB; 32]), // != authenticated client id
        });
        let payload = encode_message(&req)?;
        write_frame(&mut s, &payload).await?;
        s.finish()?;
        // The handler resets the stream; the read errors (we don't care
        // about the exact shape here, only the resulting metrics).
        let _ = r.read_to_end(64).await;
        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        support::reap("accept", accept_task).await??;
        shutdown([], [&server_ep]).await?;

        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dht_store_rejected_holder_mismatch_total 1"),
            "holder mismatch must bump its specific counter:\n{text}"
        );
        assert!(
            text.contains("decdn_dht_requests_failed_total 0"),
            "a deliberate protocol rejection must NOT inflate dht_requests_failed:\n{text}"
        );
        Ok(())
    }

    /// AC 17: per-hash admission matches a single `Store` — a non-staked
    /// publisher's whole batch is acked `false` (the active-staker filter
    /// is applied per-hash; the batch-level holder is valid here).
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_store_non_staked_publisher_all_rejected() -> anyhow::Result<()> {
        // Server's staker set is `{a_random_other}` ≠ the publisher, so
        // the publisher (whose holder == its authenticated id) passes the
        // holder check but fails the per-hash staker filter.
        let server_sk = fresh_key();
        let server_id = server_sk.public();
        let metrics = Arc::new(Metrics::new());
        let limiter = permissive_limiter(&metrics);
        let rate_limiter = permissive_dht_rate_limiter(&metrics);
        let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *server_id.as_bytes(),
        ))));
        let records = empty_record_store();
        // Staker set deliberately does NOT contain the client.
        let staker_set: Arc<dyn StakerSet> =
            Arc::new(decdn_node::dht::staker_set::ConfigStakerSet::empty());
        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            Arc::clone(&metrics),
            staker_set,
            Arc::clone(&records),
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
        let client_sk = fresh_key();
        let (client_ep, _) = local_endpoint(client_sk.clone(), vec![]).await?;
        let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
        let conn = client_ep
            .connect(target, ALPN_DHT)
            .await
            .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
        let (mut s, mut r) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
        let req = wire::DhtMessage::BatchStore(wire::BatchStoreRequest {
            hashes: vec![
                ContentHash::from_bytes([0x01u8; 32]),
                ContentHash::from_bytes([0x02u8; 32]),
            ],
            holder: NodeId::from_bytes(*client_sk.public().as_bytes()),
        });
        let payload = encode_message(&req)?;
        write_frame(&mut s, &payload).await?;
        s.finish()?;
        let frame = read_frame(&mut r).await?;
        let (msg, _) = decode_message::<wire::DhtMessage>(&frame)?;
        let ack = match msg {
            wire::DhtMessage::BatchStoreAck(a) => a,
            other => anyhow::bail!("expected BatchStoreAck, got {other:?}"),
        };
        assert_eq!(
            ack.results,
            vec![false, false],
            "a non-staked publisher's batch must be fully rejected per-hash"
        );
        assert!(records.lock().expect("records lock").is_empty());
        conn.close(0u32.into(), b"bye");
        shutdown([], [&client_ep]).await?;
        support::reap("accept", accept_task).await??;
        shutdown([], [&server_ep]).await?;
        Ok(())
    }

    /// AC 18: a `BatchStore` whose size exceeds the remaining per-peer
    /// rate-limit budget is partially admitted — the first `k` hashes are
    /// `true`, the over-budget tail is `false` (and not inserted). With
    /// per-peer burst 4: stage 1 charges 1 (3 left), stage 2 charges 3
    /// more, so `k = 4` of the 6 hashes are admitted.
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_store_partial_admit_when_over_rate_budget() -> anyhow::Result<()> {
        let client_sk = fresh_key();
        let holder = NodeId::from_bytes(*client_sk.public().as_bytes());
        let hashes: Vec<[u8; 32]> = (1u8..=6).map(|i| [i; 32]).collect();
        let rate_cfg = DhtRateLimitConfig {
            // per-peer is the binding layer: burst 4, slow refill so no
            // tokens come back mid-test.
            per_peer_rate_per_sec: 1.0,
            per_peer_burst: 4,
            per_ip_rate_per_sec: 1e9,
            per_ip_burst: u32::MAX,
            global_rate_per_sec: 1e9,
            global_burst: u32::MAX,
            max_tracked_per_ip: 4096,
            max_tracked_per_peer: 4096,
        };
        let (ack, records, metrics) = run_batch_store(client_sk, hashes.clone(), rate_cfg).await?;
        assert_eq!(
            ack.results,
            vec![true, true, true, true, false, false],
            "first k=4 hashes admitted (1 stage-1 + 3 stage-2), tail deferred"
        );
        // The 2 deferred-tail hashes bump the deferred counter (and are NOT
        // counted as per-hash rejections — that invariant is unit-tested).
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_dht_batch_store_hashes_deferred_rate_limit_total 2"),
            "2 deferred hashes must be reflected in the deferred counter:\n{text}"
        );
        let mut store = records.lock().expect("records lock");
        // Admitted hashes present; deferred ones absent.
        for h in hashes.iter().take(4) {
            assert!(
                store
                    .providers_at(&ContentHash::from_bytes(*h), 0)
                    .contains(&holder)
            );
        }
        for h in hashes.iter().skip(4) {
            assert!(
                !store
                    .providers_at(&ContentHash::from_bytes(*h), 0)
                    .contains(&holder),
                "deferred hash {h:?} must NOT be inserted"
            );
        }
        Ok(())
    }
}

// Publisher-side `client::batch_store` primitive driven against a real
// handler over loopback (ADR 022 §STORE Flow Batched STORE).
mod batch_store_client {
    use super::*;
    use decdn_node::dht::client;

    /// Spin up a staked-publisher DHT server whose accept loop serves an
    /// arbitrary number of inbound connections. Returns the server
    /// endpoint, its connect target, the publisher's key, the shared
    /// metrics handle, and the accept-loop join handle.
    #[allow(clippy::type_complexity)]
    async fn spin_up_looping_staked_server(
        client_id: [u8; 32],
    ) -> anyhow::Result<(
        Endpoint,
        EndpointAddr,
        iroh::PublicKey,
        Arc<Metrics>,
        tokio::task::JoinHandle<()>,
    )> {
        let server_sk = fresh_key();
        let server_id = server_sk.public();
        let metrics = Arc::new(Metrics::new());
        let limiter = permissive_limiter(&metrics);
        let rate_limiter = permissive_dht_rate_limiter(&metrics);
        let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *server_id.as_bytes(),
        ))));
        let mut active = std::collections::HashSet::new();
        active.insert(NodeId::from_bytes(client_id));
        let staker_set: Arc<dyn StakerSet> =
            Arc::new(decdn_node::dht::staker_set::ConfigStakerSet::new(active));
        let handler = Arc::new(DhtHandler::with_routing(
            server_id,
            routing,
            rate_limiter,
            limiter,
            Arc::clone(&metrics),
            staker_set,
            empty_record_store(),
        ));
        let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_DHT.to_vec()]).await?;
        let server_ep_bg = server_ep.clone();
        let accept_task = tokio::spawn(async move {
            while let Some(incoming) = server_ep_bg.accept().await {
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    let Ok(connecting) = incoming.accept() else {
                        return;
                    };
                    let Ok(conn) = connecting.await else {
                        return;
                    };
                    let _ = handler.accept(conn).await;
                });
            }
        });
        let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
        Ok((server_ep, target, server_id, metrics, accept_task))
    }

    /// AC 17 over the real outbound primitive: `client::batch_store`
    /// against a handler that supports batching returns one `accepted`
    /// per hash, all `true` for a staked publisher.
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_store_primitive_roundtrip() -> anyhow::Result<()> {
        let client_sk = fresh_key();
        let holder = *client_sk.public().as_bytes();
        let (server_ep, target, _sid, _metrics, accept_task) =
            spin_up_looping_staked_server(holder).await?;
        let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
        let hashes: Vec<[u8; 32]> = (1u8..=4).map(|i| [i; 32]).collect();
        let ack = client::batch_store(
            &client_ep,
            target,
            hashes
                .iter()
                .copied()
                .map(ContentHash::from_bytes)
                .collect(),
            NodeId::from_bytes(holder),
        )
        .await?;
        assert_eq!(ack.results, vec![true; 4]);
        shutdown([accept_task], [&client_ep, &server_ep]).await?;
        Ok(())
    }
}
