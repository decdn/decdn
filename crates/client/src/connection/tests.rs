use std::time::Duration;

use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMode};
use tokio::runtime::Handle;

use decdn_protocol::ALPN_CLIENT;

/// Bind a loopback endpoint with relays off, returning it and a dialable
/// address.
async fn loopback_endpoint(alpns: Vec<Vec<u8>>) -> (Endpoint, EndpointAddr) {
    let ep = Endpoint::builder(presets::Minimal)
        .alpns(alpns)
        .relay_mode(RelayMode::Disabled)
        .bind_addr(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::LOCALHOST,
            0,
        ))
        .expect("loopback bind address")
        .bind()
        .await
        .expect("bind loopback endpoint");
    let socket = ep
        .bound_sockets()
        .into_iter()
        .find(std::net::SocketAddr::is_ipv4)
        .expect("an IPv4 bound socket");
    let addr = EndpointAddr::new(ep.id()).with_ip_addr(socket);
    (ep, addr)
}

/// How the throwaway runtime dials.
#[derive(Clone, Copy)]
enum Via {
    /// A one-shot [`super::dial`].
    OneShot,
    /// A [`super::WarmConnection::connect`].
    Warm,
}

/// Dial a peer that holds its side open, from a throwaway current-thread
/// runtime on its own thread, then drop that runtime — the shape of a
/// `decdn-node` pull leg. `dial_runtime` is passed through to the dial.
/// Returns the dialling endpoint, the dialled handle (still held, so the
/// connection is still open), and the peer endpoint, which must outlive the
/// test's close.
async fn dial_from_a_dropped_runtime(
    via: Via,
    dial_runtime: Option<Handle>,
) -> (Endpoint, Box<dyn std::any::Any + Send>, Endpoint) {
    let (peer, peer_addr) = loopback_endpoint(vec![ALPN_CLIENT.to_vec()]).await;
    let accept_peer = peer.clone();
    tokio::spawn(async move {
        if let Some(incoming) = accept_peer.accept().await
            && let Ok(connecting) = incoming.accept()
        {
            let _held = connecting.await;
            std::future::pending::<()>().await;
        }
    });
    let (ep, _) = loopback_endpoint(Vec::new()).await;
    let dialer = ep.clone();
    let conn = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build throwaway runtime");
        let held: Box<dyn std::any::Any + Send> = match via {
            Via::OneShot => Box::new(
                rt.block_on(super::dial(
                    &dialer,
                    peer_addr,
                    dial_runtime.as_ref(),
                    "connect failed",
                ))
                .expect("dial the peer"),
            ),
            Via::Warm => Box::new(
                rt.block_on(super::WarmConnection::connect(
                    &dialer,
                    peer_addr,
                    Duration::from_secs(10),
                    dial_runtime.as_ref(),
                ))
                .expect("warm-dial the peer"),
            ),
        };
        drop(rt);
        held
    })
    .join()
    .expect("dialling thread");
    (ep, conn, peer)
}

/// A dial run on a long-lived runtime leaves the connection's driver there,
/// so the connection drains after the dialling runtime is gone and the
/// endpoint closes promptly (#2185).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dial_on_a_long_lived_runtime_lets_the_endpoint_close() {
    let (ep, _conn, _peer) =
        dial_from_a_dropped_runtime(Via::OneShot, Some(Handle::current())).await;
    tokio::time::timeout(Duration::from_secs(10), ep.close())
        .await
        .expect("the endpoint must close once its connection drains");
}

/// The warm connection's dial honours the dial runtime the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_warm_dial_on_a_long_lived_runtime_lets_the_endpoint_close() {
    let (ep, _warm, _peer) = dial_from_a_dropped_runtime(Via::Warm, Some(Handle::current())).await;
    tokio::time::timeout(Duration::from_secs(10), ep.close())
        .await
        .expect("the endpoint must close once its warm connection drains");
}

/// A dial whose runtime is shutting down fails as this node's fault, not the
/// peer's, and says the task was cancelled rather than that it panicked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dial_on_a_shut_down_runtime_is_a_local_fault() {
    let (peer, peer_addr) = loopback_endpoint(vec![ALPN_CLIENT.to_vec()]).await;
    let (ep, _) = loopback_endpoint(Vec::new()).await;
    let gone = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build the dial runtime");
    let handle = gone.handle().clone();
    gone.shutdown_background();

    let err = tokio::time::timeout(
        Duration::from_secs(10),
        super::dial(&ep, peer_addr, Some(&handle), "connect failed"),
    )
    .await
    .expect("a dial on a shut-down runtime must fail, not hang")
    .expect_err("a dial on a shut-down runtime must fail");
    assert!(
        err.downcast_ref::<crate::LocalPullFault>().is_some(),
        "the failure must be marked local: {err:#}"
    );
    assert!(
        format!("{err:#}").contains("cancelled"),
        "the failure must say the task was cancelled: {err:#}"
    );
    drop(peer);
}

/// A peer that accepts every connection and holds each one open, counting
/// them. Returns its address and the count.
async fn holding_peer() -> (
    Endpoint,
    EndpointAddr,
    std::sync::Arc<std::sync::atomic::AtomicU64>,
) {
    let (peer, peer_addr) = loopback_endpoint(vec![ALPN_CLIENT.to_vec()]).await;
    let accepted = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (accept_peer, count) = (peer.clone(), std::sync::Arc::clone(&accepted));
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Some(incoming) = accept_peer.accept().await {
            if let Ok(connecting) = incoming.accept()
                && let Ok(conn) = connecting.await
            {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                held.push(conn);
            }
        }
    });
    (peer, peer_addr, accepted)
}

/// Wait until the peer has accepted `n` connections, then check it
/// accepts no more for a moment.
async fn assert_accepted(accepted: &std::sync::atomic::AtomicU64, n: u64) {
    let load = || accepted.load(std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(10), async {
        while load() < n {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the peer accepts the dials");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(load(), n, "the peer accepts exactly {n} connections");
}

/// Two gets for one node dial once and share the connection, and two
/// concurrent gets share one dial.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_gets_for_one_node_dial_once() {
    let (_peer, peer_addr, accepted) = holding_peer().await;
    let (ep, _) = loopback_endpoint(Vec::new()).await;
    let connections = super::Connections::new(ep.clone());

    let (a, b) = tokio::join!(connections.get(&peer_addr), connections.get(&peer_addr));
    let (a, b) = (a.expect("first get"), b.expect("second get"));
    let c = connections.get(&peer_addr).await.expect("third get");
    assert!(std::sync::Arc::ptr_eq(&a, &b) && std::sync::Arc::ptr_eq(&a, &c));
    assert_eq!(connections.dials(), 1, "one dial for three gets");
    assert_accepted(&accepted, 1).await;
}

/// A get after the node's connection closes dials again. Invalidating a
/// live connection unpins it too: the next get dials, and the unpinned
/// connection stays open for its holder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_get_after_the_connection_closes_dials_again() {
    let (_peer, peer_addr, accepted) = holding_peer().await;
    let (ep, _) = loopback_endpoint(Vec::new()).await;
    let connections = super::Connections::new(ep.clone());

    let first = connections.get(&peer_addr).await.expect("first get");
    first.connection().close(0u32.into(), b"test-closed");
    assert!(first.is_closed());
    let second = connections.get(&peer_addr).await.expect("get after close");
    assert!(!std::sync::Arc::ptr_eq(&first, &second));
    assert!(!second.is_closed());
    assert_eq!(connections.dials(), 2, "a closed connection redials");

    connections.invalidate(peer_addr.id, &second).await;
    let third = connections
        .get(&peer_addr)
        .await
        .expect("get after invalidate");
    assert!(
        !std::sync::Arc::ptr_eq(&second, &third),
        "invalidate unpins a live connection"
    );
    assert!(
        !second.is_closed(),
        "its holder keeps the unpinned connection"
    );
    assert_eq!(connections.dials(), 3);
    assert_accepted(&accepted, 3).await;
}

/// A stale invalidate leaves a newer connection pinned: leg A faults on
/// C1 and invalidates it after leg B already redialled C2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_invalidate_keeps_the_newer_connection() {
    let (_peer, peer_addr, _accepted) = holding_peer().await;
    let (ep, _) = loopback_endpoint(Vec::new()).await;
    let connections = super::Connections::new(ep.clone());

    let c1 = connections.get(&peer_addr).await.expect("C1");
    c1.connection().close(0u32.into(), b"test-closed");
    let c2 = connections.get(&peer_addr).await.expect("B redials C2");
    connections.invalidate(peer_addr.id, &c1).await;
    let after = connections.get(&peer_addr).await.expect("get after A");
    assert!(std::sync::Arc::ptr_eq(&c2, &after), "C2 stays pinned");
    assert!(!c2.is_closed());
    assert_eq!(connections.dials(), 2);
}

/// A peer that echoes every stream back.
async fn echo_peer() -> (Endpoint, EndpointAddr) {
    let (peer, peer_addr) = loopback_endpoint(vec![ALPN_CLIENT.to_vec()]).await;
    let accept_peer = peer.clone();
    tokio::spawn(async move {
        while let Some(incoming) = accept_peer.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    tokio::spawn(async move {
                        let mut buf = [0u8; 64];
                        while let Ok(Some(n)) = recv.read(&mut buf).await {
                            let Some(bytes) = buf.get(..n) else { break };
                            if send.write_all(bytes).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            });
        }
    });
    (peer, peer_addr)
}

/// A sibling leg still streaming on C1 keeps streaming after C1 is
/// unpinned, and after the map and the faulting leg drop their handles.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sibling_leg_keeps_streaming_on_an_unpinned_connection() {
    let (_peer, peer_addr) = echo_peer().await;
    let (ep, _) = loopback_endpoint(Vec::new()).await;
    let connections = super::Connections::new(ep.clone());

    let faulting = connections.get(&peer_addr).await.expect("leg A");
    let sibling = connections.get(&peer_addr).await.expect("leg B");
    let (mut send, mut recv) = sibling.connection().open_bi().await.expect("open_bi");
    let mut echo = [0u8; 1];
    send.write_all(b"a").await.expect("write before");
    recv.read_exact(&mut echo).await.expect("read before");
    assert_eq!(&echo, b"a");

    connections.invalidate(peer_addr.id, &faulting).await;
    drop(faulting);
    let next = connections.get(&peer_addr).await.expect("next leg");
    assert!(!std::sync::Arc::ptr_eq(&sibling, &next));

    send.write_all(b"b").await.expect("write after");
    recv.read_exact(&mut echo).await.expect("read after");
    assert_eq!(&echo, b"b", "the sibling's stream still flows");
    assert!(!sibling.is_closed());
}

/// A peer that takes the connection and never answers a stream: the leg's
/// open falls under its deadline, which unpins the connection, so the
/// next get dials a new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leg_that_times_out_unpins_its_connection() {
    use alloy::primitives::{Address, U256};
    use decdn_bao_range::align_range;

    use crate::source::BlobSource as _;

    let (_peer, peer_addr, _accepted) = holding_peer().await;
    let (ep, _) = loopback_endpoint(Vec::new()).await;
    let connections = super::Connections::new(ep.clone());
    let ctx = crate::source::ctx_with(0xB0, U256::from(1_000_000u64));
    let ledger = std::sync::Arc::new(ctx.new_ledger());
    let slash = decdn_incentive::slash_judge_domain(1, Address::ZERO);
    let source = crate::PeerSource::new(
        &ep,
        peer_addr.clone(),
        std::sync::Arc::new(std::sync::Mutex::new(ctx)),
        ledger,
        &slash,
        Address::repeat_byte(0xB0),
        [0u8; 32],
        0,
        0,
        crate::PullDeadlines::new(Duration::from_millis(500), Duration::from_secs(5), 0)
            .expect("deadlines"),
        Some(connections.clone()),
    );

    let pinned = connections.get(&peer_addr).await.expect("first get");
    let err = source
        .open([7u8; 32], align_range(0, 0, 1024).expect("range"))
        .await
        .expect_err("a peer that never answers must time out");
    assert!(
        err.downcast_ref::<crate::PullTimeout>().is_some(),
        "expected a PullTimeout: {err:#}"
    );
    let next = connections
        .get(&peer_addr)
        .await
        .expect("get after the fault");
    assert!(!std::sync::Arc::ptr_eq(&pinned, &next));
    assert_eq!(connections.dials(), 2);
}

/// The control: the same dial on the dropped runtime itself takes the
/// connection's driver down with it, and the endpoint's close never
/// finishes. Without it, the test above would pass for a setup that never
/// stranded anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dial_on_a_dropped_runtime_blocks_the_endpoint_close() {
    let (ep, _conn, _peer) = dial_from_a_dropped_runtime(Via::OneShot, None).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), ep.close())
            .await
            .is_err(),
        "a connection whose driver was dropped must keep the close waiting"
    );
}
