//! Two-endpoint loopback test for `cdn/probe/v1`.
//!
//! Spawns a server endpoint running the probe handler, connects a client
//! endpoint over iroh on localhost, sends a `ProbeRequest`, and verifies the
//! response echoes the nonce and reports the server's node id and rate.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use decdn_protocol::{
    ALPN_PROBE,
    message::{ProbeRequest, ProbeResponse},
};
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};

#[allow(dead_code)] // Test only exercises a subset; full trait/metrics surface is used by the binary.
#[path = "../src/handlers/mod.rs"]
mod handlers;

#[allow(dead_code)]
#[path = "../src/metrics.rs"]
mod metrics;

use handlers::{Handler, probe::ProbeHandler};
use metrics::Metrics;

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
    let bytes = postcard::to_allocvec(&req)?;
    send.write_all(&bytes)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    let resp_bytes = recv
        .read_to_end(4096)
        .await
        .map_err(|e| anyhow::anyhow!("read: {e}"))?;
    let resp: ProbeResponse = postcard::from_bytes(&resp_bytes)?;

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
