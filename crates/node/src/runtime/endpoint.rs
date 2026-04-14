//! Iroh `Endpoint` construction.

use std::sync::Arc;

use iroh::{Endpoint, SecretKey, endpoint::presets};

use crate::handlers::Handler;

/// Build an iroh `Endpoint` bound to `bind_port`, advertising the ALPNs served
/// by each handler in `handlers`.
pub async fn build(
    secret_key: &SecretKey,
    bind_port: u16,
    handlers: &[Arc<dyn Handler>],
) -> anyhow::Result<Endpoint> {
    let alpns: Vec<Vec<u8>> = handlers.iter().map(|h| h.alpn().to_vec()).collect();

    let bind_addr = std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, bind_port);

    let ep = Endpoint::builder(presets::N0)
        .secret_key(secret_key.clone())
        .alpns(alpns)
        .bind_addr(bind_addr)
        .map_err(|e| anyhow::anyhow!("invalid bind addr {bind_addr}: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("endpoint bind failed: {e}"))?;

    Ok(ep)
}
