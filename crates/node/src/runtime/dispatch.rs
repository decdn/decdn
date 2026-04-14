//! Accept-loop: route accepted connections to handlers by ALPN.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use iroh::Endpoint;
use tokio::task::JoinSet;

use crate::handlers::Handler;
use crate::metrics::Metrics;

/// Ceiling on ALPN negotiation + handshake completion. Caps slow-handshake
/// attacks that would otherwise occupy a connection slot until QUIC's idle
/// timeout elapsed.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Run the accept loop until `ep.accept()` returns `None` (endpoint closed),
/// then drain outstanding per-connection tasks so shutdown is bounded.
#[allow(clippy::cognitive_complexity)] // Linear: build table, accept loop, drain.
pub async fn run(ep: Endpoint, handlers: Vec<Arc<dyn Handler>>, metrics: Arc<Metrics>) {
    let table: HashMap<Vec<u8>, Arc<dyn Handler>> = handlers
        .into_iter()
        .map(|h| (h.alpn().to_vec(), h))
        .collect();
    let table = Arc::new(table);

    let mut tasks: JoinSet<()> = JoinSet::new();

    while let Some(incoming) = ep.accept().await {
        let table = Arc::clone(&table);
        let metrics = Arc::clone(&metrics);
        tasks.spawn(async move {
            if let Err(err) = handle_one(incoming, &table, &metrics).await {
                tracing::warn!(%err, "connection handling failed");
            }
        });

        // Reap finished tasks opportunistically so the JoinSet doesn't grow
        // unboundedly under a steady load.
        while let Some(result) = tasks.try_join_next() {
            if let Err(err) = result {
                tracing::warn!(%err, "connection task failed");
            }
        }
    }

    tracing::info!(
        pending = tasks.len(),
        "accept loop exited; draining handlers"
    );
    while let Some(result) = tasks.join_next().await {
        if let Err(err) = result {
            tracing::warn!(%err, "connection task failed during shutdown");
        }
    }
    tracing::info!("connection handler drain complete");
}

async fn handle_one(
    incoming: iroh::endpoint::Incoming,
    table: &HashMap<Vec<u8>, Arc<dyn Handler>>,
    metrics: &Metrics,
) -> anyhow::Result<()> {
    let mut connecting = incoming
        .accept()
        .map_err(|e| anyhow::anyhow!("incoming accept failed: {e}"))?;

    let alpn = tokio::time::timeout(HANDSHAKE_TIMEOUT, connecting.alpn())
        .await
        .map_err(|_| anyhow::anyhow!("ALPN negotiation timed out after {HANDSHAKE_TIMEOUT:?}"))?
        .map_err(|e| anyhow::anyhow!("alpn read failed: {e}"))?;

    let Some(handler) = table.get(&alpn).cloned() else {
        tracing::warn!(alpn = %String::from_utf8_lossy(&alpn), "unknown ALPN; dropping");
        return Ok(());
    };

    let conn = tokio::time::timeout(HANDSHAKE_TIMEOUT, connecting)
        .await
        .map_err(|_| anyhow::anyhow!("handshake timed out after {HANDSHAKE_TIMEOUT:?}"))?
        .map_err(|e| anyhow::anyhow!("connection handshake failed: {e}"))?;

    let _guard = metrics.connection_guard();
    handler.handle(conn).await
}
