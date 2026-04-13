//! Accept-loop: route accepted connections to handlers by ALPN.

use std::collections::HashMap;
use std::sync::Arc;

use iroh::Endpoint;

use crate::handlers::Handler;
use crate::metrics::Metrics;

/// Run the accept loop until `ep.accept()` returns `None` (endpoint closed).
pub async fn run(ep: Endpoint, handlers: Vec<Arc<dyn Handler>>, metrics: Arc<Metrics>) {
    let table: HashMap<Vec<u8>, Arc<dyn Handler>> = handlers
        .into_iter()
        .map(|h| (h.alpn().to_vec(), h))
        .collect();
    let table = Arc::new(table);

    while let Some(incoming) = ep.accept().await {
        let table = Arc::clone(&table);
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            if let Err(err) = handle_one(incoming, &table, &metrics).await {
                tracing::warn!(%err, "connection handling failed");
            }
        });
    }

    tracing::info!("accept loop exited");
}

async fn handle_one(
    incoming: iroh::endpoint::Incoming,
    table: &HashMap<Vec<u8>, Arc<dyn Handler>>,
    metrics: &Metrics,
) -> anyhow::Result<()> {
    let mut connecting = incoming
        .accept()
        .map_err(|e| anyhow::anyhow!("incoming accept failed: {e}"))?;

    let alpn = connecting
        .alpn()
        .await
        .map_err(|e| anyhow::anyhow!("alpn read failed: {e}"))?;

    let Some(handler) = table.get(&alpn).cloned() else {
        tracing::warn!(alpn = %String::from_utf8_lossy(&alpn), "unknown ALPN; dropping");
        return Ok(());
    };

    let conn = connecting
        .await
        .map_err(|e| anyhow::anyhow!("connection handshake failed: {e}"))?;

    metrics.connection_opened();
    let result = handler.handle(conn).await;
    metrics.connection_closed();
    result
}
