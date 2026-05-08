//! End-to-end test for `decdn node top`: spawn the daemon's metrics
//! server in-process and assert the CLI's parsed snapshot fields
//! against real bytes the encoder wrote.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::test]
async fn node_top_pipeline_reads_real_metrics_server() -> anyhow::Result<()> {
    // Bind on an ephemeral loopback port; same pattern admin_peers.rs
    // uses for the JSON-RPC server.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr: SocketAddr = listener.local_addr()?;
    let metrics = Arc::new(decdn_node::metrics::Metrics::new());
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let metrics_for_server = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        let _ = decdn_node::metrics::serve(listener, metrics_for_server, stop_rx).await;
    });

    // Bump a couple of counters so the snapshot has non-zero fields —
    // proves the parser saw the real bytes the encoder wrote.
    metrics.dispatch_permit_acquired();
    metrics.cache_metrics().hits.inc();
    metrics.cache_metrics().bytes_returned.inc_by(1024);

    // Fetch via reqwest, parse, build a Snapshot — the same path the
    // CLI's `run` takes in JSON mode.
    let url = format!("http://{addr}/metrics");
    let body = reqwest::Client::new()
        .get(&url)
        .send()
        .await?
        .text()
        .await?;
    let parsed = decdn_cli::commands::node_top::parse_openmetrics(&body);
    let snap = decdn_cli::commands::node_top::Snapshot::from_metrics(&parsed);
    assert_eq!(
        snap.dispatch_in_flight, 1,
        "in_flight should reflect the dispatch_permit_acquired bump"
    );
    assert_eq!(
        snap.cache_hits, 1,
        "hits should reflect the cache_metrics().hits.inc() bump"
    );
    assert_eq!(
        snap.cache_bytes_returned, 1024,
        "bytes should reflect the inc_by(1024) bump"
    );

    // Drive the high-level `run` in JSON mode against the same
    // address. We don't capture stdout — this is a smoke test that
    // the full pipeline (resolver → reqwest → parser → renderer →
    // exit) runs without panicking against a real daemon endpoint.
    let args = decdn_common::cli::TopArgs {
        metrics_url: Some(format!("http://{addr}")),
        config: None,
        interval_ms: 1_000,
        json: true,
        timeout_ms: 5_000,
    };
    decdn_cli::commands::node_top::run(&args, None).await?;

    let _ = stop_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
    Ok(())
}
