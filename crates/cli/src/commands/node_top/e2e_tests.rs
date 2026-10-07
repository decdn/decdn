//! End-to-end test against the daemon's real metrics server.
//! In-module rather than a `tests/` integration test because
//! the assertions exercise `pub(crate)` items (parser +
//! `Snapshot::from_metrics`); going via `tests/` would force a
//! `pub` API surface that no non-test caller needs.

use super::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::test]
async fn e2e_pipeline_reads_real_metrics_server() -> anyhow::Result<()> {
    // Bind on an ephemeral loopback port; same pattern the
    // sibling tests in `tests/admin_rpc.rs` use.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr: SocketAddr = listener.local_addr()?;
    let metrics = Arc::new(decdn_node::metrics::Metrics::new());
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let metrics_for_server = Arc::clone(&metrics);
    // Capture the server's Result so a panic or encoder error
    // surfaces as a real test failure with the actual cause,
    // rather than a confusing "got 0, expected 1" downstream.
    let server = tokio::spawn(async move {
        decdn_node::metrics::serve(listener, metrics_for_server, stop_rx).await
    });

    // Bump a couple of counters so the snapshot has non-zero
    // fields — proves the parser saw the real bytes the encoder
    // wrote, not just an empty/200 response.
    metrics.dispatch_permit_acquired();
    metrics.cache_metrics().hits.inc();
    metrics.cache_metrics().bytes_returned.inc_by(1024);

    // Fetch via reqwest, parse, build a Snapshot — the same
    // path the CLI's `run` takes in JSON mode.
    let url = format!("http://{addr}/metrics");
    let body = reqwest::Client::new()
        .get(&url)
        .send()
        .await?
        .text()
        .await?;
    let parsed = parse_openmetrics(&body);
    let snap = Snapshot::from_metrics(&parsed);
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
    // address. Smoke-tests resolver → reqwest → parser →
    // renderer → exit against a real daemon endpoint.
    let args = decdn_common::cli::TopArgs {
        metrics_url: Some(format!("http://{addr}")),
        config: None,
        interval_ms: 1_000,
        json: true,
        timeout_ms: 5_000,
    };
    run(&args, None).await?;

    // Tell the server to stop, then await the JoinHandle so a
    // panic in the spawned task surfaces here. The outer
    // `timeout` Elapsed is fine to drop — that's a cleanup
    // race, not a logic bug — but the inner JoinError is not.
    let _ = stop_tx.send(());
    let join = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .context("metrics server task did not exit within 2s")?;
    join.context("metrics server task panicked")?
        .context("metrics server returned an error")?;
    Ok(())
}

#[tokio::test]
async fn fetch_metrics_surfaces_non_2xx_as_error() -> anyhow::Result<()> {
    // Spawn a tiny listener that replies 503 to every request.
    // Locks in the bail at fetch_metrics' status check so a
    // future refactor that stops checking status fails here
    // rather than silently feeding garbage to the parser.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr: SocketAddr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        // One-shot: accept, write a 503, drop. Any subsequent
        // connections close cleanly when the listener drops.
        if let Ok((mut sock, _)) = listener.accept().await {
            use tokio::io::AsyncWriteExt;
            let _ = sock
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\n\
                      Content-Length: 0\r\n\r\n",
                )
                .await;
            let _ = sock.shutdown().await;
        }
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let url = format!("http://{addr}/metrics");
    let err = fetch_metrics(&client, &url)
        .await
        .expect_err("503 must surface as Err");
    let msg = format!("{err:#}");
    // Assert the status code shows up — `StatusCode`'s Display
    // emits `503 Service Unavailable`, so checking for "503" is
    // sufficient and tighter than a `||` over both substrings
    // (which always co-occur today).
    assert!(
        msg.contains("503"),
        "expected 503 in error chain, got: {msg}"
    );

    // Wait for the listener task and propagate JoinError so a
    // panic in the spawned write loop surfaces with the real
    // cause, rather than the assertion above passing on a
    // torn-down server. Same shape as the e2e test above.
    let join = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .context("listener task did not exit within 1s")?;
    join.context("listener task panicked")?;
    Ok(())
}
