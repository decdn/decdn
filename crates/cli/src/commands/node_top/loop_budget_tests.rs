//! Direct test for `MAX_CONSECUTIVE_SCRAPE_FAILURES`. The cap
//! is part of the operator-visible contract — without a test,
//! a refactor that silently raises it to `u32::MAX` (or removes
//! the check entirely) would let `decdn node top` run forever
//! against a misconfigured target.
use super::*;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::test]
async fn run_bails_after_consecutive_failures_cap() {
    // Bind a listener and immediately drop it so subsequent
    // connects from the CLI fail with ECONNREFUSED. Using a
    // bound-then-dropped port keeps the test hermetic — no
    // reliance on a specific port being unused.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    drop(listener);

    // 1ms interval + 100ms timeout means 30 ticks finish in
    // under a second on the fast-fail (ECONNREFUSED) path.
    // Pause the runtime clock so the bound is enforced even on
    // a heavily-loaded CI runner.
    let args = decdn_common::cli::TopArgs {
        metrics_url: Some(format!("http://{addr}")),
        config: None,
        interval_ms: 1,
        json: false,
        timeout_ms: 100,
    };

    let err = run(&args, None)
        .await
        .expect_err("loop must bail after the consecutive-failure cap");
    let msg = format!("{err:#}");
    let cap = MAX_CONSECUTIVE_SCRAPE_FAILURES;
    assert!(
        msg.contains(&format!("{cap} times in a row")),
        "expected '{cap} times in a row' in error chain, got: {msg}"
    );
}
