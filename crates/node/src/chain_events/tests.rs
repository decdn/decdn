use super::*;

/// A call that never resolves must fail at [`DEFAULT_RPC_CALL_TIMEOUT`] rather
/// than wedge the tick forever, and the error must name the call so the
/// caller's context composes onto something diagnostic. `start_paused` lets
/// tokio auto-advance to the deadline, so this costs no wall-clock time.
///
/// This is the mechanism every self-bounding sink read relies on (the
/// `nodeIdOf` / `scope_check` pattern), so it is pinned here rather than at
/// each call site.
#[tokio::test(start_paused = true)]
async fn timed_bounds_a_hanging_call_at_the_default() {
    let hang = std::future::pending::<std::result::Result<u64, std::io::Error>>();
    let err = timed(None, "nodeIdOf", hang)
        .await
        .err()
        .map(|e| format!("{e:#}"));
    assert!(
        err.as_ref()
            .is_some_and(|e| e.contains("nodeIdOf timed out after")),
        "a hanging call must time out and name itself: {err:?}"
    );
}

/// An explicit timeout overrides the default.
#[tokio::test(start_paused = true)]
async fn timed_honours_an_explicit_timeout() {
    let hang = std::future::pending::<std::result::Result<u64, std::io::Error>>();
    let started = tokio::time::Instant::now();
    let _ = timed(Some(Duration::from_millis(50)), "get_logs", hang).await;
    assert!(
        started.elapsed() < DEFAULT_RPC_CALL_TIMEOUT,
        "explicit timeout must win over the 10s default"
    );
}

/// A call that succeeds inside the deadline passes its value through.
#[tokio::test(start_paused = true)]
async fn timed_passes_a_prompt_success_through() {
    let ok = async { Ok::<u64, std::io::Error>(7) };
    assert_eq!(timed(None, "get_block_number", ok).await.ok(), Some(7));
}

/// The hanging provider really hangs, and `timed` really bounds it through a
/// *real* alloy provider rather than a bare `pending()` future.
///
/// This pins the test-support seam itself: every wiring test in the watcher
/// modules is only as good as this. If a future alloy bump makes
/// `HangingTransport` resolve or reject, this fails here — one clear failure
/// — instead of silently turning five wiring tests into tautologies.
#[tokio::test(start_paused = true)]
async fn hanging_provider_stalls_until_timed_fires() {
    use alloy::providers::Provider;

    let provider = super::test_support::hanging_provider();
    let err = timed(None, "get_block_number", provider.get_block_number())
        .await
        .err()
        .map(|e| format!("{e:#}"));
    assert!(
        err.as_ref()
            .is_some_and(|e| e.contains("get_block_number timed out after")),
        "a hanging provider must fail at the deadline, not resolve or error early: {err:?}"
    );
}
