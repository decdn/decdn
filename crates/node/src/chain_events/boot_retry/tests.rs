use super::*;
use alloy::transports::RpcError;
use anyhow::Context;
use std::cell::Cell;

fn resp(code: i64, message: &str) -> alloy::transports::TransportError {
    RpcError::ErrorResp(
        serde_json::from_value(serde_json::json!({ "code": code, "message": message })).unwrap(),
    )
}

/// The shape a boot read's error takes: a contract error under the
/// bootstrap's own `with_context` layers.
fn wrapped(e: alloy::contract::Error) -> anyhow::Error {
    Err::<(), _>(e)
        .context("blacklistedAddressCount")
        .context("enumerate and enforce the ContentBlacklist boot snapshot")
        .unwrap_err()
}

fn retries(metrics: &Metrics) -> u64 {
    let text = metrics.encode().unwrap();
    text.lines()
        .find_map(|l| l.strip_prefix("decdn_chain_boot_read_retries_total "))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("counter not exported:\n{text}"))
}

#[test]
fn the_issue_2159_provider_errors_are_transient() {
    for (code, message) in [
        (1, "no available upstreams to process the request"),
        (19, "Temporary internal error. Please retry"),
    ] {
        let err = wrapped(alloy::contract::Error::TransportError(resp(code, message)));
        assert!(!is_permanent_boot_error(&err), "code {code}");
    }
    // The same response reached as a raw `TransportError` (a `timed`
    // provider read, not a contract call).
    let raw = anyhow::Error::new(resp(19, "Temporary internal error"))
        .context("PaymentPool.usdc() self-check");
    assert!(!is_permanent_boot_error(&raw));
}

#[test]
fn untyped_errors_are_transient() {
    assert!(!is_permanent_boot_error(&anyhow::anyhow!(
        "blacklistedAddressCount timed out after 10s"
    )));
}

#[test]
fn deterministic_faults_are_permanent() {
    assert!(is_permanent_boot_error(&wrapped(
        alloy::contract::Error::UnknownFunction("usdc".to_string())
    )));
    assert!(is_permanent_boot_error(&wrapped(
        alloy::contract::Error::TransportError(resp(-32601, "method not found"))
    )));
    let fault = anyhow::Error::new(BootFault("ABI or decoder fault".to_string()))
        .context("CapacityBond registry bootstrap");
    assert!(is_permanent_boot_error(&fault));
}

#[tokio::test(start_paused = true)]
async fn a_transient_error_is_retried_until_success() {
    let metrics = Arc::new(Metrics::new());
    let boot = BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics));
    let calls = Cell::new(0u32);
    let start = Instant::now();

    let out = boot
        .run("test read", || {
            let n = calls.get();
            calls.set(n + 1);
            async move {
                if n == 0 {
                    Err(wrapped(alloy::contract::Error::TransportError(resp(
                        1,
                        "no available upstreams",
                    ))))
                } else {
                    Ok(7)
                }
            }
        })
        .await
        .unwrap();

    assert_eq!(out, 7);
    assert_eq!(calls.get(), 2);
    assert_eq!(retries(&metrics), 1);
    assert_eq!(start.elapsed(), WATCHER_INITIAL_BACKOFF);
}

#[tokio::test(start_paused = true)]
async fn a_permanent_error_is_not_retried() {
    let metrics = Arc::new(Metrics::new());
    let boot = BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics));
    let calls = Cell::new(0u32);

    let err = boot
        .run("test read", || {
            calls.set(calls.get() + 1);
            async { Err::<(), _>(wrapped(alloy::contract::Error::ContractNotDeployed)) }
        })
        .await
        .unwrap_err();

    assert_eq!(calls.get(), 1);
    assert_eq!(retries(&metrics), 0);
    let msg = format!("{err:#}");
    assert!(!msg.contains("gave up"), "{msg}");
    assert!(
        msg.contains("test read: deterministic failure, not retried"),
        "{msg}"
    );
}

#[tokio::test(start_paused = true)]
async fn the_budget_bounds_a_dead_endpoint() {
    let metrics = Arc::new(Metrics::new());
    let budget = Duration::from_secs(10);
    let boot = BootRetry::new(budget, Arc::clone(&metrics));
    let calls = Cell::new(0u32);
    let start = Instant::now();

    let err = boot
        .run("test read", || {
            calls.set(calls.get() + 1);
            async { Err::<(), _>(anyhow::anyhow!("connection refused")) }
        })
        .await
        .unwrap_err();

    // Backoffs of 1 s, 2 s and 4 s fit in 10 s; the next 8 s would not.
    assert_eq!(calls.get(), 4);
    assert_eq!(retries(&metrics), 3);
    assert_eq!(start.elapsed(), Duration::from_secs(7));
    let msg = format!("{err:#}");
    assert!(msg.contains("test read: gave up after 4 attempts"), "{msg}");
    assert!(msg.contains("connection refused"), "{msg}");
}

/// One deadline covers every `run`: a later read gets only what the earlier
/// ones left, so four bootstraps cannot stretch boot to four budgets.
#[tokio::test(start_paused = true)]
async fn the_budget_is_shared_across_reads() {
    let metrics = Arc::new(Metrics::new());
    let boot = BootRetry::new(Duration::from_secs(10), Arc::clone(&metrics));
    let start = Instant::now();

    // Read A fails three times (sleeps of 1 s, 2 s and 4 s), then succeeds.
    let calls_a = Cell::new(0u32);
    boot.run("read a", || {
        let n = calls_a.get();
        calls_a.set(n + 1);
        async move {
            if n < 3 {
                Err(anyhow::anyhow!("connection refused"))
            } else {
                Ok(())
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(start.elapsed(), Duration::from_secs(7));

    // Read B starts with 3 s left: sleeps of 1 s and 2 s end exactly on the
    // deadline, which is still allowed; the next 4 s would not fit.
    let calls_b = Cell::new(0u32);
    let err = boot
        .run("read b", || {
            calls_b.set(calls_b.get() + 1);
            async { Err::<(), _>(anyhow::anyhow!("connection refused")) }
        })
        .await
        .unwrap_err();

    assert_eq!(calls_b.get(), 3);
    assert_eq!(retries(&metrics), 5);
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    let msg = format!("{err:#}");
    assert!(msg.contains("read b: gave up after 3 attempts"), "{msg}");
    assert!(msg.contains("budget of 10s is spent"), "{msg}");
}

/// A capped sub-budget ends at its cap, and never past the parent deadline.
#[tokio::test(start_paused = true)]
async fn a_capped_budget_ends_at_the_earlier_deadline() {
    let metrics = Arc::new(Metrics::new());
    let boot = BootRetry::new(Duration::from_secs(10), Arc::clone(&metrics));
    let start = Instant::now();
    let dead = || async { Err::<(), _>(anyhow::anyhow!("connection refused")) };

    // A 3 s cap: sleeps of 1 s and 2 s fit, the next 4 s does not.
    boot.capped(Duration::from_secs(3))
        .run("capped", dead)
        .await
        .unwrap_err();
    assert_eq!(start.elapsed(), Duration::from_secs(3));

    // A cap past the parent deadline is clipped to it: 7 s are left, so
    // sleeps of 1 s, 2 s and 4 s fit and the next 8 s does not.
    boot.capped(Duration::from_mins(1))
        .run("clipped", dead)
        .await
        .unwrap_err();
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    assert_eq!(retries(&metrics), 5);
}

/// The production budget and the 60 s backoff cap together: a dead endpoint
/// sleeps 1+2+4+8+16+32 s, then 60 s eight times, and gives up at 543 s.
#[tokio::test(start_paused = true)]
async fn a_dead_endpoint_gives_up_within_the_default_budget() {
    let metrics = Arc::new(Metrics::new());
    let boot = BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics));
    let calls = Cell::new(0u32);
    let start = Instant::now();

    boot.run("test read", || {
        calls.set(calls.get() + 1);
        async { Err::<(), _>(anyhow::anyhow!("connection refused")) }
    })
    .await
    .unwrap_err();

    assert_eq!(calls.get(), 15);
    assert_eq!(start.elapsed(), Duration::from_secs(543));
}

/// No contract at the configured address: a real empty `eth_call` return
/// decodes as `ZeroData` under the `timed` wrapper the boot reads use, and is
/// permanent.
#[tokio::test]
async fn an_empty_call_return_is_permanent() {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use decdn_incentive::capacity_bond::CapacityBond;

    let asserter = Asserter::new();
    asserter.push_success(&alloy::primitives::Bytes::new());
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    let bond = CapacityBond::new(alloy::primitives::Address::repeat_byte(0x11), provider);

    let err = super::super::timed(None, "pausedTotal", bond.pausedTotal().call())
        .await
        .context("pausedTotal")
        .unwrap_err();
    assert!(
        err.chain().any(|c| matches!(
            c.downcast_ref::<alloy::contract::Error>(),
            Some(alloy::contract::Error::ZeroData(..))
        )),
        "{err:#}"
    );
    assert!(is_permanent_boot_error(&err));
}

#[tokio::test(start_paused = true)]
async fn a_zero_budget_is_a_single_attempt() {
    let metrics = Arc::new(Metrics::new());
    let boot = BootRetry::single_attempt(Arc::clone(&metrics));
    let calls = Cell::new(0u32);

    boot.run("test read", || {
        calls.set(calls.get() + 1);
        async { Err::<(), _>(anyhow::anyhow!("connection refused")) }
    })
    .await
    .unwrap_err();

    assert_eq!(calls.get(), 1);
    assert_eq!(retries(&metrics), 0);
}
