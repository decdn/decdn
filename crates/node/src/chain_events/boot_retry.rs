//! In-process retry for the boot-time chain reads (#2159).
//!
//! `build_chain_and_handlers` runs four fail-closed chain bootstraps in
//! sequence: the `CapacityBond` registry snapshot, the slash-detection
//! enumeration, the `PaymentPool` `usdc()` self-check and the `ContentBlacklist`
//! deny-set enumeration with its enforcement pass. Each one must succeed before
//! the node serves, and a boot is only as likely to succeed as the product of
//! its reads. A single transient provider error therefore must not end the
//! process: each bootstrap retries its read phase through `BootRetry::run` and
//! fails only on a deterministic fault or once the shared boot deadline passes.
//!
//! The best-effort fee-share reads (`fee_shares::seed_from_chain`) run between
//! the slash and `usdc()` reads. They retry the same way but fall back to the
//! floor share instead of failing boot, and they run on a short sub-budget
//! (`BootRetry::capped`) so a flapping provider cannot spend the deadline the
//! later fail-closed reads need.
//!
//! Startup stays fail-closed. A retry delays readiness; it never opens the ALPN
//! router before the deny-set is loaded and enforced.
//!
//! Three boot-time facts shape the loop:
//!
//! - No signal handler exists yet, so SIGTERM or SIGINT ends a retrying boot at
//!   once. The only durable writes a boot attempt makes are sticky, idempotent
//!   cache evictions; everything else it sets is in memory. An abrupt exit
//!   therefore needs no teardown.
//! - [`SharedHead`](super::shared_head::SharedHead) caches a failed head read
//!   for half the poll interval. Retries inside that window replay the cached
//!   error — the first two at the default 7 s poll interval — so the cost is at
//!   most one cache TTL of the budget.
//! - The metrics listener binds after these reads, so
//!   `decdn_chain_boot_read_retries_total` is scrapeable only once boot
//!   completes. The per-attempt `WARN` is the live signal during boot.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use decdn_client::provider::{is_permanent_contract_error, is_permanent_rpc_error};
use decdn_common::redact::sanitize_err_chain;
use tokio::time::Instant;
use tracing::warn;

use super::{WATCHER_INITIAL_BACKOFF, WATCHER_MAX_BACKOFF};
use crate::metrics::Metrics;

/// The boot deadline: a wall-clock window that starts at [`BootRetry::new`] and
/// covers every boot chain read, including its successful attempts and the
/// other bring-up work between them. A provider that stays down for the rest of
/// this window is treated as dead: the daemon exits, and the supervisor
/// (systemd, Kubernetes) surfaces it.
pub(crate) const BOOT_CHAIN_RETRY_BUDGET: Duration = Duration::from_mins(10);

/// A deterministic fault the node detects itself, such as a decoder that
/// returns mismatched array lengths or a cache that cannot evict. A boot read
/// never retries it.
#[derive(Debug)]
pub(crate) struct BootFault(pub(crate) String);

impl std::fmt::Display for BootFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BootFault {}

/// The shared deadline for the boot-time chain reads. Build one per boot and
/// pass it by reference to every bootstrap. See the [module docs](self).
#[derive(Debug)]
pub struct BootRetry {
    started: Instant,
    deadline: Instant,
    budget: Duration,
    metrics: Arc<Metrics>,
}

impl BootRetry {
    /// Start the clock: the deadline is `budget` from now. A retry sleep never
    /// ends past it, so a budget under the first backoff (1 s) makes every
    /// `run` a single attempt.
    #[must_use]
    pub fn new(budget: Duration, metrics: Arc<Metrics>) -> Self {
        let started = Instant::now();
        // A budget too large to add is effectively unbounded; a far-future
        // deadline keeps that without the overflow panic.
        let deadline = started
            .checked_add(budget)
            .unwrap_or_else(|| started + Duration::from_secs(u64::from(u32::MAX)));
        Self {
            started,
            deadline,
            budget,
            metrics,
        }
    }

    /// A deadline that allows no retry: every `run` makes one attempt.
    #[must_use]
    pub fn single_attempt(metrics: Arc<Metrics>) -> Self {
        Self::new(Duration::ZERO, metrics)
    }

    /// A sub-budget for a best-effort read: the earlier of this deadline and
    /// `cap` from now. A best-effort read that retries on it cannot spend the
    /// time the later fail-closed reads need.
    #[must_use]
    pub(crate) fn capped(&self, cap: Duration) -> Self {
        let now = Instant::now();
        let deadline = now
            .checked_add(cap)
            .map_or(self.deadline, |d| d.min(self.deadline));
        Self {
            started: self.started,
            deadline,
            budget: deadline.saturating_duration_since(now),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Run `attempt` until it succeeds, fails with a permanent error
    /// ([`is_permanent_boot_error`]), or the next backoff would end past the
    /// boot deadline. Each retry logs a `WARN`, bumps
    /// `decdn_chain_boot_read_retries_total`, and sleeps on the watcher backoff
    /// schedule (1 s doubling to 60 s). The deadline bounds the sleeps, not the
    /// attempts: the last attempt can run past it by its own duration.
    ///
    /// # Errors
    ///
    /// Returns a permanent error with a context that says it was not retried,
    /// or the last transient error with a context that names the attempt count
    /// and the spent budget.
    pub(crate) async fn run<T, F, Fut>(&self, what: &'static str, mut attempt: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let start = Instant::now();
        let mut backoff = WATCHER_INITIAL_BACKOFF;
        let mut attempts: u32 = 0;
        loop {
            attempts = attempts.saturating_add(1);
            let err = match attempt().await {
                Ok(v) => return Ok(v),
                Err(err) => err,
            };
            if is_permanent_boot_error(&err) {
                return Err(err.context(format!("{what}: deterministic failure, not retried")));
            }
            if Instant::now() + backoff > self.deadline {
                let elapsed = start.elapsed();
                let boot_elapsed = self.started.elapsed();
                let budget = self.budget;
                return Err(err.context(format!(
                    "{what}: gave up after {attempts} attempts in {elapsed:?}; the shared \
                     boot chain-read budget of {budget:?} is spent ({boot_elapsed:?} since \
                     the boot reads began)"
                )));
            }
            warn!(
                read = what,
                attempt = attempts,
                backoff_secs = backoff.as_secs(),
                error = %sanitize_err_chain(&err),
                "boot chain read failed; retrying after backoff"
            );
            self.metrics.chain_boot_read_retried();
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(WATCHER_MAX_BACKOFF);
        }
    }
}

/// Whether a boot-time chain read failed deterministically, so a retry cannot
/// succeed.
///
/// Walks the error chain for the first typed cause:
///
/// - a [`BootFault`] is permanent;
/// - an `alloy::contract::Error` or a `TransportError` is classified by the
///   classifier the client's registry discovery shares
///   ([`is_permanent_contract_error`], [`is_permanent_rpc_error`]). The head
///   read keeps its typed cause through the `SharedHead` cache, so this covers
///   the head read that opens the registry, slash and blacklist bootstraps.
///
/// An error with no typed cause is transient. That covers a
/// `chain_events::timed` timeout and the blacklist enumeration's count checks,
/// which an inconsistent RPC view of the pinned block can trip. A response
/// that is not JSON-RPC at all (`DeserError`) is transient as well: a proxy
/// under load can return one, and the budget bounds a URL that always does.
fn is_permanent_boot_error(err: &anyhow::Error) -> bool {
    err.chain()
        .find_map(|cause| {
            if cause.is::<BootFault>() {
                Some(true)
            } else if let Some(e) = cause.downcast_ref::<alloy::contract::Error>() {
                Some(is_permanent_contract_error(e))
            } else {
                cause
                    .downcast_ref::<alloy::transports::TransportError>()
                    .map(is_permanent_rpc_error)
            }
        })
        .unwrap_or(false)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use alloy::transports::RpcError;
    use anyhow::Context;
    use std::cell::Cell;

    fn resp(code: i64, message: &str) -> alloy::transports::TransportError {
        RpcError::ErrorResp(
            serde_json::from_value(serde_json::json!({ "code": code, "message": message }))
                .unwrap(),
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
}
