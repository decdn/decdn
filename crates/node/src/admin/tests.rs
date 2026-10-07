use super::*;
use decdn_cache::{Hash, Origin};

/// The RPC message carries the store fault's whole cause chain, so an
/// operator running `decdn node evict` sees why the store failed.
#[test]
fn cache_error_rpc_message_carries_the_cause_chain() {
    let err = CacheError::Store(anyhow::anyhow!("disk-root-7f3a").context("tag drop"));
    let rpc = cache_error_to_rpc(&err);
    assert_eq!(rpc.message(), "store error: tag drop: disk-root-7f3a");
}

/// Build a throwaway tempdir-backed cache for tests. The health
/// method doesn't touch it, but `AdminState::new` requires one —
/// wrapping the engine over a `tempfile::TempDir` keeps each test
/// self-contained, and the returned `TempDir` must outlive the engine
/// (callers bind it with `_tmp` so RAII handles cleanup at end of test).
async fn test_cache() -> (CacheEngine, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = CacheEngine::open(tmp.path(), Vec::new(), 1)
        .await
        .expect("cache open");
    (cache, tmp)
}

async fn state_with() -> (AdminState, tempfile::TempDir) {
    let (cache, tmp) = test_cache().await;
    let state = AdminState::new(
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        Arc::new(DrainTrigger::new()),
        Arc::new(crate::metrics::Metrics::new()),
    );
    (state, tmp)
}

#[tokio::test]
async fn slashes_unavailable_without_handle() {
    let (state, _tmp) = state_with().await;
    let rpc = AdminRpcImpl::new(state);
    let err = rpc.slashes().await.expect_err("no slash handle wired");
    assert_eq!(err.code(), SLASH_DETECTION_UNAVAILABLE_CODE);
}

#[tokio::test]
async fn slashes_reports_detected_records_newest_first() {
    use alloy::primitives::{B256, U256};
    let store: crate::slash_watcher::SlashStore = Arc::new(std::sync::RwLock::new(vec![
        crate::slash_watcher::DetectedSlash {
            slash_id: U256::from(1u64),
            offense_type: 0,
            amount: U256::from(100u64),
            evidence_hash: B256::repeat_byte(0x11),
            block_number: Some(10),
            appeal_window_close: Some(999),
        },
        crate::slash_watcher::DetectedSlash {
            slash_id: U256::from(2u64),
            offense_type: 1,
            amount: U256::from(200u64),
            evidence_hash: B256::repeat_byte(0x22),
            block_number: Some(20),
            appeal_window_close: None,
        },
    ]));
    let (state, _tmp) = state_with().await;
    let rpc = AdminRpcImpl::new(state.with_slash_detection(SlashStatusHandles { store }));
    let resp = rpc.slashes().await.expect("slashes ok");
    assert_eq!(resp.slashes.len(), 2);
    // Newest-first: the watcher appends in detection order, the method reverses.
    let first = resp.slashes.first().expect("first slash");
    assert_eq!(first.slash_id, "2");
    assert_eq!(first.offense_type, 1);
    assert_eq!(first.amount, "200");
    assert_eq!(first.appeal_window_close, None);
    let second = resp.slashes.get(1).expect("second slash");
    assert_eq!(second.slash_id, "1");
    assert_eq!(second.appeal_window_close, Some(999));
}

#[tokio::test]
async fn health_returns_hex_node_id_and_nondecreasing_uptime() {
    let id = [0xCDu8; 32];
    let started = Instant::now();
    let (cache, _tmp) = test_cache().await;
    let state = AdminState::new(
        id,
        started,
        cache,
        None,
        Arc::new(DrainTrigger::new()),
        Arc::new(crate::metrics::Metrics::new()),
    );
    let rpc = AdminRpcImpl::new(state);

    let first = rpc.health().await.expect("health ok");
    assert_eq!(first.node_id, "cd".repeat(32));

    // Uptime is monotonic non-decreasing across calls — `Instant`
    // is monotonic, so a second call after at least one elapsed-tick
    // worth of work must report a value >= the first.
    let second = rpc.health().await.expect("health ok");
    assert!(
        second.uptime_s >= first.uptime_s,
        "uptime regressed: {} -> {}",
        first.uptime_s,
        second.uptime_s,
    );
}

/// Evict round-trip: `admin_v1_evict` of a hex hash that's been pulled
/// into the cache returns `was_present: true` and subsequent gets fail
/// with `NotFound`. Without this, a regression that no-op'd
/// `admin_v1_evict` would leak through unit tests.
#[tokio::test]
async fn admin_evict_blocks_subsequent_serve() -> anyhow::Result<()> {
    use bytes::Bytes;
    use std::future::Future;
    use std::pin::Pin;

    // Inline stub origin so we don't have to depend on a test-only
    // `decdn-cache` export. Single-blob, hash matches payload.
    #[derive(Debug)]
    struct StubOrigin {
        data: Bytes,
        hash: Hash,
    }
    impl decdn_cache::Origin for StubOrigin {
        fn kind(&self) -> decdn_cache::OriginKind {
            decdn_cache::OriginKind::Http
        }

        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>>
                    + Send
                    + '_,
            >,
        > {
            let result = if hash == self.hash {
                Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
            } else {
                Ok(decdn_cache::OriginFetch::NotFound)
            };
            Box::pin(async move { result })
        }
    }

    let payload = b"admin evict";
    let hash = Hash::new(payload);
    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(StubOrigin {
        data: Bytes::from(payload.to_vec()),
        hash,
    }) as Arc<dyn decdn_cache::Origin>;
    let cache = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 1).await?;

    // Prime the cache with the blob so the evict has something to remove.
    let _ = cache.get(hash).await?;

    let state = AdminState::new(
        [0u8; 32],
        Instant::now(),
        cache.clone(),
        None,
        Arc::new(DrainTrigger::new()),
        Arc::new(crate::metrics::Metrics::new()),
    );
    let rpc = AdminRpcImpl::new(state);

    let resp = rpc
        .evict(EvictRequest {
            hash: alloy::primitives::hex::encode(hash.as_bytes()),
            dry_run: false,
        })
        .await
        .expect("evict ok");
    assert!(resp.was_present, "expected was_present=true");
    assert!(!resp.dry_run, "real evict must not set dry_run");

    match cache.get(hash).await {
        Err(CacheError::NotFound { .. }) => Ok(()),
        other => Err(anyhow::anyhow!(
            "expected NotFound after admin evict, got {other:?}"
        )),
    }
}

#[tokio::test]
async fn admin_evict_rejects_bad_hex() {
    let (state, _tmp) = state_with().await;
    let rpc = AdminRpcImpl::new(state);
    let err = rpc
        .evict(EvictRequest {
            hash: "not-hex".into(),
            dry_run: false,
        })
        .await
        .expect_err("expected invalid-params error");
    // INVALID_PARAMS_CODE; double-checked here so a typo'd code constant
    // still surfaces as a test failure.
    assert_eq!(err.code(), -32_602);
}

#[tokio::test]
async fn admin_evict_accepts_uppercase_0x_prefix() {
    // `0X` and uppercase hex must both be tolerated; this guards
    // against the case-sensitive `strip_prefix("0x")` regression
    // flagged in PR review.
    let (cache, _tmp) = test_cache().await;
    let state = AdminState::new(
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        Arc::new(DrainTrigger::new()),
        Arc::new(crate::metrics::Metrics::new()),
    );
    let rpc = AdminRpcImpl::new(state);
    let hash = Hash::new(b"prefix-test");
    let upper = format!(
        "0X{}",
        alloy::primitives::hex::encode(hash.as_bytes()).to_uppercase()
    );
    let resp = rpc
        .evict(EvictRequest {
            hash: upper,
            dry_run: false,
        })
        .await
        .expect("0X-prefixed uppercase hex should parse");
    // was_present=false because the test cache has no origin and we
    // never `get`-ed the hash; the parse alone must succeed.
    assert!(!resp.was_present);
}

/// `admin_v1_evict { dry_run: true }` returns the pre-evict
/// snapshot but does *not* mutate cache state — a follow-up `has`
/// must still report the blob present, and a follow-up `get` must
/// still serve. Without this assertion a regression that ignored
/// the flag and ran the real `evict()` would silently slip through
/// (the response shape is the same; the side-effect is what
/// matters).
#[tokio::test]
async fn admin_evict_dry_run_does_not_mutate() -> anyhow::Result<()> {
    use bytes::Bytes;
    use std::future::Future;
    use std::pin::Pin;

    #[derive(Debug)]
    struct StubOrigin {
        data: Bytes,
        hash: Hash,
    }
    impl decdn_cache::Origin for StubOrigin {
        fn kind(&self) -> decdn_cache::OriginKind {
            decdn_cache::OriginKind::Http
        }

        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>>
                    + Send
                    + '_,
            >,
        > {
            let result = if hash == self.hash {
                Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
            } else {
                Ok(decdn_cache::OriginFetch::NotFound)
            };
            Box::pin(async move { result })
        }
    }

    let payload = b"dry-run preview";
    let hash = Hash::new(payload);
    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(StubOrigin {
        data: Bytes::from(payload.to_vec()),
        hash,
    }) as Arc<dyn decdn_cache::Origin>;
    let cache = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 1).await?;
    let _ = cache.get(hash).await?;

    let state = AdminState::new(
        [0u8; 32],
        Instant::now(),
        cache.clone(),
        None,
        Arc::new(DrainTrigger::new()),
        Arc::new(crate::metrics::Metrics::new()),
    );
    let rpc = AdminRpcImpl::new(state);

    let resp = rpc
        .evict(EvictRequest {
            hash: alloy::primitives::hex::encode(hash.as_bytes()),
            dry_run: true,
        })
        .await
        .expect("dry-run evict ok");

    // Wire shape — every dry-run-only field is meaningful.
    assert!(resp.dry_run, "expected dry_run=true on response");
    assert!(resp.was_present, "blob primed via get(); should be served");
    assert_eq!(
        resp.preview.size_bytes,
        Some(payload.len() as u64),
        "expected size_bytes={}, got {:?}",
        payload.len(),
        resp.preview.size_bytes,
    );
    assert!(
        resp.preview.last_accessed_us_ago.is_some(),
        "expected Some(last_accessed_us_ago) after get()"
    );
    assert!(!resp.preview.pinned);
    assert!(
        !resp.preview.already_evicted,
        "dry-run must not flip evicted flag"
    );
    // Origin egress-cost cue (#439, #284). Engine here is
    // configured with a single-origin chain, so the preview must
    // carry exactly one entry — the StubOrigin reports `Http`.
    assert_eq!(
        resp.preview.origin_kinds,
        vec![decdn_cache::OriginKind::Http],
        "expected [Http], got {:?}",
        resp.preview.origin_kinds,
    );

    // Cache state untouched: the blob is still served, the
    // evicted-log entry was not created, and no fsync hit disk.
    assert!(
        !cache.is_evicted(hash),
        "dry-run must not commit to evicted set"
    );
    assert!(cache.has(hash).await?, "dry-run must not stop serve");
    assert!(
        !tmp.path().join("evicted.log").exists(),
        "dry-run must not create evicted.log"
    );
    Ok(())
}

/// A real evict followed by a dry-run on the same hash must report
/// `already_evicted: true` and `was_present: false` — the operator
/// is using dry-run to confirm an idempotent re-run is in fact a
/// no-op. The size field still reports the on-disk bytes since
/// the iroh-blobs store hasn't been GC'd yet (#518).
#[tokio::test]
async fn admin_evict_dry_run_after_real_evict_reports_already_evicted() -> anyhow::Result<()> {
    use bytes::Bytes;
    use std::future::Future;
    use std::pin::Pin;

    #[derive(Debug)]
    struct StubOrigin {
        data: Bytes,
        hash: Hash,
    }
    impl decdn_cache::Origin for StubOrigin {
        fn kind(&self) -> decdn_cache::OriginKind {
            decdn_cache::OriginKind::Http
        }

        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>>
                    + Send
                    + '_,
            >,
        > {
            let result = if hash == self.hash {
                Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
            } else {
                Ok(decdn_cache::OriginFetch::NotFound)
            };
            Box::pin(async move { result })
        }
    }

    let payload = b"already-evicted preview";
    let hash = Hash::new(payload);
    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(StubOrigin {
        data: Bytes::from(payload.to_vec()),
        hash,
    }) as Arc<dyn decdn_cache::Origin>;
    let cache = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 1).await?;
    let _ = cache.get(hash).await?;
    cache.evict(hash).await?;

    let state = AdminState::new(
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        Arc::new(DrainTrigger::new()),
        Arc::new(crate::metrics::Metrics::new()),
    );
    let rpc = AdminRpcImpl::new(state);

    let resp = rpc
        .evict(EvictRequest {
            hash: alloy::primitives::hex::encode(hash.as_bytes()),
            dry_run: true,
        })
        .await
        .expect("dry-run evict ok");

    assert!(resp.dry_run);
    assert!(
        !resp.was_present,
        "post-evict has() should report absent → was_present=false"
    );
    assert!(
        resp.preview.already_evicted,
        "expected already_evicted=true on a re-run"
    );
    // On-disk size still reported — operators want to see the
    // disk-reclaim potential even though `served=false`.
    assert_eq!(resp.preview.size_bytes, Some(payload.len() as u64));
    Ok(())
}

/// `admin_v1_reload` on a node with no `ReloadHook` (started without
/// `--config`) must surface `CONFIG_PATH_UNSET_CODE` rather than a
/// generic failure, so an operator script can tell "no path on disk
/// to re-read" from "reload tried and failed".
#[tokio::test]
async fn admin_reload_without_hook_returns_config_path_unset() {
    let (state, _tmp) = state_with().await;
    let rpc = AdminRpcImpl::new(state);
    let err = rpc.reload().await.expect_err("expected error");
    assert_eq!(err.code(), -32_003);
}

/// Happy path: a hook pointing at a valid config file applies the
/// reload via the same `RuntimeReloadState::reload` SIGHUP uses, and
/// the response carries the post-reload `log_level`. Asserts the
/// wire-format field so a regression that dropped it (or stringified the
/// level wrong) fails the unit test.
#[tokio::test]
async fn admin_reload_applies_and_returns_post_reload_values() {
    use crate::runtime::RuntimeReloadState;
    use decdn_common::cli::common::LogLevel;

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("node.toml");
    std::fs::write(&path, "[observability]\nlog_level = \"debug\"\n").expect("write config");

    let setter: crate::runtime::LogLevelSetter =
        Box::new(|_| Ok(crate::runtime::LogLevelApply::Installed));
    let reload_state = Arc::new(RuntimeReloadState::for_test_with_setter(
        LogLevel::Info,
        setter,
    ));
    let hook = ReloadHook {
        reload_state: Arc::clone(&reload_state),
        config_path: path.clone(),
    };
    let (cache, _tmp) = test_cache().await;
    let state = AdminState::new(
        [0u8; 32],
        Instant::now(),
        cache,
        Some(hook),
        Arc::new(DrainTrigger::new()),
        Arc::new(crate::metrics::Metrics::new()),
    );
    let rpc = AdminRpcImpl::new(state);

    let resp = rpc.reload().await.expect("reload ok");
    assert_eq!(resp.log_level, "debug");
}

/// `DrainTrigger::fire` followed by `wait()` resolves. The Notify
/// stores a permit when no waiter is present, so the `notified()`
/// future claims it immediately — no race window or ordering
/// requirement between `fire` and `wait` in tests.
#[tokio::test]
async fn drain_trigger_fire_then_wait_resolves() {
    let trigger = DrainTrigger::new();
    let honored = trigger.fire(false);
    assert!(!honored, "first writer's value is the effective one");
    let waited = tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
    assert!(waited.is_ok(), "wait() did not resolve after fire()");
}

/// Firing repeatedly must not deadlock a subsequent `wait`. `Notify`
/// coalesces multiple `notify_one` calls into a single permit, so the
/// second and third `fire` while no waiter is pending are no-ops and
/// the *first* permit is still available for the next `wait`. Catches
/// a future reimplementation that internally tracks "fired" state and
/// burns one permit per call (e.g. a hand-rolled `Mutex<bool>` that
/// returns a never-resolving future on the second call). The third
/// fire makes the test robust against a "burns one permit per fire"
/// bug — two would still resolve under that buggy implementation if
/// the first fire stored a permit and the second consumed it before
/// `wait` was polled.
#[tokio::test]
async fn drain_trigger_repeated_fire_does_not_deadlock_wait() {
    let trigger = DrainTrigger::new();
    let h1 = trigger.fire(false);
    let h2 = trigger.fire(false); // second fire — coalesces, permit still available
    let h3 = trigger.fire(false); // third fire — same coalesce; defends against the
    // "burns one permit per fire" regression class
    assert_eq!(
        (h1, h2, h3),
        (false, false, false),
        "all three fires must report the same effective value"
    );
    let waited = tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
    assert!(
        waited.is_ok(),
        "wait() did not resolve after three fire() calls"
    );
}

/// First-writer-wins on `wait_admin` (issue #604 review): two
/// races on the same trigger must not produce an ack that lies
/// about what the runtime will see. The first `fire(true)` stores
/// `true`; a second `fire(false)` must return `true` (the
/// effective value the runtime will read), not its own argument.
#[tokio::test]
async fn drain_trigger_fire_is_first_writer_wins() {
    let trigger = DrainTrigger::new();
    let first = trigger.fire(true);
    let second = trigger.fire(false);
    assert!(first, "first fire reports its own value");
    assert!(
        second,
        "second fire reports the prior writer's value, not its own"
    );
    assert!(
        trigger.wait_admin(),
        "runtime reader sees the first writer's value"
    );
}

/// `admin_v1_drain` fires the trigger and returns `initiated: true`.
/// Verifies the RPC handler -> trigger -> Notify chain end-to-end
/// without spinning up a full runtime.
#[tokio::test]
async fn admin_drain_fires_trigger_and_returns_initiated() {
    let trigger = Arc::new(DrainTrigger::new());
    let (cache, _tmp) = test_cache().await;
    let state = AdminState::new(
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        Arc::clone(&trigger),
        Arc::new(crate::metrics::Metrics::new()),
    );
    let rpc = AdminRpcImpl::new(state);

    let resp = rpc
        .drain(DrainRequest { wait_admin: false })
        .await
        .expect("drain ok");
    assert!(resp.initiated, "expected initiated=true");
    // `wait_admin: false` matches the SIGTERM-equivalent ordering.
    // `wait_admin_honored` therefore mirrors the request and is also
    // `false` — the CLI uses this to refuse polling when the drain
    // that fired did not opt in.
    assert!(
        !trigger.wait_admin(),
        "default DrainRequest must not enable wait_admin"
    );
    assert!(
        !resp.wait_admin_honored,
        "default DrainRequest must report wait_admin_honored=false"
    );

    // Verify the trigger actually fired: `wait()` should resolve
    // immediately because the Notify stored a permit.
    let waited = tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
    assert!(
        waited.is_ok(),
        "drain RPC did not fire the underlying DrainTrigger"
    );
}

/// `admin_v1_drain` with `wait_admin: true` (issue #604)
/// establishes the trigger's effective value atomically via
/// `fire(true)` so the runtime's reader sees it after `wait()`
/// resolves. Cross-thread visibility is via the
/// `Notify::notify_one → notified()` happens-before edge.
#[tokio::test]
async fn admin_drain_with_wait_admin_sets_flag_before_fire() {
    let trigger = Arc::new(DrainTrigger::new());
    let (cache, _tmp) = test_cache().await;
    let state = AdminState::new(
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        Arc::clone(&trigger),
        Arc::new(crate::metrics::Metrics::new()),
    );
    let rpc = AdminRpcImpl::new(state);

    // Pre-condition: trigger starts with wait_admin=false.
    assert!(
        !trigger.wait_admin(),
        "trigger must start with wait_admin=false"
    );

    let resp = rpc
        .drain(DrainRequest { wait_admin: true })
        .await
        .expect("drain ok");
    assert!(resp.initiated, "expected initiated=true");
    assert!(
        trigger.wait_admin(),
        "wait_admin=true request must set the trigger flag"
    );
    // Server reports the ack so the CLI can refuse to poll when a
    // server doesn't honor `--wait`.
    assert!(
        resp.wait_admin_honored,
        "wait_admin=true request must report wait_admin_honored=true"
    );

    // The fire must still happen — runtime needs to wake up either way.
    let waited = tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
    assert!(
        waited.is_ok(),
        "drain RPC with wait_admin=true must still fire the trigger"
    );
}

/// `admin_v1_health.in_flight_streams` reflects the live
/// `dispatch_in_flight` gauge value (issue #604). Without this
/// wiring, `decdn node drain --wait` would loop forever — the
/// polling client sees a constant 0 regardless of the actual
/// in-flight handler count.
#[tokio::test]
async fn health_reports_in_flight_streams_from_dispatch_gauge() {
    use decdn_common::config::ResolvedSecurity;

    let trigger = Arc::new(DrainTrigger::new());
    let (cache, _tmp) = test_cache().await;
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let state = AdminState::new(
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        Arc::clone(&trigger),
        Arc::clone(&metrics),
    );
    let rpc = AdminRpcImpl::new(state);

    // Baseline: no permits held, gauge reads 0.
    let h = rpc.health().await.expect("health ok");
    assert_eq!(h.in_flight_streams, 0, "baseline must be 0");

    // Acquire a permit via the limiter; the gauge increments.
    // Use a permissive resolved-security so neither layer rejects.
    let limiter = crate::dispatch::ConnectionLimiter::new(
        &ResolvedSecurity {
            max_concurrent_handlers: u32::MAX,
            per_source_rate_per_sec: 1e9,
            per_source_burst: u32::MAX,
            max_tracked_sources: 16,
        },
        Arc::clone(&metrics),
    );
    let permit = limiter
        .acquire_for_test(Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)))
        .expect("acquire permit");

    let h = rpc.health().await.expect("health ok");
    assert_eq!(
        h.in_flight_streams, 1,
        "expected gauge=1 while one permit is held"
    );

    // Dropping the permit decrements the gauge.
    drop(permit);
    let h = rpc.health().await.expect("health ok");
    assert_eq!(
        h.in_flight_streams, 0,
        "expected gauge=0 after dropping the permit"
    );
}

/// Failure path: a hook pointing at a missing file must surface
/// `RELOAD_ERROR_CODE` and the underlying reload state must keep its
/// previous values (the SIGHUP arm's "previous values retained"
/// contract — we route through the same code, so this is a check
/// that the RPC layer didn't accidentally swap an `Ok(...)` somewhere
/// in the error mapping).
#[tokio::test]
async fn admin_reload_with_missing_config_returns_reload_error() {
    use crate::runtime::RuntimeReloadState;
    use decdn_common::cli::common::LogLevel;

    let dir = tempfile::tempdir().expect("tempdir");
    // Path inside a tempdir that we never write to — guaranteed
    // missing without depending on filesystem state outside the test.
    let path = dir.path().join("does-not-exist.toml");

    let setter: crate::runtime::LogLevelSetter =
        Box::new(|_| Ok(crate::runtime::LogLevelApply::Installed));
    let reload_state = Arc::new(RuntimeReloadState::for_test_with_setter(
        LogLevel::Info,
        setter,
    ));
    let hook = ReloadHook {
        reload_state: Arc::clone(&reload_state),
        config_path: path,
    };
    let (cache, _tmp) = test_cache().await;
    let state = AdminState::new(
        [0u8; 32],
        Instant::now(),
        cache,
        Some(hook),
        Arc::new(DrainTrigger::new()),
        Arc::new(crate::metrics::Metrics::new()),
    );
    let rpc = AdminRpcImpl::new(state);

    let err = rpc.reload().await.expect_err("expected error");
    assert_eq!(err.code(), -32_004);
    // The error path returns `RELOAD_ERROR_CODE` rather than an
    // accidental `Ok(...)` — the previous-values-retained contract is
    // exercised by the reload unit tests.
    let _ = reload_state;
}

/// Build a `DhtStatusHandles` seeded with two peers in distinct
/// buckets, a fixed staker set, one provider record, one scheduled
/// republish, and a non-zero refresh clock — enough to exercise every
/// field of `StatusResponse`.
fn seeded_dht_handles() -> DhtStatusHandles {
    use crate::dht::routing::NodeId;
    use crate::dht::{ConfigStakerSet, RecordStore, RecordStoreConfig, RepublishScheduler};
    use decdn_protocol::{ContentHash, Coverage};

    // self_id = all-zero; p_high lands in bucket 255 (top bit set),
    // p_low in bucket 0 (only the lowest bit differs).
    let self_id = NodeId::from_bytes([0u8; 32]);
    let mut table = RoutingTable::new(self_id);
    let mut high = [0u8; 32];
    high[0] = 0x80;
    let mut low = [0u8; 32];
    low[31] = 0x01;
    assert!(table.insert(NodeId::from_bytes(high)));
    assert!(table.insert(NodeId::from_bytes(low)));

    let mut stakers = std::collections::HashSet::new();
    stakers.insert(NodeId::from_bytes([1u8; 32]));
    stakers.insert(NodeId::from_bytes([2u8; 32]));
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(stakers));

    let mut store = RecordStore::new(RecordStoreConfig::default());
    store.insert_at(
        NodeId::from_bytes([5u8; 32]),
        ContentHash::from_bytes([7u8; 32]),
        Coverage::full(1),
        1_000,
    );

    let republish = Arc::new(RepublishScheduler::new());
    republish.schedule_steady(ContentHash::from_bytes([9u8; 32]));

    DhtStatusHandles {
        routing: Arc::new(StdMutex::new(table)),
        staker_set,
        record_store: Arc::new(StdMutex::new(store)),
        republish,
        refresh_clock: Arc::new(AtomicU64::new(1_700_000_000_000_000)),
        refresh_interval: Duration::from_hours(1),
    }
}

#[tokio::test]
async fn status_reports_routing_and_dht_health() {
    let (state, _tmp) = state_with().await;
    let state = state.with_dht(seeded_dht_handles());
    let rpc = AdminRpcImpl::new(state);

    let resp = rpc.status().await.expect("status ok");

    assert_eq!(resp.routing.total_peers, 2);
    assert_eq!(resp.routing.non_empty_buckets, 2);
    // Buckets are reported ascending by index: bucket 0 then bucket 255.
    let indices: Vec<u16> = resp.routing.buckets.iter().map(|b| b.index).collect();
    assert_eq!(indices, vec![0, 255]);
    for b in &resp.routing.buckets {
        assert_eq!(b.fill, 1);
    }
    assert_eq!(resp.routing.bucket_capacity, 20);
    assert_eq!(resp.routing.refresh_interval_s, 3_600);
    assert_eq!(resp.routing.last_refresh_us, Some(1_700_000_000_000_000));
    assert_eq!(resp.known_stakers, 2);
    assert_eq!(resp.record_store.records, 1);
    assert_eq!(resp.record_store.capacity, 1_000_000);
    assert_eq!(resp.republish.scheduled_records, 1);
}

/// The operator address wired via `with_operator_address` is reported as an
/// EIP-55 checksummed string, and an `AdminState` with none reports `None`
/// (#1906).
#[tokio::test]
async fn status_reports_operator_address_when_wired() {
    let operator: Address = "0x52908400098527886e0f7030069857d2e4169ee7"
        .parse()
        .expect("valid address");

    let (state, _tmp) = state_with().await;
    let rpc = AdminRpcImpl::new(
        state
            .with_dht(seeded_dht_handles())
            .with_operator_address(operator),
    );
    let resp = rpc.status().await.expect("status ok");
    // EIP-55 checksum, not the lowercase input.
    assert_eq!(
        resp.operator_address.as_deref(),
        Some("0x52908400098527886E0F7030069857D2E4169EE7")
    );

    let (state, _tmp) = state_with().await;
    let rpc = AdminRpcImpl::new(state.with_dht(seeded_dht_handles()));
    let resp = rpc.status().await.expect("status ok");
    assert_eq!(resp.operator_address, None);
}

/// A zero refresh clock (no bucket-refresh pass has completed yet)
/// must surface as `last_refresh_us: None`, not `Some(0)`.
#[tokio::test]
async fn status_never_refreshed_reports_none() {
    let (state, _tmp) = state_with().await;
    let mut handles = seeded_dht_handles();
    handles.refresh_clock = Arc::new(AtomicU64::new(0));
    let rpc = AdminRpcImpl::new(state.with_dht(handles));

    let resp = rpc.status().await.expect("status ok");
    assert_eq!(resp.routing.last_refresh_us, None);
}

/// Without DHT handles attached (the `new`-only construction the other
/// admin tests use), `status` returns [`DHT_UNAVAILABLE_CODE`] rather
/// than panicking or returning a misleading empty snapshot.
#[tokio::test]
async fn status_without_dht_returns_unavailable_error() {
    let (state, _tmp) = state_with().await;
    let rpc = AdminRpcImpl::new(state);

    let err = rpc
        .status()
        .await
        .expect_err("expected DHT-unavailable error");
    assert_eq!(err.code(), DHT_UNAVAILABLE_CODE);
}

/// A poisoned routing-table mutex (a writer panicked while holding it)
/// must surface as the distinct [`DHT_POISONED_CODE`], never panic and
/// never be conflated with the benign "no DHT wired" case. Exercises
/// the `lock_or_rpc_err` anti-panic net.
#[tokio::test]
async fn status_poisoned_routing_mutex_returns_poisoned_error() {
    let (state, _tmp) = state_with().await;
    let handles = seeded_dht_handles();
    // Poison the routing-table mutex by panicking while holding it.
    let routing = Arc::clone(&handles.routing);
    std::thread::spawn(move || {
        let _guard = routing.lock();
        panic!("intentional poison");
    })
    .join()
    .expect_err("the spawned thread must panic to poison the lock");

    let rpc = AdminRpcImpl::new(state.with_dht(handles));
    let err = rpc.status().await.expect_err("expected DHT-poisoned error");
    assert_eq!(err.code(), DHT_POISONED_CODE);
}

/// Symmetric to the routing test: the record-store read also goes
/// through `lock_or_rpc_err`, so a poisoned record-store mutex must
/// surface as [`DHT_POISONED_CODE`] too. Guards against a future edit
/// swapping this site for an `unwrap` while the routing site keeps the
/// anti-panic net.
#[tokio::test]
async fn status_poisoned_record_store_mutex_returns_poisoned_error() {
    let (state, _tmp) = state_with().await;
    let handles = seeded_dht_handles();
    // Poison the record-store mutex by panicking while holding it.
    let record_store = Arc::clone(&handles.record_store);
    std::thread::spawn(move || {
        let _guard = record_store.lock();
        panic!("intentional poison");
    })
    .join()
    .expect_err("the spawned thread must panic to poison the lock");

    let rpc = AdminRpcImpl::new(state.with_dht(handles));
    let err = rpc.status().await.expect_err("expected DHT-poisoned error");
    assert_eq!(err.code(), DHT_POISONED_CODE);
}

// ---- admin_v1_lanes (issue #749) ----

use alloy::primitives::{Address, B256};
use decdn_incentive::MemoryPoolStateStore;

/// Build a hydrated [`LaneState`] with the given identity + outstanding
/// claim. `pool_byte` seeds the pool id, `signer_byte` the signer (which is
/// also the reported counterparty in the shared-pool model); `last_amount`
/// is the lane's cumulative claim. The provider is a fixed non-signer
/// address so a signer/provider transposition would be caught.
fn mk_lane(pool_byte: u8, signer_byte: u8, last_amount: u64) -> LaneState {
    let mut id = [0u8; 32];
    id[31] = pool_byte;
    let mut signer = [0u8; 20];
    signer[19] = signer_byte;
    let provider = Address::repeat_byte(0xEE);
    LaneState::hydrate(
        id.into(),
        Address::from(signer),
        provider,
        U256::from(1_000_000_000u64), // cap — irrelevant to the snapshot
        0,                            // expiry — untracked
        U256::from(last_amount),
        U256::from(last_amount), // bytes_delivered — irrelevant to the snapshot
        None,
        decdn_incentive::LaneChain::NONE,
    )
}

/// `build_lane_snapshots` maps each [`LaneState`] to its wire DTO:
/// hex ids, narrowed amounts, and threshold-based eligibility. A lane
/// at/above the threshold is eligible; one below is not. In the shared-pool
/// model the per-lane deposit and nonce are not tracked, so both report 0.
#[test]
fn build_lane_snapshots_maps_fields_and_eligibility() {
    let states = vec![mk_lane(1, 0xAA, 2_500_000), mk_lane(2, 0xBB, 500_000)];
    let ages = HashMap::new();
    let threshold = U256::from(1_000_000u64);
    let snaps = build_lane_snapshots(&states, threshold, &ages);
    assert_eq!(snaps.len(), 2);
    // No activity recorded → both have `None`; within the `None`
    // group ordering is by descending outstanding, so the
    // 2_500_000-claim lane comes first.
    let first = snaps.first().expect("first snapshot");
    assert_eq!(first.outstanding_micro_usdc, 2_500_000);
    assert_eq!(first.deposit_micro_usdc, 0);
    assert_eq!(first.last_nonce, 0);
    assert!(
        first.pool_id.starts_with("0x"),
        "pool_id must be 0x-hex: {}",
        first.pool_id
    );
    assert!(
        first.counterparty.starts_with("0x"),
        "counterparty must be 0x-hex: {}",
        first.counterparty
    );
    assert_eq!(first.seconds_since_last_voucher, None);
    assert!(
        first.settlement_eligible,
        "2.5 USDC claim >= 1 USDC threshold → eligible"
    );
    let second = snaps.get(1).expect("second snapshot");
    assert_eq!(second.outstanding_micro_usdc, 500_000);
    assert!(
        !second.settlement_eligible,
        "0.5 USDC claim < 1 USDC threshold → not eligible"
    );
}

/// Threshold boundary: outstanding exactly equal to the threshold is
/// eligible (`>=`), matching the redeemer's `< threshold` short-circuit.
#[test]
fn build_lane_snapshots_threshold_is_inclusive() {
    let states = vec![mk_lane(1, 0xAA, 1_000_000)];
    let ages = HashMap::new();
    let snaps = build_lane_snapshots(&states, U256::from(1_000_000u64), &ages);
    assert!(
        snaps.first().expect("snapshot").settlement_eligible,
        "outstanding == threshold must be eligible (>=)"
    );
}

/// A lane with recorded voucher activity sorts ahead of one with
/// none, and reports `Some(age)`.
#[test]
fn build_lane_snapshots_orders_active_lanes_first() {
    let active = mk_lane(1, 0xAA, 100);
    let idle = mk_lane(2, 0xBB, 9_000_000);
    // `active` has a known age; `idle` is absent from the map (→ `None`).
    // `idle` has a much larger outstanding, but no activity — the active
    // lane must still sort first (recency beats size).
    let ages = HashMap::from([(active.key(), 3u64)]);
    let snaps = build_lane_snapshots(&[idle, active], U256::from(1_000_000u64), &ages);
    let first = snaps.first().expect("first snapshot");
    assert!(
        first.seconds_since_last_voucher.is_some(),
        "active lane (with a touch) must sort first"
    );
    assert_eq!(first.outstanding_micro_usdc, 100);
}

/// `admin_v1_lanes` with no lane handles wired returns an empty
/// list and a zero threshold (not an error) — a node with no payment
/// surface legitimately has nothing to report.
#[tokio::test]
async fn lanes_without_handles_returns_empty() {
    let (state, _tmp) = state_with().await;
    let rpc = AdminRpcImpl::new(state);
    let resp = rpc.lanes().await.expect("lanes ok");
    assert!(resp.lanes.is_empty());
    assert_eq!(resp.redeem_threshold_micro_usdc, 0);
}

/// End-to-end through the RPC method: a store seeded with two lanes
/// surfaces both, with the configured threshold echoed and eligibility
/// computed against it.
#[tokio::test]
async fn lanes_rpc_reports_seeded_store() -> anyhow::Result<()> {
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&mk_lane(1, 0xAA, 2_000_000))?;
    store.record(&mk_lane(2, 0xBB, 100_000))?;

    let (state, _tmp) = state_with().await;
    let handles = LaneStatusHandles {
        pool_store: store as Arc<dyn PoolStateStore>,
        lane_activity: LaneActivityClock::empty(),
        redeem_threshold_micro_usdc: 1_000_000,
    };
    let rpc = AdminRpcImpl::new(state.with_lanes(handles));
    let resp = rpc.lanes().await.expect("lanes ok");
    assert_eq!(resp.redeem_threshold_micro_usdc, 1_000_000);
    assert_eq!(resp.lanes.len(), 2);
    // Both have no activity → ordered by descending outstanding.
    let first = resp.lanes.first().expect("first");
    assert_eq!(first.outstanding_micro_usdc, 2_000_000);
    assert!(first.settlement_eligible);
    let second = resp.lanes.get(1).expect("second");
    assert_eq!(second.outstanding_micro_usdc, 100_000);
    assert!(!second.settlement_eligible);
    Ok(())
}

// ---- admin_v1_pools (#2078) ----

/// The deployment every seeded buyer pool lives on.
const BUYER_DEPLOYMENT: decdn_incentive::Deployment = decdn_incentive::Deployment {
    chain_id: 421_614,
    payment_pool: Address::repeat_byte(0x9c),
};

/// Seed one buyer pool with two lanes, deliberately out of sorted order.
fn mk_buyer_pool(pool_byte: u8, deposit: u64) -> BuyerPoolState {
    BuyerPoolState::hydrate(
        B256::repeat_byte(pool_byte),
        BUYER_DEPLOYMENT,
        Address::repeat_byte(0x11),
        Address::repeat_byte(0xcd),
        U256::from(deposit),
        vec![
            (
                LaneKey {
                    pool_id: B256::repeat_byte(pool_byte),
                    signer: Address::repeat_byte(0x11),
                    provider: Address::repeat_byte(0xbb),
                },
                decdn_incentive::buyer_pool::BuyerLaneProgress {
                    last_amount: U256::from(2_000u64),
                    last_bytes: U256::from(20_000u64),
                },
            ),
            (
                LaneKey {
                    pool_id: B256::repeat_byte(pool_byte),
                    signer: Address::repeat_byte(0x11),
                    provider: Address::repeat_byte(0xaa),
                },
                decdn_incentive::buyer_pool::BuyerLaneProgress {
                    last_amount: U256::from(1_000u64),
                    last_bytes: U256::from(10_000u64),
                },
            ),
        ],
        U256::ZERO,
    )
}

/// `admin_v1_pools` with no buyer store wired is an ERROR, not an empty
/// list. The distinction is the whole point of the method (#2078): "this
/// node never pays for pulls" and "this node owns no pools" send an
/// operator hunting a stranded deposit in opposite directions.
#[tokio::test]
async fn pools_without_store_is_unavailable_not_empty() {
    let (state, _tmp) = state_with().await;
    let rpc = AdminRpcImpl::new(state);
    let err = rpc.pools().await.expect_err("pools must fail when unwired");
    assert_eq!(err.code(), BUYER_POOL_UNAVAILABLE_CODE);
}

/// End-to-end through the RPC method: a seeded store surfaces every pool
/// and every lane, sorted, with the micro-USDC amounts intact.
#[tokio::test]
async fn pools_rpc_reports_seeded_store() -> anyhow::Result<()> {
    let store = Arc::new(decdn_incentive::MemoryBuyerPoolStore::new());
    // Recorded newest-id-first so the response's sort is doing real work.
    store.record(&mk_buyer_pool(0xbb, 9_000_000))?;
    store.record(&mk_buyer_pool(0xaa, 10_000_000))?;

    let (state, _tmp) = state_with().await;
    let rpc = AdminRpcImpl::new(state.with_buyer_pools(store as Arc<dyn BuyerPoolStore>));
    let resp = rpc.pools().await.expect("pools ok");

    assert_eq!(resp.pools.len(), 2);
    assert!(resp.skipped.is_empty());
    let first = resp.pools.first().expect("first pool");
    assert_eq!(
        first.pool_id,
        format!("{:#x}", B256::repeat_byte(0xaa)),
        "pools sort by pool_id, not store order"
    );
    assert_eq!(first.deposit_micro_usdc, 10_000_000);
    assert_eq!(first.chain_id, BUYER_DEPLOYMENT.chain_id);
    assert_eq!(
        first.payment_pool,
        BUYER_DEPLOYMENT.payment_pool.to_string()
    );
    // Lanes sort by (signer, provider); 0xaa..aa precedes 0xbb..bb.
    let lane = first.lanes.first().expect("first lane");
    assert_eq!(lane.provider, Address::repeat_byte(0xaa).to_string());
    assert_eq!(lane.last_amount_micro_usdc, 1_000);
    assert_eq!(lane.last_bytes_delivered, 10_000);
    Ok(())
}

/// A deposit beyond `u64::MAX` micro-USDC saturates rather than wrapping.
/// Unreachable for a real pool, but the narrowing must not silently report
/// a tiny deposit for a huge one.
#[test]
fn build_buyer_pools_response_saturates_oversized_deposit() {
    let pool = BuyerPoolState::new(
        B256::repeat_byte(0x01),
        BUYER_DEPLOYMENT,
        Address::repeat_byte(0x11),
        Address::repeat_byte(0xcd),
        U256::MAX,
    );
    let resp = build_buyer_pools_response(vec![pool], &[]);
    assert_eq!(
        resp.pools.first().expect("pool").deposit_micro_usdc,
        u64::MAX
    );
}

/// An empty `pools` beside a non-empty `skipped` is a real state and must
/// survive to the wire: the escrowed deposit behind an undecodable row is
/// exactly what an operator is looking for.
#[test]
fn build_buyer_pools_response_keeps_skipped_when_no_pool_decodes() {
    let resp = build_buyer_pools_response(
        Vec::new(),
        &[B256::repeat_byte(0x44), B256::repeat_byte(0x22)],
    );
    assert!(resp.pools.is_empty());
    assert_eq!(
        resp.skipped,
        vec![
            format!("{:#x}", B256::repeat_byte(0x22)),
            format!("{:#x}", B256::repeat_byte(0x44)),
        ],
        "skipped ids sort for stable output"
    );
}

/// Every id this response carries must parse back as a `PoolId`.
///
/// The CLI matches these ids against the chain's own enumeration to decide
/// which pools the node tracks (`decdn pool list --all`). A spelling it
/// cannot parse means a pool the daemon is paying from renders as
/// untracked, which reads as a stranded deposit to recover. Nothing else
/// compares the two sides of this format, so it is pinned where the
/// spelling is produced.
#[test]
fn build_buyer_pools_response_ids_parse_back() {
    use std::str::FromStr as _;

    let tracked = B256::repeat_byte(0x01);
    let skipped = B256::repeat_byte(0x02);
    let resp = build_buyer_pools_response(
        vec![BuyerPoolState::new(
            tracked,
            BUYER_DEPLOYMENT,
            Address::repeat_byte(0x11),
            Address::repeat_byte(0xcd),
            U256::from(1u64),
        )],
        &[skipped],
    );

    let parsed = B256::from_str(&resp.pools.first().expect("pool").pool_id)
        .expect("a tracked pool id must parse back");
    assert_eq!(parsed, tracked);
    let parsed = B256::from_str(resp.skipped.first().expect("skipped"))
        .expect("a skipped pool id must parse back");
    assert_eq!(parsed, skipped);
}

/// Reader wiring guard (issue #1733): the RPC reads last-voucher ages off
/// the client handler's live lane registry via [`LaneActivityClock`]. A
/// stamped lane must report `Some(age)` and sort ahead of an idle lane with
/// a larger claim (recency beats size); an unstamped lane still reads
/// `None`. The stamp-under-the-accept-lock end-to-end path is covered by
/// `client_loopback`'s `accepted_voucher_advances_lane_activity_clock`.
#[tokio::test]
async fn lanes_rpc_reflects_stamped_lane_through_activity_clock() -> anyhow::Result<()> {
    let store = Arc::new(MemoryPoolStateStore::new());
    // `active` has the smaller claim; `idle` the larger. Without a stamp,
    // `idle` would sort first (descending outstanding). A stamp on `active`
    // must flip that — proving the reader sees the lane's stamp.
    let active = mk_lane(1, 0xAA, 100);
    let idle = mk_lane(2, 0xBB, 9_000_000);
    let active_id = active.key();
    store.record(&active)?;
    store.record(&idle)?;

    let (state, _tmp) = state_with().await;
    let handles = LaneStatusHandles {
        pool_store: store as Arc<dyn PoolStateStore>,
        lane_activity: LaneActivityClock::with_stamped_lanes(&[active_id]),
        redeem_threshold_micro_usdc: 1_000_000,
    };
    let rpc = AdminRpcImpl::new(state.with_lanes(handles));
    let resp = rpc.lanes().await.expect("lanes ok");
    assert_eq!(resp.lanes.len(), 2);

    let first = resp.lanes.first().expect("first");
    assert!(
        first.seconds_since_last_voucher.is_some(),
        "the stamped lane must report Some(age), not None"
    );
    assert_eq!(
        first.outstanding_micro_usdc, 100,
        "the stamped (active) lane must sort first despite the smaller claim"
    );
    // The unstamped lane still reads None through the same reader.
    let second = resp.lanes.get(1).expect("second");
    assert_eq!(second.seconds_since_last_voucher, None);
    Ok(())
}

/// A store whose `load_all` errors surfaces as
/// [`POOL_STORE_ERROR_CODE`] rather than a generic transport fault.
#[tokio::test]
async fn lanes_rpc_surfaces_store_load_failure() {
    use decdn_incentive::{LaneKey, StoreError};

    #[derive(Debug)]
    struct FailingStore;
    impl PoolStateStore for FailingStore {
        fn load_all(&self) -> Result<Vec<LaneState>, StoreError> {
            Err(StoreError::Backend("simulated load failure".to_string()))
        }
        fn record(&self, _state: &LaneState) -> Result<(), StoreError> {
            Ok(())
        }
        fn forget(&self, _key: LaneKey) -> Result<(), StoreError> {
            Ok(())
        }
        fn get(&self, _key: LaneKey) -> Result<Option<LaneState>, StoreError> {
            Ok(None)
        }
    }

    let (state, _tmp) = state_with().await;
    let handles = LaneStatusHandles {
        pool_store: Arc::new(FailingStore) as Arc<dyn PoolStateStore>,
        lane_activity: LaneActivityClock::empty(),
        redeem_threshold_micro_usdc: 1_000_000,
    };
    let rpc = AdminRpcImpl::new(state.with_lanes(handles));
    let err = rpc.lanes().await.expect_err("expected store-load error");
    assert_eq!(err.code(), POOL_STORE_ERROR_CODE);
}

/// `bind` accepts both loopback and non-loopback addresses; the
/// non-loopback path emits a `WARN` (#845) but never rejects. We can't
/// cheaply intercept the tracing emission, so this is a smoke test of
/// both branches plus IPv6 loopback — a future refactor that narrowed
/// the predicate to e.g. `addr.ip() == Ipv4Addr::LOCALHOST` would regress
/// on `::1` and break here visibly. Mirrors `metrics::bind`'s test.
#[tokio::test]
async fn bind_accepts_loopback_and_warns_on_non_loopback() {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    // IPv4 loopback: warn-free.
    let v4_loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let listener = bind(v4_loopback).unwrap();
    let bound = listener.local_addr().unwrap();
    assert!(
        bound.ip().is_loopback(),
        "IPv4 loopback bind should resolve to a loopback addr: got {bound}"
    );
    drop(listener);

    // IPv6 loopback `::1`: also warn-free. Some hosts disable IPv6; skip
    // rather than fail if the bind itself errors.
    let v6_loopback = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
    if let Ok(listener) = bind(v6_loopback) {
        let bound = listener.local_addr().unwrap();
        assert!(
            bound.ip().is_loopback(),
            "IPv6 loopback bind should resolve to a loopback addr: got {bound}"
        );
    }

    // Unspecified (`0.0.0.0`): allowed, but the bind path WARNs. A
    // regression that rejected unspecified would surface as a `bind`
    // error here.
    let unspecified = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let listener = bind(unspecified).unwrap();
    let bound = listener.local_addr().unwrap();
    assert!(
        bound.ip().is_unspecified(),
        "0.0.0.0 bind should resolve to the unspecified addr: got {bound}"
    );
    drop(listener);
}
