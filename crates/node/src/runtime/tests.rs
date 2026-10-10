use super::*;
use std::assert_matches;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// `with_poll_interval` overrides alloy's localhost-detected 250 ms client
/// poll default — the interval alloy's pending-tx receipt heartbeat polls on
/// (#1011). Building against a `127.0.0.1` URL exercises the exact path that
/// triggers it — alloy would seed 250 ms — and the helper must replace it. No
/// network I/O: `poll_interval()` reads a local atomic on the client.
#[test]
fn with_poll_interval_overrides_alloy_local_default() {
    let url: alloy::transports::http::reqwest::Url =
        "http://127.0.0.1:8545".parse().expect("static URL parses");
    let bare = ProviderBuilder::new().connect_http(url.clone());
    // Precondition: alloy seeds the 250 ms localhost default we are fixing.
    assert_eq!(bare.client().poll_interval(), Duration::from_millis(250));

    let provider = with_poll_interval(
        ProviderBuilder::new().connect_http(url),
        Duration::from_secs(7),
    );
    assert_eq!(provider.client().poll_interval(), Duration::from_secs(7));
}

#[test]
fn provider_factory_preserves_role_polling_policy() {
    let url: HttpUrl = "http://localhost:8545".parse().expect("valid URL");
    let interval = Duration::from_secs(7);

    let providers = ProviderFactory::new(url, Arc::new(metrics::Metrics::new()));

    let read = providers.read_only(interval);
    let head = providers.shared_head();
    let seller = providers.seller_wallet(PrivateKeySigner::random(), interval);
    let buyer = providers.buyer_wallet(PrivateKeySigner::random(), interval);

    assert_eq!(read.client().poll_interval(), interval);
    assert_eq!(head.client().poll_interval(), Duration::from_millis(250));
    assert_eq!(seller.client().poll_interval(), interval);
    assert_eq!(buyer.client().poll_interval(), interval);
}

/// `admin_stop_order` defaults to `Early` (the original
/// `appendix-local-admin-http` ordering) — the admin server stops
/// before `router.shutdown` for SIGINT/SIGTERM and for any drain
/// RPC that didn't ask for `wait_admin`.
#[test]
fn admin_stop_order_default_is_early() {
    let trigger = admin::DrainTrigger::new();
    // No fire yet → wait_admin() returns false.
    assert_eq!(
        admin_stop_order(ShutdownSignal::AdminDrain, &trigger),
        AdminStopOrder::Early,
    );
    // A drain RPC that explicitly asks for the default ordering.
    let _ = trigger.fire(false);
    assert_eq!(
        admin_stop_order(ShutdownSignal::AdminDrain, &trigger),
        AdminStopOrder::Early,
    );
}

/// `admin_stop_order` returns `AfterRouter` when (and only when)
/// the drain originated from the admin RPC *and* it asked for
/// `wait_admin: true`.
#[test]
fn admin_stop_order_admin_drain_with_wait_admin_is_after_router() {
    let trigger = admin::DrainTrigger::new();
    let _ = trigger.fire(true);
    assert_eq!(
        admin_stop_order(ShutdownSignal::AdminDrain, &trigger),
        AdminStopOrder::AfterRouter,
    );
}

/// SIGINT wins over a sticky `wait_admin=true` flag: an RPC that
/// stored `wait_admin=true` mid-flight when an operator hit
/// Ctrl-C must not flip the runtime onto the `AfterRouter` path.
/// The operator's stated intent (SIGINT = stop now) wins.
#[test]
fn admin_stop_order_sigint_overrides_sticky_wait_admin() {
    let trigger = admin::DrainTrigger::new();
    let _ = trigger.fire(true);
    assert_eq!(
        admin_stop_order(ShutdownSignal::Sigint, &trigger),
        AdminStopOrder::Early,
    );
}

/// Same as the SIGINT test, but for SIGTERM (unix only).
#[cfg(unix)]
#[test]
fn admin_stop_order_sigterm_overrides_sticky_wait_admin() {
    let trigger = admin::DrainTrigger::new();
    let _ = trigger.fire(true);
    assert_eq!(
        admin_stop_order(ShutdownSignal::Sigterm, &trigger),
        AdminStopOrder::Early,
    );
}

#[tokio::test]
async fn listener_waits_for_successful_initial_blacklist_sync() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let (ready_tx, ready_rx) = oneshot::channel();
    let started = Arc::new(AtomicBool::new(false));
    let gate = gate_listener_on_blacklist_sync(ready_rx, {
        let started = Arc::clone(&started);
        move || started.store(true, Ordering::SeqCst)
    });
    tokio::pin!(gate);

    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut gate)
            .await
            .is_err(),
        "listener gate must stay pending while initial blacklist sync is pending"
    );
    assert!(
        !started.load(Ordering::SeqCst),
        "no ALPN listener may start before blacklist readiness"
    );

    ready_tx.send(Ok(())).expect("readiness receiver is live");
    assert!(gate.await.is_ok(), "successful sync should open the gate");
    assert!(
        started.load(Ordering::SeqCst),
        "listener starts only after successful blacklist readiness"
    );
}

#[tokio::test]
async fn listener_stays_closed_when_initial_blacklist_sync_fails() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let (ready_tx, ready_rx) = oneshot::channel();
    let started = Arc::new(AtomicBool::new(false));
    ready_tx
        .send(Err("ContentBlacklist getLogs unavailable".to_string()))
        .expect("readiness receiver is live");

    let err = gate_listener_on_blacklist_sync(ready_rx, {
        let started = Arc::clone(&started);
        move || started.store(true, Ordering::SeqCst)
    })
    .await
    .expect_err("failed initial sync must fail startup");

    assert!(
        format!("{err:#}").contains("ContentBlacklist getLogs unavailable"),
        "startup error should preserve the sync failure: {err:#}"
    );
    assert!(
        !started.load(Ordering::SeqCst),
        "listener must remain closed after a failed initial sync"
    );
}

#[tokio::test]
async fn listener_fails_closed_when_watcher_drops_readiness() {
    use std::sync::atomic::{AtomicBool, Ordering};

    // The watcher task died (panicked/aborted) before signaling either
    // outcome: the gate must treat a dropped sender as a hard failure, not
    // hang or silently open the listeners.
    let (ready_tx, ready_rx) = oneshot::channel();
    let started = Arc::new(AtomicBool::new(false));
    drop(ready_tx);

    let err = gate_listener_on_blacklist_sync(ready_rx, {
        let started = Arc::clone(&started);
        move || started.store(true, Ordering::SeqCst)
    })
    .await
    .expect_err("a dropped readiness channel must fail startup closed");

    assert!(
        format!("{err:#}").contains("blacklist watcher exited before initial sync completed"),
        "startup error should explain the watcher exited early: {err:#}"
    );
    assert!(
        !started.load(Ordering::SeqCst),
        "listener must remain closed when the watcher never reported readiness"
    );
}

/// Build a minimal `ResolvedConfig` with the cache section
/// pointed at the given `origin`. Other sections carry sensible
/// dummies — only the cache is exercised. Mirrors the fixture
/// shape used in `runtime::reload::tests` and
/// `crates/node/tests/sighup_signal.rs`.
///
/// Returns the owning `TempDir` alongside the config so the
/// caller binds it (`let (_tmp, cfg) = ...`) and the directory
/// lives until end-of-test. A pid-keyed directory is not safe
/// here: tests in a binary that uses `cargo test` (rather than
/// `cargo nextest`) share the process and would race on shared
/// `cache_dir` state inside `CacheEngine::open_full`.
fn cfg_with_origin(origin: Option<ResolvedOrigin>) -> (tempfile::TempDir, ResolvedConfig) {
    cfg_with_origins(origin.map(|o| vec![o]).unwrap_or_default())
}

#[allow(clippy::too_many_lines)] // exhaustive ResolvedConfig test builder
fn cfg_with_origins(origins: Vec<ResolvedOrigin>) -> (tempfile::TempDir, ResolvedConfig) {
    use decdn_common::config::{
        ResolvedBlockchain, ResolvedIdentity, ResolvedNetwork, ResolvedObservability,
        ResolvedPayment, ResolvedSecurity,
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache_dir = tmp.path().to_path_buf();
    let cfg = ResolvedConfig {
        identity: ResolvedIdentity {
            data_dir: cache_dir.clone(),
            region: None,
        },
        network: ResolvedNetwork {
            bind_port: 4433,
            relay_urls: Vec::new(),
            discovery: decdn_common::config::ResolvedDiscovery::default(),
        },
        blockchain: ResolvedBlockchain {
            origin_assignment_address: None,
            origin_directory_positive_ttl_sec:
                decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_POSITIVE_TTL_SEC,
            origin_directory_negative_ttl_sec:
                decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_NEGATIVE_TTL_SEC,
            origin_directory_cache_capacity:
                decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_CACHE_CAPACITY,
            publisher_registry_address: None,
            rpc_url: "http://localhost:8545".into(),
            eth_keystore: PathBuf::from("/tmp/keystore.json"),
            keystore_password_file: None,
            payment_pool_address: "0x0000000000000000000000000000000000000001".into(),
            capacity_bond_address: "0x0000000000000000000000000000000000000002".into(),
            rpc_watchdog_interval_sec: 30,
            event_poll_interval_ms: 7000,
            get_logs_max_block_span: decdn_common::config::DEFAULT_GET_LOGS_MAX_BLOCK_SPAN,
            fee_shares_poll_interval_sec: 3600,
            redeem_threshold_micro_usdc: 1_000_000,
            redeem_max_vouchers_per_tx: 300,
            redeem_interval_secs: 300,
            buyer_working_deposit_micro_usdc: 10_000_000,
            buyer_max_approve: true,
            pool_min_remaining_deposit_micro_usdc: 1_000_000,
            pool_floor_signer_live_windows: 8,
            slash_judge_address: "0x0000000000000000000000000000000000000003".to_string(),
            content_blacklist_address: None,
            content_blacklist_poll_interval_sec: 600,
            chain_staleness_grace_sec: 1800,
            chain_id: decdn_common::config::DEFAULT_CHAIN_ID,
        },
        cache: decdn_common::config::ResolvedCache {
            cache_dir,
            cache_size_mb: 1024,
            disk_headroom_mb: 8192,
            max_blob_size_mb: 128,
            max_rate_per_mb: 0,
            origins,
            pinned_hashes: decdn_cache::PinnedHashes::empty(),
            origin_retry: decdn_cache::RetryPolicy::default(),
            circuit_breaker: decdn_cache::CircuitBreakerPolicy::default(),
            user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
            gc_interval_sec: 0,
            fs_rescan_interval_sec: 0,
            origin_probe_ttl_sec: decdn_common::config::DEFAULT_ORIGIN_PROBE_TTL_SEC,
            origin_probe_negative_ttl_sec:
                decdn_common::config::DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL_SEC,
            origin_probe_fault_ttl_sec: decdn_common::config::DEFAULT_ORIGIN_PROBE_FAULT_TTL_SEC,
            origin_probe_timeout_ms: decdn_common::config::DEFAULT_ORIGIN_PROBE_TIMEOUT_MS,
            origin_probe_memo_capacity: decdn_common::config::DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY,
            eviction_high_water_pct: 90,
            eviction_target_pct: 80,
            eviction_per_sweep_budget: 16,
            eviction_tick_secs: 1,
            max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
            stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
            node_to_node_pull_through_enabled: false,
            relay_foreign_namespaces: decdn_common::config::DEFAULT_RELAY_FOREIGN_NAMESPACES,
            node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
            node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
            node_pull_stall_window_sec: decdn_common::config::DEFAULT_NODE_PULL_STALL_WINDOW_SEC,
            node_pull_min_throughput_bps:
                decdn_common::config::DEFAULT_NODE_PULL_MIN_THROUGHPUT_BPS,
            eviction_policy: decdn_common::config::DEFAULT_EVICTION_POLICY.to_string(),
            admission_policy: decdn_common::config::DEFAULT_ADMISSION_POLICY.to_string(),
            tinylfu: decdn_common::config::ResolvedTinyLfu {
                sketch_bytes: decdn_common::config::DEFAULT_TINYLFU_SKETCH_BYTES,
                promotion_threshold: decdn_common::config::DEFAULT_TINYLFU_PROMOTION_THRESHOLD,
                probation_target_pct: decdn_common::config::DEFAULT_TINYLFU_PROBATION_TARGET_PCT,
                aging_halflife_sec: decdn_common::config::DEFAULT_TINYLFU_AGING_HALFLIFE_SEC,
            },
            serve_economics: decdn_common::config::ResolvedServeEconomics {
                policy: decdn_common::config::DEFAULT_SERVE_ECONOMICS_POLICY.to_string(),
                discount_bps: decdn_common::config::DEFAULT_SERVE_ECONOMICS_DISCOUNT_BPS,
                n_max: decdn_common::config::DEFAULT_SERVE_ECONOMICS_N_MAX,
                warming_budget: decdn_common::config::DEFAULT_SERVE_ECONOMICS_WARMING_BUDGET,
                warming_refill: decdn_common::config::DEFAULT_SERVE_ECONOMICS_WARMING_REFILL,
            },
        },
        payment: ResolvedPayment {
            rate_per_mb: 10,
            credit_max: decdn_common::config::DEFAULT_CREDIT_MAX,
            frame_target_bytes: decdn_common::config::DEFAULT_FRAME_TARGET_BYTES,
            credit_ramp_divisor: decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
            voucher_commit_interval_ms: decdn_common::config::DEFAULT_VOUCHER_COMMIT_INTERVAL_MS,
        },
        observability: ResolvedObservability {
            log_level: decdn_common::cli::common::LogLevel::Info,
            log_format: decdn_common::cli::common::LogFormat::Pretty,
            metrics_port: 9090,
            metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            admin_port: Some(9191),
            otlp_endpoint: None,
        },
        security: ResolvedSecurity {
            max_concurrent_handlers: 256,
            per_source_rate_per_sec: 100.0,
            per_source_burst: 200,
            max_tracked_sources: 4096,
        },
        load_shed: decdn_common::config::ResolvedLoadShed::default(),
        dht: decdn_common::config::ResolvedDht::default(),
        probe: decdn_common::config::ResolvedProbe::default(),
        receipts: decdn_common::config::ResolvedReceipts::default(),
        content: decdn_common::config::ResolvedContent::default(),
    };
    (tmp, cfg)
}

/// `build_cache` must construct the S3 backend without performing
/// network I/O (#437). The SDK lazily connects on the first
/// `GetObject` call, so a successful `build_cache` proves the
/// resolver-to-runtime conversion (`s3_origin_config_from_resolved`)
/// runs end-to-end and that the SDK's `ClientBuilder::build` doesn't
/// surface its `BehaviorVersion`-missing runtime error for either
/// credential variant. The integration suite at
/// `crates/cache/tests/s3_origin.rs` exercises the wire path via
/// `aws-smithy-mocks`.
#[tokio::test]
async fn build_cache_constructs_s3_origin_for_default_chain() {
    use decdn_common::config::{ResolvedS3Config, ResolvedS3Credentials};

    let s3 = ResolvedS3Config {
        bucket: "decdn-blobs".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: None,
        path_style: false,
        prefix: String::new(),
        credentials: Some(ResolvedS3Credentials::DefaultChain { profile: None }),
        decompress: decdn_cache::DecompressMode::Auto,
    };
    let (_tmp, cfg) = cfg_with_origin(Some(ResolvedOrigin::S3(s3)));
    let metrics_handle = Arc::new(metrics::Metrics::new());

    // Construction must succeed end-to-end. A failure here means the
    // resolver-to-runtime conversion regressed or the SDK's lazy-
    // connect contract changed (and we'd be doing I/O at startup).
    let _engine = build_cache(&cfg, metrics_handle, None)
        .await
        .expect("S3 origin must construct without I/O");
}

/// Same as above but with the `Static` credential path so the
/// `SecretString::expose` unwrap arm in `s3_origin_config_from_resolved`
/// is exercised. The integration tests use `mock_client!` which doesn't
/// route through the resolved-config layer at all, so this is the only
/// place the conversion gets covered.
#[tokio::test]
async fn build_cache_constructs_s3_origin_for_static_credentials() {
    use decdn_common::config::secret::SecretString;
    use decdn_common::config::{ResolvedS3Config, ResolvedS3Credentials};

    let s3 = ResolvedS3Config {
        bucket: "decdn-blobs".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: None,
        path_style: false,
        prefix: "blobs/".to_string(),
        credentials: Some(ResolvedS3Credentials::Static {
            access_key_id: SecretString::new("AKIA-test-fake"),
            secret_access_key: SecretString::new("secret-fake"),
            session_token: None,
        }),
        decompress: decdn_cache::DecompressMode::Auto,
    };
    let (_tmp, cfg) = cfg_with_origin(Some(ResolvedOrigin::S3(s3)));
    let metrics_handle = Arc::new(metrics::Metrics::new());

    let _engine = build_cache(&cfg, metrics_handle, None)
        .await
        .expect("S3 origin with static credentials must construct without I/O");
}

/// `build_cache` hands the cache engine the own-origin range-pull read
/// budget from `cache.node_pull_stall_window_sec` and
/// `cache.node_pull_min_throughput_bps` (ADR 037), and a zero throughput
/// floor leaves those reads unbounded.
#[tokio::test]
async fn build_cache_wires_the_origin_read_budget() {
    let (_tmp, mut cfg) = cfg_with_origin(None);
    cfg.cache.node_pull_stall_window_sec = 7;
    cfg.cache.node_pull_min_throughput_bps = 12_345;
    let cache = build_cache(&cfg, Arc::new(metrics::Metrics::new()), None)
        .await
        .expect("cache must construct");
    assert_eq!(
        cache.origin_read_budget_parts(),
        Some((std::time::Duration::from_secs(7), 12_345))
    );

    let (_tmp, mut cfg) = cfg_with_origin(None);
    cfg.cache.node_pull_min_throughput_bps = 0;
    let cache = build_cache(&cfg, Arc::new(metrics::Metrics::new()), None)
        .await
        .expect("cache must construct");
    assert_eq!(cache.origin_read_budget_parts(), None);
}

/// ADR 041 estimator decoupling: `cache.serve_economics.policy = "margin"`
/// must build and wire the shared frequency estimator even when neither
/// `cache.eviction_policy` nor `cache.admission_policy` is `"tinylfu"` —
/// the `margin` policy's `n_hat` buy-ceiling input needs a heat signal
/// independent of which cache policy is selected. Exercises the real
/// production wiring function (`wire_cache_policies`), not a re-derivation
/// of its condition.
#[tokio::test]
async fn margin_policy_builds_estimator_without_tinylfu_cache_policy() {
    let (_tmp, mut cfg) = cfg_with_origin(None);
    assert_eq!(
        cfg.cache.eviction_policy, "lru",
        "test assumes the lru default"
    );
    assert_eq!(
        cfg.cache.admission_policy, "always",
        "test assumes the always-admit default"
    );
    cfg.cache.serve_economics.policy = "margin".to_string();

    let metrics_handle = Arc::new(metrics::Metrics::new());
    let cache = build_cache(&cfg, metrics_handle, None)
        .await
        .expect("cache must construct");
    assert!(
        !cache.has_frequency_estimator(),
        "no estimator before wiring runs"
    );

    let (_eviction_policy, estimator) = wire_cache_policies(&cfg.cache, &cache);

    assert!(
        estimator.is_some(),
        "margin serve-economics policy must build a frequency estimator even under \
         lru eviction / always admission"
    );
    assert!(
        cache.has_frequency_estimator(),
        "the built estimator must be installed on the cache engine"
    );
}

/// The conversion helper unwraps `SecretString` via `.expose()`.
/// Verifying the cleartext bytes survive the conversion — without
/// this, a refactor that replaces `.expose()` with a placeholder
/// would silently break `SigV4` signing at runtime. Direct unit
/// test on the conversion function avoids the SDK round-trip.
///
/// Also pins `region` and `endpoint_url` field-equivalence between
/// the resolved form and the runtime form. The conversion uses
/// field access (not destructuring), so a new field added to one
/// side and forgotten on the other wouldn't be caught at compile
/// time — this assertion is the safety net.
#[test]
fn s3_origin_config_from_resolved_preserves_static_credentials() {
    use decdn_common::config::secret::SecretString;
    use decdn_common::config::{ResolvedS3Config, ResolvedS3Credentials};

    let endpoint =
        decdn_cache::parse_origin_url("https://r2.example/").expect("test URL must parse");
    let resolved = ResolvedS3Config {
        bucket: "b".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: Some(endpoint),
        path_style: true,
        prefix: "blobs/".to_string(),
        credentials: Some(ResolvedS3Credentials::Static {
            access_key_id: SecretString::new("ak-1"),
            secret_access_key: SecretString::new("sk-1"),
            session_token: Some(SecretString::new("tok-1")),
        }),
        decompress: decdn_cache::DecompressMode::Auto,
    };
    let runtime = s3_origin_config_from_resolved(&resolved);
    assert_eq!(runtime.bucket, "b");
    assert_eq!(runtime.region, "us-east-1");
    // OriginUrl doesn't implement PartialEq; compare via Display.
    assert_eq!(
        runtime.endpoint_url.as_ref().map(ToString::to_string),
        Some("https://r2.example/".to_string()),
    );
    assert!(runtime.path_style);
    assert_eq!(runtime.prefix, "blobs/");
    match runtime.credentials.expect("static creds preserved") {
        S3Credentials::Static {
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            assert_eq!(access_key_id, "ak-1");
            assert_eq!(secret_access_key, "sk-1");
            assert_eq!(session_token.as_deref(), Some("tok-1"));
        }
        S3Credentials::DefaultChain { .. } => panic!("expected Static after conversion"),
    }
}

/// Sibling of the Static-credentials round-trip: pins the
/// `DefaultChain` arm of `s3_origin_config_from_resolved` along
/// with `endpoint_url: None` and `path_style: false` (the
/// virtual-hosted-style AWS / R2 default). Without this test the
/// `DefaultChain { profile }` -> `DefaultChain { profile }` arm
/// has no direct coverage; the construction tests above call
/// `build_cache` but only assert it returns `Ok`, not that
/// `profile` survived the conversion.
#[test]
fn s3_origin_config_from_resolved_preserves_default_chain() {
    use decdn_common::config::{ResolvedS3Config, ResolvedS3Credentials};

    let resolved = ResolvedS3Config {
        bucket: "b".to_string(),
        region: "eu-west-1".to_string(),
        endpoint_url: None,
        path_style: false,
        prefix: String::new(),
        credentials: Some(ResolvedS3Credentials::DefaultChain {
            profile: Some("decdn-prod".to_string()),
        }),
        decompress: decdn_cache::DecompressMode::Auto,
    };
    let runtime = s3_origin_config_from_resolved(&resolved);
    assert_eq!(runtime.region, "eu-west-1");
    assert!(runtime.endpoint_url.is_none());
    assert!(!runtime.path_style);
    assert!(runtime.prefix.is_empty());
    match runtime.credentials.expect("default-chain creds preserved") {
        S3Credentials::DefaultChain { profile } => {
            assert_eq!(profile.as_deref(), Some("decdn-prod"));
        }
        S3Credentials::Static { .. } => panic!("expected DefaultChain after conversion"),
    }
}

// Operators grep `signal=SIGINT` / `signal=SIGTERM` / `signal=admin-drain`
// in the structured "shutdown signal received" log line; a rename here
// would silently break dashboards and runbooks.
#[test]
fn shutdown_signal_display_is_stable() {
    assert_eq!(ShutdownSignal::Sigint.to_string(), "SIGINT");
    #[cfg(unix)]
    assert_eq!(ShutdownSignal::Sigterm.to_string(), "SIGTERM");
    assert_eq!(ShutdownSignal::AdminDrain.to_string(), "admin-drain");
}

/// A `spawn_periodic` task exits promptly when its stop signal fires, even
/// when the next tick is far away. This is the shared shutdown contract for
/// every periodic runner (dispatch / region / record-store / DHT / probe
/// GC): without the `biased; stop-before-tick` select, a regression that
/// dropped the stop arm or polled it after `ticker.tick()` would silently
/// extend `SHUTDOWN_DEADLINE` by up to one full tick interval (60s at
/// defaults). Also pins the burned first tick — the body must not run before
/// the first interval elapses.
#[tokio::test]
async fn spawn_periodic_exits_promptly_on_shutdown() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let ticked = Arc::new(AtomicUsize::new(0));
    let mut tasks: JoinSet<()> = JoinSet::new();
    // 60s interval mirrors the runtime default: on a regression that polled
    // the ticker before the stop signal the task would hang for 60s, so the
    // timeout below catches the real bug rather than an unrelated
    // short-interval race.
    let stop_tx = {
        let ticked = Arc::clone(&ticked);
        spawn_periodic(&mut tasks, "test", Duration::from_mins(1), move || {
            ticked.fetch_add(1, Ordering::Relaxed);
        })
    };

    // Give the task a moment to enter the select loop (past the burned first
    // tick), then signal shutdown; it should exit well within the timeout,
    // with generous headroom for slow CI runners.
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop_tx.send(()).expect("receiver still alive");

    let joined = tokio::time::timeout(Duration::from_millis(500), tasks.join_next()).await;
    assert_matches!(
        joined,
        Ok(Some(Ok(()))),
        "spawn_periodic must exit within 500ms of shutdown signal; a 60s hang \
         here means the stop arm of the select was lost"
    );
    assert_eq!(
        ticked.load(Ordering::Relaxed),
        0,
        "tick body ran even though the first tick is burned and the interval never elapsed"
    );
}

/// `spawn_periodic` actually runs its tick body on each interval — the
/// complement to the burned-first-tick assertion above. A regression that
/// broke the `ticker.tick() => tick()` arm (or burned every tick) would
/// leave all five production GC/log tasks silently dead, and no shutdown
/// test would catch it.
#[tokio::test]
async fn spawn_periodic_fires_the_tick_body() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let ticked = Arc::new(AtomicUsize::new(0));
    let mut tasks: JoinSet<()> = JoinSet::new();
    // Short interval so the first (post-burn) tick lands fast; the stop
    // sender is held for the task's lifetime so it is not cancelled early.
    let _stop_tx = {
        let ticked = Arc::clone(&ticked);
        spawn_periodic(&mut tasks, "test", Duration::from_millis(10), move || {
            ticked.fetch_add(1, Ordering::Relaxed);
        })
    };

    // Poll for at least one tick with generous headroom for slow CI runners,
    // rather than sleeping a fixed duration that could race the first tick.
    tokio::time::timeout(Duration::from_secs(2), async {
        while ticked.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("spawn_periodic never ran its tick body within 2s");
}

/// Mount a JSON-RPC `200 OK` POST handler. The mount lives on
/// `server` until the next `server.reset().await`; callers flip
/// state by resetting and mounting `mount_unhealthy` instead.
async fn mount_healthy(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(server)
        .await;
}

/// Mount a JSON-RPC `500` POST handler. Used to flip the watchdog
/// from healthy to unhealthy without tearing the listener down.
async fn mount_unhealthy(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(500))
        .mount(server)
        .await;
}

/// Poll `metrics.rpc_healthy_value()` until it equals `expected` or
/// the deadline expires. Returns the observed value either way so
/// failures show what we actually saw rather than just timing out.
async fn wait_for_gauge(metrics: &Arc<metrics::Metrics>, expected: i64) -> i64 {
    let deadline = std::time::Instant::now() + Duration::from_millis(1500);
    loop {
        let v = metrics.rpc_healthy_value();
        if v == expected || std::time::Instant::now() >= deadline {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "current_thread", start_paused = false)]
async fn rpc_watchdog_tracks_endpoint_transitions() {
    // Healthy -> unhealthy -> healthy transitions, all observed via
    // the `rpc_healthy` gauge. Tight 100ms tick keeps the test under
    // 5s wall-clock while still exercising multiple poll cycles.
    let server = MockServer::start().await;
    mount_healthy(&server).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let metrics = Arc::new(metrics::Metrics::new());
    // Mirror `run()`: the caller seeds the gauge from the startup
    // probe, so the watchdog can assume `prev_healthy = true` without
    // emitting a spurious "recovered" log on the first tick.
    metrics.rpc_healthy(true);
    let (tx, rx) = oneshot::channel::<()>();
    let handle = spawn_rpc_watchdog(
        client,
        server.uri(),
        Duration::from_millis(100),
        Arc::clone(&metrics),
        rx,
    );

    // Caller-seeded above; the wait still confirms the loop is
    // running and has observed at least one healthy probe.
    assert_eq!(wait_for_gauge(&metrics, 1).await, 1, "should be healthy");

    // Flip to unhealthy. wiremock's last-mounted response wins for
    // matching POSTs, so the next probe sees a 500.
    server.reset().await;
    mount_unhealthy(&server).await;
    assert_eq!(
        wait_for_gauge(&metrics, 0).await,
        0,
        "should detect unhealthy",
    );

    // Bring it back. Watchdog should recover within a few ticks.
    server.reset().await;
    mount_healthy(&server).await;
    assert_eq!(
        wait_for_gauge(&metrics, 1).await,
        1,
        "should detect recovery",
    );

    // Clean shutdown.
    let _ = tx.send(());
    let join_res = tokio::time::timeout(Duration::from_secs(2), handle).await;
    assert!(join_res.is_ok(), "watchdog should exit on shutdown signal");
}

/// The preflight classifier maps a probe response to the right retry verdict
/// (#1106/#1108): 2xx = healthy, 429/5xx = transient (retryable), other 4xx =
/// fatal (retrying won't help). A misclassification would either crash-loop a
/// node on a transient startup 429 or retry a genuine misconfig to budget.
#[tokio::test]
async fn preflight_classifier_maps_statuses_to_verdicts() {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("build client");
    for (status, want) in [
        (200u16, "healthy"),
        (429, "transient"),
        (500, "transient"),
        (503, "transient"),
        (400, "fatal"),
        (404, "fatal"),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status).set_body_string("{}"))
            .mount(&server)
            .await;
        let got = match probe_rpc_classified(&client, &server.uri()).await {
            RpcProbe::Healthy => "healthy",
            RpcProbe::Transient(_) => "transient",
            RpcProbe::Fatal(_) => "fatal",
        };
        assert_eq!(got, want, "status {status} misclassified");
    }
}

/// A refused connection reaches the probe verdict with its failure class and
/// without the `rpc_url` secret. Port 1 on loopback refuses at once, so this
/// needs no network and pins the real reqwest error chain, whose class sits
/// in `source()` beneath a top layer that names the URL.
#[tokio::test]
async fn preflight_transient_keeps_the_class_and_drops_the_url() {
    let client = preflight_client();
    let probe =
        probe_rpc_classified(&client, "http://127.0.0.1:1/v3/SECRETKEY?apikey=SECRETKEY").await;
    let RpcProbe::Transient(msg) = probe else {
        panic!("a refused connection is transient");
    };
    assert!(!msg.contains("SECRETKEY"), "leaked the rpc_url: {msg}");
    assert!(
        msg.to_lowercase().contains("connect"),
        "lost the failure class: {msg}"
    );
}

/// Returns 429 for the first `fail_first` calls, then 200 — models a
/// rate-limited endpoint recovering after the startup burst.
struct FlakyThenHealthy {
    calls: std::sync::atomic::AtomicUsize,
    fail_first: usize,
}

impl wiremock::Respond for FlakyThenHealthy {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n < self.fail_first {
            ResponseTemplate::new(429)
        } else {
            ResponseTemplate::new(200).set_body_string("{}")
        }
    }
}

/// Build the 5s-timeout preflight client the production path uses.
fn preflight_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client")
}

/// A transient (429) preflight is retried with backoff and succeeds once the
/// endpoint recovers — the #1108 startup-burst-429 case, where a transient 429
/// must not exit the process. Drives `preflight_retry` with a 1ms backoff so the
/// real HTTP path
/// to wiremock runs unpaused but the retries stay fast.
#[tokio::test]
async fn preflight_retries_transient_then_succeeds() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(FlakyThenHealthy {
            calls: std::sync::atomic::AtomicUsize::new(0),
            fail_first: 2,
        })
        .mount(&server)
        .await;
    preflight_retry(&preflight_client(), &server.uri(), Duration::from_millis(1))
        .await
        .expect("preflight should recover after two transient 429s");
}

/// A persistent transient failure aborts bring-up after the retry budget — a
/// genuinely-throttled/dead endpoint is not silently tolerated forever.
#[tokio::test]
async fn preflight_exhausts_budget_on_persistent_transient() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&server)
        .await;
    let err = preflight_retry(&preflight_client(), &server.uri(), Duration::from_millis(1))
        .await
        .expect_err("persistent 429 must fail after the retry budget");
    assert!(
        err.to_string().contains("attempts"),
        "error should report the exhausted retry budget: {err}"
    );
}

/// A fatal (non-429 4xx) preflight aborts immediately — retrying a bad
/// path/auth won't help, so fail fast with a clear message.
#[tokio::test]
async fn preflight_fatal_aborts_immediately() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;
    let err = preflight_retry(&preflight_client(), &server.uri(), Duration::from_millis(1))
        .await
        .expect_err("a fatal 4xx must abort bring-up");
    assert!(
        err.to_string().contains("unexpected status"),
        "error should surface the fatal status: {err}"
    );
}

/// Smoke test for the post-fixup `ShutdownStreams::recv` contract:
/// a real SIGTERM raised from inside the test process must resolve
/// the future to `ShutdownSignal::Sigterm`. nextest runs each test
/// in its own process so the signal cannot leak across tests.
/// `nix::sys::signal::raise` keeps the workspace `unsafe_code =
/// "forbid"` lint clean.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_streams_recv_resolves_on_sigterm() {
    use std::time::Duration;

    use nix::sys::signal::{Signal, raise};

    let mut streams = ShutdownStreams::install();
    // Spawn the raise on a separate task so `recv()` is awaiting
    // on the SIGTERM stream by the time the signal arrives. The
    // small sleep gives `install()` a chance to register tokio's
    // handler — without it the kernel could deliver SIGTERM with
    // the default disposition (terminate the process) before the
    // tokio handler is in place. 20ms is far longer than the
    // install path needs.
    tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        raise(Signal::SIGTERM).expect("raise SIGTERM");
    });
    let signal = tokio::time::timeout(Duration::from_millis(500), streams.recv())
        .await
        .expect("ShutdownStreams::recv did not resolve within 500ms of SIGTERM");
    assert_matches!(signal, ShutdownSignal::Sigterm);
}

/// Guard test for ADR 005 transport defaults. Catches accidental edits to
/// the constants and verifies `quic_transport_config()` builds without
/// error — the integration test in `tests/probe_loopback.rs` rebuilds
/// the config on its own to shorten the idle window, so without this
/// assertion a regression that changes the defaults (or removes the
/// helper's call site) would not be caught.
#[test]
fn adr_005_transport_defaults() {
    assert_eq!(QUIC_MAX_IDLE_TIMEOUT, Duration::from_secs(30));
    assert_eq!(QUIC_KEEP_ALIVE_INTERVAL, Duration::from_secs(10));
    assert_eq!(QUIC_MAX_CONCURRENT_BIDI_STREAMS, 100);
    quic_transport_config().expect("quic_transport_config builds");
}

#[test]
fn parse_relay_urls_reports_offending_entry() {
    let ok = parse_relay_urls(&[
        "https://relay-a.example".to_string(),
        "https://relay-b.example".to_string(),
    ])
    .expect("valid relay URLs parse");
    assert_eq!(ok.len(), 2);

    let err =
        parse_relay_urls(&["not a url".to_string()]).expect_err("invalid relay URL is rejected");
    assert!(
        err.to_string().contains("not a url"),
        "error should name the offending entry: {err}"
    );
}

#[test]
fn parse_relay_urls_error_redacts_userinfo() {
    // Malformed URLs that still carry credentials must not leak them — both
    // the well-formed-authority shape and a password containing a literal
    // `@` (which a naive first-`@` split would leak).
    for entry in [
        "https://user:s3cret@host:notaport",
        "https://user:p@s3cret@host:notaport",
    ] {
        let err = parse_relay_urls(&[entry.to_string()]).expect_err("malformed URL is rejected");
        let msg = err.to_string();
        assert!(
            !msg.contains("s3cret"),
            "credentials leaked for {entry:?}: {msg}"
        );
        assert!(
            !msg.contains("user:"),
            "userinfo leaked for {entry:?}: {msg}"
        );
        assert!(
            msg.contains("***@host"),
            "host should still appear for {entry:?}: {msg}"
        );
    }
}

#[tokio::test]
async fn report_home_relay_false_when_relay_never_connects() {
    // A closed port stands in for a dead relay: iroh's relay client never
    // connects, so the report times out and warns.
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_port = dead.local_addr().unwrap().port();
    drop(dead);

    let sk = SecretKey::generate();
    let transport = quic_transport_config().unwrap();
    let relays = vec![format!("http://127.0.0.1:{dead_port}")];
    let ep = build_endpoint(&sk, 0, &relays, &ResolvedDiscovery::default(), transport)
        .await
        .expect("endpoint binds with an unreachable relay");
    assert!(!report_home_relay(ep.home_relay_status(), Duration::from_millis(500)).await);
    ep.close().await;
}

/// Bind an `IPV6_V6ONLY` UDP socket on `[::]:port`, or return `None` when
/// the host has no usable IPv6 stack.
fn bind_v6_only_udp(port: u16) -> Option<socket2::Socket> {
    let sock = socket2::Socket::new(
        socket2::Domain::IPV6,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .ok()?;
    sock.set_only_v6(true).ok()?;
    let addr = SocketAddr::from(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, port, 0, 0));
    sock.bind(&addr.into()).ok()?;
    Some(sock)
}

/// A port that is free on `0.0.0.0` and, when the host has IPv6, on `[::]`.
fn free_dual_stack_port() -> u16 {
    for _ in 0..32 {
        let v4 = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
        let port = v4.local_addr().unwrap().port();
        if bind_v6_only_udp(0).is_none() || bind_v6_only_udp(port).is_some() {
            return port;
        }
    }
    panic!("no port free on both address families");
}

#[tokio::test]
async fn build_endpoint_binds_ipv6_on_bind_port() {
    let port = free_dual_stack_port();
    let has_v6 = bind_v6_only_udp(0).is_some();
    let sk = SecretKey::generate();
    let transport = quic_transport_config().unwrap();
    let ep = build_endpoint(&sk, port, &[], &ResolvedDiscovery::default(), transport)
        .await
        .expect("endpoint binds on both families");
    let bound = ep.bound_sockets();
    assert!(
        bound.iter().any(|a| a.is_ipv4() && a.port() == port),
        "no IPv4 socket on {port}: {bound:?}"
    );
    if has_v6 {
        // One IPv6 socket, on the configured port — not iroh's random default.
        let v6: Vec<_> = bound.iter().filter(|a| a.is_ipv6()).collect();
        assert_eq!(v6.len(), 1, "expected one IPv6 socket: {bound:?}");
        assert_eq!(v6.first().map(|a| a.port()), Some(port), "{bound:?}");
    }
    ep.close().await;
}

#[tokio::test]
async fn build_endpoint_falls_back_to_ipv4_when_ipv6_bind_fails() {
    // Holding `[::]:port` makes the endpoint's IPv6 bind fail the same way
    // a host without IPv6 does. The node must still start on IPv4.
    // Without an IPv6 stack every IPv6 bind fails on its own; port 0 then
    // stands in for the configured port.
    let (port, _v6_holder) = if bind_v6_only_udp(0).is_none() {
        (0, None)
    } else {
        let (port, v6) = (0..32)
            .find_map(|_| {
                let v6 = bind_v6_only_udp(0)?;
                let port = v6.local_addr().ok()?.as_socket()?.port();
                std::net::UdpSocket::bind(("0.0.0.0", port)).ok()?;
                Some((port, v6))
            })
            .expect("a port free on IPv4 whose IPv6 twin we hold");
        (port, Some(v6))
    };
    let sk = SecretKey::generate();
    let transport = quic_transport_config().unwrap();
    let ep = build_endpoint(&sk, port, &[], &ResolvedDiscovery::default(), transport)
        .await
        .expect("an IPv6 bind failure does not stop the node");
    let bound = ep.bound_sockets();
    assert!(bound.iter().all(SocketAddr::is_ipv4), "{bound:?}");
    assert!(
        port == 0 || bound.iter().any(|a| a.port() == port),
        "no IPv4 socket on {port}: {bound:?}"
    );
    ep.close().await;
}

#[test]
fn warn_if_no_ipv6_socket_reports_ipv6_presence() {
    let bind_v6 = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 4433, 0, 0);
    let v4 = SocketAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 4433));
    assert!(!warn_if_no_ipv6_socket(&[], bind_v6));
    assert!(!warn_if_no_ipv6_socket(&[v4], bind_v6));
    assert!(warn_if_no_ipv6_socket(
        &[v4, SocketAddr::from(bind_v6)],
        bind_v6
    ));
}

#[tokio::test]
async fn build_endpoint_binds_when_all_relays_unreachable() {
    // A non-empty, all-unreachable relay set is advisory only: bring-up
    // returns at once and the background relay report warns later, so a
    // transient relay outage can't wedge node startup.
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_port = dead.local_addr().unwrap().port();
    drop(dead);

    let sk = SecretKey::generate();
    let transport = quic_transport_config().unwrap();
    let relays = vec![format!("http://127.0.0.1:{dead_port}")];
    let ep = build_endpoint(&sk, 0, &relays, &ResolvedDiscovery::default(), transport)
        .await
        .expect("all-unreachable relay set proceeds with a warning");
    ep.close().await;
}

#[tokio::test]
async fn build_endpoint_binds_with_portless_relay_url() {
    // A relay URL with no explicit or scheme-default port is one iroh
    // accepts, so bring-up binds rather than hard-failing a config iroh
    // would have routed.
    let sk = SecretKey::generate();
    let transport = quic_transport_config().unwrap();
    let relays = vec!["relay://portless-a".to_string()];
    let ep = build_endpoint(&sk, 0, &relays, &ResolvedDiscovery::default(), transport)
        .await
        .expect("portless relay URL binds");
    ep.close().await;
}

#[tokio::test]
async fn build_endpoint_uses_n0_when_discovery_empty() {
    // Empty discovery composes the n0 pkarr publisher and DNS lookup. The
    // lookup legs aren't introspectable, so assert bring-up binds cleanly.
    let sk = SecretKey::generate();
    let transport = quic_transport_config().unwrap();
    let ep = build_endpoint(&sk, 0, &[], &ResolvedDiscovery::default(), transport)
        .await
        .expect("n0 endpoint binds with empty discovery");
    ep.close().await;
}

#[tokio::test]
async fn build_endpoint_binds_with_custom_discovery() {
    // Custom pkarr+DNS plus a static peer drop the build onto
    // `presets::Minimal` and compose the configured address-lookup legs; with
    // no relay map the n0 relay default is restored. Exercises
    // `add_discovery_lookups` end to end (publish/resolve are background/lazy,
    // so binding does not require reaching the configured infra).
    let sk = SecretKey::generate();
    let transport = quic_transport_config().unwrap();
    let peer_id = SecretKey::generate().public().to_string();
    let discovery = ResolvedDiscovery {
        pkarr_url: Some("https://pkarr.example/".to_string()),
        dns_origin: Some("discovery.example.".to_string()),
        peers: vec![decdn_common::config::ResolvedDiscoveryPeer {
            node_id: peer_id,
            relay_url: Some("https://relay.example/".to_string()),
            addrs: vec!["203.0.113.4:4433".to_string()],
        }],
    };
    let ep = build_endpoint(&sk, 0, &[], &discovery, transport)
        .await
        .expect("custom-discovery endpoint binds");
    ep.close().await;
}

#[tokio::test]
async fn build_endpoint_binds_with_discovery_and_custom_relay() {
    // Discovery and a reachable custom relay together: both legs wire onto the
    // Minimal base (custom relay map overrides the restored n0 default).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_port = listener.local_addr().unwrap().port();
    let sk = SecretKey::generate();
    let transport = quic_transport_config().unwrap();
    let relays = vec![format!("http://127.0.0.1:{up_port}")];
    let discovery = ResolvedDiscovery {
        pkarr_url: None,
        dns_origin: Some("discovery.example.".to_string()),
        peers: Vec::new(),
    };
    let ep = build_endpoint(&sk, 0, &relays, &discovery, transport)
        .await
        .expect("discovery + custom relay endpoint binds");
    ep.close().await;
}

/// The runtime hands `blockchain.get_logs_max_block_span` to the poller:
/// dropping the setter would silently ignore the knob.
#[tokio::test]
async fn chain_poller_builder_uses_the_configured_get_logs_span() {
    struct NoHead;
    #[async_trait::async_trait]
    impl HeadSource for NoHead {
        async fn head(&self) -> anyhow::Result<u64> {
            Ok(0)
        }
    }
    let (_tmp, mut cfg) = cfg_with_origin(None);
    cfg.blockchain.get_logs_max_block_span = 150;
    let node_metrics = Arc::new(metrics::Metrics::new());
    let builder = chain_poller_builder(
        Arc::new(NoHead),
        Duration::from_secs(7),
        &cfg.blockchain,
        &node_metrics,
    );
    assert_eq!(builder.span_ceiling(), 150);

    // The hooks land in the exported series: `run` reports the starting
    // span, and a range rejection and a window retry bump their counters.
    let built = builder.build();
    assert!(built.is_ok(), "an empty poller builds");
    let Ok(poller) = built else { return };
    poller.fire_range_rejection_for_test();
    poller.fire_window_retry_for_test();
    poller.fire_window_deferred_for_test();
    let provider = alloy::providers::ProviderBuilder::new()
        .connect_mocked_client(alloy::providers::mock::Asserter::new());
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    crate::chain_events::multiplexed_poller::run(provider, poller, shutdown).await;
    let scrape = node_metrics.encode().unwrap_or_default();
    assert!(
        scrape.lines().any(|l| l == "decdn_chain_get_logs_span 150"),
        "span gauge not wired: {scrape}"
    );
    assert!(
        scrape
            .lines()
            .any(|l| l == "decdn_chain_get_logs_range_rejections_total 1"),
        "rejection counter not wired"
    );
    assert!(
        scrape
            .lines()
            .any(|l| l == "decdn_chain_get_logs_retries_total 1"),
        "window-retry counter not wired"
    );
    assert!(
        scrape
            .lines()
            .any(|l| l == "decdn_chain_get_logs_deferred_total 1"),
        "window-deferred counter not wired"
    );
}

/// Bind a loopback endpoint with relays off, returning it and a dialable
/// address.
async fn loopback_endpoint(alpns: Vec<Vec<u8>>) -> (Endpoint, iroh::EndpointAddr) {
    let ep = Endpoint::builder(presets::Minimal)
        .alpns(alpns)
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::LOCALHOST,
            0,
        ))
        .expect("loopback bind address")
        .bind()
        .await
        .expect("bind loopback endpoint");
    let socket = *ep
        .bound_sockets()
        .iter()
        .find(|a| a.is_ipv4())
        .expect("an IPv4 bound socket");
    let addr = iroh::EndpointAddr::new(ep.id()).with_ip_addr(socket);
    (ep, addr)
}

/// A router whose endpoint holds one connection to a peer that keeps it open.
/// With `strand`, the connection is dialled on a throwaway runtime that is then
/// dropped, taking the connection's QUIC driver with it: the shape a pull leg
/// that dials on its own runtime leaves behind (#2185).
async fn router_holding_a_connection(
    strand: bool,
) -> (Router, iroh::endpoint::Connection, Endpoint) {
    const ALPN: &[u8] = b"cdn/close-router-test/v1";
    let (peer, peer_addr) = loopback_endpoint(vec![ALPN.to_vec()]).await;
    let accept_peer = peer.clone();
    tokio::spawn(async move {
        if let Some(incoming) = accept_peer.accept().await
            && let Ok(connecting) = incoming.accept()
        {
            let _held = connecting.await;
            std::future::pending::<()>().await;
        }
    });
    let (ep, _) = loopback_endpoint(Vec::new()).await;
    let conn = if strand {
        let dialer = ep.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build throwaway runtime");
            let conn = rt
                .block_on(dialer.connect(peer_addr, ALPN))
                .expect("dial on the throwaway runtime");
            drop(rt);
            conn
        })
        .join()
        .expect("dialling thread")
    } else {
        ep.connect(peer_addr, ALPN)
            .await
            .expect("dial on the test runtime")
    };
    (Router::builder(ep).spawn(), conn, peer)
}

/// A router whose endpoint holds a connection with no driver never finishes
/// its shutdown on its own; `close_router` gives up at the deadline and
/// reports it, so node teardown goes on to redeem and flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_router_gives_up_on_a_stranded_connection_at_its_deadline() {
    let (router, _conn, _peer) = router_holding_a_connection(true).await;
    let deadline = Duration::from_millis(500);
    let started = std::time::Instant::now();
    assert!(
        !close_router(&router, deadline).await,
        "a close stuck on a stranded connection must be reported as unfinished"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the close must return near its deadline, took {:?}",
        started.elapsed()
    );
}

/// The control: the same router with the connection's driver still running
/// finishes inside a generous deadline. Without it, the test above would pass
/// for any setup that stalls the close, stranded driver or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_router_finishes_when_every_connection_drains() {
    let (router, _conn, _peer) = router_holding_a_connection(false).await;
    assert!(
        close_router(&router, Duration::from_secs(10)).await,
        "a close with a driven connection must finish inside the deadline"
    );
}

// ---- deployment preflight (the guard for the irreversible lane-store drop) ----

/// The deployment every preflight test configures.
const PREFLIGHT_DEPLOYMENT: crate::pool_store::Deployment = crate::pool_store::Deployment {
    chain_id: 421_614,
    payment_pool: alloy::primitives::Address::repeat_byte(0x77),
};

/// One queued answer for [`preflight_provider`]. The value is JSON, not
/// raw bytes (unlike `buyer_pool.rs`' `MockCall`), because
/// `eth_chainId` answers with a quantity.
enum MockAnswer {
    /// Answer the call with this value.
    Ok(serde_json::Value),
    /// Fault the call as a transient RPC error does, so the preflight
    /// retries it.
    TransientError,
    /// Fault the call as a node answers an `eth_call` that reverts: JSON-RPC
    /// error code 3, "execution reverted".
    Revert,
}

/// A mocked provider answering the preflight's reads in order:
/// `eth_chainId`, `eth_getCode`, then (when reached) the `getRateBounds()`
/// call.
fn preflight_provider(answers: Vec<MockAnswer>) -> impl Provider + Clone {
    let asserter = alloy::providers::mock::Asserter::new();
    for answer in answers {
        match answer {
            MockAnswer::Ok(value) => asserter.push_success(&value),
            MockAnswer::TransientError => asserter.push_failure_msg("transient rpc fault"),
            MockAnswer::Revert => asserter.push_failure(
                serde_json::from_value(serde_json::json!({
                    "code": 3,
                    "message": "execution reverted",
                    "data": "0x",
                }))
                .expect("a code-3 error payload must deserialize"),
            ),
        }
    }
    ProviderBuilder::new().connect_mocked_client(asserter)
}

/// `eth_chainId` answering the configured chain.
fn chain_id_ok() -> serde_json::Value {
    serde_json::json!(alloy::primitives::U64::from(421_614u64))
}

/// `eth_getCode` answering one byte of code (`0x60`, `PUSH1`): the
/// preflight checks only that code is present, never what it is.
fn code_present() -> serde_json::Value {
    serde_json::json!(alloy::primitives::Bytes::from(vec![0x60]))
}

/// `getRateBounds()` answering a per-MB rate floor, as every `PaymentPool`
/// does.
fn rate_bounds_answers() -> serde_json::Value {
    use alloy::sol_types::SolValue;
    let floor = alloy::primitives::U256::from(10u64);
    serde_json::json!(alloy::primitives::Bytes::from(floor.abi_encode()))
}

/// An empty-bytes answer: no code at the address for `eth_getCode`, or no
/// return data for an `eth_call`.
fn empty_bytes() -> serde_json::Value {
    serde_json::json!(alloy::primitives::Bytes::default())
}

fn preflight_retry_budget() -> BootRetry {
    BootRetry::new(
        DEPLOYMENT_PREFLIGHT_BUDGET,
        Arc::new(crate::metrics::Metrics::new()),
    )
}

/// The healthy path: matching chain id, code present, `getRateBounds()`
/// answers.
#[tokio::test]
async fn a_matching_deployment_passes_the_preflight() {
    let provider = preflight_provider(vec![
        MockAnswer::Ok(chain_id_ok()),
        MockAnswer::Ok(code_present()),
        MockAnswer::Ok(rate_bounds_answers()),
    ]);
    check_deployment_preflight(provider, PREFLIGHT_DEPLOYMENT, &preflight_retry_budget())
        .await
        .expect("a matching deployment must pass");
}

/// A chain-id mismatch aborts at once with an error naming the config key
/// and the consequence — it must never be retried into the 60s budget.
#[tokio::test]
async fn a_chain_id_mismatch_aborts_the_preflight() {
    let provider = preflight_provider(vec![
        MockAnswer::Ok(serde_json::json!(alloy::primitives::U64::from(1u64))),
        MockAnswer::Ok(code_present()),
    ]);
    let err = check_deployment_preflight(
        provider,
        PREFLIGHT_DEPLOYMENT,
        &BootRetry::single_attempt(Arc::new(crate::metrics::Metrics::new())),
    )
    .await
    .expect_err("a mismatched chain id must abort");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("blockchain.chain_id") && msg.contains("rebind the lane store"),
        "the error must name the key and the consequence: {msg}"
    );
}

/// A codeless address aborts: nothing is deployed there.
#[tokio::test]
async fn a_codeless_payment_pool_address_aborts_the_preflight() {
    let provider = preflight_provider(vec![
        MockAnswer::Ok(chain_id_ok()),
        MockAnswer::Ok(empty_bytes()),
    ]);
    let err = check_deployment_preflight(
        provider,
        PREFLIGHT_DEPLOYMENT,
        &BootRetry::single_attempt(Arc::new(crate::metrics::Metrics::new())),
    )
    .await
    .expect_err("a codeless address must abort");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("has no code") && msg.contains("blockchain.payment_pool_address"),
        "the error must name the key: {msg}"
    );
}

/// A contract that does not answer `getRateBounds()` — a sibling address
/// pasted from the same deploy manifest — aborts as a permanent contract
/// error rather than passing on code presence alone. Both shapes of "no such
/// view" are covered: the revert a real node answers (JSON-RPC code 3) and
/// the empty return data of a contract with no code path for the selector.
/// Run on the full retry budget (`start_paused`, so any sleep is free) to
/// pin that each classifies PERMANENT: the "not retried" context proves the
/// budget was not spun on a deterministic misconfig.
#[tokio::test(start_paused = true)]
async fn a_non_payment_pool_contract_aborts_the_preflight() {
    for (shape, probe_answer) in [
        ("revert", MockAnswer::Revert),
        ("no data", MockAnswer::Ok(empty_bytes())),
    ] {
        let provider = preflight_provider(vec![
            MockAnswer::Ok(chain_id_ok()),
            MockAnswer::Ok(code_present()),
            probe_answer,
        ]);
        let err =
            check_deployment_preflight(provider, PREFLIGHT_DEPLOYMENT, &preflight_retry_budget())
                .await
                .expect_err("a non-PaymentPool target must abort");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("does not answer PaymentPool.getRateBounds()"),
            "{shape}: the error must say the contract identity check failed: {msg}"
        );
        assert!(
            msg.contains("not retried"),
            "{shape}: a wrong contract is deterministic and must not spin the retry \
             budget: {msg}"
        );
    }
}

/// A transient RPC fault is retried and the preflight then passes — a
/// rate-limited endpoint must not brick boot. `start_paused` makes the
/// retry backoff sleep cost no real time.
#[tokio::test(start_paused = true)]
async fn a_transient_rpc_error_is_retried_by_the_preflight() {
    let provider = preflight_provider(vec![
        MockAnswer::TransientError,
        MockAnswer::Ok(chain_id_ok()),
        MockAnswer::Ok(code_present()),
        MockAnswer::Ok(rate_bounds_answers()),
    ]);
    check_deployment_preflight(provider, PREFLIGHT_DEPLOYMENT, &preflight_retry_budget())
        .await
        .expect("a transient fault must be retried to success");
}
