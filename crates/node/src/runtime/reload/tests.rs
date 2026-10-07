use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::Mutex;

use super::*;
use decdn_common::cli::common::LogLevel;
use decdn_common::config::{
    ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedIdentity, ResolvedNetwork,
    ResolvedObservability, ResolvedPayment, ResolvedSecurity,
};

/// Build a no-op log-level setter that records the most recent level.
fn recording_setter() -> (LogLevelSetter, Arc<Mutex<Option<LogLevel>>>) {
    let last = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&last);
    let setter: LogLevelSetter = Box::new(move |lvl| {
        *captured.lock().unwrap() = Some(lvl);
        Ok(LogLevelApply::Installed)
    });
    (setter, last)
}

/// Minimal `ResolvedConfig` for seeding the reload state. Only the
/// fields the reload path reads are populated meaningfully.
#[allow(clippy::too_many_lines)] // exhaustive struct literal, not real complexity
fn seed_resolved(rate: u64, level: LogLevel) -> ResolvedConfig {
    ResolvedConfig {
        identity: ResolvedIdentity {
            data_dir: PathBuf::from("/tmp/decdn-test"),
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
        cache: ResolvedCache {
            cache_dir: PathBuf::from("/tmp/cache"),
            cache_size_mb: 1024,
            disk_headroom_mb: 8192,
            max_blob_size_mb: 128,
            max_rate_per_mb: 0,
            origins: Vec::new(),
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
            rate_per_mb: rate,
            credit_max: decdn_common::config::DEFAULT_CREDIT_MAX,
            frame_target_bytes: decdn_common::config::DEFAULT_FRAME_TARGET_BYTES,
            credit_ramp_divisor: decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
            voucher_commit_interval_ms: decdn_common::config::DEFAULT_VOUCHER_COMMIT_INTERVAL_MS,
        },
        observability: ResolvedObservability {
            log_level: level,
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
        receipts: decdn_common::config::ResolvedReceipts::default(),
        dht: decdn_common::config::ResolvedDht::default(),
        probe: decdn_common::config::ResolvedProbe::default(),
        content: decdn_common::config::ResolvedContent::default(),
    }
}

fn write_config(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("node.toml");
    std::fs::write(&path, body).unwrap();
    path
}

/// A reload whose file also changes the restart-required
/// `payment.rate_per_mb` still applies the reloadable sections (here
/// `log_level`) and succeeds. The restart-required *notice* is covered by
/// the SIGHUP integration test in `crates/node/tests/sighup_signal.rs`.
#[tokio::test]
async fn reload_applies_log_level_and_ignores_rate_change() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[payment]\nrate_per_mb = 99\n\n[observability]\nlog_level = \"debug\"\n",
    );

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );

    state.reload(&path).await.unwrap();

    assert_eq!(*captured.lock().unwrap(), Some(LogLevel::Debug));
}

#[tokio::test]
async fn reload_skips_log_level_when_unchanged() {
    // First reload always applies (cache starts as `None` to handle
    // a startup `RUST_LOG` override). The skip-when-unchanged
    // behaviour is observable on the *second* reload, when the
    // cached level matches the file.
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[payment]\nrate_per_mb = 5\n\n[observability]\nlog_level = \"info\"\n",
    );

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );

    // First reload applies (forces apply on `None` cache).
    state.reload(&path).await.unwrap();
    assert_eq!(*captured.lock().unwrap(), Some(LogLevel::Info));
    // Drop the captured value to detect a no-op on the second pass.
    *captured.lock().unwrap() = None;
    // Second reload sees the cached level and skips the setter.
    state.reload(&path).await.unwrap();
    assert!(captured.lock().unwrap().is_none());
}

/// A setter that keeps a `RUST_LOG` filter must not make the reload report
/// the file level as live: the snapshot stays `None`, and the next reload
/// asks the setter again rather than skipping it as unchanged.
#[tokio::test]
async fn kept_rust_log_filter_is_not_reported_as_the_live_level() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "[observability]\nlog_level = \"debug\"\n");
    let calls = Arc::new(Mutex::new(0_u32));
    let counted = Arc::clone(&calls);
    let setter: LogLevelSetter = Box::new(move |_| {
        *counted.lock().unwrap() += 1;
        Ok(LogLevelApply::KeptRustLog)
    });
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &seed_resolved(1, LogLevel::Info),
        setter,
    );

    state.reload(&path).await.unwrap();
    assert_eq!(state.current().log_level, None);
    state.reload(&path).await.unwrap();
    assert_eq!(state.current().log_level, None);
    assert_eq!(
        *calls.lock().unwrap(),
        2,
        "each reload asks the setter again"
    );
}

/// First-reload-applies guarantee: `current_log_level` initialises
/// to `None`, so the first reload after startup always invokes the
/// setter even when the file's `log_level` matches
/// `initial.observability.log_level`. The setter, not this section,
/// decides whether a `RUST_LOG` filter stays in place
/// ([`LogLevelApply::KeptRustLog`]).
#[tokio::test]
async fn first_reload_applies_log_level_even_when_matching_initial() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[payment]\nrate_per_mb = 1\n\n[observability]\nlog_level = \"info\"\n",
    );

    // Resolved-at-startup level is also `info` — old code would
    // think "nothing changed" and skip. New code must still apply.
    let initial = seed_resolved(1, LogLevel::Info);
    let (setter, captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );

    state.reload(&path).await.unwrap();
    assert_eq!(*captured.lock().unwrap(), Some(LogLevel::Info));
}

/// Fail-stop guarantee: when the setter errors, the reload surfaces the
/// error rather than partially applying. Inject a setter that always
/// returns an error and assert the reload fails.
#[tokio::test]
async fn reload_errors_when_log_level_setter_fails() {
    let dir = tempfile::tempdir().unwrap();
    // Log level differs from the cached value (None, i.e. force-apply
    // path) so the setter is actually called and gets the chance to fail.
    let path = write_config(dir.path(), "[observability]\nlog_level = \"debug\"\n");

    let failing_setter: LogLevelSetter =
        Box::new(|_| Err(anyhow::anyhow!("simulated tracing-reload failure")));

    let initial = seed_resolved(42, LogLevel::Info);
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        failing_setter,
    );
    let metrics = Arc::new(crate::metrics::Metrics::new());
    state.attach_metrics(Arc::clone(&metrics));

    let err = state.reload(&path).await.unwrap_err();
    assert!(format!("{err:#}").contains("simulated tracing-reload failure"));
    // The failed reload is counted once.
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_config_reload_failures_total 1"),
        "{text}"
    );
}

/// Transactional contract for the *log-level* mutex: a poisoned
/// `current_log_level` must surface as an error from the reload function
/// rather than committing a partial reload.
#[tokio::test]
async fn reload_errors_when_log_level_mutex_poisoned() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "[observability]\nlog_level = \"debug\"\n");

    let initial = seed_resolved(33, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );

    // Poison the log-level mutex by holding the lock in a thread
    // that panics. A normal `JoinHandle` lets us discard the panic
    // payload — `std::thread::scope` would rethrow on join and
    // abort the test before the assertion runs.
    let st = Arc::new(state);
    let st_for_thread = Arc::clone(&st);
    let join = std::thread::spawn(move || {
        let _guard = st_for_thread.log_level.current.lock().unwrap();
        panic!("intentional panic to poison mutex");
    });
    let _ = join.join(); // discard the panic payload
    assert!(st.log_level.current.is_poisoned());

    let err = st.reload(&path).await.unwrap_err();
    assert!(format!("{err:#}").contains("log-level mutex poisoned"));
}

/// The `reload_lock` only serialises concurrent reloads; it guards no
/// data, so a poisoned lock (a prior panic-mid-reload) must not wedge
/// future reloads. A reload after poisoning still recovers the guard
/// and applies the file — the setter fires.
#[tokio::test]
async fn reload_recovers_from_poisoned_reload_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "[observability]\nlog_level = \"debug\"\n");

    let initial = seed_resolved(42, LogLevel::Info);
    let (setter, captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );

    // Poison the reload lock on a thread that panics while holding it.
    let st = Arc::new(state);
    let st_for_thread = Arc::clone(&st);
    let join = std::thread::spawn(move || {
        let _guard = st_for_thread.reload_lock.lock().unwrap();
        panic!("intentional panic to poison reload lock");
    });
    let _ = join.join();
    assert!(st.reload_lock.is_poisoned());

    // Reload still succeeds — the poisoned lock is recovered in place.
    st.reload(&path).await.expect("reload recovers from poison");
    assert_eq!(captured.lock().unwrap().as_ref(), Some(&LogLevel::Debug));
}

/// A second reload of the same file must succeed and not touch the
/// rate (already at target). We can't directly capture `tracing`
/// lines without a subscriber fixture, but the public-state behaviour
/// the operator cares about is "reload remains idempotent across
/// repeated SIGHUPs".
#[tokio::test]
async fn reload_is_idempotent_across_repeated_sighups() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        concat!(
            "[identity]\nregion = \"US\"\n\n",
            "[network]\nbind_port = 4433\n\n",
            "[payment]\nrate_per_mb = 11\n\n",
            "[observability]\nlog_level = \"info\"\n",
        ),
    );

    let initial = seed_resolved(11, LogLevel::Info);
    let (setter, captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );

    // Three reloads of the same file: first applies, the next two
    // are no-ops on log level.
    state.reload(&path).await.unwrap();
    *captured.lock().unwrap() = None;
    state.reload(&path).await.unwrap();
    state.reload(&path).await.unwrap();
    assert!(captured.lock().unwrap().is_none());
}

/// `payment.*` is restart-required, so the reload path leaves it unparsed:
/// even a `rate_per_mb = 0` (which the startup resolver rejects) reloads
/// cleanly. Startup validation catches the invalid value.
#[tokio::test]
async fn reload_ignores_invalid_rate_since_payment_is_restart_required() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "[payment]\nrate_per_mb = 0\n");

    let initial = seed_resolved(42, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );

    state
        .reload(&path)
        .await
        .expect("a restart-required payment field must not fail the reload");
}

#[tokio::test]
async fn reload_returns_error_on_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("does-not-exist.toml");

    let initial = seed_resolved(7, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );
    let err = state.reload(&missing).await.unwrap_err();
    assert!(format!("{err:#}").contains("failed to read config file"));
}

/// A malformed TOML body must reject the reload before any commit
/// side-effect runs: the setter is never called. Without this test the
/// transactional guarantees only get exercised on the *resolution*
/// failure paths, not on the parse failure path.
#[tokio::test]
async fn reload_returns_error_on_malformed_toml() {
    let dir = tempfile::tempdir().unwrap();
    // Unterminated section header + dangling assignment — guaranteed
    // to fail the TOML parser without depending on any specific
    // diagnostic message.
    let path = write_config(dir.path(), "[payment\nrate_per_mb = ");

    let initial = seed_resolved(42, LogLevel::Info);
    let (setter, captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );

    let err = state.reload(&path).await.unwrap_err();
    // Don't bind the test to a specific TOML diagnostic; just check
    // the call failed.
    assert!(!format!("{err:#}").is_empty());

    assert!(captured.lock().unwrap().is_none());
}

// ----- content denylist hot-reload (ADR 011 §Local Denylist, #1168) -----

fn denylist_state(initial: &decdn_common::config::ResolvedConfig) -> RuntimeReloadState {
    RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        initial,
        recording_setter().0,
    )
}

/// The point of making `[content]` reloadable: an operator discharging a
/// one-hour removal order must not have to bounce the daemon (dropping
/// every in-flight paid stream) to do it.
///
/// Asserts through `CacheEngine::is_denied`, not the deny-set handle,
/// because reaching the cache is the whole fix — that is what suppresses
/// probe `has_blob`, DHT republish, and `populate` alongside the serve gate.
#[tokio::test]
async fn reload_applies_content_denylist_to_the_cache_lever() {
    let dir = tempfile::tempdir().unwrap();
    let h = make_hex_hash(7);
    let origin = "0x000000000000000000000000000000000000dEaD";
    let path = write_config(
        dir.path(),
        &format!("[content]\ndenied_hashes = [\"{h}\"]\ndenied_origins = [\"{origin}\"]\n"),
    );

    let initial = seed_resolved(10, LogLevel::Info);
    let state = denylist_state(&initial);
    let (engine, _tmp) = build_test_cache().await;
    state.attach_cache(Some(engine.clone()));
    let deny = state.content_denylist();
    let hash = decdn_cache::Hash::from_str(&h).unwrap();

    assert!(!engine.is_denied(hash), "nothing denied before reload");
    assert!(!engine.refuses(hash), "and nothing refused");

    state.reload(&path).await.expect("reload succeeds");

    assert!(engine.is_denied(hash), "hash denied after reload");
    assert!(
        engine.refuses(hash),
        "and therefore refused by probe/DHT/populate too"
    );
    assert!(
        deny.is_origin_denied(&origin.parse().unwrap()),
        "origin denied after reload"
    );
}

/// The handler holds the same `Arc` the reload path swaps, so a reload is
/// effective without rebuilding the handler. If these ever diverged the
/// denylist would silently stop applying to live connections.
#[tokio::test]
async fn content_denylist_handle_is_shared_not_copied() {
    let initial = seed_resolved(10, LogLevel::Info);
    let state = denylist_state(&initial);
    assert!(
        Arc::ptr_eq(&state.content_denylist(), &state.content_denylist()),
        "every caller must get the same deny-set"
    );
}

/// A malformed entry must fail the whole reload rather than committing a
/// partial (or empty) denylist — un-denying content mid-takedown is the
/// failure mode the two-phase commit exists to prevent.
#[tokio::test]
async fn reload_rejects_malformed_denied_hash_and_keeps_prior_set() {
    let dir = tempfile::tempdir().unwrap();
    let h = make_hex_hash(7);
    let good = write_config(
        dir.path(),
        &format!("[content]\ndenied_hashes = [\"{h}\"]\n"),
    );
    let initial = seed_resolved(10, LogLevel::Info);
    let state = denylist_state(&initial);
    let (engine, _tmp) = build_test_cache().await;
    state.attach_cache(Some(engine.clone()));
    state.reload(&good).await.expect("first reload succeeds");

    let bad = dir.path().join("bad.toml");
    std::fs::write(&bad, "[content]\ndenied_hashes = [\"zzz-not-hex\"]\n").unwrap();
    let err = state.reload(&bad).await.expect_err("malformed entry fails");
    assert!(
        format!("{err:#}").contains("denied_hashes"),
        "error names the field: {err:#}"
    );

    assert!(
        engine.is_denied(decdn_cache::Hash::from_str(&h).unwrap()),
        "the prior denylist must survive a failed reload"
    );
}

/// Emptying the section un-denies — the operator's own lever works in both
/// directions (a wrongful takedown must be reversible without a restart).
/// This is the property that rules out reusing the sticky `evict` latch.
#[tokio::test]
async fn reload_clears_content_denylist_when_emptied() {
    let dir = tempfile::tempdir().unwrap();
    let h = make_hex_hash(7);
    let with = write_config(
        dir.path(),
        &format!("[content]\ndenied_hashes = [\"{h}\"]\n"),
    );
    let initial = seed_resolved(10, LogLevel::Info);
    let state = denylist_state(&initial);
    let (engine, _tmp) = build_test_cache().await;
    state.attach_cache(Some(engine.clone()));
    state.reload(&with).await.unwrap();
    let hash = decdn_cache::Hash::from_str(&h).unwrap();
    assert!(engine.is_denied(hash));

    let without = dir.path().join("empty.toml");
    std::fs::write(&without, "[content]\n").unwrap();
    state.reload(&without).await.unwrap();

    assert!(!engine.is_denied(hash), "denylist cleared");
    assert!(!engine.refuses(hash), "and no longer refused anywhere");
}

// ----- pinned_hashes hot-reload (#276) -----

/// Build a 64-char lowercase hex hash for tests.
fn make_hex_hash(seed: u8) -> String {
    use std::fmt::Write as _;

    let mut bytes = [0u8; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        let i_u8 = u8::try_from(i).unwrap_or(0);
        *b = i_u8.wrapping_add(seed);
    }
    let mut s = String::with_capacity(64);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Construct a real (filesystem-backed) cache engine in a temp dir
/// for pinning-reload tests. Tests that don't need a full cache
/// just leave the engine unattached.
async fn build_test_cache() -> (decdn_cache::CacheEngine, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let engine = decdn_cache::CacheEngine::open(tmp.path(), Vec::new(), 16)
        .await
        .unwrap();
    (engine, tmp)
}

#[tokio::test]
async fn reload_swaps_pinned_hashes_on_attached_cache() {
    let dir = tempfile::tempdir().unwrap();
    let h1 = make_hex_hash(1);
    let h2 = make_hex_hash(2);
    let body = format!("[cache]\npinned_hashes = [\"{h1}\", \"{h2}\"]\n");
    let path = write_config(dir.path(), &body);

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );
    let (cache, _tmp_cache) = build_test_cache().await;
    state.attach_cache(Some(cache.clone()));

    // Before reload: empty pinned set.
    assert_eq!(cache.pinned_snapshot().len(), 0);

    state.reload(&path).await.unwrap();

    // After reload: both hashes pinned.
    let pinned = cache.pinned_snapshot();
    assert_eq!(pinned.len(), 2, "expected both hashes pinned");
}

#[tokio::test]
async fn reload_rejects_invalid_pinned_hash_and_keeps_previous_set() {
    let dir = tempfile::tempdir().unwrap();
    let h_good = make_hex_hash(3);
    // First reload: pin a valid hash.
    let path = write_config(
        dir.path(),
        &format!("[cache]\npinned_hashes = [\"{h_good}\"]\n"),
    );

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );
    let (cache, _tmp_cache) = build_test_cache().await;
    state.attach_cache(Some(cache.clone()));
    state.reload(&path).await.unwrap();
    assert_eq!(cache.pinned_snapshot().len(), 1);

    // Second reload: invalid hash. Must reject and keep the previous set.
    let bad_path = write_config(dir.path(), "[cache]\npinned_hashes = [\"zzz-not-hex\"]\n");
    let err = state.reload(&bad_path).await.unwrap_err();
    assert!(
        format!("{err:#}").contains("pinned_hashes") || format!("{err:#}").contains("64 hex"),
        "error should reference the invalid pinned hash: {err:#}"
    );
    // Previous pinned set retained.
    assert_eq!(
        cache.pinned_snapshot().len(),
        1,
        "previous pinned set must survive a rejected reload"
    );
}

#[tokio::test]
async fn reload_without_attached_cache_is_noop_for_pinning() {
    // Confirms the reload path doesn't blow up when no cache has
    // been attached yet (early startup window) — `attach_cache(None)`
    // is the default, and parse_pinned_hashes still runs but the
    // ArcSwap never happens.
    let dir = tempfile::tempdir().unwrap();
    let h = make_hex_hash(9);
    let path = write_config(dir.path(), &format!("[cache]\npinned_hashes = [\"{h}\"]\n"));

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );
    // Intentionally NOT calling attach_cache.

    // Reload should still succeed; pinned hashes are parsed (so a
    // malformed entry would still reject), they just don't land
    // anywhere.
    state.reload(&path).await.unwrap();
}

/// N → 0 transition: operator removes pinned hashes between
/// reloads. Without this test, a regression where `set_pinned`
/// short-circuits on empty input (e.g. `if new.is_empty() {
/// return; }`) would slip through silently — pinned hashes would
/// stay pinned forever, resisting eviction even after the operator
/// took them off the list.
#[tokio::test]
async fn reload_unpins_when_pinned_hashes_emptied() {
    let dir = tempfile::tempdir().unwrap();
    let h = make_hex_hash(11);

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );
    let (cache, _tmp_cache) = build_test_cache().await;
    state.attach_cache(Some(cache.clone()));

    // First reload pins one hash.
    let pin_path = write_config(dir.path(), &format!("[cache]\npinned_hashes = [\"{h}\"]\n"));
    state.reload(&pin_path).await.unwrap();
    assert_eq!(cache.pinned_snapshot().len(), 1);

    // Second reload presents an empty list — the engine's pinned
    // set must shrink to zero.
    let empty_path = write_config(dir.path(), "[cache]\npinned_hashes = []\n");
    state.reload(&empty_path).await.unwrap();
    assert!(
        cache.pinned_snapshot().is_empty(),
        "pinned set must be empty after operator removes all entries"
    );
}

/// Direct test for the poison-recovery branch in `attach_cache`.
/// `reload_errors_when_log_level_mutex_poisoned` poisons the
/// log-level slot, but never the cache slot itself. This locks in the
/// recovery path that commit `a148c02` introduced — silently
/// no-op'ing on a poisoned cache mutex would turn every subsequent
/// reload into a silent no-op for pinning.
#[tokio::test]
async fn attach_cache_recovers_from_poisoned_mutex() {
    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = Arc::new(RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    ));

    // Poison the cache mutex by panicking inside a held guard.
    let st_for_thread = Arc::clone(&state);
    let join = std::thread::spawn(move || {
        let _guard = st_for_thread.pinned.engine.lock().unwrap();
        panic!("intentional panic to poison cache mutex");
    });
    let _ = join.join();
    assert!(
        state.pinned.engine.is_poisoned(),
        "test setup: cache mutex should be poisoned"
    );

    // Recovery path: `attach_cache` must accept the new engine
    // despite the poison.
    let (cache, _tmp_cache) = build_test_cache().await;
    state.attach_cache(Some(cache.clone()));

    let stored = state
        .pinned
        .engine
        .lock()
        .map_or_else(|p| p.into_inner().is_some(), |g| g.is_some());
    assert!(stored, "attach_cache must store engine despite poison");
}

// ----- security hot-reload (#235) -----

/// Build a `ConnectionLimiter` with the same defaults `seed_resolved`
/// uses for `ResolvedSecurity`. Returned by `Arc` so tests can clone
/// it into both `attach_limiter` and assertions about the live state.
fn build_test_limiter() -> Arc<crate::dispatch::ConnectionLimiter> {
    use crate::dispatch::ConnectionLimiter;
    use crate::metrics::Metrics;
    use decdn_common::config::ResolvedSecurity;
    let metrics = Arc::new(Metrics::new());
    Arc::new(ConnectionLimiter::new(
        &ResolvedSecurity {
            max_concurrent_handlers: 256,
            per_source_rate_per_sec: 100.0,
            per_source_burst: 200,
            max_tracked_sources: 4096,
        },
        metrics,
    ))
}

#[tokio::test]
async fn reload_applies_security_when_limiter_attached() {
    // Tighten per-source burst from default 200 down to 1; after
    // reload the live limiter must reject the second acquire from
    // the same source IP.
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[security]\n\
         per_source_rate_per_sec = 0.001\n\
         per_source_burst = 1\n",
    );

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );
    let limiter = build_test_limiter();
    state.attach_limiter(Some(Arc::clone(&limiter)));

    state.reload(&path).await.unwrap();

    // Per-source burst is now 1.
    let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
    let _p1 = limiter
        .acquire_for_test(Some(ip))
        .expect("first per-source acquire");
    let err = limiter
        .acquire_for_test(Some(ip))
        .expect_err("second per-source acquire must reject after reload");
    assert_eq!(err, crate::dispatch::RejectReason::PerSource);
}

#[tokio::test]
async fn reload_without_attached_limiter_is_noop_for_security() {
    // No limiter attached → reload still parses + validates security
    // but doesn't blow up. Equivalent of the cache "noop for pinning"
    // test that already exists.
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[security]\nper_source_burst = 5\nper_source_rate_per_sec = 1.0\n",
    );
    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );
    // Intentionally NOT calling attach_limiter.
    state.reload(&path).await.unwrap();
}

#[tokio::test]
async fn reload_rejects_invalid_security_and_keeps_previous() {
    // Negative rate is invalid; reload must reject *and* the rate
    // atomic and log-level setter must NOT have moved (all-or-
    // nothing reload — invalid security blocks every other field
    // too).
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[payment]\n\
         rate_per_mb = 99\n\
         [observability]\n\
         log_level = \"debug\"\n\
         [security]\n\
         per_source_rate_per_sec = -1.0\n",
    );
    let initial = seed_resolved(42, LogLevel::Info);
    let (setter, captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );
    state.attach_limiter(Some(build_test_limiter()));

    let err = state.reload(&path).await.unwrap_err();
    assert!(format!("{err:#}").contains("per_source_rate_per_sec"));
    // "Previous values retained on error" applies to the whole reload:
    // the log-level setter must not have run when security rejected.
    assert!(
        captured.lock().unwrap().is_none(),
        "log-level setter must not have run when security rejected"
    );
}

/// SIGHUP aggregates problems across sections into one error;
/// all-or-nothing — no section's previous value moves and the
/// log-level setter is never called.
#[tokio::test]
async fn reload_aggregates_problems_across_sections() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[cache]\n\
         pinned_hashes = [\"notahash\"]\n\
         [security]\n\
         per_source_rate_per_sec = -1.0\n",
    );

    let initial = seed_resolved(42, LogLevel::Info);
    let (setter, captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );

    let err = state.reload(&path).await.unwrap_err();
    let msg = format!("{err:#}");
    // Aggregated envelope from `ConfigDiagnostics::into_result`. `[payment]`
    // is restart-required and never resolved on reload, so it cannot
    // contribute a problem here — the two reloadable sections do.
    assert!(
        msg.contains("configuration has 2 problem(s)"),
        "expected 2-problem envelope, got: {msg}"
    );
    // Every offending field is named in the same error.
    assert!(
        msg.contains("cache.pinned_hashes"),
        "missing pinned-hashes field: {msg}"
    );
    assert!(
        msg.contains("security.per_source_rate_per_sec"),
        "missing security field: {msg}"
    );

    // All-or-nothing: the log-level setter must not have run.
    assert!(
        captured.lock().unwrap().is_none(),
        "log-level setter must not have run when any section rejected"
    );
}

/// Setter-failure case extended with security: the log-level setter
/// returning Err must rollback before the limiter reload runs.
#[tokio::test]
async fn reload_keeps_security_when_log_level_setter_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[observability]\nlog_level = \"debug\"\n\
         [security]\n\
         per_source_rate_per_sec = 0.001\n\
         per_source_burst = 1\n",
    );
    let failing_setter: LogLevelSetter =
        Box::new(|_| Err(anyhow::anyhow!("simulated tracing-reload failure")));
    let initial = seed_resolved(10, LogLevel::Info);
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        failing_setter,
    );
    let limiter = build_test_limiter();
    state.attach_limiter(Some(Arc::clone(&limiter)));

    let err = state.reload(&path).await.unwrap_err();
    assert!(format!("{err:#}").contains("simulated tracing-reload failure"));

    // Limiter must NOT have been mutated — the per-node burst stays
    // at the seed_resolved default (200), so two acquires from
    // different IPs still succeed.
    let _p1 = limiter
        .acquire_for_test(Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(
            10, 0, 0, 1,
        ))))
        .expect("setter failure must not have shrunk per-source burst");
    let _p2 = limiter
        .acquire_for_test(Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(
            10, 0, 0, 2,
        ))))
        .expect("seed burst (200) still in effect → second succeeds");
}

/// Mirror of `attach_cache_recovers_from_poisoned_mutex` for the
/// limiter slot — silent no-op on a poisoned mutex would turn every
/// subsequent reload into a silent no-op for security.
#[tokio::test]
async fn attach_limiter_recovers_from_poisoned_mutex() {
    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = Arc::new(RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    ));

    let st_for_thread = Arc::clone(&state);
    let join = std::thread::spawn(move || {
        let _guard = st_for_thread.security.limiter.lock().unwrap();
        panic!("intentional panic to poison limiter mutex");
    });
    let _ = join.join();
    assert!(
        state.security.limiter.is_poisoned(),
        "test setup: limiter mutex should be poisoned"
    );

    let limiter = build_test_limiter();
    state.attach_limiter(Some(Arc::clone(&limiter)));

    let stored = state
        .security
        .limiter
        .lock()
        .map_or_else(|p| p.into_inner().is_some(), |g| g.is_some());
    assert!(stored, "attach_limiter must store engine despite poison");
}

/// A non-reloadable section present in the file only earns a
/// "requires restart" notice — it must not gate the reload. A file
/// carrying `[network]` (restart-only) alongside a reloadable
/// `log_level` change still applies the reloadable field.
#[tokio::test]
async fn reload_applies_despite_restart_only_section_present() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[observability]\nlog_level = \"debug\"\n\n[network]\nbind_port = 4433\n",
    );

    let initial = seed_resolved(42, LogLevel::Info);
    let (setter, captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );

    state.reload(&path).await.expect("reload applies");
    assert_eq!(*captured.lock().unwrap(), Some(LogLevel::Debug));
}

/// The `[cache]` restart notice is gated on a *non-reloadable* field
/// being set: a `pinned_hashes`-only edit (the section's sole
/// hot-reloadable field) must not trip it, while any other field must.
#[test]
fn cache_notice_gate_ignores_pinned_hashes_only() {
    use decdn_common::config::types::CacheConfig;

    // Only the hot-reloadable field set -> no restart notice.
    let pinned_only = CacheConfig {
        pinned_hashes: Some(vec!["deadbeef".to_string()]),
        ..CacheConfig::default()
    };
    assert!(!cache_has_restart_required_field(&pinned_only));
    // Empty section -> no restart notice.
    assert!(!cache_has_restart_required_field(&CacheConfig::default()));
    // A non-reloadable field set -> notice.
    let with_dir = CacheConfig {
        cache_dir: Some(PathBuf::from("/tmp/decdn-cache")),
        ..CacheConfig::default()
    };
    assert!(cache_has_restart_required_field(&with_dir));
}

/// The `[observability]` restart notice diffs resolved values against
/// startup: an unchanged value or a `log_level`-only edit (hot-reloadable)
/// stays silent, and each changed restart-required field is named.
#[test]
fn observability_restart_notice_names_only_changed_fields() {
    let startup_resolved = seed_resolved(10, LogLevel::Info).observability;
    let startup = StartupObservability::from_resolved(&startup_resolved);

    let unchanged = seed_resolved(10, LogLevel::Debug).observability;
    assert!(observability_restart_required_changes(&startup, &unchanged).is_empty());

    let mut reloaded = seed_resolved(10, LogLevel::Info).observability;
    reloaded.otlp_endpoint = Some("http://collector:4317".to_string());
    reloaded.metrics_port = 9999;
    assert_eq!(
        observability_restart_required_changes(&startup, &reloaded),
        ["metrics_port", "otlp_endpoint"]
    );

    // One field at a time, so a name paired with the wrong comparison fails.
    for field in [
        "log_format",
        "metrics_port",
        "metrics_bind",
        "admin_port",
        "otlp_endpoint",
    ] {
        let mut reloaded = seed_resolved(10, LogLevel::Info).observability;
        match field {
            "log_format" => reloaded.log_format = decdn_common::cli::common::LogFormat::Json,
            "metrics_port" => reloaded.metrics_port = 9999,
            "metrics_bind" => {
                reloaded.metrics_bind = std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED);
            }
            "admin_port" => reloaded.admin_port = None,
            _ => reloaded.otlp_endpoint = Some("http://collector:4317".to_string()),
        }
        assert_eq!(
            observability_restart_required_changes(&startup, &reloaded),
            [field],
            "changing only {field}"
        );
    }
}

/// SIGHUP with a `[load_shed]` block swaps the live controller's policy:
/// a controller pinned to `resource-pressure` with a single-slot high
/// water mark sheds a second concurrent miss, and after reloading to
/// `always-admit` the same shape of request admits.
#[tokio::test]
async fn load_shed_section_swaps_policy_on_reload() {
    let start = decdn_common::config::ResolvedLoadShed {
        policy: decdn_common::config::LoadShedPolicyKind::ResourcePressure,
        egress_budget_mbps: 0,
        max_concurrent_serves_high: 1,
        max_concurrent_serves_low: 0,
        per_client_serve_cap: 0,
    };
    let controller = crate::load_shed::LoadShedController::from_config(&start);
    // Occupy the single slot so ResourcePressure would shed a new miss.
    let _held = controller
        .try_admit(
            crate::load_shed::RequestClass::CacheHit,
            alloy::primitives::B256::ZERO,
        )
        .unwrap();
    assert!(
        controller
            .try_admit(
                crate::load_shed::RequestClass::CacheMiss,
                alloy::primitives::B256::from([1u8; 32])
            )
            .is_err(),
        "a full slot must shed a new miss under resource-pressure"
    );

    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "[load_shed]\npolicy = \"always-admit\"\n");
    let initial = seed_resolved(42, LogLevel::Info);
    let (setter, _captured) = recording_setter();
    let state = RuntimeReloadState::new(
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &initial,
        setter,
    );
    state.attach_load_shed(Some(Arc::clone(&controller)));

    state.reload(&path).await.unwrap();

    // Now always-admit: the previously-shed miss admits.
    assert!(
        controller
            .try_admit(
                crate::load_shed::RequestClass::CacheMiss,
                alloy::primitives::B256::from([2u8; 32])
            )
            .is_ok(),
        "reload to always-admit must let a previously-shed miss through"
    );
}
