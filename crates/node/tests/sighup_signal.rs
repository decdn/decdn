//! Persistent-signal-stream contract for SIGHUP and SIGTERM.
//!
//! The unit tests in `runtime::reload::tests` stub out signal delivery
//! and call `RuntimeReloadState::reload` directly. That covers the reload
//! semantics, but it can't catch the bug the SIGHUP path was originally
//! refactored to fix: re-creating the `tokio::signal::unix::Signal`
//! every iteration of the runtime select loop drops signals delivered
//! while a reload is in flight. These tests raise real signals at the
//! installed `HupStream` / `ShutdownStreams` and assert the persistent
//! stream observes them — a regression that re-installs per-iteration
//! would either drop the second signal or hot-spin.
//!
//! Gated `#[cfg(unix)]`: SIGHUP/SIGTERM don't exist on Windows and
//! `tokio::signal::unix` isn't compiled there.
//!
//! **Run with `cargo nextest run` (the repo's preferred runner), not
//! `cargo test`.** `tokio::signal::unix` registration is process-global:
//! a SIGHUP raised by one test notifies *every* `Signal` stream in the
//! process. nextest runs each test in its own process, so the tests here
//! are isolated. Under `cargo test`'s in-process thread parallelism these
//! tests would cross-deliver signals to each other and race.

#![cfg(unix)]
// Tests legitimately call `.unwrap()` / `.expect()` on harness
// scaffolding. The workspace anti-panic policy applies to runtime code.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use decdn_common::cli::common::LogLevel;
use decdn_common::cli::run::ObservabilityArgs;
use decdn_common::config::{
    ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedIdentity, ResolvedNetwork,
    ResolvedObservability, ResolvedPayment, ResolvedSecurity,
};
use decdn_node::dispatch::{ConnectionLimiter, RejectReason};
use decdn_node::metrics::Metrics;
use decdn_node::runtime::{LogLevelSetter, RuntimeReloadState};
use nix::sys::signal::{Signal, raise};

/// Build the same minimal `ResolvedConfig` the unit tests use.
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
            rate_bounds_poll_interval_sec: 3600,
            fee_shares_poll_interval_sec: 3600,
            redeem_threshold_micro_usdc: 1_000_000,
            redeem_max_vouchers_per_tx: 300,
            redeem_interval_secs: 300,
            buyer_working_deposit_micro_usdc: 10_000_000,
            buyer_max_approve: true,
            pool_min_remaining_deposit_micro_usdc: 1_000_000,
            pool_floor_signer_live_windows: 8,
            pool_floor_signer_bucket_windows: 8,
            pool_floor_signer_refill_secs: 60,
            slash_judge_address: "0x0000000000000000000000000000000000000003".to_string(),
            content_blacklist_address: None,
            content_blacklist_poll_interval_sec: 600,
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
            frame_target_bytes: decdn_common::config::DEFAULT_FRAME_TARGET_BYTES,
            rate_per_mb: rate,
            delivery_floor: 0,
            credit_max: decdn_common::config::DEFAULT_CREDIT_MAX,
            credit_ramp_divisor: decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
            voucher_commit_interval_ms: decdn_common::config::DEFAULT_VOUCHER_COMMIT_INTERVAL_MS,
        },
        observability: ResolvedObservability {
            log_level: level,
            log_format: decdn_common::cli::LogFormat::Pretty,
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
    }
}

/// Setter that records every applied log level into a shared `Vec`. The
/// unit-test version only retains the most recent value; this version
/// retains the *sequence* so we can prove that two SIGHUPs raised in
/// quick succession both got their respective reloads applied.
fn recording_setter() -> (LogLevelSetter, Arc<Mutex<Vec<LogLevel>>>) {
    let levels = Arc::new(Mutex::new(Vec::<LogLevel>::new()));
    let captured = Arc::clone(&levels);
    let setter: LogLevelSetter = Box::new(move |lvl| {
        captured.lock().unwrap().push(lvl);
        Ok(())
    });
    (setter, levels)
}

fn write_config(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
}

/// `io::Write` sink that appends every byte into a shared buffer. Used to
/// capture the reload path's `tracing` output so a test can assert on the
/// `info`-level "ignoring change to X (requires restart)" lines — a
/// restart-required field is intentionally never applied to *live runtime
/// state*, so a tracing event is the only place its rejection is
/// observable.
#[derive(Clone)]
struct BufferWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Snapshot the captured log buffer as a UTF-8 string.
fn captured_logs(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
}

/// Raise SIGHUP from a cooperatively-scheduled task once the caller parks
/// on `hup.recv()`. The 50ms guard mirrors the install-race guard the
/// other tests in this file use.
fn raise_sighup_soon() {
    tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        raise(Signal::SIGHUP).expect("raise SIGHUP");
    });
}

/// Drives a `HupStream`-style reload loop on the current task. Each
/// iteration: wait for the next SIGHUP, then call
/// `RuntimeReloadState::reload`. The loop terminates after `expected`
/// successful reloads so the test doesn't hang on a missing signal.
async fn run_reload_loop(
    state: Arc<RuntimeReloadState>,
    path: PathBuf,
    expected: usize,
) -> anyhow::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut hup = signal(SignalKind::hangup())?;
    let mut applied = 0usize;
    while applied < expected {
        // `recv()` is the supported way to await repeated signals; the
        // persistent stream is exactly what's being verified here.
        if hup.recv().await.is_none() {
            anyhow::bail!("SIGHUP stream closed before {expected} reloads");
        }
        if let Err(err) = state.reload(&path).await {
            anyhow::bail!("reload failed at iteration {applied}: {err:#}");
        }
        applied += 1;
    }
    Ok(())
}

/// Two SIGHUPs raised in quick succession against a persistent
/// `Signal` stream must both be observed by the reload loop. A
/// regression that re-installed the signal per iteration would drop
/// the second one (the kernel coalesces while no handler is
/// registered, and a freshly re-installed `Signal` only delivers
/// signals that arrive *after* its install).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_sighup_observes_both_signals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, levels) = recording_setter();
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

    // First config: log_level=info.
    write_config(&path, "[observability]\nlog_level = \"info\"\n");

    // Spawn the reload loop on a separate task so we can raise signals
    // from this one. Two reloads expected: one per SIGHUP.
    let loop_state = Arc::clone(&state);
    let loop_path = path.clone();
    let loop_handle = tokio::spawn(async move { run_reload_loop(loop_state, loop_path, 2).await });

    // Give the reload loop a moment to install its `Signal` handler
    // before we raise. Without this the SIGHUP can be delivered before
    // the handler is registered and the test races to a hang. 50ms is
    // far longer than the install path needs in practice.
    tokio::time::sleep(Duration::from_millis(50)).await;

    raise(Signal::SIGHUP).expect("raise SIGHUP #1");

    // Wait for the first reload to land. Spinning on the recorded
    // levels is faster than a fixed sleep and bounds the wait.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while levels.lock().unwrap().is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        levels.lock().unwrap().len(),
        1,
        "first SIGHUP did not produce a reload within 2s"
    );

    // Second config: log_level=debug.
    write_config(&path, "[observability]\nlog_level = \"debug\"\n");
    raise(Signal::SIGHUP).expect("raise SIGHUP #2");

    // Bounded wait for the loop task to finish — it exits after the
    // second reload. A drop here means we lost the second signal.
    tokio::time::timeout(Duration::from_secs(2), loop_handle)
        .await
        .expect("reload loop did not finish within 2s of second SIGHUP")
        .expect("reload loop task panicked")
        .expect("reload loop returned Err");

    let captured = levels.lock().unwrap().clone();
    assert_eq!(
        captured,
        vec![LogLevel::Info, LogLevel::Debug],
        "both SIGHUPs must produce ordered reloads"
    );
}

/// End-to-end SIGHUP→reload→`ConnectionLimiter::reload` chain (#235).
///
/// The reload-unit tests cover the in-process commit semantics; this
/// test proves the SIGHUP path actually wires through to the live
/// limiter. Without this we'd have no test exercising
/// `runtime::reload::reload`'s `limiter_guard` arm against a real OS
/// signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sighup_applies_security_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, _levels) = recording_setter();
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

    // Build a real limiter at the seed defaults (per_source_burst = 200).
    let metrics = Arc::new(Metrics::new());
    let limiter = Arc::new(ConnectionLimiter::new(&initial.security, metrics));
    state.attach_limiter(Some(Arc::clone(&limiter)));

    // Tighten per-source burst to 1.
    write_config(
        &path,
        "[security]\n\
         per_source_rate_per_sec = 0.001\n\
         per_source_burst = 1\n",
    );

    let loop_state = Arc::clone(&state);
    let loop_path = path.clone();
    let loop_handle = tokio::spawn(async move { run_reload_loop(loop_state, loop_path, 1).await });

    // Same install-race guard as the persistent-SIGHUP test above.
    tokio::time::sleep(Duration::from_millis(50)).await;
    raise(Signal::SIGHUP).expect("raise SIGHUP");

    tokio::time::timeout(Duration::from_secs(2), loop_handle)
        .await
        .expect("reload loop did not finish within 2s of SIGHUP")
        .expect("reload loop task panicked")
        .expect("reload loop returned Err");

    // Live limiter now has per-source burst=1: first acquire from an
    // IP succeeds, second from the same IP rejects on per-source.
    let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
    let _p1 = limiter
        .acquire_for_test(Some(ip))
        .expect("first per-source acquire post-SIGHUP");
    let err = limiter
        .acquire_for_test(Some(ip))
        .expect_err("second per-source acquire must reject after SIGHUP-applied burst=1");
    assert_eq!(err, RejectReason::PerSource);
}

/// SIGHUP must apply changes to the mutable `observability.log_level` while
/// restart-required sections (`payment`, `network`, `blockchain`, `cache`,
/// `identity`) surface an `info`-level "ignoring change to X
/// (requires restart)" notice — never a silently-applied or silently-dropped
/// value (#499). `payment.rate_per_mb` is restart-required, so a `[payment]`
/// change earns the notice like any other non-reloadable section.
///
/// The unit tests in `runtime::reload::tests` exercise the section notice
/// emitter directly and the other tests in this file only ever feed
/// reloadable sections, so nothing here previously drove a real SIGHUP that
/// carried *both* a mutable and a restart-required change in the same file.
/// A regression that wrongly classified a restart-required section as
/// reloadable (applying it silently) would go uncaught.
///
/// The notice is unconditional (`warn_restart_required_sections`): it fires
/// once per restart-required section *present* in the reloaded file,
/// whether or not the section changed. The two reloads assert that:
///   - **Reload #1** carries `[network]` and `[payment]` → one notice each;
///     `[blockchain]` / `[cache]` (absent from the file) stay silent.
///   - **Reload #2** carries `[network]` and `[payment]` again plus new
///     `[blockchain]` / `[cache]` / `[identity]` → each present
///     restart-required section emits one notice (no cross-reload suppression),
///     all alongside the mutable `observability.log_level` change that must
///     still apply.
///
/// Runs on a single-thread runtime so the buffer-capturing subscriber
/// installed via `set_default` (thread-local) observes the reload, which
/// is awaited inline on the same thread rather than on a spawned task.
// One deliberately sequential narrative: two full SIGHUP→reload cycles
// whose ordering (present-section notices on reload #1, then a buffer
// clear and re-emit on reload #2) is the property under test.
// Splitting it into helpers would hide that ordering, not clarify it.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "current_thread")]
async fn sighup_applies_mutable_but_rejects_restart_required_fields() {
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::util::SubscriberInitExt;

    let log_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let sink = BufferWriter(Arc::clone(&log_buf));
    // Thread-local default subscriber (RAII guard from `set_default`).
    // Capture is confined to this test by the *combination* of: the
    // thread-local guard, the `current_thread` runtime, and `reload`
    // being awaited inline (not spawned) — see the doc comment above.
    // It also relies on no test in this binary installing a *global*
    // subscriber (none does). The asserted lines are `info`-level
    // (`warn_ignored` → `tracing::info!`), so `INFO` is the minimum
    // capture level; broader would only add noise to failure dumps.
    let _log_guard = tracing_subscriber::fmt()
        .with_writer(move || sink.clone())
        .with_ansi(false)
        .with_max_level(LevelFilter::INFO)
        .finish()
        .set_default();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, levels) = recording_setter();
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

    let mut hup = {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::hangup()).expect("install SIGHUP stream")
    };

    // Count "ignoring change to <section>.*" notices in the buffer. We
    // assert on the stable section prefix + the `(requires restart)`
    // suffix rather than the full middle field list, so a cosmetic edit
    // to a field list in `warn_ignored` doesn't break the test while a
    // misclassification regression (a restart-only section wrongly
    // treated as reloadable, so its notice vanishes) still does.
    let notice_count = |logs: &str, section: &str| -> usize {
        let prefix = format!("ignoring change to {section}.*");
        logs.lines()
            .filter(|l| l.contains(&prefix) && l.contains("(requires restart)"))
            .count()
    };

    // --- Reload #1: a mutable `log_level` + restart-required `[payment]`
    //     and `[network]` sections. The log_level change applies; `[payment]`
    //     and `[network]` are present so each emits exactly one notice;
    //     blockchain/cache (not in the file) stay silent.
    write_config(
        &path,
        "[payment]\n\
         rate_per_mb = 11\n\n\
         [observability]\n\
         log_level = \"info\"\n\n\
         [network]\n\
         bind_port = 5555\n",
    );
    raise_sighup_soon();
    hup.recv().await.expect("first SIGHUP");
    state.reload(&path).await.expect("first reload");

    assert_eq!(
        *levels.lock().unwrap().last().unwrap(),
        LogLevel::Info,
        "mutable log_level must take effect on the first SIGHUP"
    );
    let after_first = captured_logs(&log_buf);
    assert_eq!(
        notice_count(&after_first, "network"),
        1,
        "a present restart-required [network] section must emit exactly \
         one (requires restart) notice, got:\n{after_first}"
    );
    assert_eq!(
        notice_count(&after_first, "payment"),
        1,
        "a present restart-required [payment] section must emit exactly \
         one (requires restart) notice, got:\n{after_first}"
    );
    assert_eq!(
        notice_count(&after_first, "blockchain"),
        0,
        "blockchain absent from the file must not warn, got:\n{after_first}"
    );
    assert_eq!(
        notice_count(&after_first, "cache"),
        0,
        "cache absent from the file must not warn, got:\n{after_first}"
    );
    // `[observability]` is present but sets only the hot-reloadable
    // `log_level`, so it does not warn (field-gated notice, not
    // section-presence).
    assert_eq!(
        notice_count(&after_first, "observability"),
        0,
        "observability with only log_level set (reloadable) must not warn, got:\n{after_first}"
    );

    // Isolate reload #2's notices from reload #1's so the counts below
    // reflect a single reload (the notice fires on field presence, not on
    // change, so the cumulative buffer would otherwise double-count
    // fields present in both files).
    log_buf.lock().unwrap().clear();

    // --- Reload #2: the mutable `log_level` change applies, bundled with
    //     restart-required ones. payment/network/blockchain/identity/
    //     dht/probe/receipts warn on presence; `[cache]`
    //     warns because it sets the non-reloadable `cache_dir` (a
    //     pinned_hashes-only edit would not); `[observability]` does NOT warn
    //     because it sets only the reloadable `log_level`; `[security]` is
    //     fully reloadable, so it stays silent. Empty `[dht]`/`[probe]`/
    //     `[receipts]`/`[security]` tables are "present" so they exercise
    //     those emitter branches.
    write_config(
        &path,
        "[payment]\n\
         rate_per_mb = 22\n\n\
         [observability]\n\
         log_level = \"debug\"\n\n\
         [network]\n\
         bind_port = 5555\n\n\
         [blockchain]\n\
         rpc_url = \"http://changed.example:9999\"\n\n\
         [cache]\n\
         cache_dir = \"/tmp/decdn-test-other-cache\"\n\n\
         [identity]\n\
         region = \"US\"\n\n\
         [dht]\n\n\
         [probe]\n\n\
         [receipts]\n\n\
         [security]\n",
    );
    raise_sighup_soon();
    hup.recv().await.expect("second SIGHUP");
    state.reload(&path).await.expect("second reload");

    // The mutable log_level took effect even though the file also carried
    // restart-required changes.
    assert_eq!(
        levels.lock().unwrap().clone(),
        vec![LogLevel::Info, LogLevel::Debug],
        "mutable log_level must update across both reloads"
    );

    let logs = captured_logs(&log_buf);

    // Every section carrying a non-reloadable field emits exactly one
    // notice — including `[payment]` and `[network]`, present again (the
    // notice fires on field presence, not on a change vs the previous reload).
    for section in [
        "payment",
        "network",
        "blockchain",
        "cache",
        "identity",
        "dht",
        "probe",
        "receipts",
    ] {
        assert_eq!(
            notice_count(&logs, section),
            1,
            "expected exactly one (requires restart) notice for [{section}] \
             carrying a non-reloadable field, got:\n{logs}"
        );
    }

    // Fully-reloadable `[security]`, and `[observability]` which set only the
    // reloadable `log_level`, stay silent.
    for section in ["security", "observability"] {
        assert_eq!(
            notice_count(&logs, section),
            0,
            "[{section}] must not warn (no non-reloadable field set), got:\n{logs}"
        );
    }
}

/// A resolve-time notice must reach the operator's structured log stream on
/// the SIGHUP path, where the subscriber has been live for hours.
///
/// `resolve_security_into` runs in two places: once at startup, before
/// `init_tracing` installs a subscriber, and again on every SIGHUP. An
/// operator running `log_format = "json"` and shipping only the structured
/// stream needs `field` to arrive as a real event field it can filter on — the
/// reload's own "section applied" event carries the new value but neither the
/// severity nor the word the operator greps for.
///
/// The capture is a JSON subscriber rather than the pretty formatter for that
/// reason: rendered text cannot tell a structured `field` from one interpolated
/// into the message, which is the whole property being claimed.
///
/// The same three constraints as `sighup_applies_mutable_but_rejects_restart_required_fields`
/// make the capture sound: a thread-local `set_default` guard, a
/// `current_thread` runtime, and `reload` awaited inline rather than spawned.
#[tokio::test(flavor = "current_thread")]
async fn sighup_routes_a_resolve_notice_into_the_log_stream() {
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::util::SubscriberInitExt;

    /// Find the captured JSON event whose `field` value is `field`, and return
    /// its level and message.
    fn notice_event(logs: &str, field: &str) -> Option<(String, String)> {
        logs.lines().find_map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).ok()?;
            let fields = v.get("fields")?;
            // `field` must be its own event field, not text inside the message.
            if fields.get("field")?.as_str()? != field {
                return None;
            }
            Some((
                v.get("level")?.as_str()?.to_string(),
                fields.get("message")?.as_str()?.to_string(),
            ))
        })
    }

    let log_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let sink = BufferWriter(Arc::clone(&log_buf));
    let _log_guard = tracing_subscriber::fmt()
        .json()
        .with_writer(move || sink.clone())
        .with_ansi(false)
        .with_max_level(LevelFilter::INFO)
        .finish()
        .set_default();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");

    let initial = seed_resolved(10, LogLevel::Info);
    let (setter, _levels) = recording_setter();
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

    let mut hup = {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::hangup()).expect("install SIGHUP stream")
    };

    // Both `0`s are documented escape hatches, so the reload must *succeed* —
    // the notices are the whole observable effect, which is what made them so
    // easy to lose. The two levels ride the same reload so the `Info` arm is
    // exercised alongside the `Warn` one.
    write_config(
        &path,
        "[security]\n\
         max_tracked_sources = 0\n\
         per_source_rate_per_sec = 0.0\n\
         per_source_burst = 0\n",
    );
    raise_sighup_soon();
    hup.recv().await.expect("first SIGHUP");
    state.reload(&path).await.expect("0 is valid, not an error");

    let logs = captured_logs(&log_buf);
    let (level, message) =
        notice_event(&logs, "security.max_tracked_sources").unwrap_or_else(|| {
            panic!("no notice for the zeroed bookkeeping cap, got:\n{logs}");
        });
    assert_eq!(
        level, "WARN",
        "an unbounded bookkeeping map must be WARN, not INFO"
    );
    assert!(
        message.contains("unbounded"),
        "the notice must carry the word an operator alerts on: {message}"
    );
    assert!(
        !message.contains("security.max_tracked_sources"),
        "the label rides as a field, so the message must not repeat it: {message}"
    );

    let (level, message) = notice_event(&logs, "security.per_source_rate_per_sec")
        .unwrap_or_else(|| panic!("no notice for the disabled rate limit, got:\n{logs}"));
    assert_eq!(
        level, "INFO",
        "a deliberate opt-out must not page anyone alerting on WARN"
    );
    assert!(message.contains("disabled"), "{message}");

    // An aborted reload applies nothing, so its notices would describe a
    // config the node is not running. Clear the buffer, then SIGHUP a file
    // whose `[security]` section carries both the notice trigger and a fatal
    // problem.
    log_buf.lock().unwrap().clear();
    write_config(
        &path,
        "[security]\n\
         max_tracked_sources = 0\n\
         per_source_rate_per_sec = -1.0\n",
    );
    raise_sighup_soon();
    hup.recv().await.expect("second SIGHUP");
    state
        .reload(&path)
        .await
        .expect_err("a negative rate must abort the reload");

    let logs = captured_logs(&log_buf);
    assert!(
        !logs.contains("unbounded"),
        "an aborted reload must not report notices about a config it did not \
         apply, got:\n{logs}"
    );
}

/// The other abort path: a reload that resolves clean but fails a phase-2
/// `fallible_commit` must not report notices either.
///
/// This leg is easy to miss because the resolve gate looks like the only way
/// out. It is not: a `fallible_commit` error returns *before* any
/// `infallible_swap`, and `infallible_swap` is where the notice-bearing
/// sections put their values — so a notice emitted before phase 2 describes a
/// config the node is provably not running.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_commit_reports_no_notices() {
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::util::SubscriberInitExt;

    let log_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let sink = BufferWriter(Arc::clone(&log_buf));
    let _log_guard = tracing_subscriber::fmt()
        .with_writer(move || sink.clone())
        .with_ansi(false)
        .with_max_level(LevelFilter::INFO)
        .finish()
        .set_default();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");

    let initial = seed_resolved(10, LogLevel::Info);
    let failing_setter: LogLevelSetter =
        Box::new(|_| Err(anyhow::anyhow!("simulated tracing-reload failure")));
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
        failing_setter,
    ));

    let mut hup = {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::hangup()).expect("install SIGHUP stream")
    };

    // Both sections resolve clean. `log_level` differs from the seeded value
    // so the setter is actually called — and fails, taking the whole reload
    // down with the security values still unapplied.
    write_config(
        &path,
        "[observability]\n\
         log_level = \"debug\"\n\
         [security]\n\
         max_tracked_sources = 0\n",
    );
    raise_sighup_soon();
    hup.recv().await.expect("SIGHUP");
    let err = state
        .reload(&path)
        .await
        .expect_err("a failing setter must abort the reload");
    assert!(format!("{err:#}").contains("simulated tracing-reload failure"));

    let logs = captured_logs(&log_buf);
    assert!(
        !logs.contains("unbounded"),
        "a reload that aborted in phase 2 applied no security values, so it \
         must not report their notices, got:\n{logs}"
    );
}
