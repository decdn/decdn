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
use std::sync::atomic::Ordering;
use std::time::Duration;

use decdn_common::cli::common::LogLevel;
use decdn_common::cli::run::{ObservabilityArgs, PaymentArgs};
use decdn_common::config::{
    ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedGossip, ResolvedIdentity,
    ResolvedNetwork, ResolvedObservability, ResolvedPayment, ResolvedSecurity,
};
use decdn_node::dispatch::{ConnectionLimiter, RejectReason};
use decdn_node::metrics::Metrics;
use decdn_node::runtime::{LogLevelSetter, RuntimeReloadState};
use nix::sys::signal::{Signal, raise};

/// Build the same minimal `ResolvedConfig` the unit tests use.
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
            enable_0rtt: true,
        },
        blockchain: ResolvedBlockchain {
            origin_assignment_address: None,
            publisher_registry_address: None,
            origin_directory_from_block: 0,
            rpc_url: "http://localhost:8545".into(),
            eth_keystore: PathBuf::from("/tmp/keystore.json"),
            keystore_password_file: None,
            payment_channel_address: "0x0000000000000000000000000000000000000001".into(),
            capacity_bond_address: "0x0000000000000000000000000000000000000002".into(),
            rpc_watchdog_interval_sec: 30,
            event_poll_interval_ms: 7000,
            redeem_threshold_micro_usdc: 1_000_000,
            buyer_deposit_micro_usdc: 10_000_000,
            buyer_max_approve: true,
            settlement_auto_threshold_micro_usdc: None,
            settlement_auto_by_voucher_nonce_span: None,
            slash_judge_address: "0x0000000000000000000000000000000000000003".to_string(),
            slash_judge_from_block: 0,
            content_blacklist_address: None,
            content_blacklist_from_block: 0,
            content_blacklist_poll_interval_sec: 600,
            chain_id: decdn_common::config::DEFAULT_CHAIN_ID,
        },
        cache: ResolvedCache {
            cache_dir: PathBuf::from("/tmp/cache"),
            cache_size_mb: 1024,
            max_blob_size_mb: 128,
            origins: Vec::new(),
            pinned_hashes: decdn_cache::PinnedHashes::empty(),
            origin_retry: decdn_cache::RetryPolicy::default(),
            circuit_breaker: decdn_cache::CircuitBreakerPolicy::default(),
            user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
            gc_interval_sec: 0,
            max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
            stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
            node_to_node_pull_through_enabled: false,
            node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
            node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
            pull_ahead_bytes: decdn_cache::Bytes::new(
                decdn_common::config::DEFAULT_PULL_AHEAD_BYTES,
            ),
            max_unrecouped_leech_bytes: decdn_cache::Bytes::new(
                decdn_common::config::DEFAULT_MAX_UNRECOUPED_LEECH_BYTES,
            ),
            pull_share_ratio_percent: decdn_cache::Percent::new(
                decdn_common::config::DEFAULT_PULL_SHARE_RATIO_PERCENT,
            ),
            pull_through_require_authorized_origin: false,
        },
        payment: ResolvedPayment {
            rate_per_mb: rate,
            delivery_floor: 0,
            delivery_ceiling: decdn_protocol::MAX_RATE_PER_MB,
            voucher_interval_mb: decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB,
        },
        observability: ResolvedObservability {
            log_level: level,
            log_format: decdn_common::cli::LogFormat::Pretty,
            metrics_port: 9090,
            metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            admin_port: Some(9191),
            otlp_endpoint: None,
            region_accounting_interval_sec:
                decdn_common::config::DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC,
        },
        gossip: ResolvedGossip {
            announce_interval_sec: 60,
            peer_ttl_sec: 600,
            subscribe_global: false,
            subscribe_reputation: true,
            reputation_publish_interval_sec: 3600,
            allowlist: Vec::new(),
            max_peer_table_entries: 100_000,
        },
        security: ResolvedSecurity {
            max_concurrent_handlers: 256,
            per_source_rate_per_sec: 100.0,
            per_source_burst: 200,
            max_tracked_sources: 4096,
        },
        dht: decdn_common::config::ResolvedDht::default(),
        probe: decdn_common::config::ResolvedProbe::default(),
        receipts: decdn_common::config::ResolvedReceipts::default(),
        prefetch: decdn_common::config::ResolvedPrefetch::default(),
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
/// `info`-level "ignoring change to X (requires restart)" diff lines — a
/// restart-required field is intentionally never applied to *live runtime
/// state*, so a tracing event is the only place its rejection is
/// observable (the changed file content is also folded into the diff
/// baseline, but that is not a runtime-behaviour surface).
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
        PaymentArgs {
            rate_per_mb: None,
            delivery_floor: None,
            delivery_ceiling: None,
        },
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
    let shared_rate = state.rate_per_mb();

    // First config: rate=11, log_level=info.
    write_config(
        &path,
        "[payment]\nrate_per_mb = 11\n\n[observability]\nlog_level = \"info\"\n",
    );

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
    assert_eq!(shared_rate.load(Ordering::Relaxed), 11);

    // Second config: rate=22, log_level=debug.
    write_config(
        &path,
        "[payment]\nrate_per_mb = 22\n\n[observability]\nlog_level = \"debug\"\n",
    );
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
    assert_eq!(shared_rate.load(Ordering::Relaxed), 22);
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
        PaymentArgs {
            rate_per_mb: None,
            delivery_floor: None,
            delivery_ceiling: None,
        },
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

/// SIGHUP must apply changes to mutable fields (`payment.rate_per_mb`,
/// `observability.log_level`) while *rejecting* changes to
/// restart-required fields (`network`, `blockchain`, `cache`, `identity`,
/// `gossip`) — the rejection surfacing as an `info`-level "ignoring change
/// to X (requires restart)" log line, never as a silently-applied or
/// silently-dropped value (#499).
///
/// The unit tests in `runtime::reload::tests` exercise the diff helpers
/// directly and the other tests in this file only ever feed reloadable
/// sections, so nothing here previously drove a real SIGHUP that carried
/// *both* a mutable and a restart-required change in the same file. A
/// regression in the diff logic (e.g. a restart-required section wrongly
/// classified as reloadable, or the notice suppressed) would go uncaught.
///
/// The two reloads exercise all three branches of the section diff
/// (`log_ignored_other_sections::changed`):
///   - **Reload #1** carries `[network]` against the empty startup
///     baseline → the *absent→present* branch must emit the one-shot
///     "requires restart" notice (the no-baseline conservative-warn path,
///     `reload.rs` `log_ignored_other_sections`), while `blockchain` /
///     `cache` (absent in the file) stay silent.
///   - **Reload #2** keeps `[network]` byte-identical → the
///     *unchanged-section* branch must **not** re-emit the notice
///     (suppression), while newly-introduced `blockchain` / `cache` /
///     `identity` / `gossip` changes each emit one — all alongside
///     mutable `payment` / `observability` changes that must still apply.
///
/// Runs on a single-thread runtime so the buffer-capturing subscriber
/// installed via `set_default` (thread-local) observes the reload, which
/// is awaited inline on the same thread rather than on a spawned task.
// One deliberately sequential narrative: two full SIGHUP→reload cycles
// whose ordering (baseline → suppression) is the property under test.
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
        PaymentArgs {
            rate_per_mb: None,
            delivery_floor: None,
            delivery_ceiling: None,
        },
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
    let shared_rate = state.rate_per_mb();

    let mut hup = {
        use tokio::signal::unix::{SignalKind, signal};
        signal(SignalKind::hangup()).expect("install SIGHUP stream")
    };

    // Count "ignoring change to <section>.*" notices in the buffer. We
    // assert on the stable section prefix + the `(requires restart)`
    // suffix rather than the full middle field list, so a cosmetic edit
    // to a field list in `warn_ignored` doesn't break the test while a
    // misclassification / suppression regression still does.
    let notice_count = |logs: &str, section: &str| -> usize {
        let prefix = format!("ignoring change to {section}.*");
        logs.lines()
            .filter(|l| l.contains(&prefix) && l.contains("(requires restart)"))
            .count()
    };

    // --- Reload #1: mutable fields + a restart-required `[network]`
    //     section against the empty startup baseline. Mutable changes
    //     apply; the absent→present network section hits the no-baseline
    //     conservative-warn path and must emit exactly one notice;
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
        shared_rate.load(Ordering::Relaxed),
        11,
        "mutable rate_per_mb must take effect on the first SIGHUP"
    );
    let after_first = captured_logs(&log_buf);
    assert_eq!(
        notice_count(&after_first, "network"),
        1,
        "a present restart-required [network] section against an empty \
         baseline must emit exactly one (requires restart) notice, got:\n{after_first}"
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

    // --- Reload #2: mutable fields change; `[network]` is byte-identical
    //     (must NOT re-warn — suppression branch); blockchain/cache/
    //     identity/gossip are newly introduced and changed (each must
    //     warn). Proves mutable changes still apply when bundled with
    //     restart-required ones.
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
         [gossip]\n\
         announce_interval_sec = 120\n",
    );
    raise_sighup_soon();
    hup.recv().await.expect("second SIGHUP");
    state.reload(&path).await.expect("second reload");

    // Mutable fields took effect even though the file also carried
    // restart-required changes.
    assert_eq!(
        shared_rate.load(Ordering::Relaxed),
        22,
        "mutable rate_per_mb must update even when the file also carries \
         restart-required changes"
    );
    assert_eq!(
        levels.lock().unwrap().clone(),
        vec![LogLevel::Info, LogLevel::Debug],
        "mutable log_level must update across both reloads"
    );

    let logs = captured_logs(&log_buf);

    // Suppression branch: an unchanged restart-required section must not
    // re-emit its notice on the second reload.
    assert_eq!(
        notice_count(&logs, "network"),
        1,
        "byte-identical [network] across reloads must NOT re-emit the \
         (requires restart) notice (suppression branch), got:\n{logs}"
    );

    // Newly-introduced restart-required sections were each rejected with
    // their own notice.
    for section in ["blockchain", "cache", "identity", "gossip"] {
        assert_eq!(
            notice_count(&logs, section),
            1,
            "expected exactly one (requires restart) notice for changed \
             [{section}] section, got:\n{logs}"
        );
    }
}
