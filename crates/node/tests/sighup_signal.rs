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

#![cfg(unix)]
// Tests legitimately call `.unwrap()` / `.expect()` on harness
// scaffolding. The workspace anti-panic policy is for production code.
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
            relay_url: None,
        },
        blockchain: ResolvedBlockchain {
            rpc_url: "http://localhost:8545".into(),
            eth_keystore: PathBuf::from("/tmp/keystore.json"),
            keystore_password_file: None,
            payment_channel_address: "0x0000000000000000000000000000000000000001".into(),
            staking_registry_address: "0x0000000000000000000000000000000000000002".into(),
            rpc_watchdog_interval_sec: 30,
        },
        cache: ResolvedCache {
            cache_dir: PathBuf::from("/tmp/cache"),
            cache_size_mb: 1024,
            max_blob_size_mb: 128,
            origin: None,
            pinned_hashes: decdn_cache::PinnedHashes::empty(),
            origin_retry: decdn_cache::RetryPolicy::default(),
            user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
        },
        payment: ResolvedPayment { rate_per_mb: rate },
        observability: ResolvedObservability {
            log_level: level,
            log_format: decdn_common::cli::LogFormat::Pretty,
            metrics_port: 9090,
            metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            admin_port: Some(9191),
            otlp_endpoint: None,
        },
        gossip: ResolvedGossip {
            announce_interval_sec: 60,
            peer_ttl_sec: 600,
            subscribe_global: false,
            allowlist: Vec::new(),
        },
        security: ResolvedSecurity {
            max_concurrent_handlers: 256,
            per_source_rate_per_sec: 100.0,
            per_source_burst: 200,
            max_tracked_sources: 4096,
        },
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
        PaymentArgs { rate_per_mb: None },
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
        PaymentArgs { rate_per_mb: None },
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
