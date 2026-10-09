//! The `sponsord` daemon: loads the treasury key, checks it owns the pool,
//! then serves the HTTP API and runs the pool keeper until Ctrl-C or SIGTERM.

use std::sync::Arc;

use sponsord::config::DaemonConfig;
use sponsord::http::{self, ApiState};
use sponsord_core::Sponsor;
use tokio_util::sync::CancellationToken;

/// Print a fatal error through [`decdn_common::redact::sanitize_err_chain`]
/// instead of returning it, which would let std's `Termination` impl
/// Debug-print it unredacted. `ChainPool` already strips the RPC URL, whose
/// path or query often holds an API key, from its errors; this is the
/// backstop for any other error that names it (#38).
#[tokio::main]
#[expect(
    clippy::print_stderr,
    reason = "process exit boundary: the final error line, before or after tracing"
)]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {}", decdn_common::redact::sanitize_err_chain(&e));
            std::process::ExitCode::FAILURE
        }
    }
}

/// `RUST_LOG` filtering, as `tracing_subscriber::fmt::init` does, with
/// `alloy_transport_http` held at `info` whatever `RUST_LOG` says: it logs
/// only at debug and trace, inside a `debug_span!` that records the RPC URL,
/// and every event inside that span (reqwest's, hyper's) would print it (#38).
fn init_tracing() -> anyhow::Result<()> {
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("alloy_transport_http=info".parse()?);
    tracing_subscriber::fmt().with_env_filter(filter).init();
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    init_tracing()?;
    let DaemonConfig {
        bind,
        api_token,
        treasury_keystore,
        treasury_password,
        chain,
        limits,
        keeper,
    } = DaemonConfig::load()?;
    // Moved, not cloned, into the loader: the password is dropped (and wiped)
    // once the key is decrypted instead of living for the daemon's lifetime.
    let signer = tokio::task::spawn_blocking(move || {
        decdn_incentive::eth_identity::load_signer(&treasury_keystore, treasury_password.expose())
    })
    .await??;
    let sponsor = Arc::new(Sponsor::connect(signer, chain, limits).await?);

    // Bound before the keeper starts: a bind failure that returned while a
    // top-up was in flight would drop it between the approve and the top-up.
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let shutdown = CancellationToken::new();
    let keeper = tokio::spawn(sponsor.keeper(keeper, shutdown.clone()));
    let app = http::router(ApiState::new(sponsor, api_token));
    tracing::info!(%bind, "sponsord listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await?;
    // Let a top-up already sent finish before exiting.
    keeper.await?;
    Ok(())
}

/// Resolve on Ctrl-C or SIGTERM, cancelling `shutdown`. A handler that fails
/// to register waits forever instead of resolving, so only an actual signal
/// shuts the daemon down.
async fn shutdown_signal(shutdown: CancellationToken) {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = term => {}
    }
    tracing::info!("shutting down");
    shutdown.cancel();
}
