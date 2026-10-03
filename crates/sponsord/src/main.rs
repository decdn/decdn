use std::sync::Arc;

use sponsord::config::DaemonConfig;
use sponsord::http::{self, ApiState};
use sponsord_core::Sponsor;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

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
    let shutdown = CancellationToken::new();
    tokio::spawn(sponsor.keeper(keeper, shutdown.clone()));
    let app = http::router(ApiState::new(sponsor, api_token));
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "sponsord listening");
    axum::serve(listener, app).await?;
    Ok(())
}
