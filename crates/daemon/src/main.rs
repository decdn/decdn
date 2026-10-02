use std::sync::Arc;

use sponsord::config::DaemonConfig;
use sponsord::http::{self, ApiState};
use sponsord_core::Sponsor;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = DaemonConfig::from_env()?;
    let sponsor = Arc::new(Sponsor::connect(cfg.sponsor).await?);
    tokio::spawn(sponsor.pool_keeper(cfg.pool_low_water, cfg.pool_refill, cfg.pool_watch_interval));
    let app = http::router(ApiState {
        sponsor,
        api_token: Arc::from(cfg.api_token),
    });
    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    tracing::info!(bind = %cfg.bind, "sponsord listening");
    axum::serve(listener, app).await?;
    Ok(())
}
