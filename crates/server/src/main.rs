use std::time::Duration;

use sponsord::config::ServerConfig;
use sponsord::{http, pool_watch, state};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = ServerConfig::from_env()?;
    let bind = cfg.bind;
    let pool_id = cfg.pool_id;
    let low_water = cfg.pool_low_water;
    let refill = cfg.pool_refill;
    let watch_interval = Duration::from_secs(cfg.pool_watch_interval_secs);
    let app_state = state::build(cfg).await?;
    tokio::spawn(pool_watch::run(
        app_state.clone(),
        watch_interval,
        low_water,
        refill,
        pool_id,
    ));
    let app = http::router(app_state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "sponsord listening");
    axum::serve(listener, app).await?;
    Ok(())
}
