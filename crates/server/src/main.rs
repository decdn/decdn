use std::time::Duration;

use sponsord::config::ServerConfig;
use sponsord::{http, reclaim, state};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = ServerConfig::from_env()?;
    let bind = cfg.bind;
    let ttl_secs = cfg.channel_ttl_secs;
    let reclaim_interval = Duration::from_secs(cfg.reclaim_interval_secs);
    let app_state = state::build(cfg).await?;
    tokio::spawn(reclaim::run(app_state.clone(), reclaim_interval, ttl_secs));
    let app = http::router(app_state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("sponsord listening on {bind}");
    axum::serve(listener, app).await?;
    Ok(())
}
