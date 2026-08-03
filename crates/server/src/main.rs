use sponsord::config::ServerConfig;
use sponsord::{http, state};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = ServerConfig::from_env()?;
    let bind = cfg.bind;
    let app_state = state::build(cfg).await?;
    let app = http::router(app_state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("sponsord listening on {bind}");
    axum::serve(listener, app).await?;
    Ok(())
}
