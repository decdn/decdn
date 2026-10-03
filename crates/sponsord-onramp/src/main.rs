use std::sync::Arc;
use std::time::Duration;

use sponsord_api::client::DaemonClient;
use sponsord_onramp::captcha::{CaptchaVerifier, Turnstile};
use sponsord_onramp::config::OnrampConfig;
use sponsord_onramp::daemon::CapabilitySource;
use sponsord_onramp::{http, state};

/// Bound on every outbound call (daemon and Turnstile), so a hung peer
/// fails a request instead of holding it open.
const OUTBOUND_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = OnrampConfig::load()?;
    let http_client = reqwest::Client::builder()
        .timeout(OUTBOUND_TIMEOUT)
        .build()?;
    let source: Arc<dyn CapabilitySource> = Arc::new(DaemonClient::new(
        &cfg.daemon_url,
        cfg.daemon_token.clone(),
        http_client.clone(),
    ));
    let turnstile: Arc<dyn CaptchaVerifier> =
        Arc::new(Turnstile::new(cfg.turnstile_secret.clone(), http_client));
    let bind = cfg.bind;
    let app = http::router(state::build(cfg, source, turnstile).await?);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "sponsord-onramp listening");
    axum::serve(listener, app).await?;
    Ok(())
}
