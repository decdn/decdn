use std::sync::Arc;

use crate::captcha::CaptchaVerifier;
use crate::config::OnrampConfig;
use crate::daemon::{CapabilitySource, DaemonInfo};
use crate::store::Store;

/// Shared state handed to every HTTP handler.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub source: Arc<dyn CapabilitySource>,
    pub turnstile: Arc<dyn CaptchaVerifier>,
    pub cfg: Arc<OnrampConfig>,
    /// The daemon's `/v1/info`, read once at startup; the installers render
    /// its chain id and `PaymentPool` address.
    pub chain: Arc<DaemonInfo>,
}

/// Assemble `AppState`: read the daemon's `/v1/info`, refuse configured
/// terms it would reject, and open the grant store.
///
/// # Errors
///
/// The daemon is unreachable or rejects the token, the configured terms are
/// out of its bounds, or the store fails to open.
pub async fn build(
    cfg: OnrampConfig,
    source: Arc<dyn CapabilitySource>,
    turnstile: Arc<dyn CaptchaVerifier>,
) -> anyhow::Result<AppState> {
    let info = source
        .info()
        .await
        .map_err(|e| anyhow::anyhow!("read daemon info from {}: {e}", cfg.daemon_url))?;
    cfg.check_against(&info)?;
    let store = Arc::new(Store::open(&cfg.data_dir)?);
    Ok(AppState {
        store,
        source,
        turnstile,
        cfg: Arc::new(cfg),
        chain: Arc::new(info),
    })
}
