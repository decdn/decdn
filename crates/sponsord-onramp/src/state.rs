use std::sync::Arc;

use sponsord_api::MicroUsdc;
use sponsord_api::onramp::Profile;
use sponsord_api::time::{Clock, SystemClock};

use crate::config::OnrampConfig;
use crate::daemon::CapabilitySource;
use crate::gate::Gate;
use crate::grants::GrantCache;
use crate::installer::Installers;
use crate::net::{ClientIpSource, RateLimiter};

/// The terms the onramp asks the daemon for; `None` takes its maximum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequestedTerms {
    pub spending_cap: Option<MicroUsdc>,
    pub ttl_secs: Option<u64>,
}

/// Shared state handed to every HTTP handler. It holds no secrets: those
/// stay inside the gate and the daemon client.
#[derive(Clone)]
pub struct AppState {
    pub gate: Arc<dyn Gate>,
    pub daemon: Arc<dyn CapabilitySource>,
    pub grants: Arc<GrantCache>,
    pub terms: RequestedTerms,
    /// Served at `GET /v1/profile`; its chain id and `PaymentPool` come from
    /// the daemon's `/v1/info`, read once at startup.
    pub profile: Arc<Profile>,
    pub installers: Arc<Installers>,
    pub clock: Arc<dyn Clock>,
    pub client_ip: ClientIpSource,
    pub fund_limit: Arc<RateLimiter>,
    pub poll_limit: Arc<RateLimiter>,
}

/// Assemble `AppState`: read the daemon's `/v1/info`, refuse configured
/// terms it would reject, and render the installers.
///
/// # Errors
///
/// The daemon is unreachable or rejects the token, or the configured terms
/// are out of its bounds.
pub async fn build(
    cfg: &OnrampConfig,
    daemon: Arc<dyn CapabilitySource>,
    gate: Arc<dyn Gate>,
) -> anyhow::Result<AppState> {
    let info = daemon
        .info()
        .await
        .map_err(|e| anyhow::anyhow!("read daemon info from {}: {e}", cfg.daemon_url))?;
    cfg.check_against(&info)?;
    Ok(AppState {
        gate,
        daemon,
        grants: Arc::default(),
        terms: RequestedTerms {
            spending_cap: cfg.spending_cap,
            ttl_secs: cfg.ttl_secs,
        },
        profile: Arc::new(Profile {
            chain_id: info.chain_id,
            rpc_url: cfg.rpc_url.clone(),
            payment_pool: info.payment_pool,
            capacity_bond: cfg.capacity_bond,
            slash_judge: cfg.slash_judge,
            min_cli_version: cfg.min_cli_version.clone(),
        }),
        installers: Arc::new(Installers::render(cfg)),
        clock: Arc::new(SystemClock),
        client_ip: cfg.client_ip.clone(),
        fund_limit: Arc::new(RateLimiter::new(cfg.fund_rate_per_min)),
        poll_limit: Arc::new(RateLimiter::new(cfg.poll_rate_per_min)),
    })
}
