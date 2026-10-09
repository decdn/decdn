//! [`AppState`], the state every HTTP handler shares, and [`build`], which
//! assembles it at startup.

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
    /// Spending cap per capability (`ONRAMP_SPENDING_CAP_MICRO_USDC`).
    pub spending_cap: Option<MicroUsdc>,
    /// Lifetime per capability, in seconds (`ONRAMP_TTL_SECS`).
    pub ttl_secs: Option<u64>,
}

/// Shared state handed to every HTTP handler. It holds no secrets: those
/// stay inside the gate and the daemon client.
#[derive(Clone)]
pub struct AppState {
    /// What a person must pass before `POST /v1/fund` issues anything.
    pub gate: Arc<dyn Gate>,
    /// Where capabilities come from: the sponsord daemon.
    pub daemon: Arc<dyn CapabilitySource>,
    /// Capabilities issued and waiting for the polling CLI.
    pub grants: Arc<GrantCache>,
    /// The terms asked of the daemon for every capability.
    pub terms: RequestedTerms,
    /// Served at `GET /v1/profile`; its chain id and `PaymentPool` come from
    /// the daemon's `/v1/info`, read once at startup.
    pub profile: Arc<Profile>,
    /// The installer scripts, rendered once at startup.
    pub installers: Arc<Installers>,
    /// The time source for rate limits and hand-off expiry.
    pub clock: Arc<dyn Clock>,
    /// Where the requester's address comes from.
    pub client_ip: ClientIpSource,
    /// Per-address limit on `POST /v1/fund`.
    pub fund_limit: Arc<RateLimiter>,
    /// Per-address limit on `GET /v1/capability`.
    pub poll_limit: Arc<RateLimiter>,
}

// By hand: the gate, the daemon client and the clock are trait objects.
impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("terms", &self.terms)
            .field("profile", &self.profile)
            .field("client_ip", &self.client_ip)
            .finish_non_exhaustive()
    }
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
