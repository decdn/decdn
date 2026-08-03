use std::sync::Arc;

use crate::cap::Cap;
use crate::captcha::Turnstile;
use crate::config::ServerConfig;
use crate::store::Store;
use crate::treasury::Treasury;

/// Shared server state handed to every HTTP handler.
///
/// Note: the plan's `AppState` also carries a `discovery: Arc<Discovery>`
/// field, but `Discovery` is built in Task 10 (not yet implemented). It is
/// omitted here for now; Task 10/11 adds it.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub cap: Cap,
    pub treasury: Arc<dyn Treasury>,
    pub turnstile: Arc<Turnstile>,
    pub cfg: Arc<ServerConfig>,
}

/// Assemble the full `AppState` from a `ServerConfig`: opens the redb store,
/// builds the monthly cap policy and the Turnstile client, and connects the
/// on-chain treasury via `cfg.build_treasury()`.
pub async fn build(cfg: ServerConfig) -> anyhow::Result<AppState> {
    let store = Arc::new(Store::open(&cfg.data_dir)?);
    let cap = Cap {
        monthly_limit: cfg.monthly_cap,
    };
    let turnstile = Arc::new(Turnstile::new(
        cfg.turnstile_secret.clone(),
        reqwest::Client::new(),
    ));
    let treasury: Arc<dyn Treasury> = Arc::from(cfg.build_treasury().await?);
    Ok(AppState {
        store,
        cap,
        treasury,
        turnstile,
        cfg: Arc::new(cfg),
    })
}
