use std::sync::Arc;

use crate::cap::Cap;
use crate::captcha::Turnstile;
use crate::config::ServerConfig;
use crate::discovery::Discovery;
use crate::store::Store;
use crate::treasury::Treasury;

/// Shared server state handed to every HTTP handler.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub cap: Cap,
    pub treasury: Arc<dyn Treasury>,
    pub turnstile: Arc<Turnstile>,
    pub cfg: Arc<ServerConfig>,
    pub discovery: Arc<Discovery>,
}

/// Assemble the full `AppState` from a `ServerConfig`: opens the redb store,
/// builds the monthly cap policy and the Turnstile client, connects the
/// on-chain treasury via `cfg.build_treasury()`, and wires up hash → node
/// discovery against the same RPC and `CapacityBond` address.
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
    let discovery = Arc::new(Discovery {
        rpc_url: cfg.rpc_url.clone(),
        capacity_bond: cfg.capacity_bond,
    });
    Ok(AppState {
        store,
        cap,
        treasury,
        turnstile,
        cfg: Arc::new(cfg),
        discovery,
    })
}
