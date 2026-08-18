use std::sync::Arc;

use crate::captcha::{CaptchaVerifier, Turnstile};
use crate::config::ServerConfig;
use crate::issuer::Issuer;
use crate::store::Store;
use crate::treasury::Treasury;

/// Shared server state handed to every HTTP handler.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub treasury: Arc<dyn Treasury>,
    pub issuer: Arc<Issuer>,
    pub turnstile: Arc<dyn CaptchaVerifier>,
    pub cfg: Arc<ServerConfig>,
}

/// Assemble `AppState`: open the store, load the hot wallet once, build the
/// pool treasury + capability issuer from it, and confirm this wallet owns
/// the configured pool before serving.
pub async fn build(cfg: ServerConfig) -> anyhow::Result<AppState> {
    let store = Arc::new(Store::open(&cfg.data_dir)?);
    let turnstile: Arc<dyn CaptchaVerifier> = Arc::new(Turnstile::new(
        cfg.turnstile_secret.clone(),
        reqwest::Client::new(),
    ));
    let signer = cfg.load_treasury_signer().await?;
    let issuer = Arc::new(cfg.build_issuer(signer.clone()));
    let treasury: Arc<dyn Treasury> = Arc::from(cfg.build_treasury(signer).await?);

    // Boot check: the hot wallet must own the configured pool, else every
    // capability we sign is worthless (the node recovers a non-owner).
    let owner = treasury.pool_owner(cfg.pool_id).await?;
    anyhow::ensure!(
        owner == treasury.owner_address(),
        "configured SPONSOR_POOL_ID {} is owned on-chain by {owner}, not the treasury wallet {} \
         — wrong pool id, keystore, or contract",
        cfg.pool_id,
        treasury.owner_address()
    );

    Ok(AppState {
        store,
        treasury,
        issuer,
        turnstile,
        cfg: Arc::new(cfg),
    })
}
