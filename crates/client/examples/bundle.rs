//! Pull a bundle into a directory as a delegate with [`BundlePuller`], and
//! swap in a fresh capability when the pull reports that the current one runs
//! low.
//!
//! ```text
//! cargo run -p decdn-client --example bundle -- <bundle-hash-hex> <output-dir>
//! ```
//!
//! The deployment comes from `DECDN_RPC_URL`, `DECDN_CHAIN_ID`,
//! `DECDN_PAYMENT_POOL`, `DECDN_SLASH_JUDGE` and `DECDN_CAPACITY_BOND`, and the
//! local state lives in `DECDN_DATA_DIR`. The delegate is the keystore at
//! `DECDN_KEYSTORE` (password in `DECDN_KEYSTORE_PASSWORD`) with the `dcap1:`
//! capability token in `DECDN_CAPABILITY`.
//!
//! A registered signer's terms are write-once on chain, so a fresh capability
//! names a fresh key. When the pull signals that the capability runs low, the
//! example loads the next one from `DECDN_KEYSTORE_NEXT` and
//! `DECDN_CAPABILITY_NEXT`, where an application would ask its sponsor
//! gateway, and swaps it in. The pull goes on under the new key at the next
//! voucher boundary. Without a next credential, a pull the capability cannot
//! finish ends with [`BundleEnd::FundingNeeded`].

use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result, bail};
use decdn_client::bundle::{BundleEnd, BundlePuller, Funding, parse_hash, select_nodes};
use decdn_client::session::{ChainConfig, DiscoveryOptions, PullSite};
use decdn_client::{CredentialSlot, FundingEvent, endpoint};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity::load_signer;
use decdn_incentive::{CapabilityGrant, SignedCapability};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let mut args = std::env::args().skip(1);
    let hash = parse_hash(
        &args
            .next()
            .context("pass the bundle manifest's BLAKE3 hash")?,
    )?;
    let out_dir = PathBuf::from(args.next().context("pass an output directory")?);

    let chain = ChainConfig {
        rpc_url: var("DECDN_RPC_URL")?,
        chain_id: var("DECDN_CHAIN_ID")?.parse()?,
        payment_pool: var("DECDN_PAYMENT_POOL")?.parse()?,
        slash_judge: var("DECDN_SLASH_JUDGE")?.parse()?,
        capacity_bond: Some(var("DECDN_CAPACITY_BOND")?.parse()?),
    };
    let site = PullSite {
        data_dir: var("DECDN_DATA_DIR")?.into(),
        region: None,
        region_allowlist: Vec::new(),
    };
    let discovery = DiscoveryOptions::default();

    // Resolve the holders first, so a failed discovery costs no key unlock.
    let nodes = select_nodes(&chain, &site, &discovery).await?;
    let (signer, capability) = credential("DECDN_KEYSTORE", "DECDN_CAPABILITY")?;
    let slot = CredentialSlot::new(signer, capability)?;

    // The store records each lane's watermark; keep it for the life of the
    // capability.
    let store = Arc::new(RedbBuyerPoolStore::open(&site.data_dir)?);
    let relays = endpoint::resolve_relays(None, None)?;
    let endpoint = endpoint::client_endpoint(&relays, &endpoint::client_discovery(None)?).await?;
    let mut puller = BundlePuller::builder(
        chain,
        site,
        endpoint.clone(),
        store,
        Funding::Delegated { slot: slot.clone() },
        discovery,
        nodes,
    )
    .relays(relays)
    .build()
    .await?;

    let manifest = puller.fetch_manifest(hash, &out_dir, true).await?;
    // The pull future runs on this task, so the swap watch runs beside it here
    // instead of on a spawned task.
    let cancel = CancellationToken::new();
    let outcome = tokio::select! {
        outcome = puller.pull(&manifest, &out_dir, false, &cancel) => outcome,
        swapped = swap_when_low(&slot) => match swapped {
            Err(e) => return Err(e),
            Ok(never) => match never {},
        },
    };
    // Every lane's payment lands in the store before the endpoint closes.
    puller.finish().await;
    endpoint.close().await;

    let report = &outcome.report;
    for failure in &report.failures {
        tracing::warn!("failed: {}: {}", failure.path, failure.error);
    }
    tracing::info!(
        "{} fetched, {} linked, {} skipped, {} failed; downloaded {} bytes",
        report.fetched,
        report.linked,
        report.skipped,
        report.failed,
        report.downloaded
    );
    match outcome.end {
        BundleEnd::Complete if report.failed == 0 => Ok(()),
        BundleEnd::Complete => bail!("{} entries failed; rerun to retry them", report.failed),
        BundleEnd::FundingNeeded { error, .. } | BundleEnd::Stopped(error) => Err(error),
        _ => bail!("the pull stopped early; rerun to resume it"),
    }
}

/// Swap the next credential into `slot` the first time the pull reports that
/// the current one runs low. Returns only on an error.
async fn swap_when_low(slot: &CredentialSlot) -> Result<Infallible> {
    let mut events = slot.funding_events();
    let mut swapped = false;
    loop {
        events.changed().await?;
        let low = matches!(
            *events.borrow_and_update(),
            Some(FundingEvent::RunningLow { .. })
        );
        if low && !swapped {
            swapped = true;
            match credential("DECDN_KEYSTORE_NEXT", "DECDN_CAPABILITY_NEXT") {
                Ok((signer, capability)) => {
                    slot.swap(signer, capability)?;
                    tracing::info!("the capability ran low; swapped in the next one");
                }
                Err(e) => tracing::warn!("the capability runs low, and no next one: {e:#}"),
            }
        }
    }
}

/// The delegate key in the keystore `keystore_var` names, and the capability
/// token in `capability_var`.
fn credential(
    keystore_var: &str,
    capability_var: &str,
) -> Result<(Arc<PrivateKeySigner>, SignedCapability)> {
    let signer = load_signer(
        &PathBuf::from(var(keystore_var)?),
        &var("DECDN_KEYSTORE_PASSWORD")?,
    )?;
    let grant = CapabilityGrant::from_token(&var(capability_var)?)?;
    Ok((Arc::new(signer), grant.to_signed_capability()?))
}

/// The environment variable `name`.
fn var(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("set {name}"))
}
