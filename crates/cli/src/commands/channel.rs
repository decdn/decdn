//! `decdn channel` — client-side payment-channel lifecycle.
//!
//! `coop-close` cooperatively settles the channel tracked for a provider
//! (#971): it dials the provider over `cdn/client/v1`, requests a
//! `CooperativeClose` waiver over the channel's final watermark, and submits
//! `cooperativeClose` on-chain — one tx, no 48h dispute window (ADR 003
//! §Cooperative close). The reusable request→verify→submit core lives in
//! [`decdn_client_pull::cooperative_close`]; this command is the thin
//! flag/store/endpoint wiring around it. The node→node buyer reconcile (#972)
//! drives the same core on a timer.
//!
//! If the provider has no channel / no accepted voucher it declines and the
//! channel must be closed the ordinary way (`closeChannel` → dispute window →
//! `settleChannel`); auto-fallback is out of scope here.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::Address;
use decdn_client_pull::cooperative_close::{
    AuthorizedWatermark, CooperativeCloseOutcome, cooperative_close,
};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_channel::BuyerChannelStore;
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::voucher_domain;
use iroh::{EndpointAddr, PublicKey};

use super::chain_ctx;
use decdn_client_pull::endpoint as client_endpoint;
use decdn_client_pull::provider;

/// Dispatch `decdn channel <subcommand>`.
pub async fn channel_dispatch(
    args: &cli::ChannelArgs,
    config_path: Option<&Path>,
) -> anyhow::Result<()> {
    match &args.command {
        cli::ChannelCommand::CoopClose(a) => coop_close(a, config_path).await,
    }
}

/// Chain coordinates resolved flag > `[blockchain]`/`[identity]` config >
/// default. Pure (parse-only) so the precedence is unit-testable.
#[derive(Debug)]
struct ResolvedChain {
    rpc_url: String,
    payment_channel: Address,
    chain_id: u64,
    data_dir: PathBuf,
    keystore: PathBuf,
}

fn resolve_chain(args: &cli::CoopCloseArgs, file: &FileConfig) -> anyhow::Result<ResolvedChain> {
    let bc = file.blockchain.as_ref();
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| bc.and_then(|b| b.rpc_url.clone()))
        .ok_or_else(|| anyhow::anyhow!("rpc_url not set (--rpc-url or blockchain.rpc_url)"))?;
    let payment_channel_raw = args
        .payment_channel_address
        .clone()
        .or_else(|| bc.and_then(|b| b.payment_channel_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "payment_channel_address not set (--payment-channel-address or \
                 blockchain.payment_channel_address)"
            )
        })?;
    let payment_channel =
        chain_ctx::parse_address(&payment_channel_raw, "payment_channel_address")?;
    let chain_id = args
        .chain_id
        .or_else(|| bc.and_then(|b| b.chain_id))
        .unwrap_or(DEFAULT_CHAIN_ID);
    let data_dir = args
        .data_dir
        .clone()
        .or_else(|| file.identity.as_ref().and_then(|i| i.data_dir.clone()))
        .map(|p| expand_tilde(&p))
        .or_else(cli::default_data_dir)
        .ok_or_else(|| {
            anyhow::anyhow!("data_dir not set and no default available (pass --data-dir)")
        })?;
    let keystore = args
        .keystore
        .clone()
        .or_else(|| bc.and_then(|b| b.eth_keystore.clone()))
        .map_or_else(
            || eth_identity::keystore_path(&data_dir),
            |p| expand_tilde(&p),
        );
    Ok(ResolvedChain {
        rpc_url,
        payment_channel,
        chain_id,
        data_dir,
        keystore,
    })
}

async fn coop_close(args: &cli::CoopCloseArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(args, &file)?;

    let node_id = PublicKey::from_str(&args.node_id)
        .map_err(|e| anyhow::anyhow!("invalid --node-id {:?}: {e}", args.node_id))?;
    let provider = chain_ctx::parse_address(&args.provider_address, "--provider-address")?;

    // The channel to close is the one tracked for this provider; its watermark is
    // exactly what this client paid, and the cap on what we'll sign away.
    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;
    let state = store.get_by_provider(provider)?.ok_or_else(|| {
        anyhow::anyhow!("no buyer channel tracked for provider {provider} — nothing to close")
    })?;
    let authorized = AuthorizedWatermark {
        amount: state.last_amount,
        nonce: state.last_nonce,
        bytes_delivered: state.last_bytes_delivered,
    };

    // Buyer signer (the client voucher + the cooperativeClose tx). Password from
    // env, else TTY.
    let password = read_password(
        &[
            PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV),
            PasswordSource::Prompt { confirm: false },
        ],
        "eth keystore password",
    )?;
    let signer = Arc::new(load_signer(&chain.keystore, &password)?);
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentChannel::new(chain.payment_channel, rpc);
    let domain = voucher_domain(chain.chain_id, chain.payment_channel);

    let relays = client_endpoint::resolve_relays(args.relay_url.as_deref(), config_path)?;
    let disc = client_endpoint::client_discovery(config_path)?;
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;
    let mut target = EndpointAddr::new(node_id);
    if let Some(addr) = args.addr {
        target = target.with_ip_addr(addr);
    }
    if let Some(url) = relays.first() {
        target = target.with_relay_url(url.clone());
    }

    let outcome = cooperative_close(
        &endpoint,
        target,
        &contract,
        state.channel_id,
        provider,
        state.token,
        authorized,
        &signer,
        &domain,
        Duration::from_millis(args.timeout_ms),
    )
    .await?;

    match outcome {
        CooperativeCloseOutcome::Settled => {
            // Deposit refunded on-chain; drop the now-terminal channel so a later
            // fetch opens a fresh one. A failed forget only risks a stale reuse
            // attempt (rejected on-chain), so warn rather than fail the close.
            if let Err(e) = store.forget_if_channel(provider, state.channel_id) {
                eprintln!(
                    "warning: channel {} settled on-chain but clearing it from the buyer store \
                     failed: {e}",
                    state.channel_id
                );
            }
            println!(
                "cooperatively closed channel {} (provider {provider}); deposit refunded",
                state.channel_id
            );
            Ok(())
        }
        CooperativeCloseOutcome::Declined => {
            anyhow::bail!(
                "provider {provider} declined the cooperative close (no channel or no voucher \
                 yet); close it the ordinary way with closeChannel"
            )
        }
        CooperativeCloseOutcome::Reverted => {
            anyhow::bail!(
                "cooperativeClose reverted on-chain for channel {} (it may have raced a \
                 withdraw/close, or the watermark regressed); close it the ordinary way",
                state.channel_id
            )
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn args() -> cli::CoopCloseArgs {
        cli::CoopCloseArgs {
            node_id: "n".into(),
            addr: None,
            relay_url: None,
            provider_address: "0x0000000000000000000000000000000000000001".into(),
            rpc_url: None,
            payment_channel_address: None,
            chain_id: None,
            keystore: None,
            data_dir: Some(PathBuf::from("/tmp/d")),
            timeout_ms: 30_000,
        }
    }

    fn config(body: &str) -> FileConfig {
        toml::from_str(body).unwrap()
    }

    #[test]
    fn flags_override_config() {
        let mut a = args();
        a.rpc_url = Some("http://flag:8545".into());
        a.chain_id = Some(99);
        let pc = "0x1111111111111111111111111111111111111111";
        a.payment_channel_address = Some(pc.into());
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\n\
             payment_channel_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&a, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.chain_id, 99);
        assert_eq!(r.payment_channel, Address::from_str(pc).unwrap());
    }

    #[test]
    fn config_fills_unset_flags_and_chain_id_defaults() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_channel_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&args(), &file).unwrap();
        assert_eq!(r.rpc_url, "http://config:8545");
        assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(
            r.keystore,
            eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
        );
    }

    #[test]
    fn missing_payment_channel_errors() {
        let file = config("[blockchain]\nrpc_url = \"http://config:8545\"\n");
        let err = resolve_chain(&args(), &file).unwrap_err();
        assert!(
            err.to_string().contains("payment_channel_address not set"),
            "{err}"
        );
    }
}
