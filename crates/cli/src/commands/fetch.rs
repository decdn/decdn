//! `decdn fetch` — standalone client-side paid pull of a single
//! content-addressed blob over `cdn/client/v1` (issues #391, #940).
//!
//! Turnkey paying sibling of [`super::probe`]: dial a node by explicit
//! `--node-id`/`--addr`/`--relay-url`, **auto-open-or-reuse** a `PaymentChannel`
//! with `--provider-address`, run one delivery exchange via
//! [`decdn_client_pull::stream_fetch_tracked`] (signing cumulative vouchers,
//! resuming the channel's persisted watermark), verify the `slash_sig` recovers
//! to the provider (ADR 014 §1), BLAKE3-check the whole blob, persist the new
//! watermark, and write the bytes atomically.
//!
//! Channel lifecycle (#940): a live channel for the provider in the persistent
//! [`RedbBuyerChannelStore`] is reused (watermark resumed); otherwise one is
//! opened on-chain (USDC `approve` if needed → `openChannel`) via the shared
//! [`decdn_client_pull::buyer_channel::open_channel`] kernel and recorded. The
//! chain coordinates resolve flag > `[blockchain]`/`[identity]` config > default.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_client_pull::buyer_channel::{ensure_allowance, open_channel};
use decdn_client_pull::{ChannelContext, VoucherProgress, stream_fetch_tracked};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_common::identity::fresh_secret_key;
use decdn_incentive::buyer_channel::{AdvanceOutcome, BuyerChannelStore};
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{slash_judge_domain, voucher_domain};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, PublicKey};

use super::{chain_ctx, client_endpoint};

/// Default deposit when opening a new channel: 10 USDC (ADR 003 § Deposit
/// Economics recommended minimum). Clamped up to the on-chain `minDeposit`.
const DEFAULT_DEPOSIT_MICRO_USDC: u64 = 10_000_000;

/// Parse a user-supplied BLAKE3 hash (64 hex chars, optional `0x` prefix).
fn parse_hash(s: &str) -> anyhow::Result<[u8; 32]> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    let h = blake3::Hash::from_hex(hex).map_err(|e| {
        anyhow::anyhow!("invalid --hash {s:?}: expected 64 hex chars (BLAKE3 digest): {e}")
    })?;
    Ok(*h.as_bytes())
}

/// Chain coordinates resolved flag > `[blockchain]`/`[identity]` config >
/// default. Pure (parse-only) so the precedence is unit-testable.
#[derive(Debug)]
struct ResolvedChain {
    rpc_url: String,
    payment_channel: Address,
    slash_judge: Address,
    chain_id: u64,
    keystore: PathBuf,
    data_dir: PathBuf,
    deposit: U256,
    max_approve: bool,
}

fn resolve_chain(args: &cli::FetchArgs, file: &FileConfig) -> anyhow::Result<ResolvedChain> {
    let bc = file.blockchain.as_ref();
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| bc.and_then(|b| b.rpc_url.clone()))
        .ok_or_else(|| anyhow::anyhow!("rpc_url not set (--rpc-url or blockchain.rpc_url)"))?;

    let pc_raw = args
        .payment_channel_address
        .clone()
        .or_else(|| bc.and_then(|b| b.payment_channel_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "payment_channel_address not set (--payment-channel-address or \
                 blockchain.payment_channel_address)"
            )
        })?;
    let payment_channel = chain_ctx::parse_address(&pc_raw, "payment_channel_address")?;

    let sj_raw = args
        .slash_judge_address
        .clone()
        .or_else(|| bc.and_then(|b| b.slash_judge_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "slash_judge_address not set (--slash-judge-address or \
                 blockchain.slash_judge_address)"
            )
        })?;
    let slash_judge = chain_ctx::parse_address(&sj_raw, "slash_judge_address")?;

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

    let deposit = U256::from(
        args.deposit_micro_usdc
            .or_else(|| bc.and_then(|b| b.buyer_deposit_micro_usdc))
            .unwrap_or(DEFAULT_DEPOSIT_MICRO_USDC),
    );
    let max_approve = bc.and_then(|b| b.buyer_max_approve).unwrap_or(true);

    Ok(ResolvedChain {
        rpc_url,
        payment_channel,
        slash_judge,
        chain_id,
        keystore,
        data_dir,
        deposit,
        max_approve,
    })
}

/// Current unix time in seconds (for channel-expiry checks).
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Fetch a single blob over `cdn/client/v1`, auto-opening/reusing a payment
/// channel, and write it atomically to `--output`. `config_path` (the global
/// `--config`) supplies relays (#935) and the chain coordinates.
pub async fn fetch(args: &cli::FetchArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let hash = parse_hash(&args.hash)?;
    let provider = chain_ctx::parse_address(&args.provider_address, "--provider-address")?;
    let node_id = PublicKey::from_str(&args.node_id)
        .map_err(|e| anyhow::anyhow!("invalid --node-id {:?}: {e}", args.node_id))?;

    // Relays: `--relay-url` overrides `network.relay_urls` (#935).
    let relays = client_endpoint::resolve_relays(args.relay_url.as_deref(), config_path)?;
    if args.addr.is_none() && relays.is_empty() {
        anyhow::bail!(
            "no way to reach the node: pass --addr, or set network.relay_urls in config \
             (or --relay-url)"
        );
    }

    let file = load_file_config(config_path)?;
    let chain = resolve_chain(args, &file)?;

    // Buyer signer (vouchers + the openChannel tx). Password from env, else TTY.
    let password = read_password(
        &[
            PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV),
            PasswordSource::Prompt { confirm: false },
        ],
        "eth keystore password",
    )?;
    let signer = Arc::new(load_signer(&chain.keystore, &password)?);
    let self_address = signer.address();

    let rpc = chain_ctx::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentChannel::new(chain.payment_channel, rpc.clone());
    let voucher_dom = voucher_domain(chain.chain_id, chain.payment_channel);
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);

    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;

    // Reuse a live channel for this provider (resuming its watermark), else open
    // and persist a new one.
    let ctx = open_or_reuse(
        &store,
        &contract,
        &rpc,
        &signer,
        &voucher_dom,
        provider,
        self_address,
        chain.payment_channel,
        chain.deposit,
        chain.max_approve,
    )
    .await?;
    let channel_id = ctx.channel_id;

    // Client endpoint — same minimal one-shot setup as `probe`.
    let bind_addr = std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0);
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(fresh_secret_key())
        .relay_mode(client_endpoint::relay_mode(&relays))
        .max_tls_tickets(decdn_protocol::SESSION_TICKET_CACHE_SIZE)
        .bind_addr(bind_addr)
        .map_err(|e| anyhow::anyhow!("invalid bind addr {bind_addr}: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("endpoint bind failed: {e}"))?;

    let mut target = EndpointAddr::new(node_id);
    if let Some(addr) = args.addr {
        target = target.with_ip_addr(addr);
    }
    if let Some(url) = relays.first() {
        target = target.with_relay_url(url.clone());
    }

    let timestamp_us = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros()),
    )
    .unwrap_or(u64::MAX);

    // `stream_fetch_tracked` reports the acked watermark via `progress` even on
    // an error/timeout, so a paid-but-failed delivery still advances the stored
    // watermark — otherwise the next reuse would re-sign a stale nonce.
    let max_blob_bytes = args.max_blob_mb.saturating_mul(1024 * 1024);
    let mut progress = VoucherProgress::default();
    let result = stream_fetch_tracked(
        &endpoint,
        target,
        &ctx,
        &slash_dom,
        provider,
        hash,
        0,
        timestamp_us,
        Duration::from_millis(args.timeout_ms),
        max_blob_bytes,
        &mut progress,
    )
    .await;

    if let Some((nonce, bytes_delivered, amount)) = progress.acked() {
        // The bytes were paid for; any failure to persist the new watermark only
        // risks a rejected reuse next time, so warn rather than mask the fetch
        // outcome. A non-`Advanced` outcome (unknown provider / channel replaced
        // / regression) means the watermark did NOT move — same hazard as a
        // backend error — so surface it too rather than dropping it on the floor.
        match store.advance_progress(provider, channel_id, nonce, bytes_delivered, amount) {
            Ok(AdvanceOutcome::Advanced) => {}
            Ok(other) => eprintln!(
                "warning: voucher watermark not persisted for channel {channel_id} \
                 (provider {provider}): {other:?}; the next reuse may re-sign a stale nonce"
            ),
            Err(e) => eprintln!(
                "warning: failed to persist voucher watermark for channel {channel_id} \
                 (provider {provider}): {e}"
            ),
        }
    }

    let blob = result?;
    write_blob_atomic(&args.output, &blob)
        .map_err(|e| anyhow::anyhow!("write {}: {e}", args.output.display()))?;
    println!("fetched {} bytes -> {}", blob.len(), args.output.display());
    Ok(())
}

/// Reuse the live channel tracked for `provider` (resuming its watermark), or
/// open and persist a new one. A tracked-but-expired channel is replaced
/// (opening a fresh one); reclaiming the expired channel's deposit is deferred
/// (the node service handles reclaim, #940 follow-up — until then a replaced
/// expired channel's residual deposit is reclaim-able only manually).
#[allow(clippy::too_many_arguments)]
async fn open_or_reuse<P>(
    store: &RedbBuyerChannelStore,
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    rpc: &P,
    signer: &Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    provider: Address,
    self_address: Address,
    payment_channel_addr: Address,
    deposit: U256,
    max_approve: bool,
) -> anyhow::Result<ChannelContext>
where
    P: alloy::providers::Provider + Clone,
{
    if let Some(state) = store.get_by_provider(provider)? {
        if !state.is_expired_at(unix_now()) {
            return Ok(ChannelContext::for_buyer_channel(
                &state,
                Arc::clone(signer),
                voucher_domain.clone(),
            ));
        }
        eprintln!(
            "warning: tracked buyer channel {} (provider {provider}) expired; opening a \
             replacement (the expired channel's deposit must be reclaimed manually for now)",
            state.channel_id
        );
    }

    // Authoritative USDC token for the channel, from the contract itself.
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentChannel.usdc(): {e}"))?;
    // Clamp the deposit up to the on-chain floor so `openChannel` can't revert
    // for under-funding on a network with a higher `minDeposit` (matches the
    // node's buyer path and the `--deposit-micro-usdc` help text).
    let min_deposit = contract
        .minDeposit()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentChannel.minDeposit(): {e}"))?;
    let deposit = deposit.max(min_deposit);
    if max_approve {
        ensure_allowance(rpc, token, self_address, payment_channel_addr).await?;
    }
    let opened = open_channel(
        contract,
        Arc::clone(signer),
        voucher_domain,
        token,
        self_address,
        provider,
        deposit,
    )
    .await?;
    // The deposit is escrowed on-chain; a failed local record leaves it
    // untracked (reconcile against the tx).
    store.record(&opened.state).map_err(|e| {
        anyhow::anyhow!(
            "buyer channel opened on-chain (tx {}) but persisting it failed; the deposit is \
             escrowed but untracked — reconcile manually: {e}",
            opened.tx
        )
    })?;
    Ok(opened.ctx)
}

/// Write `bytes` to `target` atomically: a unique `O_CREAT|O_EXCL` temp in the
/// destination directory, then an atomic rename-replace.
fn write_blob_atomic(target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let parent = target.parent().filter(|p| !p.as_os_str().is_empty());
    let mut tmp = match parent {
        Some(p) => tempfile::NamedTempFile::new_in(p)?,
        None => tempfile::NamedTempFile::new_in(".")?,
    };
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(target).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    fn args() -> cli::FetchArgs {
        cli::FetchArgs {
            node_id: "n".into(),
            hash: "h".into(),
            output: PathBuf::from("/tmp/out"),
            addr: None,
            relay_url: None,
            provider_address: "0x0000000000000000000000000000000000000001".into(),
            rpc_url: None,
            payment_channel_address: None,
            slash_judge_address: None,
            chain_id: None,
            keystore: None,
            data_dir: Some(PathBuf::from("/tmp/d")),
            deposit_micro_usdc: None,
            max_blob_mb: 1024,
            timeout_ms: 30_000,
        }
    }

    fn config(body: &str) -> FileConfig {
        toml::from_str(body).expect("parse test config")
    }

    #[test]
    fn flags_override_config() {
        let mut a = args();
        a.rpc_url = Some("http://flag:8545".into());
        a.chain_id = Some(99);
        let pc = "0x1111111111111111111111111111111111111111";
        a.payment_channel_address = Some(pc.into());
        a.slash_judge_address = Some("0x2222222222222222222222222222222222222222".into());
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
        );
        let r = resolve_chain(&a, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.chain_id, 99);
        assert_eq!(r.payment_channel, Address::from_str(pc).unwrap());
    }

    #[test]
    fn config_fills_unset_flags_and_defaults() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\nbuyer_deposit_micro_usdc = 5000000\n",
        );
        let r = resolve_chain(&args(), &file).unwrap();
        assert_eq!(r.rpc_url, "http://config:8545");
        // chain_id absent everywhere → default.
        assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(r.deposit, U256::from(5_000_000u64));
        // keystore defaults under the data dir.
        assert_eq!(
            r.keystore,
            eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
        );
    }

    #[test]
    fn missing_rpc_url_errors() {
        let file = config(
            "[blockchain]\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
        );
        let err = resolve_chain(&args(), &file).unwrap_err();
        assert!(err.to_string().contains("rpc_url not set"), "{err}");
    }

    #[test]
    fn write_blob_atomic_replaces_existing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.bin");
        std::fs::write(&path, b"old contents that are longer").expect("seed");
        write_blob_atomic(&path, b"new").expect("write");
        assert_eq!(std::fs::read(&path).expect("read back"), b"new");
    }

    #[test]
    fn parse_hash_round_trips_and_rejects_short() {
        let digest = blake3::hash(b"payload");
        let hex = digest.to_hex();
        assert_eq!(parse_hash(&format!("0x{hex}")).unwrap(), *digest.as_bytes());
        assert_eq!(parse_hash(&hex).unwrap(), *digest.as_bytes());
        assert!(parse_hash("deadbeef").is_err());
    }
}
