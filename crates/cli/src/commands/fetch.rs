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
use decdn_incentive::buyer_channel::{AdvanceOutcome, BuyerChannelStore};
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{slash_judge_domain, voucher_domain};
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};

use super::discovery::{self, NodeCandidate};
use super::probe_client::probe_once;
use super::{chain_ctx, client_endpoint};

/// Default deposit when opening a new channel: 10 USDC (ADR 003 § Deposit
/// Economics recommended minimum). Clamped up to the on-chain `minDeposit`.
const DEFAULT_DEPOSIT_MICRO_USDC: u64 = 10_000_000;

/// Per-candidate probe timeout during auto-discovery (#936). The K probes run
/// concurrently, so this bounds selection latency rather than the overall fetch
/// (`--timeout-ms`); a dead candidate falls out of selection after this.
const SELECT_PROBE_TIMEOUT_MS: u64 = 5_000;

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
    /// `CapacityBond` registry for auto-discovery (no `--node-id`). `None` when
    /// neither the flag nor `blockchain.capacity_bond_address` is set — only an
    /// error on the discovery path, never on the explicit-node path.
    capacity_bond: Option<Address>,
    chain_id: u64,
    keystore: PathBuf,
    data_dir: PathBuf,
    /// Client region for region-first discovery ordering (`--region` >
    /// `identity.region`). `None` skips the ordering.
    region: Option<String>,
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

    // Optional: only the auto-discovery path reads it, and it errors there if
    // unset rather than failing every explicit-node fetch.
    let capacity_bond = args
        .capacity_bond_address
        .clone()
        .or_else(|| bc.and_then(|b| b.capacity_bond_address.clone()))
        .map(|raw| chain_ctx::parse_address(&raw, "capacity_bond_address"))
        .transpose()?;

    let region = args
        .region
        .clone()
        .or_else(|| file.identity.as_ref().and_then(|i| i.region.clone()));

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
        capacity_bond,
        chain_id,
        keystore,
        data_dir,
        region,
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

/// Auto-discover a node to fetch `hash` from (#936): read the active node set
/// from `CapacityBond`, take the region-nearest [`discovery::SELECT_K`]
/// candidates, probe them concurrently over `endpoint`, keep those that hold the
/// blob, and [`discovery::rank`] the holders (preferring a node we already have
/// a live channel with when it is close enough). Returns the chosen candidate
/// (its `node_id` to dial and `eth_address` as the provider).
async fn discover_provider(
    endpoint: &Endpoint,
    store: &RedbBuyerChannelStore,
    rpc_url: &str,
    capacity_bond: Address,
    client_region: Option<&str>,
    relay_hint: Option<RelayUrl>,
    hash: [u8; 32],
) -> anyhow::Result<NodeCandidate> {
    let all = discovery::active_nodes(rpc_url, capacity_bond).await?;
    if all.is_empty() {
        anyhow::bail!("no active nodes in the CapacityBond registry at {capacity_bond}");
    }
    let selected = discovery::select_candidates(all, client_region, discovery::SELECT_K);

    let timestamp_us = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros()),
    )
    .unwrap_or(u64::MAX);

    // Probe the K candidates concurrently in one task (`probe_once` is not
    // `Send` — its `&dyn ProbeMetrics` param — so `join_all` over a shared
    // `&endpoint` beats `tokio::spawn`). `probe_once`'s internal timeout bounds
    // each leg.
    let probes = selected.into_iter().map(|cand| {
        let relay = relay_hint.clone();
        async move {
            let mut target = EndpointAddr::new(cand.node_id);
            if let Some(url) = relay {
                target = target.with_relay_url(url);
            }
            let res = probe_once(
                endpoint,
                target,
                hash,
                timestamp_us,
                true,
                None,
                Duration::from_millis(SELECT_PROBE_TIMEOUT_MS),
            )
            .await;
            (cand, res.ok())
        }
    });
    let results = futures_util::future::join_all(probes).await;

    // Keep blob-holders with their RTT, tagging whether a live channel exists.
    // `slash_sig`/correlation are NOT validated here — selection only needs
    // has_blob + RTT; the chosen node's delivery is fully verified downstream.
    let probe_count = results.len();
    let mut holders = Vec::new();
    for (cand, res) in results {
        let Some((resp, rtt_ms)) = res else { continue };
        if !resp.body.has_blob {
            continue;
        }
        let has_live_channel = store
            .get_by_provider(cand.eth_address)?
            .is_some_and(|s| !s.is_expired_at(unix_now()));
        holders.push(discovery::Probed {
            candidate: cand,
            rtt_ms,
            has_live_channel,
        });
    }

    let pick = discovery::rank(&holders).ok_or_else(|| {
        anyhow::anyhow!("none of the {probe_count} probed node(s) hold the requested blob")
    })?;
    Ok(pick.candidate.clone())
}

/// Resolve the node to fetch from: the explicit `--node-id` (today's path,
/// requiring `--provider-address` and a reachable `--addr`/relay), or
/// auto-discovery (#936) when `--node-id` is omitted (deriving the provider from
/// the chosen node's registry entry). Returns `(node_id_to_dial, provider)`.
async fn resolve_target_node(
    args: &cli::FetchArgs,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
    store: &RedbBuyerChannelStore,
    relays: &[RelayUrl],
    hash: [u8; 32],
) -> anyhow::Result<(PublicKey, Address)> {
    if let Some(raw) = &args.node_id {
        if args.addr.is_none() && relays.is_empty() {
            anyhow::bail!(
                "no way to reach the node: pass --addr, or set network.relay_urls in \
                 config (or --relay-url)"
            );
        }
        let node_id = PublicKey::from_str(raw)
            .map_err(|e| anyhow::anyhow!("invalid --node-id {raw:?}: {e}"))?;
        let provider_raw = args
            .provider_address
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--provider-address is required with --node-id"))?;
        let provider = chain_ctx::parse_address(provider_raw, "--provider-address")?;
        return Ok((node_id, provider));
    }

    let capacity_bond = chain.capacity_bond.ok_or_else(|| {
        anyhow::anyhow!(
            "auto-discovery needs capacity_bond_address (--capacity-bond-address or \
             blockchain.capacity_bond_address), or pass --node-id to dial directly"
        )
    })?;
    let picked = discover_provider(
        endpoint,
        store,
        &chain.rpc_url,
        capacity_bond,
        chain.region.as_deref(),
        relays.first().cloned(),
        hash,
    )
    .await?;
    eprintln!(
        "discovered node {} (provider {}, region {:?})",
        picked.node_id, picked.eth_address, picked.region_hint
    );
    Ok((picked.node_id, picked.eth_address))
}

/// Fetch a single blob over `cdn/client/v1`, auto-opening/reusing a payment
/// channel, and write it atomically to `--output`. `config_path` (the global
/// `--config`) supplies relays (#935), discovery (#936), and chain coordinates.
///
/// With `--node-id` the node is dialed explicitly (today's path). Without it,
/// `fetch` auto-discovers (#936): read the active set from `CapacityBond`, probe
/// the region-nearest candidates, pick a holder (channel-aware ranking), and
/// derive `--provider-address` from its registry entry.
pub async fn fetch(args: &cli::FetchArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let hash = parse_hash(&args.hash)?;

    // Relays: `--relay-url` overrides `network.relay_urls` (#935). Discovery:
    // `[network.discovery]` composes operator resolution legs, else N0 (#936).
    let relays = client_endpoint::resolve_relays(args.relay_url.as_deref(), config_path)?;
    let disc = client_endpoint::client_discovery(config_path)?;

    let file = load_file_config(config_path)?;
    let chain = resolve_chain(args, &file)?;

    // The store is read by the discovery channel-aware ranking and recorded into
    // by open-or-reuse; open it once.
    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;

    // One discovery-enabled endpoint, reused for probing and the delivery dial.
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;

    // Resolve the node to fetch from: explicit `--node-id`, or auto-discover.
    let (node_id, provider) =
        resolve_target_node(args, &chain, &endpoint, &store, &relays, hash).await?;

    // Buyer signer (vouchers + the openChannel tx). Loaded after selection so a
    // failed discovery never prompts for a keystore password. Password from env,
    // else TTY.
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

    let mut target = EndpointAddr::new(node_id);
    // A direct `--addr` only applies to the explicit-node path; a discovered
    // node is reached via the resolved address + relay hint.
    if let (Some(addr), Some(_)) = (args.addr, &args.node_id) {
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
            node_id: Some("n".into()),
            hash: "h".into(),
            output: PathBuf::from("/tmp/out"),
            addr: None,
            relay_url: None,
            provider_address: Some("0x0000000000000000000000000000000000000001".into()),
            rpc_url: None,
            payment_channel_address: None,
            slash_judge_address: None,
            capacity_bond_address: None,
            region: None,
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
