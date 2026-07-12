//! `decdn fetch` — standalone client-side paid pull of a single
//! content-addressed blob over `cdn/client/v1` (issues #391, #940).
//!
//! Turnkey paying sibling of [`super::probe`]: dial a node by explicit
//! `--node-id`/`--addr`/`--relay-url` (or auto-discover one, #936),
//! **auto-open-or-reuse** a `PaymentChannel` with `--provider-address`, run one
//! delivery exchange via [`decdn_client_pull::stream_fetch_tracked`] (signing
//! cumulative vouchers, resuming the channel's persisted watermark), verify the
//! `slash_sig` recovers to the provider (ADR 014 §1), BLAKE3-check the whole
//! blob, persist the new watermark, and write the bytes atomically.
//!
//! Channel lifecycle (#940): a live channel for the provider in the persistent
//! [`RedbBuyerChannelStore`] is reused (watermark resumed); otherwise one is
//! opened on-chain (USDC `approve` if needed → `openChannel`) via the shared
//! [`decdn_client_pull::buyer_channel::open_channel`] kernel and recorded. The
//! chain coordinates resolve flag > `[blockchain]`/`[identity]` config > default.
//!
//! The chain/discovery/delivery seams (`resolve_chain`, `resolve_target_node`,
//! `probe_and_rank`, `open_or_reuse`, `fetch_blob`, `write_blob_atomic`) are
//! `pub(crate)` so `decdn bundle pull` (#391) reuses the same paid-fetch kernel
//! across a manifest's many entries.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_client_pull::buyer_channel::{ensure_allowance, open_channel};
use decdn_client_pull::{
    ChannelContext, ProgressCallback, VoucherProgress, sign_client_binding,
    stream_fetch_tracked_with_progress,
};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_channel::{AdvanceOutcome, BuyerChannelStore};
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{bind_node_id_domain, slash_judge_domain, voucher_domain};
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};

use super::chain_ctx;
use decdn_client_pull::discovery::{self, NodeCandidate};
use decdn_client_pull::endpoint as client_endpoint;
use decdn_client_pull::probe::probe_once;
use decdn_client_pull::provider;

/// Default deposit when opening a new channel: 10 USDC (ADR 003 § Deposit
/// Economics recommended minimum). Clamped up to the on-chain `minDeposit`.
const DEFAULT_DEPOSIT_MICRO_USDC: u64 = 10_000_000;

/// Per-candidate probe timeout during auto-discovery (#936). The K probes run
/// concurrently, so this bounds selection latency rather than the overall fetch
/// (`--timeout-ms`); a dead candidate falls out of selection after this.
const SELECT_PROBE_TIMEOUT_MS: u64 = 5_000;

/// Parse a user-supplied BLAKE3 hash: 64 hex chars, optionally `0x`- or
/// `b3:`-prefixed (the `b3:` form is what bundle manifests carry).
pub(crate) fn parse_hash(s: &str) -> anyhow::Result<[u8; 32]> {
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("b3:"))
        .unwrap_or(s);
    let h = blake3::Hash::from_hex(hex).map_err(|e| {
        anyhow::anyhow!(
            "invalid hash {s:?}: expected 64 hex chars (BLAKE3 digest), optional `0x`/`b3:` \
             prefix: {e}"
        )
    })?;
    Ok(*h.as_bytes())
}

/// Current unix time in seconds (for channel-expiry checks).
pub(crate) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Current unix time in microseconds (the requester-echoed `timestamp_us`).
pub(crate) fn micros_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros()),
    )
    .unwrap_or(u64::MAX)
}

/// Build the `decdn fetch` delivery progress bar (#1118). Drawn to stderr and
/// TTY-aware — `indicatif` hides it automatically when stderr is not a terminal,
/// so a piped/redirected fetch emits no bar. A steady tick animates the spinner
/// during the pre-byte connect/handshake so the command never looks hung. The
/// bar counts **wire** bytes (content plus interleaved bao proof), so its total
/// runs slightly above the final content-byte count printed on completion — it
/// tracks the transfer, not the payload size.
fn new_progress_bar() -> indicatif::ProgressBar {
    let style = indicatif::ProgressStyle::with_template(
        "{spinner:.green} {bytes}/{total_bytes} ({bytes_per_sec}, {eta}) [{wide_bar:.cyan/blue}]",
    )
    // A bad template is a programming error, not a runtime one; fall back to the
    // built-in bar rather than panic (clippy forbids `unwrap`/`expect`).
    .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar())
    .progress_chars("=>-");
    let bar = indicatif::ProgressBar::new(0);
    bar.set_style(style);
    bar.enable_steady_tick(Duration::from_millis(120));
    bar
}

/// Chain coordinates resolved flag > `[blockchain]`/`[identity]` config >
/// default. Pure (parse-only) so the precedence is unit-testable.
#[derive(Debug)]
pub(crate) struct ResolvedChain {
    pub(crate) rpc_url: String,
    pub(crate) payment_channel: Address,
    pub(crate) slash_judge: Address,
    /// `CapacityBond` registry for auto-discovery (no `--node-id`). `None` when
    /// neither the flag nor `blockchain.capacity_bond_address` is set — only an
    /// error on the discovery path, never on the explicit-node path.
    pub(crate) capacity_bond: Option<Address>,
    pub(crate) chain_id: u64,
    pub(crate) keystore: PathBuf,
    pub(crate) data_dir: PathBuf,
    /// Client region for region-first discovery ordering (`--region` >
    /// `identity.region`). `None` skips the ordering.
    pub(crate) region: Option<String>,
    pub(crate) deposit: U256,
    pub(crate) max_approve: bool,
}

pub(crate) fn resolve_chain(
    args: &cli::ClientFetchArgs,
    file: &FileConfig,
) -> anyhow::Result<ResolvedChain> {
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

/// Probe `candidates` for `hash` over `endpoint` and pick the best holder
/// (channel-aware ranking, #936). Shared by `fetch`'s one-shot discovery and
/// `bundle pull`'s per-entry discovery (which reads the active set once, then
/// re-probes this list per entry). `slash_sig`/correlation are NOT validated
/// here — selection only needs `has_blob` + RTT; the chosen node's delivery is
/// fully verified downstream. Errors if none of the probed candidates hold it.
pub(crate) async fn probe_and_rank(
    endpoint: &Endpoint,
    store: &RedbBuyerChannelStore,
    candidates: &[NodeCandidate],
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
) -> anyhow::Result<NodeCandidate> {
    let timestamp_us = micros_now();
    // Probe concurrently in one task (`probe_once` is not `Send` — its
    // `&dyn ProbeMetrics` param — so `join_all` over a shared `&endpoint` beats
    // `tokio::spawn`). `probe_once`'s internal timeout bounds each leg.
    let probes = candidates.iter().map(|cand| {
        let relay = relay_hint.cloned();
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
            candidate: cand.clone(),
            rtt_ms,
            has_live_channel,
        });
    }

    let pick = discovery::rank(&holders).ok_or_else(|| {
        anyhow::anyhow!("none of the {probe_count} probed node(s) hold the requested blob")
    })?;
    Ok(pick.candidate.clone())
}

/// Auto-discover a node to fetch `hash` from (#936): read the active node set
/// from `CapacityBond`, take the region-nearest [`discovery::SELECT_K`]
/// candidates, and [`probe_and_rank`] them. Returns the chosen candidate.
async fn discover_provider(
    endpoint: &Endpoint,
    store: &RedbBuyerChannelStore,
    rpc_url: &str,
    capacity_bond: Address,
    client_region: Option<&str>,
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
) -> anyhow::Result<NodeCandidate> {
    let all = discovery::active_nodes(rpc_url, capacity_bond).await?;
    if all.is_empty() {
        anyhow::bail!("no active nodes in the CapacityBond registry at {capacity_bond}");
    }
    let selected = discovery::select_candidates(all, client_region, discovery::SELECT_K);
    probe_and_rank(endpoint, store, &selected, relay_hint, hash).await
}

/// Resolve the node to fetch from: the explicit `--node-id` (requiring
/// `--provider-address`), or auto-discovery (#936) when `--node-id` is omitted
/// (deriving the provider from the chosen node's registry entry). Returns
/// `(node_id_to_dial, provider)`.
pub(crate) async fn resolve_target_node(
    args: &cli::ClientFetchArgs,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
    store: &RedbBuyerChannelStore,
    relays: &[RelayUrl],
    hash: [u8; 32],
) -> anyhow::Result<(PublicKey, Address)> {
    if let Some(raw) = &args.node_id {
        // No reachability pre-check: the endpoint is discovery-enabled, so a
        // node-id resolves via `[network.discovery]` / `presets::N0` (plus its
        // default relays) even without `--addr` or configured relays. `clap`
        // guarantees `--provider-address` is present alongside `--node-id`.
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
        relays.first(),
        hash,
    )
    .await?;
    eprintln!(
        "discovered node {} (provider {}, region {:?})",
        picked.node_id, picked.eth_address, picked.region_hint
    );
    Ok((picked.node_id, picked.eth_address))
}

/// Fetch one blob over `cdn/client/v1` against an already-resolved channel
/// `ctx` + dial `target`, persisting the voucher watermark afterwards. Returns
/// the verified blob bytes. The caller is responsible for serializing concurrent
/// calls that share a channel (`ctx`/`provider`) — vouchers on one channel use a
/// strictly-increasing nonce, so two in-flight fetches on the same channel would
/// race it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_blob(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_dom: &Eip712Domain,
    provider: Address,
    store: &RedbBuyerChannelStore,
    hash: [u8; 32],
    timeout: Duration,
    max_blob_bytes: u64,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<Vec<u8>> {
    let channel_id = ctx.channel_id;
    let timestamp_us = micros_now();
    // `stream_fetch_tracked_with_progress` reports the acked watermark via
    // `progress` even on an error/timeout, so a paid-but-failed delivery still
    // advances the stored watermark — otherwise the next reuse would re-sign a
    // stale nonce. `on_progress` is the byte-delivery readout (`fetch` renders a
    // bar; `bundle pull` passes `None`).
    let mut progress = VoucherProgress::default();
    let result = stream_fetch_tracked_with_progress(
        endpoint,
        target,
        ctx,
        slash_dom,
        provider,
        hash,
        0,
        timestamp_us,
        timeout,
        max_blob_bytes,
        &mut progress,
        on_progress,
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

    Ok(result?.to_vec())
}

/// Fetch a single blob over `cdn/client/v1`, auto-opening/reusing a payment
/// channel, and write it atomically to `--output`. `config_path` (the global
/// `--config`) supplies relays (#935), discovery (#936), and chain coordinates.
///
/// With `--node-id` the node is dialed explicitly. Without it, `fetch`
/// auto-discovers (#936): read the active set from `CapacityBond`, probe the
/// region-nearest candidates, pick a holder (channel-aware ranking), and derive
/// `--provider-address` from its registry entry.
pub async fn fetch(args: &cli::FetchArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let hash = parse_hash(&args.hash)?;
    let common = &args.common;

    // Relays: `--relay-url` overrides `network.relay_urls` (#935). Discovery:
    // `[network.discovery]` composes operator resolution legs, else N0 (#936).
    let relays = client_endpoint::resolve_relays(common.relay_url.as_deref(), config_path)?;
    let disc = client_endpoint::client_discovery(config_path)?;

    let file = load_file_config(config_path)?;
    let chain = resolve_chain(common, &file)?;

    // The store is read by the discovery channel-aware ranking and recorded into
    // by open-or-reuse; open it once.
    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;

    // One discovery-enabled endpoint, reused for probing and the delivery dial.
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;

    // Resolve the node to fetch from: explicit `--node-id`, or auto-discover.
    let (node_id, provider) =
        resolve_target_node(common, &chain, &endpoint, &store, &relays, hash).await?;

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

    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
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

    // ADR 005 client identity binding (#1115): sign our OWN iroh NodeId with the
    // buyer key so the serving node can prove we own the channel and reactively
    // pull a cache-missed blob from its configured origin. The bind domain's
    // verifying contract is the `CapacityBond`; without it configured we can't
    // sign, so we fall back to the pre-#1115 behavior (only already-cached
    // content is served — a cache miss is refused).
    let ctx = if let Some(capacity_bond) = chain.capacity_bond {
        let bind_dom = bind_node_id_domain(chain.chain_id, capacity_bond);
        let own_node_id = B256::from(*endpoint.id().as_bytes());
        ctx.with_client_binding(sign_client_binding(&signer, own_node_id, &bind_dom)?)
    } else {
        eprintln!(
            "warning: [blockchain].capacity_bond_address is not set; omitting the client \
             identity binding, so a node cannot reactively pull this blob from its origin \
             (only already-cached content will be served)"
        );
        ctx
    };

    let mut target = EndpointAddr::new(node_id);
    // `--addr` requires `--node-id` (clap), so it only pins the explicit-node
    // path; a discovered node is reached via its resolved address + relay hint.
    if let Some(addr) = common.addr {
        target = target.with_ip_addr(addr);
    }
    if let Some(url) = relays.first() {
        target = target.with_relay_url(url.clone());
    }

    let max_blob_bytes = common.max_blob_mb.saturating_mul(1024 * 1024);
    // Delivery progress bar (#1118). `indicatif` draws to stderr and hides
    // itself automatically when stderr is not a terminal, so a piped/redirected
    // fetch stays silent. The bar starts length-less; the first callback (which
    // fires once the signed `StreamResponse` fixes the total) sets its length.
    let bar = new_progress_bar();
    // `ProgressBar` is `Arc`-backed, so the clone the callback owns drives the
    // same bar we `finish_and_clear` below. The callback must be `'static`
    // (`ProgressCallback`), hence the owned clone rather than a borrow.
    let cb_bar = bar.clone();
    // `expected` is constant across the pull, so set the bar length once (it
    // takes a write lock) rather than on every chunk in the hot receive loop.
    let length_set = std::sync::atomic::AtomicBool::new(false);
    let on_progress = move |received: u64, expected: u64| {
        if !length_set.swap(true, std::sync::atomic::Ordering::Relaxed) {
            cb_bar.set_length(expected);
        }
        cb_bar.set_position(received);
    };
    let blob = fetch_blob(
        &endpoint,
        target,
        &ctx,
        &slash_dom,
        provider,
        &store,
        hash,
        Duration::from_millis(common.timeout_ms),
        max_blob_bytes,
        Some(&on_progress),
    )
    .await;
    // Clear the bar before the terminal outcome (success line or error) so it
    // never overwrites the final message, on either path.
    bar.finish_and_clear();
    let blob = blob?;

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
pub(crate) async fn open_or_reuse<P>(
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
pub(crate) fn write_blob_atomic(target: &Path, bytes: &[u8]) -> std::io::Result<()> {
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

    fn common() -> cli::ClientFetchArgs {
        cli::ClientFetchArgs {
            node_id: Some("n".into()),
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
        let mut c = common();
        c.rpc_url = Some("http://flag:8545".into());
        c.chain_id = Some(99);
        let pc = "0x1111111111111111111111111111111111111111";
        c.payment_channel_address = Some(pc.into());
        c.slash_judge_address = Some("0x2222222222222222222222222222222222222222".into());
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
        );
        let r = resolve_chain(&c, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.chain_id, 99);
        assert_eq!(r.payment_channel, Address::from_str(pc).unwrap());
    }

    #[test]
    fn config_fills_unset_flags_and_defaults() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\nbuyer_deposit_micro_usdc = 5000000\n",
        );
        let r = resolve_chain(&common(), &file).unwrap();
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
        let err = resolve_chain(&common(), &file).unwrap_err();
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
        assert_eq!(
            parse_hash(&format!("b3:{hex}")).unwrap(),
            *digest.as_bytes()
        );
        assert_eq!(parse_hash(&hex).unwrap(), *digest.as_bytes());
        assert!(parse_hash("deadbeef").is_err());
    }
}
