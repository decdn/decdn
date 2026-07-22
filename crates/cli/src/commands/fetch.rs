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
//! [`RedbBuyerChannelStore`] is reused (watermark resumed) — and auto-refilled
//! on-chain via `topUp` when its remaining deposit has run low (#1103), so a
//! sustained series of fetches against one provider isn't stranded; otherwise
//! one is opened on-chain (USDC `approve` if needed → `openChannel`) via the
//! shared [`decdn_client_pull::buyer_channel::open_channel`] kernel and
//! recorded. The chain coordinates resolve flag > `[blockchain]`/`[identity]`
//! config > default.
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
use decdn_client_pull::buyer_channel::{
    LOW_WATER_DIVISOR, ensure_allowance, open_channel, refill_amount, top_up,
};
use decdn_client_pull::{
    ChannelContext, ProgressCallback, PullDeadlines, UpstreamRefused, VoucherProgress,
    sign_client_binding, stream_fetch_tracked_with_progress,
};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_channel::{AdvanceOutcome, BuyerChannelStore};
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{bind_node_id_domain, slash_judge_domain, voucher_domain};
use decdn_protocol::client::StreamError;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};

use super::chain_ctx;
use super::file_manifest;
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
    /// Directory holding the buyer-channel redb store and (by default) the
    /// keystore. Client-scoped (`~/.decdn/client`) unless an explicit
    /// `--data-dir`/`identity.data_dir` is given.
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
    let payment_channel = chain_ctx::parse_nonzero_address(&pc_raw, "payment_channel_address")?;

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
    let slash_judge = chain_ctx::parse_nonzero_address(&sj_raw, "slash_judge_address")?;

    // Optional: only the auto-discovery path reads it, and it errors there if
    // unset rather than failing every explicit-node fetch.
    let capacity_bond = args
        .capacity_bond_address
        .clone()
        .or_else(|| bc.and_then(|b| b.capacity_bond_address.clone()))
        .map(|raw| chain_ctx::parse_nonzero_address(&raw, "capacity_bond_address"))
        .transpose()?;

    let region = args
        .region
        .clone()
        .or_else(|| file.identity.as_ref().and_then(|i| i.region.clone()));

    let chain_id = args
        .chain_id
        .or_else(|| bc.and_then(|b| b.chain_id))
        .unwrap_or(DEFAULT_CHAIN_ID);

    // Client data dir: an explicit `--data-dir`/`identity.data_dir` wins,
    // otherwise the client-scoped `~/.decdn/client` (not the node-shaped
    // `~/.decdn`, so a pure client install doesn't masquerade as a node).
    let data_dir = args
        .data_dir
        .clone()
        .or_else(|| file.identity.as_ref().and_then(|i| i.data_dir.clone()))
        .map(|p| expand_tilde(&p))
        .or_else(cli::default_client_data_dir)
        .ok_or_else(|| {
            anyhow::anyhow!("data_dir not set and no default available (pass --data-dir)")
        })?;

    // Keystore: an explicit `--keystore`/`blockchain.eth_keystore` wins; otherwise
    // `keystore.json` under the (client-scoped) data dir.
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
    // Client default: exact (deposit-sized) USDC approval, not an unlimited
    // standing allowance. `buyer_max_approve = true` opts a power user back into
    // the node/operator posture. (The daemon's own default stays unlimited.)
    let max_approve = bc.and_then(|b| b.buyer_max_approve).unwrap_or(false);

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

/// Client-side proxy-warming knobs (#1174, ADR 037), distilled from
/// [`cli::ClientFetchArgs`]. RTT thresholds are held as `f64` ms to compare
/// directly against probe RTTs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProxyWarmingParams {
    pub enabled: bool,
    pub rtt_threshold_ms: f64,
    pub margin_ms: f64,
}

impl ProxyWarmingParams {
    pub(crate) fn from_args(args: &cli::ClientFetchArgs) -> Self {
        // Widen through u32 so the u64 → f64 cast is lossless (ms knobs are
        // small); saturate the implausible overflow rather than lose precision.
        let ms = |v: u64| f64::from(u32::try_from(v).unwrap_or(u32::MAX));
        Self {
            enabled: args.proxy_warming,
            rtt_threshold_ms: ms(args.proxy_warming_rtt_threshold_ms),
            margin_ms: ms(args.proxy_warming_margin_ms),
        }
    }
}

/// Probe `candidates` for `hash` over `endpoint` and pick the best holder
/// (channel-aware ranking, #936). Shared by `fetch`'s one-shot discovery and
/// `bundle pull`'s per-entry discovery (which reads the active set once, then
/// re-probes this list per entry). `slash_sig`/correlation are NOT validated
/// here — selection only needs `has_blob` + RTT; the chosen node's delivery is
/// fully verified downstream. Errors if none of the probed candidates hold it.
///
/// When `warming` is enabled (opt-in, #1174/ADR 037) and the best holder is
/// distant, this may instead return a probed **non-holder** that is measurably
/// nearer, so it serves via window-paced pull-through and seeds a regional copy.
pub(crate) async fn probe_and_rank(
    endpoint: &Endpoint,
    store: &RedbBuyerChannelStore,
    candidates: &[NodeCandidate],
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
    warming: ProxyWarmingParams,
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
    // Probed bonded non-holders are the proxy-warming candidate pool (ADR 037 §
    // Candidate pool): reachable nodes that don't hold the blob, with a measured
    // RTT. Only collected when warming is enabled.
    let mut warming_pool: Vec<discovery::WarmingCandidate> = Vec::new();
    for (cand, res) in results {
        let Some((resp, rtt_ms)) = res else { continue };
        if resp.body.has_blob {
            let has_live_channel = store
                .get_by_provider(cand.eth_address)?
                .is_some_and(|s| !s.is_expired_at(unix_now()));
            holders.push(discovery::Probed {
                candidate: cand.clone(),
                rtt_ms,
                has_live_channel,
            });
        } else if warming.enabled {
            warming_pool.push(discovery::WarmingCandidate {
                node_id: cand.node_id,
                eth_address: cand.eth_address,
                rtt_ms,
            });
        }
    }

    // Proxy-warming pre-step (ADR 037 § Client selection policy): if the best
    // holder is distant and a probed non-holder beats it by the margin, route the
    // paid request through that nearer non-holder — it serves via window-paced
    // pull-through and becomes the first regional copy. RTT-only ranking; never a
    // gamble (empty order ⇒ fall through to the direct holder).
    if warming.enabled && !holders.is_empty() {
        let best_holder_rtt = holders
            .iter()
            .map(|h| h.rtt_ms)
            .fold(f64::INFINITY, f64::min);
        let order = discovery::proxy_warming_order(
            best_holder_rtt,
            warming.rtt_threshold_ms,
            warming.margin_ms,
            &warming_pool,
        );
        if let Some(proxy) = order.first() {
            eprintln!(
                "proxy-warming: routing through nearer non-holder {} ({:.1}ms) instead of the \
                 best holder ({:.1}ms) to seed a regional copy (ADR 037)",
                proxy.node_id, proxy.rtt_ms, best_holder_rtt
            );
            return Ok(NodeCandidate {
                node_id: proxy.node_id,
                eth_address: proxy.eth_address,
                // Region deliberately dropped rather than carried over: ADR 037
                // §"Ranking key is measured RTT only" forbids region from
                // influencing warming, and `region_hint` is only ever read by
                // `select_candidates`' pre-probe shortlist and operator logging.
                // Leaving it unset keeps a spoofed region from riding along.
                region_hint: None,
            });
        }
    }

    let pick = discovery::rank(&holders).ok_or_else(|| {
        anyhow::anyhow!("none of the {probe_count} probed node(s) hold the requested blob")
    })?;
    Ok(pick.candidate.clone())
}

/// Auto-discover a node to fetch `hash` from (#936): read the active node set
/// from `CapacityBond`, take the region-nearest [`discovery::SELECT_K`]
/// candidates, and [`probe_and_rank`] them. Returns the chosen candidate.
/// Callers must have already unwrapped `chain.capacity_bond` into the
/// "auto-discovery needs `capacity_bond_address`" error, which is why the
/// address is a separate parameter rather than read back off `chain`.
async fn discover_provider(
    endpoint: &Endpoint,
    store: &RedbBuyerChannelStore,
    chain: &ResolvedChain,
    capacity_bond: Address,
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
    // The warming params and the registry deadline are both derived from the
    // same `args`, so they travel as `args` rather than as two more positional
    // parameters (clippy caps this function at 7).
    args: &cli::ClientFetchArgs,
) -> anyhow::Result<NodeCandidate> {
    let bootstrap = discovery::bootstrap_nodes(
        &chain.rpc_url,
        capacity_bond,
        &chain.data_dir,
        args.discovery_cap(),
    )
    .await?;
    // `client-pull` cannot log this itself — `decdn` installs no tracing
    // subscriber — and a silently stale peer list is exactly what the user
    // needs told, so the provenance comes back in the return value.
    if let Some(warning) = bootstrap.warning() {
        eprintln!("{warning}");
    }
    let all = bootstrap.into_peers();
    if all.is_empty() {
        anyhow::bail!("no active nodes in the CapacityBond registry at {capacity_bond}");
    }
    let selected = discovery::select_candidates(all, chain.region.as_deref(), discovery::SELECT_K);
    let warming = ProxyWarmingParams::from_args(args);
    probe_and_rank(endpoint, store, &selected, relay_hint, hash, warming).await
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
    // `--timeout-ms` bounds the registry read (#1349). Nothing did before: its
    // retry schedule alone can burn 36 s, so `decdn fetch --timeout-ms 5000`
    // could sit far longer than 5 s before the transfer it caps had started.
    //
    // The bound is applied INSIDE `bootstrap_nodes`, around the read alone,
    // rather than wrapped around discovery from out here. Wrapping from out
    // here also cancels the ADR 012 § Bootstrap step 4 cache fallback, so a
    // client holding a usable `peers.json` would be handed a hard failure
    // instead of the degraded-but-working fetch the cache exists to provide.
    let picked = discover_provider(
        endpoint,
        store,
        chain,
        capacity_bond,
        relays.first(),
        hash,
        args,
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
/// Reconnect an opaque `delivery refused: NotFound` to its likely cause when the
/// request went out WITHOUT an ADR 005 client binding. An unbound request (no
/// `blockchain.capacity_bond_address`, so nothing to sign) cannot authorize the
/// node to reactively pull a cache-missed blob from its origin, so the node
/// returns a bare `NotFound` — indistinguishable, without this, from a genuinely
/// absent/blacklisted/wrong hash. Scoped to `NotFound` (a size/blacklist refusal
/// is not fixed by a binding) and to unbound contexts, so a bound fetch's error
/// is passed through untouched. Every other error is returned verbatim.
fn annotate_unbound_cache_miss(err: anyhow::Error, ctx: &ChannelContext) -> anyhow::Error {
    let refused_not_found = err
        .downcast_ref::<UpstreamRefused>()
        .is_some_and(|refused| matches!(refused.error, StreamError::NotFound));
    if ctx.client_binding.is_none() && refused_not_found {
        err.context(
            "no client identity binding was sent because \
             blockchain.capacity_bond_address is unset, so the node could not \
             reactively pull this cache-missed blob from its origin; set \
             blockchain.capacity_bond_address to enable reactive pull-through",
        )
    } else {
        err
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_blob(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_dom: &Eip712Domain,
    provider: Address,
    store: &RedbBuyerChannelStore,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    deadlines: PullDeadlines,
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
        namespace_id,
        0,
        timestamp_us,
        deadlines,
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
#[allow(clippy::too_many_lines)]
pub async fn fetch(args: &cli::FetchArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let hash = parse_hash(&args.hash)?;
    let common = &args.common;
    // Before any network or keystore work: a hard cap at or below TWICE the stall budget
    // parses fine and silently disables stall detection — the open stage is bounded by that
    // same budget, so both can run inside the cap consecutively (#1145 review). The
    // `PullDeadlines::capped` below refuses it too; this is the early error, in the flags the
    // user actually typed.
    common.validate()?;

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
    // and persist a new one, with the ADR 005 client binding attached.
    //
    // Rebuilt before EVERY blob rather than hoisted: the context snapshots the
    // channel's voucher watermark (`prior_nonce`), which `fetch_blob` advances in
    // the store as it pays. A chunked-manifest fetch (#1183) issues many
    // sequential pulls on one channel, and reusing a stale context would re-sign
    // an already-spent nonce. It also gives each chunk a fresh low-deposit
    // refill check (#1103).
    let ctx = build_channel_ctx(
        &store,
        &contract,
        &rpc,
        &signer,
        &voucher_dom,
        provider,
        self_address,
        &chain,
        &endpoint,
    )
    .await?;

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
    let (bar, on_progress) = delivery_progress();
    // The namespace routing hint (ADR 005 § Namespace routing): `--namespace <id>`
    // → big-endian `uint256`; absent => `NO_NAMESPACE` (best-effort cache/DHT).
    // A `DECDNMAN` manifest's chunk pulls inherit it (the chunks are that
    // namespace's content), threaded through `ChunkFetcher` below.
    let namespace_id = args
        .namespace
        .map_or(decdn_protocol::client::NO_NAMESPACE, |n| {
            alloy::primitives::U256::from(n).to_be_bytes()
        });
    let blob = fetch_blob(
        &endpoint,
        // Cloned, not moved: a `DECDNMAN` manifest expands into per-chunk pulls
        // to the same node below (#1183).
        target.clone(),
        &ctx,
        &slash_dom,
        provider,
        &store,
        hash,
        namespace_id,
        // A node that accepts the connection and never answers is as dead as one that
        // stops mid-stream, so the same budget answers both (#1134). `capped` enforces
        // that the hard cap outlasts them both — `ClientFetchArgs::validate` has already
        // said so in the user's own flags, so this `?` is the belt to that braces (#1145
        // review).
        PullDeadlines::capped(
            common.stall_timeout(),
            common.stall_timeout(),
            common.hard_cap(),
        )?,
        max_blob_bytes,
        Some(&on_progress),
    )
    .await;
    // Clear the bar before the terminal outcome (success line or error) so it
    // never overwrites the final message, on either path.
    bar.finish_and_clear();
    let blob = blob.map_err(|err| annotate_unbound_cache_miss(err, &ctx))?;

    // A `DECDNMAN` manifest blob is a chunked FILE, not the file's bytes: expand
    // it into per-chunk pulls and reconstruct (ADR 012 § Download flow, #1183).
    // Anything without the magic is a raw blob and is written through unchanged.
    if let Some(manifest) = file_manifest::sniff(&blob) {
        let chunks = ChunkFetcher {
            endpoint: &endpoint,
            target: &target,
            store: &store,
            contract: &contract,
            rpc: &rpc,
            signer: &signer,
            voucher_dom: &voucher_dom,
            slash_dom: &slash_dom,
            chain: &chain,
            provider,
            self_address,
            deadlines: PullDeadlines::capped(
                common.stall_timeout(),
                common.stall_timeout(),
                common.hard_cap(),
            )?,
            max_blob_bytes,
            namespace_id,
        };
        return chunks
            .reconstruct(&manifest?, hash, &args.output, !common.no_keep_blobs)
            .await;
    }

    write_blob_atomic(&args.output, &blob)
        .map_err(|e| anyhow::anyhow!("write {}: {e}", args.output.display()))?;
    println!("fetched {} bytes -> {}", blob.len(), args.output.display());
    Ok(())
}

/// The `decdn fetch` delivery progress bar plus the callback that drives it,
/// returned as a pair so the caller can `finish_and_clear` the bar before its
/// terminal message (#1118).
fn delivery_progress() -> (indicatif::ProgressBar, impl Fn(u64, u64) + 'static) {
    let bar = new_progress_bar();
    // `ProgressBar` is `Arc`-backed, so the clone the callback owns drives the
    // same bar the caller clears. The callback must be `'static`
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
    (bar, on_progress)
}

/// The fetch-wide state a chunked `DECDNMAN` download needs to pull each chunk
/// (#1183), borrowed from [`fetch`]'s own locals.
///
/// It exists because chunk pulls are *repeated* single-blob fetches: they reuse
/// the node, channel, and deadlines already resolved for the manifest blob, but
/// each needs a freshly-rebuilt [`ChannelContext`] (see [`build_channel_ctx`]).
/// Bundling them beats threading a dozen arguments through a free function.
struct ChunkFetcher<'a, P: alloy::providers::Provider + Clone> {
    endpoint: &'a Endpoint,
    /// The node that served the manifest; its chunks are pulled from it too.
    target: &'a EndpointAddr,
    store: &'a RedbBuyerChannelStore,
    contract: &'a PaymentChannel::PaymentChannelInstance<P>,
    rpc: &'a P,
    signer: &'a Arc<PrivateKeySigner>,
    voucher_dom: &'a Eip712Domain,
    slash_dom: &'a Eip712Domain,
    chain: &'a ResolvedChain,
    provider: Address,
    self_address: Address,
    /// Applied per chunk, matching how `--stall-timeout-ms`/`--timeout-ms` are
    /// documented to apply per entry for `bundle pull`.
    deadlines: PullDeadlines,
    max_blob_bytes: u64,
    /// The namespace the manifest's content is published under (ADR 002 §
    /// Retrieval by namespace); every chunk pull routes on it. `NO_NAMESPACE`
    /// when `--namespace` was omitted.
    namespace_id: [u8; 32],
}

impl<P: alloy::providers::Provider + Clone> ChunkFetcher<'_, P> {
    /// Pull one chunk blob on a fresh channel context.
    async fn fetch(&self, hash: [u8; 32]) -> anyhow::Result<Vec<u8>> {
        let ctx = build_channel_ctx(
            self.store,
            self.contract,
            self.rpc,
            self.signer,
            self.voucher_dom,
            self.provider,
            self.self_address,
            self.chain,
            self.endpoint,
        )
        .await?;
        fetch_blob(
            self.endpoint,
            self.target.clone(),
            &ctx,
            self.slash_dom,
            self.provider,
            self.store,
            hash,
            self.namespace_id,
            self.deadlines,
            self.max_blob_bytes,
            // No per-chunk byte bar: it would reset once per chunk and read as a
            // stuttering restart. Chunk progress is the printed lines here plus
            // the per-chunk error context from `file_manifest::reconstruct`.
            None,
        )
        .await
        .map_err(|err| annotate_unbound_cache_miss(err, &ctx))
    }

    /// Reconstruct the manifest's file into `output` and report.
    async fn reconstruct(
        &self,
        manifest: &file_manifest::FileManifest,
        manifest_hash: [u8; 32],
        output: &Path,
        keep_blobs: bool,
    ) -> anyhow::Result<()> {
        println!(
            "fetched a file manifest ({} chunk(s), {} bytes); reconstructing",
            manifest.chunks.len(),
            manifest.total_bytes
        );
        let written = file_manifest::reconstruct(
            manifest,
            manifest_hash,
            &file_manifest::downloads_root(&self.chain.data_dir),
            output,
            keep_blobs,
            |chunk_hash| self.fetch(chunk_hash),
        )
        .await?;
        println!("reconstructed {written} bytes -> {}", output.display());
        Ok(())
    }
}

/// [`open_or_reuse`] plus the ADR 005 client identity binding (#1115): sign our
/// OWN iroh `NodeId` with the buyer key so the serving node can prove we own the
/// channel and reactively pull a cache-missed blob from its configured origin.
///
/// The bind domain's verifying contract is the `CapacityBond`; without one
/// configured we can't sign, so the request goes out unbound and the node serves
/// only content it already holds (a cache miss is refused). No warning is
/// emitted for that — it would fire on every successful cached fetch too,
/// training users to ignore it; the refusal is explained at the point of failure
/// by [`annotate_unbound_cache_miss`].
///
/// Its own function (rather than inline in [`fetch`]) because a chunked-manifest
/// download rebuilds the context once per chunk — see the call site.
#[allow(clippy::too_many_arguments)]
async fn build_channel_ctx<P>(
    store: &RedbBuyerChannelStore,
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    rpc: &P,
    signer: &Arc<PrivateKeySigner>,
    voucher_dom: &Eip712Domain,
    provider: Address,
    self_address: Address,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
) -> anyhow::Result<ChannelContext>
where
    P: alloy::providers::Provider + Clone,
{
    let ctx = open_or_reuse(
        store,
        contract,
        rpc,
        signer,
        voucher_dom,
        provider,
        self_address,
        chain.payment_channel,
        chain.deposit,
        chain.max_approve,
    )
    .await?;
    let Some(capacity_bond) = chain.capacity_bond else {
        return Ok(ctx);
    };
    let bind_dom = bind_node_id_domain(chain.chain_id, capacity_bond);
    let own_node_id = B256::from(*endpoint.id().as_bytes());
    Ok(ctx.with_client_binding(sign_client_binding(signer, own_node_id, &bind_dom)?))
}

/// Reuse the live channel tracked for `provider` (resuming its watermark), or
/// open and persist a new one. A reused channel whose remaining deposit has run
/// low is auto-refilled on-chain via `topUp` before it is returned (#1103) — see
/// [`refill_amount`] for the policy. A tracked-but-expired channel is instead
/// replaced (opening a fresh one), since `topUp` cannot extend expiry; reclaiming
/// the expired channel's deposit is deferred (the node service handles reclaim,
/// #940 follow-up — until then a replaced expired channel's residual deposit is
/// reclaim-able only manually).
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
            // Auto-refill a live channel whose remaining deposit has run low, so
            // a sustained series of fetches against one provider isn't stranded
            // by a spent-down deposit (#1103). `topUp` does not extend expiry, so
            // a near-expiry channel is still replaced below, never topped up.
            let low_water = deposit / U256::from(LOW_WATER_DIVISOR);
            let additional = refill_amount(state.deposit, state.last_amount, deposit, low_water);
            let state = if additional.is_zero() {
                state
            } else {
                eprintln!(
                    "buyer channel {} (provider {provider}) low on deposit ({} µUSDC remaining); \
                     topping up {additional} µUSDC",
                    state.channel_id,
                    state.deposit.saturating_sub(state.last_amount)
                );
                // `topUp` pulls `additional` USDC via `transferFrom`, so the
                // channel's standing allowance must cover it first. A pre-existing
                // channel DB reused under a wallet whose allowance was revoked (or
                // an exact-approve open, which leaves zero residual allowance after
                // `openChannel` consumes it) would otherwise revert. Ensure it in
                // the caller's mode: unlimited under `--max-approve`, else exactly
                // `additional`.
                ensure_allowance(
                    rpc,
                    state.token,
                    self_address,
                    payment_channel_addr,
                    if max_approve { None } else { Some(additional) },
                )
                .await?;
                // The escrowed-but-untracked outcomes are logged inside `top_up`;
                // the CLI re-reads the row below and reflects whatever landed.
                let _ = top_up(contract, store, provider, additional).await?;
                // Re-read so the returned context's deposit reflects the top-up
                // (and any concurrent watermark advance the store folded in);
                // fall back to the pre-top-up state if the row vanished.
                store.get_by_provider(provider)?.unwrap_or(state)
            };
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
    // `max_approve` opts into an unlimited standing allowance; otherwise approve
    // exactly the (clamped) deposit being escrowed. Unconditional either way — the
    // old `false` branch issued no approve at all, so `openChannel`'s internal
    // `transferFrom` reverted unless the wallet had pre-approved out of band.
    let approve_amount = if max_approve { None } else { Some(deposit) };
    ensure_allowance(
        rpc,
        token,
        self_address,
        payment_channel_addr,
        approve_amount,
    )
    .await?;
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

/// A unique `O_CREAT|O_EXCL` temp file in `target`'s directory, ready to be
/// `persist`ed over it. Staging beside the destination is what makes the final
/// rename atomic (same filesystem) — a temp in `/tmp` would not be.
pub(crate) fn temp_in_parent(target: &Path) -> std::io::Result<tempfile::NamedTempFile> {
    match target.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(p) => tempfile::NamedTempFile::new_in(p),
        None => tempfile::NamedTempFile::new_in("."),
    }
}

/// Write `bytes` to `target` atomically: a unique `O_CREAT|O_EXCL` temp in the
/// destination directory, then an atomic rename-replace.
pub(crate) fn write_blob_atomic(target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut tmp = temp_in_parent(target)?;
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
            proxy_warming: false,
            proxy_warming_rtt_threshold_ms: 150,
            proxy_warming_margin_ms: 30,
            chain_id: None,
            keystore: None,
            data_dir: Some(PathBuf::from("/tmp/d")),
            deposit_micro_usdc: None,
            max_blob_mb: 1024,
            stall_timeout_ms: 30_000,
            timeout_ms: 3_600_000,
            no_keep_blobs: false,
        }
    }

    fn config(body: &str) -> FileConfig {
        toml::from_str(body).expect("parse test config")
    }

    fn ctx_with(binding: Option<decdn_protocol::client::ClientBinding>) -> ChannelContext {
        ChannelContext {
            channel_id: B256::ZERO,
            token: Address::ZERO,
            deposit: U256::ZERO,
            client_signer: Arc::new(PrivateKeySigner::random()),
            voucher_domain: bind_node_id_domain(1, Address::ZERO),
            prior_nonce: U256::ZERO,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: binding,
        }
    }

    /// The refusal these tests annotate, built the way the fetch path builds it: the typed
    /// `UpstreamRefused` sentinel (#1144). Never hand-roll one with
    /// `anyhow!("delivery refused: …")` — the annotation downcasts, so a look-alike string
    /// would exercise nothing and pass against a hint that never fires in production.
    fn refusal(error: StreamError) -> anyhow::Error {
        anyhow::Error::new(UpstreamRefused {
            error,
            response: None,
        })
    }

    /// An unbound (no `capacity_bond_address`) fetch refused with `NotFound` gets
    /// the actionable hint attached, reconnecting the opaque refusal to its cause.
    #[test]
    fn unbound_notfound_refusal_gets_actionable_hint() {
        let annotated =
            annotate_unbound_cache_miss(refusal(StreamError::NotFound), &ctx_with(None));
        assert!(
            annotated.to_string().contains("capacity_bond_address"),
            "expected the binding hint, got: {annotated}"
        );
    }

    /// A bound fetch's error is passed through untouched — a `NotFound` there is a
    /// genuine miss, not a missing-binding problem.
    #[test]
    fn bound_notfound_refusal_is_untouched() {
        let signer = PrivateKeySigner::random();
        let binding =
            sign_client_binding(&signer, B256::ZERO, &bind_node_id_domain(1, Address::ZERO))
                .expect("sign binding");
        let annotated =
            annotate_unbound_cache_miss(refusal(StreamError::NotFound), &ctx_with(Some(binding)));
        assert!(!annotated.to_string().contains("capacity_bond_address"));
    }

    /// A refusal that is NOT `NotFound` gets no binding hint even when unbound: a
    /// client binding authorizes reactive pull-through, so it cannot fix a node
    /// that is degraded (`InternalError`) or a blob that is over the ceiling.
    /// Previously indistinguishable — the string sniff matched any refusal whose
    /// text happened to contain `NotFound`.
    #[test]
    fn unbound_non_notfound_refusal_is_untouched() {
        for error in [StreamError::InternalError, StreamError::BlobTooLarge] {
            let annotated = annotate_unbound_cache_miss(refusal(error.clone()), &ctx_with(None));
            assert!(
                !annotated.to_string().contains("capacity_bond_address"),
                "{error:?} must not get the binding hint"
            );
        }
    }

    /// A non-`NotFound` failure (e.g. a transport error) is never mislabeled as a
    /// missing-binding problem, even when unbound.
    #[test]
    fn unbound_non_notfound_error_is_untouched() {
        let annotated = annotate_unbound_cache_miss(
            anyhow::anyhow!("connect failed: timed out"),
            &ctx_with(None),
        );
        assert!(!annotated.to_string().contains("capacity_bond_address"));
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

    /// The deadlines a fetch actually runs under (#1134): an inactivity bound for
    /// health, plus a generous overall cap that only a pathological provider can reach
    /// (#1145 review). The blob-size ceiling feeds into NEITHER — it used to scale the
    /// deadline at an assumed 35 MiB/s, which is what made a large-but-healthy transfer
    /// fail.
    #[test]
    fn deadlines_are_stall_bound_with_a_generous_leak_guard() {
        let mut c = common();
        assert_eq!(c.stall_timeout(), Duration::from_secs(30));
        assert_eq!(c.hard_cap(), Duration::from_hours(1));
        c.max_blob_mb = 4096;
        assert_eq!(
            c.hard_cap(),
            Duration::from_hours(1),
            "blob size must not move the deadline"
        );
        c.timeout_ms = 200_000;
        assert_eq!(c.hard_cap(), Duration::from_secs(200));
    }

    #[test]
    fn client_default_max_approve_is_exact() {
        // Absent `buyer_max_approve` → client defaults to exact (deposit-sized)
        // approval, i.e. `max_approve == false`.
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
        );
        let r = resolve_chain(&common(), &file).unwrap();
        assert!(!r.max_approve);
    }

    #[test]
    fn buyer_max_approve_true_opts_into_unlimited() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\nbuyer_max_approve = true\n",
        );
        let r = resolve_chain(&common(), &file).unwrap();
        assert!(r.max_approve);
    }

    #[test]
    fn explicit_data_dir_not_client_scoped() {
        // `common()` sets an explicit data_dir; it must be used verbatim (no
        // client-subdir scoping) for both the store dir and the keystore.
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
        );
        let r = resolve_chain(&common(), &file).unwrap();
        assert_eq!(r.data_dir, PathBuf::from("/tmp/d"));
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

    /// A present-but-zero contract address fails fast at resolve time via the
    /// shared `parse_nonzero_address` guard, not as an opaque on-chain revert
    /// later (#1213). Each contract address on the fetch path is checked
    /// independently; the EOA `--provider-address` stays unguarded by design.
    #[test]
    fn resolve_chain_rejects_zero_payment_channel() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x0000000000000000000000000000000000000000\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
        );
        let err = resolve_chain(&common(), &file).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("payment_channel_address"), "{err}");
        assert!(msg.contains("must not be the zero address"), "{err}");
    }

    #[test]
    fn resolve_chain_rejects_zero_slash_judge() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x0000000000000000000000000000000000000000\"\n",
        );
        let err = resolve_chain(&common(), &file).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("slash_judge_address"), "{err}");
        assert!(msg.contains("must not be the zero address"), "{err}");
    }

    #[test]
    fn resolve_chain_rejects_zero_capacity_bond() {
        // `capacity_bond_address` is optional, but a present zero must still
        // error (it does not silently resolve to `None`).
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\ncapacity_bond_address = \"0x0000000000000000000000000000000000000000\"\n",
        );
        let err = resolve_chain(&common(), &file).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("capacity_bond_address"), "{err}");
        assert!(msg.contains("must not be the zero address"), "{err}");
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
