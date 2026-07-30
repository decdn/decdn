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
//! across a bundle's many entries.

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
    ChannelContext, ChannelLedger, Cumulative, ProgressCallback, PullDeadlines,
    ResumeOffsetPastEnd, UpstreamRefused, UpstreamVoucherRejected, VoucherProgress,
    open_progressive_pull, sign_client_binding, stream_fetch_tracked_with_progress,
};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_channel::{AdvanceOutcome, BuyerChannelState, BuyerChannelStore};
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{bind_node_id_domain, slash_judge_domain, voucher_domain};
use decdn_protocol::client::StreamError;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};

use super::chain_ctx;
use decdn_client_pull::discovery::{self, NodeCandidate};
use decdn_client_pull::endpoint as client_endpoint;
use decdn_client_pull::probe::probe_once;
use decdn_client_pull::provider;

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

/// Parse a user-supplied `--channel-id`: 64 hex chars, optionally `0x`-prefixed
/// (the on-chain `channelId` is `keccak256(client, provider, channelNonce)`, a
/// `bytes32`).
pub(crate) fn parse_channel_id(s: &str) -> anyhow::Result<B256> {
    B256::from_str(s).map_err(|e| {
        anyhow::anyhow!("invalid --channel-id {s:?}: expected a 32-byte hex hash: {e}")
    })
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
    /// Deposit to escrow when OPENING a new channel (ignored on reuse).
    pub(crate) initial_deposit: U256,
    /// Deposit a reused channel's proactive refill targets once it has served
    /// verified bytes. `0` disables top-up.
    pub(crate) working_deposit: U256,
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

    let initial_deposit = U256::from(
        args.initial_deposit_micro_usdc
            .or_else(|| bc.and_then(|b| b.buyer_initial_deposit_micro_usdc))
            .unwrap_or(decdn_common::config::DEFAULT_BUYER_INITIAL_DEPOSIT_MICRO_USDC),
    );
    let working_deposit = U256::from(
        args.working_deposit_micro_usdc
            .or_else(|| bc.and_then(|b| b.buyer_working_deposit_micro_usdc))
            .unwrap_or(decdn_common::config::DEFAULT_BUYER_WORKING_DEPOSIT_MICRO_USDC),
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
        initial_deposit,
        working_deposit,
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
    // Probe concurrently in one task. `probe_once`'s future is `Send`, so
    // `tokio::spawn` would work too; `join_all` over a shared `&endpoint` is
    // kept because it needs no per-probe clone. `probe_once`'s internal
    // timeout bounds each leg.
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

/// Resolve the iroh node id that serves `provider`, for the `--channel-id`
/// adopt path (#1481): an explicit `--node-id` is used directly (the same flag
/// the auto-open path accepts), otherwise the `CapacityBond` registry is
/// searched for the entry whose `eth_address` matches `provider` — there is no
/// discovered-node ranking to derive it from, since adopt-by-id skips the
/// probe/select step entirely.
pub(crate) async fn resolve_node_for_provider(
    args: &cli::ClientFetchArgs,
    chain: &ResolvedChain,
    provider: Address,
) -> anyhow::Result<PublicKey> {
    if let Some(raw) = &args.node_id {
        return PublicKey::from_str(raw)
            .map_err(|e| anyhow::anyhow!("invalid --node-id {raw:?}: {e}"));
    }
    let capacity_bond = chain.capacity_bond.ok_or_else(|| {
        anyhow::anyhow!(
            "--channel-id without --node-id needs capacity_bond_address (--capacity-bond-address \
             or blockchain.capacity_bond_address) to look up the provider's node id, or pass \
             --node-id to dial it directly"
        )
    })?;
    let bootstrap = discovery::bootstrap_nodes(
        &chain.rpc_url,
        capacity_bond,
        &chain.data_dir,
        args.discovery_cap(),
    )
    .await?;
    if let Some(warning) = bootstrap.warning() {
        eprintln!("{warning}");
    }
    bootstrap
        .into_peers()
        .into_iter()
        .find(|c| c.eth_address == provider)
        .map(|c| c.node_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "provider {provider} not found in the CapacityBond registry; pass --node-id to \
                 dial it directly"
            )
        })
}

/// Authoritative on-chain view of a channel (a `getChannel` read), distilled
/// into just the fields [`adopt_decision`] needs. Keeping it scalar (rather
/// than the alloy `Channel` binding) lets the decision be unit-tested without
/// constructing contract types — mirrors `decdn-node`'s `OnChainOpen`
/// (`crates/node/src/buyer_channel.rs`), plus `status` (the CLI adopt path
/// must itself distinguish `Closing`/`Closed`, unlike the node reconcile,
/// which only cares whether the channel is still `Open`).
// No `Debug` derive: the generated `PaymentChannel::Status` doesn't implement
// it (see `status_label` for the printable form used in error messages).
#[derive(Clone)]
pub(crate) struct OnChainChannelView {
    pub(crate) channel_id: B256,
    /// On-chain `channel.client` — the funder / refund destination. Distinct
    /// from `voucher_signer`.
    pub(crate) client: Address,
    pub(crate) provider: Address,
    /// On-chain `channel.voucherSigner` — the pinned EIP-712 signer (#1481).
    pub(crate) voucher_signer: Address,
    pub(crate) token: Address,
    pub(crate) deposit: U256,
    pub(crate) expires_at: u64,
    pub(crate) claimed_nonce: U256,
    pub(crate) claimed_bytes: U256,
    pub(crate) claimed_amount: U256,
    pub(crate) status: PaymentChannel::Status,
}

/// Outcome of the pure adopt-by-id decision (#1481). Naming the reject reasons
/// (rather than collapsing to a bare `Option`) lets [`hydrate_channel_by_id`]
/// give the user an actionable, specific error for each one.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AdoptOutcome {
    /// Adoptable: `voucherSigner` is a key we hold, the channel is `Open`, and
    /// unexpired. Carries the buyer-store row to persist, watermark already
    /// seeded from the on-chain claimed totals. Boxed to keep the enum small
    /// (the other variants are unit), mirroring the node reconcile's
    /// `ReconcileOutcome::Rehydrate`.
    Adopt(Box<BuyerChannelState>),
    /// This process cannot sign vouchers `ch.voucherSigner` would accept.
    NotSignable,
    /// `ch.status` is `Closing` or `Closed`.
    NotOpen,
    /// `now >= ch.expiresAt`.
    Expired,
}

/// Printable form of `PaymentChannel::Status` — the alloy-generated enum
/// doesn't implement `Debug` (see the `OnChainChannelView` note), so error
/// messages that name the status go through this instead of `{:?}`.
const fn status_label(s: PaymentChannel::Status) -> &'static str {
    match s {
        PaymentChannel::Status::Open => "Open",
        PaymentChannel::Status::Closing => "Closing",
        PaymentChannel::Status::Closed => "Closed",
        // `Status` is a Solidity `enum`, decoded from a `uint8`; alloy's binding
        // is not proven exhaustive against a future on-chain discriminant.
        _ => "unknown",
    }
}

/// Decide whether to adopt an on-chain channel by id (#1481, `decdn fetch
/// --channel-id`). Pure (no I/O) so the policy is unit-testable; the network
/// `getChannel` read lives in [`hydrate_channel_by_id`].
///
/// Checked in order: can this process sign for `voucherSigner` (else the
/// channel is useless to us no matter its status), is it still `Open`, is it
/// unexpired. On success, the returned [`BuyerChannelState`] has its
/// watermark seeded from the on-chain claimed totals via
/// [`BuyerChannelState::advance`] — mirroring the node reconcile's
/// `reconcile_decision` — so a channel that already saw deliveries resumes at
/// the right nonce instead of re-signing from zero (which the provider would
/// reject). `advance` cannot regress here (`new()` zeroes `last_*` and
/// on-chain claimed totals are `>= 0`); on the impossible error the
/// un-advanced (zero-watermark) state is kept rather than panicking.
pub(crate) fn adopt_decision(ch: &OnChainChannelView, my_key: Address, now: u64) -> AdoptOutcome {
    if ch.voucher_signer != my_key {
        return AdoptOutcome::NotSignable;
    }
    if !matches!(ch.status, PaymentChannel::Status::Open) {
        return AdoptOutcome::NotOpen;
    }
    if ch.expires_at != 0 && now >= ch.expires_at {
        return AdoptOutcome::Expired;
    }
    let mut state = BuyerChannelState::new(
        ch.channel_id,
        ch.provider,
        ch.client,
        ch.voucher_signer,
        ch.token,
        ch.deposit,
        ch.expires_at,
    );
    if let Err(err) = state.advance(ch.claimed_nonce, ch.claimed_bytes, ch.claimed_amount) {
        eprintln!(
            "warning: channel {} on-chain claimed totals could not seed the watermark ({err}); \
             adopting with a zero watermark",
            ch.channel_id
        );
    }
    AdoptOutcome::Adopt(Box::new(state))
}

/// Adopt a delegated channel by id (publisher-pays, #1481): first use hydrates
/// a buyer-store row from chain; later uses reuse it. `store.get_by_channel_id`
/// is tried first — if a row is already tracked, its provider is trusted (no
/// re-verification against chain on every fetch) and reused as-is. Otherwise
/// `getChannel` is read, [`adopt_decision`] is applied, and on `Adopt` the row
/// is persisted before returning.
///
/// `expected_provider` is `Some` when the caller also passed
/// `--provider-address`; a mismatch against the channel's actual provider is a
/// hard error rather than silently overriding the flag.
///
/// Returns the built [`ChannelContext`] plus the resolved provider address (so
/// the caller can dial it) or a clear, actionable error — never lets an
/// unadoptable channel fall through to a confusing on-chain revert.
pub(crate) async fn hydrate_channel_by_id<P>(
    store: &RedbBuyerChannelStore,
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    channel_id: B256,
    my_key: Address,
    voucher_domain: &Eip712Domain,
    signer: &Arc<PrivateKeySigner>,
    expected_provider: Option<Address>,
) -> anyhow::Result<(ChannelContext, Address)>
where
    P: alloy::providers::Provider + Clone,
{
    if let Some(state) = store.get_by_channel_id(channel_id)? {
        if let Some(expected) = expected_provider {
            anyhow::ensure!(
                expected == state.provider,
                "channel {channel_id} is tracked for provider {}, not --provider-address {expected}",
                state.provider
            );
        }
        let provider = state.provider;
        return Ok((
            ChannelContext::for_buyer_channel(&state, Arc::clone(signer), voucher_domain.clone()),
            provider,
        ));
    }

    let ch = contract
        .getChannel(channel_id)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentChannel.getChannel({channel_id}): {e}"))?;
    if let Some(expected) = expected_provider {
        anyhow::ensure!(
            expected == ch.provider,
            "channel {channel_id} provider is {}, not --provider-address {expected}",
            ch.provider
        );
    }
    let view = OnChainChannelView {
        channel_id,
        client: ch.client,
        provider: ch.provider,
        voucher_signer: ch.voucherSigner,
        token: ch.token,
        deposit: ch.deposit,
        expires_at: ch.expiresAt,
        claimed_nonce: ch.claimedNonce,
        claimed_bytes: ch.claimedBytes,
        claimed_amount: ch.claimedAmount,
        status: ch.status,
    };
    match adopt_decision(&view, my_key, unix_now()) {
        AdoptOutcome::Adopt(state) => {
            store
                .record(&state)
                .map_err(|e| anyhow::anyhow!("persist adopted channel {channel_id}: {e}"))?;
            let provider = state.provider;
            Ok((
                ChannelContext::for_buyer_channel(
                    &state,
                    Arc::clone(signer),
                    voucher_domain.clone(),
                ),
                provider,
            ))
        }
        AdoptOutcome::NotSignable => anyhow::bail!(
            "channel {channel_id} voucherSigner ({}) is not in your keystore ({my_key})",
            view.voucher_signer
        ),
        AdoptOutcome::NotOpen => anyhow::bail!(
            "channel {channel_id} is not Open (status: {}) — nothing to adopt",
            status_label(view.status)
        ),
        AdoptOutcome::Expired => anyhow::bail!(
            "channel {channel_id} expired at {} — nothing to adopt",
            view.expires_at
        ),
    }
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
        .is_some_and(|refused| matches!(refused.error(), StreamError::NotFound));
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

/// The scratch file a streaming fetch writes into before it is promoted to
/// `--output`. Living beside the destination (not in `/tmp`) is what makes the
/// final step an atomic same-filesystem rename, and what lets a later invocation
/// find the partial and resume it.
fn partial_path(output: &Path) -> PathBuf {
    let mut name = output.as_os_str().to_os_string();
    name.push(".partial");
    PathBuf::from(name)
}

/// Outcome of a streaming fetch into the partial file.
struct StreamedFetch {
    /// Whole-blob size the node signed for.
    total_bytes: u64,
    /// Byte offset this attempt started from — non-zero when a prior partial was
    /// resumed. Drives the whole-file re-hash, which is only needed when some of
    /// the output came off disk rather than off the verified wire.
    resumed_from: u64,
}

/// Fetch `hash` into `<output>.partial`, writing each bao chunk group the moment
/// it verifies and resuming an interrupted prior attempt (#1120, #1122).
///
/// This is the streaming counterpart of [`fetch_blob`]. Where that buffers the
/// whole blob in RAM (twice — the wire form and then the decoded form) and writes
/// once at the end, this holds one chunk group and appends as it goes, so peak
/// memory is independent of blob size and an interruption leaves a resumable
/// prefix on disk instead of nothing.
///
/// Resume re-runs discovery in the caller, not here: content is content-addressed,
/// so whichever node this call is pointed at can serve the tail.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)] // One sequential open→stream→persist→classify attempt loop. Each stage's comment explains a money-relevant decision (which watermark to settle, when a partial is poison, why a flush failure outranks a pull failure); splitting them out would separate those from the loop state they justify.
async fn fetch_blob_streaming(
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
    max_rate_per_mb: u64,
    partial: &Path,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<StreamedFetch> {
    use std::io::{BufWriter, Seek, SeekFrom};

    // What a previous attempt left behind, snapped down to a chunk-group
    // boundary: the server anchors its proof to whole groups, so anything past
    // the last boundary has to be re-fetched anyway.
    //
    // Only `NotFound` means "no partial". Any other stat error (permissions, a
    // transient fault, `ENOTDIR`) must NOT be read as zero: that would silently
    // truncate a large verified prefix and re-pay for the whole blob, which is
    // real money lost to a condition we could have reported.
    let mut byte_offset = decdn_client_pull::sink::resume_offset(existing_partial_len(partial)?);

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(false)
        .open(partial)
        .map_err(|e| anyhow::anyhow!("open {}: {e}", partial.display()))?;
    // The truncate-and-seek to `byte_offset` happens at the top of the attempt
    // loop below, which covers the first pass too — the file's length always
    // equals the verified prefix length when a pull starts.

    // One ledger for this channel, seeded from its persisted cumulative state so
    // the first voucher continues at `prior_nonce + 1` rather than restarting
    // from zero (which the node rejects as a stale nonce). Same seeding the
    // buffered path does internally.
    let ledger = Arc::new(ChannelLedger::new(Cumulative {
        nonce: ctx.prior_nonce,
        bytes: ctx.prior_bytes_delivered,
        amount: ctx.prior_amount,
    }));

    // Wallet-less resume (#1481): a voucher rejection carrying a bundle signed by
    // our OWN key means our persisted watermark had fallen behind what the node
    // holds — reseed from it and reopen. The buffered path gets this from
    // `fetch_inner`'s internal loop; the streaming path has to drive its own,
    // because between attempts the OUTPUT FILE must be rewound to `byte_offset`
    // (the retry re-fetches the same span, and appending it twice would corrupt
    // the download). `client-pull` cannot do that for us — it does not own the
    // file — so the loop lives here and the two share `MAX_RESUME_ATTEMPTS`.
    // A `loop` with an explicit counter, not `for attempt in 0..=MAX`: the
    // stale-partial restart below must NOT consume the resume budget (it is a
    // restart, not a retry), and with a `for` range a restart on the final
    // iteration fell out of the loop entirely — dropping the real error for an
    // internal "unreachable" string, after telling the user it was refetching.
    // Here every path either returns or increments, so there is no fall-through
    // to get wrong.
    let mut total_bytes = 0u64;
    let mut attempt = 0u32;
    let mut restarted = false;
    loop {
        // Open BEFORE touching the file. The rewind below is destructive, and an
        // open can fail for reasons that have nothing to do with the partial (the
        // node has evicted the blob, the channel is unknown, the link is down) —
        // truncating first would destroy a verified, paid-for prefix and then
        // fail anyway, leaving the user worse off than when they started.
        let opened = open_progressive_pull(
            endpoint,
            target.clone(),
            ctx,
            Arc::clone(&ledger),
            slash_dom,
            provider,
            hash,
            namespace_id,
            byte_offset,
            micros_now(),
            max_blob_bytes,
            max_rate_per_mb,
            deadlines,
        )
        .await;

        let result = match opened {
            Ok((header, pull)) => {
                total_bytes = header.total_bytes;
                // The open succeeded, so this attempt is really going to write.
                // Rewind to the verified prefix: a no-op on the first pass, and on
                // a retry it discards whatever the failed attempt left behind.
                file.set_len(byte_offset)
                    .map_err(|e| anyhow::anyhow!("truncate {}: {e}", partial.display()))?;
                file.seek(SeekFrom::Start(byte_offset))
                    .map_err(|e| anyhow::anyhow!("seek {}: {e}", partial.display()))?;
                let mut writer = BufWriter::new(&mut file);
                let pulled = decdn_client_pull::sink::pull_to_sink(
                    pull,
                    hash,
                    header.total_bytes,
                    byte_offset,
                    &mut writer,
                    on_progress,
                )
                .await;
                // Flush before anything else: bytes stuck in the `BufWriter` are
                // bytes the resume path would re-pay for. This must run on the
                // ERROR path too — that is the whole point of the partial file.
                let flushed = std::io::Write::flush(&mut writer)
                    .map_err(|e| anyhow::anyhow!("flush {}: {e}", partial.display()));
                drop(writer);
                combine_pull_and_flush(pulled, flushed)
            }
            Err(e) => Err(e),
        };

        // The upstream may hold vouchers we sent but never saw acked, so persist
        // the watermark before doing anything else — otherwise the next attempt
        // re-signs a spent nonce and the channel is stranded, which is exactly
        // the failure #1122 describes.
        let progress = watermark_after(&result, &ledger, ctx.prior_nonce);
        persist_watermark(store, provider, ctx.channel_id, &progress);

        let Err(err) = result else {
            file.sync_all()
                .map_err(|e| anyhow::anyhow!("sync {}: {e}", partial.display()))?;
            return Ok(StreamedFetch {
                total_bytes,
                resumed_from: byte_offset,
            });
        };
        // Retry from the start when the refusal is consistent with the resume
        // offset being wrong (see `resume_may_be_stale`). At most once per
        // invocation, and it is a restart rather than a retry, so it does not
        // spend the resume budget.
        //
        // This is safe to attempt on an ambiguous signal ONLY because the rewind
        // now happens after a successful open: if the node simply does not have
        // the blob, the from-zero open fails too and the prefix is still on disk,
        // untouched, for a later run against a node that does.
        if !restarted && byte_offset > 0 && resume_may_be_stale(&err) {
            eprintln!(
                "note: the node would not serve a resume at byte {byte_offset} of {}; \
                 retrying from the start in case the partial belongs to another blob \
                 ({err})",
                partial.display()
            );
            restarted = true;
            byte_offset = 0;
            continue;
        }
        if attempt >= decdn_client_pull::MAX_RESUME_ATTEMPTS {
            return Err(err);
        }
        match decdn_client_pull::resumable_watermark(&err, ctx) {
            Some(bundle) => {
                ledger.reseed(Cumulative::from(bundle));
                // Worth surfacing rather than logging silently: it means this
                // client's persisted watermark had fallen behind what it had
                // actually signed, which is the #1122 desync healing itself.
                eprintln!(
                    "note: the node holds a later voucher than this client recorded; \
                     resynced and retrying (attempt {}/{})",
                    attempt + 1,
                    decdn_client_pull::MAX_RESUME_ATTEMPTS + 1
                );
                attempt += 1;
            }
            None => return Err(err),
        }
    }
}

/// Whether a failed open is consistent with the resume offset being wrong — i.e.
/// the `.partial` on disk belonging to a different (or larger) blob.
///
/// Two signals qualify, and the second is unavoidably ambiguous:
///
/// - [`ResumeOffsetPastEnd`] — the node signed a response whose `total_bytes` is
///   at or below our offset. Unambiguous, but only reachable against a
///   non-conforming server.
/// - `NotFound` — what an honest node actually sends. Its range gate refuses
///   `byte_offset >= total_bytes` with `RangeNotSatisfiable` *before* signing,
///   and that **deliberately collapses to `NotFound` on the wire** alongside a
///   cache miss and an unknown channel (`ServeRejectReason::wire_error`): telling
///   a client its range was bad is a reputation-benign refusal, and the codes are
///   kept indistinguishable on purpose. So the client cannot separate "your
///   offset is past the end" from "I don't have this blob".
///
/// Acting on the ambiguous one is safe here only because the caller rewinds the
/// partial *after* a successful open: a genuine cache miss fails the from-zero
/// open too, and the prefix survives untouched.
///
/// Everything else — stalls, resets, hash mismatches, local flush failures — must
/// NOT qualify. Those say nothing about the offset, and treating them as stale
/// would discard a verified prefix the user has already paid for.
fn resume_may_be_stale(err: &anyhow::Error) -> bool {
    if err.downcast_ref::<ResumeOffsetPastEnd>().is_some() {
        return true;
    }
    err.downcast_ref::<UpstreamRefused>()
        .is_some_and(|refused| matches!(refused.error(), StreamError::NotFound))
}

/// Combine a pull result with the flush of the bytes it wrote.
///
/// A flush failure is LOCAL and terminal — the disk is full, or the file went
/// away — so it wins over the pull's error: reporting "the peer stalled" for an
/// `ENOSPC` sends the user hunting in the wrong place, and a local fault must
/// never be mistaken for anything the retry loop can resolve.
///
/// But the pull error is CHAINED rather than dropped. It can be the only record
/// that the peer served corrupt bytes (`HashMismatch`, which also means the
/// on-disk prefix is poisoned) or that the channel needs a top-up — losing it
/// would erase the misbehaviour along with the diagnosis.
fn combine_pull_and_flush(
    pulled: anyhow::Result<VoucherProgress>,
    flushed: anyhow::Result<()>,
) -> anyhow::Result<VoucherProgress> {
    match (pulled, flushed) {
        (Ok(progress), Ok(())) => Ok(progress),
        (Ok(_), Err(flush_err)) => Err(flush_err),
        (Err(pull_err), Ok(())) => Err(pull_err),
        (Err(pull_err), Err(flush_err)) => {
            Err(flush_err.context(format!("the transfer had also failed: {pull_err:#}")))
        }
    }
}

/// The watermark to persist after an attempt, given how it ended.
///
/// The two ledger readers are equal whenever every voucher has been resolved, and
/// diverge exactly when some are still armed — so which one is correct depends on
/// whether the upstream's silence is ambiguous:
///
/// - **Ambiguous failure** (stall, reset, local fault): the upstream persists a
///   voucher before it acks, so it most likely holds the armed ones. Settle HIGH
///   (`settlement`). Settling low re-signs a spent nonce and wedges the channel;
///   settling high at worst skips a nonce, which the serve side meters and
///   accepts. The errors are not symmetric, so take the survivable one (#1122).
/// - **Explicit rejection**: the node told us it refused a voucher and tore the
///   stream down. It applies vouchers in strict nonce order, so everything
///   pipelined BEHIND the rejected one (#1484 sends optimistically, so there
///   usually is something) was provably never taken. `resolve_reject` pops only
///   the front of the outstanding set, so `settlement` would still report those
///   followers. Settle at the acked watermark instead — and note the store
///   refuses to regress (`AdvanceOutcome::Regressed` is warn-only), so an
///   inflated value here is PERMANENT and can push `last_amount` above the
///   deposit, wedging the channel from the other direction.
fn watermark_after(
    outcome: &anyhow::Result<VoucherProgress>,
    ledger: &ChannelLedger,
    prior_nonce: U256,
) -> VoucherProgress {
    match outcome {
        Ok(progress) => *progress,
        Err(e) if e.downcast_ref::<UpstreamVoucherRejected>().is_some() => {
            VoucherProgress::from_cumulative(ledger.committed(), prior_nonce)
        }
        Err(_) => VoucherProgress::from_cumulative(ledger.settlement(), prior_nonce),
    }
}

/// Length of an existing partial download, or `0` if there isn't one.
///
/// Only `NotFound` means "no partial". Any other stat error (permissions, a
/// transient fault, `ENOTDIR`) must NOT be folded to zero: the caller truncates
/// to this value, so a silent `0` would destroy a large verified prefix and
/// re-pay for the whole blob — real money lost to a condition we could have
/// reported.
fn existing_partial_len(path: &Path) -> anyhow::Result<u64> {
    match std::fs::metadata(path) {
        Ok(m) => Ok(m.len()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(anyhow::anyhow!(
            "stat {}: {e} (refusing to treat this as 'no partial download' — \
             that would discard a resumable prefix and re-pay for it)",
            path.display()
        )),
    }
}

/// Persist what the channel paid, warning rather than masking the fetch outcome.
///
/// Shared by the buffered and streaming paths: the bytes were paid for either
/// way, and a failure to record that only risks a rejected reuse next time.
fn persist_watermark(
    store: &RedbBuyerChannelStore,
    provider: Address,
    channel_id: B256,
    progress: &VoucherProgress,
) {
    let Some((nonce, bytes_delivered, amount)) = progress.acked() else {
        return;
    };
    // A non-`Advanced` outcome (unknown provider / channel replaced / regression)
    // means the watermark did NOT move — same hazard as a backend error — so
    // surface it too rather than dropping it on the floor.
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
    max_rate_per_mb: u64,
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
        max_rate_per_mb,
        &mut progress,
        on_progress,
    )
    .await;

    persist_watermark(store, provider, channel_id, &progress);

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

    // `--channel-id` (#1481, publisher-pays): adopt an existing channel by id
    // instead of deriving/auto-opening one from `--provider-address`. Bypasses
    // `resolve_target_node`/`open_or_reuse` entirely — the target node is
    // derived from the adopted channel's on-chain `provider`, and there is no
    // discovery/probe/select step to run before prompting for the keystore
    // password.
    let (node_id, provider, ctx, slash_dom) = if let Some(raw_channel_id) = &common.channel_id {
        let channel_id = parse_channel_id(raw_channel_id)?;
        let expected_provider = common
            .provider_address
            .as_deref()
            .map(|p| chain_ctx::parse_address(p, "--provider-address"))
            .transpose()?;

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

        let (ctx, provider) = hydrate_channel_by_id(
            &store,
            &contract,
            channel_id,
            self_address,
            &voucher_dom,
            &signer,
            expected_provider,
        )
        .await?;
        let ctx = attach_client_binding(ctx, &chain, &endpoint, &signer)?;
        let node_id = resolve_node_for_provider(common, &chain, provider).await?;

        (node_id, provider, ctx, slash_dom)
    } else {
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
        // and persist a new one, with the ADR 005 client binding attached. The
        // context snapshots the channel's voucher watermark (`prior_nonce`) and gives
        // the fetch a low-deposit refill check (#1103).
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

        (node_id, provider, ctx, slash_dom)
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
    let (bar, on_progress) = delivery_progress();
    // The namespace routing hint (ADR 005 § Namespace routing): `--namespace <id>`
    // → big-endian `uint256`; absent => `NO_NAMESPACE` (best-effort cache/DHT).
    let namespace_id = args
        .namespace
        .map_or(decdn_protocol::client::NO_NAMESPACE, |n| {
            alloy::primitives::U256::from(n).to_be_bytes()
        });
    // Stream to `<output>.partial` rather than buffering the blob (#1120): peak
    // memory becomes one chunk group instead of ~2× the blob, and an interrupted
    // fetch leaves a resumable prefix behind instead of nothing. A prior partial
    // is picked up automatically and only the un-fetched tail is re-paid for.
    let partial = partial_path(&args.output);
    let streamed = fetch_blob_streaming(
        &endpoint,
        target,
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
        common.max_rate_per_mb,
        &partial,
        Some(&on_progress),
    )
    .await;
    // Clear the bar before the terminal outcome (success line or error) so it
    // never overwrites the final message, on either path.
    bar.finish_and_clear();
    // On error the partial file is deliberately LEFT in place — it is what the
    // next invocation resumes from, and deleting it here would re-charge the user
    // for every byte already paid for.
    let streamed = streamed.map_err(|err| annotate_unbound_cache_miss(err, &ctx))?;

    // A resumed fetch mixed bytes this process verified on the wire with bytes an
    // earlier process left on disk. The latter are unverified — not because a
    // prefix cannot be checked, but because this client keeps no outboard sidecar
    // to check it against (see `sink::resume_offset`) — so settle it here against
    // the content hash before anything is promoted. A fetch that started at 0
    // verified every byte as it landed and needs no second pass.
    if streamed.resumed_from > 0 {
        let file = std::fs::File::open(&partial)
            .map_err(|e| anyhow::anyhow!("reopen {}: {e}", partial.display()))?;
        let genuine =
            decdn_client_pull::sink::resume_is_genuine(hash, std::io::BufReader::new(file))
                .map_err(|e| anyhow::anyhow!("verify {}: {e}", partial.display()))?;
        if !genuine {
            // The bad bytes are in the prefix we inherited, so there is nothing
            // to salvage — drop it so the retry starts clean rather than
            // resuming onto the same corruption forever. If the removal itself
            // fails, say so: telling the user it "has been discarded" when it
            // has not sends them into a loop where the error message is what
            // prevents them diagnosing it.
            let hex = blake3::Hash::from_bytes(hash).to_hex();
            return Err(match std::fs::remove_file(&partial) {
                Ok(()) => anyhow::anyhow!(
                    "the partial download at {} did not match {hex} and has been discarded; \
                     re-run to fetch it cleanly",
                    partial.display()
                ),
                Err(e) => anyhow::anyhow!(
                    "the partial download at {} did not match {hex} and could not be removed \
                     ({e}); delete it by hand before re-running, or every retry will resume \
                     onto the same corrupt bytes",
                    partial.display()
                ),
            });
        }
    }

    // Promote the verified partial in place of a copy-through: an atomic rename
    // on the same filesystem, so `--output` never exists in a half-written state
    // and the blob is never held in memory to be written a second time.
    std::fs::rename(&partial, &args.output).map_err(|e| {
        anyhow::anyhow!(
            "promote {} -> {}: {e}",
            partial.display(),
            args.output.display()
        )
    })?;
    println!(
        "fetched {} bytes -> {}",
        streamed.total_bytes,
        args.output.display()
    );
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
        chain.initial_deposit,
        chain.working_deposit,
        chain.max_approve,
    )
    .await?;
    attach_client_binding(ctx, chain, endpoint, signer)
}

/// Attach the ADR 005 client identity binding to an already-built
/// [`ChannelContext`] (#1481): sign our OWN iroh `NodeId` with the buyer key so
/// the serving node can prove we own the channel and reactively pull a
/// cache-missed blob from its configured origin. Split out of
/// [`build_channel_ctx`] so the `--channel-id` adopt path — which builds its
/// context via [`hydrate_channel_by_id`] instead of [`open_or_reuse`] — can
/// attach the same binding without going through the auto-open kernel.
///
/// No binding when `chain.capacity_bond` is unset — see
/// [`build_channel_ctx`]'s docs for why that is silent.
pub(crate) fn attach_client_binding(
    ctx: ChannelContext,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
    signer: &Arc<PrivateKeySigner>,
) -> anyhow::Result<ChannelContext> {
    let Some(capacity_bond) = chain.capacity_bond else {
        return Ok(ctx);
    };
    let bind_dom = bind_node_id_domain(chain.chain_id, capacity_bond);
    let own_node_id = B256::from(*endpoint.id().as_bytes());
    Ok(ctx.with_client_binding(sign_client_binding(signer, own_node_id, &bind_dom)?))
}

/// Decision for the reuse-time deposit refill, gated on the on-chain
/// `topUp`-is-funder-only rule (`PaymentChannel.sol:379`, #1481). Pure (no I/O)
/// so the funder gate is unit-testable without a live contract; reused by any
/// future caller of the refill policy, not just `open_or_reuse`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TopUpDecision {
    /// The channel's remaining deposit is above the low-water mark; nothing to do.
    NotNeeded,
    /// Refill `additional` (`µUSDC`) on-chain — the local key is the channel's funder.
    TopUp(U256),
    /// The deposit is low/exhausted but the local key is not the funder (a
    /// delegated, publisher-pays channel, #1481) and so cannot `topUp`. The
    /// caller must fail fast with an actionable message rather than attempt
    /// (and let revert) an unauthorized `topUp`.
    Exhausted,
}

/// Gate [`refill_amount`]'s policy on funder ownership: only the on-chain
/// `client` (the funder) may call `topUp`; a delegate signing vouchers on a
/// publisher-pays channel cannot, no matter how depleted the deposit is.
pub(crate) fn top_up_decision(additional: U256, is_funder: bool) -> TopUpDecision {
    if additional.is_zero() {
        TopUpDecision::NotNeeded
    } else if is_funder {
        TopUpDecision::TopUp(additional)
    } else {
        TopUpDecision::Exhausted
    }
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
    initial_deposit: U256,
    working_deposit: U256,
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
            //
            // Refill toward the WORKING target (graduation), not the small initial
            // open deposit. `working_deposit == 0` disables top-up: leave the
            // channel as-is.
            let additional = if working_deposit.is_zero() {
                U256::ZERO
            } else {
                let low_water = working_deposit / U256::from(LOW_WATER_DIVISOR);
                refill_amount(state.deposit, state.last_amount, working_deposit, low_water)
            };
            let state = match top_up_decision(additional, state.funder == self_address) {
                TopUpDecision::NotNeeded => state,
                // `topUp` is funder-only on-chain (`PaymentChannel.sol:379`); this key
                // is a delegate (publisher-pays, #1481) holding only the
                // voucher-signing key, so attempting it would revert. Fail fast with
                // an actionable message instead of letting that opaque revert surface.
                TopUpDecision::Exhausted => anyhow::bail!(
                    "buyer channel {} (provider {provider}) is low on deposit ({} µUSDC \
                     remaining of {} deposited) and this key ({self_address}) is not the \
                     funder ({}); channel exhausted — ask the publisher to top up (the \
                     delegate cannot)",
                    state.channel_id,
                    state.deposit.saturating_sub(state.last_amount),
                    state.deposit,
                    state.funder,
                ),
                TopUpDecision::TopUp(additional) => {
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
                }
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
    // node's buyer path and the `--initial-deposit-micro-usdc` help text).
    let min_deposit = contract
        .minDeposit()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentChannel.minDeposit(): {e}"))?;
    let deposit = initial_deposit.max(min_deposit);
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
        // ZERO => self-signing (funder signs); publisher-pays passes a delegate via `channel open`.
        Address::ZERO,
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
            channel_id: None,
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
            initial_deposit_micro_usdc: None,
            working_deposit_micro_usdc: None,
            max_blob_mb: 1024,
            max_rate_per_mb: 0,
            stall_timeout_ms: 30_000,
            timeout_ms: 3_600_000,
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
        anyhow::Error::new(UpstreamRefused::mid_stream(error))
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
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\nbuyer_initial_deposit_micro_usdc = 500000\nbuyer_working_deposit_micro_usdc = 5000000\n",
        );
        let r = resolve_chain(&common(), &file).unwrap();
        assert_eq!(r.rpc_url, "http://config:8545");
        // chain_id absent everywhere → default.
        assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(r.initial_deposit, U256::from(500_000u64));
        assert_eq!(r.working_deposit, U256::from(5_000_000u64));
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

    /// The partial lives beside `--output` with a suffix, not in a temp dir: the
    /// promote step is a rename, which is only atomic within one filesystem, and
    /// a later invocation has to be able to find it to resume.
    /// The stale-partial restart must fire on the refusals that say something
    /// about the OFFSET, and on nothing else.
    ///
    /// The predicate this replaced was `resumable_watermark(..).is_none()` — i.e.
    /// "anything that is not a resumable voucher rejection" — which is true of
    /// every ordinary stall, reset and local flush failure. Each of those would
    /// have truncated a verified prefix the user already paid for and re-fetched
    /// the whole blob: the exact waste resume exists to prevent, and invisible to
    /// every other test because they all drive the happy path.
    ///
    /// `NotFound` has to be in the accepted set even though it is ambiguous: an
    /// honest node refuses an out-of-bounds range with `RangeNotSatisfiable`,
    /// which collapses to `NotFound` on the wire by design, so it is the only
    /// signal a real resume-past-the-end ever produces.
    #[test]
    fn only_offset_shaped_refusals_count_as_a_stale_partial() {
        use decdn_client_pull::{HashMismatch, PullStalled};

        let past_end = anyhow::Error::new(ResumeOffsetPastEnd {
            total_bytes: 100,
            byte_offset: 500_000,
        });
        assert!(
            resume_may_be_stale(&past_end),
            "an explicit past-end response must restart"
        );

        // `mid_stream` is just the no-evidence constructor; `resume_may_be_stale`
        // reads only the error code, which is the same on both refusal shapes.
        let refused_not_found =
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound));
        assert!(
            resume_may_be_stale(&refused_not_found),
            "NotFound is what an honest node's range refusal collapses to"
        );

        // Everything below must NOT restart. A resumed transfer that merely
        // hiccups has to keep its prefix.
        let transient: Vec<anyhow::Error> = vec![
            anyhow::Error::new(PullStalled {
                after: Duration::from_secs(30),
            }),
            anyhow::Error::new(HashMismatch),
            anyhow::anyhow!("flush /tmp/out.partial: No space left on device"),
            anyhow::anyhow!("connection reset"),
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::InternalError)),
        ];
        for err in &transient {
            assert!(
                !resume_may_be_stale(err),
                "this says nothing about the offset and must not discard the prefix: {err:#}"
            );
        }
    }

    /// Which ledger reader an attempt persists, per outcome. This is money: the
    /// store refuses to regress a watermark (`AdvanceOutcome::Regressed` is
    /// warn-only), so a value written here is effectively permanent.
    ///
    /// The three cases must differ, and each is wrong in a different direction:
    ///
    /// - success → whatever the pull reported;
    /// - AMBIGUOUS failure → `settlement()`, the high reader. The upstream
    ///   persists a voucher before acking, so it probably holds the armed ones;
    ///   settling low re-signs a spent nonce and wedges the channel (#1122).
    /// - EXPLICIT rejection → `committed()`, the low reader. The node refused a
    ///   voucher and tore the stream down, and it applies them in strict nonce
    ///   order, so anything pipelined behind the rejected one was provably never
    ///   taken. `resolve_reject` pops only the FRONT of the outstanding set, so
    ///   `settlement()` would still report those followers and inflate
    ///   `last_amount` — potentially above the deposit, wedging the channel from
    ///   the other side.
    #[tokio::test]
    async fn watermark_after_picks_the_reader_that_matches_the_outcome() -> anyhow::Result<()> {
        use decdn_protocol::client::VoucherRejectReason;

        // Two vouchers issued and left armed — never acked, never rejected.
        let armed = || async {
            let ledger = ChannelLedger::new(Cumulative::default());
            for _ in 0..2u32 {
                ledger.issue(1_000, 10, |_next| async { Ok(()) }).await?;
            }
            Ok::<_, anyhow::Error>(ledger)
        };

        // The two readers must actually diverge, or this test proves nothing.
        let ledger = armed().await?;
        anyhow::ensure!(
            ledger.settlement().nonce > ledger.committed().nonce,
            "fixture is inert: settlement and committed must differ while vouchers are armed"
        );
        let high = ledger.settlement().nonce;
        let low = ledger.committed().nonce;

        // Ambiguous failure — a stall says nothing about what the node kept.
        let ambiguous: anyhow::Result<VoucherProgress> = Err(anyhow::anyhow!("peer stalled"));
        let got = watermark_after(&ambiguous, &ledger, U256::ZERO);
        anyhow::ensure!(
            got.acked().map(|(n, _, _)| n) == Some(high),
            "an ambiguous failure must settle HIGH, or the channel wedges on a spent nonce"
        );

        // Explicit rejection — the node told us it took nothing further.
        let rejected: anyhow::Result<VoucherProgress> =
            Err(anyhow::Error::new(UpstreamVoucherRejected {
                reason: VoucherRejectReason::StaleNonce,
                bundle: None,
            }));
        let got = watermark_after(&rejected, &ledger, U256::ZERO);
        let persisted = got.acked().map(|(n, _, _)| n);
        anyhow::ensure!(
            persisted == Some(low) || persisted.is_none(),
            "an explicit rejection must settle at the ACKED watermark ({low}), not {persisted:?}              — vouchers pipelined behind a rejected one were never taken, and the store will              not let an inflated value be corrected later"
        );
        Ok(())
    }

    /// A flush failure is local and terminal, so it must win — but the pull error
    /// must survive as context. It can be the only record that the peer served
    /// corrupt bytes (which also means the on-disk prefix is poisoned) or that the
    /// channel needs a top-up; the `and_then` this replaced dropped it entirely.
    #[test]
    fn combine_pull_and_flush_prefers_the_flush_error_but_keeps_the_pull_cause() {
        let progress = VoucherProgress::default();

        let ok = combine_pull_and_flush(Ok(progress), Ok(()));
        assert!(ok.is_ok(), "both succeeding must succeed");

        let pull_only = combine_pull_and_flush(Err(anyhow::anyhow!("peer stalled")), Ok(()))
            .expect_err("a failed pull must fail");
        assert!(format!("{pull_only:#}").contains("peer stalled"));

        let flush_only = combine_pull_and_flush(Ok(progress), Err(anyhow::anyhow!("disk full")))
            .expect_err("a failed flush must fail");
        assert!(format!("{flush_only:#}").contains("disk full"));

        // The load-bearing case: both failed.
        let both = combine_pull_and_flush(
            Err(anyhow::anyhow!(
                "received bytes do not match requested hash"
            )),
            Err(anyhow::anyhow!("disk full")),
        )
        .expect_err("both failing must fail");
        let rendered = format!("{both:#}");
        assert!(
            rendered.contains("disk full"),
            "the local flush fault must win: {rendered}"
        );
        assert!(
            rendered.contains("do not match requested hash"),
            "the pull cause must be chained, not dropped: {rendered}"
        );
    }

    /// Only `NotFound` may be read as "no partial". Any other stat error folded to
    /// `0` would truncate a large verified prefix and silently re-pay for it.
    #[test]
    fn existing_partial_len_reports_stat_errors_instead_of_zero() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let absent = dir.path().join("nothing.partial");
        assert_eq!(existing_partial_len(&absent)?, 0, "absent means no partial");

        let present = dir.path().join("some.partial");
        std::fs::write(&present, b"0123456789")?;
        assert_eq!(
            existing_partial_len(&present)?,
            10,
            "present reports length"
        );

        // A regular file used as a directory component: stat fails with something
        // that is NOT NotFound, which must surface rather than read as zero.
        let not_a_dir = present.join("child.partial");
        let err = existing_partial_len(&not_a_dir)
            .expect_err("a non-NotFound stat error must not be folded to 0");
        assert!(
            format!("{err:#}").contains("refusing to treat this"),
            "the error must name the money consequence: {err:#}"
        );
        Ok(())
    }

    #[test]
    fn partial_path_sits_beside_the_output() {
        let p = partial_path(Path::new("/data/out/movie.mkv"));
        assert_eq!(p, Path::new("/data/out/movie.mkv.partial"));
        // A bare filename must stay relative — joining onto a parent of "" would
        // otherwise send it to the filesystem root.
        assert_eq!(
            partial_path(Path::new("blob.bin")),
            Path::new("blob.bin.partial")
        );
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

    #[test]
    fn parse_channel_id_round_trips_and_rejects_garbage() {
        let id = B256::repeat_byte(0x42);
        assert_eq!(parse_channel_id(&id.to_string()).unwrap(), id);
        assert!(parse_channel_id("not-a-hash").is_err());
    }

    /// Fixture on-chain view for [`adopt_decision`] tests (#1481): a channel
    /// that is signable, `Open`, and unexpired — each test below flips exactly
    /// one predicate away from adoptable.
    fn adoptable_view(my_key: Address) -> OnChainChannelView {
        OnChainChannelView {
            channel_id: B256::repeat_byte(0x11),
            client: Address::repeat_byte(0x22),
            provider: Address::repeat_byte(0x33),
            voucher_signer: my_key,
            token: Address::repeat_byte(0x44),
            deposit: U256::from(1_000_000u64),
            expires_at: 2_000_000_000,
            claimed_nonce: U256::from(5u64),
            claimed_bytes: U256::from(500u64),
            claimed_amount: U256::from(50_000u64),
            status: PaymentChannel::Status::Open,
        }
    }

    /// A channel whose `voucherSigner` this process holds, is `Open`, and is
    /// unexpired is adopted — and the built [`BuyerChannelState`] carries the
    /// right identity fields plus a watermark seeded from the on-chain claimed
    /// totals (not zeroed), so a reused channel resumes at the right nonce.
    #[test]
    fn adopt_decision_accepts_signable_open_unexpired_channel() {
        let my_key = Address::repeat_byte(0xAA);
        let view = adoptable_view(my_key);
        let now = 1_000_000_000;

        let AdoptOutcome::Adopt(state) = adopt_decision(&view, my_key, now) else {
            panic!("expected Adopt for a signable/open/unexpired channel");
        };
        assert_eq!(state.channel_id, view.channel_id);
        assert_eq!(state.voucher_signer, my_key);
        assert_eq!(state.funder, view.client);
        assert_eq!(state.provider, view.provider);
        assert_eq!(state.last_nonce, view.claimed_nonce);
        assert_eq!(state.last_bytes_delivered, view.claimed_bytes);
        assert_eq!(state.last_amount, view.claimed_amount);
    }

    /// A `voucherSigner` this process does not hold can never be adopted, no
    /// matter its status/expiry — there is no way to sign a voucher it would
    /// accept.
    #[test]
    fn adopt_decision_rejects_a_signer_we_do_not_hold() {
        let my_key = Address::repeat_byte(0xAA);
        let view = adoptable_view(Address::repeat_byte(0xBB));
        assert_eq!(
            adopt_decision(&view, my_key, 1_000_000_000),
            AdoptOutcome::NotSignable
        );
    }

    /// A `Closed` channel is never adoptable, even if this key could sign for it.
    #[test]
    fn adopt_decision_rejects_closed_channel() {
        let my_key = Address::repeat_byte(0xAA);
        let mut view = adoptable_view(my_key);
        view.status = PaymentChannel::Status::Closed;
        assert_eq!(
            adopt_decision(&view, my_key, 1_000_000_000),
            AdoptOutcome::NotOpen
        );
    }

    /// A channel past its `expiresAt` is never adoptable — the provider can no
    /// longer serve against it (`withdraw`/`closeChannel` disallowed past expiry).
    #[test]
    fn adopt_decision_rejects_expired_channel() {
        let my_key = Address::repeat_byte(0xAA);
        let mut view = adoptable_view(my_key);
        view.expires_at = 100;
        assert_eq!(adopt_decision(&view, my_key, 200), AdoptOutcome::Expired);
    }

    /// The funder-only `topUp` gate (#1481): a local key that IS the channel's
    /// funder gets the refill amount to top up; a delegate holding only the
    /// voucher-signing key is `Exhausted` instead of being handed an amount it
    /// would fail to submit on-chain (`topUp` reverts for a non-funder caller).
    #[test]
    fn top_up_decision_gates_on_funder() {
        assert_eq!(top_up_decision(U256::ZERO, true), TopUpDecision::NotNeeded);
        assert_eq!(top_up_decision(U256::ZERO, false), TopUpDecision::NotNeeded);
        assert_eq!(
            top_up_decision(U256::from(100u64), true),
            TopUpDecision::TopUp(U256::from(100u64))
        );
        assert_eq!(
            top_up_decision(U256::from(100u64), false),
            TopUpDecision::Exhausted
        );
    }
}
