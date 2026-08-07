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
//! `probe_and_rank`, `open_or_reuse`, `drive_fetch`/`DriveFetchDeps`,
//! `temp_in_parent`) are `pub(crate)` so `decdn bundle pull` (#391) reuses the
//! same gap-driven, reactive-top-up fetch core across a bundle's many entries.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_client_pull::buyer_channel::{
    LOW_WATER_DIVISOR, ensure_allowance, open_channel, refill_amount, top_up,
};
use decdn_client_pull::driver::{DriveConfig, drive};
use decdn_client_pull::source::{Funder, SourceFuture};
use decdn_client_pull::{
    BudgetPacer, ChannelContext, ChannelLedger, ClientRangedStore, Cumulative, PeerSource,
    ProgressCallback, PullDeadlines, UpstreamRefused, UpstreamVoucherRejected, VoucherProgress,
    open_progressive_pull, sign_client_binding,
};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_channel::{
    AdvanceOutcome, BuyerChannelState, BuyerChannelStore, DepositOutcome,
};
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::rate::min_payment;
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

    // The two deposit knobs carry the same invariants the daemon resolver
    // (`resolve_blockchain_into`) enforces, and they must be checked HERE too: this
    // path resolves the raw `[blockchain]` table plus the CLI flags without going
    // through that resolver, so without these the client would accept a config file
    // `decdn config validate` rejects, and `--initial-deposit-micro-usdc 0` would
    // surface as an opaque `openChannel` `ZeroAmount` revert instead of a load-time
    // message. Keep the wording in step with `resolve_blockchain_into`.
    let initial_deposit_micro_usdc = args
        .initial_deposit_micro_usdc
        .or_else(|| bc.and_then(|b| b.buyer_initial_deposit_micro_usdc))
        .unwrap_or(decdn_common::config::DEFAULT_BUYER_INITIAL_DEPOSIT_MICRO_USDC);
    anyhow::ensure!(
        initial_deposit_micro_usdc > 0,
        "buyer_initial_deposit_micro_usdc must be > 0 (openChannel reverts ZeroAmount on a \
         zero deposit) — set --initial-deposit-micro-usdc or \
         blockchain.buyer_initial_deposit_micro_usdc"
    );
    let working_deposit_micro_usdc = args
        .working_deposit_micro_usdc
        .or_else(|| bc.and_then(|b| b.buyer_working_deposit_micro_usdc))
        .unwrap_or(decdn_common::config::DEFAULT_BUYER_WORKING_DEPOSIT_MICRO_USDC);
    anyhow::ensure!(
        working_deposit_micro_usdc == 0 || working_deposit_micro_usdc >= initial_deposit_micro_usdc,
        "buyer_working_deposit_micro_usdc must be 0 (disable top-up) or >= \
         buyer_initial_deposit_micro_usdc (the refill target cannot be smaller than the \
         initial open deposit) — got {working_deposit_micro_usdc} vs \
         {initial_deposit_micro_usdc}"
    );
    let initial_deposit = U256::from(initial_deposit_micro_usdc);
    let working_deposit = U256::from(working_deposit_micro_usdc);
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
/// Reconnect an opaque `delivery refused: NotFound` to its likely cause(s).
///
/// Two causes are checked, and BOTH may be attached: the channel cannot cover what
/// the node reserves before serving, and/or the request carried no ADR 005 client
/// binding. They are independent — an unbound fetch on a drained channel is both,
/// and fixing only the one we happened to name first leaves the retry failing
/// identically. An unbound request (no
/// `blockchain.capacity_bond_address`, so nothing to sign) cannot authorize the
/// node to reactively pull a cache-missed blob from its origin, so the node
/// returns a bare `NotFound` — indistinguishable, without this, from a genuinely
/// absent/blacklisted/wrong hash. Scoped to `NotFound` — a size or blacklist
/// refusal is fixed by neither cause — so every other error is returned verbatim.
/// A BOUND fetch is no longer passed through untouched: the deposit cause applies
/// to it too, and is the more common one now that #1519 refuses an underfunded
/// channel before any fill.
fn annotate_unbound_cache_miss(err: anyhow::Error, ctx: &ChannelContext) -> anyhow::Error {
    let Some(refused) = err.downcast_ref::<UpstreamRefused>() else {
        return err;
    };
    if !matches!(refused.error(), StreamError::NotFound) {
        return err;
    }

    // Both causes are checked and BOTH may attach. They are independent — an
    // unbound fetch on a drained channel is both — and naming only the first
    // leaves the user fixing one and retrying into the other.
    let mut causes: Vec<String> = Vec::new();

    // Cause (a): our channel cannot cover what the node reserves before serving
    // (#1516/#1519). The signed refusal carries the node's quoted `rate_per_mb`,
    // and the channel's own deposit and cumulative paid amount are right here, so
    // the comparison uses only OUR channel and data the node already sent us — it
    // does not weaken the deliberate collapse of seven reject reasons onto
    // `NotFound` (which exists so a prober cannot map other clients' balances).
    //
    // The threshold is an ESTIMATE, not a proof: the node's floor is one credit
    // window at ITS configuration, which we cannot read. Estimated with the shipped
    // defaults — 8 MiB (`DEFAULT_CREDIT_WINDOW_BYTES`) over a 4 MiB
    // `DEFAULT_VOUCHER_INTERVAL_MB`, so the window is the binding term. (The
    // *protocol* 1 MiB interval would under-estimate by 8x and stay silent across
    // exactly the headroom band a stock node refuses in.) An operator who raised
    // `credit_window_bytes` has a higher floor than this, so the miss direction is
    // "we stay silent when we could have spoken" — never a fabricated shortfall.
    // Phrased as a possibility, and as a lower bound, for that reason.
    if let Some(quoted_rate) = refused.evidence().map(|resp| resp.body.rate_per_mb) {
        let headroom = ctx.deposit.saturating_sub(ctx.prior_amount);
        let estimate = min_payment(
            decdn_common::config::DEFAULT_CREDIT_WINDOW_BYTES,
            quoted_rate,
        );
        if quoted_rate > 0 && headroom < estimate {
            causes.push(format!(
                "this channel's remaining deposit ({headroom}) is below the ~{estimate} \
                 the node reserves before serving at its quoted rate of {quoted_rate} per \
                 MB — and that estimate is a LOWER bound, since the node may reserve \
                 several times it depending on its configured credit window. The fetch \
                 path already auto-refills below a low-water mark, so reaching this means \
                 the configured working deposit is itself too small: raise \
                 `--working-deposit-micro-usdc` (or \
                 `blockchain.buyer_working_deposit_micro_usdc`) and retry"
            ));
        }
    }

    // Cause (b): no binding, so the node would not reactively pull for us.
    if ctx.client_binding.is_none() {
        causes.push(
            "no client identity binding was sent because \
             blockchain.capacity_bond_address is unset, so the node could not \
             reactively pull this cache-missed blob from its origin; set \
             blockchain.capacity_bond_address to enable reactive pull-through"
                .to_string(),
        );
    }

    match causes.len() {
        0 => err,
        1 => err.context(causes.concat()),
        // Numbered rather than joined with "and": each clause is a full sentence
        // with its own remedy, so a reader needs to see they are separate fixes.
        n => err.context(format!(
            "{n} possible causes: {}",
            causes
                .iter()
                .enumerate()
                .map(|(i, c)| format!("({}) {c}", i + 1))
                .collect::<Vec<_>>()
                .join("; ")
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
    let (node_id, provider, ctx, slash_dom, contract, rpc, self_address) =
        if let Some(raw_channel_id) = &common.channel_id {
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

            (
                node_id,
                provider,
                ctx,
                slash_dom,
                contract,
                rpc,
                self_address,
            )
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

            (
                node_id,
                provider,
                ctx,
                slash_dom,
                contract,
                rpc,
                self_address,
            )
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
    // A node that accepts the connection and never answers is as dead as one that
    // stops mid-stream, so the same budget answers both (#1134). `capped` enforces
    // that the hard cap outlasts them both — `ClientFetchArgs::validate` has already
    // said so in the user's own flags, so this `?` is the belt to those braces.
    let deadlines = PullDeadlines::capped(
        common.stall_timeout(),
        common.stall_timeout(),
        common.hard_cap(),
    )?;

    // Immutable channel fact captured before `ctx` moves into `drive_fetch`
    // (which wraps it behind the shared, interior-mutable handle #1608 A5).
    let channel_id = ctx.channel_id;

    // The shared pull/funding deps the driver core borrows for the whole fetch.
    let deps = DriveFetchDeps {
        endpoint: &endpoint,
        store: &store,
        contract: &contract,
        rpc: &rpc,
        slash_dom: &slash_dom,
        self_address,
        chain: &chain,
        namespace_id,
        max_rate_per_mb: common.max_rate_per_mb,
        max_blob_bytes,
        deadlines,
    };

    // Run the gap-driven driver core (header-probe -> ClientRangedStore ->
    // PeerSource -> drive -> watermark). The `indicatif` bar and its `on_progress`
    // closure stay here — `drive_fetch` reports only through the callback. Clear
    // the bar around the call so it never overwrites the terminal outcome (success
    // line or error), on either path.
    let result = drive_fetch(
        &deps,
        ctx,
        target,
        provider,
        channel_id,
        hash,
        &args.output,
        Some(&on_progress),
        || bar.finish_and_clear(),
    )
    .await;
    // Safety net for the header-probe-failure early-return path inside
    // `drive_fetch` (before `drive()` ever runs, so `finish_progress` above is
    // never invoked on that path). `finish_and_clear` is idempotent, so this is
    // a harmless no-op on the success/drive-error paths where the hook already
    // cleared the bar before `persist_watermark` ran.
    bar.finish_and_clear();
    let total_bytes = result?;

    println!("fetched {total_bytes} bytes -> {}", args.output.display());
    Ok(())
}

/// Shared pull/funding deps [`drive_fetch`] borrows for the lifetime of one
/// fetch. Mirrors the locals `fetch()` and `bundle_pull::PullCtx` already hold so
/// both callers drive the same gap-driven core (P4). Every field is a borrow or a
/// `Copy` scalar; the per-fetch `ctx`, target, and output are passed to
/// `drive_fetch` directly (the `ctx` moves, since it must be wrapped in
/// `Arc<Mutex>`).
pub(crate) struct DriveFetchDeps<'a, P> {
    pub(crate) endpoint: &'a Endpoint,
    pub(crate) store: &'a RedbBuyerChannelStore,
    pub(crate) contract: &'a PaymentChannel::PaymentChannelInstance<P>,
    pub(crate) rpc: &'a P,
    pub(crate) slash_dom: &'a Eip712Domain,
    pub(crate) self_address: Address,
    pub(crate) chain: &'a ResolvedChain,
    pub(crate) namespace_id: [u8; 32],
    pub(crate) max_rate_per_mb: u64,
    pub(crate) max_blob_bytes: u64,
    pub(crate) deadlines: PullDeadlines,
}

/// The gap-driven driver core shared by `decdn fetch` and `bundle pull` (P4):
/// learn `total_bytes` from a throwaway header-only open, open the
/// [`ClientRangedStore`] beside `output`, `drive` only the missing ranges into it
/// (bao-verifying every byte on ingest and promoting `.partial` to `output` on
/// finalize), then persist the resulting voucher watermark. Returns the whole-blob
/// `total_bytes` on success.
///
/// Behavior-preserving extraction of the block `fetch()` ran inline. The
/// `indicatif` bar stays owned by the caller; `drive_fetch` reports progress
/// only through `progress` and clears the bar via the `finish_progress` hook
/// (called right after `drive()` returns, before `persist_watermark`, matching
/// the pre-extraction ordering — `bundle pull` passes a no-op). The cache-miss
/// annotation is applied on both the header-open and the drive failure, so both
/// callers get the same explained error.
///
/// `ctx` moves in — it is wrapped in `Arc<Mutex>` so the source and driver can
/// share it (the source clones it to open each gap's pull; the driver credits a
/// mid-fetch top-up's new deposit through the same handle so the next open sees
/// it).
/// Which ledger reader `drive_fetch` persists, per drive outcome. This is
/// money: the store refuses to regress a watermark (`AdvanceOutcome::Regressed`
/// is warn-only), so a value written here is effectively permanent.
///
/// - `Ok` → `committed`, what the pull reported was acked.
/// - Explicit `UpstreamVoucherRejected` → `committed`. The node refused a
///   voucher and tore the stream down, applying vouchers in strict nonce
///   order, so anything pipelined behind the rejected one was provably never
///   taken. Settling at `settlement` would still count those followers and
///   could inflate the persisted amount past what the node actually holds.
/// - Any other `Err` → `settlement`, the high reader. The upstream persists a
///   voucher before acking, so an ambiguous failure (stall, IO error, …)
///   probably holds the armed ones; settling low risks re-signing a spent
///   nonce and wedging the channel (#1122).
fn select_watermark(
    outcome: &anyhow::Result<()>,
    committed: Cumulative,
    settlement: Cumulative,
    prior_nonce: U256,
) -> VoucherProgress {
    match outcome {
        Ok(()) => VoucherProgress::from_cumulative(committed, prior_nonce),
        Err(err) if err.downcast_ref::<UpstreamVoucherRejected>().is_some() => {
            VoucherProgress::from_cumulative(committed, prior_nonce)
        }
        Err(_) => VoucherProgress::from_cumulative(settlement, prior_nonce),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drive_fetch<P>(
    deps: &DriveFetchDeps<'_, P>,
    ctx: ChannelContext,
    target: EndpointAddr,
    provider: Address,
    channel_id: B256,
    hash: [u8; 32],
    output: &Path,
    progress: Option<&ProgressCallback>,
    finish_progress: impl FnOnce(),
) -> anyhow::Result<u64>
where
    P: alloy::providers::Provider + Clone,
{
    // Immutable channel facts captured before `ctx` moves behind the shared,
    // interior-mutable handle the driver and source both read/write (#1608 A5).
    let prior_nonce = ctx.prior_nonce;
    let token = ctx.token;

    // One ledger for this channel, seeded from its persisted cumulative state so
    // the first voucher continues at `prior_nonce + 1` (the node rejects a
    // restart-from-zero as a stale nonce). Shared (`Arc`) with the driver and
    // source; the watermark to persist afterwards is read straight back off it.
    let ledger = Arc::new(ChannelLedger::new(Cumulative {
        nonce: prior_nonce,
        bytes: ctx.prior_bytes_delivered,
        amount: ctx.prior_amount,
    }));

    // Learn the whole-blob size before constructing the ranged store: the store is
    // keyed on `(root, total_bytes)`, and the signed `StreamResponse` header is the
    // authoritative source of `total_bytes`. This throwaway open is a handshake
    // only — no voucher is signed until the first paid interval, so it pays nothing
    // — and its pull is dropped immediately; `drive` re-opens exactly the gaps it
    // needs. The cache-miss annotation is applied here too, so an unbound or
    // underfunded refusal is still explained at this first contact.
    let (header, first_pull) = open_progressive_pull(
        deps.endpoint,
        target.clone(),
        &ctx,
        Arc::clone(&ledger),
        deps.slash_dom,
        provider,
        hash,
        deps.namespace_id,
        0,
        micros_now(),
        deps.max_blob_bytes,
        deps.max_rate_per_mb,
        deps.deadlines,
        0,
    )
    .await
    .map_err(|err| annotate_unbound_cache_miss(err, &ctx))?;
    let total_bytes = header.total_bytes;
    drop(first_pull);

    // The store sits beside `output`, keyed by the output's own file name, so its
    // promoted final path IS `output` (no post-finalize rename) and its `.partial`
    // matches the pre-#1608 `<output>.partial` placement. A prior `.partial.ranges`
    // record resumes; only `missing_ranges(R)` is re-pulled and no already-held
    // byte is re-paid.
    let (store_dir, stem) = ranged_store_location(output)?;
    let ranged_store = ClientRangedStore::open_or_create(&store_dir, &stem, hash, total_bytes)
        .map_err(|e| anyhow::anyhow!("open ranged store for {}: {e}", output.display()))?;

    // `topUp` is funder-only on-chain, so a delegate voucher-signer (publisher-pays,
    // #1481) cannot reactively top up — decided once here, up front.
    let is_funder = deps
        .store
        .get_by_provider(provider)?
        .map(|state| state.funder)
        .is_some_and(|funder| funder == deps.self_address);
    let funder = CliFunder {
        contract: deps.contract,
        rpc: deps.rpc,
        store: deps.store,
        provider,
        token,
        self_address: deps.self_address,
        payment_channel_addr: deps.chain.payment_channel,
        max_approve: deps.chain.max_approve,
        is_funder,
    };

    // Share the context behind interior mutability: the source clones it to open
    // each gap's pull, and the driver credits a mid-fetch top-up's new deposit
    // through the same handle so the next open sees it.
    let ctx = Arc::new(Mutex::new(ctx));
    let peer_source = PeerSource::new(
        deps.endpoint,
        target,
        Arc::clone(&ctx),
        Arc::clone(&ledger),
        deps.slash_dom,
        provider,
        deps.namespace_id,
        deps.max_blob_bytes,
        deps.max_rate_per_mb,
        deps.deadlines,
    );
    let pacer = BudgetPacer::new();
    let drive_config = DriveConfig::cli(deps.chain.working_deposit);

    // The gap-driven fetch: `drive` pulls ONLY `missing_ranges(0, 0)` — the whole
    // blob on a fresh fetch, just the gap on a resume — into the ranged store,
    // which bao-verifies every byte on `ingest_stream` and promotes the `.partial`
    // to `output` on `finalize`.
    let drive_result = drive(
        &ranged_store,
        &peer_source,
        &pacer,
        &funder,
        &ctx,
        &ledger,
        hash,
        0,
        0,
        &drive_config,
        progress,
        None, // pacing_wait: BudgetPacer never returns PaceDecision::Wait
    )
    .await;

    // Clear the progress bar (or run the caller's no-op, for `bundle pull`)
    // now — matching the pre-extraction `fetch()`, which cleared its `indicatif`
    // bar immediately after `drive(...).await` returned and BEFORE
    // `persist_watermark`. `persist_watermark` can `eprintln!` a rare
    // non-advance warning; that warning must never race the still-active bar.
    finish_progress();

    // Persist the voucher watermark from the shared ledger the same way the
    // pre-#1608 loop's `watermark_after` chose it: on an explicit voucher
    // rejection the acked (committed) watermark is safe; on an ambiguous failure
    // settle HIGH (`settlement`) so a reuse never re-signs a spent nonce.
    let vprogress = select_watermark(
        &drive_result,
        ledger.committed(),
        ledger.settlement(),
        prior_nonce,
    );
    persist_watermark(deps.store, provider, channel_id, &vprogress);

    // On error the `.partial` + sidecars are deliberately LEFT in place — they are
    // what the next invocation resumes from (only the still-missing gap is
    // re-pulled, and no held byte is re-paid). The whole-file re-hash the old
    // streaming path ran (`verify_resumed_prefix`) is GONE: the store bao-verifies
    // every ingested byte and `finalize` runs a whole-blob `valid_ranges` sweep, so
    // an inherited prefix is verified structurally, not by a second full re-hash.
    drive_result.map_err(|err| match ctx.lock() {
        Ok(guard) => annotate_unbound_cache_miss(err, &guard),
        Err(_) => err,
    })?;

    Ok(total_bytes)
}

/// The CLI's [`Funder`] over its funding chain (#1608 A5): a mid-fetch reactive
/// top-up runs the same `top_up_decision -> ensure_allowance -> top_up` path the
/// pre-#1608 `fetch_blob_streaming` loop ran inline, now behind the driver's
/// injected [`Funder`] seam so the gap driver stays chain-handle-agnostic. The
/// driver decides WHETHER to fund (its pacer confirms a genuine, ledger-
/// corroborated exhaustion and that budget/attempts remain); this only executes
/// the on-chain move and returns the [`DepositOutcome`] for the driver to credit.
struct CliFunder<'a, P> {
    contract: &'a PaymentChannel::PaymentChannelInstance<P>,
    rpc: &'a P,
    store: &'a RedbBuyerChannelStore,
    provider: Address,
    token: Address,
    self_address: Address,
    payment_channel_addr: Address,
    max_approve: bool,
    /// Whether `self_address` is the channel's on-chain funder. `topUp` is
    /// funder-only (`PaymentChannel.sol`), so a delegate voucher-signer
    /// (publisher-pays, #1481) yields [`TopUpDecision::Exhausted`] and top-up
    /// fails fast with an actionable message rather than an opaque revert.
    is_funder: bool,
}

impl<P> Funder for CliFunder<'_, P>
where
    P: alloy::providers::Provider + Clone,
{
    fn max_topups(&self) -> u32 {
        decdn_client_pull::MAX_TOPUP_ATTEMPTS
    }

    fn top_up(&self, additional: U256) -> SourceFuture<'_, DepositOutcome> {
        Box::pin(async move {
            match top_up_decision(additional, self.is_funder) {
                TopUpDecision::TopUp(additional) => {
                    // `topUp` pulls `additional` USDC via `transferFrom`, so the
                    // standing allowance must cover it first: unlimited under
                    // `--max-approve`, else exactly `additional`.
                    ensure_allowance(
                        self.rpc,
                        self.token,
                        self.self_address,
                        self.payment_channel_addr,
                        if self.max_approve {
                            None
                        } else {
                            Some(additional)
                        },
                    )
                    .await?;
                    // The escrowed-but-untracked outcomes (`UnknownChannel` /
                    // `ChannelMismatch`) come straight back for the driver to treat
                    // as terminal — it will not credit a deposit it cannot track.
                    top_up(self.contract, self.store, self.provider, additional).await
                }
                TopUpDecision::Exhausted => anyhow::bail!(
                    "channel exhausted mid-fetch: this key ({}) only signs vouchers and is not \
                     the channel's funder; it cannot top up — ask the funder to raise the deposit",
                    self.self_address
                ),
                // The pacer only asks for a strictly-positive top-up, so a zero
                // `additional` (`NotNeeded`) is unreachable; reject it defensively.
                TopUpDecision::NotNeeded => anyhow::bail!(
                    "reactive top-up requested with nothing to add (deposit already at the working \
                     target)"
                ),
            }
        })
    }
}

/// Where the [`ClientRangedStore`] for `--output` lives: its directory (the
/// output's parent, or the current dir) and its stem (the output's own file
/// name). Keying the store by the output name makes its promoted final path IS
/// `--output` (no post-finalize rename), and its `.partial` sits beside the
/// destination exactly like the pre-#1608 `<output>.partial`, so promotion is a
/// same-filesystem atomic rename.
fn ranged_store_location(output: &Path) -> anyhow::Result<(PathBuf, String)> {
    let dir = match output.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let stem = output
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("--output {} has no usable file name", output.display()))?
        .to_string();
    Ok((dir, stem))
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
/// replaced (opening a fresh one), since `topUp` cannot extend expiry. Before the
/// replacement opens, the expired channel's wind-down is kicked off best-effort
/// (#1553): an expired-and-open channel reclaims its residual deposit in a single
/// `reclaimExpired`, no dispute window, so the USDC returns without the operator
/// remembering anything. That step never *fails* the fetch — its errors are
/// swallowed — but it is awaited (a `getChannel` read plus, on the common path, a
/// `reclaimExpired` tx and receipt wait), so it can add some latency before the
/// replacement opens. A channel in any other on-chain state is only reported, and
/// `decdn channel clean` stays the backstop that finalizes it (its expired row
/// survives in the store, keyed by channel id, even after the provider index
/// re-points to the replacement).
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
            "tracked buyer channel {} (provider {provider}) expired; opening a replacement",
            state.channel_id
        );
        // `topUp` cannot extend expiry, so the expired channel is replaced below.
        // Kick off its wind-down first (#1553): an expired-and-open channel
        // reclaims its residual deposit in one `reclaimExpired` (no dispute
        // window), so the USDC returns without the operator remembering to run
        // `decdn channel clean`. Best-effort — a failure never aborts the fetch,
        // though the call is awaited (a read, and usually a reclaim tx + receipt
        // wait) so it can add latency; `channel clean` remains the backstop for
        // anything it can't finalize in one shot.
        super::channel::reclaim_replaced_expired(
            store,
            contract,
            signer.as_ref(),
            voucher_domain,
            &state,
            unix_now(),
        )
        .await;
    }

    // Authoritative USDC token for the channel, from the contract itself.
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentChannel.usdc(): {e}"))?;
    // Escrowed as configured — there is no on-chain floor to clamp up to,
    // only a non-zero requirement (`openChannel` reverts `ZeroAmount`).
    let deposit = initial_deposit;
    // `max_approve` opts into an unlimited standing allowance; otherwise approve
    // exactly the deposit being escrowed. Unconditional either way — the
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

    /// `resolve_chain` must enforce the same two deposit invariants the daemon
    /// resolver does (#1497 review). It reads the raw `[blockchain]` table plus the
    /// CLI flags rather than going through `resolve_blockchain_into`, so without
    /// its own checks `decdn fetch` would accept a config file that
    /// `decdn config validate` rejects — a validator that does not validate what
    /// actually runs — and `--initial-deposit-micro-usdc 0` would reach the chain
    /// and surface as an opaque `openChannel` `ZeroAmount` revert.
    #[test]
    fn resolve_chain_rejects_deposits_the_daemon_resolver_would_reject() {
        let base = "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n";

        // A zero initial deposit can never open a channel.
        let err = resolve_chain(
            &common(),
            &config(&format!("{base}buyer_initial_deposit_micro_usdc = 0\n")),
        )
        .expect_err("a zero initial deposit must be refused at resolve time");
        assert!(
            err.to_string().contains("buyer_initial_deposit_micro_usdc"),
            "the error must name the offending field; got: {err}"
        );

        // A nonzero working target below the initial open size would make the very
        // first refill shrink the channel.
        let err = resolve_chain(
            &common(),
            &config(&format!(
                "{base}buyer_initial_deposit_micro_usdc = 10000000\nbuyer_working_deposit_micro_usdc = 1000000\n"
            )),
        )
        .expect_err("a working target below the initial deposit must be refused");
        assert!(
            err.to_string().contains("buyer_working_deposit_micro_usdc"),
            "the error must name the offending field; got: {err}"
        );

        // `0` is the explicit disable sentinel, not a too-small target: it must
        // still resolve however large the initial deposit is.
        let r = resolve_chain(
            &common(),
            &config(&format!(
                "{base}buyer_initial_deposit_micro_usdc = 10000000\nbuyer_working_deposit_micro_usdc = 0\n"
            )),
        )
        .expect("0 disables top-up and must remain valid");
        assert_eq!(r.working_deposit, U256::ZERO);
    }

    /// The flags are the surface these invariants are easiest to violate from, and
    /// they bypass the config file entirely — so they need their own coverage.
    #[test]
    fn resolve_chain_validates_the_cli_flags_too() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_channel_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
        );
        let mut args = common();
        args.initial_deposit_micro_usdc = Some(0);
        let err = resolve_chain(&args, &file)
            .expect_err("--initial-deposit-micro-usdc 0 must be refused before the RPC");
        assert!(
            err.to_string().contains("buyer_initial_deposit_micro_usdc"),
            "got: {err}"
        );

        let mut args = common();
        args.initial_deposit_micro_usdc = Some(5_000_000);
        args.working_deposit_micro_usdc = Some(1_000_000);
        let err = resolve_chain(&args, &file)
            .expect_err("a flag-set working target below the initial deposit must be refused");
        assert!(
            err.to_string().contains("buyer_working_deposit_micro_usdc"),
            "got: {err}"
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
    fn ranged_store_location_sits_beside_the_output() {
        // The store's stem is the output's file name and its dir is the output's
        // parent, so its promoted final path (`dir/stem`) IS `--output` and its
        // `.partial` (`dir/{stem}.partial`) sits beside the destination — the
        // pre-#1608 `<output>.partial` placement, without a post-finalize rename.
        let (dir, stem) = ranged_store_location(Path::new("/data/out/movie.mkv")).unwrap();
        assert_eq!(dir, Path::new("/data/out"));
        assert_eq!(stem, "movie.mkv");
        assert_eq!(dir.join(&stem), Path::new("/data/out/movie.mkv"));
        assert_eq!(
            dir.join(format!("{stem}.partial")),
            Path::new("/data/out/movie.mkv.partial")
        );

        // A bare filename must stay relative — the dir falls back to "." rather
        // than joining onto a parent of "" (which would send it to the root).
        let (dir, stem) = ranged_store_location(Path::new("blob.bin")).unwrap();
        assert_eq!(dir, Path::new("."));
        assert_eq!(stem, "blob.bin");
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

    /// `select_watermark` is the money-critical 3-way choice `drive_fetch`
    /// persists after `drive(...)` returns: which ledger reader to trust, per
    /// outcome. Build two distinct `Cumulative` values standing in for
    /// `committed` and `settlement` and assert each outcome picks the intended
    /// one — this is the coverage the deleted `watermark_after` test carried
    /// for the pre-#1608 loop.
    #[test]
    fn select_watermark_picks_the_reader_that_matches_the_outcome() {
        use decdn_protocol::client::VoucherRejectReason;

        let committed = Cumulative {
            nonce: U256::from(3u64),
            bytes: U256::from(3_000u64),
            amount: U256::from(30u64),
        };
        let settlement = Cumulative {
            nonce: U256::from(5u64),
            bytes: U256::from(5_000u64),
            amount: U256::from(50u64),
        };
        let prior_nonce = U256::ZERO;

        // Success — persist the acked (committed) watermark.
        let ok: anyhow::Result<()> = Ok(());
        let got = select_watermark(&ok, committed, settlement, prior_nonce);
        assert_eq!(
            got.acked().map(|(n, _, _)| n),
            Some(committed.nonce),
            "success must persist the committed watermark"
        );

        // Explicit voucher rejection — still committed: vouchers pipelined
        // behind the rejected one were provably never taken.
        let rejected: anyhow::Result<()> = Err(anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::StaleNonce,
            bundle: None,
        }));
        let got = select_watermark(&rejected, committed, settlement, prior_nonce);
        assert_eq!(
            got.acked().map(|(n, _, _)| n),
            Some(committed.nonce),
            "an explicit rejection must persist the committed watermark"
        );

        // Ambiguous failure — settle HIGH so a reuse never re-signs a spent nonce.
        let ambiguous: anyhow::Result<()> = Err(anyhow::anyhow!("peer stalled"));
        let got = select_watermark(&ambiguous, committed, settlement, prior_nonce);
        assert_eq!(
            got.acked().map(|(n, _, _)| n),
            Some(settlement.nonce),
            "an ambiguous failure must persist the settlement watermark"
        );
    }
}
