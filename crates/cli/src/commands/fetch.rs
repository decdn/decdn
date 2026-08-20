//! `decdn fetch` — standalone client-side paid pull of a single
//! content-addressed blob over `cdn/client/v1` (issues #391, #940).
//!
//! Turnkey paying sibling of [`super::probe`]: dial a node by explicit
//! `--node-id`/`--addr`/`--relay-url` (or auto-discover one, #936),
//! **auto-open-or-reuse** the caller's own `PaymentPool` deposit, run one
//! delivery exchange via [`decdn_client_pull::stream_fetch_tracked`]-shaped
//! gap-driven core (signing cumulative vouchers, resuming the pool lane's
//! persisted watermark), verify the `slash_sig` recovers to the provider
//! (ADR 014 §1), BLAKE3-check the whole blob, persist the new watermark, and
//! write the bytes atomically.
//!
//! Pool lifecycle: the caller's live pool in the persistent
//! [`RedbBuyerPoolStore`] is reused (the `(signer, provider)` lane watermark is
//! resumed) — and auto-refilled on-chain via `topUp` when its remaining
//! deposit has run low, so a sustained series of fetches isn't stranded;
//! otherwise one is opened on-chain (USDC `approve` if needed → `openPool`) via
//! the shared [`decdn_client_pull::buyer_pool::open_pool`] kernel and recorded.
//! One pool fans out to every provider the caller pays (ADR 003) — there is no
//! per-provider open. The chain coordinates resolve flag >
//! `[blockchain]`/`[identity]` config > default.
//!
//! The chain/discovery/delivery seams (`resolve_chain`, `resolve_target_node`,
//! `probe_and_rank`, `open_or_reuse_pool`, `drive_fetch`/`DriveFetchDeps`,
//! `temp_in_parent`) are `pub(crate)` so `decdn bundle pull` (#391) reuses the
//! same gap-driven, reactive-top-up fetch core across a bundle's many entries.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_client_pull::buyer_pool::{
    LOW_WATER_DIVISOR, ensure_allowance, issue_self_capability, open_pool, refill_amount, top_up,
};
use decdn_client_pull::driver::{DriveConfig, drive};
use decdn_client_pull::source::{Funder, SourceFuture};
use decdn_client_pull::{
    BlobTooLargeClaim, BudgetPacer, ClientRangedStore, Cumulative, PeerSource, PoolContext,
    ProgressCallback, PullDeadlines, UpstreamRefused, UpstreamVoucherRejected, VoucherProgress,
    open_progressive_pull, sign_client_binding,
};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_pool::{AdvanceOutcome, BuyerPoolState, BuyerPoolStore, DepositOutcome};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::rate::min_payment;
use decdn_incentive::{
    CapabilityGrant, LaneKey, PoolId, bind_node_id_domain, slash_judge_domain, voucher_domain,
};
use decdn_protocol::client::StreamError;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};

use decdn_client_pull::discovery::{self, NodeCandidate};
use decdn_client_pull::endpoint as client_endpoint;
use decdn_client_pull::probe::probe_once;
use decdn_client_pull::provider;

/// Per-candidate probe timeout during auto-discovery (#936). The K probes run
/// concurrently, so this bounds selection latency rather than the overall fetch
/// (`--timeout-ms`); a dead candidate falls out of selection after this.
const SELECT_PROBE_TIMEOUT_MS: u64 = 5_000;

/// Expiry stamped on the self-owned capability [`open_or_reuse_pool`] signs.
/// The CLI fetcher owns its own pool (owner == signer), so there is no
/// delegation to time-box — `u64::MAX` means "never expires", and the pool's
/// own grace-window close is the only lifecycle gate (there is no pool
/// expiry, ADR 003). Mirrors `decdn_client_pull::buyer_pool`'s own (private)
/// `SELF_CAPABILITY_EXPIRY`.
const SELF_CAPABILITY_EXPIRY: u64 = u64::MAX;

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
    pub(crate) payment_pool: Address,
    pub(crate) slash_judge: Address,
    /// `CapacityBond` registry for auto-discovery (no `--node-id`). `None` when
    /// neither the flag nor `blockchain.capacity_bond_address` is set — only an
    /// error on the discovery path, never on the explicit-node path.
    pub(crate) capacity_bond: Option<Address>,
    pub(crate) chain_id: u64,
    pub(crate) keystore: PathBuf,
    /// Directory holding the buyer-pool redb store and (by default) the
    /// keystore. Client-scoped (`~/.decdn/client`) unless an explicit
    /// `--data-dir`/`identity.data_dir` is given.
    pub(crate) data_dir: PathBuf,
    /// Client region for region-first discovery ordering (`--region` >
    /// `identity.region`). `None` skips the ordering.
    pub(crate) region: Option<String>,
    /// Deposit to escrow when OPENING a pool, and the target a reused pool's
    /// proactive refill restores toward once it has served verified bytes.
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

    let pp_raw = args
        .payment_pool_address
        .clone()
        .or_else(|| bc.and_then(|b| b.payment_pool_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "payment_pool_address not set (--payment-pool-address or \
                 blockchain.payment_pool_address)"
            )
        })?;
    let payment_pool = super::chain_ctx::parse_nonzero_address(&pp_raw, "payment_pool_address")?;

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
    let slash_judge = super::chain_ctx::parse_nonzero_address(&sj_raw, "slash_judge_address")?;

    // Optional: only the auto-discovery path reads it, and it errors there if
    // unset rather than failing every explicit-node fetch.
    let capacity_bond = args
        .capacity_bond_address
        .clone()
        .or_else(|| bc.and_then(|b| b.capacity_bond_address.clone()))
        .map(|raw| super::chain_ctx::parse_nonzero_address(&raw, "capacity_bond_address"))
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

    // The deposit knob carries the same invariant the daemon resolver
    // (`resolve_blockchain_into`) enforces, and it must be checked HERE too: this
    // path resolves the raw `[blockchain]` table plus the CLI flags without going
    // through that resolver, so without this the client would accept a config file
    // `decdn config validate` rejects, and `--working-deposit-micro-usdc 0` would
    // surface as an opaque `openPool` `ZeroAmount` revert instead of a load-time
    // message. Keep the wording in step with `resolve_blockchain_into`.
    let working_deposit_micro_usdc = args
        .working_deposit_micro_usdc
        .or_else(|| bc.and_then(|b| b.buyer_working_deposit_micro_usdc))
        .unwrap_or(decdn_common::config::DEFAULT_BUYER_WORKING_DEPOSIT_MICRO_USDC);
    anyhow::ensure!(
        working_deposit_micro_usdc > 0,
        "buyer_working_deposit_micro_usdc must be > 0 (openPool reverts ZeroAmount on a \
         zero deposit) — set --working-deposit-micro-usdc or \
         blockchain.buyer_working_deposit_micro_usdc"
    );
    let working_deposit = U256::from(working_deposit_micro_usdc);
    // Client default: exact (deposit-sized) USDC approval, not an unlimited
    // standing allowance. `buyer_max_approve = true` opts a power user back into
    // the node/operator posture. (The daemon's own default stays unlimited.)
    let max_approve = bc.and_then(|b| b.buyer_max_approve).unwrap_or(false);

    Ok(ResolvedChain {
        rpc_url,
        payment_pool,
        slash_judge,
        capacity_bond,
        chain_id,
        keystore,
        data_dir,
        region,
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

/// Probe `candidates` for `hash` over `endpoint` and return the ordered
/// provider-failover list (#1174, ADR 037 § Fallback): the sequence `fetch`
/// tries in turn, each entry a fallback for the one before it, until one
/// delivers the blob. `slash_sig`/correlation are NOT validated here — selection
/// only needs `has_blob` + RTT; the chosen node's delivery is fully verified
/// downstream. Errors if none of the probed candidates hold it.
///
/// Every candidate is probed on equal footing: opening cost is
/// provider-independent, because the caller has ONE pool that fans out to
/// every provider (ADR 003), so there is no per-provider "already funded"
/// distinction to prefer.
///
/// The order is proxy-warming candidates first (nearest RTT first, ADR 037 §
/// Client selection policy) when warming is enabled and engages, then the
/// holders nearest RTT first. A caller that walks it therefore gets ADR 037's
/// exact fallback shape: the chosen proxy, then the next candidate, and finally
/// the direct holder — routing around a proxy that declines or stalls without
/// ever surfacing an error while a holder remains.
pub(crate) async fn probe_and_order(
    endpoint: &Endpoint,
    candidates: &[NodeCandidate],
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
    warming: ProxyWarmingParams,
) -> anyhow::Result<Vec<NodeCandidate>> {
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
            holders.push(discovery::Probed {
                candidate: cand.clone(),
                rtt_ms,
                // No per-provider funding distinction in the pool model — see
                // the doc comment above.
                has_live_channel: false,
            });
        } else if warming.enabled {
            warming_pool.push(discovery::WarmingCandidate {
                node_id: cand.node_id,
                eth_address: cand.eth_address,
                rtt_ms,
            });
        }
    }

    if holders.is_empty() {
        anyhow::bail!("none of the {probe_count} probed node(s) hold the requested blob");
    }

    let ordered = failover_order(holders, &warming_pool, warming);
    if let Some((node_id, proxy_rtt, best_holder_rtt)) = ordered.warming_lead {
        eprintln!(
            "proxy-warming: routing through nearer non-holder {node_id} ({proxy_rtt:.1}ms) \
             instead of the best holder ({best_holder_rtt:.1}ms) to seed a regional copy, \
             falling back to the holder if it declines (ADR 037)",
        );
    }
    Ok(ordered.order)
}

/// The ordered provider-failover list plus, when a proxy leads it, that proxy's
/// identity for the operator log line. Split from [`probe_and_order`] as a pure
/// function so the ordering is unit-tested without live probing.
struct FailoverOrder {
    /// The candidates to try in turn: proxy-warming non-holders first (nearest
    /// RTT first) when warming engages, then the holders nearest RTT first.
    order: Vec<NodeCandidate>,
    /// `Some((proxy_node_id, proxy_rtt_ms, best_holder_rtt_ms))` when a warming
    /// proxy is prepended; `None` when the list is just the holders.
    warming_lead: Option<(PublicKey, f64, f64)>,
}

/// Assemble the failover order (#1174, ADR 037 § Client selection policy) from
/// the probed `holders` and the `warming_pool` of probed non-holders.
///
/// The holders form the backbone, nearest RTT first — and, absent proxy warming,
/// the whole list. When warming is enabled and the best holder is distant, the
/// non-holders that beat it by the margin are PREPENDED nearest first, so the
/// request routes through the nearest one (it serves via window-paced
/// pull-through and becomes the first regional copy) and falls over through the
/// remaining proxies to the direct holder. RTT-only ranking; never a gamble (an
/// empty proxy order leaves the list as just the holders). `has_live_channel` is
/// uniformly `false` in the pool model, so the RTT sort matches
/// `discovery::rank`'s single pick at its head.
fn failover_order(
    mut holders: Vec<discovery::Probed>,
    warming_pool: &[discovery::WarmingCandidate],
    warming: ProxyWarmingParams,
) -> FailoverOrder {
    holders.sort_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms));
    let best_holder_rtt = holders
        .iter()
        .map(|h| h.rtt_ms)
        .fold(f64::INFINITY, f64::min);
    let proxy_order = if warming.enabled {
        discovery::proxy_warming_order(
            best_holder_rtt,
            warming.rtt_threshold_ms,
            warming.margin_ms,
            warming_pool,
        )
    } else {
        Vec::new()
    };
    let warming_lead = proxy_order
        .first()
        .map(|nearest| (nearest.node_id, nearest.rtt_ms, best_holder_rtt));
    let proxies = proxy_order.iter().map(|proxy| NodeCandidate {
        node_id: proxy.node_id,
        eth_address: proxy.eth_address,
        // Region deliberately dropped rather than carried over: ADR 037
        // §"Ranking key is measured RTT only" forbids region from influencing
        // warming, and `region_hint` is only ever read by `select_candidates`'
        // pre-probe shortlist and operator logging. Leaving it unset keeps a
        // spoofed region from riding along.
        region_hint: None,
    });
    let order = proxies
        .chain(holders.iter().map(|h| h.candidate.clone()))
        .collect();
    FailoverOrder {
        order,
        warming_lead,
    }
}

/// Auto-discover the failover order to fetch `hash` from (#936): read the
/// active set from `CapacityBond`, take the region-nearest
/// [`discovery::SELECT_K`] candidates, and [`probe_and_order`] them. Returns the
/// ordered candidate list `fetch` tries in turn (#1174).
/// Callers must have already unwrapped `chain.capacity_bond` into the
/// "auto-discovery needs `capacity_bond_address`" error, which is why the
/// address is a separate parameter rather than read back off `chain`.
async fn discover_provider(
    endpoint: &Endpoint,
    chain: &ResolvedChain,
    capacity_bond: Address,
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
    // The warming params and the registry deadline are both derived from the
    // same `args`, so they travel as `args` rather than as two more positional
    // parameters (clippy caps this function at 7).
    args: &cli::ClientFetchArgs,
) -> anyhow::Result<Vec<NodeCandidate>> {
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
    probe_and_order(endpoint, &selected, relay_hint, hash, warming).await
}

/// Resolve the ordered failover list of nodes to fetch from (#1174): the
/// explicit `--node-id` (requiring `--provider-address`) as a single-element
/// list, or auto-discovery (#936) when `--node-id` is omitted (deriving each
/// provider from the chosen node's registry entry). The caller tries the entries
/// in turn, failing over on a retryable delivery failure (ADR 037 § Fallback).
pub(crate) async fn resolve_target_node(
    args: &cli::ClientFetchArgs,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
    relays: &[RelayUrl],
    hash: [u8; 32],
) -> anyhow::Result<Vec<NodeCandidate>> {
    if let Some(raw) = &args.node_id {
        // No reachability pre-check: the endpoint is discovery-enabled, so a
        // node-id resolves via `[network.discovery]` / `presets::N0` (plus its
        // default relays) even without `--addr` or configured relays. `clap`
        // guarantees `--provider-address` is present alongside `--node-id`.
        // A pinned node is its own only candidate: there is nothing to fail over
        // to, so the list has one entry and the retry loop runs it once.
        let node_id = PublicKey::from_str(raw)
            .map_err(|e| anyhow::anyhow!("invalid --node-id {raw:?}: {e}"))?;
        let provider_raw = args
            .provider_address
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--provider-address is required with --node-id"))?;
        let provider = super::chain_ctx::parse_address(provider_raw, "--provider-address")?;
        return Ok(vec![NodeCandidate {
            node_id,
            eth_address: provider,
            region_hint: None,
        }]);
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
    // The bound is applied INSIDE `bootstrap_nodes` (around the read alone)
    // rather than wrapped around discovery from out here. Wrapping from out
    // here also cancels the ADR 012 § Bootstrap step 4 cache fallback, so a
    // client holding a usable `peers.json` would be handed a hard failure
    // instead of the degraded-but-working fetch the cache exists to provide.
    let order =
        discover_provider(endpoint, chain, capacity_bond, relays.first(), hash, args).await?;
    if let Some(primary) = order.first() {
        eprintln!(
            "discovered {} candidate node(s); primary {} (provider {}, region {:?})",
            order.len(),
            primary.node_id,
            primary.eth_address,
            primary.region_hint
        );
    }
    Ok(order)
}

/// Reconnect an opaque `delivery refused: NotFound` to its likely cause(s).
///
/// Two causes are checked, and BOTH may be attached: the pool cannot cover what
/// the node reserves before serving, and/or the request carried no ADR 005
/// client binding. They are independent — an unbound fetch on a drained pool is
/// both, and fixing only the one we happened to name first leaves the retry
/// failing identically. An unbound request (no `blockchain.capacity_bond_address`,
/// so nothing to sign) cannot authorize the node to reactively pull a
/// cache-missed blob from its origin, so the node returns a bare `NotFound` —
/// indistinguishable, without this, from a genuinely absent/blacklisted/wrong
/// hash. Scoped to `NotFound` — a size or blacklist refusal is fixed by neither
/// cause — so every other error is returned verbatim.
fn annotate_unbound_cache_miss(err: anyhow::Error, ctx: &PoolContext) -> anyhow::Error {
    let Some(refused) = err.downcast_ref::<UpstreamRefused>() else {
        return err;
    };
    if !matches!(refused.error(), StreamError::NotFound) {
        return err;
    }

    // Both causes are checked and BOTH may attach. They are independent — an
    // unbound fetch on a drained pool is both — and naming only the first
    // leaves the user fixing one and retrying into the other.
    let mut causes: Vec<String> = Vec::new();

    // Cause (a): our pool cannot cover what the node reserves before serving.
    // The signed refusal carries the node's quoted `rate_per_mb`, and the
    // pool's own deposit and cumulative paid amount on this lane are right
    // here, so the comparison uses only OUR pool and data the node already
    // sent us — it does not weaken the deliberate collapse of seven reject
    // reasons onto `NotFound` (which exists so a prober cannot map other
    // clients' balances).
    //
    // The node's pre-flight reservation is one chunk (the ramp floor); the window
    // only widens as this pool pays, so one chunk is the true lower bound on what
    // it reserves before serving. Estimated with the fixed `CHUNK_BYTES`, since we
    // cannot read the node's config — and because `CHUNK_BYTES == BYTES_PER_MB`,
    // that estimate is exactly the quoted per-MB rate. The miss direction is "we
    // stay silent when we could have spoken" — never a fabricated shortfall — so
    // it is phrased as a possibility and as a lower bound.
    if let Some(quoted_rate) = refused.evidence().map(|resp| resp.body.rate_per_mb) {
        let headroom = ctx.deposit.saturating_sub(ctx.prior_amount);
        let estimate = min_payment(decdn_protocol::client::CHUNK_BYTES, quoted_rate);
        if quoted_rate > 0 && headroom < estimate {
            causes.push(format!(
                "this pool's remaining deposit ({headroom}) is below the ~{estimate} the node \
                 reserves before serving at its quoted rate of {quoted_rate} per MB. The fetch \
                 path already auto-refills below a low-water mark, so reaching this means the \
                 configured working deposit is itself too small: raise \
                 `--working-deposit-micro-usdc` (or `blockchain.buyer_working_deposit_micro_usdc`) \
                 and retry"
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

/// Persist what the pool lane paid, warning rather than masking the fetch
/// outcome.
///
/// Shared by the buffered and streaming paths: the bytes were paid for either
/// way, and a failure to record that only risks a rejected reuse next time.
fn persist_watermark(
    store: &RedbBuyerPoolStore,
    owner: Address,
    pool_id: PoolId,
    lane: LaneKey,
    progress: &VoucherProgress,
) {
    let Some((bytes_delivered, amount)) = progress.advanced() else {
        return;
    };
    // A non-`Advanced` outcome (unknown pool / replaced owner slot / regression)
    // means the watermark did NOT move — same hazard as a backend error — so
    // surface it too rather than dropping it on the floor.
    match store.advance_progress(owner, pool_id, lane, bytes_delivered, amount) {
        Ok(AdvanceOutcome::Advanced) => {}
        Ok(other) => eprintln!(
            "warning: voucher watermark not persisted for pool {pool_id} (provider {}): \
             {other:?}; the next reuse may re-sign a stale watermark",
            lane.provider
        ),
        Err(e) => eprintln!(
            "warning: failed to persist voucher watermark for pool {pool_id} (provider {}): {e}",
            lane.provider
        ),
    }
}

/// Fetch a single blob over `cdn/client/v1`, auto-opening/reusing the caller's
/// `PaymentPool` deposit, and write it atomically to `--output`. `config_path`
/// (the global `--config`) supplies relays (#935), discovery (#936), and chain
/// coordinates.
///
/// With `--node-id` the node is dialed explicitly. Without it, `fetch`
/// auto-discovers (#936): read the active set from `CapacityBond`, probe the
/// region-nearest candidates, and derive `--provider-address` from each
/// candidate's registry entry, failing over across them (#1174).
// Linear provider-failover loop over the resolved candidates plus the one-time
// provider-independent setup — long but flat, not complex.
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
    let mut chain = resolve_chain(common, &file)?;

    // Delegated adoption: `--capability`/`--capability-file` names a pool the
    // caller does NOT own and a capability authorizing this client's key to
    // spend against it. `None` => the unchanged self-owned pool path. Disables
    // reactive top-up in `chain` (a delegate cannot fund an owner's pool).
    let grant = resolve_delegation_grant(common, &mut chain)?;

    // The buyer-pool store, opened once and recorded into by open-or-reuse.
    let store = RedbBuyerPoolStore::open(&chain.data_dir)?;

    // One discovery-enabled endpoint, reused for probing and the delivery dial.
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;

    // Resolve the ordered failover list: explicit `--node-id`, or auto-discover.
    let candidates = resolve_target_node(common, &chain, &endpoint, &relays, hash).await?;

    // Buyer signer (vouchers + the openPool/topUp tx). Loaded after selection so a
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
    let contract = PaymentPool::new(chain.payment_pool, rpc.clone());
    let voucher_dom = voucher_domain(chain.chain_id, chain.payment_pool);
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {e}"))?;

    let max_blob_bytes = common.max_blob_mb.saturating_mul(1024 * 1024);
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
    // This same stall budget is the ADR 037 § Fallback progress deadline: a proxy
    // (or holder) that makes no progress within it trips the stall, and the
    // failover loop below routes to the next candidate.
    let deadlines = PullDeadlines::capped(
        common.stall_timeout(),
        common.stall_timeout(),
        common.hard_cap(),
    )?;

    // The shared pull/funding deps the driver core borrows for the whole fetch.
    // Every field is provider-independent, so it is built once and reused across
    // every failover candidate (the provider is passed to `drive_fetch` per-try).
    let deps = DriveFetchDeps {
        endpoint: &endpoint,
        store: &store,
        contract: &contract,
        rpc: &rpc,
        slash_dom: &slash_dom,
        self_address,
        token,
        chain: &chain,
        namespace_id,
        max_rate_per_mb: common.max_rate_per_mb,
        max_blob_bytes,
        deadlines,
    };

    // Provider failover (#1174, ADR 037 § Fallback): try each resolved candidate
    // in turn until one delivers the blob. All candidates draw on the ONE shared
    // pool (ADR 003) — each provider is a distinct lane, and a lane for a
    // not-yet-paid provider opens nothing on-chain — and the `ClientRangedStore`
    // beside `--output` is keyed on `(hash, total_bytes)`, so a fail-over resumes
    // the partial and re-pays nothing already delivered. A retryable failure
    // (a cache-miss `NotFound`, a stall, a transport fault) advances to the next
    // candidate; a terminal one (pool/funder exhausted, blob over the cap) stops
    // immediately; the last error is returned when the list is exhausted.
    let mut last_err: Option<anyhow::Error> = None;
    for (attempt, candidate) in candidates.iter().enumerate() {
        let provider = candidate.eth_address;

        // Delegated (`--capability`): adopt the named pool + owner capability, no
        // open. Self-owned: reuse the caller's live pool (resuming this provider's
        // lane watermark) or open and persist a new one. Both attach the ADR 005
        // client binding. Rebuilt per candidate because the lane is per-provider.
        let ctx = build_ctx_for_fetch(
            grant.as_ref(),
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

        let mut target = EndpointAddr::new(candidate.node_id);
        // `--addr` requires `--node-id` (clap), so it only pins the single
        // explicit-node candidate; a discovered node is reached via its resolved
        // address + relay hint.
        if let Some(addr) = common.addr {
            target = target.with_ip_addr(addr);
        }
        if let Some(url) = relays.first() {
            target = target.with_relay_url(url.clone());
        }

        // Immutable pool fact captured before `ctx` moves into `drive_fetch`.
        let pool_id = ctx.pool_id;

        // A fresh delivery progress bar per attempt (#1118). `indicatif` draws to
        // stderr and hides itself when stderr is not a terminal. On a resumed
        // fail-over it counts only the remaining transfer — the ranged store
        // re-pulls only the missing ranges.
        let (bar, on_progress) = delivery_progress();
        let result = drive_fetch(
            &deps,
            ctx,
            target,
            provider,
            pool_id,
            hash,
            &args.output,
            Some(&on_progress),
            || bar.finish_and_clear(),
        )
        .await;
        // Safety net for the header-probe-failure early-return path inside
        // `drive_fetch` (before `drive()` ever runs). `finish_and_clear` is
        // idempotent, so this is a harmless no-op on paths where the hook already
        // cleared the bar.
        bar.finish_and_clear();

        // On the delegated path `SpendingCapExhausted`, `CapabilityExpired`, and
        // `PoolExhausted` are all terminal — the delegate cannot top up an owner's
        // pool or raise/re-mint its own capability — so reconnect them to the
        // owner-side remedy rather than leaving a bare "voucher rejected: ...".
        let err = match result {
            Ok(bytes) => {
                println!("fetched {bytes} bytes -> {}", args.output.display());
                return Ok(());
            }
            Err(err) if grant.is_some() => annotate_delegated_exhaustion(err),
            Err(err) => err,
        };

        // Stop on a terminal failure, or on a retryable one with nothing left to
        // fail over to (the last error is what the caller sees). Otherwise route
        // to the next candidate.
        let more_candidates = attempt + 1 < candidates.len();
        if retry_disposition(&err) == RetryDisposition::Terminal || !more_candidates {
            return Err(err);
        }
        eprintln!(
            "fetch: provider {provider} could not deliver ({err:#}); failing over to the next of \
             {} candidate(s)",
            candidates.len(),
        );
        last_err = Some(err);
    }

    // The list is empty only if `resolve_target_node` returned no candidates,
    // which it never does (discovery errors on an empty holder set, and the
    // explicit path yields one). `last_err` is therefore set whenever the loop
    // falls through; keep a defensive error for the unreachable empty case.
    Err(last_err
        .unwrap_or_else(|| anyhow::anyhow!("no candidate node could deliver the requested blob")))
}

/// Reconnect a delegated fetch's terminal owner-remedy voucher rejection
/// (`SpendingCapExhausted`, `CapabilityExpired`, `PoolExhausted`) to the
/// owner-side remedy: the delegate holds no wallet on this pool, so it cannot
/// `topUp`, raise its own cap, or mint itself a fresh capability. Any other
/// error passes through verbatim (a stall, a transport fault, or a `NotFound`
/// already annotated by [`annotate_unbound_cache_miss`] inside `drive_fetch`).
pub(crate) fn annotate_delegated_exhaustion(err: anyhow::Error) -> anyhow::Error {
    use decdn_protocol::client::VoucherRejectReason;

    let needs_owner = err
        .downcast_ref::<UpstreamVoucherRejected>()
        .is_some_and(|rejected| {
            matches!(
                rejected.reason,
                VoucherRejectReason::SpendingCapExhausted
                    | VoucherRejectReason::CapabilityExpired
                    | VoucherRejectReason::PoolExhausted
            )
        });
    if needs_owner {
        err.context(
            "capability cap exhausted, capability expired, or pool balance exhausted — ask the \
             pool owner to top up the pool or issue a fresh, higher-cap capability (a delegated \
             client cannot top up a pool it does not own)",
        )
    } else {
        err
    }
}

/// The multi-source engagement gate (ADR 039): whether a fetch should fan out
/// across several holders via [`decdn_client_pull::multi_source_fetch`] rather
/// than the single-source failover loop above.
///
/// All three conditions must hold: the kill switch (`--multi-source`) is on,
/// the blob clears the size floor (fanning out a small blob only adds lane
/// overhead for no parallelism win), and at least two admissible holders
/// exist to fan out across (one holder is exactly the single-source path,
/// just with extra bookkeeping).
///
/// Not yet called from `drive_fetch`/`bundle_pull`: see the Task 7 report
/// (`.superpowers/sdd/2026-08-20-multi-source-parallel-fetch-plan/task-7-report.md`)
/// for why wiring it in is blocked — `multi_source_fetch` takes exactly one
/// shared `ctx`/`ledger`/`funder` for every source in the admitted set, but
/// each admitted source is a DIFFERENT on-chain provider with its OWN
/// `(signer, provider)` payment lane (ADR 039 § Payment model), so a single
/// shared `PoolContext`/`PoolLedger` cannot correctly sign vouchers payable to
/// more than one of them.
#[must_use]
#[allow(dead_code)]
pub(crate) const fn should_multi_source(
    enabled: bool,
    total_bytes: u64,
    min_bytes: u64,
    admissible: usize,
) -> bool {
    enabled && total_bytes > min_bytes && admissible >= 2
}

/// Whether a failed delivery attempt should fall over to the next candidate
/// provider (#1174, ADR 037 § Fallback), or end the fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryDisposition {
    /// The failure is a property of THIS provider or its delivery — not of the
    /// content or the caller's pool — so the next candidate is worth trying.
    RetryElsewhere,
    /// Another provider cannot fix this: the shared pool or funder is refused
    /// everywhere, or the blob is unservable to this client whoever holds it.
    Terminal,
}

/// Classify a [`drive_fetch`] failure for provider failover (#1174, ADR 037 §
/// Fallback): decide whether continuing to the next candidate can succeed.
///
/// The classification follows the retry disposition each [`StreamError`] variant
/// already documents, plus the pool model's global facts:
///
/// - A payment-layer rejection ([`UpstreamVoucherRejected`], or a mid-stream
///   [`StreamError::VoucherRejected`]) is **terminal**. One pool fans out to
///   every provider (ADR 003), so its remaining deposit, its capability cap, and
///   the on-chain delivery floor are the same against any provider, and
///   `drive_fetch` has already exhausted any wallet-less watermark self-heal.
/// - [`StreamError::OriginBlacklisted`] is **terminal** — the pool's funder is
///   refused under this address everywhere.
/// - A [`BlobTooLargeClaim`] is **terminal** — the blob is BLAKE3-addressed, so
///   its size is identical whoever serves it, and it stays over the client's cap.
/// - Every other refusal ([`StreamError::NotFound`], `Overloaded`,
///   `BlobTooLarge`, `InternalError`, `EvictedSinceProbe`, `HashBlacklisted`)
///   and every non-refusal error — a stall (the progress deadline tripped), a
///   transport fault, or a bao/hash verification failure on the bytes this node
///   served — is a property of this provider's delivery, so the fetch **fails
///   over**. When every candidate is exhausted the caller returns the last such
///   error, so a genuinely absent or wrong hash still surfaces its refusal.
pub(crate) fn retry_disposition(err: &anyhow::Error) -> RetryDisposition {
    use RetryDisposition::{RetryElsewhere, Terminal};

    if err.downcast_ref::<UpstreamVoucherRejected>().is_some()
        || err.downcast_ref::<BlobTooLargeClaim>().is_some()
    {
        return Terminal;
    }
    if let Some(refused) = err.downcast_ref::<UpstreamRefused>() {
        return match refused.error() {
            StreamError::OriginBlacklisted | StreamError::VoucherRejected { .. } => Terminal,
            _ => RetryElsewhere,
        };
    }
    RetryElsewhere
}

/// Shared pull/funding deps [`drive_fetch`] borrows for the lifetime of one
/// fetch. Mirrors the locals `fetch()` and `bundle_pull::PullCtx` already hold so
/// both callers drive the same gap-driven core (P4). Every field is a borrow or a
/// `Copy` scalar; the per-fetch `ctx`, target, and output are passed to
/// `drive_fetch` directly (the `ctx` moves, since it must be wrapped in
/// `Arc<Mutex>`).
pub(crate) struct DriveFetchDeps<'a, P> {
    pub(crate) endpoint: &'a Endpoint,
    pub(crate) store: &'a RedbBuyerPoolStore,
    pub(crate) contract: &'a PaymentPool::PaymentPoolInstance<P>,
    pub(crate) rpc: &'a P,
    pub(crate) slash_dom: &'a Eip712Domain,
    pub(crate) self_address: Address,
    /// The pool's settlement token (USDC), read once via `PaymentPool.usdc()`
    /// and threaded through rather than re-read per top-up.
    pub(crate) token: Address,
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
/// The `indicatif` bar stays owned by the caller; `drive_fetch` reports progress
/// only through `progress` and clears the bar via the `finish_progress` hook
/// (called right after `drive()` returns, before `persist_watermark`). The
/// cache-miss annotation is applied on both the header-open and the drive
/// failure, so both callers get the same explained error.
///
/// `ctx` moves in — it is wrapped in `Arc<Mutex>` so the source and driver can
/// share it (the source clones it to open each gap's pull; the driver credits a
/// mid-fetch top-up's new deposit through the same handle so the next open sees
/// it).
fn select_watermark(
    outcome: &anyhow::Result<()>,
    committed: Cumulative,
    settlement: Cumulative,
    prior_amount: U256,
) -> VoucherProgress {
    let cum = match outcome {
        Ok(()) => committed,
        Err(err) if err.downcast_ref::<UpstreamVoucherRejected>().is_some() => committed,
        // Any other `Err` (stall, IO error, …): the node persists a voucher
        // before acking, so an ambiguous failure probably holds the armed one —
        // settle HIGH so a reuse never re-signs a spent lane state.
        Err(_) => settlement,
    };
    VoucherProgress::from_cumulative(cum, prior_amount)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drive_fetch<P>(
    deps: &DriveFetchDeps<'_, P>,
    ctx: PoolContext,
    target: EndpointAddr,
    provider: Address,
    pool_id: PoolId,
    hash: [u8; 32],
    output: &Path,
    progress: Option<&ProgressCallback>,
    finish_progress: impl FnOnce(),
) -> anyhow::Result<u64>
where
    P: alloy::providers::Provider + Clone,
{
    // Immutable lane fact captured before `ctx` moves behind the shared,
    // interior-mutable handle the driver and source both read/write.
    let prior_amount = ctx.prior_amount;
    let lane = LaneKey {
        pool_id,
        signer: deps.self_address,
        provider,
    };

    // One ledger for this lane, seeded from its persisted cumulative state so
    // the first voucher continues at `prior_amount` (a restart-from-zero would
    // be rejected as a regression). Shared (`Arc`) with the driver and source;
    // the watermark to persist afterwards is read straight back off it.
    let ledger = Arc::new(ctx.new_ledger());

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
    // matches the `<output>.partial` placement. A prior `.partial.ranges` record
    // resumes; only `missing_ranges(R)` is re-pulled and no already-held byte is
    // re-paid.
    let (store_dir, stem) = ranged_store_location(output)?;
    let ranged_store = ClientRangedStore::open_or_create(&store_dir, &stem, hash, total_bytes)
        .map_err(|e| anyhow::anyhow!("open ranged store for {}: {e}", output.display()))?;

    let funder = CliFunder {
        contract: deps.contract,
        rpc: deps.rpc,
        store: deps.store,
        owner: deps.self_address,
        pool_id,
        token: deps.token,
        payment_pool_addr: deps.chain.payment_pool,
        max_approve: deps.chain.max_approve,
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
        None, // served_paid: no downstream leg on the client path
    )
    .await;

    // Clear the progress bar (or run the caller's no-op, for `bundle pull`) now —
    // BEFORE `persist_watermark`, which can `eprintln!` a rare non-advance
    // warning that must never race the still-active bar.
    finish_progress();

    // Persist the voucher watermark from the shared ledger: on an explicit
    // voucher rejection the acked (committed) watermark is safe; on an ambiguous
    // failure settle HIGH (`settlement`) so a reuse never re-signs a spent lane
    // state.
    let vprogress = select_watermark(
        &drive_result,
        ledger.committed(),
        ledger.settlement(),
        prior_amount,
    );
    persist_watermark(deps.store, deps.self_address, pool_id, lane, &vprogress);

    // On error the `.partial` + sidecars are deliberately LEFT in place — they are
    // what the next invocation resumes from (only the still-missing gap is
    // re-pulled, and no held byte is re-paid). The store bao-verifies every
    // ingested byte and `finalize` runs a whole-blob `valid_ranges` sweep, so an
    // inherited prefix is verified structurally, not by a second full re-hash.
    drive_result.map_err(|err| match ctx.lock() {
        Ok(guard) => annotate_unbound_cache_miss(err, &guard),
        Err(_) => err,
    })?;

    Ok(total_bytes)
}

/// The CLI's [`Funder`]: a mid-fetch reactive top-up runs the same
/// `ensure_allowance -> top_up -> add_deposit` path `open_or_reuse_pool`'s
/// proactive low-water refill runs, now behind the driver's injected [`Funder`]
/// seam so the gap driver stays chain-handle-agnostic. The driver decides
/// WHETHER to fund (its pacer confirms a genuine, ledger-corroborated
/// exhaustion and that budget/attempts remain); this only executes the
/// on-chain move and returns the [`DepositOutcome`] for the driver to credit.
///
/// There is no funder-vs-delegate split here: the CLI fetcher is always its
/// own pool owner, so `top_up` is unconditionally authorized (`topUp` is
/// owner-only on-chain, and owner == signer on this path).
struct CliFunder<'a, P> {
    contract: &'a PaymentPool::PaymentPoolInstance<P>,
    rpc: &'a P,
    store: &'a RedbBuyerPoolStore,
    owner: Address,
    pool_id: PoolId,
    token: Address,
    payment_pool_addr: Address,
    max_approve: bool,
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
            // `topUp` pulls `additional` USDC via `transferFrom`, so the
            // standing allowance must cover it first: unlimited under
            // `--max-approve`, else exactly `additional`.
            ensure_allowance(
                self.rpc,
                self.token,
                self.owner,
                self.payment_pool_addr,
                if self.max_approve {
                    None
                } else {
                    Some(additional)
                },
            )
            .await?;
            let credited = top_up(self.contract, self.pool_id, additional).await?;
            // The escrowed-but-untracked outcomes (`UnknownPool` / `PoolMismatch`)
            // come straight back for the driver to treat as terminal — it will
            // not credit a deposit it cannot track.
            self.store
                .add_deposit(self.owner, self.pool_id, credited)
                .map_err(|e| anyhow::anyhow!("persist pool top-up: {e}"))
        })
    }
}

/// Where the [`ClientRangedStore`] for `--output` lives: its directory (the
/// output's parent, or the current dir) and its stem (the output's own file
/// name). Keying the store by the output name makes its promoted final path IS
/// `--output` (no post-finalize rename), and its `.partial` sits beside the
/// destination exactly like `<output>.partial`, so promotion is a
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

/// Resolve the delegated `--capability`/`--capability-file` token into a
/// [`CapabilityGrant`], and — when one is present — disable reactive top-up in
/// `chain` (a delegate owns no pool it could `topUp`). `None` selects the
/// unchanged self-owned pool path. Shared by `decdn fetch` and `bundle pull`.
///
/// # Errors
///
/// When `--capability-file` cannot be read, or the token fails to decode.
pub(crate) fn resolve_delegation_grant(
    common: &cli::ClientFetchArgs,
    chain: &mut ResolvedChain,
) -> anyhow::Result<Option<CapabilityGrant>> {
    let grant = common
        .resolve_capability_token()?
        .map(|token| CapabilityGrant::from_token(&token))
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid --capability token: {e}"))?;
    if grant.is_some() {
        chain.working_deposit = U256::ZERO;
    }
    Ok(grant)
}

/// Select the pool context for one fetch: the delegated adoption path when a
/// `--capability` [`CapabilityGrant`] is present (adopt the named pool, present
/// the owner's capability, no open), else the self-owned open-or-reuse path.
/// Extracted so `fetch()` stays one readable pass over its stages.
#[allow(clippy::too_many_arguments)]
async fn build_ctx_for_fetch<P>(
    grant: Option<&CapabilityGrant>,
    store: &RedbBuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    rpc: &P,
    signer: &Arc<PrivateKeySigner>,
    voucher_dom: &Eip712Domain,
    provider: Address,
    self_address: Address,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
) -> anyhow::Result<PoolContext>
where
    P: alloy::providers::Provider + Clone,
{
    if let Some(grant) = grant {
        build_delegated_pool_ctx(
            store,
            contract,
            signer,
            voucher_dom,
            provider,
            self_address,
            chain,
            endpoint,
            grant,
        )
        .await
    } else {
        build_pool_ctx(
            store,
            contract,
            rpc,
            signer,
            voucher_dom,
            provider,
            self_address,
            chain,
            endpoint,
        )
        .await
    }
}

/// [`open_or_reuse_pool`] plus the ADR 005 client identity binding (#1115):
/// sign our OWN iroh `NodeId` with the buyer key so the serving node can prove
/// we own the pool and reactively pull a cache-missed blob from its configured
/// origin.
///
/// The bind domain's verifying contract is the `CapacityBond`; without one
/// configured we can't sign, so the request goes out unbound and the node serves
/// only content it already holds (a cache miss is refused). No warning is
/// emitted for that — it would fire on every successful cached fetch too,
/// training users to ignore it; the refusal is explained at the point of failure
/// by [`annotate_unbound_cache_miss`].
#[allow(clippy::too_many_arguments)]
async fn build_pool_ctx<P>(
    store: &RedbBuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    rpc: &P,
    signer: &Arc<PrivateKeySigner>,
    voucher_dom: &Eip712Domain,
    provider: Address,
    self_address: Address,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
) -> anyhow::Result<PoolContext>
where
    P: alloy::providers::Provider + Clone,
{
    let ctx = open_or_reuse_pool(
        store,
        contract,
        rpc,
        signer,
        voucher_dom,
        provider,
        self_address,
        chain.payment_pool,
        chain.working_deposit,
        chain.max_approve,
    )
    .await?;
    attach_client_binding(ctx, chain, endpoint, signer)
}

/// Reject a delegated fetch whose loaded key is not the signer the capability
/// authorizes. The client can only sign vouchers as `self_address`, and a
/// capability scoped to a different signer would have every voucher rejected as
/// `WrongSigner` — so fail fast, before any network or on-chain work.
fn ensure_delegate_signer(self_address: Address, authorized: Address) -> anyhow::Result<()> {
    anyhow::ensure!(
        self_address == authorized,
        "this capability authorizes signer {authorized}, but the loaded key is {self_address} — \
         load the delegate keystore the pool owner assigned (--keystore / \
         blockchain.eth_keystore), or ask for a capability issued to {self_address}",
    );
    Ok(())
}

/// Build a [`PoolContext`] that adopts a pool the caller does NOT own, from a
/// delegated [`CapabilityGrant`] (`decdn pool assign` → `--capability`).
///
/// Unlike [`build_pool_ctx`] this opens nothing on-chain and signs no
/// self-capability: it presents the OWNER's capability (rebuilt from the token)
/// so the serving node registers this client's signer on its first redemption.
/// It hard-fails when the loaded keystore is not the delegate the capability
/// authorizes — the client cannot sign vouchers under a capability scoped to a
/// different signer, and a node would reject them as `WrongSigner`.
///
/// The pool's on-chain row is read once for the informational `deposit` and to
/// confirm the pool exists (a zero-owner row means it was never opened on this
/// contract). The lane resumes from any locally-tracked watermark for
/// `(pool, this signer, provider)`; a delegate that has never streamed this lane
/// starts at zero. The ADR 005 client binding is attached the same as the
/// self-owned path — it only authorizes reactive origin pull-through when the
/// binding owner also owns the pool, which a delegate does not, so a cache miss
/// still refuses (already-cached content serves and is paid via the capability).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_delegated_pool_ctx<P>(
    store: &RedbBuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    signer: &Arc<PrivateKeySigner>,
    voucher_dom: &Eip712Domain,
    provider: Address,
    self_address: Address,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
    grant: &CapabilityGrant,
) -> anyhow::Result<PoolContext>
where
    P: alloy::providers::Provider + Clone,
{
    ensure_delegate_signer(self_address, grant.signer)?;

    let signed_capability = grant.to_signed_capability().map_err(|e| {
        anyhow::anyhow!("capability token carries a malformed owner signature: {e}")
    })?;

    let pool_id = grant.pool_id;
    let pool = contract
        .getPool(pool_id)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read on-chain pool {pool_id} for the capability: {e}"))?;
    anyhow::ensure!(
        pool.owner != Address::ZERO,
        "pool {pool_id} named by the capability does not exist on this PaymentPool contract \
         ({}) — wrong --payment-pool-address/chain, or a stale token",
        chain.payment_pool,
    );

    // Resume this delegate's lane watermark if a local record exists; a delegate
    // that has never streamed this lane starts at zero (a fresh lane).
    let lane = LaneKey {
        pool_id,
        signer: self_address,
        provider,
    };
    let (prior_bytes, prior_amount) = store
        .get_by_pool_id(pool_id)?
        .and_then(|state| state.lane_progress(lane))
        .map_or((U256::ZERO, U256::ZERO), |p| (p.last_bytes, p.last_amount));

    // A transient state carrying the informational pool facts the context reads
    // (`pool_id`, `deposit`); it is not persisted (the delegate owns no pool row).
    // The pool no longer stores its token — it is the contract's immutable
    // `usdc()`, so read it there rather than duplicating it per pool.
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {e}"))?;
    let state = BuyerPoolState::new(pool_id, pool.owner, token, U256::from(pool.deposit));
    let ctx = PoolContext::for_pool(&state, Arc::clone(signer), voucher_dom.clone())
        .with_provider(provider, prior_bytes, prior_amount)
        .with_capability(signed_capability);
    attach_client_binding(ctx, chain, endpoint, signer)
}

/// Attach the ADR 005 client identity binding to an already-built
/// [`PoolContext`] (#1115): sign our OWN iroh `NodeId` with the buyer key so
/// the serving node can prove we own the pool and reactively pull a
/// cache-missed blob from its configured origin. Separate from
/// [`build_pool_ctx`] so `decdn bundle pull` — which takes the `open_lock`
/// itself around [`open_or_reuse_pool`] — can attach the same binding outside
/// that critical section.
///
/// No binding when `chain.capacity_bond` is unset — see [`build_pool_ctx`]'s
/// docs for why that is silent.
pub(crate) fn attach_client_binding(
    ctx: PoolContext,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
    signer: &Arc<PrivateKeySigner>,
) -> anyhow::Result<PoolContext> {
    let Some(capacity_bond) = chain.capacity_bond else {
        return Ok(ctx);
    };
    let bind_dom = bind_node_id_domain(chain.chain_id, capacity_bond);
    let own_node_id = B256::from(*endpoint.id().as_bytes());
    Ok(ctx.with_client_binding(sign_client_binding(signer, own_node_id, &bind_dom)?))
}

/// Reuse the caller's live pool (resuming `provider`'s lane watermark), or open
/// and persist a new one. A reused pool whose remaining deposit has run low is
/// auto-refilled on-chain via `topUp` before it is returned — see
/// [`refill_amount`] for the policy. There is no pool expiry (ADR 003), so
/// there is no replace-on-expiry branch: the same pool is reused for the
/// caller's whole lifetime, across every provider.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn open_or_reuse_pool<P>(
    store: &RedbBuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    rpc: &P,
    signer: &Arc<PrivateKeySigner>,
    voucher_domain: &Eip712Domain,
    provider: Address,
    self_address: Address,
    payment_pool_addr: Address,
    working_deposit: U256,
    max_approve: bool,
) -> anyhow::Result<PoolContext>
where
    P: alloy::providers::Provider + Clone,
{
    if let Some(state) = store.get_by_owner(self_address)? {
        let lane = LaneKey {
            pool_id: state.pool_id,
            signer: self_address,
            provider,
        };
        let (prior_bytes, prior_amount) = state
            .lane_progress(lane)
            .map_or((U256::ZERO, U256::ZERO), |p| (p.last_bytes, p.last_amount));

        // Auto-refill a live pool whose remaining deposit has run low, so a
        // sustained series of fetches isn't stranded by a spent-down deposit.
        let low_water = working_deposit / U256::from(LOW_WATER_DIVISOR);
        let additional = refill_amount(state.deposit, prior_amount, working_deposit, low_water);
        let state = if additional.is_zero() {
            state
        } else {
            eprintln!(
                "buyer pool {} low on deposit ({} µUSDC remaining of {} deposited); topping up \
                 {additional} µUSDC",
                state.pool_id,
                state.deposit.saturating_sub(prior_amount),
                state.deposit,
            );
            // `topUp` pulls `additional` USDC via `transferFrom`, so the pool's
            // standing allowance must cover it first. Ensure it in the caller's
            // mode: unlimited under `--max-approve`, else exactly `additional`.
            ensure_allowance(
                rpc,
                state.token,
                self_address,
                payment_pool_addr,
                if max_approve { None } else { Some(additional) },
            )
            .await?;
            let credited = top_up(contract, state.pool_id, additional).await?;
            // The escrowed-but-untracked outcomes are logged inside `top_up`; the
            // CLI re-reads the row below and reflects whatever landed.
            match store.add_deposit(self_address, state.pool_id, credited) {
                Ok(DepositOutcome::Added(_)) => {}
                Ok(other) => eprintln!(
                    "warning: pool {} topped up on-chain but the local record was not updated: \
                     {other:?}",
                    state.pool_id
                ),
                Err(e) => eprintln!(
                    "warning: pool {} topped up on-chain but persisting it locally failed: {e}",
                    state.pool_id
                ),
            }
            store.get_by_pool_id(state.pool_id)?.unwrap_or(state)
        };
        // Uncapped: a self-owned capability delegates spend to the owner's own
        // key, so the pool deposit — not the capability cap — is the real
        // spending bound. Capping at `state.deposit` here would freeze the
        // on-chain cap at the pre-top-up deposit (`_registerCapability` is
        // idempotent past first redemption) and reject spend past it.
        let capability = issue_self_capability(
            signer.as_ref(),
            state.pool_id,
            U256::MAX,
            SELF_CAPABILITY_EXPIRY,
            voucher_domain,
        )?;
        return Ok(
            PoolContext::for_pool(&state, Arc::clone(signer), voucher_domain.clone())
                .with_provider(provider, prior_bytes, prior_amount)
                .with_capability(capability),
        );
    }

    // Authoritative USDC token for the pool, from the contract itself.
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {e}"))?;
    // Escrowed as configured — there is no on-chain floor to clamp up to,
    // only a non-zero requirement (`openPool` reverts `ZeroAmount`). The shared
    // pool is fully withdrawable, so the buyer opens at the working deposit
    // directly rather than a smaller first-contact lock.
    let deposit = working_deposit;
    // `max_approve` opts into an unlimited standing allowance; otherwise approve
    // exactly the deposit being escrowed.
    let approve_amount = if max_approve { None } else { Some(deposit) };
    ensure_allowance(rpc, token, self_address, payment_pool_addr, approve_amount).await?;
    let opened = open_pool(
        contract,
        Arc::clone(signer),
        voucher_domain,
        token,
        self_address,
        deposit,
    )
    .await?;
    // The deposit is escrowed on-chain; a failed local record leaves it
    // untracked (reconcile against the tx).
    store.record(&opened.state).map_err(|e| {
        anyhow::anyhow!(
            "buyer pool opened on-chain (tx {}) but persisting it failed; the deposit is \
             escrowed but untracked — reconcile manually: {e}",
            opened.tx
        )
    })?;
    Ok(opened
        .ctx
        .with_provider(provider, U256::ZERO, U256::ZERO)
        .with_capability(opened.capability))
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
            rpc_url: None,
            payment_pool_address: None,
            slash_judge_address: None,
            capacity_bond_address: None,
            region: None,
            proxy_warming: false,
            proxy_warming_rtt_threshold_ms: 150,
            proxy_warming_margin_ms: 30,
            multi_source: false,
            no_multi_source: false,
            max_sources: 4,
            multi_source_min_bytes: 67_108_864,
            unit_deadline_ms: 10_000,
            chain_id: None,
            keystore: None,
            data_dir: Some(PathBuf::from("/tmp/d")),
            working_deposit_micro_usdc: None,
            max_blob_mb: 1024,
            max_rate_per_mb: 0,
            stall_timeout_ms: 30_000,
            timeout_ms: 3_600_000,
            capability: None,
            capability_file: None,
        }
    }

    fn config(body: &str) -> FileConfig {
        toml::from_str(body).expect("parse test config")
    }

    /// The pure multi-source engagement gate (#1760-series follow-on): engage
    /// only when the kill switch is on, the blob clears the size floor, and at
    /// least two admissible holders exist to fan out across.
    #[test]
    fn engagement_gate_requires_enabled_size_and_two_holders() {
        assert!(should_multi_source(true, 100 << 20, 64 << 20, 2));
        assert!(!should_multi_source(false, 100 << 20, 64 << 20, 4)); // kill switch
        assert!(!should_multi_source(true, 10 << 20, 64 << 20, 4)); // below size gate
        assert!(!should_multi_source(true, 100 << 20, 64 << 20, 1)); // one holder
    }

    fn ctx_with(binding: Option<decdn_protocol::client::ClientBinding>) -> PoolContext {
        PoolContext {
            pool_id: B256::ZERO,
            provider: Address::ZERO,
            deposit: U256::ZERO,
            client_signer: Arc::new(PrivateKeySigner::random()),
            voucher_domain: bind_node_id_domain(1, Address::ZERO),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: binding,
            capability: None,
        }
    }

    /// The refusal these tests annotate, built the way the fetch path builds it: the typed
    /// `UpstreamRefused` sentinel (#1144). Never hand-roll one with
    /// `anyhow!("delivery refused: …")` — the annotation downcasts, so a look-alike string
    /// would exercise nothing and pass against a hint that never fires in production.
    fn refusal(error: StreamError) -> anyhow::Error {
        anyhow::Error::new(UpstreamRefused::mid_stream(error))
    }

    /// A payment-layer voucher rejection is terminal for failover: the shared
    /// pool's cap/deposit/floor are global, so the next provider fails the same.
    #[test]
    fn voucher_rejection_is_terminal() {
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
        });
        assert_eq!(super::retry_disposition(&err), RetryDisposition::Terminal);
    }

    /// A refused `OriginBlacklisted` is terminal — the pool's funder is refused
    /// under this address everywhere, so no other provider can serve it.
    #[test]
    fn origin_blacklisted_refusal_is_terminal() {
        let err = refusal(StreamError::OriginBlacklisted);
        assert_eq!(super::retry_disposition(&err), RetryDisposition::Terminal);
    }

    /// The client's own size cap, tripped by the server-signed `total_bytes`, is
    /// terminal — the blob is content-addressed, so its size is the same anywhere.
    #[test]
    fn blob_too_large_claim_is_terminal() {
        let err = anyhow::Error::new(BlobTooLargeClaim {
            claimed: 1 << 40,
            ceiling: 1 << 20,
        });
        assert_eq!(super::retry_disposition(&err), RetryDisposition::Terminal);
    }

    /// Every "try another node" refusal fails over to the next candidate.
    #[test]
    fn node_specific_refusals_fail_over() {
        for error in [
            StreamError::NotFound,
            StreamError::Overloaded,
            StreamError::BlobTooLarge,
            StreamError::InternalError,
            StreamError::EvictedSinceProbe,
            StreamError::HashBlacklisted,
        ] {
            assert_eq!(
                super::retry_disposition(&refusal(error.clone())),
                RetryDisposition::RetryElsewhere,
                "{error:?} must fail over to the next candidate"
            );
        }
    }

    /// A stall, transport fault, or bao/hash verification failure carries no
    /// typed sentinel; it is specific to this provider's delivery, so fail over.
    #[test]
    fn untyped_delivery_failures_fail_over() {
        let err = anyhow::anyhow!("connect failed: timed out");
        assert_eq!(
            super::retry_disposition(&err),
            RetryDisposition::RetryElsewhere,
        );
    }

    fn node_key(seed: u8) -> PublicKey {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn holder(seed: u8, rtt_ms: f64) -> discovery::Probed {
        discovery::Probed {
            candidate: NodeCandidate {
                node_id: node_key(seed),
                eth_address: Address::repeat_byte(seed),
                region_hint: None,
            },
            rtt_ms,
            has_live_channel: false,
        }
    }

    fn warming_params(enabled: bool) -> ProxyWarmingParams {
        ProxyWarmingParams {
            enabled,
            rtt_threshold_ms: 150.0,
            margin_ms: 30.0,
        }
    }

    /// With warming off, the failover order is exactly the holders, nearest RTT
    /// first, and no proxy leads.
    #[test]
    fn failover_order_is_holders_by_rtt_when_warming_off() {
        let holders = vec![holder(3, 300.0), holder(1, 100.0), holder(2, 200.0)];
        let out = super::failover_order(holders, &[], warming_params(false));
        assert!(out.warming_lead.is_none());
        let ids: Vec<_> = out.order.iter().map(|c| c.node_id).collect();
        assert_eq!(ids, vec![node_key(1), node_key(2), node_key(3)]);
    }

    /// When warming engages, a nearer non-holder leads the list, the rest of the
    /// proxies follow nearest first, and the holders form the tail — so a walker
    /// gets proxy → … → direct holder (ADR 037 § Fallback).
    #[test]
    fn failover_order_prepends_proxies_then_holders() {
        // Holders are all distant (>150ms threshold); two proxies beat the best
        // holder (200ms) by ≥30ms, one (190ms) does not.
        let holders = vec![holder(10, 200.0), holder(11, 250.0)];
        let warming_pool = vec![
            discovery::WarmingCandidate {
                node_id: node_key(21),
                eth_address: Address::repeat_byte(21),
                rtt_ms: 90.0,
            },
            discovery::WarmingCandidate {
                node_id: node_key(22),
                eth_address: Address::repeat_byte(22),
                rtt_ms: 150.0,
            },
            discovery::WarmingCandidate {
                node_id: node_key(23),
                eth_address: Address::repeat_byte(23),
                rtt_ms: 190.0,
            },
        ];
        let out = super::failover_order(holders, &warming_pool, warming_params(true));

        // The nearest qualifying proxy (90ms) leads and is reported for the log.
        let lead = out.warming_lead.expect("a proxy should lead");
        assert_eq!(lead.0, node_key(21));
        assert!((lead.2 - 200.0).abs() < f64::EPSILON, "best holder rtt");

        let ids: Vec<_> = out.order.iter().map(|c| c.node_id).collect();
        assert_eq!(
            ids,
            vec![
                node_key(21), // proxy 90ms
                node_key(22), // proxy 150ms (beats 200 by 50 ≥ 30)
                node_key(10), // holder 200ms
                node_key(11), // holder 250ms
            ],
            "proxy 23 (190ms) misses the 30ms margin and is dropped; holders tail the list",
        );
        // The prepended proxy carries no region hint (spoof-proofing, ADR 037).
        assert!(out.order[0].region_hint.is_none());
    }

    /// Warming that does not engage — no proxy clears the margin — leaves the
    /// list as just the holders, with no lead.
    #[test]
    fn failover_order_no_qualifying_proxy_is_holders_only() {
        let holders = vec![holder(10, 200.0)];
        // A proxy only 10ms nearer misses the 30ms margin.
        let warming_pool = vec![discovery::WarmingCandidate {
            node_id: node_key(21),
            eth_address: Address::repeat_byte(21),
            rtt_ms: 190.0,
        }];
        let out = super::failover_order(holders, &warming_pool, warming_params(true));
        assert!(out.warming_lead.is_none());
        let ids: Vec<_> = out.order.iter().map(|c| c.node_id).collect();
        assert_eq!(ids, vec![node_key(10)]);
    }

    /// The delegated signer gate accepts the authorized key and rejects any
    /// other, naming both addresses so the operator can see the mismatch.
    #[test]
    fn ensure_delegate_signer_matches_or_rejects() {
        let key = Address::repeat_byte(0xa1);
        assert!(super::ensure_delegate_signer(key, key).is_ok());

        let other = Address::repeat_byte(0xb2);
        let err = super::ensure_delegate_signer(other, key)
            .expect_err("a non-authorized key must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains(&key.to_string()),
            "names the authorized signer: {msg}"
        );
        assert!(
            msg.contains(&other.to_string()),
            "names the loaded key: {msg}"
        );
    }

    /// A delegated `SpendingCapExhausted` is reconnected to the owner-side remedy; the
    /// delegate holds no wallet on the pool, so "top up / re-issue" is the fix.
    #[test]
    fn delegated_spending_cap_exhausted_gets_the_owner_remedy_hint() {
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
        });
        let annotated = super::annotate_delegated_exhaustion(err);
        assert!(
            annotated.to_string().contains("exhausted"),
            "expected the exhaustion remedy, got: {annotated}"
        );
    }

    /// Any other error passes through the delegated-exhaustion annotator
    /// untouched — only the three owner-remedy reasons
    /// (`SpendingCapExhausted`, `CapabilityExpired`, `PoolExhausted`) name the
    /// owner-side remedy.
    #[test]
    fn delegated_non_cap_error_is_untouched() {
        let annotated = super::annotate_delegated_exhaustion(anyhow::anyhow!("stalled"));
        assert_eq!(annotated.to_string(), "stalled");
    }

    /// A delegated `CapabilityExpired` rejection also gets the owner-remedy
    /// hint: the delegate cannot mint itself a fresh capability either.
    #[test]
    fn delegated_capability_expired_gets_the_owner_remedy_hint() {
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: decdn_protocol::client::VoucherRejectReason::CapabilityExpired,
            bundle: None,
        });
        let annotated = super::annotate_delegated_exhaustion(err);
        assert!(
            annotated.to_string().contains("expired"),
            "expected the expiry remedy, got: {annotated}"
        );
    }

    /// A delegated `PoolExhausted` rejection also gets the owner-remedy
    /// hint: it is a pool-wide deposit shortfall, not this signer's cap.
    #[test]
    fn delegated_pool_exhausted_gets_the_owner_remedy_hint() {
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: decdn_protocol::client::VoucherRejectReason::PoolExhausted,
            bundle: None,
        });
        let annotated = super::annotate_delegated_exhaustion(err);
        assert!(
            annotated.to_string().contains("exhausted"),
            "expected the exhaustion remedy, got: {annotated}"
        );
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
        let pp = "0x1111111111111111111111111111111111111111";
        c.payment_pool_address = Some(pp.into());
        c.slash_judge_address = Some("0x2222222222222222222222222222222222222222".into());
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\npayment_pool_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
        );
        let r = resolve_chain(&c, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.chain_id, 99);
        assert_eq!(r.payment_pool, Address::from_str(pp).unwrap());
    }

    #[test]
    fn config_fills_unset_flags_and_defaults() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_pool_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\nbuyer_working_deposit_micro_usdc = 5000000\n",
        );
        let r = resolve_chain(&common(), &file).unwrap();
        assert_eq!(r.rpc_url, "http://config:8545");
        // chain_id absent everywhere → default.
        assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(r.working_deposit, U256::from(5_000_000u64));
        // keystore defaults under the data dir.
        assert_eq!(
            r.keystore,
            eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
        );
    }

    /// `resolve_chain` must enforce the same deposit invariant the daemon
    /// resolver does. It reads the raw `[blockchain]` table plus the
    /// CLI flags rather than going through `resolve_blockchain_into`, so without
    /// its own check `decdn fetch` would accept a config file that
    /// `decdn config validate` rejects — a validator that does not validate what
    /// actually runs — and `--working-deposit-micro-usdc 0` would reach the chain
    /// and surface as an opaque `openPool` `ZeroAmount` revert.
    #[test]
    fn resolve_chain_rejects_deposits_the_daemon_resolver_would_reject() {
        let base = "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_pool_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n";

        // A zero working deposit can never open a pool.
        let err = resolve_chain(
            &common(),
            &config(&format!("{base}buyer_working_deposit_micro_usdc = 0\n")),
        )
        .expect_err("a zero working deposit must be refused at resolve time");
        assert!(
            err.to_string().contains("buyer_working_deposit_micro_usdc"),
            "the error must name the offending field; got: {err}"
        );
    }

    #[test]
    fn select_watermark_ok_settles_at_committed() {
        let committed = Cumulative {
            bytes: U256::from(10u64),
            amount: U256::from(20u64),
        };
        let settlement = Cumulative {
            bytes: U256::from(30u64),
            amount: U256::from(40u64),
        };
        let progress = select_watermark(&Ok(()), committed, settlement, U256::ZERO);
        assert_eq!(
            progress.advanced(),
            Some((committed.bytes, committed.amount))
        );
    }

    #[test]
    fn select_watermark_ambiguous_error_settles_high() {
        let committed = Cumulative {
            bytes: U256::from(10u64),
            amount: U256::from(20u64),
        };
        let settlement = Cumulative {
            bytes: U256::from(30u64),
            amount: U256::from(40u64),
        };
        let err = Err(anyhow::anyhow!("stall"));
        let progress = select_watermark(&err, committed, settlement, U256::ZERO);
        assert_eq!(
            progress.advanced(),
            Some((settlement.bytes, settlement.amount))
        );
    }
}
