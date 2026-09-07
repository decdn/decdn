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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_client_pull::buyer_pool::{
    LOW_WATER_DIVISOR, SELF_CAPABILITY_CAP, ToppedUpPool, ensure_allowance, escrowed_but_untracked,
    grade_deposit_credit, issue_self_capability, open_pool, refill_amount, top_up,
    topped_up_effect,
};
use decdn_client_pull::driver::{DriveConfig, drive};
use decdn_client_pull::source::{Funder, SourceFuture};
use decdn_client_pull::{
    BudgetPacer, ClientRangedStore, Cumulative, MultiSourceConfig, PeerSource, PoolContext,
    PoolExhausted, PoolLedger, ProgressCallback, PullDeadlines, RetryDisposition, SourceLane,
    UpstreamRefused, UpstreamVoucherRejected, VoucherProgress, multi_source_fetch,
    open_progressive_pull, retry_disposition, sign_client_binding,
};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_pool::{AdvanceOutcome, BuyerPoolState, BuyerPoolStore, DepositOutcome};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity::{self, PasswordUse, load_signer, read_password};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::rate::min_payment;
use decdn_incentive::{
    CapabilityGrant, LaneKey, PoolId, bind_node_id_domain, slash_judge_domain, voucher_domain,
};
use decdn_protocol::client::StreamError;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};

use decdn_client_pull::RangedStore;
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
/// bar counts **delivered content bytes** against the blob's content size — the
/// driver reports `base_present + received` (verified ranged-store leaf bytes),
/// not the wire size, so its total matches the byte count printed on
/// completion.
fn new_progress_bar() -> indicatif::ProgressBar {
    let style = indicatif::ProgressStyle::with_template(
        // Rate/ETA come from `{msg}` (see `delivery_progress`), not the built-in
        // `{bytes_per_sec}`/`{eta}` — those swing wildly on bursty chunk arrival.
        "{spinner:.green} {bytes}/{total_bytes} {msg}[{wide_bar:.cyan/blue}]",
    )
    // A bad template is a programming error, not a runtime one; fall back to the
    // built-in bar rather than panic (clippy forbids `unwrap`/`expect`).
    .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar())
    .progress_chars("=>-");
    let bar = indicatif::ProgressBar::new(0);
    bar.set_style(style);
    bar.enable_steady_tick(Duration::from_millis(120));
    // When client logging is enabled, share stderr with the tracing subscriber
    // through its `MultiProgress` so log lines do not corrupt the bar; a no-op
    // (returns the bar unchanged) on the default no-subscriber path.
    crate::logging::attach_progress_bar(bar)
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
    /// File holding the keystore password, consulted after the
    /// `DECDN_KEYSTORE_PASSWORD` env var and before a prompt. CLI/env-only, like
    /// the daemon's `ResolvedBlockchain::keystore_password_file` — passwords do
    /// not belong in a config file even by reference.
    pub(crate) keystore_password_file: Option<PathBuf>,
    /// Directory holding the buyer-pool redb store and (by default) the
    /// keystore. Client-scoped (`~/.decdn/client`) unless an explicit
    /// `--data-dir`/`identity.data_dir` is given.
    pub(crate) data_dir: PathBuf,
    /// Client region for region-first discovery ordering (`--region` >
    /// `identity.region`). `None` skips the ordering.
    pub(crate) region: Option<String>,
    /// Client region allowlist (`[client] region_allowlist`):
    /// narrows which peers `discover_provider` probes/discovers, never ranks
    /// them. Empty when the config omits `[client]` or the list, which is a
    /// no-op in [`discovery::select_candidates_filtered`]. Invalid entries
    /// (fail `Region::parse`) are dropped with an `eprintln!` warning in
    /// [`resolve_chain`] rather than failing config resolution — a typo'd
    /// region code shrinks the filter, it does not break the fetch.
    /// `eprintln!`, not `tracing::warn!`: `decdn` installs no tracing
    /// subscriber (see [`discovery::bootstrap_nodes`]'s doc comment), so a
    /// `warn!` here would reach nobody.
    pub(crate) region_allowlist: Vec<decdn_protocol::Region>,
    /// Deposit to escrow when OPENING a pool, and the target a reused pool's
    /// proactive refill restores toward once it has served verified bytes.
    pub(crate) working_deposit: U256,
    pub(crate) max_approve: bool,
}

/// Parse `[client] region_allowlist` into [`decdn_protocol::Region`]s.
/// Parsed here — not carried as raw strings — so an invalid code is reported
/// once at config-resolution time rather than on every fetch's discovery
/// path. An entry that fails `Region::parse` is dropped, not fatal: it costs
/// the filter one entry, not the whole fetch. Absent `[client]` or an absent
/// `region_allowlist` both yield an empty `Vec`, which is a no-op filter.
fn parse_region_allowlist(file: &FileConfig) -> Vec<decdn_protocol::Region> {
    file.client
        .as_ref()
        .and_then(|c| c.region_allowlist.as_ref())
        .into_iter()
        .flatten()
        .filter_map(|code| {
            let parsed = decdn_protocol::Region::parse(code);
            if parsed.is_none() {
                eprintln!(
                    "warning: client.region_allowlist entry {code:?} is not a recognized \
                     region code; dropping it from the filter"
                );
            }
            parsed
        })
        .collect()
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

    // The `[client] region_allowlist` pre-filters discovery/probing.
    let region_allowlist = parse_region_allowlist(file);

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
        keystore_password_file: args.keystore_password_file.as_deref().map(expand_tilde),
        data_dir,
        region,
        region_allowlist,
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

/// The terminal "nothing can serve this blob" error for [`probe_and_order`],
/// naming the failure that actually happened (#1911). A candidate leaves the
/// selection loop three ways: it never answered (unreachable), it answered but
/// its `slash_sig` did not recover (unverifiable), or it answered but was
/// unusable — a `has_blob`/coverage mismatch that no honest responder produces.
/// When any probe was unverifiable the cause is almost always local
/// configuration rather than missing content, so that case gets its own message;
/// otherwise the parenthetical accounts for the reachable-but-unanswered and the
/// answered-but-unusable candidates separately. Reached only when there is
/// neither a cache holder nor a reachable non-holder to pull through — a
/// `has_blob:false` answer is a serve target, not a failure, so it never lands
/// here.
fn no_serve_target_error(
    probe_count: usize,
    unreachable: usize,
    unverifiable: usize,
) -> anyhow::Error {
    if unverifiable > 0 {
        return anyhow::anyhow!(
            "{unverifiable} of {probe_count} probed node(s) answered, but their probe \
             signatures did not recover to the operator address each is registered \
             under. That is usually local configuration rather than missing content: \
             check that blockchain.slash_judge_address and blockchain.chain_id match \
             the deployment these nodes registered against"
        );
    }
    // With no unverifiable candidate, every non-unreachable one answered but was
    // dropped as unusable (has_blob/coverage mismatch) — name both counts so the
    // message never implies a silent, wholly-unreachable set when some replied.
    let unusable = probe_count.saturating_sub(unreachable);
    anyhow::anyhow!(
        "none of the {probe_count} probed node(s) could serve the blob \
         ({unreachable} did not answer, {unusable} answered but were unusable)"
    )
}

/// Probe `candidates` for `hash` over `endpoint` and return the ordered
/// provider-failover list (#1174, ADR 037 § Fallback): the sequence `fetch`
/// tries in turn, each entry a fallback for the one before it, until one
/// delivers the blob. Errors only when no probed candidate is reachable to serve
/// at all — a cache holder OR a bonded non-holder that can pull through (#1911).
///
/// Every response is verified before it can influence the order: value
/// invariants, echoed-field correlation, and `slash_sig` recovery to the
/// candidate's on-chain operator address (ADR 014 §1). Selection reads
/// `has_blob` and `rate_per_mb` off the response, and both are only meaningful
/// once the signature attributes them to the peer — an unverified quote is a
/// claim no one is accountable for, so a node could win selection on a rate it
/// never committed to. A response that fails is dropped and its candidate
/// skipped, exactly as for a timeout; it is requester-local policy and never
/// scored against the peer, since a signature that does not recover attributes
/// nothing to anyone.
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
///
/// When no candidate holds the blob, the order is instead the reachable
/// non-holders, nearest RTT first, as pull-through serve targets — see
/// [`failover_order`] for why an empty holder set bootstraps rather than fails.
pub(crate) async fn probe_and_order(
    endpoint: &Endpoint,
    candidates: &[NodeCandidate],
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
    warming: ProxyWarmingParams,
    slash_domain: &alloy::sol_types::Eip712Domain,
) -> anyhow::Result<ResolvedTargets> {
    let timestamp_us = micros_now();
    // Probe concurrently in one task. `probe_once`'s future is `Send`, so
    // `tokio::spawn` would work too; `join_all` over a shared `&endpoint` is
    // kept because it needs no per-probe clone. `probe_once`'s internal
    // timeout bounds each leg.
    let probes = candidates.iter().map(|cand| {
        let target = probe_target(cand, relay_hint.cloned());
        async move {
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
    // Probed bonded nodes that answered `has_blob:false` — reachable, with a
    // measured RTT, but not holding the blob in their cache store. They serve
    // two roles: the proxy-warming candidate pool (ADR 037 § Candidate pool)
    // when a holder exists but is distant, and — when NO holder answers — the
    // pull-through serve targets a cold blob's first fetch bootstraps from
    // (#1911), since `has_blob:false` from the cache store does not mean the
    // node cannot serve via its own origin. Always collected, because the second
    // role does not depend on warming being on.
    let mut non_holders: Vec<discovery::WarmingCandidate> = Vec::new();
    // Three ways a candidate drops out, counted separately: the terminal error below
    // has to name the one that actually happened. A wrong `slash_judge_address` or
    // `chain_id` makes EVERY honest node fail verification, and reporting that as
    // "nobody holds the blob" sends the operator hunting for missing content instead
    // of a local misconfiguration.
    let mut unreachable = 0usize;
    let mut unverifiable = 0usize;
    // (node_id, rtt_ms, rate_per_mb) for each holder that answered a probe
    // this fetch, harvested into the peer store off the critical path
    // (spawn_harvest, called from discover_provider).
    let mut probed_samples: Vec<(PublicKey, f64, u64)> = Vec::new();
    for (cand, res) in results {
        let Some((resp, resp_ext, rtt_ms)) = res else {
            unreachable += 1;
            continue;
        };
        // Verify BEFORE the response can influence the order (ADR 014 §1). A
        // failure is requester-local policy — drop it and move on, no reputation
        // effect, because an unrecovered signature attributes nothing to anyone.
        if let Err(e) = decdn_client_pull::probe::verify_probe_response(
            &resp,
            cand.eth_address,
            slash_domain,
            hash,
            timestamp_us,
        ) {
            // `decdn` installs no tracing subscriber, so this goes to stderr —
            // a candidate silently vanishing from selection is exactly what the
            // user needs told.
            unverifiable += 1;
            eprintln!(
                "warning: dropping an unverifiable probe response from {}: {e}",
                cand.node_id
            );
            continue;
        }
        // #1506: `has_blob` and `coverage.is_empty()` are a biconditional by
        // construction on an honest responder — neither field is signed, and
        // an inconsistency has no attributable author, so drop the candidate
        // rather than score it (same reasoning as an unrecovered `slash_sig`
        // above).
        if !resp_ext.consistent_with(resp.body.has_blob) {
            eprintln!(
                "warning: dropping a probe response from {} with has_blob/coverage mismatch",
                cand.node_id
            );
            continue;
        }
        // Every verified responder contributes a probe sample, holder or not.
        probed_samples.push((cand.node_id, rtt_ms, resp.body.rate_per_mb));
        if resp.body.has_blob {
            holders.push(discovery::Probed {
                candidate: cand.clone(),
                rtt_ms,
                total_bytes: resp_ext.total_bytes,
                coverage: resp_ext.coverage.clone(),
            });
        } else {
            non_holders.push(discovery::WarmingCandidate {
                node_id: cand.node_id,
                eth_address: cand.eth_address,
                rtt_ms,
            });
        }
    }

    // Terminal only when NOTHING can serve — no holder AND no reachable
    // non-holder to pull through. An empty holder set alone is not terminal: a
    // cold blob is origin-only with zero cache holders, the normal first-fetch
    // state (#1911), so as long as one bonded node answered it is a serve target.
    if holders.is_empty() && non_holders.is_empty() {
        return Err(no_serve_target_error(
            probe_count,
            unreachable,
            unverifiable,
        ));
    }

    if holders.is_empty() {
        // No cache holder, but reachable non-holders can serve via pull-through.
        // `decdn` installs no tracing subscriber, so the operator learns on
        // stderr why the fetch is talking to nodes that answered `has_blob:false`.
        eprintln!(
            "no probed node holds the blob in cache; falling back to {} reachable bonded \
             non-holder(s) as pull-through serve targets — a node serves an authorized miss \
             from its own origin (#1911)",
            non_holders.len()
        );
    }

    let ordered = failover_order(holders, &non_holders, warming);
    if let Some((node_id, proxy_rtt, best_holder_rtt)) = ordered.warming_lead {
        eprintln!(
            "proxy-warming: routing through nearer non-holder {node_id} ({proxy_rtt:.1}ms) \
             instead of the best holder ({best_holder_rtt:.1}ms) to seed a regional copy, \
             falling back to the holder if it declines (ADR 037)",
        );
    }
    Ok(ResolvedTargets {
        candidates: ordered.order,
        size_hint: ordered.size_hint,
        coverage_by_node: ordered.coverage_by_node,
        probed_samples,
        from_store_fast_path: false, // just probed: reachability-checked
    })
}

/// The ordered provider-failover list plus, when a proxy leads it, that proxy's
/// identity for the operator log line. Split from [`probe_and_order`] as a pure
/// function so the ordering is unit-tested without live probing.
struct FailoverOrder {
    /// The candidates to try in turn: proxy-warming non-holders first (nearest
    /// RTT first) when warming engages, then the holders nearest RTT first — or,
    /// when no holder answered, the reachable non-holders nearest RTT first as
    /// pull-through serve targets (#1911).
    order: Vec<NodeCandidate>,
    /// `Some((proxy_node_id, proxy_rtt_ms, best_holder_rtt_ms))` when a warming
    /// proxy is prepended; `None` when the list is just the holders.
    warming_lead: Option<(PublicKey, f64, f64)>,
    /// The largest blob size any holder reported in its probe, when any did
    /// (`ProbeResponse::total_bytes`). The LARGEST rather than the first: the
    /// field is unsigned, and this only ever DECLINES fan-out, so taking the
    /// maximum keeps one node's understated hint from suppressing multi-source
    /// for the whole set. An overstated one costs nothing — the real header
    /// governs once the fan-out engages.
    size_hint: Option<u64>,
    /// Each probed holder's measured [`decdn_protocol::Coverage`] (#1506's B1),
    /// keyed by `node_id`. Proxy-warming candidates never appear here — they
    /// are non-holders by definition, so a lookup miss on them (and on any
    /// node this map otherwise has no entry for) means "no measured
    /// coverage", which the multi-source lane builder treats as a full
    /// holder rather than as a gap in the data.
    coverage_by_node: HashMap<PublicKey, decdn_protocol::Coverage>,
}

/// Assemble the failover order (#1174, ADR 037 § Client selection policy) from
/// the probed `holders` and the `non_holders` — probed bonded nodes that
/// answered `has_blob:false`.
///
/// The holders form the backbone, nearest RTT first — and, absent proxy warming,
/// the whole list. When warming is enabled and the best holder is distant, the
/// non-holders that beat it by the margin are PREPENDED nearest first, so the
/// request routes through the nearest one (it serves via window-paced
/// pull-through and becomes the first regional copy) and falls over through the
/// remaining proxies to the direct holder. RTT-only ranking; never a gamble (an
/// empty proxy order leaves the list as just the holders).
///
/// When `holders` is empty the blob is cache-cold — its normal first-fetch state,
/// since every object is born origin-only with zero cache holders (#1911). The
/// order is then the `non_holders`, nearest RTT first, as real pull-through serve
/// targets: a node serves an authorized miss from its own origin (or node-to-node)
/// even when its cache store answers `has_blob:false`. This fallback is
/// independent of proxy warming — it is how a caching network bootstraps cold
/// content, not a latency optimization — so it applies whether warming is on or
/// off. `warming_lead`, `size_hint`, and `coverage_by_node` are all empty here,
/// since no holder answered.
fn failover_order(
    mut holders: Vec<discovery::Probed>,
    non_holders: &[discovery::WarmingCandidate],
    warming: ProxyWarmingParams,
) -> FailoverOrder {
    if holders.is_empty() {
        let mut cold = non_holders.iter().collect::<Vec<_>>();
        cold.sort_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms));
        let order = cold
            .iter()
            .map(|c| NodeCandidate {
                node_id: c.node_id,
                eth_address: c.eth_address,
                // As for a proxy lead: `region_hint` never rides along on a
                // candidate the client assembled from probe RTTs alone.
                region_hint: None,
                // A warming candidate carries no registry addresses (the probe
                // RTT path does not thread them), so it dials via iroh
                // discovery — the pre-multiaddr behavior, unchanged.
                multiaddrs: Bytes::new(),
            })
            .collect();
        return FailoverOrder {
            order,
            warming_lead: None,
            size_hint: None,
            coverage_by_node: HashMap::new(),
        };
    }
    holders.sort_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms));
    // Captured before `holders` is consumed into `order` below — each probed
    // holder's real coverage, for the multi-source lane builder (#1506's B3).
    let coverage_by_node: HashMap<PublicKey, decdn_protocol::Coverage> = holders
        .iter()
        .map(|h| (h.candidate.node_id, h.coverage.clone()))
        .collect();
    let best_holder_rtt = holders
        .iter()
        .map(|h| h.rtt_ms)
        .fold(f64::INFINITY, f64::min);
    let proxy_order = if warming.enabled {
        discovery::proxy_warming_order(
            best_holder_rtt,
            warming.rtt_threshold_ms,
            warming.margin_ms,
            non_holders,
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
        // Warming proxies dial via iroh discovery: `WarmingCandidate` carries no
        // registry addresses, so no direct-dial hint is available here. A
        // follow-up could thread them through for relay-free warming.
        multiaddrs: Bytes::new(),
    });
    let order = proxies
        .chain(holders.iter().map(|h| h.candidate.clone()))
        .collect();
    let size_hint = holders.iter().filter_map(|h| h.total_bytes).max();
    FailoverOrder {
        order,
        warming_lead,
        size_hint,
        coverage_by_node,
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
) -> anyhow::Result<ResolvedTargets> {
    // Probe-less fast path: when the store already holds enough fresh,
    // unsuppressed candidates, skip the network probe round entirely and rank
    // by the store's own EWMA latency. Falls through to today's bootstrap +
    // probe path whenever the store can't back a full candidate set — never a
    // new failure mode, only a possible extra probe round.
    let peer_store = decdn_client_pull::PeerStore::open(&chain.data_dir);
    let store_cfg = decdn_client_pull::StoreConfig::default();
    if !args.rediscover
        && let Some(targets) =
            store_fast_path(&peer_store, &store_cfg, args.max_sources, now_secs_cli())
    {
        return Ok(targets);
    }
    let bootstrap = discovery::bootstrap_nodes(
        &chain.rpc_url,
        capacity_bond,
        &chain.data_dir,
        args.discovery_cap(),
    )
    .await?;
    // Captured before `bootstrap` is consumed: the registry-outage fallback
    // (`Bootstrap::Cached`, the peer store's surviving identities) must
    // IGNORE `chain.region_allowlist` — the client is already
    // degraded to whatever the store still has, and narrowing that further
    // by region risks starving the fetch entirely over data that is already
    // possibly stale.
    let is_live_registry = matches!(bootstrap, discovery::Bootstrap::Live { .. });
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
    // Captured BEFORE `select_candidates_filtered` truncates to `SELECT_K`:
    // the harvest persists identity for the whole registry read, not just
    // the shortlist that got probed (#1911-series peer store).
    let registry_candidates = all.clone();
    let allow: &[decdn_protocol::Region] = if is_live_registry {
        chain.region_allowlist.as_slice()
    } else {
        &[]
    };
    let mut selected = discovery::select_candidates_filtered(
        all,
        chain.region.as_deref(),
        discovery::SELECT_K,
        allow,
    );
    // Progressive widening: a too-thin region filter must never
    // starve the fetch. Re-run unfiltered over the same registry read when
    // the filtered pool falls below the store's own freshness floor.
    if !allow.is_empty() && selected.len() < store_cfg.min_fresh_candidates {
        selected = discovery::select_candidates_filtered(
            registry_candidates.clone(),
            chain.region.as_deref(),
            discovery::SELECT_K,
            &[],
        );
    }
    let warming = ProxyWarmingParams::from_args(args);
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
    let targets =
        probe_and_order(endpoint, &selected, relay_hint, hash, warming, &slash_dom).await?;
    // Identity is harvested ONLY against a live registry read, and
    // `resolve_bootstrap` already does that: on a `Bootstrap::Live` read it
    // upserts+prunes identity for every returned node. So the harvest here
    // writes stats only and never identity. Two reasons:
    //   - On the `Bootstrap::Cached` outage path `registry_candidates` IS the
    //     store's own surviving identities; re-`upsert_identity`-ing them would
    //     "confirm" identity against the store itself and reset
    //     `identity_seen_at_secs` — contradicting its meaning ("last confirmed
    //     against the registry") and keeping a departed node from ever becoming
    //     `identity_prunable` while outages recur.
    //   - On the live path it would be a redundant second identity write over
    //     what `resolve_bootstrap` already wrote.
    // The probed stats are harvested on both paths.
    //
    // Off the fetch's critical path: the handle is intentionally dropped,
    // never awaited here (tests await it directly for determinism).
    drop(spawn_harvest(
        &chain.data_dir,
        Vec::new(),
        targets.probed_samples.clone(),
    ));
    Ok(targets)
}

/// Build a probe-less [`ResolvedTargets`] straight from the peer store: rank
/// every [`decdn_client_pull::PeerRecord::selectable`] record by its
/// EWMA `latency_ms` (ascending, `None` sorts last), project each back to a
/// [`NodeCandidate`], and admit per-operator via
/// [`discovery::admit_sources`]. Returns `None` when fewer than
/// `cfg.min_fresh_candidates` records are selectable, or when admission still
/// leaves the set below that floor — either way the caller falls back to the
/// probe path unchanged. Takes no [`Endpoint`], so it structurally issues no
/// network probe.
fn store_fast_path(
    store: &decdn_client_pull::PeerStore,
    cfg: &decdn_client_pull::StoreConfig,
    max_sources: usize,
    now_secs: u64,
) -> Option<ResolvedTargets> {
    // Require BOTH a fresh, unsuppressed latency sample AND a non-prunable
    // identity — symmetric with `resolve_bootstrap`'s outage-fallback filter. A
    // stats-only placeholder that `record_sample` created for an unknown peer
    // carries `eth_address: Address::ZERO` and `identity_seen_at_secs == 0`, so
    // it is always `identity_prunable` and can never project a `0x0` payment
    // lane into a fetch here.
    let mut fresh: Vec<_> = store
        .load_all()
        .into_iter()
        .filter(|r| r.selectable(now_secs, cfg) && !r.identity_prunable(now_secs, cfg))
        .collect();
    if fresh.len() < cfg.min_fresh_candidates {
        return None;
    }
    fresh.sort_by(|a, b| {
        a.latency_ms
            .unwrap_or(f64::MAX)
            .total_cmp(&b.latency_ms.unwrap_or(f64::MAX))
    });
    let ordered: Vec<NodeCandidate> = fresh
        .iter()
        .map(decdn_client_pull::PeerRecord::as_candidate)
        .collect();
    let candidates = discovery::admit_sources(ordered, max_sources);
    if candidates.len() < cfg.min_fresh_candidates {
        return None;
    }
    Some(ResolvedTargets {
        candidates,
        size_hint: None,
        coverage_by_node: HashMap::new(),
        probed_samples: Vec::new(),
        // The one site that sets this: these candidates are projected from the
        // store without a probe, so the driver must keep discovery in reserve.
        from_store_fast_path: true,
    })
}

/// Persist a discovery session's identity + probe stats off the fetch's
/// critical path: `upsert_identity` for every registry candidate (whether or
/// not it was probed), `record_sample` for each probed triple, then
/// `prune_and_cap` to bound store growth. Never awaited on the fetch path —
/// the returned handle exists so tests can await it for determinism.
pub(crate) fn spawn_harvest(
    data_dir: &Path,
    registry: Vec<NodeCandidate>,
    probed: Vec<(PublicKey, f64, u64)>,
) -> tokio::task::JoinHandle<()> {
    let dir = data_dir.to_path_buf();
    tokio::spawn(async move {
        let store = decdn_client_pull::PeerStore::open(&dir);
        let cfg = decdn_client_pull::StoreConfig::default();
        let now = now_secs_cli();
        for cand in &registry {
            let _ = store.upsert_identity(cand, now);
        }
        for (id, rtt_ms, rate) in probed {
            let _ = store.record_sample(&id, rtt_ms, rate, now, &cfg);
        }
        let _ = store.prune_and_cap(now, &cfg);
    })
}

/// Seconds since the Unix epoch, saturating to 0 on a clock before the epoch
/// (never on this platform in practice) rather than panicking.
fn now_secs_cli() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The ordered failover list plus what discovery already learned about the
/// blob's size.
pub(crate) struct ResolvedTargets {
    /// The candidates to try in turn (#1174).
    pub(crate) candidates: Vec<NodeCandidate>,
    /// The blob size a holder reported in its probe, when any did. Lets the
    /// multi-source engagement gate apply its size floor BEFORE opening a pool
    /// and a throwaway header stream just to learn the size — work the
    /// single-source path then repeats when the gate declines. `None` on the
    /// pinned `--node-id` path and whenever no holder reported a size, where the
    /// gate falls back to the header open.
    pub(crate) size_hint: Option<u64>,
    /// Each probed holder's measured [`decdn_protocol::Coverage`] (#1506's B1),
    /// keyed by `node_id` — what the multi-source lane builder reads instead of
    /// assuming every admitted candidate is a full holder. Empty on the pinned
    /// `--node-id` path, where nothing was probed; a lookup miss there (as
    /// everywhere else) reads as "no measured coverage" and the lane builder
    /// falls back to [`decdn_protocol::Coverage::full`].
    pub(crate) coverage_by_node: HashMap<PublicKey, decdn_protocol::Coverage>,
    /// `(node_id, rtt_ms, rate_per_mb)` for each holder that answered a probe
    /// this fetch — harvested into the peer store.
    pub(crate) probed_samples: Vec<(PublicKey, f64, u64)>,
    /// `true` only when this set came from the probe-less [`store_fast_path`],
    /// which projects candidates from the store without probing them. The
    /// driver reads it to enforce the store's approved invariant: a fresh but
    /// unreachable fast-path set must never make a fetch fail that discovery
    /// would have served, so exhausting one with a retryable error triggers a
    /// single in-fetch rediscovery. `false` on every probed / pinned path,
    /// where the candidates were already reachability-checked.
    pub(crate) from_store_fast_path: bool,
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
) -> anyhow::Result<ResolvedTargets> {
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
        return Ok(ResolvedTargets {
            candidates: vec![NodeCandidate {
                node_id,
                eth_address: provider,
                region_hint: None,
                // A `--node-id`-pinned target takes its direct address from
                // `--addr` at the dial site, not from the registry.
                multiaddrs: Bytes::new(),
            }],
            // A pinned node is one candidate, so multi-source never engages and
            // no size hint is needed.
            size_hint: None,
            // Nothing was probed on this path, so no holder coverage was
            // measured; irrelevant anyway since multi-source never engages here.
            coverage_by_node: HashMap::new(),
            // Nothing was probed on this path, so there is nothing to harvest.
            probed_samples: Vec::new(),
            // A pinned `--node-id` is not a store projection; there is nothing
            // to rediscover if it fails.
            from_store_fast_path: false,
        });
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
    // here also cancels the ADR 012 § Bootstrap step 4 peer-store fallback,
    // so a client with a usable peer store would be handed a hard failure
    // instead of the degraded-but-working fetch the store exists to provide.
    let order =
        discover_provider(endpoint, chain, capacity_bond, relays.first(), hash, args).await?;
    if !order.candidates.is_empty() {
        eprintln!("Discovered {} node(s):", order.candidates.len());
        for c in &order.candidates {
            eprintln!(
                "  * {} / {} / {}",
                c.region_hint.as_ref().map_or("??", |r| r.as_str()),
                node_id_tail(&c.node_id),
                short_address(&c.eth_address),
            );
        }
    }
    Ok(order)
}

/// Last 4 hex characters of a node id, for a compact operator-facing listing.
///
/// The full 64-hex id is unwieldy in a per-line summary; the tail is enough to
/// tell candidates apart at a glance. Falls back to the full string only if it
/// is somehow shorter than 4 characters (it never is for a `PublicKey`).
fn node_id_tail(node_id: &PublicKey) -> String {
    let s = node_id.to_string();
    s.get(s.len().saturating_sub(4)..).unwrap_or(&s).to_owned()
}

/// Shorten an Ethereum address to `0x` + first 4 + `...` + last 4 hex
/// characters (e.g. `0xa43d...fdCe`), preserving the checksummed casing.
///
/// Returns the full string unchanged if it is unexpectedly short.
fn short_address(addr: &Address) -> String {
    let s = addr.to_string();
    match (s.get(..6), s.get(s.len().saturating_sub(4)..)) {
        (Some(head), Some(tail)) if s.len() > 10 => format!("{head}...{tail}"),
        _ => s,
    }
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
             {other:?}; the next reuse may re-sign a stale watermark, which that provider \
             rejects — close and reopen the pool if reuse starts failing",
            lane.provider
        ),
        Err(e) => eprintln!(
            "warning: failed to persist voucher watermark for pool {pool_id} (provider {}): {e}; \
             the next reuse may re-sign a stale watermark, which that provider rejects — close \
             and reopen the pool if reuse starts failing",
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
    // These are `mut` so the store-fast-path rediscovery fallback below can
    // replace them with a freshly discovered set within this same fetch.
    let targets = resolve_target_node(common, &chain, &endpoint, &relays, hash).await?;
    let mut candidates = targets.candidates;
    let mut coverage_by_node = targets.coverage_by_node;
    let mut size_hint = targets.size_hint;
    let mut from_store_fast_path = targets.from_store_fast_path;

    // Buyer signer (vouchers + the openPool/topUp tx). Loaded after selection so a
    // failed discovery never prompts for a keystore password. Password from env,
    // else `--keystore-password-file`, else TTY.
    let password = read_password(
        &super::chain_ctx::password_sources(
            chain.keystore_password_file.as_deref(),
            PasswordUse::Unlock,
        ),
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
        common.min_throughput_bps(),
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

    // The fetch runs at most twice: once over the resolved candidate set, and —
    // if that set came from the probe-less store fast path and its failover
    // exhausts with a RETRYABLE error — once more over a freshly DISCOVERED set.
    // The store must never make a fetch fail that probing would have served
    // (ADR 037 § in-fetch discovery fallback), so a fresh-but-unreachable
    // fast-path set falls through to real discovery within this same fetch. The
    // second pass forces discovery (fast path off), so it cannot loop back into
    // the fast path; `rediscovered` caps it at one retry regardless.
    let mut rediscovered = false;
    loop {
        // Multi-source fan-out (ADR 039): when enabled, the blob clears the size
        // floor, and at least two operator-distinct holders are admissible, fetch
        // it in parallel across a per-provider lane set. A `None` gate (kill switch
        // off, too small, too few holders) falls through to the single-source loop
        // below unchanged. Each lane pays its OWN provider on its OWN `(ctx,
        // ledger)`; the scheduler gates every lane on the shared pool's remaining
        // balance.
        {
            let (bar, on_progress, meter) = delivery_progress();
            let multi = try_multi_source_fetch(
                &deps,
                &args.common,
                grant.as_ref(),
                &signer,
                &voucher_dom,
                &candidates,
                &coverage_by_node,
                &relays,
                hash,
                &args.output,
                size_hint,
                Some(&on_progress),
                // One fetch at a time: no cross-fetch pool opens to serialize.
                None,
            )
            .await;
            bar.finish_and_clear();
            match multi {
                Ok(Some(bytes)) => {
                    print_fetch_summary(bytes, &meter, &args.output);
                    return Ok(());
                }
                // Gate not met: run the single-source failover loop below. The gate
                // itself reports which condition it was.
                Ok(None) => {}
                // The fan-out engaged and failed. Only a TERMINAL failure ends the
                // fetch: a pool exhaustion (no lane and no provider can fund it) or
                // what the shared classifier rules terminal. Anything else is
                // precisely the class the failover loop below was built to survive —
                // returning it here would fail a recoverable fetch that the
                // pre-fan-out path completed by trying the next candidate. The loop
                // resumes the same `.partial`, so nothing already paid for is
                // re-bought.
                Err(err)
                    if retry_disposition(&err) == RetryDisposition::Terminal
                        || err.downcast_ref::<PoolExhausted>().is_some() =>
                {
                    // On the delegated path reconnect a terminal exhaustion to the
                    // owner remedy, same as the single-source path does.
                    return Err(if grant.is_some() {
                        annotate_delegated_exhaustion(err)
                    } else {
                        err
                    });
                }
                Err(err) => {
                    eprintln!(
                        "multi-source fetch failed ({err:#}); falling back to single-source \
                         failover over the same candidates"
                    );
                }
            }
        }

        // Provider failover (#1174, ADR 037 § Fallback): try each resolved
        // candidate in turn until one delivers the blob. All candidates draw on the
        // ONE shared pool (ADR 003) — each provider is a distinct lane, and a lane
        // for a not-yet-paid provider opens nothing on-chain — and the
        // `ClientRangedStore` beside `--output` is keyed on `(hash, total_bytes)`,
        // so a fail-over resumes the partial and re-pays nothing already delivered.
        // A retryable failure (a cache-miss `NotFound`, a stall, a transport fault)
        // advances to the next candidate; a terminal one (pool/funder exhausted,
        // blob over the cap) stops immediately; the last error is carried out when
        // the list is exhausted.
        let mut last_err: Option<anyhow::Error> = None;
        for (attempt, candidate) in candidates.iter().enumerate() {
            let provider = candidate.eth_address;

            // Delegated (`--capability`): adopt the named pool + owner capability,
            // no open. Self-owned: reuse the caller's live pool (resuming this
            // provider's lane watermark) or open and persist a new one. Both attach
            // the ADR 005 client binding. Rebuilt per candidate because the lane is
            // per-provider.
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
            // explicit-node candidate; a discovered node is reached via its
            // resolved address + relay hint.
            if let Some(addr) = common.addr {
                target = target.with_ip_addr(addr);
            }
            if let Some(url) = relays.first() {
                target = target.with_relay_url(url.clone());
            }
            // A discovered node also carries its registry multiaddrs as
            // direct-address hints, so a reachable node connects relay-free
            // (ADR 001 § Node Discovery). On the explicit `--addr` path this
            // adds nothing — a pinned candidate has empty `multiaddrs`.
            target = discovery::with_dial_addrs(target, candidate);

            // Immutable pool fact captured before `ctx` moves into `drive_fetch`.
            let pool_id = ctx.pool_id;

            // A fresh delivery progress bar per attempt (#1118). `indicatif` draws
            // to stderr and hides itself when stderr is not a terminal. On a resumed
            // fail-over it counts only the remaining transfer — the ranged store
            // re-pulls only the missing ranges.
            let (bar, on_progress, meter) = delivery_progress();
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
            // idempotent, so this is a harmless no-op on paths where the hook
            // already cleared the bar.
            bar.finish_and_clear();

            // On the delegated path `SpendingCapExhausted`, `CapabilityExpired`, and
            // `PoolExhausted` are all terminal — the delegate cannot top up an
            // owner's pool or raise/re-mint its own capability — so reconnect them
            // to the owner-side remedy rather than leaving a bare "voucher
            // rejected: ...".
            let err = match result {
                Ok(bytes) => {
                    print_fetch_summary(bytes, &meter, &args.output);
                    return Ok(());
                }
                Err(err) if grant.is_some() => annotate_delegated_exhaustion(err),
                Err(err) => err,
            };

            // Stop on a terminal failure immediately. On a retryable one, route to
            // the next candidate, or — when this was the last — break out with it so
            // the store-fast-path rediscovery fallback below can consider it.
            if retry_disposition(&err) == RetryDisposition::Terminal {
                return Err(err);
            }
            let more_candidates = attempt + 1 < candidates.len();
            if !more_candidates {
                last_err = Some(err);
                break;
            }
            eprintln!(
                "fetch: provider {provider} could not deliver ({err:#}); failing over to the \
                 next of {} candidate(s)",
                candidates.len(),
            );
            last_err = Some(err);
        }

        // The candidate list exhausted with a retryable error (or, in the
        // never-reached empty-list case, none at all). `last_err` is set whenever
        // the loop ran a candidate; keep a defensive error for the unreachable
        // empty case.
        let exhausted_err = last_err.unwrap_or_else(|| {
            anyhow::anyhow!("no candidate node could deliver the requested blob")
        });

        // In-fetch discovery fallback (ADR 037 § in-fetch discovery fallback,
        // acceptance criterion 5): when the exhausted set came from the probe-less
        // store fast path and the failure is retryable, re-resolve ONCE via full
        // discovery and run the failover again over the probed candidates. The
        // `.partial` ranged store and the shared payment pool's lane watermarks are
        // persisted on disk and keyed by content, so the second pass resumes the
        // partial and re-pays nothing already delivered (ADR 003) — it is a
        // continuation, not a fresh from-zero fetch. A terminal failure is never
        // retried this way; it returns exactly as before.
        if from_store_fast_path
            && !rediscovered
            && retry_disposition(&exhausted_err) != RetryDisposition::Terminal
        {
            rediscovered = true;
            eprintln!(
                "fetch: every probe-less store candidate was unreachable ({exhausted_err:#}); \
                 re-resolving via full discovery within this fetch and resuming the partial \
                 already held (ADR 037)"
            );
            // Force discovery for this second resolve by cloning the args and
            // setting `rediscover` — a clone, so the user's own args are untouched
            // and identity/stats harvest still runs on the discovery path.
            let mut disc_args = common.clone();
            disc_args.rediscover = true;
            let fresh = resolve_target_node(&disc_args, &chain, &endpoint, &relays, hash).await?;
            candidates = fresh.candidates;
            coverage_by_node = fresh.coverage_by_node;
            size_hint = fresh.size_hint;
            // False by construction (discovery forced), which — together with
            // `rediscovered` — guarantees the loop cannot rediscover again.
            from_store_fast_path = fresh.from_store_fast_path;
            continue;
        }

        return Err(exhausted_err);
    }
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
/// Consumed by [`try_multi_source_fetch`], which builds one per-provider payment
/// lane ([`SourceLane`]) per admitted candidate — each with its OWN
/// `(signer, provider)` `PoolContext`/`PoolLedger` (ADR 039 § Payment model) —
/// and calls [`multi_source_fetch`]; a `false` gate falls through to the
/// single-source provider-failover loop unchanged.
#[must_use]
pub(crate) const fn should_multi_source(
    enabled: bool,
    total_bytes: u64,
    min_bytes: u64,
    admissible: usize,
) -> bool {
    enabled && total_bytes > min_bytes && admissible >= 2
}

/// The engagement-gate conditions decidable without a probe: the kill switch,
/// the operator-distinct holder count, and the probe-reported size floor.
/// Returns `true` when multi-source must not engage — and says which condition
/// declined, since a user who passed `--multi-source --max-sources 8` and then
/// watches the blob arrive over one connection has no other way to tell the
/// size floor from the operator-spread filter from the kill switch.
///
/// Shared by [`try_multi_source_fetch`] and `bundle_pull`'s multi-source
/// pre-branch, which needs the answer BEFORE taking its lane lock-set: taking
/// the locks for a fetch the gate then declines would block concurrent
/// bundle entries sharing those providers for no fan-out.
///
/// `admitted` must be `discovery::admit_sources(candidates.to_vec(),
/// common.max_sources)` — the operator-distinct set the fan-out would actually
/// use. Passing it in avoids recomputing the same admission in the caller
/// (bundle pull needs it for its lane-lock set) and inside this gate.
pub(crate) fn multi_source_gate_declines(
    common: &cli::ClientFetchArgs,
    candidates: &[NodeCandidate],
    admitted: &[NodeCandidate],
    size_hint: Option<u64>,
) -> bool {
    if !common.multi_source_enabled() {
        return true;
    }
    if admitted.len() < 2 {
        eprintln!(
            "multi-source: not engaging — {} operator-distinct holder(s) among {} candidate(s), \
             and fan-out needs two (one lane per operator: two nodes of one operator would \
             share a voucher lane)",
            admitted.len(),
            candidates.len()
        );
        return true;
    }
    // The size floor, applied against discovery's probe-reported hint when there
    // is one, so a below-floor blob declines here instead of after a pool open
    // and a throwaway header stream the single-source path then repeats. The
    // hint is unsigned, so it only ever DECLINES: an overstated one falls
    // through to the authoritative header check in `try_multi_source_fetch`.
    if let Some(hint) = size_hint
        && !should_multi_source(true, hint, common.multi_source_min_bytes, admitted.len())
    {
        eprintln!(
            "multi-source: not engaging — the blob is ~{hint} bytes, below the {} byte fan-out \
             floor (--multi-source-min-bytes)",
            common.multi_source_min_bytes
        );
        return true;
    }
    false
}

// Failover classification (`retry_disposition` / `RetryDisposition`) is shared
// with the multi-source scheduler, so it lives in `decdn_client_pull::retry` and
// is imported above — the single-source loop below and the scheduler classify
// failures identically.

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

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
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

    // Captured before `target` moves into `PeerSource::new` below — the key the
    // peer store's stream-derived sample/failure is filed under (#1906-series).
    let node_id = target.id;
    let peer_store = decdn_client_pull::PeerStore::open(&deps.chain.data_dir);
    let peer_store_cfg = decdn_client_pull::StoreConfig::default();

    // Learn the whole-blob size before constructing the ranged store: the store is
    // keyed on `(root, total_bytes)`, and the signed `StreamResponse` header is the
    // authoritative source of `total_bytes`. This throwaway open is a handshake
    // only — no voucher is signed until the first paid interval, so it pays nothing
    // — and its pull is dropped immediately; `drive` re-opens exactly the gaps it
    // needs. The cache-miss annotation is applied here too, so an unbound or
    // underfunded refusal is still explained at this first contact.
    //
    // This handshake is also the observed-TTFB boundary the peer store wants
    // (#1906-series): a source that cannot even complete it is stamped as a
    // failure, and one that does hands back a real stream-derived latency —
    // best-effort in both directions (`let _ =`), never failing the fetch.
    let (header, first_pull) = match open_progressive_pull(
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
        deps.max_rate_per_mb,
        deps.deadlines,
        0,
        // One long-lived runtime: this connection's driver outlives the fetch.
        None,
    )
    .await
    {
        Ok(opened) => opened,
        Err(err) => {
            let _ = peer_store.record_failure(&node_id, now_secs_cli());
            return Err(annotate_unbound_cache_miss(err, &ctx));
        }
    };
    // The stream's own quoted rate supersedes any remembered probe rate — it is
    // the authoritative figure this fetch is actually paying.
    let _ = peer_store.record_sample(
        &node_id,
        header.ttfb_ms,
        header.rate_per_mb,
        now_secs_cli(),
        &peer_store_cfg,
    );
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
        None, // pool: single-source client fetch — one lane is the whole pool
    )
    .await;
    // The handshake succeeded (the sample above is real) but delivery itself
    // failed — still a failure of this source for selection purposes, so stamp
    // it. This runs AFTER `record_sample` above, so a failing body transfer
    // always wins the suppression: `record_failure` is the last write.
    if drive_result.is_err() {
        let _ = peer_store.record_failure(&node_id, now_secs_cli());
    }

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

/// One built payment lane for a multi-source fetch: the per-provider
/// `PoolContext`/`PoolLedger` and the [`PeerSource`] that pays with them. Owns
/// the `PeerSource` so the borrowed [`SourceLane`] the scheduler consumes can
/// point at it; the `ctx`/`ledger` `Arc`s are cloned into that `SourceLane`.
struct MultiLane<'a> {
    /// The candidate's `node_id` — the key the coverage map built in
    /// [`failover_order`] is keyed on, so the scheduler lane this becomes can
    /// look up its real measured coverage instead of assuming a full holder.
    node_id: PublicKey,
    provider: Address,
    pool_id: PoolId,
    /// The lane's persisted prior amount — the baseline
    /// [`select_watermark`]/[`persist_watermark`] compute the advance against.
    prior_amount: U256,
    ctx: Arc<Mutex<PoolContext>>,
    ledger: Arc<PoolLedger>,
    source: PeerSource<'a>,
}

/// The `SourceLane::coverage` for one multi-source lane: the REAL measured
/// `Coverage` the `cdn/probe/v1` round trip reported for that holder (#1506's
/// B1), looked up by `node_id` in the map [`failover_order`] built from the
/// probed `holders`.
///
/// A lookup miss — a source with no entry in `coverage_by_node` — gets
/// `Coverage::full`: every source reaching a multi-source lane already passed
/// the `has_blob`/`coverage` probe consistency check EXCEPT a proxy-warming
/// source (a non-holder promoted into the set to seed a regional copy via
/// pull-through, and so will serve any range) and the pinned `--node-id` path
/// (which probes nothing and passes an empty map). Both cases mean "will
/// serve", matching the full-coverage semantic.
fn lane_coverage(
    coverage_by_node: &HashMap<PublicKey, decdn_protocol::Coverage>,
    node_id: PublicKey,
    num_blocks: u32,
) -> decdn_protocol::Coverage {
    coverage_by_node
        .get(&node_id)
        .cloned()
        .unwrap_or_else(|| decdn_protocol::Coverage::full(num_blocks))
}

/// Build a probe target for a discovered candidate: its `node_id`, an optional
/// relay hint, and its registry multiaddrs as direct-address hints so a
/// reachable node is probed relay-free (ADR 001 § Node Discovery).
fn probe_target(cand: &NodeCandidate, relay: Option<RelayUrl>) -> EndpointAddr {
    let mut target = EndpointAddr::new(cand.node_id);
    if let Some(url) = relay {
        target = target.with_relay_url(url);
    }
    discovery::with_dial_addrs(target, cand)
}

/// Build the target address for a discovered candidate: its `node_id`, the
/// first configured relay hint, and its registry multiaddrs as direct-address
/// hints (ADR 001 § Node Discovery). Multi-source only runs on the auto-discovered
/// set (never the single explicit `--node-id`/`--addr`), so no pinned IP applies.
fn multi_source_target(candidate: &NodeCandidate, relays: &[RelayUrl]) -> EndpointAddr {
    let mut target = EndpointAddr::new(candidate.node_id);
    if let Some(url) = relays.first() {
        target = target.with_relay_url(url.clone());
    }
    // Registry multiaddrs as direct-address hints: a reachable lane provider
    // connects without a relay (ADR 001 § Node Discovery).
    discovery::with_dial_addrs(target, candidate)
}

/// Build one [`MultiLane`] for `candidate`: open/reuse its per-provider pool,
/// seed a ledger from that lane's persisted cumulative, and wrap a [`PeerSource`]
/// over it. Mirrors the single-source per-candidate construction in `fetch()`'s
/// failover loop, hoisted so the whole admitted set is built up front.
///
/// `open_lock`, when `Some`, serializes the pool open-or-reuse inside
/// [`build_ctx_for_fetch`] against other fetches sharing the one on-chain pool
/// (bundle pull's cross-entry concurrency, #1774); the guard is dropped before
/// any streaming, and skipped entirely on the delegated path, which opens
/// nothing on-chain. Callers must already hold every provider lock for the
/// fetch's admitted set, so the lock order stays provider-locks → `open_lock`
/// and no hold-and-wait cycle can form.
#[allow(clippy::too_many_arguments)]
async fn build_multi_lane<'a, P>(
    deps: &DriveFetchDeps<'a, P>,
    grant: Option<&CapabilityGrant>,
    signer: &Arc<PrivateKeySigner>,
    voucher_dom: &Eip712Domain,
    candidate: &NodeCandidate,
    relays: &[RelayUrl],
    open_lock: Option<&tokio::sync::Mutex<()>>,
) -> anyhow::Result<MultiLane<'a>>
where
    P: alloy::providers::Provider + Clone,
{
    let provider = candidate.eth_address;
    let ctx = {
        // Bundle pull funnels every entry — and every lane of a multi-source
        // entry — through the one shared `PaymentPool` deposit, so the
        // open-or-reuse serializes across the whole bundle. The delegated path
        // opens nothing on-chain (`build_delegated_pool_ctx` adopts the owner's
        // pool), so it skips the guard: holding the bundle-wide lock there
        // would serialize unrelated fetches for no mutual-exclusion win.
        let _open_guard = match (open_lock, grant) {
            (Some(lock), None) => Some(lock.lock().await),
            _ => None,
        };
        build_ctx_for_fetch(
            grant,
            deps.store,
            deps.contract,
            deps.rpc,
            signer,
            voucher_dom,
            provider,
            deps.self_address,
            deps.chain,
            deps.endpoint,
        )
        .await?
    };
    let pool_id = ctx.pool_id;
    let prior_amount = ctx.prior_amount;
    // One ledger per lane, seeded from its persisted `(signer, provider)`
    // cumulative so the first voucher continues the lane (a restart from zero is
    // rejected as a regression) — the same seeding `drive_fetch` does per lane.
    let ledger = Arc::new(PoolLedger::new(Cumulative {
        bytes: ctx.prior_bytes_delivered,
        amount: ctx.prior_amount,
    }));
    let target = multi_source_target(candidate, relays);
    let ctx = Arc::new(Mutex::new(ctx));
    let source = PeerSource::new(
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
    Ok(MultiLane {
        node_id: candidate.node_id,
        provider,
        pool_id,
        prior_amount,
        ctx,
        ledger,
        source,
    })
}

/// The multi-source engagement path (ADR 039). Returns `Ok(None)` when the
/// engagement gate ([`should_multi_source`]) is not met — the caller then runs
/// the single-source provider-failover loop unchanged. Returns `Ok(Some(bytes))`
/// once the blob is fetched in parallel across the admitted set, or `Err` when
/// the parallel fetch fails (the `.partial` store is left in place for a later
/// resume, exactly like a single-source failure).
///
/// Each admitted candidate becomes its OWN payment lane ([`SourceLane`]) — its
/// own on-chain provider, `PoolContext`, and `PoolLedger` — and every lane draws
/// on the one shared pool deposit, gated on the aggregate remaining so no lane
/// over-draws it (ADR 039 § Payment model). On completion each lane's voucher
/// watermark is persisted independently.
///
/// `open_lock` serializes every lane's pool open-or-reuse against other fetches
/// drawing on the same on-chain pool: `bundle pull` runs many entries
/// concurrently over ONE shared deposit, so it passes its bundle-wide lock;
/// `decdn fetch` runs one fetch at a time and passes `None`.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn try_multi_source_fetch<P>(
    deps: &DriveFetchDeps<'_, P>,
    common: &cli::ClientFetchArgs,
    grant: Option<&CapabilityGrant>,
    signer: &Arc<PrivateKeySigner>,
    voucher_dom: &Eip712Domain,
    candidates: &[NodeCandidate],
    coverage_by_node: &HashMap<PublicKey, decdn_protocol::Coverage>,
    relays: &[RelayUrl],
    hash: [u8; 32],
    output: &Path,
    size_hint: Option<u64>,
    progress: Option<&ProgressCallback>,
    open_lock: Option<&tokio::sync::Mutex<()>>,
) -> anyhow::Result<Option<u64>>
where
    P: alloy::providers::Provider + Clone,
{
    // Spread the ranked candidate set across distinct operators (ADR 039
    // § Source diversity and reputation). Every gate that can be decided without
    // a probe short-circuits BEFORE any chain/network work. Admission is computed
    // once and reused for the gate and the lane set.
    let admitted = discovery::admit_sources(candidates.to_vec(), common.max_sources);
    if multi_source_gate_declines(common, candidates, &admitted, size_hint) {
        return Ok(None);
    }
    try_multi_source_fetch_from_admitted(
        deps,
        common,
        grant,
        signer,
        voucher_dom,
        admitted,
        coverage_by_node,
        relays,
        hash,
        output,
        progress,
        open_lock,
    )
    .await
}

/// Inner multi-source fetch that assumes admission and the pre-probe gate have
/// already been decided. `admitted` is the operator-distinct set
/// `discovery::admit_sources(candidates, max_sources)` would have produced;
/// the caller must have already confirmed the gate would engage. This lets
/// `bundle_pull::PullCtx::try_multi_source` reuse the same `admitted` it used
/// for its lane-lock set without recomputing `admit_sources` inside the fan-out.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn try_multi_source_fetch_from_admitted<P>(
    deps: &DriveFetchDeps<'_, P>,
    common: &cli::ClientFetchArgs,
    grant: Option<&CapabilityGrant>,
    signer: &Arc<PrivateKeySigner>,
    voucher_dom: &Eip712Domain,
    admitted: Vec<NodeCandidate>,
    coverage_by_node: &HashMap<PublicKey, decdn_protocol::Coverage>,
    relays: &[RelayUrl],
    hash: [u8; 32],
    output: &Path,
    progress: Option<&ProgressCallback>,
    open_lock: Option<&tokio::sync::Mutex<()>>,
) -> anyhow::Result<Option<u64>>
where
    P: alloy::providers::Provider + Clone,
{
    let Some((first_candidate, rest_candidates)) = admitted.split_first() else {
        return Ok(None);
    };

    // Learn `total_bytes` from a throwaway header-only open against the first
    // admitted holder — the same handshake `drive_fetch` performs (no voucher is
    // signed, so it pays nothing). This also opens/reuses that holder's pool,
    // which the lane built below reuses, so the probe is not wasted work.
    let first = build_multi_lane(
        deps,
        grant,
        signer,
        voucher_dom,
        first_candidate,
        relays,
        open_lock,
    )
    .await?;
    let probe_target = multi_source_target(first_candidate, relays);
    // Peer-store bookkeeping for this header probe (#1906-series): the only
    // network round trip `try_multi_source_fetch_from_admitted` itself makes —
    // every other admitted lane's opens happen inside `multi_source_fetch`'s
    // scheduler, out of this function's view, so only the first candidate gets a
    // stream-derived sample/failure here. Best-effort throughout (`let _ =`).
    let peer_store = decdn_client_pull::PeerStore::open(&deps.chain.data_dir);
    let peer_store_cfg = decdn_client_pull::StoreConfig::default();
    let (header, first_pull) = {
        let ctx = first
            .ctx
            .lock()
            .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?
            .clone();
        match open_progressive_pull(
            deps.endpoint,
            probe_target,
            &ctx,
            Arc::clone(&first.ledger),
            deps.slash_dom,
            first.provider,
            hash,
            deps.namespace_id,
            0,
            micros_now(),
            deps.max_rate_per_mb,
            deps.deadlines,
            0,
            None,
        )
        .await
        {
            Ok(opened) => opened,
            Err(err) => {
                let _ = peer_store.record_failure(&first.node_id, now_secs_cli());
                return Err(match first.ctx.lock() {
                    Ok(guard) => annotate_unbound_cache_miss(err, &guard),
                    Err(_) => err,
                });
            }
        }
    };
    let _ = peer_store.record_sample(
        &first.node_id,
        header.ttfb_ms,
        header.rate_per_mb,
        now_secs_cli(),
        &peer_store_cfg,
    );
    let total_bytes = header.total_bytes;
    drop(first_pull);

    // The authoritative size gate, on the header the holder actually served:
    // below the floor a single fast holder already saturates the downlink, so
    // fan-out is pure overhead — fall through to single-source.
    if !should_multi_source(
        common.multi_source_enabled(),
        total_bytes,
        common.multi_source_min_bytes,
        admitted.len(),
    ) {
        eprintln!(
            "multi-source: not engaging — the blob is {total_bytes} bytes, below the {} byte \
             fan-out floor (--multi-source-min-bytes)",
            common.multi_source_min_bytes
        );
        return Ok(None);
    }

    // Build the rest of the lanes (the first is already built + probed).
    let mut lanes = vec![first];
    for candidate in rest_candidates {
        lanes.push(
            build_multi_lane(
                deps,
                grant,
                signer,
                voucher_dom,
                candidate,
                relays,
                open_lock,
            )
            .await?,
        );
    }

    // The `.partial` store beside `output`, keyed on `(hash, total_bytes)`; a
    // prior partial resumes and only the missing ranges are re-pulled.
    let (store_dir, stem) = ranged_store_location(output)?;
    let ranged_store = ClientRangedStore::open_or_create(&store_dir, &stem, hash, total_bytes)
        .map_err(|e| anyhow::anyhow!("open ranged store for {}: {e}", output.display()))?;

    // One shared funder over the single pool (top-up escrows into the one deposit
    // every lane draws on). The scheduler gates every lane on the aggregate
    // remaining, so a reactive top-up heals the shared pool for all of them.
    let funder = CliFunder {
        contract: deps.contract,
        rpc: deps.rpc,
        store: deps.store,
        owner: deps.self_address,
        pool_id: lanes.first().map_or(PoolId::ZERO, |l| l.pool_id),
        token: deps.token,
        payment_pool_addr: deps.chain.payment_pool,
        max_approve: deps.chain.max_approve,
    };
    let pacer = BudgetPacer::new();
    let drive_config = DriveConfig::cli(deps.chain.working_deposit);
    let ms_config = MultiSourceConfig {
        max_sources: common.max_sources,
        unit_deadline: Duration::from_millis(common.unit_deadline_ms),
    };

    // Borrow each lane's owned `PeerSource` into a scheduler `SourceLane`, cloning
    // its `ctx`/`ledger` handles. `lanes` outlives `source_lanes`.
    let num_blocks = decdn_protocol::num_blocks(total_bytes);
    let source_lanes: Vec<SourceLane<'_, PeerSource<'_>>> = lanes
        .iter()
        .map(|l| SourceLane {
            source: &l.source,
            ctx: Arc::clone(&l.ctx),
            ledger: Arc::clone(&l.ledger),
            coverage: lane_coverage(coverage_by_node, l.node_id, num_blocks),
        })
        .collect();

    let fetch_result = multi_source_fetch(
        &ranged_store,
        &source_lanes,
        &pacer,
        &funder,
        hash,
        0,
        total_bytes,
        &drive_config,
        &ms_config,
        progress,
    )
    .await;

    // Persist every lane's voucher watermark BEFORE anything can return early:
    // the bytes each lane delivered are paid for whatever the fetch as a whole
    // did.
    for (lane, vprogress) in multi_lane_watermarks(deps.self_address, &lane_watermarks(&lanes)) {
        persist_watermark(
            deps.store,
            deps.self_address,
            lane.pool_id,
            lane,
            &vprogress,
        );
    }

    fetch_result.map_err(|err| match lanes.first().map(|l| l.ctx.lock()) {
        Some(Ok(guard)) => annotate_unbound_cache_miss(err, &guard),
        _ => err,
    })?;

    anyhow::ensure!(
        ranged_store
            .is_complete()
            .await
            .map_err(|e| anyhow::anyhow!("check assembled blob {}: {e}", output.display()))?,
        "multi-source fetch of {} reported success but the assembled blob is incomplete",
        output.display()
    );
    ranged_store
        .finalize()
        .await
        .map_err(|e| anyhow::anyhow!("finalize assembled blob {}: {e}", output.display()))?;
    Ok(Some(total_bytes))
}

/// One lane's persisted-watermark inputs, lifted out of [`MultiLane`] so the
/// per-lane settlement rule is a pure function the tests can drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LaneWatermark {
    pool_id: PoolId,
    provider: Address,
    /// The lane's persisted prior amount — the baseline the advance is computed
    /// against.
    prior_amount: U256,
    /// The lane's ARMED cumulative: what the lane owes if every voucher it put
    /// on the wire was received.
    settlement: Cumulative,
}

/// Snapshot each lane's watermark inputs.
fn lane_watermarks(lanes: &[MultiLane<'_>]) -> Vec<LaneWatermark> {
    lanes
        .iter()
        .map(|l| LaneWatermark {
            pool_id: l.pool_id,
            provider: l.provider,
            prior_amount: l.prior_amount,
            settlement: l.ledger.settlement(),
        })
        .collect()
}

/// Pair each lane's own `LaneKey` with the watermark to persist for it.
///
/// Every multi-source lane settles at its ARMED cumulative — [`select_watermark`]'s
/// ambiguous-failure branch — rather than branching on the fetch's outcome the
/// way the single-source path does. The fetch result is ONE outcome shared by
/// every lane, but "did this lane's last voucher land?" is a PER-LANE question,
/// and on the multi-source path a successful fetch routinely leaves a lane
/// armed-above-committed: a tail steal drops the victim's `fill_gap` future
/// wherever it is parked, including inside the voucher exchange that `issue`
/// deliberately arms before sending. Settling that lane at `committed` on the
/// fetch's `Ok` persists a cumulative BELOW what the node can redeem, and the
/// next fetch on that lane signs a cumulative the upstream already holds —
/// rejected as a regression.
///
/// Settling high costs nothing on the clean path: with nothing armed,
/// `settlement()` equals `committed()`.
fn multi_lane_watermarks(
    signer: Address,
    lanes: &[LaneWatermark],
) -> Vec<(LaneKey, VoucherProgress)> {
    lanes
        .iter()
        .map(|l| {
            (
                LaneKey {
                    pool_id: l.pool_id,
                    signer,
                    provider: l.provider,
                },
                VoucherProgress::from_cumulative(l.settlement, l.prior_amount),
            )
        })
        .collect()
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
            let ToppedUpPool { credited, tx } =
                top_up(self.contract, self.pool_id, additional).await?;
            // Grade here rather than handing the escrowed-but-untracked
            // outcomes back for the driver to bail on. The driver treats them
            // as terminal either way, but `DepositOutcome` has nowhere to carry
            // the tx or the pool, so its bail names neither — and this is the
            // one leg where the escrow has already moved.
            let effect = topped_up_effect(self.pool_id, credited);
            let new_deposit = grade_deposit_credit(
                self.store.add_deposit(self.owner, self.pool_id, credited),
                &effect,
                tx,
            )?;
            Ok(DepositOutcome::Added(new_deposit))
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

/// Time constant for the smoothed delivery rate (seconds). Larger holds the
/// readout steadier across bursty arrival; smaller tracks real speed changes
/// faster. A few seconds keeps the number legible without lagging a genuine
/// slowdown for long.
const RATE_SMOOTHING_TAU_SECS: f64 = 3.0;

/// Rolling delivery-rate estimate behind the progress bar's `{msg}`, and the
/// running totals the end-of-fetch summary reads back.
///
/// The bar's positions are cumulative **delivered content bytes** — the driver
/// reports `base_present + received` against the blob's content size, where
/// `received` is the ranged store's verified-leaf count — not wire bytes.
///
/// Each `set_position` on the bar is bursty — many chunks land in one instant,
/// then a gap — so a naive `delta / dt` per callback spikes and collapses. This
/// holds a time-weighted exponential moving average instead: each sample folds
/// in with weight `1 - exp(-dt / tau)`, so the estimate is stable regardless of
/// how unevenly callbacks are spaced.
#[derive(Default)]
struct SpeedState {
    /// Instant and cumulative-byte position at the first observed sample. The
    /// summary measures elapsed and bytes-moved from here, so a resumed fetch
    /// (which starts at a non-zero `base_present`) reports only what this run
    /// actually transferred rather than dividing already-present bytes by this
    /// run's short window.
    started: Option<(Instant, u64)>,
    /// Instant and cumulative delivered content bytes at the previous sample.
    last: Option<(Instant, u64)>,
    /// Smoothed rate in bytes/sec. `None` until the second sample gives a `dt`.
    ewma_bps: Option<f64>,
}

/// Widen a byte count to `f64` for rate arithmetic. A single transfer never
/// approaches 2^53 bytes, so the precision the cast lint guards against is not
/// at risk here.
#[expect(
    clippy::cast_precision_loss,
    reason = "byte counts stay far below f64's 2^53 exact-integer ceiling"
)]
const fn bytes_as_f64(n: u64) -> f64 {
    n as f64
}

/// Format a non-negative bytes/sec rate as e.g. `12.3 MiB/s`. A rate at or
/// below zero (no data yet, or a stall) renders as `--`.
fn fmt_rate(bps: f64) -> String {
    if bps.is_finite() && bps >= 1.0 {
        // The rate is a small non-negative value; clamp before the cast so the
        // `HumanBytes` argument can never wrap or lose sign.
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "bps is finite and >= 1.0 here; the cast cannot wrap or go negative"
        )]
        let whole = bps.min(bytes_as_f64(u64::MAX)) as u64;
        format!("{}/s", indicatif::HumanBytes(whole))
    } else {
        "--".to_string()
    }
}

/// Estimate remaining time from the smoothed rate, formatted like `ETA 8s`.
/// Below a usable rate it reports `ETA --` rather than a divide-by-tiny blowup.
fn fmt_eta(remaining_bytes: u64, bps: f64) -> String {
    if bps >= 1.0 {
        // Clamp the projection so `from_secs_f64` never overflows `Duration`.
        let secs = (bytes_as_f64(remaining_bytes) / bps).clamp(0.0, 8.64e7);
        format!(
            "ETA {}",
            indicatif::HumanDuration(Duration::from_secs_f64(secs))
        )
    } else {
        "ETA --".to_string()
    }
}

/// Reads the transfer duration and bytes moved this run back from a
/// [`SpeedState`] after the bar finishes, for the end-of-fetch summary.
#[derive(Clone)]
struct DeliveryMeter {
    state: Arc<Mutex<SpeedState>>,
}

impl DeliveryMeter {
    /// `(elapsed across this run, content bytes this run transferred)`, or
    /// `None` if no byte ever arrived (a failure before delivery) or the lock is
    /// poisoned. Both are measured from the first observed sample, so a resumed
    /// fetch excludes the already-present `base_present` bytes it did not move.
    fn summary(&self) -> Option<(Duration, u64)> {
        let state = self.state.lock().ok()?;
        let (start_at, start_bytes) = state.started?;
        let (last_at, last_bytes) = state.last?;
        Some((
            last_at.saturating_duration_since(start_at),
            last_bytes.saturating_sub(start_bytes),
        ))
    }
}

/// The `decdn fetch` delivery progress bar, the callback that drives it, and a
/// [`DeliveryMeter`] the caller reads after `finish_and_clear` for the terminal
/// summary (#1118). The callback both advances the bar and folds each update
/// into the shared [`SpeedState`] so `{msg}` shows a steady rate/ETA.
fn delivery_progress() -> (
    indicatif::ProgressBar,
    impl Fn(u64, u64) + 'static,
    DeliveryMeter,
) {
    let bar = new_progress_bar();
    // `ProgressBar` is `Arc`-backed, so the clone the callback owns drives the
    // same bar the caller clears. The callback must be `'static`
    // (`ProgressCallback`), hence the owned clone rather than a borrow.
    let cb_bar = bar.clone();
    // `expected` is constant across the pull, so set the bar length once (it
    // takes a write lock) rather than on every chunk in the hot receive loop.
    let length_set = std::sync::atomic::AtomicBool::new(false);
    let state = Arc::new(Mutex::new(SpeedState::default()));
    let cb_state = Arc::clone(&state);
    let on_progress = move |received: u64, expected: u64| {
        if !length_set.swap(true, std::sync::atomic::Ordering::Relaxed) {
            cb_bar.set_length(expected);
        }
        cb_bar.set_position(received);

        let now = Instant::now();
        // A poisoned lock only costs this one rate update; the bar still advances.
        if let Ok(mut s) = cb_state.lock() {
            s.started.get_or_insert((now, received));
            if let Some((prev_at, prev_bytes)) = s.last {
                let dt = now.saturating_duration_since(prev_at).as_secs_f64();
                // Skip same-instant callbacks (a burst): they carry no usable
                // `dt` and would divide by ~zero into a spike.
                if dt > 0.0 {
                    let inst = bytes_as_f64(received.saturating_sub(prev_bytes)) / dt;
                    let alpha = 1.0 - (-dt / RATE_SMOOTHING_TAU_SECS).exp();
                    // Seed from 0, not `inst`: on the first sample a tiny `dt`
                    // makes `inst` huge, but `alpha * inst = (1 - exp(-dt/tau)) *
                    // (delta/dt) -> delta/tau` as `dt -> 0`, so the estimate
                    // stays bounded instead of spiking, then converges upward.
                    let prev = s.ewma_bps.unwrap_or(0.0);
                    s.ewma_bps = Some(prev + alpha * (inst - prev));
                }
            }
            s.last = Some((now, received));
            let bps = s.ewma_bps.unwrap_or(0.0);
            cb_bar.set_message(format!(
                "({}, {}) ",
                fmt_rate(bps),
                fmt_eta(expected.saturating_sub(received), bps)
            ));
        }
    };
    (bar, on_progress, DeliveryMeter { state })
}

/// Print the terminal line after a successful fetch: content bytes, and — when
/// the [`DeliveryMeter`] captured any delivery — the elapsed time and average
/// transfer rate over the content bytes this run moved. Falls back to the bare
/// byte/output line when nothing was delivered on this leg (e.g. a fully
/// resumed transfer that re-pulled no bytes).
fn print_fetch_summary(content_bytes: u64, meter: &DeliveryMeter, output: &Path) {
    match meter.summary() {
        Some((elapsed, moved_bytes)) if elapsed > Duration::ZERO && moved_bytes > 0 => {
            let avg_bps = bytes_as_f64(moved_bytes) / elapsed.as_secs_f64();
            println!(
                "fetched {content_bytes} bytes in {} (avg {}) -> {}",
                indicatif::HumanDuration(elapsed),
                fmt_rate(avg_bps),
                output.display()
            );
        }
        _ => println!("fetched {content_bytes} bytes -> {}", output.display()),
    }
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
            let ToppedUpPool { credited, tx } = top_up(contract, state.pool_id, additional).await?;
            // The USDC is escrowed the moment `topUp` mines. A local credit that
            // does not land leaves the deposit untracked, and continuing would
            // fetch on a `state.deposit` that understates the chain — so the
            // low-water check re-fires on every later fetch while nobody
            // reconciles the escrow. No bytes have been paid for on *this*
            // entry yet — `bundle pull` calls this once per entry, so earlier
            // entries may already be paid for and written — and only the escrow
            // moved, so failing here strands nothing in flight. The reactive
            // mid-fetch leg takes the same disposition (`CliFunder::top_up`).
            let effect = topped_up_effect(state.pool_id, credited);
            grade_deposit_credit(
                store.add_deposit(self_address, state.pool_id, credited),
                &effect,
                tx,
            )?;
            // The credit committed, so the row must be there. A `None` here
            // means it vanished between the write and this read — the local
            // record did not survive, which is the same untracked-escrow
            // condition, not something to paper over with the pre-top-up
            // snapshot.
            store
                .get_by_pool_id(state.pool_id)?
                .ok_or_else(|| escrowed_but_untracked(&effect, tx, "the credited row vanished"))?
        };
        // Effectively uncapped: a self-owned capability delegates spend to the
        // owner's own key, so the pool deposit — not the capability cap — is the
        // real spending bound. Capping at `state.deposit` here would freeze the
        // on-chain cap at the pre-top-up deposit (`_registerCapability` is
        // idempotent past first redemption) and reject spend past it. The cap is
        // `SELF_CAPABILITY_CAP` (`u64::MAX` µUSDC, ~$18.4T), NOT `U256::MAX`: the
        // `PaymentPool`'s `spendingCap` is a `uint64`, so a `U256::MAX` cap hashes
        // to a word the contract can never reconstruct and every redemption of this
        // lane's vouchers reverts, silently stranding the node's earnings (and,
        // because the node's exhaustion gate keys on `deposit − totalRedeemed`,
        // starving the reactive top-up path). Matches `open_pool`'s self-capability.
        let capability = issue_self_capability(
            signer.as_ref(),
            state.pool_id,
            SELF_CAPABILITY_CAP,
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
    store
        .record(&opened.state)
        .map_err(|e| escrowed_but_untracked("buyer pool opened", opened.tx, e))?;
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
            rediscover: false,
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
            keystore_password_file: None,
            data_dir: Some(PathBuf::from("/tmp/d")),
            working_deposit_micro_usdc: None,
            max_blob_mb: 1024,
            max_rate_per_mb: 0,
            stall_timeout_ms: 30_000,
            min_throughput_bps: 4096,
            timeout_ms: 3_600_000,
            capability: None,
            capability_file: None,
        }
    }

    fn config(body: &str) -> FileConfig {
        toml::from_str(body).expect("parse test config")
    }

    /// The password file is CLI/env-only — absent unless the operator passes
    /// the flag, and carried through verbatim when they do. An absolute path
    /// keeps the assertion off the ambient `$HOME` that `expand_tilde` reads.
    #[test]
    fn keystore_password_file_flows_through_and_defaults_to_none() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n\
             slash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
        );
        assert!(
            resolve_chain(&common(), &file)
                .unwrap()
                .keystore_password_file
                .is_none()
        );

        let mut a = common();
        a.keystore_password_file = Some(PathBuf::from("/abs/pw.txt"));
        assert_eq!(
            resolve_chain(&a, &file).unwrap().keystore_password_file,
            Some(PathBuf::from("/abs/pw.txt"))
        );
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

    /// `spawn_harvest`/`NodeCandidate` are `pub(crate)`, unreachable from an
    /// integration test in `crates/cli/tests/` — so this lives in-crate
    /// instead of `crates/cli/tests/peer_store_harvest.rs`. The test awaits
    /// the returned `JoinHandle` (never done on the real fetch path) purely
    /// for determinism.
    fn harvest_key(b: u8) -> PublicKey {
        iroh::SecretKey::from_bytes(&[b; 32]).public()
    }

    fn harvest_candidate(b: u8) -> NodeCandidate {
        NodeCandidate {
            node_id: harvest_key(b),
            eth_address: Address::from([b; 20]),
            region_hint: None,
            multiaddrs: Bytes::new(),
        }
    }

    #[tokio::test]
    async fn harvest_persists_identity_and_stats() {
        let dir = tempfile::tempdir().expect("tempdir");
        let regs = vec![harvest_candidate(1), harvest_candidate(2)];
        let probed = vec![(harvest_key(1), 42.0_f64, 9_u64)];
        let handle = spawn_harvest(dir.path(), regs, probed);
        handle.await.expect("harvest task join");

        let store = decdn_client_pull::PeerStore::open(dir.path());
        assert!(store.get(&harvest_key(2)).is_some());
        let r = store.get(&harvest_key(1)).expect("probed peer persisted");
        assert_eq!(r.latency_ms, Some(42.0));
        assert_eq!(r.rate_per_mb, Some(9));
    }

    /// `store_fast_path` ranks selectable records by latency and excludes a
    /// suppressed one, and refuses to engage below `min_fresh_candidates`.
    /// It takes no [`Endpoint`], so it structurally cannot issue a network
    /// probe — this is the probe-less fast path itself, not merely tested
    /// without one.
    #[test]
    fn fast_path_needs_min_fresh_and_ranks_by_latency() -> anyhow::Result<()> {
        let cfg = decdn_client_pull::StoreConfig::default();
        let dir = tempfile::tempdir()?;
        let store = decdn_client_pull::PeerStore::open(dir.path());
        let now = 10_000;
        for (b, lat) in [(1u8, 80.0), (2, 20.0), (3, 50.0)] {
            store.upsert_identity(&harvest_candidate(b), now)?;
            store.record_sample(&harvest_key(b), lat, 1, now, &cfg)?;
        }
        // A fourth, freshly-failed record: selectable would otherwise admit it,
        // but the failure suppression must exclude it.
        store.upsert_identity(&harvest_candidate(9), now)?;
        store.record_sample(&harvest_key(9), 5.0, 1, now, &cfg)?;
        store.record_failure(&harvest_key(9), now)?;

        let targets = store_fast_path(&store, &cfg, 4, now)
            .ok_or_else(|| anyhow::anyhow!("expected Some"))?;
        assert_eq!(targets.candidates.len(), 3);
        assert_eq!(targets.candidates[0].node_id, harvest_key(2)); // lowest latency first
        assert_eq!(targets.candidates[1].node_id, harvest_key(3));
        assert_eq!(targets.candidates[2].node_id, harvest_key(1));
        assert!(targets.coverage_by_node.is_empty());
        assert!(targets.probed_samples.is_empty());
        assert!(targets.size_hint.is_none());
        // The fast path is the one construction site that marks its result, so
        // the driver can fall through to discovery if every one of these
        // never-probed candidates turns out to be unreachable (ADR 037 § in-fetch
        // discovery fallback).
        assert!(targets.from_store_fast_path);

        // Only two selectable records -> below min_fresh_candidates -> None.
        let dir2 = tempfile::tempdir()?;
        let s2 = decdn_client_pull::PeerStore::open(dir2.path());
        for b in [1u8, 2] {
            s2.upsert_identity(&harvest_candidate(b), now)?;
            s2.record_sample(&harvest_key(b), 30.0, 1, now, &cfg)?;
        }
        assert!(store_fast_path(&s2, &cfg, 4, now).is_none());
        Ok(())
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

    // `retry_disposition` classification is unit-tested at its home in
    // `decdn_client_pull::retry`; the CLI reuses that exact function, so the
    // failover loop and the multi-source scheduler share one classifier.

    fn node_key(seed: u8) -> PublicKey {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn holder(seed: u8, rtt_ms: f64) -> discovery::Probed {
        holder_sized(seed, rtt_ms, None)
    }

    /// A holder whose probe reported (or withheld) a blob size — the hint the
    /// multi-source engagement gate applies its floor against.
    fn holder_sized(seed: u8, rtt_ms: f64, total_bytes: Option<u64>) -> discovery::Probed {
        discovery::Probed {
            candidate: NodeCandidate {
                node_id: node_key(seed),
                eth_address: Address::repeat_byte(seed),
                region_hint: None,
                multiaddrs: Bytes::new(),
            },
            rtt_ms,
            total_bytes,
            coverage: decdn_protocol::Coverage::empty(),
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

    /// With no holder, the failover order is the reachable bonded non-holders,
    /// nearest RTT first, as pull-through serve targets (#1911): a cold blob is
    /// origin-only with zero cache holders, so an empty holder set is the normal
    /// first-fetch state, not a terminal error. The fallback is independent of
    /// proxy warming — here warming is off — and reports no lead, size, or
    /// holder coverage, since nothing answered `has_blob`.
    #[test]
    fn failover_order_empty_holders_falls_back_to_non_holders_by_rtt() {
        let non_holders = vec![
            discovery::WarmingCandidate {
                node_id: node_key(31),
                eth_address: Address::repeat_byte(31),
                rtt_ms: 300.0,
            },
            discovery::WarmingCandidate {
                node_id: node_key(32),
                eth_address: Address::repeat_byte(32),
                rtt_ms: 100.0,
            },
        ];
        let out = super::failover_order(Vec::new(), &non_holders, warming_params(false));

        assert!(out.warming_lead.is_none(), "no holder to warm toward");
        assert!(out.size_hint.is_none(), "no holder reported a size");
        assert!(
            out.coverage_by_node.is_empty(),
            "non-holders carry no measured coverage"
        );
        let ids: Vec<_> = out.order.iter().map(|c| c.node_id).collect();
        assert_eq!(
            ids,
            vec![node_key(32), node_key(31)],
            "nearest non-holder (100ms) leads the pull-through fallback"
        );
        assert!(
            out.order.iter().all(|c| c.region_hint.is_none()),
            "pull-through candidates carry no region hint (spoof-proofing, ADR 037)"
        );
    }

    /// With neither a holder nor a reachable non-holder, the order is empty —
    /// the genuinely terminal case `probe_and_order` turns into an error before
    /// it ever calls this.
    #[test]
    fn failover_order_empty_when_no_holder_and_no_non_holder() {
        let out = super::failover_order(Vec::new(), &[], warming_params(true));
        assert!(out.order.is_empty());
        assert!(out.warming_lead.is_none());
    }

    /// `failover_order`'s `coverage_by_node` carries each holder's REAL measured
    /// coverage (#1506's B1) — not `Coverage::full` for every lane, which was
    /// the gap this task closes: disjoint per-holder coverage {block0}/{block1}
    /// survives into the map keyed by `node_id`, and a node that was never
    /// probed (e.g. a proxy) has no entry at all.
    #[test]
    fn failover_order_coverage_by_node_carries_each_holders_real_coverage() {
        let mut h1 = holder(1, 100.0);
        h1.coverage = decdn_protocol::Coverage::from_block_indices(2, [0].into_iter());
        let mut h2 = holder(2, 200.0);
        h2.coverage = decdn_protocol::Coverage::from_block_indices(2, [1].into_iter());

        let out = super::failover_order(vec![h1.clone(), h2.clone()], &[], warming_params(false));

        assert_eq!(out.coverage_by_node.get(&node_key(1)), Some(&h1.coverage));
        assert_eq!(out.coverage_by_node.get(&node_key(2)), Some(&h2.coverage));
        // Sanity: the two holders' coverage is genuinely different, not both
        // collapsed to the same (e.g. full) value.
        assert_ne!(h1.coverage, h2.coverage);
        // A node that was never probed has no entry.
        assert!(!out.coverage_by_node.contains_key(&node_key(99)));
    }

    /// [`lane_coverage`] is the read side of the same map: a holder with a
    /// measured (possibly partial) coverage gets exactly that back, while a
    /// node absent from the map — a proxy-warming source or the pinned
    /// `--node-id` path's empty map — falls back to a full holder rather than
    /// silently dropping out of the fan-out.
    #[test]
    fn lane_coverage_prefers_measured_coverage_and_falls_back_to_full() {
        let measured = decdn_protocol::Coverage::from_block_indices(4, [0, 2].into_iter());
        let mut map = HashMap::new();
        map.insert(node_key(1), measured.clone());

        assert_eq!(super::lane_coverage(&map, node_key(1), 4), measured);
        assert_eq!(
            super::lane_coverage(&map, node_key(2), 4),
            decdn_protocol::Coverage::full(4),
            "an unmeasured source (proxy / pinned --node-id) is treated as a full holder"
        );
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

    // ---- per-lane watermarks on the multi-source path ----

    fn lane_wm(pool: u8, provider: u8, prior: u64, bytes: u64, amount: u64) -> LaneWatermark {
        LaneWatermark {
            pool_id: PoolId::repeat_byte(pool),
            provider: Address::repeat_byte(provider),
            prior_amount: U256::from(prior),
            settlement: Cumulative {
                bytes: U256::from(bytes),
                amount: U256::from(amount),
            },
        }
    }

    /// Each lane's watermark is persisted under ITS OWN `LaneKey` and carries ITS
    /// OWN cumulative. Crossing the two — lane A's amount under lane B's key —
    /// strands both channels, and nothing else in the fetch path would notice.
    #[test]
    fn multi_lane_watermarks_pair_each_lane_with_its_own_key() {
        let signer = Address::repeat_byte(0x5E);
        let lanes = [lane_wm(1, 0xA1, 0, 100, 200), lane_wm(1, 0xB2, 0, 300, 400)];
        let out = super::multi_lane_watermarks(signer, &lanes);
        assert_eq!(out.len(), 2);

        assert_eq!(out[0].0.provider, Address::repeat_byte(0xA1));
        assert_eq!(out[0].0.signer, signer);
        assert_eq!(out[0].0.pool_id, PoolId::repeat_byte(1));
        assert_eq!(
            out[0].1.advanced(),
            Some((U256::from(100u64), U256::from(200u64))),
            "lane A carries lane A's cumulative"
        );

        assert_eq!(out[1].0.provider, Address::repeat_byte(0xB2));
        assert_eq!(
            out[1].1.advanced(),
            Some((U256::from(300u64), U256::from(400u64))),
            "lane B carries lane B's cumulative"
        );
    }

    /// A multi-source lane settles at its ARMED cumulative, never at `committed`.
    /// A tail steal drops the victim's `fill_gap` future wherever it is parked —
    /// including inside the voucher exchange `issue` deliberately arms before
    /// sending — and that is a routine event on a SUCCESSFUL fetch. Settling that
    /// lane low persists a cumulative below what the node can redeem, and the next
    /// fetch on the lane signs a value the upstream already holds: rejected as a
    /// regression.
    #[test]
    fn multi_lane_watermarks_settle_high_even_when_the_fetch_succeeded() {
        let signer = Address::repeat_byte(0x5E);
        // The armed cumulative sits ABOVE what was acked — the steal-cancelled
        // shape. `select_watermark(&Ok(()), ..)` would persist the lower one.
        let lanes = [lane_wm(1, 0xA1, 0, 300, 400)];
        let out = super::multi_lane_watermarks(signer, &lanes);
        assert_eq!(
            out[0].1.advanced(),
            Some((U256::from(300u64), U256::from(400u64))),
            "the armed (settlement) cumulative is what gets persisted"
        );
    }

    /// A lane that advanced nothing persists nothing: `advanced()` is `None`, and
    /// `persist_watermark` returns early rather than writing a no-op row.
    #[test]
    fn multi_lane_watermarks_report_no_advance_for_an_untouched_lane() {
        let signer = Address::repeat_byte(0x5E);
        let lanes = [lane_wm(1, 0xA1, 200, 0, 200)];
        let out = super::multi_lane_watermarks(signer, &lanes);
        assert_eq!(
            out[0].1.advanced(),
            None,
            "a lane at its prior amount has not advanced"
        );
    }

    // ---- the probe size hint that spares the engagement gate a throwaway open ----

    /// The gate's size hint is the LARGEST size any holder reported. The field is
    /// unsigned and only ever DECLINES fan-out, so taking the maximum keeps one
    /// node's understated hint from suppressing multi-source for the whole set.
    #[test]
    fn failover_order_takes_the_largest_reported_size_hint() {
        let holders = vec![
            holder_sized(1, 10.0, Some(1024)),
            holder_sized(2, 20.0, Some(64 * 1024 * 1024)),
            holder_sized(3, 30.0, None),
        ];
        let out = super::failover_order(holders, &[], warming_params(false));
        assert_eq!(out.size_hint, Some(64 * 1024 * 1024));
    }

    /// No holder reported a size: the gate has no hint and falls back to the
    /// authoritative header open.
    #[test]
    fn failover_order_has_no_size_hint_when_no_holder_reports_one() {
        let holders = vec![holder_sized(1, 10.0, None), holder_sized(2, 20.0, None)];
        let out = super::failover_order(holders, &[], warming_params(false));
        assert_eq!(out.size_hint, None);
    }

    /// A usable rate renders as a human `X/s`; a sub-1-byte/s rate (no data yet,
    /// or a stall) and any non-finite value both render as `--`.
    #[test]
    fn fmt_rate_shows_human_units_and_placeholder_below_one() {
        assert!(fmt_rate(2.0 * 1024.0 * 1024.0).ends_with("/s"));
        assert!(fmt_rate(2.0 * 1024.0 * 1024.0).contains("MiB"));
        assert_eq!(fmt_rate(0.0), "--");
        assert_eq!(fmt_rate(0.4), "--");
        assert_eq!(fmt_rate(f64::NAN), "--");
        assert_eq!(fmt_rate(f64::INFINITY), "--");
    }

    /// ETA divides remaining bytes by the smoothed rate; below a usable rate it
    /// reports `ETA --` rather than a divide-by-tiny blow-up, and a huge
    /// projection is clamped so `Duration::from_secs_f64` cannot overflow.
    #[test]
    fn fmt_eta_projects_and_guards_low_rate() {
        assert_eq!(fmt_eta(10 << 20, 0.0), "ETA --");
        assert_eq!(fmt_eta(10 << 20, 0.9), "ETA --");
        assert!(fmt_eta(10 << 20, 10.0 * 1024.0 * 1024.0).starts_with("ETA "));
        // A near-zero rate with bytes left must not panic on the clamp path.
        let _ = fmt_eta(u64::MAX, 1.0);
    }

    /// The summary measures elapsed and bytes-moved from the FIRST observed
    /// sample, not the final position — so a resumed fetch that began at a
    /// non-zero `base_present` reports only what this run actually transferred.
    #[test]
    fn summary_reports_delta_from_first_sample_not_absolute_position() {
        let t0 = Instant::now();
        let state = SpeedState {
            // Resumed at 40 MiB already present, ran for 2s to 60 MiB.
            started: Some((t0, 40 << 20)),
            last: Some((t0 + Duration::from_secs(2), 60 << 20)),
            ewma_bps: None,
        };
        let meter = DeliveryMeter {
            state: Arc::new(Mutex::new(state)),
        };
        let (elapsed, moved) = meter
            .summary()
            .expect("a delivered sample yields a summary");
        assert_eq!(elapsed, Duration::from_secs(2));
        assert_eq!(
            moved,
            20 << 20,
            "only this run's 20 MiB, not the 60 MiB total"
        );
    }

    /// No delivery ever observed (a failure before the first byte) yields no
    /// summary, so the caller falls back to the bare byte/output line.
    #[test]
    fn summary_is_none_before_any_delivery() {
        let meter = DeliveryMeter {
            state: Arc::new(Mutex::new(SpeedState::default())),
        };
        assert!(meter.summary().is_none());
    }
}
