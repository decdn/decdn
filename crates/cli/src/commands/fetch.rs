//! `decdn fetch` — standalone client-side paid pull of a single
//! content-addressed blob over `cdn/client/v1` (issues #391, #940).
//!
//! Turnkey paying sibling of [`super::probe`]: dial a node by explicit
//! `--node-id`/`--addr`/`--relay-url` (or auto-discover the holders, #936),
//! **auto-open-or-reuse** the caller's own `PaymentPool` deposit, take the
//! first size claim (the probe hint, or a header-only open when there is
//! none), and fetch it through the acquire loop
//! ([`decdn_client::acquire`]) across one [`decdn_client::PeerSource`] lane
//! per holder (signing cumulative vouchers, resuming each lane's persisted
//! watermark, verifying the `slash_sig` recovers to the provider, ADR 014 §1).
//! Every byte is bao-verified; each lane's new watermark is persisted, and the
//! file is promoted atomically. A holder that faults cools and returns; the
//! command ends on done, a fault only the user can fix, or the stop policy.
//!
//! Pool lifecycle: the caller's live pool in the persistent
//! [`RedbBuyerPoolStore`] is reused (the `(signer, provider)` lane watermark is
//! resumed) — and topped up on-chain via `topUp` (best effort) when its
//! pool-wide remaining deposit has run low, so a sustained series of fetches
//! isn't stranded;
//! otherwise one is opened on-chain (USDC `approve` if needed → `openPool`) via
//! the shared [`decdn_client::buyer_pool::open_pool`] kernel and recorded.
//! One pool fans out to every provider the caller pays (ADR 003) — there is no
//! per-provider open. The chain coordinates resolve flag >
//! `[blockchain]`/`[identity]` config > default.
//!
//! The chain/discovery/delivery seams (`resolve_chain`, `resolve_target_node`,
//! `probe_and_order`, `open_or_reuse_pool`, `build_multi_lane`/`DriveFetchDeps`,
//! `temp_in_parent`) are `pub(crate)` so `decdn bundle pull` (#391) reuses the
//! same acquire loop and reactive top-up across a bundle's many entries.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_client::buyer_pool::{
    LOW_WATER_DIVISOR, ProgressWrite, TopUpUnconfirmed, ToppedUpPool, WalletShortfall,
    ensure_allowance, escrowed_but_untracked, grade_deposit_credit, open_pool, refill_amount,
    self_owned_lane_ctx, top_up, topped_up_effect,
};
use decdn_client::driver::DriveConfig;
use decdn_client::source::{Funder, SourceFuture};
use decdn_client::{
    Connections, Cumulative, DownloadTarget, Downloader, Holder, LaneHandle, LaneLedgers,
    NoAffordableSource, NoCache, NoSourceHasBlob, PeerHealth, PeerSource, PoolContext, PoolLedger,
    ProgressClock, PullConfig, PullDeadlines, SignerCapDrained, StopPolicy, Streamer,
    UpstreamRefused, UpstreamVoucherRejected, VoucherProgress, sign_client_binding,
};

use super::cli_sources::CliSources;
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_pool::{AdvanceOutcome, BuyerPoolState, BuyerPoolStore, DepositOutcome};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity::{self, PasswordUse, load_signer};
use decdn_incentive::payment_pool::{PaymentPool, newest_solvent_owned_pool};

use anyhow::Context as _;
use decdn_incentive::rate::min_payment;
use decdn_incentive::{
    CapabilityGrant, Deployment, LaneKey, PoolId, bind_node_id_domain, slash_judge_domain,
    voucher_domain,
};
use decdn_protocol::client::StreamError;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};
use tokio_util::task::AbortOnDropHandle;

use decdn_client::UpstreamRateLimited;
use decdn_client::discovery::{self, NodeCandidate};
use decdn_client::endpoint as client_endpoint;
use decdn_client::probe::probe_once;
use decdn_client::provider;

use super::buyer_store::{ChainAdoption, DataDirSource, open_client_store_for_buy};
use super::fetch_timings::{FetchTimings, Mark};
use super::interrupt::{Interrupt, Interrupted};
use super::ordered_writes::OrderedWrites;
use super::tab_progress::TabProgress;

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
/// bar counts verified **content** bytes against the blob's `total_bytes` — the
/// unit the [`decdn_client::ProgressCallback`] reports — so length and
/// position share one unit and the bar fills to exactly 100%. The acquire loop
/// reports one monotonic total the lanes fold their per-leg deltas into, so
/// the bar never jumps between lanes' divergent local positions.
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
    // When client logging is enabled, share stderr with the tracing subscriber
    // through its `MultiProgress` so log lines do not corrupt the bar; a no-op
    // (returns the bar unchanged) on the default no-subscriber path. The bar is
    // attached before it is styled or ticked: either would draw a detached bar
    // straight to stderr, leaving an orphan line the container never clears.
    // `AndClear`: a bar dropped unfinished (a Ctrl-C) clears its line too.
    let bar = crate::logging::attach_progress_bar(
        indicatif::ProgressBar::new(0).with_finish(indicatif::ProgressFinish::AndClear),
    );
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
    /// File holding the keystore password, consulted after the
    /// `DECDN_KEYSTORE_PASSWORD` env var and before a prompt. CLI/env-only, like
    /// the daemon's `ResolvedBlockchain::keystore_password_file` — passwords do
    /// not belong in a config file even by reference.
    pub(crate) keystore_password_file: Option<PathBuf>,
    /// Directory holding the buyer-pool redb store and (by default) the
    /// keystore. Client-scoped (`~/.decdn/client`) unless an explicit
    /// `--data-dir`/`identity.data_dir` is given.
    pub(crate) data_dir: PathBuf,
    /// Which step of the ladder produced [`Self::data_dir`]. A node's data dir
    /// is acceptable when the operator named it and not when it merely fell out
    /// of the config file, so the guard needs the provenance, not just the path
    /// (#2082).
    pub(crate) data_dir_source: DataDirSource,
    /// Client region for region-first discovery ordering (`--region` >
    /// `identity.region`). `None` skips the ordering.
    pub(crate) region: Option<String>,
    /// Client region allowlist (`[client] region_allowlist`):
    /// narrows which peers `discover_provider` probes/discovers, never ranks
    /// them. Empty when the config omits `[client]` or the list, which is a
    /// no-op in [`discovery::select_candidates_filtered`]. Invalid entries
    /// (fail `Region::parse`) are dropped with a `tracing::warn!` in
    /// [`resolve_chain`] rather than failing config resolution — a typo'd
    /// region code shrinks the filter, it does not break the fetch.
    pub(crate) region_allowlist: Vec<decdn_protocol::Region>,
    /// Deposit to escrow when OPENING a pool, and the target a reused pool's
    /// proactive refill restores toward once it has served verified bytes.
    pub(crate) working_deposit: U256,
    pub(crate) max_approve: bool,
}

impl ResolvedChain {
    /// The `PaymentPool` deployment this buy runs against: the chain id and the
    /// contract address. A buyer row is reused only on this deployment.
    pub(crate) const fn deployment(&self) -> Deployment {
        Deployment {
            chain_id: self.chain_id,
            payment_pool: self.payment_pool,
        }
    }
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
                tracing::warn!(
                    "client.region_allowlist entry {code:?} is not a recognized \
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
    // Which step won is carried forward: on a node host `identity.data_dir`
    // points at the daemon's dir, and buying from there under the node's own
    // keystore is a refusal unless the operator asked for it by name (#2082).
    let (data_dir, data_dir_source) = args
        .data_dir
        .clone()
        .map(|p| (p, DataDirSource::Flag))
        .or_else(|| {
            file.identity
                .as_ref()
                .and_then(|i| i.data_dir.clone())
                .map(|p| (p, DataDirSource::Config))
        })
        .map(|(p, source)| (expand_tilde(&p), source))
        .or_else(|| cli::default_client_data_dir().map(|p| (p, DataDirSource::Default)))
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
        data_dir_source,
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
/// selection loop four ways: it never answered (unreachable), it kept shedding
/// the probe with `APP_ERR_RATE_LIMITED` (rate-limited — reachable, but refusing
/// this client's probe rate), it answered but its `slash_sig` did not recover
/// (unverifiable), or it answered but was unusable — a `has_blob`/coverage
/// mismatch that no honest responder produces. When any probe was unverifiable
/// the cause is almost always local configuration rather than missing content,
/// so that case gets its own message; otherwise the parenthetical accounts for
/// each of the other three separately. Reached only when there is neither a
/// cache holder nor a reachable non-holder to pull through — a `has_blob:false`
/// answer is a serve target, not a failure, so it never lands here.
fn no_serve_target_error(
    probe_count: usize,
    unreachable: usize,
    rate_limited: usize,
    unverifiable: usize,
) -> anyhow::Error {
    if unverifiable > 0 {
        return anyhow::Error::new(ResolveConfigFault(format!(
            "{unverifiable} of {probe_count} probed node(s) answered, but their probe \
             signatures did not recover to the operator address each is registered \
             under. That is usually local configuration rather than missing content: \
             check that blockchain.slash_judge_address and blockchain.chain_id match \
             the deployment these nodes registered against"
        )));
    }
    // With no unverifiable candidate, every candidate that neither stayed silent
    // nor shed the probe answered but was dropped as unusable (has_blob/coverage
    // mismatch) — name each count so the message never implies a silent,
    // wholly-unreachable set when some replied.
    let unusable = probe_count
        .saturating_sub(unreachable)
        .saturating_sub(rate_limited);
    let shed = if rate_limited > 0 {
        format!(", {rate_limited} rate-limited this client's probes")
    } else {
        String::new()
    };
    anyhow::anyhow!(
        "none of the {probe_count} probed node(s) could serve the blob \
         ({unreachable} did not answer{shed}, {unusable} answered but were unusable)"
    )
}

/// The waits between probe attempts on a candidate that sheds the probe with
/// `APP_ERR_RATE_LIMITED`. ADR 005's per-peer limit refills one probe every
/// 200 ms, so these clear the burst that concurrent entries of one bundle pull
/// send at its start. A candidate that still sheds after the last wait counts
/// as rate-limited, never as silent.
const PROBE_SHED_BACKOFFS_MS: [u64; 3] = [250, 500, 1000];

/// Probe one candidate, probing it again after each [`PROBE_SHED_BACKOFFS_MS`]
/// wait while it sheds the probe. Any other result — an answer, a timeout, a
/// transport failure — returns at once.
async fn probe_candidate(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hash: [u8; 32],
    timestamp_us: u64,
) -> anyhow::Result<(
    decdn_protocol::message::ProbeResponse,
    decdn_protocol::ProbeResponseExt,
    decdn_client::probe::ProbeRtt,
)> {
    let mut backoffs = PROBE_SHED_BACKOFFS_MS.iter();
    loop {
        let res = probe_once(
            endpoint,
            target.clone(),
            hash,
            timestamp_us,
            Duration::from_millis(SELECT_PROBE_TIMEOUT_MS),
        )
        .await;
        match (&res, backoffs.next()) {
            (Err(e), Some(&ms)) if probe_shed(e) => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            (Err(e), None) if probe_shed(e) => {
                tracing::info!(
                    "{} kept rate-limiting this client's probe: {e:#}",
                    target.id
                );
                return res;
            }
            _ => return res,
        }
    }
}

/// Whether a failed probe was the node shedding it with `APP_ERR_RATE_LIMITED`.
fn probe_shed(err: &anyhow::Error) -> bool {
    err.downcast_ref::<UpstreamRateLimited>().is_some()
}

/// How long a probe round keeps collecting answers after the first verified
/// holder answers. A holder that answers later is about 125 ms farther away,
/// still connecting, or still waiting for hole punching to select a direct
/// path (`probe_once` waits up to 500 ms for one after its exchange), and would
/// not lead the order. One slow connect would otherwise hold the whole round
/// for up to the probe timeout.
const PROBE_SETTLE_AFTER_HOLDER: Duration = Duration::from_millis(250);

/// Run `probes` concurrently and collect their outcomes until every probe has
/// answered or the round's deadline passes. The first outcome `is_holder`
/// accepts sets the deadline `grace` later. With `cold_grace` set and no
/// holder yet, the first outcome `is_answer` accepts sets it `cold_grace`
/// later, and a holder inside that window still ends the round `grace` after
/// it. Returns the collected outcomes, in answer order, and the probes still
/// pending. A zero `grace` returns at the first holder with every outcome
/// already ready: `timeout_at` polls the stream before it checks the deadline.
/// With no holder and no `cold_grace`, the round waits for every probe and the
/// tail is empty.
async fn settle_probes<T, F>(
    probes: impl IntoIterator<Item = F>,
    grace: Duration,
    cold_grace: Option<Duration>,
    is_holder: impl Fn(&T) -> bool,
    is_answer: impl Fn(&T) -> bool,
) -> (Vec<T>, futures_util::stream::FuturesUnordered<F>)
where
    F: std::future::Future<Output = T>,
{
    use futures_util::StreamExt as _;
    let mut pending: futures_util::stream::FuturesUnordered<F> = probes.into_iter().collect();
    let mut settled = Vec::with_capacity(pending.len());
    let mut deadline: Option<tokio::time::Instant> = None;
    let mut holder_seen = false;
    loop {
        let next = match deadline {
            None => pending.next().await,
            Some(at) => match tokio::time::timeout_at(at, pending.next()).await {
                Ok(next) => next,
                Err(_) => break,
            },
        };
        let Some(outcome) = next else { break };
        let now = tokio::time::Instant::now();
        if !holder_seen && is_holder(&outcome) {
            holder_seen = true;
            let at = now + grace;
            deadline = Some(deadline.map_or(at, |cold| cold.min(at)));
        } else if deadline.is_none()
            && let Some(cold) = cold_grace
            && is_answer(&outcome)
        {
            deadline = Some(now + cold);
        }
        settled.push(outcome);
    }
    if !pending.is_empty() {
        tracing::debug!(
            pending = pending.len(),
            holder = holder_seen,
            "probe round settled; the rest continue as its tail"
        );
    }
    (settled, pending)
}

/// Log one probe's raw result at `debug`, with the time since the round started.
fn log_probe_result(
    cand: &NodeCandidate,
    res: &anyhow::Result<(
        decdn_protocol::message::ProbeResponse,
        decdn_protocol::ProbeResponseExt,
        decdn_client::probe::ProbeRtt,
    )>,
    started: std::time::Instant,
) {
    match res {
        Ok((resp, _, rtt)) => tracing::debug!(
            node = %cand.node_id,
            elapsed_ms = started.elapsed().as_millis(),
            rtt_ms = rtt.ms,
            rtt_direct = rtt.direct,
            has_blob = resp.body.has_blob,
            "probe answered"
        ),
        Err(error) => tracing::debug!(
            node = %cand.node_id,
            elapsed_ms = started.elapsed().as_millis(),
            error = %format_args!("{error:#}"),
            "probe failed"
        ),
    }
}

/// One probed candidate, verified and classified ([`classify_probe`]).
pub(crate) enum ProbeOutcome {
    /// A verified answer that holds the blob, with its peer-store sample.
    Holder(discovery::Probed, (PublicKey, f64, u64)),
    /// A verified answer that does not hold the blob, with its peer-store
    /// sample.
    NonHolder(discovery::WarmingCandidate, (PublicKey, f64, u64)),
    /// No answer.
    Unreachable,
    /// The candidate kept shedding the probe.
    RateLimited,
    /// The answer's `slash_sig` did not recover to the candidate's operator.
    Unverifiable,
    /// The answer's `has_blob` and coverage disagree.
    Unusable,
}

impl ProbeOutcome {
    const fn is_holder(&self) -> bool {
        matches!(self, Self::Holder(..))
    }

    /// A verified answer, holder or not.
    const fn is_answer(&self) -> bool {
        matches!(self, Self::Holder(..) | Self::NonHolder(..))
    }

    /// The peer-store sample of a verified answer.
    pub(crate) const fn sample(&self) -> Option<(PublicKey, f64, u64)> {
        match self {
            Self::Holder(_, sample) | Self::NonHolder(_, sample) => Some(*sample),
            Self::Unreachable | Self::RateLimited | Self::Unverifiable | Self::Unusable => None,
        }
    }
}

/// How a probe round ends once a verified holder answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeRound {
    /// Return at the first verified holder and keep the pending probes as
    /// the round's tail ([`LateProbes`]): the first round of a fetch, whose
    /// late holders join the running fetch.
    Stream,
    /// Collect for [`PROBE_SETTLE_AFTER_HOLDER`] after the first verified
    /// holder and drop the rest: a rediscovery.
    Settle,
}

impl ProbeRound {
    /// How long the round collects after the first verified holder.
    const fn grace(self) -> Duration {
        match self {
            Self::Stream => Duration::ZERO,
            Self::Settle => PROBE_SETTLE_AFTER_HOLDER,
        }
    }

    /// How long the round collects after its first verified answer while no
    /// holder has answered. A streamed round then starts on the non-holders
    /// in hand as pull-through targets (#1911), and a holder that answers
    /// later joins the running fetch. A settled round waits for every probe.
    const fn cold_grace(self) -> Option<Duration> {
        match self {
            Self::Stream => Some(PROBE_SETTLE_AFTER_HOLDER),
            Self::Settle => None,
        }
    }
}

/// The probes a streamed round left pending, with what deciding a late
/// answer needs.
pub(crate) struct LateProbes {
    /// The pending probes, each yielding its classified outcome.
    pub(crate) tail: std::pin::Pin<Box<dyn futures_util::Stream<Item = ProbeOutcome> + Send>>,
    /// The lowest RTT among the holders the round returned.
    pub(crate) best_holder_rtt_ms: f64,
    /// The fetch's proxy-warming knobs.
    pub(crate) warming: ProxyWarmingParams,
}

/// How a resolve probes: where it records its marks, and how its probe round
/// ends.
#[derive(Clone, Copy)]
pub(crate) struct ProbeOpts<'a> {
    /// The `decdn fetch` marks; `None` elsewhere.
    pub(crate) timings: Option<&'a FetchTimings>,
    /// How the probe round ends.
    pub(crate) round: ProbeRound,
}

/// A streamed round's [`LateProbes`], taken once by whoever adopts them.
#[derive(Default)]
pub(crate) struct LateSlot(std::sync::Mutex<Option<LateProbes>>);

impl LateSlot {
    /// A slot holding `late`.
    pub(crate) const fn new(late: LateProbes) -> Self {
        Self(std::sync::Mutex::new(Some(late)))
    }

    /// Take the late probes, leaving the slot empty.
    pub(crate) fn take(&self) -> Option<LateProbes> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

/// A probe round's outcomes, split by what [`probe_and_order`] does with each.
struct ProbeTally {
    /// The verified holders.
    holders: Vec<discovery::Probed>,
    /// Probed bonded nodes that answered `has_blob:false` — reachable, with a
    /// measured RTT, but not holding the blob in their cache store. They serve
    /// two roles: the proxy-warming candidate pool (ADR 037 § Candidate pool)
    /// when a holder exists but is distant, and — when NO holder answers — the
    /// pull-through serve targets a cold blob's first fetch bootstraps from
    /// (#1911), since `has_blob:false` from the cache store does not mean the
    /// node cannot serve via its own origin. Always collected, because the
    /// second role does not depend on warming being on.
    non_holders: Vec<discovery::WarmingCandidate>,
    /// `(node_id, rtt_ms, rate_per_mb)` for each verified responder, harvested
    /// into the peer store off the critical path (`spawn_harvest`, called from
    /// `discover_provider`).
    probed_samples: Vec<(PublicKey, f64, u64)>,
    /// The ways a candidate drops out, counted separately: the terminal error
    /// has to name the one that actually happened. A wrong
    /// `slash_judge_address` or `chain_id` makes EVERY honest node fail
    /// verification, and reporting that as "nobody holds the blob" sends the
    /// operator hunting for missing content instead of a local
    /// misconfiguration; a node shedding this client's probe rate is
    /// reachable, and reporting it as silent points at the network instead.
    unreachable: usize,
    rate_limited: usize,
    unverifiable: usize,
}

impl ProbeTally {
    fn of(outcomes: Vec<ProbeOutcome>) -> Self {
        let mut tally = Self {
            holders: Vec::new(),
            non_holders: Vec::new(),
            probed_samples: Vec::new(),
            unreachable: 0,
            rate_limited: 0,
            unverifiable: 0,
        };
        for outcome in outcomes {
            match outcome {
                ProbeOutcome::Holder(holder, sample) => {
                    tally.probed_samples.push(sample);
                    tally.holders.push(holder);
                }
                ProbeOutcome::NonHolder(candidate, sample) => {
                    tally.probed_samples.push(sample);
                    tally.non_holders.push(candidate);
                }
                ProbeOutcome::Unreachable => tally.unreachable += 1,
                ProbeOutcome::RateLimited => tally.rate_limited += 1,
                ProbeOutcome::Unverifiable => tally.unverifiable += 1,
                ProbeOutcome::Unusable => {}
            }
        }
        tally
    }
}

/// Verify `cand`'s probe result and classify it. Every response is verified
/// before it can influence the order (ADR 014 §1). A failure is requester-local
/// policy, with no reputation effect, because an unrecovered signature
/// attributes nothing to anyone.
fn classify_probe(
    cand: &NodeCandidate,
    res: anyhow::Result<(
        decdn_protocol::message::ProbeResponse,
        decdn_protocol::ProbeResponseExt,
        decdn_client::probe::ProbeRtt,
    )>,
    hash: [u8; 32],
    timestamp_us: u64,
    slash_domain: &alloy::sol_types::Eip712Domain,
) -> ProbeOutcome {
    let (resp, resp_ext, rtt) = match res {
        Ok(answer) => answer,
        Err(e) if probe_shed(&e) => return ProbeOutcome::RateLimited,
        Err(_) => return ProbeOutcome::Unreachable,
    };
    if let Err(e) = decdn_client::probe::verify_probe_response(
        &resp,
        cand.eth_address,
        slash_domain,
        hash,
        timestamp_us,
    ) {
        // A candidate silently vanishing from selection is exactly what the
        // operator needs told, so log why at `warn`.
        tracing::warn!(
            "dropping an unverifiable probe response from {}: {e}",
            cand.node_id
        );
        return ProbeOutcome::Unverifiable;
    }
    // #1506: `has_blob` and `coverage.is_empty()` are a biconditional by
    // construction on an honest responder. Neither field is signed, and an
    // inconsistency has no attributable author, so drop the candidate rather
    // than score it.
    if !resp_ext.consistent_with(resp.body.has_blob) {
        tracing::warn!(
            "dropping a probe response from {} with has_blob/coverage mismatch",
            cand.node_id
        );
        return ProbeOutcome::Unusable;
    }
    let rtt_ms = rtt.ms;
    // Every verified responder contributes a probe sample, holder or not.
    let sample = (cand.node_id, rtt_ms, resp.body.rate_per_mb);
    if resp.body.has_blob {
        ProbeOutcome::Holder(
            discovery::Probed {
                candidate: cand.clone(),
                rtt_ms,
                total_bytes: resp_ext.total_bytes,
                coverage: resp_ext.coverage,
            },
            sample,
        )
    } else {
        ProbeOutcome::NonHolder(
            discovery::WarmingCandidate {
                node_id: cand.node_id,
                eth_address: cand.eth_address,
                rtt_ms,
                multiaddrs: cand.multiaddrs.clone(),
            },
            sample,
        )
    }
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
/// `has_blob` and coverage off the response and pairs them with the measured
/// RTT; `rate_per_mb` is harvested into `probed_samples` for the peer store
/// only, never ranked on — the fetch pays the stream's own signed quote, gated
/// by `--max-rate-per-mb`. `has_blob` and the harvested rate are only
/// meaningful once the signature attributes them to the peer (coverage is
/// unsigned; the consistency check below ties it to `has_blob`): an unverified
/// quote is a claim no one is accountable for, so a node could seed the store
/// with a rate it never committed to. A response that fails is
/// dropped and its candidate skipped, exactly as for a timeout; it is
/// requester-local policy and never scored against the peer, since a signature
/// that does not recover attributes nothing to anyone.
///
/// Every candidate is probed on equal footing: opening cost is
/// provider-independent, because the caller has ONE pool that fans out to
/// every provider (ADR 003), so there is no per-provider "already funded"
/// distinction to prefer.
///
/// The round ends [`PROBE_SETTLE_AFTER_HOLDER`] after the first verified
/// holder answers, or when every probe has answered. A holder the cutoff drops
/// stays reachable through the fetch's later discovery.
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
    round: ProbeRound,
) -> anyhow::Result<ResolvedTargets> {
    let timestamp_us = micros_now();
    // Probe concurrently in one task. `probe_once`'s internal timeout bounds
    // each leg; a candidate that sheds the probe is probed again after a short
    // wait (`probe_candidate`). Each probe verifies and classifies its own
    // answer, so the round ends only on a verified holder. Each probe owns its
    // inputs, so a streamed round's pending probes outlive this call.
    let started = std::time::Instant::now();
    let probes = candidates.iter().cloned().map(|cand| {
        let target = probe_target(&cand, relay_hint.cloned());
        let endpoint = endpoint.clone();
        let slash_domain = slash_domain.clone();
        async move {
            let res = probe_candidate(&endpoint, target, hash, timestamp_us).await;
            log_probe_result(&cand, &res, started);
            classify_probe(&cand, res, hash, timestamp_us, &slash_domain)
        }
    });
    let (outcomes, tail) = settle_probes(
        probes,
        round.grace(),
        round.cold_grace(),
        ProbeOutcome::is_holder,
        ProbeOutcome::is_answer,
    )
    .await;

    let ProbeTally {
        holders,
        non_holders,
        probed_samples,
        unreachable,
        rate_limited,
        unverifiable,
    } = ProbeTally::of(outcomes);

    // Terminal only when NOTHING can serve — no holder AND no reachable
    // non-holder to pull through. An empty holder set alone is not terminal: a
    // cold blob is origin-only with zero cache holders, the normal first-fetch
    // state (#1911), so as long as one bonded node answered it is a serve target.
    if holders.is_empty() && non_holders.is_empty() {
        return Err(no_serve_target_error(
            candidates.len(),
            unreachable,
            rate_limited,
            unverifiable,
        ));
    }

    if holders.is_empty() {
        // No cache holder, but reachable non-holders can serve via pull-through.
        // Log why the fetch is talking to nodes that answered `has_blob:false`.
        tracing::info!(
            "no node that has answered holds the blob in cache; starting on {} reachable \
             bonded non-holder(s) as pull-through serve targets — a node serves an authorized \
             miss from its own origin (#1911), and a holder that answers later joins the fetch",
            non_holders.len()
        );
    }

    let size_hint = nearest_size_hint(&holders);
    let late = late_slot(round, tail, &holders, warming);
    let ordered = failover_order(holders, &non_holders, warming);
    if let Some((node_id, proxy_rtt, best_holder_rtt)) = ordered.warming_lead {
        tracing::info!(
            "proxy-warming: routing through nearer non-holder {node_id} ({proxy_rtt:.1}ms) \
             instead of the best holder ({best_holder_rtt:.1}ms) to seed a regional copy, \
             falling back to the holder if it declines (ADR 037)",
        );
    }
    Ok(ResolvedTargets {
        candidates: ordered.order,
        coverage_by_node: ordered.coverage_by_node,
        probed_samples,
        pinned: false,
        size_hint,
        late,
    })
}

/// A streamed round's pending probes as its [`LateSlot`], with the best RTT
/// among the `holders` it returned. Empty for a settled round or an empty
/// tail.
///
/// A task drives the probes from here on, so they keep running and timing
/// while the fetch unlocks its signer and opens its pool before anything reads
/// the tail. Their answers wait in a channel. The task stops when the tail
/// stream is dropped, so no probe outlives the fetch that adopted it.
fn late_slot<F>(
    round: ProbeRound,
    tail: futures_util::stream::FuturesUnordered<F>,
    holders: &[discovery::Probed],
    warming: ProxyWarmingParams,
) -> LateSlot
where
    F: std::future::Future<Output = ProbeOutcome> + Send + 'static,
{
    use futures_util::StreamExt as _;
    if round != ProbeRound::Stream || tail.is_empty() {
        return LateSlot::default();
    }
    let best_holder_rtt_ms = holders
        .iter()
        .map(|h| h.rtt_ms)
        .fold(f64::INFINITY, f64::min);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let task = AbortOnDropHandle::new(tokio::spawn(tail.for_each(move |outcome| {
        // A send fails only once the tail stream is gone, which aborts this
        // task too.
        let _ = tx.send(outcome);
        std::future::ready(())
    })));
    let answers = futures_util::stream::unfold((rx, task), |(mut rx, task)| async move {
        rx.recv().await.map(|outcome| (outcome, (rx, task)))
    });
    LateSlot::new(LateProbes {
        tail: Box::pin(answers),
        best_holder_rtt_ms,
        warming,
    })
}

/// The size hint of the nearest holder that gave one: the fetch's first size
/// claim ([`ResolvedTargets::size_hint`]).
fn nearest_size_hint(holders: &[discovery::Probed]) -> Option<u64> {
    holders
        .iter()
        .filter(|h| h.total_bytes.is_some())
        .min_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms))
        .and_then(|h| h.total_bytes)
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
    /// Each probed holder's measured [`decdn_protocol::Coverage`] (#1506),
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
/// off. `warming_lead` and `coverage_by_node` are both empty here,
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
                // Registry addresses ride along so a cold-order target dials
                // directly (ADR 001 § Node Discovery).
                multiaddrs: c.multiaddrs.clone(),
            })
            .collect();
        return FailoverOrder {
            order,
            warming_lead: None,
            coverage_by_node: HashMap::new(),
        };
    }
    holders.sort_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms));
    // Captured before `holders` is consumed into `order` below — each probed
    // holder's real coverage, for the multi-source lane builder (#1506).
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
        // Registry addresses ride along so a warming proxy dials directly
        // (ADR 001 § Node Discovery).
        multiaddrs: proxy.multiaddrs.clone(),
    });
    let order = proxies
        .chain(holders.iter().map(|h| h.candidate.clone()))
        .collect();
    FailoverOrder {
        order,
        warming_lead,
        coverage_by_node,
    }
}

/// Auto-discover the failover order to fetch `hash` from (#936): read the
/// active set from `CapacityBond`, take a random, region-first sample of
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
    probe: ProbeOpts<'_>,
) -> anyhow::Result<ResolvedTargets> {
    let mark_probe_start = || {
        if let Some(timings) = probe.timings {
            timings.mark(Mark::ProbeStart);
        }
    };
    // Probe-less fast path: when the store already holds enough fresh,
    // unsuppressed candidates, skip the network probe round entirely and rank
    // by the store's own EWMA latency. Falls through to today's bootstrap +
    // probe path whenever the store can't back a full candidate set — never a
    // new failure mode, only a possible extra probe round.
    let peer_store = decdn_client::PeerStore::open(&chain.data_dir);
    let store_cfg = decdn_client::StoreConfig::default();
    if !args.rediscover
        && let Some(targets) =
            store_fast_path(&peer_store, &store_cfg, args.max_sources, now_secs_cli())
    {
        return Ok(targets);
    }
    // Registry read-skip (identity-fresh, latency-stale): when the store still
    // holds enough recently-confirmed identities, build the candidate set from
    // them and re-probe for fresh RTT, issuing NO `getRegisteredNodes`. This
    // decouples "skip the registry read" (bounded by the 24h identity horizon)
    // from "skip the probe" (bounded by the 10-min latency TTL, the fast path
    // above). Falls through to the registry read when too few identities are
    // fresh. `--rediscover` forces the read (checked above alongside the fast
    // path).
    if !args.rediscover {
        let cached = identity_fresh_candidates(&peer_store, &store_cfg, now_secs_cli());
        let selected = select_with_widening(
            cached,
            chain.region.as_deref(),
            chain.region_allowlist.as_slice(),
            &store_cfg,
        );
        if selected.len() >= store_cfg.min_fresh_candidates {
            let warming = ProxyWarmingParams::from_args(args);
            let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
            mark_probe_start();
            let targets = probe_and_order(
                endpoint,
                &selected,
                relay_hint,
                hash,
                warming,
                &slash_dom,
                probe.round,
            )
            .await?;
            // Stats-only harvest, never identity — identity refreshes ONLY on a
            // real registry read (same reasoning as the `Bootstrap::Cached`
            // path): re-`upsert_identity` here would "confirm" identity against
            // the store itself, reset `identity_seen_at_secs`, and keep the 24h
            // refresh horizon from ever expiring.
            drop(spawn_harvest(
                &chain.data_dir,
                Vec::new(),
                targets.probed_samples.clone(),
            ));
            return Ok(targets);
        }
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
    // `decdn-client` returns the provenance rather than logging it itself; a
    // silently stale peer list is exactly what the operator needs told, so
    // surface it here at `warn`.
    if let Some(warning) = bootstrap.warning() {
        tracing::warn!("{warning}");
    }
    let all = bootstrap.into_peers();
    if all.is_empty() {
        anyhow::bail!("no active nodes in the CapacityBond registry at {capacity_bond}");
    }
    let allow: &[decdn_protocol::Region] = if is_live_registry {
        chain.region_allowlist.as_slice()
    } else {
        &[]
    };
    let selected = select_with_widening(all, chain.region.as_deref(), allow, &store_cfg);
    let warming = ProxyWarmingParams::from_args(args);
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
    mark_probe_start();
    let targets = probe_and_order(
        endpoint,
        &selected,
        relay_hint,
        hash,
        warming,
        &slash_dom,
        probe.round,
    )
    .await?;
    // Identity is harvested ONLY against a live registry read, and
    // `resolve_bootstrap` already does that: on a `Bootstrap::Live` read it
    // upserts+prunes identity for every returned node. So the harvest here
    // writes stats only and never identity. Two reasons:
    //   - On the `Bootstrap::Cached` outage path the returned peer set (`all`) IS
    //     the store's own surviving identities; re-`upsert_identity`-ing them would
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
/// every [`decdn_client::PeerRecord::selectable`] record by its
/// EWMA `latency_ms` (ascending, `None` sorts last), project each back to a
/// [`NodeCandidate`], and admit per-operator via
/// [`discovery::admit_sources`]. Returns `None` when fewer than
/// `cfg.min_fresh_candidates` records are selectable, or when admission still
/// leaves the set below that floor — either way the caller falls back to the
/// probe path unchanged. Takes no [`Endpoint`], so it structurally issues no
/// network probe.
fn store_fast_path(
    store: &decdn_client::PeerStore,
    cfg: &decdn_client::StoreConfig,
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
        .map(decdn_client::PeerRecord::as_candidate)
        .collect();
    let candidates = discovery::admit_sources(ordered, max_sources);
    if candidates.len() < cfg.min_fresh_candidates {
        return None;
    }
    Some(ResolvedTargets {
        candidates,
        coverage_by_node: HashMap::new(),
        probed_samples: Vec::new(),
        pinned: false,
        size_hint: None,
        late: LateSlot::default(),
    })
}

/// Gather the cached peers whose identity is fresh enough to build a candidate
/// set from without re-reading the registry: identity confirmed within
/// [`decdn_client::StoreConfig::identity_refresh_secs`], not
/// failure-suppressed, and not past the prune horizon. Latency freshness is NOT
/// required — this is exactly the identity-fresh/latency-stale case that
/// [`store_fast_path`] rejects; the caller re-probes these records for a fresh
/// RTT. Returns them projected to [`NodeCandidate`], unranked (the probe orders
/// them); empty when none qualify, letting the caller fall through to the
/// registry read.
fn identity_fresh_candidates(
    store: &decdn_client::PeerStore,
    cfg: &decdn_client::StoreConfig,
    now_secs: u64,
) -> Vec<NodeCandidate> {
    store
        .load_all()
        .into_iter()
        .filter(|r| {
            r.identity_fresh(now_secs, cfg)
                && !r.identity_prunable(now_secs, cfg)
                && !r.failure_suppressed(now_secs, cfg)
        })
        .map(|r| r.as_candidate())
        .collect()
}

/// The persisted pool's settlement token, or `None` when the owner has no pool
/// row yet. The token is `PaymentPool.usdc()`, immutable per contract, so a
/// persisted [`BuyerPoolState::token`] equals a fresh on-chain read — letting a
/// repeat fetch skip the `usdc()` `eth_call`. Only a first-ever pool falls
/// through to the on-chain read.
///
/// "Immutable per contract" is the whole premise, so the row has to be on the
/// deployment being read: a row from another `PaymentPool` deployment caches
/// that deployment's `usdc()`, and answering with it would approve and price
/// against the wrong token. Such a row reads as absent and the caller pays the
/// round-trip.
fn cached_pool_token(
    store: &RedbBuyerPoolStore,
    self_address: Address,
    deployment: Deployment,
) -> anyhow::Result<Option<Address>> {
    Ok(store
        .get_by_owner(self_address)?
        .filter(|state| state.is_on(deployment))
        .map(|state| state.token))
}

/// Region-filter a candidate set to at most [`discovery::SELECT_K`], then
/// progressively widen: a too-thin region allowlist must never starve the fetch,
/// so re-run unfiltered over the same set when the filtered pool falls below the
/// store's freshness floor. Shared by the live registry path and the
/// registry-read-skip path so both rank identically.
fn select_with_widening(
    candidates: Vec<NodeCandidate>,
    region: Option<&str>,
    allow: &[decdn_protocol::Region],
    cfg: &decdn_client::StoreConfig,
) -> Vec<NodeCandidate> {
    // Only a non-empty allowlist can trigger the widening re-run, so keep the
    // unfiltered copy only in that case — the common empty-allowlist path (no
    // filtering, no widening possible) does no extra allocation.
    let widen_fallback = (!allow.is_empty()).then(|| candidates.clone());
    let selected =
        discovery::select_candidates_filtered(candidates, region, discovery::SELECT_K, allow);
    if let Some(widened) = widen_fallback
        && selected.len() < cfg.min_fresh_candidates
    {
        return discovery::select_candidates_filtered(widened, region, discovery::SELECT_K, &[]);
    }
    selected
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
        let store = decdn_client::PeerStore::open(&dir);
        let cfg = decdn_client::StoreConfig::default();
        harvest(&store, &registry, probed, now_secs_cli(), &cfg);
    })
}

/// How many per-peer record writes one [`harvest`] made, and how many failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HarvestTally {
    /// Identity and sample writes attempted.
    writes: usize,
    /// Of those, the writes the store refused.
    failed: usize,
}

/// The body of [`spawn_harvest`]. Probe samples are the only input to the
/// peer store's latency ranking, so a refused write is logged, not dropped:
/// each one at `debug` with its peer, and once at `warn` when the store
/// refuses every record write, because the ranking then stays frozen.
fn harvest(
    store: &decdn_client::PeerStore,
    registry: &[NodeCandidate],
    probed: Vec<(PublicKey, f64, u64)>,
    now: u64,
    cfg: &decdn_client::StoreConfig,
) -> HarvestTally {
    let mut tally = HarvestTally {
        writes: 0,
        failed: 0,
    };
    let mut file = |node_id: &PublicKey, what: &str, written: anyhow::Result<()>| {
        tally.writes = tally.writes.saturating_add(1);
        if let Err(e) = written {
            tally.failed = tally.failed.saturating_add(1);
            tracing::debug!(%node_id, "could not record {what} in the peer store: {e:#}");
        }
    };
    for cand in registry {
        file(
            &cand.node_id,
            "an identity",
            store.upsert_identity(cand, now),
        );
    }
    for (node_id, rtt_ms, rate) in probed {
        let written = store.record_sample(&node_id, rtt_ms, rate, now, cfg);
        file(&node_id, "a probe sample", written);
    }
    if let Err(e) = store.prune_and_cap(now, cfg) {
        tracing::debug!("could not prune the peer store: {e:#}");
    }
    if tally.writes > 0 && tally.failed == tally.writes {
        tracing::warn!(
            writes = tally.writes,
            "the peer store refused every write of this probe round; \
             its latency ranking stays at its last state"
        );
    }
    tally
}

/// Seconds since the Unix epoch, saturating to 0 on a clock before the epoch
/// (never on this platform in practice) rather than panicking.
pub(crate) fn now_secs_cli() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A holder resolution that only the user's configuration can fix: a missing
/// `capacity_bond_address`, a malformed `--node-id` or `--provider-address`,
/// or probe signatures that do not recover to their operators.
#[derive(Debug)]
pub(crate) struct ResolveConfigFault(pub(crate) String);

impl std::fmt::Display for ResolveConfigFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ResolveConfigFault {}

/// The holders a command starts from. A resolution the configuration breaks
/// ([`ResolveConfigFault`]) ends the command. Any other failure, such as a
/// sole holder that is down at start, starts with no holder: the acquire loop
/// then discovers with backoff under the stop policy.
///
/// # Errors
///
/// A [`ResolveConfigFault`].
pub(crate) fn holders_or_none(
    resolved: anyhow::Result<ResolvedTargets>,
) -> anyhow::Result<ResolvedTargets> {
    match resolved {
        Ok(targets) => Ok(targets),
        Err(err) if err.downcast_ref::<ResolveConfigFault>().is_some() => Err(err),
        Err(err) => {
            tracing::warn!(
                error = %decdn_common::redact::sanitize_err_chain(&err),
                "no holder resolved yet; the fetch keeps looking"
            );
            Ok(ResolvedTargets {
                candidates: Vec::new(),
                coverage_by_node: HashMap::new(),
                probed_samples: Vec::new(),
                pinned: false,
                size_hint: None,
                late: LateSlot::default(),
            })
        }
    }
}

/// The ordered holder list plus what discovery learned about each holder.
pub(crate) struct ResolvedTargets {
    /// The candidates, nearest first (#1174).
    pub(crate) candidates: Vec<NodeCandidate>,
    /// Each probed holder's measured [`decdn_protocol::Coverage`] (#1506),
    /// keyed by `node_id` — what a holder's lane is planned against instead of
    /// assuming every candidate is a full holder. Empty on the pinned
    /// `--node-id` path, where nothing was probed; a lookup miss there (as
    /// everywhere else) reads as "no measured coverage", a full holder.
    pub(crate) coverage_by_node: HashMap<PublicKey, decdn_protocol::Coverage>,
    /// `(node_id, rtt_ms, rate_per_mb)` for each holder that answered a probe
    /// this fetch — harvested into the peer store.
    pub(crate) probed_samples: Vec<(PublicKey, f64, u64)>,
    /// The one candidate is the `--node-id` the user pinned. It counts as a
    /// holder, as a probe-reported holder does.
    pub(crate) pinned: bool,
    /// The blob size the nearest probed holder reported, an unsigned hint
    /// ([`discovery::Probed::total_bytes`]). `None` when nothing was probed
    /// (a pinned `--node-id`, the peer-store fast path) or no holder knew it.
    pub(crate) size_hint: Option<u64>,
    /// The probes a streamed round left pending ([`ProbeRound::Stream`]).
    /// Empty on every other path.
    pub(crate) late: LateSlot,
}

/// The one holder of a pinned `--node-id` at `provider`. Nothing was probed,
/// so it carries no size hint.
fn pinned_targets(node_id: PublicKey, provider: Address) -> ResolvedTargets {
    ResolvedTargets {
        candidates: vec![NodeCandidate {
            node_id,
            eth_address: provider,
            region_hint: None,
            // A `--node-id`-pinned target takes its direct address from
            // `--addr` at the dial site, not from the registry.
            multiaddrs: Bytes::new(),
        }],
        // Nothing was probed on this path, so no holder coverage was
        // measured: the pinned node is a full holder.
        coverage_by_node: HashMap::new(),
        // Nothing was probed on this path, so there is nothing to harvest.
        probed_samples: Vec::new(),
        pinned: true,
        // Nothing was probed, so the first claim comes from a header.
        size_hint: None,
        late: LateSlot::default(),
    }
}

/// Resolve the holders to fetch from (#1174): the explicit `--node-id`
/// (requiring `--provider-address`) as the one holder, or auto-discovery (#936)
/// when `--node-id` is omitted (deriving each provider from its node's registry
/// entry), nearest first. The acquire loop stripes across them; a set that goes
/// stale is refreshed by a later discovery with `--rediscover` forced, which
/// reads the registry rather than the peer store. `probe` says where the probe
/// round's start is recorded and how the round ends.
pub(crate) async fn resolve_target_node(
    args: &cli::ClientFetchArgs,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
    relays: &[RelayUrl],
    hash: [u8; 32],
    probe: ProbeOpts<'_>,
) -> anyhow::Result<ResolvedTargets> {
    if let Some(raw) = &args.node_id {
        // No reachability pre-check: the endpoint is discovery-enabled, so a
        // node-id resolves via `[network.discovery]` / `presets::N0` (plus its
        // default relays) even without `--addr` or configured relays. `clap`
        // guarantees `--provider-address` is present alongside `--node-id`.
        // A pinned node is its own only holder: a fault cools it and the fetch
        // waits for it to return.
        let config = |e: anyhow::Error| anyhow::Error::new(ResolveConfigFault(format!("{e:#}")));
        let node_id = PublicKey::from_str(raw)
            .map_err(|e| config(anyhow::anyhow!("invalid --node-id {raw:?}: {e}")))?;
        let provider_raw = args.provider_address.as_deref().ok_or_else(|| {
            config(anyhow::anyhow!(
                "--provider-address is required with --node-id"
            ))
        })?;
        let provider =
            super::chain_ctx::parse_address(provider_raw, "--provider-address").map_err(config)?;
        return Ok(pinned_targets(node_id, provider));
    }

    let capacity_bond = chain.capacity_bond.ok_or_else(|| {
        anyhow::Error::new(ResolveConfigFault(
            "auto-discovery needs capacity_bond_address (--capacity-bond-address or \
             blockchain.capacity_bond_address), or pass --node-id to dial directly"
                .to_string(),
        ))
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
    let order = discover_provider(
        endpoint,
        chain,
        capacity_bond,
        relays.first(),
        hash,
        args,
        probe,
    )
    .await?;
    if !order.candidates.is_empty() {
        // A header event, then one event per candidate — the log-side shape of
        // the previous per-line stderr listing.
        tracing::info!("discovered {} node(s):", order.candidates.len());
        for c in &order.candidates {
            tracing::info!(
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
/// hash. Scoped to `NotFound`, and to [`NoSourceHasBlob`] (every source said
/// `NotFound`) — a size or blacklist refusal is fixed by neither cause — so
/// every other error is returned verbatim.
pub(crate) fn annotate_unbound_cache_miss(err: anyhow::Error, ctx: &PoolContext) -> anyhow::Error {
    let refused = err
        .downcast_ref::<UpstreamRefused>()
        .filter(|refused| matches!(refused.error(), StreamError::NotFound));
    if refused.is_none() && err.downcast_ref::<NoSourceHasBlob>().is_none() {
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
    if let Some(quoted_rate) = refused
        .and_then(UpstreamRefused::evidence)
        .map(|resp| resp.body.rate_per_mb)
    {
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

/// Settles a drive that is dropped before it returns, as a Ctrl-C does.
///
/// A drive persists its lanes' voucher watermarks after it returns. A dropped
/// drive never gets there, so this guard runs `settle` from its `Drop` instead:
/// the lanes' paid bytes are queued for recording, at the armed (HIGH)
/// settlement ([`queue_face_watermarks`]), and the next run's vouchers continue
/// from them. A drive that returns calls [`Self::disarm`] and settles on its
/// normal path.
pub(crate) struct SettleOnDrop<F: FnOnce()> {
    /// The settle to run on drop; `None` once disarmed.
    settle: Option<F>,
}

impl<F: FnOnce()> SettleOnDrop<F> {
    /// Arm `settle` for a drop before [`Self::disarm`].
    pub(crate) const fn new(settle: F) -> Self {
        Self {
            settle: Some(settle),
        }
    }

    /// The drive returned: its normal path settles, so the drop does nothing.
    pub(crate) fn disarm(mut self) {
        self.settle = None;
    }
}

impl<F: FnOnce()> Drop for SettleOnDrop<F> {
    fn drop(&mut self) {
        if let Some(settle) = self.settle.take() {
            settle();
        }
    }
}

/// Persist what the pool lane paid, warning rather than masking the fetch
/// outcome.
///
/// Shared by the single- and multi-source paths: the bytes were paid for either
/// way, and a failure to record that only risks a rejected reuse next time.
fn persist_watermark(
    store: &RedbBuyerPoolStore,
    owner: Address,
    pool_id: PoolId,
    lane: LaneKey,
    progress: &VoucherProgress,
) {
    let Some(write) = ProgressWrite::of(progress) else {
        return;
    };
    let label = match write {
        ProgressWrite::Rebase { .. } => "rebased",
        ProgressWrite::Advance { .. } => "advanced",
        _ => "recorded",
    };
    let outcome = write.apply(store, owner, pool_id, lane);
    // A non-`Advanced` outcome (unknown pool / replaced owner slot / regression)
    // means the watermark did NOT move — same hazard as a backend error — so
    // surface it too rather than dropping it on the floor. A lost rebase write
    // heals itself: the next run signs from the old anchor, draws `Underpaid`
    // again, and rebases again.
    let (bytes_delivered, amount) = progress.totals();
    match outcome {
        Ok(AdvanceOutcome::Advanced) => {}
        Ok(other) => tracing::warn!(
            write = label,
            %bytes_delivered,
            %amount,
            "{label} voucher watermark not persisted for pool {pool_id} (provider {}): \
             {other:?}; the next reuse may re-sign a stale watermark, which that provider \
             rejects",
            lane.provider
        ),
        Err(e) => tracing::warn!(
            write = label,
            %bytes_delivered,
            %amount,
            "failed to persist {label} voucher watermark for pool {pool_id} (provider {}): \
             {e}; the next reuse may re-sign a stale watermark, which that provider rejects",
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
/// auto-discovers (#936): read the active set from `CapacityBond`, probe a
/// random, region-first sample of candidates, and derive `--provider-address` from each
/// candidate's registry entry, striping across them (#1174).
///
/// # Errors
///
/// [`Interrupted`] on Ctrl-C, [`decdn_client::GaveUp`] once the stop policy's
/// no-progress limit passes, or the fault that ended the fetch.
pub async fn fetch(args: &cli::FetchArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let timings = FetchTimings::start();
    let hash = parse_hash(&args.hash)?;
    let common = &args.common;
    // Before any network or keystore work, reject a flag combination clap
    // cannot express, naming the flags the user actually typed.
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
    // The guard runs here, before the keystore password prompt: the answer
    // depends only on the data dir, and a refusal must not cost a prompt first.
    // Shared, so a lane's watermark write runs off the runtime thread.
    let store = Arc::new(open_client_store_for_buy(
        &chain.data_dir,
        chain.data_dir_source,
        "fetch",
    )?);

    // One discovery-enabled endpoint, reused for probing and the delivery dial.
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;
    timings.mark(Mark::Endpoint);
    // The body runs in `fetch_over`, so the endpoint closes on every exit —
    // success, an early return, an error, or a Ctrl-C — and its open connections
    // end cleanly instead of being aborted on drop. A Ctrl-C drops `fetch_over`
    // mid-transfer: each drive's drop guard queues what it paid for recording,
    // and the `.partial` is the next run's resume prefix.
    let mut interrupt = Interrupt::watch();
    // The command's peer-record and watermark writes, off the runtime thread.
    let writes = OrderedWrites::default();
    let result = tokio::select! {
        result = fetch_over(args, hash, &relays, &chain, grant, &store, &endpoint, &writes, &timings) => {
            result
        }
        () = interrupt.wait() => Err(Interrupted.into()),
    };
    let output = if wants_stdout(&args.output) {
        "stdout"
    } else {
        "file"
    };
    timings.log(output, result.is_ok());
    // On every exit, a Ctrl-C's included: what the drives queued is durable
    // before the command returns.
    writes.settle().await;
    endpoint.close().await;
    result
}

/// The per-stream idle floor inside one paid leg: a leg whose stream opens no
/// byte, or delivers none, for this long ends as a source fault. The acquire
/// loop's lane watchdog ([`decdn_client::LANE_WATCHDOG`]) covers a lane that
/// makes slow progress.
pub(crate) const STALL_WINDOW: Duration = Duration::from_secs(30);

/// The part of [`fetch`] that runs over its open `endpoint`: resolve the
/// holders, take the first size claim (the probe hint, or a header-only open
/// when there is none), then fetch it through the acquire loop. The command
/// ends on done, a fault only the user can fix, or the stop policy: a
/// terminal waits for Ctrl-C, a script gives up after 10 minutes
/// without progress, and `--give-up-after-secs` overrides both.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn fetch_over(
    args: &cli::FetchArgs,
    hash: [u8; 32],
    relays: &[RelayUrl],
    chain: &ResolvedChain,
    grant: Option<CapabilityGrant>,
    store: &Arc<RedbBuyerPoolStore>,
    endpoint: &Endpoint,
    writes: &OrderedWrites,
    timings: &FetchTimings,
) -> anyhow::Result<()> {
    let common = &args.common;
    // The holders to start from: the explicit `--node-id`, or auto-discovery.
    // Only a configuration fault ends the fetch here; with no holder yet, the
    // first open discovers them. The round streams: the fetch starts at the
    // first verified holder, and the holders that answer later join it.
    let probe = ProbeOpts {
        timings: Some(timings),
        round: ProbeRound::Stream,
    };
    let resolved =
        holders_or_none(resolve_target_node(common, chain, endpoint, relays, hash, probe).await)?;
    timings.mark(Mark::Resolved);

    // Buyer signer (vouchers + the openPool/topUp tx). Loaded after selection so a
    // failed discovery never prompts for a keystore password. Password from env,
    // else `--keystore-password-file`, else TTY.
    let password = super::chain_ctx::read_keystore_password(
        &super::chain_ctx::password_sources(
            chain.keystore_password_file.as_deref(),
            PasswordUse::Unlock,
        ),
        "eth keystore password",
    )?
    .into_secret();
    let signer = Arc::new(load_signer(&chain.keystore, &password)?);
    timings.mark(Mark::Unlocked);
    let self_address = signer.address();

    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc.clone());
    let voucher_dom = voucher_domain(chain.chain_id, chain.payment_pool);
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
    // The pool's token is `PaymentPool.usdc()`, immutable per contract. A repeat
    // fetch already holds it in the persisted pool row, so read it there and skip
    // the eth_call; only a first-ever pool (no row) pays the `usdc()` round-trip.
    // The reuse branch of `open_or_reuse_pool` already trusts this same
    // `state.token`, so this only makes the top-level value consistent with it.
    let token = match cached_pool_token(store, self_address, chain.deployment())? {
        Some(token) => token,
        None => contract
            .usdc()
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {e}"))?,
    };

    let max_blob_bytes = common.max_blob_mb.saturating_mul(1024 * 1024);
    // The namespace routing hint (ADR 005 § Namespace routing): `--namespace <id>`
    // → big-endian `uint256`; absent => `NO_NAMESPACE` (best-effort cache/DHT).
    let namespace_id = args
        .namespace
        .map_or(decdn_protocol::client::NO_NAMESPACE, |n| {
            alloy::primitives::U256::from(n).to_be_bytes()
        });
    // A node that accepts the connection and never answers is as dead as one that
    // stops mid-stream, so the same window answers both (#1134). The pull carries
    // no overall wall-clock cap: it completes for any blob size as long as the
    // upstream keeps feeding it bytes.
    let deadlines = PullDeadlines::new(STALL_WINDOW, STALL_WINDOW, 0)?;
    // One connection per node for the whole fetch: every lane and leg opens
    // its streams on it.
    let connections = Connections::new(endpoint.clone());
    let funding = RunFunding::default();

    // The shared pull/funding deps every lane borrows for the whole fetch.
    let deps = DriveFetchDeps {
        endpoint,
        store,
        contract: &contract,
        rpc: &rpc,
        slash_dom: &slash_dom,
        self_address,
        token,
        chain,
        namespace_id,
        max_rate_per_mb: common.max_rate_per_mb,
        max_blob_bytes,
        deadlines,
        connections: &connections,
        writes,
        funding: &funding,
        timings: Some(timings),
    };

    let clock = Arc::new(ProgressClock::new());
    let stop = StopPolicy::new(
        std::io::IsTerminal::is_terminal(&std::io::stderr()),
        common.give_up_after(),
        clock,
    );
    let health = Arc::new(PeerHealth::default());
    // The fetch's lanes build concurrently and every one opens or reuses the
    // one pool on-chain, so the open-or-reuse runs one lane at a time, as it
    // does across a bundle pull's entries.
    let open_lock = tokio::sync::Mutex::new(());
    let sources = CliSources::new(
        &deps,
        common,
        relays,
        grant.as_ref(),
        &signer,
        &voucher_dom,
        Some(&open_lock),
        None,
        None,
    );
    let holders = sources.holders_from(&resolved);
    timings.set_holders_start(holders.len());
    // A fetch dropped by Ctrl-C queues every lane's vouchers for recording too.
    let on_drop = SettleOnDrop::new(|| sources.persist_watermarks_detached());
    let result = async {
        // The first size claim: the probe's hint, or a header-only open.
        let claim = sources.first_claim(hash, holders, &health, &stop).await?;
        if wants_stdout(&args.output) {
            return stream_to_stdout(
                &deps,
                &sources,
                claim.holders,
                health,
                hash,
                claim.total_bytes,
                stop,
                common.max_sources,
            )
            .await;
        }
        download_to_file(
            &deps,
            &sources,
            claim.holders,
            Arc::clone(&health),
            DownloadTarget {
                hash,
                total_bytes: claim.total_bytes,
                dest: &args.output,
                ranges: None,
            },
            &stop,
            common.max_sources,
        )
        .await
    }
    .await;
    on_drop.disarm();
    sources.persist_watermarks().await;
    if let Some(shortfall) = funding.shortfall() {
        eprintln!("warning: {shortfall}");
    }
    result.map_err(|err| sources.annotate(err))
}

/// Reconnect a delegated fetch's terminal owner-remedy voucher rejection
/// (`SpendingCapExhausted`, `CapabilityExpired`, `PoolExhausted`) to the
/// owner-side remedy: the delegate holds no wallet on this pool, so it cannot
/// `topUp`, raise its own cap, or mint itself a fresh capability. A
/// [`NoAffordableSource`] — no provider's next voucher fits the pool — gets the
/// same owner-side remedy. A drained or expired signer registration (a
/// [`SignerCapDrained`] the lane read from chain after a node refused it at
/// admission, or a mid-stream `SignerCapExhausted`) gets the remedy a
/// write-once registration leaves: a capability for a new signer key. Only an
/// expired registration, or one with nothing left of its cap, is named as
/// shutting out every provider; any other drain is measured against the
/// refusing provider's rate.
/// A terminal `Underpaid` — the resync budget ran out — gets its own next
/// step. Any other
/// error passes through verbatim (a stall, a transport fault, or a `NotFound`
/// already annotated by [`annotate_unbound_cache_miss`]).
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
    let underpaid = err
        .downcast_ref::<UpstreamVoucherRejected>()
        .is_some_and(|rejected| rejected.reason == VoucherRejectReason::Underpaid);
    // Only an expired registration, or one with nothing left of its cap,
    // shuts out every provider. Any other drain is measured against the
    // refusing provider's rate, and a provider at a lower rate may still
    // serve while headroom remains.
    let drained = err.downcast_ref::<SignerCapDrained>();
    let drained_everywhere = drained.is_some_and(SignerCapDrained::at_every_rate);
    let drained_at_rate = (drained.is_some() && !drained_everywhere)
        || err
            .downcast_ref::<UpstreamVoucherRejected>()
            .is_some_and(|rejected| rejected.reason == VoucherRejectReason::SignerCapExhausted);
    if drained_everywhere {
        err.context(
            "this capability's signer key has spent its whole registered cap, or its \
             registration expired, so no node can be paid for serving it. The registered cap \
             and expiry are write-once on-chain: a new token for the same key does not raise \
             them. Ask the pool owner to issue a capability for a new signer key",
        )
    } else if drained_at_rate {
        err.context(
            "this capability's signer key has less of its registered cap left than the \
             refusing provider reserves at its rate; a provider at a lower rate may still serve \
             it while headroom remains. The registered cap and expiry are write-once on-chain: \
             a new token for the same key does not raise them. To raise the limit, ask the pool \
             owner to issue a capability for a new signer key",
        )
    } else if err.downcast_ref::<NoAffordableSource>().is_some() {
        err.context(
            "no provider's next voucher fits what the delegated pool holds. Ask the pool \
             owner to top up the pool (a delegated client cannot top up a pool it does not own)",
        )
    } else if needs_owner {
        err.context(
            "capability cap exhausted, capability expired, or pool balance exhausted. Ask the \
             pool owner to top up the pool or issue a fresh, higher-cap capability; once this \
             signer key is registered on-chain its terms are write-once, so the fresh capability \
             must name a new signer key (a delegated client cannot top up a pool it does not \
             own)",
        )
    } else if underpaid {
        err.context(
            "the node kept refusing this lane's vouchers as underpaid after the resync attempts \
             ran out — retry the fetch; a lane with no accepted voucher yet means the signed \
             price is below the node's quote",
        )
    } else {
        err
    }
}

/// Shared pull/funding deps every lane of one fetch borrows for its lifetime
/// ([`build_multi_lane`]). Mirrors the locals `fetch()`
/// and `bundle_pull::PullCtx` already hold so both callers build the same
/// lanes. Every field is a borrow or a `Copy` scalar.
pub(crate) struct DriveFetchDeps<'a, P> {
    pub(crate) endpoint: &'a Endpoint,
    /// Shared, so a lane's watermark write runs on the blocking pool
    /// ([`queue_face_watermarks`]).
    pub(crate) store: &'a Arc<RedbBuyerPoolStore>,
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
    /// The command's one connection per node, shared by every lane of every
    /// fetch the command runs.
    pub(crate) connections: &'a Connections,
    /// The command's peer-record and watermark writes, run off the runtime
    /// thread in queue order across every fetch the command runs. Every
    /// ordered state write of a fetch goes through it.
    pub(crate) writes: &'a OrderedWrites,
    /// The run's funding facts, shared by every lane build and top-up of the
    /// run ([`RunFunding`]).
    pub(crate) funding: &'a RunFunding,
    /// The `decdn fetch` time-to-first-byte marks; `None` for a `bundle pull`.
    pub(crate) timings: Option<&'a FetchTimings>,
}

/// The lane's shared ledger + context: from the run registry when bundle pull
/// supplies one (so every entry/chunk on the lane shares one monotonic issuer),
/// else a fresh pair for a standalone `decdn fetch`.
///
/// On a registry reuse (an already-registered lane, second+ touch), `ctx` — this
/// call's freshly built context, which may reflect an `open_or_reuse_pool`
/// low-water top-up the registry's stored context predates — is otherwise
/// discarded in favor of the shared handle. Deposit only ever grows via
/// top-ups and the on-chain deposit is the hard backstop, so raising the
/// shared handle's `deposit` toward this call's fresh reading is always safe;
/// reconciling it here stops the pool-wide gate from reading a stale, lower
/// deposit and refusing prematurely (`PoolExhausted`) once the true balance
/// has grown. The reconcile also runs (as a no-op) when this call's build won
/// the registry race, since the shared value already equals the fresh one.
fn lane_ledger(
    ledgers: Option<&LaneLedgers>,
    lane: LaneKey,
    ctx: PoolContext,
) -> (Arc<PoolLedger>, Arc<Mutex<PoolContext>>) {
    let seed = Cumulative {
        bytes: ctx.prior_bytes_delivered,
        amount: ctx.prior_amount,
    };
    // `U256` is `Copy`; capture the fresh deposit before `ctx` moves into the
    // `get_or_insert` build closure below.
    let fresh_deposit = ctx.deposit;
    match ledgers {
        Some(reg) => {
            let h = reg.get_or_insert(lane, || LaneHandle {
                ledger: Arc::new(PoolLedger::new(seed)),
                ctx: Arc::new(Mutex::new(ctx)),
            });
            {
                let mut g = h.ctx.lock().unwrap_or_else(PoisonError::into_inner);
                g.deposit = g.deposit.max(fresh_deposit);
            }
            (h.ledger, h.ctx)
        }
        None => (Arc::new(PoolLedger::new(seed)), Arc::new(Mutex::new(ctx))),
    }
}

/// One built payment lane: the per-provider `PoolContext`/`PoolLedger` and the
/// [`PeerSource`] that pays with them. [`super::cli_sources::CliSources`] moves
/// the source into the [`decdn_client::StreamCandidate`] the acquire loop
/// consumes and keeps
/// a [`FaceLaneHandle`] on the same `ctx`/`ledger` for the watermark.
pub(crate) struct MultiLane<'a> {
    pub(crate) provider: Address,
    pub(crate) pool_id: PoolId,
    /// The lane's persisted prior amount — the baseline
    /// [`persist_watermark`] computes the advance against.
    pub(crate) prior_amount: U256,
    pub(crate) ctx: Arc<Mutex<PoolContext>>,
    pub(crate) ledger: Arc<PoolLedger>,
    /// The lane's source.
    pub(crate) source: PeerSource<'a>,
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

/// Build the target address for a candidate: its `node_id`, the pinned
/// `addr` when the user gave one (`--addr`, which requires `--node-id`), the
/// first configured relay hint, and its registry multiaddrs as direct-address
/// hints (ADR 001 § Node Discovery).
pub(crate) fn lane_target(
    candidate: &NodeCandidate,
    addr: Option<std::net::SocketAddr>,
    relays: &[RelayUrl],
) -> EndpointAddr {
    let mut target = EndpointAddr::new(candidate.node_id);
    if let Some(addr) = addr {
        target = target.with_ip_addr(addr);
    }
    if let Some(url) = relays.first() {
        target = target.with_relay_url(url.clone());
    }
    // Registry multiaddrs as direct-address hints: a reachable lane provider
    // connects without a relay (ADR 001 § Node Discovery).
    discovery::with_dial_addrs(target, candidate)
}

/// Build one [`MultiLane`] for `provider` at `target`: open/reuse the pool,
/// seed a ledger from that lane's persisted cumulative, and wrap a
/// [`PeerSource`] over it.
///
/// `open_lock`, when `Some`, serializes the pool open-or-reuse inside
/// [`build_ctx_for_fetch`]: across one fetch's concurrently built lanes, which
/// all open or reuse the one on-chain pool, and across other fetches sharing
/// that pool (bundle pull's cross-entry concurrency, #1774); the guard is dropped before
/// any streaming, and skipped entirely on the delegated path, which opens
/// nothing on-chain. A caller that holds a provider's stream permit takes it
/// before `open_lock`, so the lock order stays permit → `open_lock` and no
/// hold-and-wait cycle can form.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_multi_lane<'a, P>(
    deps: &DriveFetchDeps<'a, P>,
    grant: Option<&CapabilityGrant>,
    signer: &Arc<PrivateKeySigner>,
    voucher_dom: &Eip712Domain,
    provider: Address,
    target: EndpointAddr,
    open_lock: Option<&tokio::sync::Mutex<()>>,
    ledgers: Option<&LaneLedgers>,
) -> anyhow::Result<MultiLane<'a>>
where
    P: alloy::providers::Provider + Clone,
{
    let (ctx, spend) = {
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
            deps.funding,
        )
        .await?
    };
    let pool_id = ctx.pool_id;
    let prior_amount = ctx.prior_amount;
    let lane = LaneKey {
        pool_id,
        signer: deps.self_address,
        provider,
    };
    // The lane's ledger + context, seeded from its persisted `(signer,
    // provider)` cumulative so the first voucher continues the lane (a restart
    // from zero is rejected as a regression) — from the run registry when
    // `ledgers` is `Some`, else a fresh pair.
    let (ledger, ctx) = lane_ledger(ledgers, lane, ctx);
    // The run's view of the pool's spend counts this lane through its ledger
    // from now on, whether or not the acquire loop ever starts it. A delegated
    // lane draws on a pool this caller does not own, and joins nothing.
    if let Some(spend) = spend {
        deps.funding.join_lane(lane, spend, &ledger);
    }
    let source = lane_source(
        deps,
        target,
        Arc::clone(&ctx),
        Arc::clone(&ledger),
        provider,
    );
    // A delegated signer the node refuses at admission for a drained or
    // expired registration gets a plain `NotFound`; the chain read tells it
    // apart from a cache miss, so the fetch bars that node, or stops when no
    // node can serve the signer, instead of retrying (#2338).
    let source = if grant.is_some() {
        source.with_signer_check(deps.contract)
    } else {
        source
    };
    if let Some(timings) = deps.timings {
        timings.mark(Mark::Lane);
    }
    Ok(MultiLane {
        provider,
        pool_id,
        prior_amount,
        ctx,
        ledger,
        source,
    })
}

/// `provider`'s lane source at `target`, paying through `ctx` and `ledger`.
/// Every open takes a stream on the command's connection to `target`
/// ([`DriveFetchDeps::connections`]).
fn lane_source<'a, P>(
    deps: &DriveFetchDeps<'a, P>,
    target: EndpointAddr,
    ctx: Arc<Mutex<PoolContext>>,
    ledger: Arc<PoolLedger>,
    provider: Address,
) -> PeerSource<'a> {
    PeerSource::new(
        deps.endpoint,
        target,
        ctx,
        ledger,
        deps.slash_dom,
        provider,
        deps.namespace_id,
        deps.max_blob_bytes,
        deps.max_rate_per_mb,
        deps.deadlines,
        Some(deps.connections.clone()),
    )
}

/// One lane's watermark-persistence handle: what [`stream_lane_watermarks`]
/// needs to settle the lane AFTER the fetch, kept alive after the lane's owned
/// [`PeerSource`] has moved into its [`decdn_client::StreamCandidate`]. The
/// `ledger` and
/// `ctx` are the SAME `Arc`s the fetch paid through, so the lane's settlement
/// is read here once the fetch ends.
pub(crate) struct FaceLaneHandle {
    pub(crate) pool_id: PoolId,
    pub(crate) provider: Address,
    pub(crate) prior_amount: U256,
    pub(crate) ledger: Arc<PoolLedger>,
    /// The lane's buyer context, read to annotate an unbound cache miss.
    pub(crate) ctx: Arc<Mutex<PoolContext>>,
}

/// The per-lane [`LaneWatermark`]s to persist after a face fetch, read from the
/// retained [`FaceLaneHandle`]s — the same settle-at-armed-cumulative rule
/// [`multi_lane_watermarks`] then keys for persistence.
///
/// One watermark per ledger: a lane rebuilt on the run's shared ledger leaves a
/// second handle on it, and the first read takes the ledger's unsaved rebase,
/// so a second write for it would only repeat the first. Each rebuild read its
/// own baseline from the store, and a sibling entry's rebase can have moved
/// that baseline down since the first build, so the watermark takes the lowest
/// one: the advance past it still persists.
fn stream_lane_watermarks(lanes: &[FaceLaneHandle]) -> Vec<LaneWatermark> {
    let mut per_ledger: Vec<(&FaceLaneHandle, U256)> = Vec::new();
    for l in lanes {
        match per_ledger
            .iter_mut()
            .find(|(first, _)| Arc::ptr_eq(&first.ledger, &l.ledger))
        {
            Some((_, prior)) => *prior = (*prior).min(l.prior_amount),
            None => per_ledger.push((l, l.prior_amount)),
        }
    }
    per_ledger
        .into_iter()
        .map(|(l, prior_amount)| LaneWatermark {
            pool_id: l.pool_id,
            provider: l.provider,
            prior_amount,
            // Taken before the settlement read, so the settlement is at or above it.
            rebase_anchor: l.ledger.take_unsaved_rebase(),
            settlement: l.ledger.settlement(),
        })
        .collect()
}

/// Copy a [`decdn_client::VerifiedReader`]'s verified bytes to `sink`,
/// feeding `on_progress` the cumulative content bytes drained. Returns the total
/// drained. Only bao-verified bytes ever leave the reader, so `sink` (stdout) is
/// safe to pipe.
async fn copy_verified<R, W>(
    reader: &mut R,
    sink: &mut W,
    total_bytes: u64,
    on_progress: Option<&dyn Fn(u64, u64)>,
) -> anyhow::Result<u64>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buf = vec![0u8; 256 * 1024];
    let mut drained: u64 = 0;
    loop {
        let n = reader
            .read(&mut buf)
            .await
            .map_err(|e| anyhow::anyhow!("read verified stream: {e}"))?;
        if n == 0 {
            break;
        }
        let chunk = buf
            .get(..n)
            .ok_or_else(|| anyhow::anyhow!("read returned more than the buffer holds"))?;
        sink.write_all(chunk)
            .await
            .map_err(|e| anyhow::anyhow!("write to stdout: {e}"))?;
        drained = drained.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        if let Some(cb) = on_progress {
            cb(drained, total_bytes);
        }
    }
    sink.flush()
        .await
        .map_err(|e| anyhow::anyhow!("flush stdout: {e}"))?;
    Ok(drained)
}

/// One lane's buyer-store watermark write: the lane and the progress to record.
type LaneWrite = (LaneKey, VoucherProgress);

/// The watermark write every face lane calls for, read from the retained
/// handles now — the same settle-at-armed-cumulative rule the file path
/// applies, one write per lane ledger.
fn face_watermark_writes(self_address: Address, handles: &[FaceLaneHandle]) -> Vec<LaneWrite> {
    multi_lane_watermarks(self_address, &stream_lane_watermarks(handles))
}

/// Apply every write in `writes`. Blocking: each write that moves the row is a
/// redb commit.
fn write_watermarks(store: &RedbBuyerPoolStore, self_address: Address, writes: Vec<LaneWrite>) {
    for (lane, vprogress) in writes {
        persist_watermark(store, self_address, lane.pool_id, lane, &vprogress);
    }
}

/// Read every face lane's voucher watermark from its ledger now
/// ([`face_watermark_writes`]), queue the writes on the command's ordered
/// `writes`, and return a receiver that resolves once they land. The faces do
/// not persist, so the thin CLI layer does it after the fetch, before surfacing
/// any error: the bytes each lane delivered are paid for whatever the outcome.
///
/// Each write that moves the row is a durable redb commit, and a `bundle
/// pull`'s lanes share the caller's task, so a commit inline would stop every
/// lane of the run while it syncs (#2211). The queue keeps the writes in the
/// order the ledgers were read, across every entry: a rebase replaces the lane
/// row outright, so a rebase landing after a newer advance, or a stale advance
/// landing after a rebase, would leave the next run signing from the wrong
/// progress. The read and the queueing are one synchronous call for that
/// reason, and the order holds only while every caller polls on one task, as
/// a fetch and a `bundle pull` do.
///
/// A drop guard, which cannot wait, drops the receiver. The command then waits
/// for the queue before it returns ([`OrderedWrites::settle`]), on a Ctrl-C
/// too. A second Ctrl-C exits the process at once, and a write still queued or
/// running then is lost; the next run may re-sign a stale watermark, which its
/// provider rejects. A runtime that shuts down cancels blocking tasks that have
/// not started, and the queue then drains on the shutting-down thread.
pub(crate) fn queue_face_watermarks(
    writes: &OrderedWrites,
    store: &Arc<RedbBuyerPoolStore>,
    self_address: Address,
    handles: &[FaceLaneHandle],
) -> tokio::sync::oneshot::Receiver<()> {
    queue_watermark_writes(
        writes,
        store,
        self_address,
        face_watermark_writes(self_address, handles),
    )
}

/// Queue `lane_writes` on `writes` ([`queue_face_watermarks`]).
fn queue_watermark_writes(
    writes: &OrderedWrites,
    store: &Arc<RedbBuyerPoolStore>,
    self_address: Address,
    lane_writes: Vec<LaneWrite>,
) -> tokio::sync::oneshot::Receiver<()> {
    let store = Arc::clone(store);
    writes.queue_awaitable(move || write_watermarks(&store, self_address, lane_writes))
}

/// Stream `hash`'s verified bytes to STDOUT via the [`Streamer`] consumption face
/// (#1848 4b). Only bao-verified bytes ever reach the pipe — safe to `| tar x` —
/// the fetch is consumption-paced (a slow or early-quit consumer stops the pull
/// and the spend within one read-ahead window), and a source that faults
/// mid-stream cools while another carries on, with no re-pull or re-pay.
///
/// `max_sources` is the front lane cap. Progress draws on stderr, only when
/// stderr is a terminal; stdout carries nothing but the verified bytes. The
/// caller persists each lane's voucher watermark after the stream.
#[allow(clippy::too_many_arguments)]
async fn stream_to_stdout<P>(
    deps: &DriveFetchDeps<'_, P>,
    sources: &CliSources<'_, P>,
    holders: Vec<Holder>,
    health: Arc<PeerHealth>,
    hash: [u8; 32],
    total_bytes: u64,
    stop: StopPolicy,
    max_sources: usize,
) -> anyhow::Result<()>
where
    P: alloy::providers::Provider + Clone,
{
    // Read-ahead stays the default: the outstanding-spend bound for an
    // abandoned pipe.
    let pull_config = PullConfig {
        streamer_lane_cap: max_sources.max(1),
        ..PullConfig::new()
    };
    let funder = sources.funder();
    let drive_config = DriveConfig::cli(deps.chain.working_deposit);

    // The fill store's `.partial` lives here for the stream's lifetime; a streamed
    // blob is not kept, so a temp dir (removed on drop) under the data dir is its
    // natural home.
    let scratch = tempfile::tempdir_in(&deps.chain.data_dir)
        .map_err(|e| anyhow::anyhow!("open stream scratch dir: {e}"))?;
    let streamer = Streamer::new(
        sources,
        holders,
        health,
        funder,
        drive_config,
        scratch.path(),
    )
    .max_blob_bytes(deps.max_blob_bytes);
    let (mut reader, mut drive) = streamer
        .open(hash, total_bytes, &pull_config, Arc::new(NoCache), stop)
        .await?;

    // The bar draws on stderr, so it shows only when stderr is a terminal; stdout
    // carries the verified bytes either way.
    let bar = std::io::IsTerminal::is_terminal(&std::io::stderr()).then(delivery_progress);
    // `copy_verified` reports after each write, so the first report with a
    // byte is the first byte on stdout.
    let on_progress = |received: u64, expected: u64| {
        if received > 0
            && let Some(timings) = deps.timings
        {
            timings.mark(Mark::FirstByte);
        }
        if let Some((_, cb, _)) = &bar {
            cb(received, expected);
        }
    };

    // The drive runs beside the copy, not inside its reads: a blocked stdout
    // must not stop an open paid leg from paying and draining. The read-ahead
    // window bounds how far it runs ahead of the pipe.
    let mut stdout = tokio::io::stdout();
    let copy_result = drive
        .alongside(copy_verified(
            &mut reader,
            &mut stdout,
            total_bytes,
            Some(&on_progress),
        ))
        .await;

    if let Some((bar, _, _)) = &bar {
        bar.finish_and_clear();
    }
    let copied = copy_result.map_err(|read_err| stream_error(read_err, drive.take_error()))?;
    tracing::info!("streamed {copied} verified bytes to stdout");
    Ok(())
}

/// The error a failed stdout stream ends with: the fetch's own typed error when
/// the drive ended with one (so a give-up, a fatal fault or a unanimous verdict
/// keeps its type for the exit code and the remedy hints), else the read or
/// write error itself.
fn stream_error(read_err: anyhow::Error, fetch_err: Option<anyhow::Error>) -> anyhow::Error {
    fetch_err.unwrap_or(read_err)
}

/// Fetch `hash` to `output` via the [`Downloader`] consumption face (#1848 4c),
/// striping across `holders` with at most `max_sources` lanes at once (a
/// holder discovery adds later can take a free lane), and print the summary
/// line. The `.partial` beside `output` stays in place on a failure for a
/// later resume. The caller persists each lane's voucher watermark after the
/// fetch.
async fn download_to_file<P>(
    deps: &DriveFetchDeps<'_, P>,
    sources: &CliSources<'_, P>,
    holders: Vec<Holder>,
    health: Arc<PeerHealth>,
    target: DownloadTarget<'_>,
    stop: &StopPolicy,
    max_sources: usize,
) -> anyhow::Result<()>
where
    P: alloy::providers::Provider + Clone,
{
    let (bar, on_bar, meter) = delivery_progress();
    let on_progress = |received: u64, expected: u64| {
        if received > 0
            && let Some(timings) = deps.timings
        {
            timings.mark(Mark::FirstByte);
        }
        on_bar(received, expected);
    };
    let downloader = Downloader::new(
        sources,
        holders,
        health,
        sources.funder(),
        DriveConfig::cli(deps.chain.working_deposit),
        max_sources,
    )
    .max_blob_bytes(deps.max_blob_bytes);
    let result =
        Box::pin(downloader.fetch_to_paths_until(&[target], None, Some(&on_progress), stop)).await;
    bar.finish_and_clear();
    result?;
    // The finished file holds exactly the proven size, which can differ from
    // the first claim the fetch started from.
    let proven = tokio::fs::metadata(target.dest)
        .await
        .map_or(target.total_bytes, |m| m.len());
    print_fetch_summary(proven, &meter, target.dest);
    Ok(())
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
    /// The watermark the lane's ledger rebased down to and has not persisted.
    rebase_anchor: Option<Cumulative>,
}

/// Pair each lane's own `LaneKey` with the watermark to persist for it.
///
/// Every lane settles at its ARMED cumulative rather than branching on the
/// fetch's outcome. The fetch result is ONE outcome shared by
/// every lane, but "did this lane's last voucher land?" is a PER-LANE question,
/// and on the multi-source path a successful fetch can leave a lane
/// armed-above-committed: a cancelled or stalled leg drops its `fill_gap`
/// future wherever it is parked, including inside the voucher exchange that
/// `issue` deliberately arms before sending, and a leg stopped at a steal
/// split keeps a closing voucher that failed to send armed. Settling that lane at `committed` on the
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
                VoucherProgress::from_cumulative(l.settlement, l.prior_amount)
                    .with_rebase_anchor(l.rebase_anchor),
            )
        })
        .collect()
}

/// What the lane builds of one run learn about funding its pool, shared by
/// every fetch of the run: one `decdn fetch`, or every entry of a `bundle pull`.
///
/// It holds two facts. The first is the pool's spend across every lane: a
/// baseline for the lanes the run does not drive, plus what each lane the run
/// built has committed since. The deposit gate reads it
/// ([`Funder::pool_spent`]), so a pool shared across many providers gates on its
/// true remaining deposit whichever lanes the acquire loop starts. The second
/// is a wallet that holds too little USDC to top the pool up: once seen, no
/// later lane build of the run tries the proactive refill again, a reactive
/// top-up fails without a transaction, and the command ends with one
/// `warning:` line naming it.
#[derive(Debug, Default)]
pub(crate) struct RunFunding {
    spend: Mutex<RunSpend>,
    shortfall: Mutex<Option<String>>,
}

/// What a lane build learned about the pool's spend, for
/// [`RunFunding::join_lane`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaneSpend {
    /// The pool's spend on every other lane, as the build saw it.
    pub(crate) outside: U256,
    /// The amount the lane resumes from.
    pub(crate) prior: U256,
    /// Whether the pool row recorded the lane before this build seeded it from
    /// chain, so that an earlier lane's `outside` counts its prior.
    pub(crate) recorded: bool,
    /// Whether `outside` includes redeemed spend no tracked lane accounts for
    /// ([`BuyerPoolState::redeemed_elsewhere`], from an adopted row's
    /// `totalRedeemed`), which counts the watermark of every lane the row has
    /// not seeded.
    pub(crate) from_chain: bool,
}

/// The pool's spend as a run's lanes join it.
#[derive(Debug, Default)]
struct RunSpend {
    /// The spend on lanes the run does not drive. `None` until the first lane
    /// joins.
    baseline: Option<U256>,
    /// Whether the first lane's `outside` came from the chain.
    from_chain: bool,
    /// Every ledger each joined lane has paid through. A lane rebuilt with a
    /// fresh ledger keeps its earlier ones: vouchers on a lane are cumulative,
    /// so the lane's spend is the largest of them.
    lanes: HashMap<LaneKey, Vec<Arc<PoolLedger>>>,
}

impl RunFunding {
    /// Record that `lane` joins the run, paying through `ledger`.
    ///
    /// The first lane sets the baseline to its `outside`. Each later lane
    /// moves its prior out of the baseline when the baseline counts it: when
    /// the row recorded the lane, or when the baseline came from the chain. Its
    /// ledger counts that amount from now on. A lane that has joined before
    /// only adds `ledger`, if it is a new one.
    pub(crate) fn join_lane(&self, lane: LaneKey, spend: LaneSpend, ledger: &Arc<PoolLedger>) {
        let mut run = self.spend.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(ledgers) = run.lanes.get_mut(&lane) {
            if !ledgers.iter().any(|known| Arc::ptr_eq(known, ledger)) {
                ledgers.push(Arc::clone(ledger));
            }
            return;
        }
        run.lanes.insert(lane, vec![Arc::clone(ledger)]);
        run.baseline = Some(match run.baseline {
            None => {
                run.from_chain = spend.from_chain;
                spend.outside
            }
            Some(baseline) if spend.recorded || run.from_chain => {
                baseline.saturating_sub(spend.prior)
            }
            Some(baseline) => baseline,
        });
    }

    /// The pool's spend across every lane: the baseline plus each joined
    /// lane's committed amount. `None` before any lane joins.
    pub(crate) fn pool_spent(&self) -> Option<U256> {
        let run = self.spend.lock().unwrap_or_else(PoisonError::into_inner);
        let baseline = run.baseline?;
        Some(run.lanes.values().fold(baseline, |total, ledgers| {
            let lane = ledgers
                .iter()
                .map(|ledger| ledger.committed().amount)
                .max()
                .unwrap_or(U256::ZERO);
            total.saturating_add(lane)
        }))
    }

    /// The warning for a wallet seen to hold too little USDC for a top-up in
    /// this run, if one was.
    pub(crate) fn shortfall(&self) -> Option<String> {
        self.shortfall
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Diagnose a failed top-up leg (`err`) for `pool_id` by reading the
    /// wallet's USDC balance. When the wallet holds less than `additional`,
    /// which no retry fixes, record the shortfall and return its warning; it
    /// is logged at WARN the first time. Otherwise return `None`, after a WARN
    /// that names the failure, the balance, or why the balance could not be
    /// read.
    pub(crate) async fn check_wallet<P>(
        &self,
        rpc: &P,
        token: Address,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
        err: &anyhow::Error,
    ) -> Option<String>
    where
        P: alloy::providers::Provider + Clone,
    {
        let error = decdn_common::redact::sanitize_err_chain(err);
        let balance = match decdn_incentive::Erc20::new(token, rpc.clone())
            .balanceOf(owner)
            .call()
            .await
        {
            Ok(balance) => balance,
            Err(read) => {
                let read = decdn_common::redact::sanitize_err_chain(&anyhow::Error::new(read));
                tracing::warn!(
                    %pool_id,
                    %additional,
                    %error,
                    wallet_usdc_error = %read,
                    "buyer pool top-up failed, and the wallet's USDC balance could not be read"
                );
                return None;
            }
        };
        if balance >= additional {
            tracing::warn!(
                %pool_id,
                %additional,
                wallet_usdc = %balance,
                %error,
                "buyer pool top-up failed although the wallet holds enough USDC"
            );
            return None;
        }
        let warning = format!(
            "wallet {owner} holds {balance} µUSDC, less than the {additional} µUSDC top-up \
             buyer pool {pool_id} needs; fund the wallet so the pool can be topped up"
        );
        let mut shortfall = self
            .shortfall
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if shortfall.is_none() {
            tracing::warn!(
                %pool_id,
                %additional,
                wallet_usdc = %balance,
                %error,
                "the wallet holds too little USDC to top up the buyer pool"
            );
            *shortfall = Some(warning.clone());
        }
        Some(warning)
    }
}

/// The CLI's [`Funder`]: a mid-fetch reactive top-up runs the same
/// `ensure_allowance -> top_up -> add_deposit` path as the proactive low-water
/// refill ([`refill_if_low`]), behind the driver's [`Funder`] seam so the gap
/// driver stays chain-handle-agnostic. Unlike the proactive refill, every
/// failure here returns to the driver, which rules it unaffordable or, when the
/// escrow may have moved, fatal. The driver decides
/// WHETHER to fund (its pacer confirms a genuine, ledger-corroborated
/// exhaustion and that budget/attempts remain); this only executes the
/// on-chain move and returns the [`DepositOutcome`] for the driver to credit.
///
/// There is no funder-vs-delegate split here: the CLI fetcher is always its
/// own pool owner, so `top_up` is unconditionally authorized (`topUp` is
/// owner-only on-chain, and owner == signer on this path).
pub(crate) struct CliFunder<'a, P> {
    pub(crate) contract: &'a PaymentPool::PaymentPoolInstance<P>,
    pub(crate) rpc: &'a P,
    pub(crate) store: &'a RedbBuyerPoolStore,
    pub(crate) owner: Address,
    /// The pool to top up: the one the first lane a
    /// [`super::cli_sources::CliSources`] builds opens or reuses. Every lane
    /// pays from the one pool, and a top-up runs only from a running lane, so
    /// it is set by then.
    pub(crate) pool_id: &'a std::sync::OnceLock<PoolId>,
    pub(crate) token: Address,
    pub(crate) payment_pool_addr: Address,
    pub(crate) max_approve: bool,
    /// The run's funding facts: its outside spend, and a wallet shortfall that
    /// makes a further top-up pointless.
    pub(crate) funding: &'a RunFunding,
}

impl<P> Funder for CliFunder<'_, P>
where
    P: alloy::providers::Provider + Clone,
{
    fn max_topups(&self) -> u32 {
        decdn_client::MAX_TOPUP_ATTEMPTS
    }

    fn pool_spent(&self) -> Option<U256> {
        self.funding.pool_spent()
    }

    fn top_up(&self, additional: U256) -> SourceFuture<'_, DepositOutcome> {
        Box::pin(async move {
            let pool_id =
                self.pool_id.get().copied().ok_or_else(|| {
                    anyhow::anyhow!("no payment lane is built, so no pool to top up")
                })?;
            if let Some(shortfall) = self.funding.shortfall() {
                return Err(
                    anyhow::anyhow!("the wallet cannot fund a top-up: {shortfall}")
                        .context(WalletShortfall),
                );
            }
            // `topUp` pulls `additional` USDC via `transferFrom`, so the
            // standing allowance must cover it first: unlimited under
            // `--max-approve`, else exactly `additional`.
            let escrowed = async {
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
                top_up(self.contract, pool_id, additional).await
            }
            .await;
            // A failure before any receipt names its cause once: a wallet
            // short of USDC is recorded for the run's closing warning.
            let ToppedUpPool { credited, tx, .. } = match escrowed {
                Ok(topped_up) => topped_up,
                Err(err) if err.downcast_ref::<TopUpUnconfirmed>().is_some() => return Err(err),
                Err(err) => {
                    let short = self
                        .funding
                        .check_wallet(self.rpc, self.token, self.owner, pool_id, additional, &err)
                        .await;
                    return Err(match short {
                        Some(_) => err.context(WalletShortfall),
                        None => err,
                    });
                }
            };
            // Grade here rather than handing the escrowed-but-untracked
            // outcomes back for the driver to bail on. The driver treats them
            // as terminal either way, but `DepositOutcome` has nowhere to carry
            // the tx or the pool, so its bail names neither — and this is the
            // one leg where the escrow has already moved.
            let effect = topped_up_effect(pool_id, credited);
            let new_deposit = grade_deposit_credit(
                self.store.add_deposit(self.owner, pool_id, credited),
                &effect,
                tx,
            )?;
            Ok(DepositOutcome::Added(new_deposit))
        })
    }
}

/// Whether `--output` names the stdout stream rather than a file. Exactly a
/// single dash (`-o -`) streams verified bytes to stdout; a file literally named
/// `-` is written by addressing it as `./-`, the usual convention (#1848 4b).
fn wants_stdout(output: &Path) -> bool {
    output == Path::new("-")
}

/// Where the [`decdn_client::ClientRangedStore`] for `--output` lives: its directory (the
/// output's parent, or the current dir) and its stem (the output's own file
/// name). Keying the store by the output name makes its promoted final path IS
/// `--output` (no post-finalize rename), and its `.partial` sits beside the
/// destination exactly like `<output>.partial`, so promotion is a
/// same-filesystem atomic rename.
pub(crate) fn ranged_store_location(output: &Path) -> anyhow::Result<(PathBuf, String)> {
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

/// Rolling delivery-rate estimate behind a progress bar's `{msg}`, and the
/// running totals the end-of-fetch summary reads back. Single-blob `fetch` feeds
/// it from the delivery bar; `bundle pull` feeds it from its bottom total bar, so
/// the rate there is whole-download throughput rather than one file's.
///
/// The bar's positions are cumulative verified **content** bytes — what the
/// [`decdn_client::ProgressCallback`] reports — so the rate is content
/// throughput (the interleaved bao proof nodes the wire also carries are not
/// counted).
///
/// Each `set_position` on the bar is bursty — many chunks land in one instant,
/// then a gap — so a naive `delta / dt` per callback spikes and collapses. This
/// holds a time-weighted exponential moving average instead: each sample folds
/// in with weight `1 - exp(-dt / tau)`, so the estimate is stable regardless of
/// how unevenly callbacks are spaced.
#[derive(Default)]
pub(crate) struct SpeedState {
    /// Instant and cumulative-byte position at the first observed sample. The
    /// summary measures elapsed and bytes-moved from here, so a resumed fetch
    /// (which starts at a non-zero `base_present`) reports only what this run
    /// actually transferred rather than dividing already-present bytes by this
    /// run's short window.
    started: Option<(Instant, u64)>,
    /// Instant and cumulative received content bytes at the previous sample.
    last: Option<(Instant, u64)>,
    /// Smoothed rate in bytes/sec. `None` until the second sample gives a `dt`.
    ewma_bps: Option<f64>,
}

impl SpeedState {
    /// Fold one sample — cumulative content bytes `received` as of `now` — into
    /// the estimate and return the current smoothed rate in bytes/sec (`0.0`
    /// until a second sample gives a `dt`).
    pub(crate) fn observe(&mut self, now: Instant, received: u64) -> f64 {
        self.started.get_or_insert((now, received));
        if let Some((prev_at, prev_bytes)) = self.last {
            let dt = now.saturating_duration_since(prev_at).as_secs_f64();
            // Skip same-instant callbacks (a burst): they carry no usable `dt`
            // and would divide by ~zero into a spike.
            if dt > 0.0 {
                let inst = bytes_as_f64(received.saturating_sub(prev_bytes)) / dt;
                let alpha = 1.0 - (-dt / RATE_SMOOTHING_TAU_SECS).exp();
                // Seed from 0, not `inst`: on the first sample a tiny `dt` makes
                // `inst` huge, but `alpha * inst = (1 - exp(-dt/tau)) * (delta/dt)
                // -> delta/tau` as `dt -> 0`, so the estimate stays bounded
                // instead of spiking, then converges upward.
                let prev_bps = self.ewma_bps.unwrap_or(0.0);
                self.ewma_bps = Some(prev_bps + alpha * (inst - prev_bps));
            }
        }
        self.last = Some((now, received));
        self.ewma_bps.unwrap_or(0.0)
    }
}

/// Widen a byte count to `f64` for rate arithmetic. A single transfer never
/// approaches 2^53 bytes, so the precision the cast lint guards against is not
/// at risk here.
#[expect(
    clippy::cast_precision_loss,
    reason = "byte counts stay far below f64's 2^53 exact-integer ceiling"
)]
pub(crate) const fn bytes_as_f64(n: u64) -> f64 {
    n as f64
}

/// Format a non-negative bytes/sec rate as e.g. `12.3 MiB/s`. A rate at or
/// below zero (no data yet, or a stall) renders as `--`.
pub(crate) fn fmt_rate(bps: f64) -> String {
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
pub(crate) fn fmt_eta(remaining_bytes: u64, bps: f64) -> String {
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
pub(crate) struct DeliveryMeter {
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
/// summary (#1118). The callback advances the bar, folds each update into the
/// shared [`SpeedState`] so `{msg}` shows a steady rate/ETA, and mirrors the
/// percent into the terminal's tab bar ([`TabProgress`]) where supported; the
/// tab indicator is removed when the callback drops.
fn delivery_progress() -> (
    indicatif::ProgressBar,
    impl Fn(u64, u64) + 'static,
    DeliveryMeter,
) {
    let bar = new_progress_bar();
    // `ProgressBar` is `Arc`-backed, so the clone the callback owns drives the
    // same bar the caller clears.
    let (on_progress, meter) = bar_callback(bar.clone(), TabProgress::detect(true));
    (bar, on_progress, meter)
}

/// Build the callback that drives `bar` (it sets the bar's length to the
/// fetch's current size bound whenever it changes, advances its position to
/// the cumulative verified content-byte count, and folds each update into a
/// [`SpeedState`] for the rate/ETA `{msg}`), and return it with the
/// [`DeliveryMeter`] the caller reads after the bar finishes. `tab`, when set,
/// shows the same percent in the terminal's tab bar.
///
/// The callback's `received`/`expected` are content bytes, per
/// [`decdn_client::ProgressCallback`]. `expected` is the size bound: a size
/// claim the fetch grows or shrinks until a leg proves the size. Both the bar
/// length and its position are in the same unit.
fn bar_callback(
    bar: indicatif::ProgressBar,
    tab: Option<Arc<TabProgress>>,
) -> (impl Fn(u64, u64) + 'static, DeliveryMeter) {
    // Setting the length takes a write lock, so it is set only when the bound
    // moves, not on every chunk in the hot receive loop. `u64::MAX` is never a
    // bound, so the first report always sets it.
    let length = std::sync::atomic::AtomicU64::new(u64::MAX);
    let state = Arc::new(Mutex::new(SpeedState::default()));
    let cb_state = Arc::clone(&state);
    let on_progress = move |received: u64, expected: u64| {
        if length.swap(expected, std::sync::atomic::Ordering::Relaxed) != expected {
            bar.set_length(expected);
        }
        bar.set_position(received);
        if let Some(tab) = &tab {
            tab.update(received, expected);
        }

        // A poisoned lock only costs this one rate update; the bar still advances.
        if let Ok(mut s) = cb_state.lock() {
            let bps = s.observe(Instant::now(), received);
            bar.set_message(format!(
                "({}, {}) ",
                fmt_rate(bps),
                fmt_eta(expected.saturating_sub(received), bps)
            ));
        }
    };
    (on_progress, DeliveryMeter { state })
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
    funding: &RunFunding,
) -> anyhow::Result<(PoolContext, Option<LaneSpend>)>
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
        .map(|ctx| (ctx, None))
    } else {
        build_pool_ctx(
            store,
            contract,
            rpc,
            signer,
            provider,
            self_address,
            chain,
            endpoint,
            funding,
        )
        .await
        .map(|(ctx, spend)| (ctx, Some(spend)))
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
    provider: Address,
    self_address: Address,
    chain: &ResolvedChain,
    endpoint: &Endpoint,
    funding: &RunFunding,
) -> anyhow::Result<(PoolContext, LaneSpend)>
where
    P: alloy::providers::Provider + Clone,
{
    let (ctx, spend) = open_or_reuse_pool(
        store,
        contract,
        rpc,
        funding,
        signer,
        provider,
        self_address,
        chain.deployment(),
        chain.working_deposit,
        chain.max_approve,
        // The store this fetch writes was opened from `chain.data_dir`, and it
        // signs with `chain.keystore`; the verdict covers both.
        ChainAdoption::for_buy(&chain.data_dir, &chain.keystore)?,
    )
    .await?;
    Ok((attach_client_binding(ctx, chain, endpoint, signer)?, spend))
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
    let state = BuyerPoolState::new(
        pool_id,
        chain.deployment(),
        pool.owner,
        token,
        U256::from(pool.deposit),
    );
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

/// Adopt the live pool `owner` already holds on `deployment`, recording it in
/// the client store, or return `None` if the chain lists no `Open` pool with
/// deposit left to spend. The adopted row carries the pool's `totalRedeemed`
/// as [`BuyerPoolState::redeemed_elsewhere`].
///
/// The node adopts by the same rule at bootstrap (the newest `Open`, solvent
/// pool); the client does it on demand, because a client is a one-shot process
/// with no bootstrap to hang it on. The two differ on failure, deliberately. The
/// node fails open: an unreadable enumeration leaves the first miss to open a
/// fresh pool, because a daemon that could not buy for the rest of its life is
/// worse than an occasional stranded deposit. The client fails closed: a read
/// that faults errors rather than answering `None`, because a one-shot fetch can
/// simply be rerun, and "could not tell" must not become "go open another pool".
/// Making the two consistent would mean weakening one of them.
async fn adopt_owned_pool<P>(
    store: &RedbBuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    owner: Address,
    deployment: Deployment,
) -> anyhow::Result<Option<BuyerPoolState>>
where
    P: alloy::providers::Provider + Clone,
{
    let Some((pool_id, pool)) = newest_solvent_owned_pool(contract, owner).await.context(
        "could not ask the chain whether this wallet already owns a pool, so refusing to \
             open one blind — rerun, or check the RPC endpoint",
    )?
    else {
        return Ok(None);
    };
    let token =
        contract.usdc().call().await.with_context(|| {
            format!("read PaymentPool.usdc() while adopting live pool {pool_id}")
        })?;
    // The row has no lanes to account for what the pool already paid out, so it
    // keeps `totalRedeemed`, less what each lane it seeds takes over.
    let state = BuyerPoolState::adopt(
        pool_id,
        deployment,
        owner,
        token,
        U256::from(pool.deposit),
        U256::from(pool.totalRedeemed),
    );
    store.record(&state).with_context(|| {
        format!(
            "found live pool {pool_id} on chain but could not record it in the local buyer \
             store; nothing was escrowed, so rerunning retries the adoption"
        )
    })?;
    tracing::info!(
        %pool_id,
        deposit = pool.deposit,
        redeemed = pool.totalRedeemed,
        "adopted a live buyer pool this wallet already owns on chain, instead of opening a \
         second one"
    );
    Ok(Some(state))
}

/// The on-chain `(bytes, amount)` watermark for `lane` — what a lane with no
/// local record resumes from.
///
/// It reflects redeemed vouchers only, so a provider still holding one it has
/// not redeemed is ahead of it. That gap repairs itself inside the fetch: the
/// provider answers the first stale proof that its lane headroom cannot pay
/// with that watermark (the wallet-less resume, #1946), and the driver reseeds
/// from it — unless the resume budget is spent, where the fetch fails. Resuming below the chain
/// watermark has no such repair: every voucher at or below it redeems nothing
/// on chain, whatever the provider does.
async fn lane_watermark<P>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    lane: LaneKey,
) -> anyhow::Result<(U256, U256)>
where
    P: alloy::providers::Provider + Clone,
{
    let onchain = contract
        .getWatermark(lane.pool_id, lane.signer, lane.provider)
        .call()
        .await
        .context(
            "could not read this lane's on-chain watermark, so refusing to resume it from zero, \
             where it would stream bytes the provider cannot cash",
        )?;
    Ok((
        U256::from(onchain.bytesDelivered),
        U256::from(onchain.amount),
    ))
}

/// The pool this buy reuses, if any, and what that pool has already paid out
/// beyond what its row's lanes account for.
///
/// A tracked row is reused only if it is on `deployment` — the same chain and
/// the same `PaymentPool` address. A row from another deployment is not this
/// contract's pool, whatever its id says: `pool_id` is
/// `keccak256(owner, ownerPoolNonce)` and a redeploy restarts that nonce, so the
/// id alone will eventually name an existing, unrelated pool here, and its lane
/// progress would seed the first voucher at a cumulative this pool has never
/// redeemed against. It is treated as no row.
///
/// No row is not the same as no pool. The store loses rows — a reset data dir, a
/// new machine, a store-format change — while the pool they named is still open
/// on chain with deposit in it. Where `adoption` allows, the chain is asked
/// before anything is opened, so a lost row adopts the live pool instead of
/// escrowing a second deposit beside it, which reverts when the wallet's
/// remaining USDC cannot cover it.
///
/// An adopted row carries the pool's `totalRedeemed` as
/// [`BuyerPoolState::redeemed_elsewhere`], and keeps the part no seeded lane
/// has taken over on every later run.
/// The row has no lanes to account for what the pool paid out before
/// adoption, and without it the refill decision reads a pool other lanes
/// drained as a full deposit: no top-up fires, and the provider refuses the
/// fetch on a balance the client believes it has.
async fn pool_to_reuse<P>(
    store: &RedbBuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    self_address: Address,
    deployment: Deployment,
    adoption: ChainAdoption,
) -> anyhow::Result<Option<BuyerPoolState>>
where
    P: alloy::providers::Provider + Clone,
{
    let tracked = match store.get_by_owner(self_address)? {
        Some(state) if state.is_on(deployment) => Some(state),
        Some(state) => {
            tracing::warn!(
                pool_id = %state.pool_id,
                foreign_payment_pool = %state.deployment.payment_pool,
                foreign_chain_id = state.deployment.chain_id,
                configured_payment_pool = %deployment.payment_pool,
                configured_chain_id = deployment.chain_id,
                "ignoring a tracked buyer pool from another PaymentPool deployment; its deposit, \
                 if any, is recoverable only against `foreign_payment_pool`"
            );
            None
        }
        None => None,
    };
    Ok(match (tracked, adoption) {
        (Some(state), _) => Some(state),
        (None, ChainAdoption::Refused) => None,
        (None, ChainAdoption::Allowed) => {
            adopt_owned_pool(store, contract, self_address, deployment).await?
        }
    })
}

/// Record `lane`'s on-chain watermark `(bytes, amount)` in the row before the
/// lane resumes from it ([`BuyerPoolState::seed_lane`]), and return the row
/// with it and the progress the lane resumes from.
///
/// The pool spend the refill and the run's funding read
/// ([`BuyerPoolState::pool_spend`]) then counts the watermark once. On an
/// adopted row it moves out of the redeemed spend no lane accounts for, so the
/// spend keeps counting the other lanes' redemptions as this lane advances. A
/// zero watermark has nothing to record. A row another run already moved past
/// the watermark wins: the lane resumes from the row's progress.
///
/// # Errors
///
/// The buyer store could not commit the seed, or the row is gone or names
/// another pool now (another run forgot or replaced it). Either way the build
/// fails and retries from a fresh read: going on would price the lane, or
/// escrow a refill, against a row the store no longer holds.
fn seed_lane(
    store: &RedbBuyerPoolStore,
    mut state: BuyerPoolState,
    lane: LaneKey,
    bytes: U256,
    amount: U256,
) -> anyhow::Result<(BuyerPoolState, U256, U256)> {
    if bytes.is_zero() && amount.is_zero() {
        return Ok((state, bytes, amount));
    }
    let pool_id = state.pool_id;
    let outcome = store
        .seed_progress(state.owner, pool_id, lane, bytes, amount)
        .with_context(|| {
            format!(
                "record the on-chain watermark of lane {} in pool {pool_id}",
                lane.provider
            )
        })?;
    match outcome {
        AdvanceOutcome::Advanced => {
            // The in-memory row has no record of the lane, so the seed takes.
            if let Err(err) = state.seed_lane(lane, bytes, amount) {
                tracing::debug!(error = %err, "lane seed did not advance the in-memory row");
            }
            Ok((state, bytes, amount))
        }
        AdvanceOutcome::Regressed(_) => {
            let row = store.get_by_pool_id(pool_id)?.ok_or_else(|| {
                anyhow::anyhow!("buyer row for pool {pool_id} vanished during a lane seed")
            })?;
            let progress = row.lane_progress(lane).ok_or_else(|| {
                anyhow::anyhow!(
                    "buyer row for pool {pool_id} lost lane {} during a seed",
                    lane.provider
                )
            })?;
            Ok((row, progress.last_bytes, progress.last_amount))
        }
        AdvanceOutcome::UnknownPool | AdvanceOutcome::PoolMismatch => Err(anyhow::anyhow!(
            "the buyer row for pool {pool_id} was forgotten or replaced while lane {} was built; \
             the build retries from the store",
            lane.provider
        )),
    }
}

/// The error for a pool with no unspent deposit whose wallet cannot fund a
/// top-up: [`NoAffordableSource`], which is fatal to the command even inside a
/// lane build. Nothing a retry or another provider does can pay for the fetch.
fn cannot_pay(state: &BuyerPoolState, shortfall: &str) -> anyhow::Error {
    anyhow::Error::new(NoAffordableSource {
        deposit: state.deposit,
    })
    .context(format!(
        "buyer pool {} has no unspent deposit, and the wallet cannot fund a top-up: \
         {shortfall}",
        state.pool_id
    ))
}

/// Restore `state`'s remaining deposit (`deposit - spent`) to `working_deposit`
/// once it falls below the low water — see [`refill_amount`] — and return the
/// row to build the lane on.
///
/// `spent` is the pool-wide spend from [`BuyerPoolState::pool_spend`]. The refill is an
/// optimisation, so a wallet that holds too little USDC for it does not fail
/// the lane while the pool can still pay (and ends the command, as
/// [`NoAffordableSource`], once it cannot): the shortfall is logged at WARN,
/// recorded on `funding` for the run's closing `warning:` line, and no later
/// lane build of the run tries the refill again. Any other failure of the
/// allowance or `topUp` leg fails the lane build, which the acquire loop
/// retries with backoff. A `topUp` that was broadcast but returned no receipt
/// ([`TopUpUnconfirmed`]) may have escrowed, and once `topUp` returns a
/// receipt the escrow has moved, so a failure to credit or re-read the local
/// row ([`escrowed_but_untracked`]) is fatal: the acquire loop ends the
/// command instead of retrying, because a retry escrows again.
#[allow(clippy::too_many_arguments)]
async fn refill_if_low<P>(
    store: &RedbBuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    rpc: &P,
    funding: &RunFunding,
    state: BuyerPoolState,
    self_address: Address,
    spent: U256,
    working_deposit: U256,
    max_approve: bool,
) -> anyhow::Result<BuyerPoolState>
where
    P: alloy::providers::Provider + Clone,
{
    let low_water = working_deposit / U256::from(LOW_WATER_DIVISOR);
    let additional = refill_amount(state.deposit, spent, working_deposit, low_water);
    if additional.is_zero() {
        return Ok(state);
    }
    let remaining = state.deposit.saturating_sub(spent);
    if let Some(shortfall) = funding.shortfall() {
        // The wallet could not fund a top-up earlier in this run; asking again
        // costs an allowance read and a reverting `topUp` estimate per lane.
        if remaining.is_zero() {
            return Err(cannot_pay(&state, &shortfall));
        }
        return Ok(state);
    }
    tracing::info!(
        "buyer pool {} low on deposit ({remaining} µUSDC remaining of {} deposited); \
         topping up {additional} µUSDC",
        state.pool_id,
        state.deposit,
    );
    // `topUp` pulls `additional` USDC via `transferFrom`, so the pool's
    // standing allowance must cover it first. Ensure it in the caller's
    // mode: unlimited under `--max-approve`, else exactly `additional`.
    let escrowed = async {
        ensure_allowance(
            rpc,
            state.token,
            self_address,
            *contract.address(),
            if max_approve { None } else { Some(additional) },
        )
        .await?;
        top_up(contract, state.pool_id, additional).await
    }
    .await;
    let err = match escrowed {
        Ok(ToppedUpPool { credited, tx, .. }) => {
            // The USDC is escrowed the moment `topUp` mines. A local credit that
            // does not land leaves the deposit untracked, and continuing would
            // fetch on a `state.deposit` that understates the chain — so the
            // low-water check re-fires on every later fetch while nobody
            // reconciles the escrow. No bytes have been paid for on *this* entry
            // yet — `bundle pull` reaches this once per entry, through
            // `open_or_reuse_pool`, so earlier entries may already be paid for and
            // written — and only the escrow moved, so failing here strands nothing
            // in flight. The reactive mid-fetch leg takes the same disposition
            // (`CliFunder::top_up`).
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
            return store
                .get_by_pool_id(state.pool_id)?
                .ok_or_else(|| escrowed_but_untracked(&effect, tx, "the credited row vanished"));
        }
        Err(err) => err,
    };
    if err.downcast_ref::<TopUpUnconfirmed>().is_some() {
        return Err(err);
    }
    let Some(shortfall) = funding
        .check_wallet(
            rpc,
            state.token,
            self_address,
            state.pool_id,
            additional,
            &err,
        )
        .await
    else {
        return Err(err);
    };
    if remaining.is_zero() {
        return Err(cannot_pay(&state, &shortfall));
    }
    // `check_wallet` already logged the shortfall at WARN.
    tracing::info!(
        pool_id = %state.pool_id,
        %remaining,
        "continuing on the pool's remaining deposit (µUSDC) without a top-up"
    );
    Ok(state)
}

/// Reuse the caller's live pool (resuming `provider`'s lane watermark), or open
/// and persist a new one. A reused pool whose pool-wide remaining deposit has
/// run low gets an on-chain `topUp` before it is returned; a wallet too short
/// of USDC for it still returns the pool while it can pay — see
/// [`refill_if_low`]. The lane then joins `funding`, the run's view of the
/// pool's spend outside its lanes ([`RunFunding::join_lane`]). There is no
/// pool expiry (ADR 003), so
/// there is no replace-on-expiry branch: the same pool is reused for the
/// caller's whole lifetime, across every provider.
///
/// `deployment` is the chain and `PaymentPool` that `contract` talks to: a
/// tracked row is reused only on it, a new or adopted row carries it, and the
/// voucher EIP-712 domain derives from it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn open_or_reuse_pool<P>(
    store: &RedbBuyerPoolStore,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    rpc: &P,
    funding: &RunFunding,
    signer: &Arc<PrivateKeySigner>,
    provider: Address,
    self_address: Address,
    deployment: Deployment,
    working_deposit: U256,
    max_approve: bool,
    adoption: ChainAdoption,
) -> anyhow::Result<(PoolContext, LaneSpend)>
where
    P: alloy::providers::Provider + Clone,
{
    let payment_pool_addr = deployment.payment_pool;
    let tracked = pool_to_reuse(store, contract, self_address, deployment, adoption).await?;
    if let Some(state) = tracked {
        let lane = LaneKey {
            pool_id: state.pool_id,
            signer: self_address,
            provider,
        };
        let recorded = state.lane_progress(lane).is_some();
        let from_chain = !state.redeemed_elsewhere().is_zero();
        let (state, prior_bytes, prior_amount) = if let Some(p) = state.lane_progress(lane) {
            (state, p.last_bytes, p.last_amount)
        } else {
            // No local record of this lane — always so for a pool just adopted, and
            // for a tracked pool's first contact with a provider. The chain may
            // still hold a watermark for it, and a voucher at or below that
            // watermark redeems nothing — so resuming from zero would stream bytes
            // the provider can never cash. Resume from the chain.
            let (bytes, amount) = lane_watermark(contract, lane).await?;
            seed_lane(store, state, lane, bytes, amount)?
        };

        // Auto-refill a live pool whose remaining deposit has run low, so a
        // sustained series of fetches isn't stranded by a spent-down deposit.
        let spent = state.pool_spend();
        let state = refill_if_low(
            store,
            contract,
            rpc,
            funding,
            state,
            self_address,
            spent,
            working_deposit,
            max_approve,
        )
        .await?;
        let lane_spend = LaneSpend {
            outside: spent.saturating_sub(prior_amount),
            prior: prior_amount,
            recorded,
            from_chain,
        };
        return self_owned_lane_ctx(
            &state,
            signer,
            &deployment.voucher_domain(),
            provider,
            prior_bytes,
            prior_amount,
        )
        .map(|ctx| (ctx, lane_spend));
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
        deployment,
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
    // A fresh pool has spent nothing on any lane.
    let spend = LaneSpend {
        outside: U256::ZERO,
        prior: U256::ZERO,
        recorded: false,
        from_chain: false,
    };
    Ok((
        opened
            .ctx
            .with_provider(provider, U256::ZERO, U256::ZERO)
            .with_capability(opened.capability),
        spend,
    ))
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
    use decdn_incentive::buyer_pool::BuyerLaneProgress;

    /// A node that sheds this client's probes is named as rate-limited, apart
    /// from the silent ones; with none, the message keeps its two counts.
    #[test]
    fn no_serve_target_error_names_rate_limited_nodes_apart_from_silent_ones() {
        let msg = no_serve_target_error(3, 1, 2, 0).to_string();
        assert!(msg.contains("1 did not answer"), "{msg}");
        assert!(msg.contains("2 rate-limited this client's probes"), "{msg}");
        assert!(msg.contains("0 answered but were unusable"), "{msg}");

        let msg = no_serve_target_error(3, 3, 0, 0).to_string();
        assert!(!msg.contains("rate-limited"), "{msg}");
        assert!(msg.contains("3 did not answer, 0 answered"), "{msg}");
    }

    /// A probe outcome after `ms` on the paused clock: `(id, holder)`.
    async fn answer_after(ms: u64, id: u32, holder: bool) -> (u32, bool) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        (id, holder)
    }

    /// Every probe that answers within the grace after the first holder is kept.
    #[tokio::test(start_paused = true)]
    async fn settle_keeps_every_answer_inside_the_grace() {
        let grace = Duration::from_millis(250);
        let probes = vec![
            answer_after(20, 1, true),
            answer_after(150, 2, false),
            answer_after(260, 3, true),
        ];
        let (mut got, tail) = settle_probes(probes, grace, None, |&(_, h)| h, |_| true).await;
        got.sort_unstable();
        assert_eq!(got, vec![(1, true), (2, false), (3, true)]);
        assert!(tail.is_empty());
    }

    /// A probe still pending at the first holder's answer plus the grace is
    /// left in the tail, and the round ends at that moment.
    #[tokio::test(start_paused = true)]
    async fn settle_leaves_a_straggler_past_the_grace_in_the_tail() {
        let grace = Duration::from_millis(250);
        let started = tokio::time::Instant::now();
        let probes = vec![
            answer_after(25, 1, true),
            answer_after(170, 2, false),
            answer_after(1850, 3, true),
        ];
        let (mut got, tail) = settle_probes(probes, grace, None, |&(_, h)| h, |_| true).await;
        got.sort_unstable();
        assert_eq!(got, vec![(1, true), (2, false)]);
        assert_eq!(tail.len(), 1);
        assert_eq!(started.elapsed(), Duration::from_millis(275));
    }

    /// With no holder, the round waits for every probe.
    #[tokio::test(start_paused = true)]
    async fn settle_waits_for_every_probe_without_a_holder() {
        let grace = Duration::from_millis(250);
        let started = tokio::time::Instant::now();
        let probes = vec![answer_after(25, 1, false), answer_after(1850, 2, false)];
        let (got, tail) = settle_probes(probes, grace, None, |&(_, h)| h, |_| true).await;
        assert_eq!(got.len(), 2);
        assert!(tail.is_empty());
        assert_eq!(started.elapsed(), Duration::from_millis(1850));
    }

    /// A streamed round returns at the first holder; the straggler is in the
    /// tail and completes when polled.
    #[tokio::test(start_paused = true)]
    async fn settle_streams_from_the_first_holder() {
        use futures_util::StreamExt as _;
        let started = tokio::time::Instant::now();
        let probes = vec![
            answer_after(25, 1, true),
            answer_after(170, 2, false),
            answer_after(1850, 3, true),
        ];
        let (got, mut tail) =
            settle_probes(probes, Duration::ZERO, None, |&(_, h)| h, |_| true).await;
        assert_eq!(got, vec![(1, true)]);
        assert_eq!(started.elapsed(), Duration::from_millis(25));
        let mut late = Vec::new();
        while let Some(outcome) = tail.next().await {
            late.push(outcome);
        }
        assert_eq!(late, vec![(2, false), (3, true)]);
    }

    /// An answer already in when the first holder answers is collected, not
    /// left in the tail.
    #[tokio::test(start_paused = true)]
    async fn settle_collects_answers_ready_with_the_first_holder() {
        let probes = vec![
            answer_after(25, 1, true),
            answer_after(25, 2, false),
            answer_after(900, 3, true),
        ];
        let (mut got, tail) =
            settle_probes(probes, Duration::ZERO, None, |&(_, h)| h, |_| true).await;
        got.sort_unstable();
        assert_eq!(got, vec![(1, true), (2, false)]);
        assert_eq!(tail.len(), 1);
    }

    /// A late probe that records when it answers, then reports `Unreachable`.
    fn recording_probe(
        after: Duration,
        answered: &Arc<std::sync::Mutex<Option<Duration>>>,
    ) -> impl std::future::Future<Output = ProbeOutcome> + Send + 'static {
        let answered = Arc::clone(answered);
        let started = tokio::time::Instant::now();
        async move {
            tokio::time::sleep(after).await;
            if let Ok(mut slot) = answered.lock() {
                *slot = Some(started.elapsed());
            }
            ProbeOutcome::Unreachable
        }
    }

    fn no_warming() -> ProxyWarmingParams {
        ProxyWarmingParams {
            enabled: false,
            rtt_threshold_ms: 150.0,
            margin_ms: 30.0,
        }
    }

    /// A streamed round's pending probes keep running while nothing polls the
    /// tail, so a slow unlock between the round and the fetch neither times
    /// them out nor inflates their RTT. The answer waits in the tail.
    #[tokio::test(start_paused = true)]
    async fn late_probes_run_while_the_tail_is_not_polled() {
        use futures_util::StreamExt as _;
        let answered = Arc::new(std::sync::Mutex::new(None));
        let tail: futures_util::stream::FuturesUnordered<_> =
            std::iter::once(recording_probe(Duration::from_millis(100), &answered)).collect();
        let slot = late_slot(ProbeRound::Stream, tail, &[], no_warming());

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(*answered.lock().unwrap(), Some(Duration::from_millis(100)));
        let mut late = slot.take().expect("a streamed round keeps its tail");
        assert!(matches!(
            late.tail.next().await,
            Some(ProbeOutcome::Unreachable)
        ));
        assert!(late.tail.next().await.is_none());
    }

    /// Dropping the late probes stops the ones still running.
    #[tokio::test(start_paused = true)]
    async fn late_probes_stop_when_dropped() {
        let answered = Arc::new(std::sync::Mutex::new(None));
        let tail: futures_util::stream::FuturesUnordered<_> =
            std::iter::once(recording_probe(Duration::from_secs(1), &answered)).collect();
        let slot = late_slot(ProbeRound::Stream, tail, &[], no_warming());
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(slot);
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(*answered.lock().unwrap(), None);
    }

    /// Without a cold cutoff, a round with no holder waits for every probe and
    /// leaves no tail.
    #[tokio::test(start_paused = true)]
    async fn settle_without_a_cold_cutoff_waits_for_every_probe() {
        let probes = vec![answer_after(25, 1, false), answer_after(1850, 2, false)];
        let (got, tail) = settle_probes(probes, Duration::ZERO, None, |&(_, h)| h, |_| true).await;
        assert_eq!(got.len(), 2);
        assert!(tail.is_empty());
    }

    /// With no holder in hand, a cold cutoff ends the round its grace after
    /// the first answer; the late holder stays in the tail.
    #[tokio::test(start_paused = true)]
    async fn settle_cold_cutoff_returns_the_non_holders_in_hand() {
        use futures_util::StreamExt as _;
        let started = tokio::time::Instant::now();
        let probes = vec![
            answer_after(25, 1, false),
            answer_after(100, 2, false),
            answer_after(1850, 3, true),
        ];
        let cold = Some(Duration::from_millis(250));
        let (got, mut tail) =
            settle_probes(probes, Duration::ZERO, cold, |&(_, h)| h, |_| true).await;
        assert_eq!(got, vec![(1, false), (2, false)]);
        assert_eq!(started.elapsed(), Duration::from_millis(275));
        assert_eq!(tail.next().await, Some((3, true)));
    }

    /// A holder that answers inside the cold window ends the round at once,
    /// as it would with no window.
    #[tokio::test(start_paused = true)]
    async fn settle_cold_cutoff_yields_to_a_holder() {
        let started = tokio::time::Instant::now();
        let probes = vec![
            answer_after(25, 1, false),
            answer_after(120, 2, true),
            answer_after(1850, 3, false),
        ];
        let cold = Some(Duration::from_millis(250));
        let (got, tail) = settle_probes(probes, Duration::ZERO, cold, |&(_, h)| h, |_| true).await;
        assert_eq!(got, vec![(1, false), (2, true)]);
        assert_eq!(started.elapsed(), Duration::from_millis(120));
        assert_eq!(tail.len(), 1);
    }

    /// Only a verified answer opens the cold window: a failed probe does not.
    #[tokio::test(start_paused = true)]
    async fn settle_cold_cutoff_ignores_failed_probes() {
        let started = tokio::time::Instant::now();
        // `(id, holder)`; id 9 stands for a failed probe.
        let probes = vec![answer_after(10, 9, false), answer_after(600, 1, false)];
        let cold = Some(Duration::from_millis(250));
        let (got, tail) = settle_probes(
            probes,
            Duration::ZERO,
            cold,
            |&(_, h)| h,
            |&(id, _)| id != 9,
        )
        .await;
        assert_eq!(got.len(), 2);
        assert!(tail.is_empty());
        assert_eq!(started.elapsed(), Duration::from_millis(600));
    }

    /// A loopback probe server that closes every connection with `code`, and
    /// counts the connections it saw.
    async fn closing_probe_server(
        code: u32,
    ) -> (Endpoint, EndpointAddr, Arc<std::sync::atomic::AtomicUsize>) {
        use iroh::endpoint::{RelayMode, presets};
        let ep = Endpoint::builder(presets::Minimal)
            .alpns(vec![decdn_protocol::ALPN_PROBE.to_vec()])
            .relay_mode(RelayMode::Disabled)
            .bind_addr(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::LOCALHOST,
                0,
            ))
            .unwrap()
            .bind()
            .await
            .unwrap();
        let port = ep
            .bound_sockets()
            .into_iter()
            .find(std::net::SocketAddr::is_ipv4)
            .unwrap()
            .port();
        let addr = EndpointAddr::new(ep.id()).with_ip_addr(std::net::SocketAddr::from((
            std::net::Ipv4Addr::LOCALHOST,
            port,
        )));
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (server, count) = (ep.clone(), Arc::clone(&seen));
        tokio::spawn(async move {
            while let Some(incoming) = server.accept().await {
                let Ok(conn) = incoming.await else { continue };
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                conn.close(code.into(), b"per_peer");
                // Wait for the close to reach the client before the next accept,
                // as the node's probe limiter does.
                conn.closed().await;
            }
        });
        (ep, addr, seen)
    }

    async fn client_endpoint() -> Endpoint {
        use iroh::endpoint::{RelayMode, presets};
        Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind_addr(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::LOCALHOST,
                0,
            ))
            .unwrap()
            .bind()
            .await
            .unwrap()
    }

    /// A candidate that sheds every probe with `APP_ERR_RATE_LIMITED` is probed
    /// once plus once after each backoff, and its error still reads as a shed —
    /// so `probe_and_order` counts it as rate-limited, not silent.
    #[tokio::test(flavor = "multi_thread")]
    async fn probe_candidate_retries_a_shedding_candidate_then_reports_the_shed() {
        let (server, target, seen) =
            closing_probe_server(decdn_protocol::APP_ERR_RATE_LIMITED).await;
        let client = client_endpoint().await;
        let started = Instant::now();

        let res = probe_candidate(&client, target, [7; 32], micros_now()).await;

        let err = res.expect_err("every probe was shed");
        assert!(probe_shed(&err), "the shed survives the retries: {err:#}");
        assert_eq!(
            seen.load(std::sync::atomic::Ordering::SeqCst),
            PROBE_SHED_BACKOFFS_MS.len() + 1
        );
        let waited: u64 = PROBE_SHED_BACKOFFS_MS.iter().sum();
        assert!(started.elapsed() >= Duration::from_millis(waited));
        client.close().await;
        server.close().await;
    }

    /// Any other failure returns at once: a node that closes with a code other
    /// than `APP_ERR_RATE_LIMITED` is not probed again.
    #[tokio::test(flavor = "multi_thread")]
    async fn probe_candidate_does_not_retry_a_failure_that_is_not_a_shed() {
        let (server, target, seen) = closing_probe_server(0).await;
        let client = client_endpoint().await;

        let res = probe_candidate(&client, target, [7; 32], micros_now()).await;

        let err = res.expect_err("the connection was closed");
        assert!(!probe_shed(&err), "{err:#}");
        assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 1);
        client.close().await;
        server.close().await;
    }

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
            max_sources: 4,
            give_up_after_secs: None,
            chain_id: None,
            keystore: None,
            keystore_password_file: None,
            data_dir: Some(PathBuf::from("/tmp/d")),
            working_deposit_micro_usdc: None,
            max_blob_mb: 1024,
            max_rate_per_mb: 0,
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

    /// `-o -` streams to stdout; every other path — including a file literally
    /// named `-` addressed as `./-` — writes to disk. Only the exact single-dash
    /// output selects the stdout stream (#1848 4b).
    #[test]
    fn dash_output_selects_the_stdout_stream() {
        assert!(wants_stdout(Path::new("-")));
        assert!(!wants_stdout(Path::new("./-")));
        assert!(!wants_stdout(Path::new("-.bin")));
        assert!(!wants_stdout(Path::new("out.bin")));
    }

    /// A stdout stream that failed because the fetch gave up ends with the
    /// fetch's typed `GaveUp`, not the reader's message, so it exits 75; with
    /// no fetch error, the read or write error stands.
    #[test]
    fn a_stdout_give_up_keeps_its_type() {
        let gave_up = decdn_client::GaveUp {
            idle: Duration::from_mins(10),
        };
        let read_err = anyhow::anyhow!("read verified stream: stream fetch failed: {gave_up}");
        let err = stream_error(read_err, Some(anyhow::Error::new(gave_up)));
        assert_eq!(err.downcast_ref::<decdn_client::GaveUp>(), Some(&gave_up));

        let write_err = stream_error(anyhow::anyhow!("write to stdout: broken pipe"), None);
        assert!(write_err.downcast_ref::<decdn_client::GaveUp>().is_none());
        assert!(write_err.to_string().contains("broken pipe"));
    }

    /// `copy_verified` drains every byte to the sink in order and reports the
    /// final cumulative position as the blob's total — the stdout stream's bar
    /// signal (#1848 4b).
    #[tokio::test]
    async fn copy_verified_streams_all_bytes_and_reports_final_total() -> anyhow::Result<()> {
        let data = vec![7u8; 5000];
        let total = u64::try_from(data.len())?;
        let mut reader: &[u8] = &data;
        let mut sink: Vec<u8> = Vec::new();

        let last = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let observed = Arc::clone(&last);
        let cb = move |pos: u64, _total: u64| {
            observed.store(pos, std::sync::atomic::Ordering::SeqCst);
        };
        let cb_ref: &dyn Fn(u64, u64) = &cb;

        let drained = copy_verified(&mut reader, &mut sink, total, Some(cb_ref)).await?;

        assert_eq!(drained, total, "every byte is drained");
        assert_eq!(sink, data, "the sink receives the bytes in order");
        assert_eq!(
            last.load(std::sync::atomic::Ordering::SeqCst),
            total,
            "the final progress report reaches the blob's total"
        );
        Ok(())
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

        let store = decdn_client::PeerStore::open(dir.path());
        assert!(store.get(&harvest_key(2)).is_some());
        let r = store.get(&harvest_key(1)).expect("probed peer persisted");
        assert_eq!(r.latency_ms, Some(42.0));
        assert_eq!(r.rate_per_mb, Some(9));
    }

    #[test]
    fn harvest_counts_every_write_the_store_refuses() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A file where the store's directory belongs makes every write fail.
        std::fs::write(dir.path().join("peers"), b"").expect("block the store dir");
        let store = decdn_client::PeerStore::open(dir.path());
        let regs = vec![harvest_candidate(1), harvest_candidate(2)];
        let probed = vec![(harvest_key(1), 42.0_f64, 9_u64)];
        let cfg = decdn_client::StoreConfig::default();
        let tally = harvest(&store, &regs, probed, now_secs_cli(), &cfg);
        assert_eq!(
            tally,
            HarvestTally {
                writes: 3,
                failed: 3
            }
        );
    }

    /// `select_with_widening` re-runs unfiltered when a region allowlist filters
    /// the pool below the freshness floor, so a too-thin allowlist never starves
    /// the fetch; an empty allowlist filters nothing.
    #[test]
    fn select_with_widening_widens_when_allowlist_starves() {
        let cfg = decdn_client::StoreConfig::default();
        let de = decdn_protocol::Region::parse("DE");
        let cands: Vec<NodeCandidate> = (1u8..=4)
            .map(|b| NodeCandidate {
                node_id: harvest_key(b),
                eth_address: Address::from([b; 20]),
                region_hint: de,
                multiaddrs: alloy::primitives::Bytes::new(),
            })
            .collect();
        let us = decdn_protocol::Region::parse("US").expect("US is a valid region");

        // Allowlist excludes every candidate's region -> filtered to zero ->
        // below the floor -> widening re-runs unfiltered and keeps all four.
        let widened = select_with_widening(cands.clone(), None, &[us], &cfg);
        assert_eq!(widened.len(), 4);

        // No allowlist -> nothing filtered, nothing to widen.
        let all = select_with_widening(cands, None, &[], &cfg);
        assert_eq!(all.len(), 4);
    }

    /// `cached_pool_token` returns the persisted pool's immutable token without
    /// any contract read, and `None` (so the caller reads `usdc()` on-chain) when
    /// no pool row exists for the owner yet.
    #[test]
    fn cached_pool_token_uses_persisted_row_and_skips_rpc() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        // A fresh subpath so the store's `ensure_data_dir` creates it at 0o700;
        // the tempdir root itself is 0o755, which the store rejects.
        let store = RedbBuyerPoolStore::open(&dir.path().join("data"))?;
        let owner = Address::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let deployment = Deployment {
            chain_id: 421_614,
            payment_pool: Address::repeat_byte(0x9c),
        };

        // No row yet -> None -> caller must read usdc() on the open path.
        assert_eq!(cached_pool_token(&store, owner, deployment)?, None);

        // Persist a pool row for this owner.
        let state = BuyerPoolState::new(
            B256::repeat_byte(0xAB),
            deployment,
            owner,
            token,
            U256::from(1_000u64),
        );
        store.record(&state)?;

        // Row present -> the immutable token comes back with no contract read.
        assert_eq!(cached_pool_token(&store, owner, deployment)?, Some(token));
        // A different owner has no row -> still None.
        assert_eq!(
            cached_pool_token(&store, Address::repeat_byte(0x33), deployment)?,
            None
        );
        // The row is the same, but it names another deployment's `usdc()`.
        // Answering with it would approve and price the wrong token. The
        // address alone does not name a deployment: the same address on
        // another chain is another contract.
        let other_address = Deployment {
            payment_pool: Address::repeat_byte(0xDE),
            ..deployment
        };
        let other_chain = Deployment {
            chain_id: 1,
            ..deployment
        };
        for foreign in [other_address, other_chain] {
            assert_eq!(
                cached_pool_token(&store, owner, foreign)?,
                None,
                "a row from another PaymentPool deployment must not seed the token cache \
                 ({foreign:?})"
            );
        }
        Ok(())
    }

    /// `identity_fresh_candidates` gathers cached peers whose identity is fresh
    /// (within the refresh horizon) and not suppressed/prunable, regardless of
    /// latency freshness — the set the registry-read-skip path re-probes.
    #[test]
    fn identity_fresh_candidates_gathers_latency_stale_peers() -> anyhow::Result<()> {
        let cfg = decdn_client::StoreConfig::default();
        let dir = tempfile::tempdir()?;
        let store = decdn_client::PeerStore::open(dir.path());
        let now = 1_000_000;

        // Three identity-fresh peers whose latency is STALE (sampled long ago):
        // store_fast_path would reject them, but the registry-skip path keeps them.
        for b in [1u8, 2, 3] {
            store.upsert_identity(&harvest_candidate(b), now)?;
            let stale = now - cfg.latency_ttl_secs - 1;
            store.record_sample(&harvest_key(b), 30.0, 1, stale, &cfg)?;
        }
        // Identity-stale peer (>24h since confirmed) -> excluded.
        store.upsert_identity(&harvest_candidate(4), now - cfg.identity_refresh_secs - 1)?;
        // Freshly-failed peer -> suppressed -> excluded.
        store.upsert_identity(&harvest_candidate(5), now)?;
        store.record_failure(&harvest_key(5), now)?;

        let cands = identity_fresh_candidates(&store, &cfg, now);
        let ids: std::collections::HashSet<_> = cands.iter().map(|c| c.node_id).collect();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&harvest_key(1)));
        assert!(ids.contains(&harvest_key(2)));
        assert!(ids.contains(&harvest_key(3)));
        assert!(!ids.contains(&harvest_key(4)));
        assert!(!ids.contains(&harvest_key(5)));
        Ok(())
    }

    /// A pinned `--node-id` is one probed-as-holder candidate with no size
    /// hint (#2218): its first claim comes from a header-only open.
    #[test]
    fn a_pinned_node_resolves_with_no_size_hint() {
        let provider = Address::repeat_byte(0x42);
        let targets = super::pinned_targets(harvest_key(7), provider);
        assert!(targets.pinned);
        assert!(targets.size_hint.is_none());
        assert_eq!(targets.candidates.len(), 1);
        assert_eq!(
            targets.candidates.first().map(|c| c.eth_address),
            Some(provider)
        );
    }

    /// `store_fast_path` ranks selectable records by latency and excludes a
    /// suppressed one, and refuses to engage below `min_fresh_candidates`. It
    /// probes nothing, so it carries no size hint (#2218).
    /// It takes no [`Endpoint`], so it structurally cannot issue a network
    /// probe — this is the probe-less fast path itself, not merely tested
    /// without one.
    #[test]
    fn fast_path_needs_min_fresh_and_ranks_by_latency() -> anyhow::Result<()> {
        let cfg = decdn_client::StoreConfig::default();
        let dir = tempfile::tempdir()?;
        let store = decdn_client::PeerStore::open(dir.path());
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
        assert!(
            targets.size_hint.is_none(),
            "nothing was probed, so the first claim comes from a header open"
        );

        // Only two selectable records -> below min_fresh_candidates -> None.
        let dir2 = tempfile::tempdir()?;
        let s2 = decdn_client::PeerStore::open(dir2.path());
        for b in [1u8, 2] {
            s2.upsert_identity(&harvest_candidate(b), now)?;
            s2.record_sample(&harvest_key(b), 30.0, 1, now, &cfg)?;
        }
        assert!(store_fast_path(&s2, &cfg, 4, now).is_none());
        Ok(())
    }

    /// #2196: nearby nodes serving cold misses from a distant origin open slowly,
    /// but a stream open is not a distance sample. After repeated opens on the
    /// near nodes, the fast path still ranks them by probe RTT, ahead of the
    /// origin.
    #[test]
    fn stream_opens_do_not_rank_near_nodes_behind_the_origin() -> anyhow::Result<()> {
        let cfg = decdn_client::StoreConfig::default();
        let dir = tempfile::tempdir()?;
        let store = decdn_client::PeerStore::open(dir.path());
        let now = now_secs_cli();
        // Probe RTTs: two nearby caches and the transatlantic origin.
        for (b, rtt) in [(1u8, 57.0), (2, 78.0), (3, 394.0)] {
            store.upsert_identity(&harvest_candidate(b), now)?;
            store.record_sample(&harvest_key(b), rtt, 1, now, &cfg)?;
        }
        // A lane's first open files its quoted rate and no latency
        // (`CliSources::record_open`).
        for _ in 0..5 {
            for b in [1u8, 2] {
                store.record_open(&harvest_key(b), 4)?;
            }
        }

        let near = store
            .get(&harvest_key(1))
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        assert_eq!(near.latency_ms, Some(57.0));
        assert_eq!(near.rate_per_mb, Some(4));
        let targets = store_fast_path(&store, &cfg, 4, now)
            .ok_or_else(|| anyhow::anyhow!("expected Some"))?;
        let order: Vec<_> = targets.candidates.iter().map(|c| c.node_id).collect();
        assert_eq!(order, [harvest_key(1), harvest_key(2), harvest_key(3)]);
        Ok(())
    }

    /// A failed first open stamps the peer, so the fast path skips it, and
    /// leaves its probe RTT alone.
    #[test]
    fn failed_first_open_suppresses_the_peer() -> anyhow::Result<()> {
        let cfg = decdn_client::StoreConfig::default();
        let dir = tempfile::tempdir()?;
        let store = decdn_client::PeerStore::open(dir.path());
        let now = now_secs_cli();
        for (b, rtt) in [(1u8, 57.0), (2, 78.0), (3, 90.0), (4, 394.0)] {
            store.upsert_identity(&harvest_candidate(b), now)?;
            store.record_sample(&harvest_key(b), rtt, 1, now, &cfg)?;
        }
        // A lane's delivery fault stamps its peer (`CliSources::on_source_fault`).
        store.record_failure(&harvest_key(1), now)?;

        let failed = store
            .get(&harvest_key(1))
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        assert!(failed.last_failure_at_secs.is_some());
        assert_eq!(failed.latency_ms, Some(57.0));
        let targets = store_fast_path(&store, &cfg, 4, now)
            .ok_or_else(|| anyhow::anyhow!("expected Some"))?;
        let order: Vec<_> = targets.candidates.iter().map(|c| c.node_id).collect();
        assert_eq!(order, [harvest_key(2), harvest_key(3), harvest_key(4)]);
        Ok(())
    }

    fn ctx_with(binding: Option<decdn_protocol::client::ClientBinding>) -> PoolContext {
        ctx_with_deposit(binding, U256::ZERO)
    }

    fn ctx_with_deposit(
        binding: Option<decdn_protocol::client::ClientBinding>,
        deposit: U256,
    ) -> PoolContext {
        PoolContext {
            pool_id: B256::ZERO,
            provider: Address::ZERO,
            deposit,
            client_signer: Arc::new(PrivateKeySigner::random()),
            voucher_domain: bind_node_id_domain(1, Address::ZERO),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: binding,
            capability: None,
        }
    }

    /// A registry-backed `lane_ledger` call for the same lane must return the
    /// SAME ledger `Arc` on a second call (no fresh mint per fetch/chunk) — the
    /// invariant `LaneLedgers::get_or_insert` exists to guarantee. A `None`
    /// registry (standalone `decdn fetch`) must mint a distinct ledger every
    /// call.
    #[test]
    fn lane_ledger_shares_one_arc_through_the_registry_but_not_without_one() {
        let lane = LaneKey {
            pool_id: B256::ZERO,
            signer: Address::ZERO,
            provider: Address::from([7u8; 20]),
        };
        let registry = LaneLedgers::new();
        let (ledger_a, ctx_a) = lane_ledger(Some(&registry), lane, ctx_with(None));
        let (ledger_b, ctx_b) = lane_ledger(Some(&registry), lane, ctx_with(None));
        assert!(Arc::ptr_eq(&ledger_a, &ledger_b));
        assert!(Arc::ptr_eq(&ctx_a, &ctx_b));

        let (ledger_none_a, _) = lane_ledger(None, lane, ctx_with(None));
        let (ledger_none_b, _) = lane_ledger(None, lane, ctx_with(None));
        assert!(!Arc::ptr_eq(&ledger_none_a, &ledger_none_b));
    }

    /// A second `lane_ledger` touch of an already-registered lane must reconcile
    /// the shared handle's `ctx.deposit` UPWARD to a freshly-read higher deposit
    /// (a low-water top-up `open_or_reuse_pool` performed between the two
    /// touches) rather than silently discarding it. The pool-wide spent/credit
    /// gate reads this shared value, so a stale low deposit would refuse
    /// (`PoolExhausted`) prematurely even though the on-chain balance grew.
    #[test]
    fn lane_ledger_reconciles_shared_deposit_upward_on_reuse() {
        let lane = LaneKey {
            pool_id: B256::ZERO,
            signer: Address::ZERO,
            provider: Address::from([9u8; 20]),
        };
        let registry = LaneLedgers::new();
        let (_, ctx_a) = lane_ledger(
            Some(&registry),
            lane,
            ctx_with_deposit(None, U256::from(100)),
        );
        assert_eq!(
            ctx_a.lock().unwrap_or_else(PoisonError::into_inner).deposit,
            U256::from(100)
        );

        // A later touch on the same lane with a higher freshly-read deposit
        // (simulating a top-up) must raise the shared handle, not discard it.
        let (_, ctx_b) = lane_ledger(
            Some(&registry),
            lane,
            ctx_with_deposit(None, U256::from(250)),
        );
        assert!(Arc::ptr_eq(&ctx_a, &ctx_b));
        assert_eq!(
            ctx_b.lock().unwrap_or_else(PoisonError::into_inner).deposit,
            U256::from(250)
        );

        // A lower freshly-read deposit than the shared handle's current value
        // must never lower it (deposit only ever grows via top-ups).
        let (_, ctx_c) = lane_ledger(
            Some(&registry),
            lane,
            ctx_with_deposit(None, U256::from(10)),
        );
        assert_eq!(
            ctx_c.lock().unwrap_or_else(PoisonError::into_inner).deposit,
            U256::from(250)
        );
    }

    /// The refusal these tests annotate, built the way the fetch path builds it: the typed
    /// `UpstreamRefused` sentinel (#1144). Never hand-roll one with
    /// `anyhow!("delivery refused: …")` — the annotation downcasts, so a look-alike string
    /// would exercise nothing and pass against a hint that never fires in production.
    fn refusal(error: StreamError) -> anyhow::Error {
        anyhow::Error::new(UpstreamRefused::mid_stream(error))
    }

    fn node_key(seed: u8) -> PublicKey {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn holder(seed: u8, rtt_ms: f64) -> discovery::Probed {
        super::tests_support::holder(seed, rtt_ms)
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
                multiaddrs: Bytes::new(),
            },
            discovery::WarmingCandidate {
                node_id: node_key(22),
                eth_address: Address::repeat_byte(22),
                rtt_ms: 150.0,
                multiaddrs: Bytes::new(),
            },
            discovery::WarmingCandidate {
                node_id: node_key(23),
                eth_address: Address::repeat_byte(23),
                rtt_ms: 190.0,
                multiaddrs: Bytes::new(),
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
            multiaddrs: Bytes::new(),
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
                multiaddrs: Bytes::new(),
            },
            discovery::WarmingCandidate {
                node_id: node_key(32),
                eth_address: Address::repeat_byte(32),
                rtt_ms: 100.0,
                multiaddrs: Bytes::new(),
            },
        ];
        let out = super::failover_order(Vec::new(), &non_holders, warming_params(false));

        assert!(out.warming_lead.is_none(), "no holder to warm toward");
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

    /// Registry multiaddrs survive into both rebuilt-candidate branches of
    /// `failover_order` — the warming proxies and the cold-order non-holders —
    /// so those lanes dial the node directly rather than through a relay.
    #[test]
    fn failover_order_carries_registry_multiaddrs() -> anyhow::Result<()> {
        let packed = Bytes::from(decdn_incentive::node_register::pack_multiaddrs(&[
            "/ip4/203.0.113.10/udp/4433/quic-v1".to_string(),
        ])?);
        let pool = vec![discovery::WarmingCandidate {
            node_id: node_key(21),
            eth_address: Address::repeat_byte(21),
            rtt_ms: 90.0,
            multiaddrs: packed.clone(),
        }];

        let warm = super::failover_order(vec![holder(10, 200.0)], &pool, warming_params(true));
        let proxy = warm.order.first().context("proxy leads the order")?;
        assert_eq!(proxy.node_id, node_key(21));
        assert_eq!(
            proxy.multiaddrs, packed,
            "warming proxy keeps its addresses"
        );

        let cold = super::failover_order(Vec::new(), &pool, warming_params(false));
        let target = cold
            .order
            .first()
            .context("non-holder is the cold target")?;
        assert_eq!(target.multiaddrs, packed, "cold target keeps its addresses");
        Ok(())
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
    /// coverage (#1506) — not `Coverage::full` for every lane: disjoint
    /// per-holder coverage {block0}/{block1} survives into the map keyed by
    /// `node_id`, and a node that was never probed (e.g. a proxy) has no entry
    /// at all.
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
            proof_generation: None,
        });
        let annotated = super::annotate_delegated_exhaustion(err);
        assert!(
            annotated.to_string().contains("exhausted"),
            "expected the exhaustion remedy, got: {annotated}"
        );
    }

    /// A drained signer registration names the remedy a write-once
    /// registration leaves: a capability for a new signer key (#2338). Only a
    /// registration expired or with nothing left of its cap is named as
    /// shutting out every provider. A drain read at one provider's rate (alone,
    /// or under the stop once every known provider is barred) and a mid-stream
    /// `SignerCapExhausted` are measured against the refusing provider's rate,
    /// so the text says a cheaper provider may still serve.
    #[test]
    fn delegated_drained_signer_gets_the_new_key_remedy() {
        let drained = |remaining, expired| decdn_client::SignerCapDrained {
            pool_id: alloy::primitives::B256::ZERO,
            signer: Address::repeat_byte(0xd1),
            provider: Address::repeat_byte(0xa1),
            remaining,
            rate_per_mb: 10,
            expired,
        };
        let at_rate = [
            anyhow::Error::new(drained(5, false)),
            anyhow::Error::new(drained(5, false)).context(decdn_client::NoSourceServesSigner),
            anyhow::Error::new(UpstreamVoucherRejected {
                reason: decdn_protocol::client::VoucherRejectReason::SignerCapExhausted,
                bundle: None,
                proof_generation: None,
            }),
        ];
        for err in at_rate {
            let annotated = format!("{:#}", super::annotate_delegated_exhaustion(err));
            assert!(
                annotated.contains("new signer key")
                    && annotated.contains("lower rate may still serve")
                    && !annotated.contains("no node can be paid"),
                "expected the rate-relative new-key remedy, got: {annotated}"
            );
        }
        for err in [
            anyhow::Error::new(drained(0, false)),
            anyhow::Error::new(drained(5, true)),
        ] {
            let annotated = format!("{:#}", super::annotate_delegated_exhaustion(err));
            assert!(
                annotated.contains("new signer key") && annotated.contains("no node can be paid"),
                "expected the every-provider new-key remedy, got: {annotated}"
            );
        }
    }

    /// Any error the delegated-exhaustion annotator does not name passes
    /// through untouched.
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
            proof_generation: None,
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
            proof_generation: None,
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

    /// Every source saying `NotFound` ends the fetch as `NoSourceHasBlob`, with
    /// no `UpstreamRefused` left in the chain: an unbound fetch still gets the
    /// binding hint, and a bound one passes through untouched.
    #[test]
    fn unbound_no_source_has_blob_gets_the_binding_hint() {
        let absent = || anyhow::Error::new(NoSourceHasBlob);
        let annotated = annotate_unbound_cache_miss(absent(), &ctx_with(None));
        assert!(
            annotated.to_string().contains("capacity_bond_address"),
            "expected the binding hint, got: {annotated}"
        );
        assert!(annotated.downcast_ref::<NoSourceHasBlob>().is_some());

        let signer = PrivateKeySigner::random();
        let binding =
            sign_client_binding(&signer, B256::ZERO, &bind_node_id_domain(1, Address::ZERO))
                .expect("sign binding");
        let bound = annotate_unbound_cache_miss(absent(), &ctx_with(Some(binding)));
        assert_eq!(bound.to_string(), NoSourceHasBlob.to_string());
    }

    /// A delegated fetch no provider's voucher fits names the owner-side remedy.
    #[test]
    fn delegated_no_affordable_source_gets_the_owner_remedy_hint() {
        let err = anyhow::Error::new(NoAffordableSource {
            deposit: U256::from(5u32),
        });
        let annotated = annotate_delegated_exhaustion(err);
        assert!(
            annotated
                .to_string()
                .contains("Ask the pool owner to top up"),
            "{annotated}"
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

    fn rebase_store_fixture() -> anyhow::Result<(
        tempfile::TempDir,
        RedbBuyerPoolStore,
        Address,
        PoolId,
        LaneKey,
    )> {
        let dir = tempfile::tempdir()?;
        // A fresh subpath so the store's `ensure_data_dir` creates it at 0o700.
        let store = RedbBuyerPoolStore::open(&dir.path().join("data"))?;
        let owner = Address::repeat_byte(0x0A);
        let pool_id = PoolId::repeat_byte(0x01);
        let lane = LaneKey {
            pool_id,
            signer: owner,
            provider: Address::repeat_byte(0xB0),
        };
        let mut state = BuyerPoolState::new(
            pool_id,
            Deployment {
                chain_id: 421_614,
                payment_pool: Address::repeat_byte(0x7B),
            },
            owner,
            Address::repeat_byte(0x7C),
            U256::from(1_000u64),
        );
        state
            .advance_lane(lane, U256::from(500u64), U256::from(90u64))
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        store.record(&state)?;
        Ok((dir, store, owner, pool_id, lane))
    }

    fn lane_record(
        store: &RedbBuyerPoolStore,
        pool_id: PoolId,
        lane: LaneKey,
    ) -> anyhow::Result<BuyerLaneProgress> {
        store
            .get_by_pool_id(pool_id)?
            .and_then(|s| s.lane_progress(lane))
            .ok_or_else(|| anyhow::anyhow!("lane record missing"))
    }

    /// Two bundle entries on one provider share one connection: each entry
    /// builds its own deps and lane, and both lanes open their streams on the
    /// command's connection to the node.
    #[allow(
        clippy::too_many_lines,
        reason = "one fixture: a stream-dropping node, the lane deps, two entries"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_entries_on_one_provider_share_one_connection() -> anyhow::Result<()> {
        use decdn_client::source::BlobSource as _;
        use std::sync::atomic::{AtomicU64, Ordering};

        async fn loopback(alpns: Vec<Vec<u8>>) -> anyhow::Result<(Endpoint, EndpointAddr)> {
            let ep = Endpoint::builder(iroh::endpoint::presets::Minimal)
                .alpns(alpns)
                .relay_mode(iroh::RelayMode::Disabled)
                .bind_addr(std::net::SocketAddrV4::new(
                    std::net::Ipv4Addr::LOCALHOST,
                    0,
                ))?
                .bind()
                .await?;
            let socket = ep
                .bound_sockets()
                .into_iter()
                .find(std::net::SocketAddr::is_ipv4)
                .ok_or_else(|| anyhow::anyhow!("no IPv4 socket"))?;
            let addr = EndpointAddr::new(ep.id()).with_ip_addr(socket);
            Ok((ep, addr))
        }

        // A node that accepts every connection and drops every stream it is
        // sent, so each open fails on a connection that stays up.
        let (node, node_addr) = loopback(vec![decdn_protocol::ALPN_CLIENT.to_vec()]).await?;
        let accepted = Arc::new(AtomicU64::new(0));
        let (accept_node, count) = (node.clone(), Arc::clone(&accepted));
        tokio::spawn(async move {
            while let Some(incoming) = accept_node.accept().await {
                let Ok(connecting) = incoming.accept() else {
                    continue;
                };
                let Ok(conn) = connecting.await else { continue };
                count.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    while let Ok((send, recv)) = conn.accept_bi().await {
                        drop((send, recv));
                    }
                });
            }
        });

        let (ep, _) = loopback(Vec::new()).await?;
        let dir = tempfile::tempdir()?;
        let store = Arc::new(RedbBuyerPoolStore::open(&dir.path().join("data"))?);
        let rpc = alloy::providers::ProviderBuilder::new()
            .connect_mocked_client(alloy::providers::mock::Asserter::new());
        let contract = PaymentPool::new(Address::repeat_byte(0x33), rpc.clone());
        let chain = resolve_chain(
            &common(),
            &config(
                "[blockchain]\nrpc_url = \"http://config:8545\"\n\
                 payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n\
                 slash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
            ),
        )?;
        let slash_dom = decdn_incentive::slash_judge_domain(1, Address::repeat_byte(0x44));
        let connections = Connections::new(ep.clone());
        let writes = OrderedWrites::default();
        let funding = RunFunding::default();
        let entry_deps = || -> anyhow::Result<DriveFetchDeps<'_, _>> {
            Ok(DriveFetchDeps {
                timings: None,
                endpoint: &ep,
                store: &store,
                contract: &contract,
                rpc: &rpc,
                slash_dom: &slash_dom,
                self_address: Address::repeat_byte(0x0A),
                token: Address::repeat_byte(0x22),
                chain: &chain,
                namespace_id: decdn_protocol::client::NO_NAMESPACE,
                max_rate_per_mb: 0,
                max_blob_bytes: 0,
                deadlines: PullDeadlines::new(Duration::from_secs(5), Duration::from_secs(5), 0)?,
                connections: &connections,
                writes: &writes,
                funding: &funding,
            })
        };
        let provider = Address::repeat_byte(0xB0);
        let ctx = PoolContext {
            pool_id: B256::ZERO,
            provider,
            deposit: U256::from(1_000_000u64),
            client_signer: Arc::new(PrivateKeySigner::random()),
            voucher_domain: decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33)),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        };
        let ledger = Arc::new(ctx.new_ledger());
        let ctx = Arc::new(Mutex::new(ctx));
        let range = decdn_bao_range::align_range(0, 0, 1024)?;
        for entry in [[0x01u8; 32], [0x02u8; 32]] {
            let deps = entry_deps()?;
            let source = lane_source(
                &deps,
                node_addr.clone(),
                Arc::clone(&ctx),
                Arc::clone(&ledger),
                provider,
            );
            assert!(
                source.open(entry, range.clone()).await.is_err(),
                "the node drops every stream"
            );
        }

        assert_eq!(connections.dials(), 1, "both entries share one dial");
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn settle_on_drop_runs_only_when_dropped_armed() {
        let settled = std::cell::Cell::new(0);
        drop(SettleOnDrop::new(|| settled.set(settled.get() + 1)));
        assert_eq!(settled.get(), 1, "a dropped drive settles");
        SettleOnDrop::new(|| settled.set(settled.get() + 1)).disarm();
        assert_eq!(settled.get(), 1, "a returned drive settles on its own path");
    }

    /// `persist_watermark` overwrites the lane record down to a rebase anchor and
    /// advances it to the ledger's totals, even below what the record holds. A
    /// persist without an anchor is a monotone advance again, so a lower totals
    /// snapshot cannot move the record down.
    #[test]
    fn persist_watermark_overwrites_once_on_a_rebase() -> anyhow::Result<()> {
        let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
        let anchor = Cumulative {
            bytes: U256::from(100u64),
            amount: U256::from(60u64),
        };
        let totals = Cumulative {
            bytes: U256::from(200u64),
            amount: U256::from(70u64),
        };
        let progress = VoucherProgress::from_cumulative(totals, U256::from(90u64))
            .with_rebase_anchor(Some(anchor));
        persist_watermark(&store, owner, pool_id, lane, &progress);
        let got = lane_record(&store, pool_id, lane)?;
        assert_eq!(
            (got.last_bytes, got.last_amount),
            (totals.bytes, totals.amount)
        );

        let lower = Cumulative {
            bytes: U256::from(150u64),
            amount: U256::from(65u64),
        };
        let progress = VoucherProgress::from_cumulative(lower, U256::ZERO);
        persist_watermark(&store, owner, pool_id, lane, &progress);
        let got = lane_record(&store, pool_id, lane)?;
        assert_eq!(
            (got.last_bytes, got.last_amount),
            (totals.bytes, totals.amount),
            "without an anchor the persist is monotone"
        );
        Ok(())
    }

    /// A lane built twice on one shared ledger commits once per entry; a lane
    /// on its own ledger keeps its own write. The one write takes the lowest
    /// baseline of the ledger's handles: a sibling's rebase moved the store
    /// from 90 to 70 between the two builds, and the ledger settled at 80, so
    /// the advance from 70 to 80 still persists.
    #[test]
    fn duplicate_lane_handles_commit_once_from_the_lowest_baseline() {
        let handle = |ledger: &Arc<PoolLedger>, provider: u8, prior: u64| FaceLaneHandle {
            pool_id: PoolId::repeat_byte(1),
            provider: Address::repeat_byte(provider),
            prior_amount: U256::from(prior),
            ledger: Arc::clone(ledger),
            ctx: Arc::new(Mutex::new(ctx_with(None))),
        };
        let shared = Arc::new(PoolLedger::new(Cumulative {
            bytes: U256::from(100u64),
            amount: U256::from(80u64),
        }));
        let other = Arc::new(PoolLedger::new(Cumulative {
            bytes: U256::from(10u64),
            amount: U256::from(7u64),
        }));
        let handles = [
            handle(&shared, 0xA1, 90),
            handle(&other, 0xB2, 0),
            handle(&shared, 0xA1, 70),
        ];
        let writes = face_watermark_writes(Address::repeat_byte(0x5E), &handles);
        let providers: Vec<Address> = writes.iter().map(|(lane, _)| lane.provider).collect();
        assert_eq!(
            providers,
            [Address::repeat_byte(0xA1), Address::repeat_byte(0xB2)]
        );
        assert_eq!(
            writes[0].1.advanced(),
            Some((U256::from(100u64), U256::from(80u64))),
            "the shared lane advances past its lowest baseline"
        );
    }

    /// Hold `writes` until the returned sender fires, so every write queued
    /// meanwhile waits behind the hold.
    fn hold(writes: &OrderedWrites) -> std::sync::mpsc::Sender<()> {
        let (release, hold) = std::sync::mpsc::channel::<()>();
        writes.queue(move || {
            let _ = hold.recv();
        });
        release
    }

    /// One lane's write of `totals` over baseline `prior`, rebased to `anchor`.
    fn lane_write(
        lane: LaneKey,
        totals: (u64, u64),
        prior: u64,
        anchor: Option<(u64, u64)>,
    ) -> LaneWrite {
        let cum = |(bytes, amount): (u64, u64)| Cumulative {
            bytes: U256::from(bytes),
            amount: U256::from(amount),
        };
        (
            lane,
            VoucherProgress::from_cumulative(cum(totals), U256::from(prior))
                .with_rebase_anchor(anchor.map(cum)),
        )
    }

    /// Two entries settle one lane: the first read takes the ledger's rebase
    /// (down to 60, totals 70), the second a newer advance to 80. They land
    /// in that order, so the rebase never replaces the newer row.
    #[tokio::test]
    async fn a_rebase_read_first_never_lands_over_a_newer_advance() -> anyhow::Result<()> {
        let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
        let store = Arc::new(store);
        let writes = OrderedWrites::default();
        let release = hold(&writes);
        let rebase = lane_write(lane, (200, 70), 90, Some((100, 60)));
        drop(queue_watermark_writes(&writes, &store, owner, vec![rebase]));
        let advance = lane_write(lane, (300, 80), 70, None);
        let landed = queue_watermark_writes(&writes, &store, owner, vec![advance]);
        release.send(())?;
        tokio::time::timeout(Duration::from_secs(10), landed).await??;
        let got = lane_record(&store, pool_id, lane)?;
        assert_eq!(
            (got.last_bytes, got.last_amount),
            (U256::from(300u64), U256::from(80u64))
        );
        Ok(())
    }

    /// Two entries settle one lane: the first read is an advance to 95 from
    /// before the node refused it, the second the rebase down to 60 (totals
    /// 70). They land in that order, so the refused watermark never returns.
    #[tokio::test]
    async fn a_stale_advance_read_first_never_lands_over_a_rebase() -> anyhow::Result<()> {
        let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
        let store = Arc::new(store);
        let writes = OrderedWrites::default();
        let release = hold(&writes);
        let advance = lane_write(lane, (600, 95), 90, None);
        drop(queue_watermark_writes(
            &writes,
            &store,
            owner,
            vec![advance],
        ));
        let rebase = lane_write(lane, (200, 70), 90, Some((100, 60)));
        let landed = queue_watermark_writes(&writes, &store, owner, vec![rebase]);
        release.send(())?;
        tokio::time::timeout(Duration::from_secs(10), landed).await??;
        let got = lane_record(&store, pool_id, lane)?;
        assert_eq!(
            (got.last_bytes, got.last_amount),
            (U256::from(200u64), U256::from(70u64))
        );
        Ok(())
    }

    /// A drop guard's settle drops its receiver and returns. The write it
    /// queued, behind a slow one, still lands before the runtime finishes
    /// dropping: the runtime waits for the blocking pool as it shuts down.
    #[test]
    fn a_detached_settle_lands_by_runtime_shutdown() -> anyhow::Result<()> {
        let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
        let store = Arc::new(store);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()?;
        runtime.block_on(async {
            let writes = OrderedWrites::default();
            writes.queue(|| std::thread::sleep(Duration::from_millis(100)));
            let advance = lane_write(lane, (600, 95), 90, None);
            drop(queue_watermark_writes(
                &writes,
                &store,
                owner,
                vec![advance],
            ));
        });
        drop(runtime);
        let got = lane_record(&store, pool_id, lane)?;
        assert_eq!(
            (got.last_bytes, got.last_amount),
            (U256::from(600u64), U256::from(95u64))
        );
        Ok(())
    }

    /// The multi-source path carries each lane's own rebase anchor.
    #[test]
    fn multi_lane_watermarks_carry_the_rebase_anchor() {
        let anchor = Cumulative {
            bytes: U256::from(50u64),
            amount: U256::from(60u64),
        };
        let mut lane = lane_wm(1, 0xA1, 200, 100, 70);
        lane.rebase_anchor = Some(anchor);
        let out = super::multi_lane_watermarks(Address::repeat_byte(0x5E), &[lane]);
        assert_eq!(out[0].1.rebase_anchor(), Some(anchor));
        assert_eq!(out[0].1.totals(), (U256::from(100u64), U256::from(70u64)));
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
            rebase_anchor: None,
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
    /// A cancelled or stalled leg drops its `fill_gap` future wherever it is
    /// parked — including inside the voucher exchange `issue` deliberately arms
    /// before sending — and that can happen on a SUCCESSFUL fetch. Settling that
    /// lane low persists a cumulative below what the node can redeem, and the next
    /// fetch on the lane signs a value the upstream already holds: rejected as a
    /// regression.
    #[test]
    fn multi_lane_watermarks_settle_high_even_when_the_fetch_succeeded() {
        let signer = Address::repeat_byte(0x5E);
        // The armed cumulative sits ABOVE what was acked — the dropped-leg
        // shape. Settling at `committed` would persist the lower one.
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

/// `open_or_reuse_pool` against a mocked `PaymentPool`: what a client does when
/// its store has no row for a pool the chain says it owns.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod adoption_tests {
    use super::*;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolValue;

    const PP: Address = Address::repeat_byte(0x9c);
    /// The deployment every test buys on: chain 1, contract [`PP`].
    const DEPLOYMENT: Deployment = Deployment {
        chain_id: 1,
        payment_pool: PP,
    };
    const PROVIDER: Address = Address::repeat_byte(0x77);
    const TOKEN: Address = Address::repeat_byte(0x22);
    /// The client's working deposit: 10 USDC, so low water is 2 USDC.
    const WORKING: u64 = 10_000_000;

    fn pool(owner: Address, deposit: u64, redeemed: u64) -> PaymentPool::Pool {
        PaymentPool::Pool {
            owner,
            status: PaymentPool::Status::Open,
            disputeDeadline: 0,
            deposit,
            totalRedeemed: redeemed,
        }
    }

    fn lane(amount: u64, bytes: u64) -> Bytes {
        PaymentPool::Lane {
            amount,
            bytesDelivered: bytes,
        }
        .abi_encode()
        .into()
    }

    /// Run `open_or_reuse_pool` with its `eth_call`s answered in order from
    /// `calls`; `None` faults a call. A call past the end of the queue faults
    /// too, which is how these tests prove a path was NOT taken.
    async fn run(
        store: &RedbBuyerPoolStore,
        signer: &Arc<PrivateKeySigner>,
        adoption: ChainAdoption,
        calls: Vec<Option<Bytes>>,
    ) -> anyhow::Result<PoolContext> {
        run_in(store, signer, adoption, calls, &RunFunding::default())
            .await
            .0
            .map(|(ctx, _)| ctx)
    }

    /// [`run`] as one lane build of the run `funding` describes, with the
    /// lane's [`LaneSpend`]. The second value is whether every queued answer
    /// was read: a path that stops short leaves some unread, which is how these
    /// tests prove a path WAS taken.
    async fn run_in(
        store: &RedbBuyerPoolStore,
        signer: &Arc<PrivateKeySigner>,
        adoption: ChainAdoption,
        calls: Vec<Option<Bytes>>,
        funding: &RunFunding,
    ) -> (anyhow::Result<(PoolContext, LaneSpend)>, bool) {
        let asserter = Asserter::new();
        for call in calls {
            match call {
                Some(response) => asserter.push_success(&response),
                None => asserter.push_failure_msg("transient rpc fault"),
            }
        }
        let rpc = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let contract = PaymentPool::new(PP, rpc.clone());
        let result = open_or_reuse_pool(
            store,
            &contract,
            &rpc,
            funding,
            signer,
            PROVIDER,
            signer.address(),
            DEPLOYMENT,
            U256::from(WORKING),
            false,
            adoption,
        )
        .await;
        (result, asserter.read_q().is_empty())
    }

    fn client_store(dir: &tempfile::TempDir) -> RedbBuyerPoolStore {
        // A fresh subpath: the store requires 0o700 and creates it itself.
        RedbBuyerPoolStore::open(&dir.path().join("data")).unwrap()
    }

    /// The case that reached a user. The store has lost its row, all the
    /// wallet's USDC sits in a live pool, and the client must reuse that pool
    /// rather than try to escrow a second deposit it cannot afford.
    ///
    /// Also pins the three things adoption has to get right together: it takes
    /// the newest pool that can still pay (a drained newer one is passed over),
    /// it resumes the lane from the chain watermark rather than zero, and it
    /// records the row so the next run reuses it without asking the chain again.
    #[tokio::test]
    async fn a_lost_row_adopts_the_live_pool_and_resumes_from_the_chain_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());
        let owner = signer.address();
        let older = B256::repeat_byte(0xAA);
        let newer = B256::repeat_byte(0xBB);

        let ctx = run(
            &store,
            &signer,
            ChainAdoption::Allowed,
            vec![
                Some(vec![older, newer].abi_encode().into()),
                // Walked newest-first: `newer` is drained, `older` still pays.
                Some(pool(owner, 10_000_000, 10_000_000).abi_encode().into()),
                Some(pool(owner, 10_000_000, 1_106_908).abi_encode().into()),
                Some(TOKEN.abi_encode().into()),
                Some(lane(1_106_908, 1_106_908_000)),
            ],
        )
        .await
        .expect("a live, solvent pool is adopted — nothing is opened");

        assert_eq!(ctx.pool_id, older);
        assert_eq!(
            (ctx.prior_bytes_delivered, ctx.prior_amount),
            (U256::from(1_106_908_000u64), U256::from(1_106_908u64)),
            "the lane resumes from the chain watermark; from zero, every voucher at or \
             below it would redeem nothing"
        );
        let row = store
            .get_by_owner(owner)
            .unwrap()
            .expect("adopted row is recorded");
        assert_eq!(row.pool_id, older);
        assert!(row.is_on(DEPLOYMENT));
    }

    /// A chain read that faults refuses the fetch. It must not fall through to
    /// opening a pool: "could not tell" is not "owns nothing", and treating it
    /// as such escrows a second deposit beside a pool the wallet already funded.
    #[tokio::test]
    async fn a_faulted_enumeration_refuses_rather_than_opening() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());

        let err = run(&store, &signer, ChainAdoption::Allowed, vec![None])
            .await
            .unwrap_err();
        // The open path would also error here (its first call finds an empty
        // queue), so only the message tells "refused on purpose" from "fell
        // through to opening".
        assert!(
            format!("{err:#}").contains("refusing to open one blind"),
            "expected the deliberate refusal, got: {err:#}"
        );
        assert!(store.get_by_owner(signer.address()).unwrap().is_none());
    }

    /// A stored pool meeting a new provider resumes that lane from the chain,
    /// not from zero. This reaches every tracked pool, not only adopted ones.
    #[tokio::test]
    async fn a_tracked_pool_resumes_a_new_lane_from_the_chain_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());
        let id = B256::repeat_byte(0xCC);
        store
            .record(&BuyerPoolState::new(
                id,
                DEPLOYMENT,
                signer.address(),
                TOKEN,
                U256::from(WORKING),
            ))
            .unwrap();

        let ctx = run(
            &store,
            &signer,
            ChainAdoption::Allowed,
            vec![Some(lane(500, 500_000))],
        )
        .await
        .expect("a tracked pool reuses without touching getPools");

        assert_eq!(ctx.pool_id, id);
        assert_eq!(
            (ctx.prior_bytes_delivered, ctx.prior_amount),
            (U256::from(500_000u64), U256::from(500u64))
        );
    }

    /// A tracked row from another deployment is not reused, whether the
    /// deployment differs in its contract address or only in its chain. The
    /// pool id repeats across deployments, so reusing the row would resume lane
    /// progress this pool never redeemed against.
    ///
    /// `ChainAdoption::Refused` keeps the chain out of it: the mocked provider
    /// has no answers queued, so any read would fault the call.
    #[tokio::test]
    async fn a_row_from_another_deployment_is_not_reused() {
        let signer = Arc::new(PrivateKeySigner::random());
        let rpc = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let contract = PaymentPool::new(PP, rpc);
        let other_address = Deployment {
            payment_pool: Address::repeat_byte(0xDE),
            ..DEPLOYMENT
        };
        let other_chain = Deployment {
            chain_id: 421_614,
            ..DEPLOYMENT
        };
        for (foreign, reused) in [
            (other_address, false),
            (other_chain, false),
            (DEPLOYMENT, true),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let store = client_store(&dir);
            let id = B256::repeat_byte(0xCC);
            store
                .record(&BuyerPoolState::new(
                    id,
                    foreign,
                    signer.address(),
                    TOKEN,
                    U256::from(WORKING),
                ))
                .unwrap();

            let tracked = pool_to_reuse(
                &store,
                &contract,
                signer.address(),
                DEPLOYMENT,
                ChainAdoption::Refused,
            )
            .await
            .expect("a refused adoption reads nothing from the chain");

            assert_eq!(
                tracked.as_ref().map(|state| state.pool_id),
                reused.then_some(id),
                "row on {foreign:?}, buying on {DEPLOYMENT:?}"
            );
            assert!(tracked.is_none_or(|state| state.redeemed_elsewhere().is_zero()));
        }
    }

    /// A buy from a node's data dir never adopts, even though the chain would
    /// hand it a pool: that pool is the daemon's, and adopting it would put a
    /// second voucher series on the daemon's own lanes.
    ///
    /// The queue holds only the `usdc()` answer the open path reads first. Were
    /// adoption attempted, `getPools` would consume that word, fail to decode it,
    /// and surface the adoption refusal instead.
    #[tokio::test]
    async fn a_node_data_dir_never_adopts_the_daemons_pool() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());

        let err = run(
            &store,
            &signer,
            ChainAdoption::Refused,
            vec![Some(TOKEN.abi_encode().into())],
        )
        .await
        .unwrap_err();
        assert!(
            !format!("{err:#}").contains("already owns a pool"),
            "a node dir went down the adoption path: {err:#}"
        );
        assert!(
            store.get_by_owner(signer.address()).unwrap().is_none(),
            "nothing may be adopted into a node dir's client store"
        );
    }

    /// The amounts observed on the pool in #2288: six lanes, 8.655 of 10 USDC
    /// spent in all, the largest lane 2.374. The first lane is [`PROVIDER`]'s.
    const LIVE_LANES: [u64; 6] = [2_374_000, 2_360_000, 2_147_000, 1_043_000, 483_000, 248_000];

    /// A tracked row on [`DEPLOYMENT`] holding `deposit`, with one lane per
    /// `amounts` entry: the first is [`PROVIDER`]'s, each later one another
    /// provider's. A lane's bytes are its amount times 1000.
    fn tracked_row(id: B256, owner: Address, deposit: u64, amounts: &[u64]) -> BuyerPoolState {
        let lanes = amounts
            .iter()
            .zip(0u8..)
            .map(|(&amount, i)| {
                let provider = if i == 0 {
                    PROVIDER
                } else {
                    Address::repeat_byte(0x40 + i)
                };
                (
                    lane_key(id, owner, provider),
                    decdn_incentive::BuyerLaneProgress {
                        last_amount: U256::from(amount),
                        last_bytes: U256::from(amount) * U256::from(1000u64),
                    },
                )
            })
            .collect();
        BuyerPoolState::hydrate(
            id,
            DEPLOYMENT,
            owner,
            TOKEN,
            U256::from(deposit),
            lanes,
            U256::ZERO,
        )
    }

    fn lane_key(id: B256, owner: Address, provider: Address) -> LaneKey {
        LaneKey {
            pool_id: id,
            signer: owner,
            provider,
        }
    }

    /// A refill that fails at the allowance read, then reads a wallet holding
    /// `usdc` micro-USDC.
    fn refill_fails_with_wallet(usdc: u64) -> Vec<Option<Bytes>> {
        vec![None, Some(U256::from(usdc).abi_encode().into())]
    }

    /// The refill compares the deposit with the pool's spend over every lane.
    /// Each live lane alone leaves far more than the low water; together they
    /// leave 1.345 USDC, below the 2 USDC mark, so the pool must refill.
    #[test]
    fn pool_spend_sums_every_tracked_lane() {
        let owner = Address::repeat_byte(0x01);
        let id = B256::repeat_byte(0xEE);
        let row = tracked_row(id, owner, WORKING, &LIVE_LANES);
        let working = U256::from(WORKING);
        let low_water = working / U256::from(LOW_WATER_DIVISOR);

        let spent = row.pool_spend();
        assert_eq!(spent, U256::from(8_655_000u64));
        assert_eq!(
            refill_amount(row.deposit, spent, working, low_water),
            U256::from(8_655_000u64),
            "the refill restores the 1.345 USDC remaining to the 10 USDC working deposit"
        );
        for &one_lane in &LIVE_LANES {
            assert!(
                refill_amount(row.deposit, U256::from(one_lane), working, low_water).is_zero(),
                "no single lane reaches the low water, so the spend must be the sum over \
                 every lane"
            );
        }
    }

    /// A lane the row has no record of resumes from the chain watermark, and
    /// that watermark is spend the row's sum does not hold yet: the seed
    /// records it. A tracked lane's seed is not counted twice.
    #[test]
    fn a_seeded_lanes_chain_prior_counts_toward_the_pool_spend() {
        let owner = Address::repeat_byte(0x01);
        let id = B256::repeat_byte(0xEE);
        let mut row = tracked_row(id, owner, WORKING, &[1_000]);
        let new_lane = lane_key(id, owner, Address::repeat_byte(0x55));
        row.seed_lane(new_lane, U256::from(250_000u64), U256::from(250u64))
            .unwrap();
        assert_eq!(row.pool_spend(), U256::from(1_250u64));
        row.seed_lane(
            lane_key(id, owner, PROVIDER),
            U256::from(1_000_000u64),
            U256::from(1_000u64),
        )
        .unwrap();
        assert_eq!(row.pool_spend(), U256::from(1_250u64));
    }

    /// An adopted row has no lanes, so `totalRedeemed` is its only record of
    /// what other lanes drained. Ignored, a pool with 0.5 USDC left reads as a
    /// full 10 and no refill fires. A lane seeded from chain takes its share
    /// over, so the other lanes' share still counts as that lane advances
    /// (#2292 review): 100 redeemed, 60 of it on A, A advancing to 80 spends
    /// 120.
    #[test]
    fn pool_spend_keeps_an_adopted_pools_redeemed_spend_as_its_lanes_advance() {
        let owner = Address::repeat_byte(0x01);
        let id = B256::repeat_byte(0xEE);
        let adopted = |redeemed: u64| {
            BuyerPoolState::adopt(
                id,
                DEPLOYMENT,
                owner,
                TOKEN,
                U256::from(WORKING),
                U256::from(redeemed),
            )
        };
        assert_eq!(adopted(9_500_000).pool_spend(), U256::from(9_500_000u64));

        let a = lane_key(id, owner, PROVIDER);
        let mut row = adopted(100);
        row.seed_lane(a, U256::from(60_000u64), U256::from(60u64))
            .unwrap();
        assert_eq!(row.redeemed_elsewhere(), U256::from(40u64));
        assert_eq!(row.pool_spend(), U256::from(100u64));
        row.advance_lane(a, U256::from(80_000u64), U256::from(80u64))
            .unwrap();
        assert_eq!(row.pool_spend(), U256::from(120u64));
    }

    /// #2288 end to end: five lanes of 2 USDC spend the whole 10 USDC deposit,
    /// so building any lane fires the refill. The wallet holds no USDC, and with
    /// nothing left in the pool the lane fails. A spend read from the one lane
    /// alone would see 8 USDC remaining, fire no refill, and return `Ok`.
    #[tokio::test]
    async fn a_pool_spent_across_lanes_refills_and_fails_when_nothing_is_left() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());
        let id = B256::repeat_byte(0xEE);
        store
            .record(&tracked_row(id, signer.address(), WORKING, &[2_000_000; 5]))
            .unwrap();

        let (result, drained) = run_in(
            &store,
            &signer,
            ChainAdoption::Allowed,
            refill_fails_with_wallet(0),
            &RunFunding::default(),
        )
        .await;
        let err = result.expect_err("a pool with no unspent deposit cannot pay");
        assert!(
            format!("{err:#}").contains("has no unspent deposit, and the wallet cannot fund"),
            "expected the refill failure, got: {err:#}"
        );
        assert!(
            err.downcast_ref::<NoAffordableSource>().is_some(),
            "the lane build ends the command rather than retrying: {err:#}"
        );
        assert!(
            drained,
            "the refill read the allowance and the wallet balance"
        );
    }

    /// A lane the row has no record of joins its chain watermark to the
    /// pool-wide spend. Here the tracked lane has spent 9 USDC and the new
    /// lane's watermark the last 1, so nothing is left. Without the watermark
    /// the pool reads 1 USDC remaining and the lane builds.
    #[tokio::test]
    async fn an_untracked_lanes_chain_watermark_counts_toward_the_pool_spend() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());
        let id = B256::repeat_byte(0xEE);
        let other = lane_key(id, signer.address(), Address::repeat_byte(0x55));
        let progress = decdn_incentive::BuyerLaneProgress {
            last_amount: U256::from(9_000_000u64),
            last_bytes: U256::from(9_000_000_000u64),
        };
        store
            .record(&BuyerPoolState::hydrate(
                id,
                DEPLOYMENT,
                signer.address(),
                TOKEN,
                U256::from(WORKING),
                vec![(other, progress)],
                U256::ZERO,
            ))
            .unwrap();

        let mut calls = vec![Some(lane(1_000_000, 1_000_000_000))];
        calls.extend(refill_fails_with_wallet(0));
        let (result, drained) = run_in(
            &store,
            &signer,
            ChainAdoption::Allowed,
            calls,
            &RunFunding::default(),
        )
        .await;
        let err = result.expect_err("the untracked lane's watermark spends the rest");
        assert!(
            format!("{err:#}").contains("has no unspent deposit"),
            "{err:#}"
        );
        assert!(drained);
    }

    /// #2289: a wallet too short of USDC for the refill keeps the lane while the
    /// pool can still pay. The live pool has 1.345 USDC unspent, below the low
    /// water, so the refill fires; the wallet holds none. The lane is built on
    /// the current deposit, the row is untouched, and the run records the
    /// shortfall for its closing warning.
    #[tokio::test]
    async fn a_wallet_short_of_usdc_keeps_the_lane_while_the_pool_can_still_pay() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());
        let id = B256::repeat_byte(0xEE);
        store
            .record(&tracked_row(id, signer.address(), WORKING, &LIVE_LANES))
            .unwrap();
        let funding = RunFunding::default();

        let (result, drained) = run_in(
            &store,
            &signer,
            ChainAdoption::Allowed,
            refill_fails_with_wallet(0),
            &funding,
        )
        .await;
        let (ctx, _) =
            result.expect("1.345 USDC is left, so a short wallet must not drop the lane");

        assert!(drained, "the refill fired and read the wallet balance");
        assert_eq!(ctx.pool_id, id);
        assert_eq!(
            (ctx.prior_bytes_delivered, ctx.prior_amount),
            (U256::from(LIVE_LANES[0] * 1000), U256::from(LIVE_LANES[0])),
            "the lane resumes from its own recorded progress"
        );
        let row = store.get_by_owner(signer.address()).unwrap().unwrap();
        assert_eq!(row.deposit, U256::from(WORKING), "nothing was credited");
        assert_eq!(row.committed_amount(), U256::from(8_655_000u64));
        let shortfall = funding.shortfall().expect("the shortfall is recorded");
        assert!(shortfall.contains("holds 0 µUSDC"), "{shortfall}");
        assert!(shortfall.contains("8655000 µUSDC top-up"), "{shortfall}");
    }

    /// Once the run has seen the wallet too short of USDC, a later lane build
    /// does not ask again: no allowance read, no `topUp`. With no answers
    /// queued, any chain read would fault the build.
    #[tokio::test]
    async fn a_later_lane_build_skips_the_refill_after_a_wallet_shortfall() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());
        let id = B256::repeat_byte(0xEE);
        store
            .record(&tracked_row(id, signer.address(), WORKING, &LIVE_LANES))
            .unwrap();
        let funding = RunFunding::default();
        let (first, _) = run_in(
            &store,
            &signer,
            ChainAdoption::Allowed,
            refill_fails_with_wallet(0),
            &funding,
        )
        .await;
        first.expect("the first build records the shortfall and keeps its lane");

        let (later, _) = run_in(&store, &signer, ChainAdoption::Allowed, vec![], &funding).await;
        later.expect("the later build reads nothing from the chain");
    }

    /// A refill that fails while the wallet holds enough USDC is not a
    /// shortfall: the cause may be transient, so the lane build fails and the
    /// acquire loop retries it with backoff.
    #[tokio::test]
    async fn a_refill_failure_with_a_funded_wallet_fails_the_lane_build() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());
        let id = B256::repeat_byte(0xEE);
        store
            .record(&tracked_row(id, signer.address(), WORKING, &LIVE_LANES))
            .unwrap();
        let funding = RunFunding::default();

        let (result, drained) = run_in(
            &store,
            &signer,
            ChainAdoption::Allowed,
            refill_fails_with_wallet(WORKING),
            &funding,
        )
        .await;
        let err = result.expect_err("a funded wallet's failed refill is retried, not skipped");
        assert!(
            format!("{err:#}").contains("read USDC allowance"),
            "{err:#}"
        );
        assert!(drained);
        assert!(funding.shortfall().is_none());
    }

    /// An adopted pool that other lanes have drained to 0.5 USDC fires the
    /// refill through `totalRedeemed`, and still buys on its remainder when
    /// the wallet is empty. Adoption only takes a pool with deposit left.
    #[tokio::test]
    async fn an_adopted_pool_drained_by_other_lanes_refills_and_buys_on_its_remainder() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());
        let owner = signer.address();
        let id = B256::repeat_byte(0xDD);
        let mut calls = vec![
            Some(vec![id].abi_encode().into()),
            Some(pool(owner, 10_000_000, 9_500_000).abi_encode().into()),
            Some(TOKEN.abi_encode().into()),
            // This lane itself has spent nothing — the drain is elsewhere.
            Some(lane(0, 0)),
        ];
        calls.extend(refill_fails_with_wallet(0));
        let funding = RunFunding::default();

        let (result, drained) =
            run_in(&store, &signer, ChainAdoption::Allowed, calls, &funding).await;
        let (ctx, spend) =
            result.expect("0.5 USDC is left, so a short wallet must not drop the lane");

        assert!(
            drained,
            "0.5 USDC left is below the 2 USDC low water, so the refill must fire"
        );
        assert_eq!(ctx.pool_id, id);
        assert_eq!(
            spend,
            LaneSpend {
                outside: U256::from(9_500_000u64),
                prior: U256::ZERO,
                recorded: false,
                from_chain: true,
            }
        );
    }

    /// #2292: the run after adoption reuses the row without asking the chain.
    /// The pool's `totalRedeemed` stays in its spend: read only by the
    /// adopting run, every later run would count just the lanes this machine
    /// recorded since, and read the pool as fuller than it is. The lane the
    /// adopting run seeds from chain takes over its own share of it, so the
    /// other lanes' share still counts once that lane advances.
    #[tokio::test]
    async fn a_reused_adopted_row_keeps_the_pools_redeemed_spend() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());
        let owner = signer.address();
        let id = B256::repeat_byte(0xDE);
        let lane_spend = |outside: u64, recorded: bool| LaneSpend {
            outside: U256::from(outside),
            prior: U256::from(1_000_000u64),
            recorded,
            from_chain: true,
        };

        // 3 USDC redeemed, 1 USDC of it on this lane.
        let (adopting, drained) = run_in(
            &store,
            &signer,
            ChainAdoption::Allowed,
            vec![
                Some(vec![id].abi_encode().into()),
                Some(pool(owner, 10_000_000, 3_000_000).abi_encode().into()),
                Some(TOKEN.abi_encode().into()),
                Some(lane(1_000_000, 1_000_000_000)),
            ],
            &RunFunding::default(),
        )
        .await;
        assert!(drained);
        assert_eq!(
            adopting.expect("the live pool is adopted").1,
            lane_spend(2_000_000, false)
        );
        let row = store
            .get_by_owner(owner)
            .unwrap()
            .expect("adopted row is recorded");
        assert_eq!(row.redeemed_elsewhere(), U256::from(2_000_000u64));
        assert_eq!(row.pool_spend(), U256::from(3_000_000u64));

        // No chain answers queued: the row and its lane are tracked now.
        let (reusing, drained) = run_in(
            &store,
            &signer,
            ChainAdoption::Allowed,
            vec![],
            &RunFunding::default(),
        )
        .await;
        assert!(drained);
        let (ctx, spend) = reusing.expect("the tracked row is reused");
        assert_eq!(ctx.pool_id, id);
        assert_eq!(
            spend,
            lane_spend(2_000_000, true),
            "a later run sees the redeemed spend the adopting run saw"
        );

        // The lane pays on to 1.5 USDC: the pool has spent 3.5 USDC.
        let a = lane_key(id, owner, PROVIDER);
        let outcome = store
            .advance_progress(
                owner,
                id,
                a,
                U256::from(1_500_000_000u64),
                U256::from(1_500_000u64),
            )
            .unwrap();
        assert!(matches!(outcome, AdvanceOutcome::Advanced));
        assert_eq!(
            store.get_by_owner(owner).unwrap().unwrap().pool_spend(),
            U256::from(3_500_000u64)
        );
    }

    /// A ledger seeded at `amount`.
    fn ledger_at(amount: u64) -> Arc<PoolLedger> {
        Arc::new(PoolLedger::new(Cumulative {
            bytes: U256::from(amount) * U256::from(1000u64),
            amount: U256::from(amount),
        }))
    }

    fn spend(outside: u64, prior: u64, recorded: bool, from_chain: bool) -> LaneSpend {
        LaneSpend {
            outside: U256::from(outside),
            prior: U256::from(prior),
            recorded,
            from_chain,
        }
    }

    /// The pool's spend is the run's baseline plus every joined lane's
    /// ledger. The first lane sets the baseline; a later lane the row recorded
    /// moves its prior out of it, because its ledger counts it now; a lane the
    /// row did not hold moves nothing, because the baseline never counted its
    /// chain prior; a lane that joins again changes nothing.
    #[test]
    fn run_funding_counts_every_lane_once_on_a_tracked_pool() {
        let id = B256::repeat_byte(0xEE);
        let owner = Address::repeat_byte(0x01);
        let funding = RunFunding::default();
        assert_eq!(funding.pool_spent(), None);

        // The live pool: PROVIDER's lane is the first to join.
        let first = lane_key(id, owner, PROVIDER);
        let first_ledger = ledger_at(2_374_000);
        funding.join_lane(
            first,
            spend(6_281_000, 2_374_000, true, false),
            &first_ledger,
        );
        assert_eq!(funding.pool_spent(), Some(U256::from(8_655_000u64)));

        let second = lane_key(id, owner, Address::repeat_byte(0x41));
        funding.join_lane(
            second,
            spend(0, 2_360_000, true, false),
            &ledger_at(2_360_000),
        );
        assert_eq!(funding.pool_spent(), Some(U256::from(8_655_000u64)));

        // A new provider whose chain watermark the row never held.
        let fresh = lane_key(id, owner, Address::repeat_byte(0x55));
        funding.join_lane(fresh, spend(0, 100, false, false), &ledger_at(100));
        assert_eq!(funding.pool_spent(), Some(U256::from(8_655_100u64)));

        funding.join_lane(first, spend(0, 6_281_000, true, false), &first_ledger);
        assert_eq!(funding.pool_spent(), Some(U256::from(8_655_100u64)));
    }

    /// An adopted pool's baseline holds its redeemed spend no seeded lane
    /// accounts for, which counts every unseeded lane's redeemed watermark. A later lane's chain prior moves out of
    /// it, recorded or not, so it is counted once, by the lane's ledger.
    #[test]
    fn run_funding_counts_every_lane_once_on_an_adopted_pool() {
        let id = B256::repeat_byte(0xEE);
        let owner = Address::repeat_byte(0x01);
        let funding = RunFunding::default();
        funding.join_lane(
            lane_key(id, owner, PROVIDER),
            spend(9_000_000, 500_000, false, true),
            &ledger_at(500_000),
        );
        assert_eq!(funding.pool_spent(), Some(U256::from(9_500_000u64)));
        funding.join_lane(
            lane_key(id, owner, Address::repeat_byte(0x41)),
            spend(0, 1_000_000, false, false),
            &ledger_at(1_000_000),
        );
        assert_eq!(funding.pool_spent(), Some(U256::from(9_500_000u64)));
    }

    /// A lane's spend is what its ledgers committed, so it grows as the lane
    /// pays. A lane rebuilt with a fresh ledger from a stale prior keeps the
    /// larger of its ledgers: vouchers on a lane are cumulative.
    #[test]
    fn run_funding_follows_each_lanes_largest_ledger() {
        let id = B256::repeat_byte(0xEE);
        let owner = Address::repeat_byte(0x01);
        let funding = RunFunding::default();
        let lane = lane_key(id, owner, PROVIDER);
        let paid = ledger_at(0);
        funding.join_lane(lane, spend(1_000, 0, false, false), &paid);
        assert!(paid.reseed(Cumulative {
            bytes: U256::from(300_000u64),
            amount: U256::from(300u64),
        }));
        assert_eq!(funding.pool_spent(), Some(U256::from(1_300u64)));

        funding.join_lane(lane, spend(1_000, 0, false, false), &ledger_at(0));
        assert_eq!(funding.pool_spent(), Some(U256::from(1_300u64)));
    }

    /// A lane build hands back what it learned about the pool's spend: the
    /// spend on every other lane, and the lane's own recorded prior.
    #[tokio::test]
    async fn a_lane_build_reports_the_pools_other_spend() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let signer = Arc::new(PrivateKeySigner::random());
        let id = B256::repeat_byte(0xEE);
        store
            .record(&tracked_row(id, signer.address(), 2 * WORKING, &LIVE_LANES))
            .unwrap();

        let (result, drained) = run_in(
            &store,
            &signer,
            ChainAdoption::Allowed,
            vec![],
            &RunFunding::default(),
        )
        .await;
        let (_, spend) = result.expect("11.345 USDC is left, so no refill fires");
        assert!(drained);
        assert_eq!(
            spend,
            LaneSpend {
                outside: U256::from(8_655_000u64 - LIVE_LANES[0]),
                prior: U256::from(LIVE_LANES[0]),
                recorded: true,
                from_chain: false,
            }
        );
    }

    /// The run's funder reports the pool's spend to the deposit gate. A
    /// reactive top-up that fails for another reason is not a shortfall; after
    /// a wallet shortfall, a reactive top-up fails without a chain call and
    /// says so ([`WalletShortfall`]).
    #[tokio::test]
    async fn the_cli_funder_reads_the_run_funding() {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let rpc = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let contract = PaymentPool::new(PP, rpc.clone());
        let funding = RunFunding::default();
        funding.join_lane(
            lane_key(
                B256::repeat_byte(0xEE),
                Address::repeat_byte(0x01),
                PROVIDER,
            ),
            spend(700, 0, false, false),
            &ledger_at(0),
        );
        let pool_id = std::sync::OnceLock::new();
        pool_id.set(B256::repeat_byte(0xEE)).unwrap();
        let funder = CliFunder {
            contract: &contract,
            rpc: &rpc,
            store: &store,
            owner: Address::repeat_byte(0x01),
            pool_id: &pool_id,
            token: TOKEN,
            payment_pool_addr: PP,
            max_approve: false,
            funding: &funding,
        };
        assert_eq!(funder.pool_spent(), Some(U256::from(700u64)));

        // No answers are queued, so the wallet read faults: not a shortfall.
        let err = funder.top_up(U256::from(5u64)).await.unwrap_err();
        assert!(funding.shortfall().is_none(), "{err:#}");
        assert!(err.downcast_ref::<WalletShortfall>().is_none(), "{err:#}");

        *funding.shortfall.lock().unwrap() = Some("wallet 0x01 holds 0 µUSDC".to_owned());
        let err = funder.top_up(U256::from(5u64)).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("the wallet cannot fund a top-up: wallet 0x01 holds 0"),
            "{err:#}"
        );
        assert!(err.downcast_ref::<WalletShortfall>().is_some(), "{err:#}");
    }
}

/// Probe-shaped fixtures shared by the fetch and source tests.
#[cfg(test)]
pub(crate) mod tests_support {
    use super::{
        Address, Bytes, NodeCandidate, ProxyWarmingParams, PublicKey, ResolvedTargets, discovery,
    };

    /// A node key derived from `seed`.
    pub(crate) fn node_key(seed: u8) -> PublicKey {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    /// A probed holder of the whole blob at `rtt_ms`, paid at the address
    /// `seed` repeated.
    pub(crate) fn holder(seed: u8, rtt_ms: f64) -> discovery::Probed {
        discovery::Probed {
            candidate: NodeCandidate {
                node_id: node_key(seed),
                eth_address: Address::repeat_byte(seed),
                region_hint: None,
                multiaddrs: Bytes::new(),
            },
            rtt_ms,
            total_bytes: Some(128 * 1024 * 1024),
            coverage: decdn_protocol::Coverage::empty(),
        }
    }

    /// Two probed holders resolved the way discovery resolves them, with
    /// different measured coverage: the targets, then each holder's node.
    pub(crate) fn two_holder_targets() -> (ResolvedTargets, NodeCandidate, NodeCandidate) {
        let mut a = holder(1, 10.0);
        a.coverage = decdn_protocol::Coverage::from_block_indices(4, [0, 1].into_iter());
        let mut b = holder(2, 20.0);
        b.coverage = decdn_protocol::Coverage::from_block_indices(4, [2, 3].into_iter());
        let (node_a, node_b) = (a.candidate.clone(), b.candidate.clone());
        let probed_samples = vec![(node_a.node_id, a.rtt_ms, 1), (node_b.node_id, b.rtt_ms, 1)];
        let ordered = super::failover_order(
            vec![b, a],
            &[],
            ProxyWarmingParams {
                enabled: false,
                rtt_threshold_ms: 150.0,
                margin_ms: 30.0,
            },
        );
        let targets = ResolvedTargets {
            candidates: ordered.order,
            coverage_by_node: ordered.coverage_by_node,
            probed_samples,
            pinned: false,
            size_hint: Some(128 * 1024 * 1024),
            late: super::LateSlot::default(),
        };
        (targets, node_a, node_b)
    }
}
