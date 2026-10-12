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
//! [`decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore`] is reused (the `(signer, provider)` lane watermark is
//! resumed) — and topped up on-chain via `topUp` (best effort) when its
//! pool-wide remaining deposit has run low, so a sustained series of fetches
//! isn't stranded;
//! otherwise one is opened on-chain (USDC `approve` if needed → `openPool`) via
//! the shared [`decdn_client::buyer_pool::open_pool`] kernel and recorded.
//! One pool fans out to every provider the caller pays (ADR 003) — there is no
//! per-provider open. The chain coordinates resolve flag >
//! `[blockchain]`/`[identity]` config > default.
//!
//! Discovery, lane building and pool funding are `decdn-client`'s engine
//! seams ([`decdn_client::targets`], [`decdn_client::lanes`],
//! [`decdn_client::funding`], [`decdn_client::lane_sources`]), which
//! `decdn bundle pull` drives too. This module resolves the command's flags
//! and config into their inputs, and renders what the fetch did.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use alloy::primitives::U256;
use decdn_client::funding::{RunFunding, cached_pool_token};
use decdn_client::lane_sources::LaneSources;
use decdn_client::lanes::{LaneDeps, STALL_WINDOW, SettleOnDrop};
use decdn_client::session::{
    ChainAdoption, ChainConfig, DepositPolicy, DiscoveryOptions, PinnedNode, ProxyWarmingParams,
    PullSite,
};
use decdn_client::targets::{
    ProbeOpts, ProbeRound, ResolveConfigFault, holders_or_none, resolve_target_node,
};
use decdn_client::{
    Connections, DownloadTarget, Downloader, Holder, NoAffordableSource, NoCache, NoSourceHasBlob,
    PeerHealth, PoolContext, PoolReplaced, ProgressClock, PullConfig, PullDeadlines, RecoveryGate,
    StopPolicy, Streamer, UpstreamRefused, UpstreamVoucherRejected,
};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::eth_identity::{self, PasswordUse, load_signer};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::rate::min_payment;
use decdn_incentive::{CapabilityGrant, PoolId, slash_judge_domain, voucher_domain};
use decdn_protocol::client::StreamError;
use iroh::{Endpoint, PublicKey, RelayUrl};

use decdn_client::endpoint as client_endpoint;
use decdn_client::provider;

use super::buyer_store::{DataDirSource, chain_adoption_for_buy, open_client_store_for_buy};
use super::fetch_timings::{FetchTimings, Mark};
use super::interrupt::{Interrupt, Interrupted};
use super::tab_progress::TabProgress;
use decdn_client::ordered_writes::OrderedWrites;

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
    /// The chain the command pays on. Its `capacity_bond` is `None` when
    /// neither the flag nor `blockchain.capacity_bond_address` is set — only
    /// an error on the discovery path, never on the explicit-node path.
    pub(crate) config: ChainConfig,
    /// Where the command keeps its state, and the client's region. The data
    /// dir holds the buyer-pool redb store and (by default) the keystore. It
    /// is client-scoped (`~/.decdn/client`) unless an explicit
    /// `--data-dir`/`identity.data_dir` is given. The region is `--region` >
    /// `identity.region`. The region allowlist is `[client] region_allowlist`;
    /// invalid entries (fail `Region::parse`) are dropped with a
    /// `tracing::warn!` in [`resolve_chain`] rather than failing config
    /// resolution — a typo'd region code shrinks the filter, it does not
    /// break the fetch.
    pub(crate) site: PullSite,
    pub(crate) keystore: PathBuf,
    /// File holding the keystore password, consulted after the
    /// `DECDN_KEYSTORE_PASSWORD` env var and before a prompt. CLI/env-only, like
    /// the daemon's `ResolvedBlockchain::keystore_password_file` — passwords do
    /// not belong in a config file even by reference.
    pub(crate) keystore_password_file: Option<PathBuf>,
    /// Which step of the ladder produced the data dir. A node's data dir
    /// is acceptable when the operator named it and not when it merely fell out
    /// of the config file, so the guard needs the provenance, not just the path
    /// (#2082).
    pub(crate) data_dir_source: DataDirSource,
    /// Deposit to escrow when OPENING a pool, and the target a reused pool's
    /// proactive refill restores toward once it has served verified bytes.
    pub(crate) working_deposit: U256,
    pub(crate) max_approve: bool,
}

impl ResolvedChain {
    /// How the command funds its pool. A delegated command (`--capability`)
    /// opens no pool, so it adopts none either; a self-owned one adopts the
    /// wallet's live pool only when neither the data dir nor the keystore
    /// belongs to a node ([`chain_adoption_for_buy`]).
    ///
    /// # Errors
    ///
    /// The owner of the data dir or the keystore's dir cannot be told.
    pub(crate) fn deposit_policy(&self, delegated: bool) -> anyhow::Result<DepositPolicy> {
        let adoption = if delegated {
            ChainAdoption::Refused
        } else {
            chain_adoption_for_buy(&self.site.data_dir, &self.keystore)?
        };
        Ok(DepositPolicy {
            working_deposit: self.working_deposit,
            max_approve: self.max_approve,
            adoption,
        })
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
        config: ChainConfig {
            rpc_url,
            chain_id,
            payment_pool,
            slash_judge,
            capacity_bond,
        },
        site: PullSite {
            data_dir,
            region,
            region_allowlist,
        },
        keystore,
        keystore_password_file: args.keystore_password_file.as_deref().map(expand_tilde),
        data_dir_source,
        working_deposit,
        max_approve,
    })
}

/// Client-side proxy-warming knobs (#1174, ADR 037), distilled from
/// [`cli::ClientFetchArgs`]. RTT thresholds are held as `f64` ms to compare
/// directly against probe RTTs.
pub(crate) fn proxy_warming(args: &cli::ClientFetchArgs) -> ProxyWarmingParams {
    // Widen through u32 so the u64 → f64 cast is lossless (ms knobs are
    // small); saturate the implausible overflow rather than lose precision.
    let ms = |v: u64| f64::from(u32::try_from(v).unwrap_or(u32::MAX));
    ProxyWarmingParams {
        enabled: args.proxy_warming,
        rtt_threshold_ms: ms(args.proxy_warming_rtt_threshold_ms),
        margin_ms: ms(args.proxy_warming_margin_ms),
    }
}

/// How the command finds a blob's holders, from its flags: the `--node-id`
/// it pins (with `--provider-address` and any `--addr`), or discovery.
///
/// # Errors
///
/// A [`ResolveConfigFault`] for a malformed `--node-id` or
/// `--provider-address`, or a `--node-id` without `--provider-address`.
pub(crate) fn discovery_options(args: &cli::ClientFetchArgs) -> anyhow::Result<DiscoveryOptions> {
    Ok(DiscoveryOptions {
        pinned: pinned_node(args)?,
        rediscover: args.rediscover,
        max_sources: args.max_sources,
        registry_timeout: args.discovery_cap(),
        proxy_warming: proxy_warming(args),
    })
}

/// The node `--node-id` pins, or `None` to discover. `clap` guarantees
/// `--provider-address` is present alongside `--node-id`.
fn pinned_node(args: &cli::ClientFetchArgs) -> anyhow::Result<Option<PinnedNode>> {
    let Some(raw) = &args.node_id else {
        return Ok(None);
    };
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
    Ok(Some(PinnedNode {
        node_id,
        provider,
        addr: args.addr,
    }))
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
    // spend against it. `None` => the unchanged self-owned pool path. Zeroes
    // the working deposit in `chain` (a delegate cannot fund an owner's pool).
    let grant = resolve_delegation_grant(common, &mut chain)?;

    // The buyer-pool store, opened once and recorded into by open-or-reuse.
    // The guard runs here, before the keystore password prompt: the answer
    // depends only on the data dir, and a refusal must not cost a prompt first.
    // Shared, so a lane's watermark write runs off the runtime thread.
    let store: Arc<dyn BuyerPoolStore> = Arc::new(open_client_store_for_buy(
        &chain.site.data_dir,
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
    store: &Arc<dyn BuyerPoolStore>,
    endpoint: &Endpoint,
    writes: &OrderedWrites,
    timings: &FetchTimings,
) -> anyhow::Result<()> {
    let common = &args.common;
    // The holders to start from: the explicit `--node-id`, or auto-discovery.
    // Only a configuration fault ends the fetch here; with no holder yet, the
    // first open discovers them. The round streams: the fetch starts at the
    // first verified holder, and the holders that answer later join it.
    let discovery = discovery_options(common)?;
    let probe = ProbeOpts {
        marks: Some(timings),
        round: ProbeRound::Stream,
    };
    let resolved = holders_or_none(
        resolve_target_node(
            &discovery,
            (&chain.config, &chain.site),
            endpoint,
            relays,
            hash,
            probe,
        )
        .await,
    )?;
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

    let config = &chain.config;
    let rpc = provider::build_provider(&config.rpc_url, &signer)?;
    let contract = PaymentPool::new(config.payment_pool, rpc.clone());
    let voucher_dom = voucher_domain(config.chain_id, config.payment_pool);
    let slash_dom = slash_judge_domain(config.chain_id, config.slash_judge);
    // The pool's token is `PaymentPool.usdc()`, immutable per contract. A repeat
    // fetch already holds it in the persisted pool row, so read it there and skip
    // the eth_call; only a first-ever pool (no row) pays the `usdc()` round-trip.
    // The reuse branch of `open_or_reuse_pool` already trusts this same
    // `state.token`, so this only makes the top-level value consistent with it.
    let token = match cached_pool_token(store.as_ref(), self_address, config.deployment())? {
        Some(token) => token,
        None => contract
            .usdc()
            .call()
            .await
            .map_err(|e| anyhow::Error::new(e).context("read PaymentPool.usdc()"))?,
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

    let clock = Arc::new(ProgressClock::new());
    let stop = StopPolicy::new(
        std::io::IsTerminal::is_terminal(&std::io::stderr()),
        common.give_up_after(),
        clock,
    );
    let health = Arc::new(PeerHealth::default());
    let deposit = chain.deposit_policy(grant.is_some())?;
    // One funding recovery state for the whole fetch, both passes included.
    let recovery = Arc::new(RecoveryGate::new());
    // Set once a funding recovery step replaced the pool: the second pass then
    // runs the remaining work against the new pool, and resumes the `.partial`.
    let mut replaced: Option<PoolReplaced> = None;
    loop {
        // Per pass: the funding facts, the lanes and their handles all name the
        // pool the pass pays from.
        let funding = RunFunding::default();
        // The shared pull/funding deps every lane borrows for the whole pass.
        let deps = LaneDeps {
            endpoint,
            store,
            contract: &contract,
            rpc: &rpc,
            slash_dom: &slash_dom,
            self_address,
            token,
            chain: config,
            site: &chain.site,
            deposit,
            namespace_id,
            max_rate_per_mb: common.max_rate_per_mb,
            max_blob_bytes,
            deadlines,
            connections: &connections,
            writes,
            funding: &funding,
            marks: Some(timings),
        };
        // The fetch's lanes build concurrently and every one opens or reuses the
        // one pool on-chain, so the open-or-reuse runs one lane at a time, as it
        // does across a bundle pull's entries.
        let open_lock = tokio::sync::Mutex::new(());
        let sources = LaneSources::new(
            &deps,
            &discovery,
            relays,
            grant.as_ref(),
            &signer,
            &voucher_dom,
            Some(&open_lock),
            None,
            None,
            &recovery,
        );
        let holders = sources.holders_from(&resolved);
        if replaced.is_none() {
            timings.set_holders_start(holders.len());
        }
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
                    Arc::clone(&health),
                    hash,
                    claim.total_bytes,
                    stop.clone(),
                    (common.max_sources, timings),
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
                (common.max_sources, timings),
            )
            .await
        }
        .await;
        on_drop.disarm();
        sources.persist_watermarks().await;
        if let Some(shortfall) = funding.shortfall() {
            eprintln!("warning: {shortfall}");
        }
        let result = result.map_err(|err| annotate(err, &sources));
        match next_pass(result, &mut replaced, wants_stdout(&args.output)) {
            Some(pass_result) => return pass_result,
            None => recovery.start_next_pass(),
        }
    }
}

/// What a fetch pass that ended with `result` does next: `None` to run the
/// remaining work again against the pool a funding recovery step just opened,
/// or the command's result (ADR 003 § Funding recovery).
///
/// A pass runs again at most once: a second replacement in one command ends it
/// "funding needed". A stream to stdout never runs again, because the bytes it
/// already wrote cannot be taken back; its replacement ends the command with
/// the new pool named. Once a pass replaced the pool, the closed pool's
/// remaining balance is named on stderr, with the reclaim command.
fn next_pass(
    result: anyhow::Result<()>,
    replaced: &mut Option<PoolReplaced>,
    to_stdout: bool,
) -> Option<anyhow::Result<()>> {
    let now_replaced = result
        .as_ref()
        .err()
        .and_then(|err| err.downcast_ref::<PoolReplaced>().copied());
    if let Some(new) = now_replaced
        && replaced.is_none()
        && !to_stdout
    {
        eprintln!("note: {new}");
        *replaced = Some(new);
        return None;
    }
    if let Some(earlier) = (*replaced).or(now_replaced) {
        eprintln!(
            "note: pool {} accepts no more funds and may still hold a balance; once its \
             dispute window ends, reclaim it with: decdn pool reclaim --pool {}",
            earlier.closed, earlier.closed
        );
    }
    Some(result.map_err(|err| match (now_replaced, *replaced) {
        (Some(_), Some(_)) => err.context(
            "funding needed: the pool this command opened accepts no more funds either; run \
             the command again",
        ),
        (Some(new), None) => err.context(format!(
            "pool {} is now the current pool; run the command again to stream from it",
            new.opened
        )),
        _ => err,
    }))
}

/// Reconnect a delegated fetch that ended "funding needed" (ADR 003 §Funding
/// recovery) to the owner-side remedy: the delegate holds no wallet on this
/// pool, so it cannot `topUp`, raise its own cap, or mint itself a fresh
/// capability. A [`decdn_client::FundingNeeded::PublisherPool`] stop names the
/// pool owner's top-up: the capability still covers the fetch. A
/// [`decdn_client::FundingNeeded::NewCapability`] stop, any other
/// [`NoAffordableSource`] stop, or a funding voucher rejection
/// (`SpendingCapExhausted`, `CapabilityExpired`, `PoolExhausted`,
/// `SignerCapExhausted`) names a fresh capability for a new signer key,
/// because a registered signer's terms are write-once. A terminal `Underpaid`
/// (the resync budget ran out) gets its own next step. Any other error passes
/// through verbatim (a stall, a transport fault, a "no node will serve this",
/// or a `NotFound` already annotated by [`annotate_unbound_cache_miss`]).
pub(crate) fn annotate_delegated_exhaustion(err: anyhow::Error) -> anyhow::Error {
    use decdn_protocol::client::VoucherRejectReason;

    let funding_rejection = err
        .downcast_ref::<UpstreamVoucherRejected>()
        .is_some_and(|rejected| {
            matches!(
                rejected.reason,
                VoucherRejectReason::SpendingCapExhausted
                    | VoucherRejectReason::CapabilityExpired
                    | VoucherRejectReason::PoolExhausted
                    | VoucherRejectReason::SignerCapExhausted
            )
        });
    let underpaid = err
        .downcast_ref::<UpstreamVoucherRejected>()
        .is_some_and(|rejected| rejected.reason == VoucherRejectReason::Underpaid);
    if let Some(&decdn_client::FundingNeeded::PublisherPool { pool }) = err.downcast_ref() {
        return err.context(format!(
            "ask the owner of pool {pool} to top it up: the capability still covers the fetch, \
             and a delegated client cannot top up a pool it does not own. A node refuses a \
             signer whose registered terms are spent the same way; once this signer key is \
             registered on-chain its terms are write-once, so the owner then issues a \
             capability for a new signer key"
        ));
    }
    if funding_rejection
        || err.downcast_ref::<NoAffordableSource>().is_some()
        || err.downcast_ref::<decdn_client::FundingNeeded>().is_some()
    {
        err.context(
            "funding needed: no provider serves this capability at its current funding. Ask \
             the pool owner to top up the pool or issue a fresh capability; once this signer \
             key is registered on-chain its terms are write-once, so the fresh capability must \
             name a new signer key (a delegated client cannot top up a pool it does not own)",
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

/// Reconnect `err` to what the user can do about it: an unbound or
/// underfunded cache miss ([`annotate_unbound_cache_miss`]), and a pool no
/// provider's voucher fits — the owner-side remedy on a delegated pool
/// ([`annotate_delegated_exhaustion`]), else the top-up command.
pub(crate) fn annotate<P>(err: anyhow::Error, sources: &LaneSources<'_, P>) -> anyhow::Error
where
    P: alloy::providers::Provider + Clone,
{
    let err = match sources.first_lane_ctx() {
        Some(ctx) => annotate_with(err, &ctx),
        None => err,
    };
    if sources.is_delegated() {
        return annotate_delegated_exhaustion(err);
    }
    with_top_up_hint(err, sources.pool_id())
}

/// Name the top-up command on a self-owned pool no provider's voucher fits.
pub(crate) fn with_top_up_hint(err: anyhow::Error, pool_id: Option<PoolId>) -> anyhow::Error {
    if err.downcast_ref::<NoAffordableSource>().is_none() {
        return err;
    }
    let pool = pool_id.map_or_else(|| "<pool id>".to_string(), |id| id.to_string());
    err.context(format!(
        "top up with: decdn pool top-up --pool {pool} --amount-micro-usdc <MICRO_USDC>"
    ))
}

/// [`annotate_unbound_cache_miss`] against the lane context `ctx`.
fn annotate_with(err: anyhow::Error, ctx: &Mutex<PoolContext>) -> anyhow::Error {
    match ctx.lock() {
        Ok(guard) => annotate_unbound_cache_miss(err, &guard),
        Err(_) => err,
    }
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
    deps: &LaneDeps<'_, P>,
    sources: &LaneSources<'_, P>,
    holders: Vec<Holder>,
    health: Arc<PeerHealth>,
    hash: [u8; 32],
    total_bytes: u64,
    stop: StopPolicy,
    (max_sources, timings): (usize, &FetchTimings),
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

    // The fill store's `.partial` lives here for the stream's lifetime; a streamed
    // blob is not kept, so a temp dir (removed on drop) under the data dir is its
    // natural home.
    let scratch = tempfile::tempdir_in(&deps.site.data_dir)
        .map_err(|e| anyhow::anyhow!("open stream scratch dir: {e}"))?;
    let mut streamer = Streamer::new(sources, funder, scratch.path())
        .holders(holders)
        .health(health)
        .recovery(Arc::clone(sources.recovery()))
        .max_blob_bytes(deps.max_blob_bytes);
    if let Some(slot) = sources.credentials() {
        streamer = streamer.credentials(slot);
    }
    let (mut reader, mut drive) = streamer
        .open(hash, total_bytes, &pull_config, Arc::new(NoCache), stop)
        .await?;

    // The bar draws on stderr, so it shows only when stderr is a terminal; stdout
    // carries the verified bytes either way.
    let bar = std::io::IsTerminal::is_terminal(&std::io::stderr()).then(delivery_progress);
    // `copy_verified` reports after each write, so the first report with a
    // byte is the first byte on stdout.
    let on_progress = |received: u64, expected: u64| {
        if received > 0 {
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
    deps: &LaneDeps<'_, P>,
    sources: &LaneSources<'_, P>,
    holders: Vec<Holder>,
    health: Arc<PeerHealth>,
    target: DownloadTarget<'_>,
    stop: &StopPolicy,
    (max_sources, timings): (usize, &FetchTimings),
) -> anyhow::Result<()>
where
    P: alloy::providers::Provider + Clone,
{
    let (bar, on_bar, meter) = delivery_progress();
    let on_progress = |received: u64, expected: u64| {
        if received > 0 {
            timings.mark(Mark::FirstByte);
        }
        on_bar(received, expected);
    };
    let mut downloader = Downloader::new(sources, sources.funder())
        .holders(holders)
        .health(health)
        .recovery(Arc::clone(sources.recovery()))
        .max_lanes(max_sources)
        .max_blob_bytes(deps.max_blob_bytes);
    if let Some(slot) = sources.credentials() {
        downloader = downloader.credentials(slot);
    }
    let result =
        Box::pin(downloader.fetch_to_paths_until(&[target], Some(&on_progress), stop)).await;
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
/// [`CapabilityGrant`], and, when one is present, zero the working deposit
/// in `chain`: a delegate owns no pool it could `topUp`, so its funding
/// recovery step has nothing to add ([`decdn_client::funding::PoolFunder`]). `None` selects the
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
mod tests;
