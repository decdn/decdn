//! The deCDN client: fetch content-addressed blobs from deCDN nodes, pay for
//! them per megabyte from a USDC payment pool, and verify every byte against
//! its BLAKE3 hash as it arrives.
//!
//! The `decdn` CLI's `fetch` and `bundle pull` are built on this crate. So is
//! the node's own cache-miss pull: a node that misses in its cache is a paying
//! client of the node upstream of it.
//!
//! # Choose an entry point
//!
//! | You want to | Use |
//! |---|---|
//! | Save one blob, or a set of blobs, to files as fast as possible, and resume a partial download | [`Downloader`] |
//! | Read one blob in order, and pay only for what your reader reaches | [`Streamer`] |
//! | Drive the pull loop yourself (a node, or a custom scheduler) | [`driver::drive`], [`multi_source_fetch`], [`open_progressive_pull`] |
//!
//! Most callers want one of the two faces. The [`Downloader`] stripes a blob
//! across every holder at once, and writes each verified range at its offset in
//! a `.partial` file beside the destination. A rerun fetches only the ranges
//! that file does not hold yet. The [`Streamer`] fetches at most one read-ahead
//! window ([`PullConfig::read_ahead_bytes`]) ahead of its reader, so a reader
//! that stops early stops the spend within that window.
//!
//! # Before you fetch
//!
//! A fetch needs a funded payment pool, an iroh endpoint, and one paid lane per
//! node that holds the blob. The `download` and `stream` examples in this
//! crate's `examples/` directory run the whole sequence:
//!
//! 1. **Load the buyer key.** Vouchers and pool transactions are signed with a
//!    [`PrivateKeySigner`].
//! 2. **Reuse or open a pool.** Read the pool the buyer store already records
//!    ([`decdn_incentive::BuyerPoolStore::get_by_owner`]). If there is none,
//!    approve the deposit ([`buyer_pool::ensure_allowance`]), open a pool
//!    ([`buyer_pool::open_pool`]), and record the returned state at once: the
//!    deposit is escrowed from the moment `open_pool` returns.
//! 3. **Bring up an endpoint** with [`endpoint::client_endpoint`].
//! 4. **Find the holders.** Read the registered nodes
//!    ([`discovery::bootstrap_nodes`]), pick a sample
//!    ([`discovery::select_candidates`]), and probe each one
//!    ([`probe::probe_once`]). Verify every answer
//!    ([`probe::verify_probe_response`]) before it counts, and drop an answer
//!    whose `has_blob` and coverage disagree. A verified answer also gives the
//!    blob's size.
//! 5. **Build one lane per holder.** Pin a [`PoolContext`] to the holder's
//!    provider address with [`buyer_pool::self_owned_lane_ctx`], resuming at
//!    what the lane has already paid. Create its ledger with
//!    [`PoolContext::new_ledger`], and wrap both in a [`PeerSource`] inside a
//!    [`StreamCandidate`]. Cap the lane's rate at the rate the node signed in
//!    its probe answer ([`effective_rate_ceiling`]).
//! 6. **Fetch** with [`Downloader::fetch_to_paths`] or [`Streamer::open`].
//! 7. **Record what each lane paid**, whatever the outcome: build a
//!    [`VoucherProgress`] from the ledger's settlement, and apply the
//!    [`buyer_pool::ProgressWrite`] it calls for to the buyer store.
//!
//! # What the crate guarantees
//!
//! - **Every byte is verified.** Delivery is BLAKE3/bao verified streaming.
//!   The decoder checks each chunk group against the hash as it lands, so a
//!   corrupt range aborts the pull at that group. Nothing unverified reaches a
//!   [`VerifiedReader`] or a finished file.
//! - **A dishonest node's bill is bounded.** You pay per chunk as it arrives.
//!   What a lying node can charge you is at most one framed message plus one
//!   payment interval ([`decdn_protocol::CHUNK_BYTES`]), never the size it
//!   claimed.
//! - **Quotes are checked against your ceiling.** A stream response whose
//!   signed rate exceeds the `max_rate_per_mb` given to [`PeerSource::new`] is
//!   refused before any payment ([`RateAboveCeiling`]).
//! - **Failover is free.** Every lane draws on one shared pool, so a holder that
//!   stalls or refuses is dropped and its range goes to another holder. No
//!   delivered byte is fetched or paid for twice.
//!
//! # What stays yours
//!
//! - **Persistence.** The faces never write the buyer store. Record every
//!   lane's payment after each fetch (step 7). A lane you do not record resumes
//!   from a stale watermark next time, and its provider rejects the vouchers.
//! - **Funding policy.** A [`source::Funder`] decides whether a fetch that runs
//!   the pool low tops it up. One whose [`max_topups`](source::Funder::max_topups)
//!   is `0` never does. The fetch then fails with a [`PoolExhausted`], and
//!   [`shared_pool_disposition`] classifies that as terminal.
//! - **Which holders to use.** Discovery gives candidates; ordering and
//!   admission ([`discovery::admit_sources`]) are the caller's choice.
//!
//! # Mistakes to avoid
//!
//! - **One candidate per provider.** Each [`StreamCandidate`] pays one
//!   `(signer, provider)` lane. Two candidates for one provider sign competing
//!   vouchers on the same lane.
//! - **Always set a rate ceiling.** A `max_rate_per_mb` of `0` means no
//!   ceiling, so a node can quote low on the probe and high on the stream.
//!   Pass [`effective_rate_ceiling`] of the probed rate and any absolute cap you
//!   hold.
//! - **Never resume a lane below its on-chain watermark.** A voucher at or below
//!   the watermark redeems nothing, so the provider streams bytes it can never
//!   cash. A lane the buyer store has not seen resumes from the chain's
//!   `getWatermark`.
//! - **Keep the stream drive running.** [`Streamer::open`] returns a reader and
//!   a [`StreamDrive`]. Run your read inside [`StreamDrive::alongside`]: the
//!   drive pays and drains open legs, and it must not wait behind a blocked
//!   consumer.
//! - **Progress is in content bytes.** A [`ProgressCallback`] on the faces
//!   reports verified content bytes against the blob's size.
//!
//! # How delivery works
//!
//! [`open_progressive_pull`] sends a [`StreamRequest`], verifies the signed
//! [`StreamResponse`], and returns an [`UpstreamPull`]: the one receive loop.
//! It reads `ChunkData` and pays as it goes. It releases one hash-chain preimage
//! per delivered `CHUNK_BYTES` chunk, and signs a voucher to open a chain, to
//! roll one, and to settle a residual shorter than a chunk.
//!
//! The `ChunkData` payload is bao's interleaved verified-stream encoding, not
//! raw bytes. Every consumer feeds it to a `bao-tree` verifying decoder as it
//! arrives, through a [`sink::PullReader`] over the live pull: the ranged
//! store's [`ClientRangedStore::ingest_stream`], the node's cache admit, or the
//! `test-util` in-memory decoder. The decoder checks every chunk group against
//! the requested root. A range fetched from any offset therefore verifies on
//! its own, with no dependency on earlier bytes (ADR 038).

/// Buyer-side `PaymentPool` open kernel (#940), shared by the node service
/// and the CLI.
pub mod buyer_pool;
/// Zero-config tunables for the consumption faces over the paid pull engine
/// ([`config::PullConfig`], #1848): every knob defaults, so a caller overrides
/// only what it must, and construction needs no network or chain access.
pub mod config;
/// A caller-owned QUIC connection kept warm across many hash fetches
/// ([`connection::WarmConnection`]): one dial amortized over every hash, one
/// bi-stream per hash (no wire change), closed once on the handle's own `Drop`.
pub mod connection;
/// Range-keyed discovery coverage-map primitive plus the two
/// objective-specific planners over it (#1506): [`coverage_plan::plan_covered_runs`]
/// (node — concentrate + sticky) and `coverage_plan::spread_segments` (client —
/// spread for parallelism). Pure, no I/O.
pub(crate) mod coverage_plan;
/// Client-side node discovery (#936): read + select the active node set from
/// `CapacityBond.getRegisteredNodes`, then rank probed blob-holders.
pub mod discovery;
/// The `Downloader` consumption face (#1848 T4): fetch a set of content-addressed
/// blobs (a bundle, or a single blob) to files in a directory, out-of-order and
/// at full throughput, reusing [`ClientRangedStore`] + [`driver::drive`] so every
/// byte is bao-verified and a resumed fetch re-pulls only the missing ranges.
pub mod downloader;
/// The #1608 gap-driven fetch driver: [`driver::drive`] fills only the
/// [`missing_ranges`](decdn_bao_range::RangedStore::missing_ranges) of a request,
/// paying the minimum, by folding the resume / top-up / settle-wait / reseed loop
/// behind the [`pacer::Pacer`] + [`source::Funder`] axes.
pub mod driver;
/// One-shot client `Endpoint` construction: relay + discovery resolution for
/// the `cdn/client/v1` and `cdn/probe/v1` dial paths (#935/#936).
pub mod endpoint;
mod ledger;
/// Run-scoped registry of live per-lane voucher ledgers:
/// [`ledgers::LaneLedgers`] maps each `(pool_id, signer, provider)` lane to
/// the one [`ledgers::LaneHandle`] every concurrent fetch on that lane shares.
pub mod ledgers;
/// The pure pacing axis (#1608): [`pacer::Pacer`] / [`pacer::BudgetPacer`] decide
/// draw / top-up / wait / done / refuse for the gap-driven driver, with no I/O.
pub mod pacer;
/// Persisted per-peer knowledge base: registry-fed identity plus interaction-fed
/// latency and price, keyed by iroh [`iroh::PublicKey`], one JSON file per peer.
pub mod peer_store;
/// Reusable `cdn/probe/v1` client.
pub mod probe;
/// Sub-frame byte-progress observation (#1797): a `ProgressReader` that tallies bytes
/// off the QUIC stream beneath the message decode, and the `ThroughputFloor` that judges
/// those bytes against a minimum rate over a trailing window.
mod progress;
/// Wallet-filled HTTP provider builder for opening/settling payment channels.
pub mod provider;
/// Client-side [`decdn_bao_range::RangedStore`] backend (#1621): a
/// `.partial` + sidecar store built on `bao-tree`/`decdn-bao-range` only.
pub mod ranged_store;
/// The `APP_ERR_RATE_LIMITED` (`0x10`) transport shed, typed for the pull
/// orchestrator (ADR 013 §Application Error Codes).
pub(crate) mod rate_limited;
/// Failover classification (#1174, ADR 037 § Fallback): decide whether a fetch
/// failure is terminal or worth retrying against another provider/lane. Shared by
/// the CLI single-source loop and the multi-source scheduler.
pub mod retry;
mod scheduler;
/// Pure segmentation and tail-steal helpers for the multi-source scheduler
/// (spec §5.3): no I/O, no async.
mod segment;
/// The `Streamer` consumption face (#1848 T6): stream one blob's verified,
/// contiguous front to a consumer as it arrives, paced by consumption and bounded
/// to one read-ahead window ahead of the read cursor. A fetch-like single-blob
/// face over the same engine, with no chunk dedup.
pub mod streamer;
// Docs live in `sink.rs` as `//!`. Deliberately NOT documented here as well:
// rustdoc resolves intra-doc links on a `mod` item in THIS file's scope, so the
// module's own links (`content_paid_frontier`, …) would go unresolved
// and fail the `-D warnings` doc gate.
pub mod sink;
/// The two sourcing axes of the gap-driven driver (#1608): [`source::BlobSource`]
/// (raw-bao byte source for a range) and [`source::Funder`] (injected top-up
/// seam), plus scripted test doubles.
pub mod source;

pub use config::PullConfig;
pub use connection::WarmConnection;
pub use coverage_plan::{CoveredRun, SourceCoverage, plan_covered_runs};
pub use decdn_bao_range::RangedStore;
pub use downloader::{DownloadTarget, Downloader};
pub use driver::{
    PacingWait, PoolExhausted, SharedPool, WaitReason, drive, drive_range_set, first_leg,
};
pub use ledger::{ChainCommit, Cumulative, EpochAction, Metered, PoolLedger, Rebase, Released};
pub use ledgers::{LaneHandle, LaneLedgers};
pub use pacer::{
    BudgetPacer, DownstreamFrontier, MIN_DRAW_WINDOW, PULL_WINDOW_FLOOR, PaceDecision, PaceState,
    Pacer, RampPacer, WindowPacer,
};
pub use peer_store::{PeerRecord, PeerStore, StoreConfig};
pub use progress::throughput_watchdog;
pub use ranged_store::ClientRangedStore;
pub use rate_limited::UpstreamRateLimited;
pub use retry::{RetryDisposition, retry_disposition, shared_pool_disposition};
pub use scheduler::{ConsumptionPacing, MultiSourceConfig, SourceLane, multi_source_fetch};
pub use sink::{BlobCache, NoCache, SinkFuture};
pub use source::{
    BaoRangeReader, BlobSource, Funder, IngestStore, PeerSource, PrimedSource, SourceFuture,
};
pub use streamer::{LiveReader, StreamCandidate, StreamDrive, Streamer, VerifiedReader};

pub(crate) use ledger::StreamProof;

#[cfg(any(test, feature = "test-util"))]
pub use source::{FakeFunder, ScriptedReader, ScriptedSource};

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
#[cfg(any(test, feature = "test-util"))]
use bao_tree::BaoTree;
#[cfg(any(test, feature = "test-util"))]
use bao_tree::io::BaoContentItem;
#[cfg(any(test, feature = "test-util"))]
use bao_tree::io::fsm::{ResponseDecoder, ResponseDecoderNext};
use bytes::Bytes;
use decdn_bao_range::align_range;
#[cfg(any(test, feature = "test-util"))]
use decdn_bao_range::{AlignedRange, IROH_BLOCK_SIZE};
use decdn_incentive::{
    BuyerPoolState, EPHEMERAL_BINDING_NONCE, SignedCapability, SignedVoucher, StreamSlashData,
    Voucher, binding_signing_hash, signed_to_wire_voucher,
};
use decdn_protocol::client::{
    ClientBinding, ClientMessage, StreamError, StreamRequest, StreamRequestExt, StreamResponse,
    StreamResponseExt, VoucherRejectReason, WatermarkBundle, WireCapability,
};
use decdn_protocol::{
    ALPN_CLIENT, CHUNK_BYTES, ChunkPreimage, decode_message, encode_message, read_frame,
    write_frame,
};
use iroh::endpoint::{RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};
#[cfg(any(test, feature = "test-util"))]
use sink::{PullReader, StashedFault};

/// Per-lane context the requester needs to sign pool vouchers.
///
/// A voucher is scoped to a `(pool_id, signer, provider)` lane. `pool_id` is the
/// on-chain deposit, the signer is `client_signer.address()` (the capability key
/// the pool owner delegated spend to — the owner's own key in the single-user
/// case), and `provider` is the delivering node this stream pays.
///
/// The `prior_*` fields capture the lane's cumulative state from earlier streams
/// so a **reused** lane resumes correctly: vouchers are cumulative across the
/// lane's lifetime, so the node's `last_bytes_delivered` / `last_amount` are
/// non-zero after the first stream. Starting a fresh stream from zero would be
/// rejected (`AmountRegression` / `BytesRegression`). For a brand-new lane pass
/// `U256::ZERO` for both.
#[derive(Clone)]
pub struct PoolContext {
    /// On-chain `poolId` the vouchers draw from.
    pub pool_id: B256,
    /// The delivering node this stream pays — the voucher's `provider` field. A
    /// capability voucher is scoped to one provider and is invalid if redeemed
    /// against another.
    pub provider: Address,
    /// On-chain pool deposit (informational here; the node enforces it).
    pub deposit: U256,
    /// Client key that signs vouchers — its address is the lane's capability
    /// `signer`.
    pub client_signer: Arc<PrivateKeySigner>,
    /// `PaymentPool` EIP-712 domain.
    pub voucher_domain: Eip712Domain,
    /// Cumulative bytes paid for on this lane before this stream.
    pub prior_bytes_delivered: U256,
    /// Cumulative amount paid on this lane before this stream.
    pub prior_amount: U256,
    /// Optional ADR 005 client identity binding (address + `BindNodeId`
    /// signature over the requester's own iroh `NodeId`, see
    /// [`sign_client_binding`]). Attached to every `cdn/client/v1` request's
    /// `ext` so the serving node can recover the buyer address, confirm it owns
    /// the pool (`pull_authorized`), and reactively populate from its configured
    /// origin (#1115). `None` ⇒ no binding is sent (an unconfigured
    /// `capacity_bond` on the client, or an on-chain/registered requester).
    pub client_binding: Option<ClientBinding>,
    /// Optional pool owner capability delegating spend to `client_signer`
    /// (ADR 003 §Capability delegation, D3 `issue_self_capability` /
    /// `buyer_pool::open_pool`). Attached to every `cdn/client/v1` request's
    /// `ext` alongside `client_binding` so the serving node can register the
    /// signer on that signer's first on-chain redemption. Node-agnostic — the
    /// same `SignedCapability` is valid at every node this context streams
    /// from, since it grants spend against the pool rather than a specific
    /// provider. `None` ⇒ no capability is sent (the signer is already
    /// registered on-chain, or this stream reuses a lane a prior stream this
    /// session already delivered the capability for).
    pub capability: Option<SignedCapability>,
}

impl PoolContext {
    /// Build a context for a buyer-held pool, resuming from its persisted
    /// cumulative voucher state on the `(signer, provider)` lane (#744). Pass the
    /// `provider` this stream pays and the lane's prior `(bytes, amount)` so the
    /// next voucher continues the lane rather than restarting from zero (which
    /// the upstream node would reject). For a freshly-opened pool with an
    /// untouched lane, pass `U256::ZERO` for both priors — [`Self::for_pool`]
    /// does exactly that.
    #[must_use]
    pub const fn for_pool(
        state: &BuyerPoolState,
        client_signer: Arc<PrivateKeySigner>,
        voucher_domain: Eip712Domain,
    ) -> Self {
        Self {
            pool_id: state.pool_id,
            // A pool fans out to many providers; the fetch target is pinned
            // per-pull via [`Self::with_provider`], not stored in the pool.
            provider: Address::ZERO,
            deposit: state.deposit,
            client_signer,
            voucher_domain,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        }
    }

    /// Pin the delivering node this context pays (the voucher's `provider`) and
    /// seed the lane's prior cumulative totals. A pool serves many providers, so
    /// the target is chosen per-fetch rather than at pool open.
    #[must_use]
    pub const fn with_provider(
        mut self,
        provider: Address,
        prior_bytes_delivered: U256,
        prior_amount: U256,
    ) -> Self {
        self.provider = provider;
        self.prior_bytes_delivered = prior_bytes_delivered;
        self.prior_amount = prior_amount;
        self
    }

    /// This context's `(pool_id, signer, provider)` lane — the scope of one
    /// hash chain, and the key every seed is derived under.
    #[must_use]
    pub fn lane(&self) -> decdn_incentive::LaneKey {
        decdn_incentive::LaneKey {
            pool_id: self.pool_id,
            signer: self.client_signer.address(),
            provider: self.provider,
        }
    }

    /// The ledger for this lane, seeded from its persisted cumulative and
    /// nothing else.
    ///
    /// A chain is never resumed across a restart — the resume folds the
    /// frontier the node proved into a signed amount and opens a fresh chain
    /// (ADR 003 §Resumption folds) — so there is no chain secret to reproduce
    /// here, no counter to read back, and nothing that could have been
    /// persisted wrong. What carries across an invocation is the money owed.
    #[must_use]
    pub fn new_ledger(&self) -> PoolLedger {
        PoolLedger::new(Cumulative {
            bytes: self.prior_bytes_delivered,
            amount: self.prior_amount,
        })
    }

    /// Attach an ADR 005 client identity binding so this context's
    /// `cdn/client/v1` requests prove pool ownership to the serving node,
    /// enabling reactive cache-miss origin pull-through (#1115). Pass a binding
    /// produced by [`sign_client_binding`]; a hand-built `ClientBinding` whose
    /// `ethereum_address` and signature don't correspond (or that doesn't own the
    /// pool) is rejected by the serving node, so this only ever hurts the caller
    /// itself.
    #[must_use]
    pub fn with_client_binding(mut self, binding: ClientBinding) -> Self {
        self.client_binding = Some(binding);
        self
    }

    /// Attach a pool owner capability so this context's `cdn/client/v1`
    /// requests carry it to the serving node at session start, letting the
    /// node register `client_signer` on that signer's first on-chain
    /// redemption (ADR 003 §Capability delegation). Pass the
    /// `SignedCapability` produced by `buyer_pool::open_pool` /
    /// `issue_self_capability`; a capability whose `signer` doesn't match
    /// `client_signer` or that fails owner-signature recovery only ever hurts
    /// the caller itself, exactly like [`Self::with_client_binding`].
    #[must_use]
    pub const fn with_capability(mut self, capability: SignedCapability) -> Self {
        self.capability = Some(capability);
        self
    }
}

/// Sign an ADR 005 ephemeral client identity binding: an EIP-712
/// `BindNodeId(nodeId, nonce = 0)` attestation over the requester's OWN iroh
/// `NodeId`, signed with the buyer key. The serving node recovers the signer via
/// `ecrecover` (`verify_binding`) and checks it owns the named channel before
/// honoring a cache-miss origin pull (`pull_authorized`, ADR 003 §Off-Chain
/// Ephemeral Binding). Used by the CLI client fetch (#1115) and by node-to-node
/// pulls, where `node_origin` binds its upstream requests so an upstream can
/// chain a further reactive origin pull (#1117).
///
/// `own_node_id` MUST be the requester's own endpoint `NodeId` — what the peer
/// authenticates as `conn.remote_id()` — NOT the target node's. `bind_domain` is
/// `decdn_incentive::bind_node_id_domain(chain_id, capacity_bond)`.
///
/// # Errors
///
/// Propagates a signing error from the buyer signer.
pub fn sign_client_binding(
    signer: &PrivateKeySigner,
    own_node_id: B256,
    bind_domain: &Eip712Domain,
) -> anyhow::Result<ClientBinding> {
    let hash = binding_signing_hash(own_node_id, EPHEMERAL_BINDING_NONCE, bind_domain);
    let binding_signature = signer
        .sign_hash_sync(&hash)
        .map_err(|e| anyhow::anyhow!("sign client binding: {e}").context(LocalPullFault))?
        .as_bytes()
        .to_vec();
    Ok(ClientBinding {
        ethereum_address: signer.address().into(),
        binding_signature,
    })
}

/// Build the trailing [`StreamRequestExt`] carrying the context's client
/// identity binding and/or owner capability, or `None` when the context
/// carries neither — in which case `encode_stream_request` appends no ext
/// bytes. Applied by `open_stream`, the single request site, so the capability
/// rides the same session-start request as the binding.
fn client_binding_ext(ctx: &PoolContext) -> Option<StreamRequestExt> {
    if ctx.client_binding.is_none() && ctx.capability.is_none() {
        return None;
    }
    Some(StreamRequestExt {
        binding: ctx.client_binding.clone(),
        capability: ctx.capability.as_ref().map(|signed| WireCapability {
            spending_cap: signed.capability.spending_cap,
            expiry: signed.capability.expiry,
            owner_signature: signed.signature.as_bytes().to_vec(),
        }),
    })
}

impl std::fmt::Debug for PoolContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolContext")
            .field("pool_id", &self.pool_id)
            .field("provider", &self.provider)
            .field("deposit", &self.deposit)
            .finish_non_exhaustive()
    }
}

/// The lane's committed voucher watermark, returned by [`UpstreamPull::finish`] /
/// [`UpstreamPull::abort`] / [`UpstreamPull::progress`] and threaded through the
/// `test-util` `stream_fetch_tracked` wrapper as an out-param, so the caller can
/// persist what it paid (#852).
///
/// A pull runs through a [`PoolLedger`] seeded from the lane's prior cumulative
/// state; this watermark is that ledger read back
/// ([`VoucherProgress::from_ledger`]) — so it
/// always holds the **absolute** cumulative totals of the last presumed-accepted
/// voucher (not per-stream deltas), exactly the `(bytes_delivered, amount)` pair
/// the buyer-pool lane record expects. Because the
/// copy-back runs on every return path (including the `Err`/timeout arms), the
/// latest totals survive a mid-stream failure or a paid-but-corrupt delivery.
///
/// **Implicit acceptance.** The watermark tracks vouchers the upstream is
/// presumed to have accepted — continued delivery is acceptance (ADR 005), so a
/// sent voucher advances it and only an explicit `VoucherRejected` rewinds it.
///
/// Read the persistable totals via [`VoucherProgress::advanced`], which yields
/// `None` when nothing was paid on this stream (so there is nothing to persist).
#[derive(Clone, Copy, Debug, Default)]
pub struct VoucherProgress {
    /// Cumulative lane bytes paid for as of the last committed voucher.
    bytes_delivered: U256,
    /// Cumulative lane amount paid as of the last committed voucher.
    amount: U256,
    /// Whether the watermark moved past the lane's seed on this stream.
    advanced: bool,
    /// The node's watermark the lane's ledger rebased DOWN to
    /// ([`PoolLedger::rebase`]) and no persist has recorded yet. The caller
    /// overwrites the lane record with it once, then advances to the totals
    /// above.
    rebase_anchor: Option<Cumulative>,
}

impl VoucherProgress {
    /// Build the watermark from a ledger [`Cumulative`] plus the lane's seed
    /// amount. `advanced` is set when the cumulative `amount` rose past the seed,
    /// preserving the "something was paid iff the watermark advanced" contract
    /// even when the ledger was shared across concurrent streams.
    ///
    /// Public because a caller that owns its ledger persists from it directly
    /// rather than through the `&mut VoucherProgress` out-param — including from a
    /// `Drop`, where nothing can be awaited and [`PoolLedger::committed`] is the
    /// only readable source (#1145 review).
    #[must_use]
    pub fn from_cumulative(cum: Cumulative, prior_amount: U256) -> Self {
        Self {
            bytes_delivered: cum.bytes,
            amount: cum.amount,
            advanced: cum.amount > prior_amount,
            rebase_anchor: None,
        }
    }

    /// Attach the watermark the ledger rebased DOWN to and has not persisted
    /// yet ([`PoolLedger::take_unsaved_rebase`]).
    #[must_use]
    pub const fn with_rebase_anchor(mut self, anchor: Option<Cumulative>) -> Self {
        self.rebase_anchor = anchor;
        self
    }

    /// The watermark to overwrite the lane record with before advancing, when
    /// the ledger rebased down to the node's and no persist has recorded it
    /// yet. A monotone advance refuses a lower watermark, so without the
    /// overwrite the next run would sign from the anchor the node refused.
    #[must_use]
    pub const fn rebase_anchor(&self) -> Option<Cumulative> {
        self.rebase_anchor
    }

    /// The same watermark, read from a live ledger.
    ///
    /// Prefer this wherever a ledger is in hand: it reads
    /// [`PoolLedger::settlement`] rather than a copied-back cumulative, so a
    /// voucher left armed by an ambiguous send is still reported as owed.
    ///
    /// It also TAKES the ledger's unsaved rebase anchor, so call it only where
    /// the result is persisted: the anchor is handed out once.
    #[must_use]
    pub fn from_ledger(ledger: &PoolLedger, prior_amount: U256) -> Self {
        // Take the anchor first: every settlement read after it is at or above it.
        let anchor = ledger.take_unsaved_rebase();
        Self::from_cumulative(ledger.settlement(), prior_amount).with_rebase_anchor(anchor)
    }

    /// The cumulative `(bytes_delivered, amount)` to persist via the lane record,
    /// or `None` if nothing new was paid on this stream (nothing to record).
    #[must_use]
    pub fn advanced(&self) -> Option<(U256, U256)> {
        self.advanced.then_some((self.bytes_delivered, self.amount))
    }

    /// The cumulative `(bytes_delivered, amount)` this watermark carries,
    /// whether or not it advanced past the seed.
    #[must_use]
    pub const fn totals(&self) -> (U256, U256) {
        (self.bytes_delivered, self.amount)
    }
}

/// The upstream delivered bytes that failed bao verification against the
/// requested content root — a paid-but-corrupt delivery (the content-addressing
/// invariant, ADR 014/038). Under ADR 038 the verifier is the per-chunk-group
/// `bao-tree` decoder fed as bytes arrive, not a whole-blob re-hash, so this
/// fires the moment any group's proof mismatches. Returned (via `anyhow`) by
/// this crate's decoding consumers — [`ClientRangedStore::ingest_stream`] and the
/// `test-util` in-memory decoder — so callers can `downcast_ref` to classify corruption
/// (e.g. a reputation `Corruption` outcome) without matching on the error
/// message string. The `Display` text is kept stable for logs and the existing
/// requester tests. The node's cache admit reports the same fault as
/// `CacheError::VerifyFailed` / `CacheError::HashMismatch`, which `decdn-node`'s
/// `is_bao_corruption` folds together with this type.
#[derive(Debug)]
pub struct HashMismatch;

impl std::fmt::Display for HashMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("received bytes do not match requested hash")
    }
}

impl std::error::Error for HashMismatch {}

/// Reject a `total_bytes == 0` claim for a non-empty root as the typed
/// [`HashMismatch`] (#1054).
///
/// A 0-byte blob aligns to an empty range: the bao decoder has no chunk group to
/// anchor and would accept the empty stream for ANY root, and a store sized from a
/// `total_bytes == 0` claim has no gap to pull and is complete as created — so
/// nothing downstream ever verifies the empty stream against the root. The only
/// hash an empty blob can carry is the empty root; a source that claims `0` for any
/// other hash is a paid-but-wrong delivery. This is the one explicit verify site
/// the trivial-empty-range bypass has, shared by [`driver::drive`] (before the
/// empty store is finalized) and the in-memory `fetch_in_memory_once` path so the
/// two cannot drift. A `total_bytes > 0` claim is untouched: the streaming decoder
/// verifies it group by group.
pub(crate) fn reject_empty_claim_for_nonempty_root(
    total_bytes: u64,
    hash: [u8; 32],
) -> anyhow::Result<()> {
    if total_bytes == 0 && hash != *blake3::hash(&[]).as_bytes() {
        return Err(anyhow::Error::new(HashMismatch));
    }
    Ok(())
}

/// Typed sentinel for a pull aborted because the bytes that ACTUALLY arrived
/// crossed the buyer's `max_blob_size_bytes` ceiling (#1895). The peer's signed
/// `total_bytes` claim never drives a refusal — it is peer-controlled and
/// unverified (`StreamResponse::validate()` does not bound it) — so the ceiling
/// binds on cumulative RECEIVED, BLAKE3-verified bytes instead. Returned (not a
/// bare string) so the pull orchestrator can `downcast_ref` and classify it as a
/// buyer-side policy rejection — distinct from a hash mismatch or an unreachable
/// peer — rather than mis-attributing it to the provider's reputation. `Display`
/// carries `BlobTooLarge` so logs and the requester tests can match on it.
///
/// **Units.** `received` and `ceiling` are ALWAYS the same unit within one error —
/// the comparison at each enforcement site is apples-to-apples — but that unit
/// differs by site, so neither field is a raw `max_blob_size_bytes` config value
/// across all call sites. The receive loop ([`UpstreamPull::next_chunk`]) meters bao
/// WIRE bytes (content plus interleaved proof, ADR 038) and compares against the
/// wire size of a ceiling-sized blob. The gap-driven driver (`fill_gap`) meters
/// CONTENT bytes (the store's delivered frontier) and the resume-offset guard in
/// [`open_progressive_pull`] meters a CONTENT offset, both against the configured
/// `max_blob_size_bytes` directly. Every one is a faithful "the byte position
/// crossed the ceiling" report.
#[derive(Debug)]
pub struct BlobTooLarge {
    /// The byte position that crossed `ceiling` — wire bytes taken off the stream
    /// (receive loop), the store's content frontier (gap-driven driver), or a
    /// content resume offset already past it. Same unit as `ceiling` (see the
    /// type's **Units** note).
    pub received: u64,
    /// The ceiling `received` crossed, in the same unit as `received`.
    pub ceiling: u64,
}

/// The requested `byte_offset` is at or past the blob's end, so no resume can be
/// served from it (#1120).
///
/// Typed rather than a bare string because it is the ONE signal that proves a
/// caller's partial download does not belong to this blob — a stale `.partial`
/// left under the same `--output` by a fetch of a different or larger blob, or
/// one that completed but was killed before its rename. A resumable client keys
/// its discard-and-refetch on this and nothing else: classifying by exclusion
/// ("any error that is not a voucher rejection") would sweep in ordinary stalls
/// and resets and destroy a perfectly good prefix the user has already paid for.
///
/// Note this is a statement about the *offset*, not about the peer: the node
/// answered honestly. Callers must not score it against the provider.
#[derive(Debug)]
pub struct ResumeOffsetPastEnd {
    /// Whole-blob size the node signed for.
    pub total_bytes: u64,
    /// The offset we asked to resume from.
    pub byte_offset: u64,
}

impl std::fmt::Display for ResumeOffsetPastEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot resume at byte {}: the blob is only {} bytes",
            self.byte_offset, self.total_bytes
        )
    }
}

impl std::error::Error for ResumeOffsetPastEnd {}

impl std::fmt::Display for BlobTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "received {} bytes, crossing the {}-byte size ceiling (BlobTooLarge)",
            self.received, self.ceiling
        )
    }
}

impl std::error::Error for BlobTooLarge {}

/// Typed sentinel for a server that signed an open-stage `StreamResponse`
/// (`ok == true`) quoting a per-MB `rate_per_mb` above the buyer's effective
/// ceiling (#1375). The buyer aborts **before paying**, so the profitable
/// "quote low on the probe, quote high on the stream, then deliver" bait-and-switch
/// never gets paid.
///
/// The ceiling is the lower of two bounds the caller supplies: the probe rate the
/// candidate was selected on (a stream quote may not exceed what the probe
/// advertised) and an optional absolute buyer config (`max_rate_per_mb`). Because
/// the probe-relative bound is always applied by the node pull path, a *completed*
/// delivery is by construction priced at or below the probe rate — so it can never
/// be rate-manipulation (`SlashJudge` requires `stream.ratePerMb > probe.ratePerMb`),
/// and the challengeable case is exactly this abort.
///
/// Carries the operator's own signed `response` — already verified against
/// `expected_signer` by `verify_response` before construction, so it still
/// recovers to the delivering node and a caller could replay it (paired with the
/// same node's `ProbeResponse`) to `SlashJudge` with no re-signing.
///
/// **Slashability caveat.** The evidence is only rate-*manipulation* evidence when
/// the **probe-relative** bound was the one exceeded: `SlashJudge` requires
/// `stream.ratePerMb > probe.ratePerMb`. The ceiling here is
/// `min(probe_rate, config_absolute)` ([`effective_rate_ceiling`]), so when the
/// buyer's absolute `config` bound is the binding one the quote may sit at or below
/// the probe rate and be perfectly honest. A challenger must therefore still
/// confirm `quote > probeRate` before submitting; this type does not assert it.
///
/// **Consumption.** The signed response is retained *on the error* so a caller can
/// act on it, but the node's pull-failure handler does not itself submit a
/// challenge — auto-slashing is deferred (the same footing as the daemon never
/// auto-challenging an [`UpstreamRefused`], whose evidence is exercised only by the
/// e2e harness today). `evidence()` is the only accessor; the field is private and
/// the sole constructor derives it from the verified response, so the
/// verified-signature invariant cannot be bypassed by a hand-built value.
pub struct RateAboveCeiling {
    quoted_rate_per_mb: u64,
    ceiling_rate_per_mb: u64,
    response: StreamResponse,
}

impl RateAboveCeiling {
    /// Build the abort error for an open-stage quote above `ceiling`. Crate-private
    /// and the SINGLE construction path (`open_progressive_pull` routes through
    /// it), so `quoted_rate_per_mb` is always
    /// *derived* from `response.body.rate_per_mb` and cannot desync from the
    /// retained evidence — the same "derive, don't trust the caller to set it
    /// consistently" discipline as [`UpstreamRefused::open`]. Callers MUST have run
    /// `verify_response` on `response` first (the field docs' signature invariant).
    fn over_ceiling(response: StreamResponse, ceiling_rate_per_mb: u64) -> anyhow::Error {
        anyhow::Error::new(Self {
            quoted_rate_per_mb: response.body.rate_per_mb,
            ceiling_rate_per_mb,
            response,
        })
    }

    /// The per-MB rate the server quoted in the signed `StreamResponse`.
    #[must_use]
    pub const fn quoted_rate_per_mb(&self) -> u64 {
        self.quoted_rate_per_mb
    }

    /// The effective ceiling the quote exceeded (min of probe rate and config).
    #[must_use]
    pub const fn ceiling_rate_per_mb(&self) -> u64 {
        self.ceiling_rate_per_mb
    }

    /// The operator's own signed `StreamResponse`, verified against
    /// `expected_signer` before this value was built. See the type docs for the
    /// slashability caveat (only `quote > probeRate` is manipulation).
    #[must_use]
    pub const fn evidence(&self) -> &StreamResponse {
        &self.response
    }
}

impl std::fmt::Debug for RateAboveCeiling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Elide the signed body + 65-byte `slash_sig` to a length, as `UpstreamRefused`
        // does, so a logged abort stays readable.
        f.debug_struct("RateAboveCeiling")
            .field("quoted_rate_per_mb", &self.quoted_rate_per_mb)
            .field("ceiling_rate_per_mb", &self.ceiling_rate_per_mb)
            .field("evidence_slash_sig_len", &self.response.slash_sig.len())
            .finish()
    }
}

impl std::fmt::Display for RateAboveCeiling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "server quoted {} per MB, exceeding the buyer ceiling {} per MB (RateAboveCeiling)",
            self.quoted_rate_per_mb, self.ceiling_rate_per_mb
        )
    }
}

impl std::error::Error for RateAboveCeiling {}

/// Combine the two buyer rate ceilings into one effective bound, treating `0` as
/// "unbounded" on each input (the probe-relative bound and the absolute config
/// bound are both optional). Returns `0` only when BOTH are unbounded.
#[must_use]
pub const fn effective_rate_ceiling(probe_relative: u64, config_absolute: u64) -> u64 {
    match (probe_relative, config_absolute) {
        (0, c) => c,
        (p, 0) => p,
        (p, c) if p < c => p,
        (_, c) => c,
    }
}

/// Typed sentinel for the buyer's own per-candidate pull deadline firing (#857).
/// Returned (not a bare string) so the pull orchestrator can `downcast_ref` and
/// recognize that the timeout is OUR local deadline — a possibly mis-sized
/// configuration value — not evidence the provider is unreachable, and so must
/// not tar the provider's local reputation score. `Display` keeps the
/// stable `timed out` text for logs (and for the `!contains("timed out")`
/// negative assertion in `node_to_node_pull_through`'s deadline test).
///
/// Since #1134 it is raised by OUR OWN wall clocks only, and the message is
/// deliberately stage-NEUTRAL because there are THREE of them: the shared
/// `open_stream` open bound (`PullDeadlines::open`), the optional overall
/// `hard_cap` the `test-util` wrappers apply, and the stall clock elapsing before
/// the FIRST byte (`cumulative == 0`) in the streaming loop — where it is
/// measuring the server's time-to-first-byte, not mid-stream inactivity. `decdn-node`
/// also wraps `open_progressive_pull` in its per-candidate budget as belt-and-braces,
/// which raises the same sentinel.
///
/// The one thing it is NOT is a mid-stream STALL once bytes have flowed — that is
/// [`PullStalled`], a fact about the PEER that scores its reputation, whereas this is a
/// fact about our own possibly mis-sized budget (or an honest slow server's TTFB) and
/// must not. Keeping the two apart is the whole point of the split, and the `cumulative
/// == 0` carve-out is where the two loops draw the line (see [`PullStalled`]).
///
/// Callers that want to name the stage add a `.context(…)` layer; `downcast_ref`
/// still recovers the sentinel through it — pinned by
/// `buyer_side_sentinels_survive_anyhow_downcast` in `decdn-node`'s `node_origin.rs`,
/// not in this crate.
#[derive(Debug)]
pub struct PullTimeout {
    /// How long the pull ran before the budget elapsed.
    pub after: Duration,
}

impl std::fmt::Display for PullTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upstream pull timed out after {:?}", self.after)
    }
}

impl std::error::Error for PullTimeout {}

/// Typed sentinel for the upstream rejecting a voucher we presented mid-stream
/// (#857) — e.g. a stale nonce (#852), deposit exhaustion, or a wrong-channel
/// mismatch. This is OUR payment-side fault, not the provider's, so the pull
/// orchestrator `downcast_ref`s it to skip the candidate WITHOUT recording a
/// reputation observation (mirroring the buyer channel-open-failure arm). Named
/// with the `Upstream` prefix to disambiguate from the protocol-level
/// `StreamError::VoucherRejected` reason enum, whose `reason` it carries verbatim
/// (the `Copy` `VoucherRejectReason`, not a lossy stringification) so a future
/// caller can branch on retry-vs-top-up-vs-abandon without re-parsing a message.
/// `Display` keeps the stable `voucher rejected` text for logs.
///
/// `bundle` mirrors the wire [`WatermarkBundle`] verbatim (issue #1481): `Some`
/// only for the gated regression/exhaustion reasons, and only when the node
/// verified the rejected voucher recovered to the pool capability.s pinned
/// `signer` before attaching it. A caller that sees `Some` alongside
/// `AmountRegression`/`BytesRegression`/`SpendingCapExhausted` can self-heal —
/// re-seed its ledger to the bundle's watermark
/// ([`crate::ledger::Cumulative::from`]) and resume from `bytes_delivered` —
/// rather than treating the rejection as terminal. `SpendingCapExhausted`
/// with `bundle: None` means there is no signer-verified watermark to resume
/// from (or, more commonly, that a wallet-less delegate simply has no local
/// means to add deposit) — the caller must surface that to the app rather
/// than loop. `CapabilityExpired` and `PoolExhausted` are never
/// watermark-gated (no bundle is ever attached) and are always terminal —
/// the fix is a fresh capability or an owner top-up, not a resync.
#[derive(Debug)]
pub struct UpstreamVoucherRejected {
    /// Why the seller refused the voucher.
    pub reason: VoucherRejectReason,
    /// The seller's signer-verified watermark, when it sent one, so the buyer
    /// can resync its cumulative state and retry. `None` means there is
    /// nothing to resync from and the caller must surface the failure.
    pub bundle: Option<WatermarkBundle>,
    /// The ledger generation the rejected voucher was signed under
    /// (`StreamProof::Voucher`), when the stream knows it. `None` for a
    /// rejected reveal, or a rejection read before this stream claimed
    /// anything; [`PoolLedger::rebase`] treats it as current.
    pub proof_generation: Option<u64>,
}

impl std::fmt::Display for UpstreamVoucherRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The `{:?}` rendering of the reason is load-bearing: the loopback tests
        // assert `.contains("CapExceeded")` / `.contains("Expired")`-style substrings
        // on this string. A custom `Display` for `VoucherRejectReason` would have to
        // reproduce the variant names verbatim, so keep the Debug rendering here.
        write!(f, "voucher rejected: {:?}", self.reason)
    }
}

impl std::error::Error for UpstreamVoucherRejected {}

/// Typed sentinel for an upstream *refusing* delivery up front — a
/// `StreamResponse` with `body.ok == false` (#1144). Carries the wire
/// [`StreamError`] verbatim (mirroring [`UpstreamVoucherRejected`]'s typed
/// `reason`, not a lossy stringification) so the pull orchestrator can
/// `downcast_ref` and give each refusal the verdict it deserves, rather than
/// folding every one into "unreachable".
///
/// That distinction is the whole point. A refusal is proof the peer is *reachable
/// and answering*, so most codes are no evidence at all that it is degraded:
///
/// - `NotFound` — the honest, empty-provider answer, and it is NODE-scoped, not
///   blob-scoped (`ServeRejectReason::wire_error` deliberately collapses seven
///   reasons onto it so channel existence cannot be probed). Scoring it as
///   `Unreachable` punished a node for truthfully saying it lacks a blob.
/// - `EvictedSinceProbe` / `BlobTooLarge` — likewise honest, and DURABLE for this
///   (peer, hash): asking again soon gets the same answer, so the consumer suppresses
///   the pair for the full negative-cache TTL rather than burning a candidate slot on it.
/// - `Overloaded` — honest but transient. Backpressure is to be respected, not punished,
///   so it earns only the short `REFUSAL_SUPPRESSION_TTL`.
/// - `VoucherRejected` — OUR payment fault, not the peer's. Listed because it genuinely
///   arrives here: it is the code designated for MID-stream, and the mid-stream receive
///   arms wrap any `StreamError` into this sentinel (#1145 review).
/// - `InternalError` — the one code that IS evidence of a degraded peer; it means
///   "unexpected failure, do not retry THIS node" (#1129).
///
/// `node_origin::classify_refusal` is the consumer and matches this exhaustively, so a new
/// wire code breaks that build rather than silently inheriting a verdict.
///
/// Only the wire code is recoverable here, never the finer server-side
/// `ServeRejectReason` — that collapse is intentional and must not be reversed.
/// `Display` keeps the stable `delivery refused: {code}` text that logs, the CLI's
/// cache-miss annotation, and the loopback tests match on.
/// The two refusal shapes are a private inner enum, not `pub` variants: enum
/// variant fields cannot be made private, so exposing them would let any caller
/// build an `Open { .. }` directly and bypass the crate-private `open`
/// constructor's error-from-response derivation — reintroducing exactly the
/// doc-comment-only invariant this type exists to remove (#1377). The newtype
/// keeps the only paths to a value the two constructors, so the "present response
/// ⇒ open-stage, verified" invariant is a property of the type rather than a
/// convention.
pub struct UpstreamRefused {
    kind: Kind,
}

enum Kind {
    /// Open-stage refusal: the upstream signed a [`StreamResponse`] with
    /// `body.ok == false`, already verified against `expected_signer` by
    /// [`verify_response`] before the value is built, so `error` is derived
    /// *from* the response's trailing [`StreamResponseExt`] and `response`
    /// always recovers to `expected_signer`.
    Open {
        error: StreamError,
        // Boxed (issue #1481 review): `StreamError::VoucherRejected` grew an
        // optional `WatermarkBundle`, which pushed the unboxed variant past
        // clippy's `large_enum_variant` threshold relative to `MidStream`.
        response: Box<StreamResponse>,
    },
    /// Mid-stream refusal: a bare [`ClientMessage::StreamError`] frame that
    /// arrived after the open stage. It carries no signature (#1042), so there
    /// is never a signed `StreamResponse` to retain — `None` by construction.
    MidStream { error: StreamError },
}

impl UpstreamRefused {
    /// Build the open-stage refusal for a `body.ok == false` response, deriving
    /// the wire `error` *from* the response's trailing [`StreamResponseExt`] so
    /// the two can never disagree (#1377 invariant 2). The one construction site
    /// [`open_progressive_pull`]'s open stage uses, so every caller classifies a
    /// refusal the same way.
    ///
    /// Returns `anyhow::Error` rather than `Self` because the `None`-error arm is
    /// a protocol violation, not a refusal: callers MUST have run
    /// [`verify_response`] first, whose `StreamResponseExt::validate` rejects
    /// `ok == false` with no error code (`MissingStreamError`). Reaching the
    /// `None` arm means that invariant was bypassed, so it is surfaced as the
    /// violation it is rather than defaulted to an invented code — which would
    /// launder a malformed refusal into a plausible-looking one.
    ///
    /// Takes the whole `response` rather than just its `error` so the operator's
    /// own signed refusal survives on the value ([`Self::evidence`], #1042) —
    /// `body` is exactly the field set the `SlashJudge` EIP-712 `StreamResponse`
    /// typehash covers and `slash_sig` is the operator's secp256k1 signature over
    /// it.
    ///
    /// A refusal (`ok == false`) is **not** on-chain slash evidence: rate
    /// manipulation requires `ok == true` and blacklist violation requires a
    /// served claim (ADR 014), so a node may sign refusals freely. The retention
    /// is kept on its own merits — it is an attributable, non-repudiable record
    /// of *why* a paid pull was declined, which the caller can log, surface, or
    /// present in a dispute without re-signing anything.
    fn open(response: StreamResponse, ext: &StreamResponseExt) -> anyhow::Error {
        // `ok == true` is not a refusal at all — building an `Open` from it would
        // mint an evidence-carrying refusal with `ok == true`, violating invariant
        // 3. `validate` already rejects `(ok: true, error: Some)` as
        // `StreamErrorWithOk`, so reaching here with `ok == true` means that
        // invariant was bypassed; surface it as the protocol violation it is rather
        // than fold it into a typed refusal. Makes invariant 3 a construction
        // property, not just a call-site precondition (#1377 review).
        if response.body.ok {
            return anyhow::anyhow!(
                "open-stage refusal constructed from an ok == true response \
                 (StreamResponse::validate invariant bypassed)"
            );
        }
        match ext.error.clone() {
            Some(error) => anyhow::Error::new(Self {
                kind: Kind::Open {
                    error,
                    response: Box::new(response),
                },
            }),
            None => anyhow::anyhow!(
                "delivery refused but the validated response carried no error code \
                 (StreamResponseExt::validate invariant bypassed)"
            ),
        }
    }

    /// Build the refusal for a **mid-stream** [`ClientMessage::StreamError`]
    /// frame — the refusal that arrives *after* the open stage, in reply to a
    /// delivery chunk or a voucher.
    ///
    /// Such a frame carries no signature (#1042), so [`Self::evidence`] is `None`
    /// here **by construction** and this is the only place that decision is made.
    /// The four mid-stream receive sites route through it so none can drift into
    /// synthesising a `StreamResponse` — which would hand an observer an unsigned
    /// artifact the evidence contract promises always recovers to the delivering
    /// node (#1378). The open-stage counterpart is the crate-private `open`.
    ///
    /// Public because callers outside the crate (and their tests) legitimately
    /// build mid-stream refusals; it is safe to expose precisely because it
    /// *cannot* carry a signed response — the encapsulation that matters guards
    /// the attributable `open` ([`Self::evidence`]), which stays crate-private
    /// so a caller cannot mint a refusal attributed to an operator.
    #[must_use]
    pub const fn mid_stream(error: StreamError) -> Self {
        Self {
            kind: Kind::MidStream { error },
        }
    }

    /// The wire code the upstream signed. Always a `StreamError` as it appeared
    /// on the wire — never a server-side `ServeRejectReason`, whose seven-way
    /// collapse onto `NotFound` is deliberate and one-way
    /// (`handlers::client::wire_error`). Total over both shapes.
    #[must_use]
    pub const fn error(&self) -> &StreamError {
        match &self.kind {
            Kind::Open { error, .. } | Kind::MidStream { error } => error,
        }
    }

    /// The upstream's own signed [`StreamResponse`], present iff the refusal
    /// arrived at the **open stage**. `None` for a mid-stream frame.
    ///
    /// An attributable record, not loose diagnostics — already verified against
    /// `expected_signer` by `verify_response` before this value is built, so a
    /// present value always recovers to `expected_signer`: the operator address
    /// the caller bound this pull to, which is what `SlashJudge._checkRegistered`
    /// resolves `nodeId` against.
    ///
    /// Note it is not by itself *slash* evidence — no offense admits a refusal
    /// (`ok == false`) as its stream leg (ADR 014). What it gives the caller is a
    /// non-repudiable statement of why the pull was declined, signed by the
    /// operator and usable verbatim, with no re-signing.
    #[must_use]
    pub fn evidence(&self) -> Option<&StreamResponse> {
        match &self.kind {
            Kind::Open { response, .. } => Some(response.as_ref()),
            Kind::MidStream { .. } => None,
        }
    }
}

impl std::fmt::Debug for UpstreamRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Elide the 65-byte `slash_sig` (and the full signed body) to a length so
        // `tracing::error!(?err)` stays readable — the derived `Debug` would dump
        // the entire attestation on every logged refusal.
        let mut dbg = f.debug_struct("UpstreamRefused");
        dbg.field("error", self.error());
        match self.evidence() {
            Some(resp) => dbg.field("evidence_slash_sig_len", &resp.slash_sig.len()),
            None => dbg.field("evidence", &Option::<()>::None),
        };
        dbg.finish()
    }
}

impl std::fmt::Display for UpstreamRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The `{:?}` rendering is load-bearing, exactly as in
        // `UpstreamVoucherRejected`: the loopback tests assert
        // `.contains("NotFound")` on this string, and `StreamError` has no
        // `Display`. What renders here is always a WIRE code — never a node-side
        // `ServeRejectReason` such as `UnknownChannel`, which collapses to
        // `NotFound` before it leaves the server (see `wire_error`). Matching on a
        // reject-reason name would therefore never fire.
        write!(f, "delivery refused: {:?}", self.error())
    }
}

impl std::error::Error for UpstreamRefused {}

/// Typed sentinel for an upstream that went SILENT mid-stream — bytes were flowing, and
/// then no byte of progress for the stall budget (#1134). Distinct from [`PullTimeout`],
/// which is a bound of OURS elapsing, and the distinction is load-bearing:
///
/// A whole-transfer deadline cannot tell "the provider is dead" from "this blob is big" or
/// "this link is slow", so `PullTimeout` must NOT tar the provider — it fires on perfectly
/// healthy transfers. A stall deadline resets on every byte received, so it fires ONLY when
/// a provider stops delivering while we wait. That IS evidence the peer is unreachable, and
/// `classify_pull_failure` scores it as such in the local per-peer EWMA (ADR 008).
///
/// # What the bound rests on
///
/// Two things, and it is worth being precise about both, because scoring a peer on a bound
/// that does not hold is how an honest node's local reputation gets unfairly defamed.
///
/// **The non-empty-`ChunkData` invariant (#1088).** With empty frames banned, "a frame
/// arrived" and "bytes made progress" are the same statement, so a peer cannot hold the
/// floor open with padding. The receive loop ([`UpstreamPull::next_chunk`]) has no
/// independent progress check behind that floor: it is safe because no frame a peer can
/// send makes zero progress, not because it verifies that it did. The floor is
/// structural rather than advisory — `ChunkData`'s field is private, and its constructor
/// and decode gate both reject an empty payload — so it cannot be relaxed by forgetting
/// to call a validator (#1145 review).
///
/// **At least one byte having already arrived.** The reset is what makes a stall the peer's
/// fault, so before the FIRST byte there has been no reset and the argument does not apply:
/// the clock is measuring the server's time-to-first-byte, which scales with blob size
/// (the serve path materialises the whole bao wire via `export_bao_range` before it can emit
/// chunk #1). Both loops therefore raise [`PullTimeout`] — exonerating — when the budget
/// elapses at `cumulative == 0`, and this sentinel only once bytes have flowed (#1145
/// review). Without that split, a 1 GiB blob off a cold disk would score an honest server as
/// unreachable for the crime of being big.
#[derive(Debug)]
pub struct PullStalled {
    /// The inactivity budget that elapsed after bytes had been flowing.
    pub after: Duration,
}

impl std::fmt::Display for PullStalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upstream stalled: no progress for {:?}", self.after)
    }
}

impl std::error::Error for PullStalled {}

/// Marker for a pull failure that is OURS, not the upstream's — a broken local
/// signer, an encode fault, a bad range computation (#1145 review).
///
/// Everything else that reaches `classify_pull_failure`'s catch-all is attributable
/// to the peer: a failed dial, a dropped connection, a bad signature, an unexpected
/// frame. Scoring the peer `Unreachable` there is correct, and is in fact the
/// PRIMARY way a dead node is detected — so the catch-all must stay as it is.
///
/// The exception was this class. A node whose own signer is broken cannot pay
/// anybody, and would walk the candidate list tarring every honest
/// provider it met with an `Unreachable` — a local EWMA hit; ADR 008 scoring is
/// local-only — on the strength of its own fault. Attach this marker at a
/// local-fault site and the classifier exonerates the peer and warns about us instead.
///
/// # It also decides what a CLIENT is told (#1560)
///
/// Reputation is not the only consequence. A second consumer — the node crate's
/// `record_pool_open_failure` — reads this marker to choose between answering a
/// downstream client `StreamError::NotFound` ("we could not obtain this blob") and
/// `InternalError` ("unexpected failure; do not retry this node"). That path involves no
/// peer and no reputation at all.
///
/// So attaching this marker at a NEW site changes serve-path refusals, not just scoring.
/// Attach it when the failure means *this node* cannot serve anyone — a broken signer, an
/// unreadable or unwritable store, a poisoned lock, an unfunded wallet. Do NOT attach it to
/// a condition that is specific to one peer, one channel, or one blob, however much it is
/// "our side" of the exchange: a wedged channel to a single provider is ours and is still a
/// clean miss, because the node can serve every other request perfectly well.
///
/// It is a marker, so it composes: `.context(LocalPullFault)` on any error.
#[derive(Debug)]
pub struct LocalPullFault;

impl std::fmt::Display for LocalPullFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("local buyer-side fault (not attributable to the upstream)")
    }
}

impl std::error::Error for LocalPullFault {}

/// The bounds on a pull, each matched to the stage it governs (#1134, #1797).
///
/// A single whole-transfer deadline is mostly useless as a health signal: it has to be
/// sized against `blob size × link speed`, so it kills legitimate large or slow-but-healthy
/// transfers while a value small enough to catch a dead peer quickly cannot serve a big blob
/// at all. The operator ends up tuning a number that has nothing to do with node health.
///
/// Split by stage instead:
///
/// - **`window` + `floor_bps`** bound the STREAMING stage by THROUGHPUT. The requester
///   counts bytes read off the QUIC stream (sub-frame, so the measure is frame-size
///   independent) and trips when the bytes across the trailing `window` fall below
///   `floor_bps · window`. This catches both a wedge (throughput drops to zero) and a
///   slow drip (throughput below the floor), and is indifferent to transfer size and link
///   speed. `floor_bps == 0` leaves pure idle detection: at least one byte per window.
///   This is the primary mechanism. The abort is requester-local policy and does not score
///   the peer.
/// - **`hard_cap`** is an optional overall wall-clock escape hatch, `None` by
///   default. It exists for a caller that must bound total runtime regardless; it
///   is not how a stalled peer is detected.
///
/// - **`open`** bounds the OPEN stage (dial → request → verified `StreamResponse`)
///   by WALL CLOCK. That stage is bounded work whose duration does not depend on
///   the blob, so a slow one really is a stall and a wall clock is the right tool.
///
/// Every stage must carry a bound of its own. It is not enough for a caller to
/// wrap the whole pull in a timeout and call the open "bounded": the handshake
/// happens *inside* [`open_progressive_pull`], and the production callers run
/// with no overall cap, so leaving the bound to the caller would leave the
/// `StreamResponse` read with no bound at all — a peer that accepts a connection
/// and then says nothing would hang the pull forever. `open` exists so that
/// cannot be expressed.
/// # The relational invariant
///
/// `hard_cap`, when set, must STRICTLY EXCEED `open + window`. Both clocks below run inside
/// the cap's, and in the worst case consecutively — the open can legitimately consume its
/// whole budget before the streaming stage begins, and the floor needs one full `window` to
/// fire — so a cap that does not outlast both means the cap always fires first and the
/// throughput floor can never trip under it. The pull then looks fully configured while its
/// peer-health signal is dead.
///
/// [`Self::capped`] is fallible and the fields are private BECAUSE of that (#1145 review).
/// The invariant belongs on this type: with `pub` fields, callers build it with a struct
/// literal and the only check is a hardcoded `timeout > 2 × window` in the CLI's
/// `ClientFetchArgs::validate`, correct only because those call sites set `open` from the same
/// knob as `window`. Adding an
/// `--open-timeout-ms` flag would have made it silently wrong, in the direction that reopens
/// the hole. The invariant belongs to the type that has the values.
#[derive(Debug, Clone, Copy)]
pub struct PullDeadlines {
    /// Wall-clock bound on the open stage: dial, request, and the signed
    /// `StreamResponse`. Bounded work — a slow one is a stall.
    open: Duration,
    /// Trailing window over which the streaming-stage throughput floor is measured.
    window: Duration,
    /// Minimum bytes-per-second the streaming stage must sustain over `window`. `0`
    /// disables the throughput test and leaves pure idle detection (one byte per window).
    floor_bps: u64,
    /// Optional overall wall-clock cap on the whole exchange. `None` = uncapped;
    /// `open` and `window` between them are what keep an uncapped pull from hanging.
    hard_cap: Option<Duration>,
}

/// A [`PullDeadlines`] whose bounds cannot do their job. Carries the values so a CLI
/// can render the arithmetic back to the user rather than just saying "invalid".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineError {
    /// A zero budget elapses on its first poll, so the stage it bounds can never run.
    ZeroBudget,
    /// The cap does not outlast `open + window`, so it always fires first and the
    /// throughput floor — the signal that a healthy stream is still making progress —
    /// can never fire.
    CapCannotOutlastItsStages {
        /// The open-stage budget.
        open: Duration,
        /// The throughput-floor window that follows it.
        window: Duration,
        /// The whole-pull cap, which must exceed `open + window`.
        hard_cap: Duration,
    },
}

impl std::fmt::Display for DeadlineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroBudget => write!(f, "a deadline of zero elapses before any work can run"),
            Self::CapCannotOutlastItsStages {
                open,
                window,
                hard_cap,
            } => write!(
                f,
                "the overall cap ({hard_cap:?}) must exceed the open bound ({open:?}) plus the \
                 throughput-floor window ({window:?}): both run inside it and in the worst case \
                 consecutively, so below that the cap always elapses first and a stalled \
                 provider can never be detected"
            ),
        }
    }
}

impl std::error::Error for DeadlineError {}

impl PullDeadlines {
    /// The recommended shape: a wall clock on the open, inactivity on the stream,
    /// and no overall cap — so a pull of any size completes as long as the upstream
    /// keeps feeding it bytes.
    ///
    /// # Errors
    ///
    /// [`DeadlineError::ZeroBudget`] for a zero `open` or `window`. `floor_bps` may be zero
    /// (idle-detection mode: one byte per window).
    ///
    /// A zero `open` or `window` must fail here, not pass. One might argue it is a bound that
    /// fires too EAGERLY — loud, immediately obvious — rather than one that silently never
    /// fires. That is exactly backwards: a zero window makes the throughput floor demand
    /// progress over no time at all, so the streaming stage can never satisfy it.
    ///
    /// The only thing standing between config and that state was a `> 0` check in a resolver
    /// in another crate — the same advisory-invariant shape `ChunkData` had before #1088, and
    /// the reason this type owns its bounds at all.
    pub const fn new(
        open: Duration,
        window: Duration,
        floor_bps: u64,
    ) -> Result<Self, DeadlineError> {
        if open.is_zero() || window.is_zero() {
            return Err(DeadlineError::ZeroBudget);
        }
        Ok(Self {
            open,
            window,
            floor_bps,
            hard_cap: None,
        })
    }

    /// As [`Self::new`], plus an overall wall-clock cap on the whole exchange.
    ///
    /// # Errors
    ///
    /// [`DeadlineError::CapCannotOutlastItsStages`] if `hard_cap` does not strictly exceed
    /// `open + window`, and [`DeadlineError::ZeroBudget`] for a zero `open` or `window`. See
    /// the type's own docs for why this is the one constructor that must be fallible.
    pub fn capped(
        open: Duration,
        window: Duration,
        floor_bps: u64,
        hard_cap: Duration,
    ) -> Result<Self, DeadlineError> {
        if open.is_zero() || window.is_zero() {
            return Err(DeadlineError::ZeroBudget);
        }
        if hard_cap <= open.saturating_add(window) {
            return Err(DeadlineError::CapCannotOutlastItsStages {
                open,
                window,
                hard_cap,
            });
        }
        Ok(Self {
            open,
            window,
            floor_bps,
            hard_cap: Some(hard_cap),
        })
    }

    /// The open-stage wall clock.
    #[must_use]
    pub const fn open(&self) -> Duration {
        self.open
    }

    /// The streaming-stage throughput-floor window.
    #[must_use]
    pub const fn window(&self) -> Duration {
        self.window
    }

    /// The streaming-stage throughput floor, in bytes per second. `0` = idle detection only.
    #[must_use]
    pub const fn floor_bps(&self) -> u64 {
        self.floor_bps
    }

    /// The overall cap, if any.
    #[must_use]
    pub const fn hard_cap(&self) -> Option<Duration> {
        self.hard_cap
    }

    /// The single-deadline shape: one budget serving as the open bound, the
    /// streaming window, AND the overall cap, with the throughput floor disabled
    /// (`floor_bps == 0`, idle detection only). **Test-only — do not reach for this in
    /// production.** That conflation is exactly what #1134 set out to remove, and the
    /// name reads far more like a legitimate policy choice than it is.
    ///
    /// Note what it quietly costs, beyond re-introducing the size-coupled deadline:
    /// because `hard_cap == window`, and the cap's clock starts at the top of the whole
    /// exchange while the streaming window starts only once the open has completed, **the
    /// hard cap always elapses first — so [`PullStalled`] can never fire under it.** A pull
    /// built this way silently cannot detect a stalled peer, and so cannot score one.
    /// Every loopback test using this helper is exercising a pull with the stall
    /// signal disabled; the stall path is covered by
    /// `node_origin_mid_stream_silence_does_not_score_stalled_upstream`, which builds its
    /// deadlines explicitly.
    ///
    /// That is a statement about the `test-util` `stream_fetch*` wrappers, where
    /// `with_hard_cap` wraps the whole exchange. [`open_progressive_pull`] and
    /// [`driver::drive`] never consult `hard_cap` at all, so a `whole_transfer` used there
    /// would leave `PullStalled` perfectly able to fire — which is not a reprieve, just a
    /// different reason not to reach for this (#1145 review).
    ///
    /// It is also the reason this constructor stays infallible while [`Self::capped`] is not:
    /// it deliberately builds the very state `capped` refuses.
    ///
    /// TEST-ONLY, and now unrepresentable in production by construction: gated behind the
    /// `test-util` feature (#1145 review), so a production caller cannot name it and reach for
    /// the disabled-floor / uncapped state — it wants [`Self::new`] (floor-bounded) or
    /// [`Self::capped`] (floor-bounded with a leak guard). Used by the loopback helper
    /// `stream_fetch` and directly by the `client_loopback` suite, whose blobs are small
    /// enough that none of this matters.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub const fn whole_transfer(timeout: Duration) -> Self {
        Self {
            open: timeout,
            window: timeout,
            floor_bps: 0,
            hard_cap: Some(timeout),
        }
    }
}

/// Fetch `hash` from `target` over `cdn/client/v1` into memory, paying as bytes
/// arrive and verifying each bao chunk group as it lands.
///
/// `expected_signer` is the delivering node's Ethereum address, used to verify
/// the response `slash_sig`. `byte_offset` resumes a partial fetch. Use
/// [`stream_fetch_tracked`] instead if you need to persist the voucher watermark
/// the channel reached (#852); this convenience wrapper discards it.
///
/// TEST-ONLY, like the whole `stream_fetch*` family: gated behind the `test-util`
/// feature alongside [`PullDeadlines::whole_transfer`], the single-deadline shape
/// it passes (#1145 review). The wrappers are thin in-memory drivers of the one
/// receive loop, [`UpstreamPull`], for suites that want the decoded bytes back;
/// production callers stream into a store through [`driver::drive`] /
/// [`PeerSource`].
///
/// # Errors
///
/// Fails on connect/transport errors, an invalid or zero-rate response, a
/// `slash_sig` that does not recover to `expected_signer`, a mismatched echoed
/// field, a mid-stream `VoucherRejected`, a hash mismatch, or a timeout.
#[cfg(any(test, feature = "test-util"))]
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    timeout: Duration,
) -> anyhow::Result<Bytes> {
    stream_fetch_tracked(
        endpoint,
        target,
        ctx,
        slash_domain,
        expected_signer,
        hash,
        // `stream_fetch` models a node-to-node pull; a suite that routes on a
        // namespace calls `stream_fetch_tracked` directly.
        decdn_protocol::client::NO_NAMESPACE,
        byte_offset,
        timestamp_us,
        // The single-deadline shape (#1134): this helper's callers are
        // loopback tests moving tiny blobs, for which one budget serving as both
        // the overall cap and the stall bound is harmless. Production paths take
        // `PullDeadlines` directly and split the two.
        PullDeadlines::whole_transfer(timeout),
        // No buyer-side blob-size or rate ceiling on this loopback helper; a suite
        // that exercises either passes it through `stream_fetch_tracked`.
        0,
        0,
        &mut VoucherProgress::default(),
    )
    .await
}

/// Like [`stream_fetch`], but drives the fetch over a caller-owned
/// [`WarmConnection`] instead of dialling a fresh connection (#1848 T1). Two
/// calls with the same `warm` reuse one dialled connection — one bi-stream per
/// hash — which is exactly what the connection-reuse test asserts.
///
/// TEST-ONLY, like the rest of the `stream_fetch*` family. Runs a single attempt
/// against a one-shot [`PoolLedger`] seeded from `ctx.prior_*` (the loopback
/// blobs it serves never trigger the wallet-less resume loop).
///
/// # Errors
///
/// The same set as [`stream_fetch`].
#[cfg(any(test, feature = "test-util"))]
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_on(
    warm: &WarmConnection,
    ctx: &PoolContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    timeout: Duration,
) -> anyhow::Result<Bytes> {
    let ledger = Arc::new(ctx.new_ledger());
    fetch_in_memory_once_on(
        warm,
        ctx,
        ledger,
        slash_domain,
        expected_signer,
        hash,
        // Models a node-to-node pull, matching `stream_fetch`.
        decdn_protocol::client::NO_NAMESPACE,
        byte_offset,
        timestamp_us,
        0,
        0,
        PullDeadlines::whole_transfer(timeout),
        None,
    )
    .await
}

/// Like `stream_fetch`, but reports the channel's acked voucher watermark via
/// the `progress` out-param so the caller can persist what it paid (#852).
///
/// `progress` is an out-param: on return it holds the cumulative
/// `(bytes_delivered, amount)` of the last *acked* voucher. Internally the pull
/// runs against a one-shot [`PoolLedger`] seeded from `ctx.prior_*`; the
/// ledger's snapshot is copied back into `progress` on every return path — `Ok`,
/// `Err`, or timeout — so the caller can record progress even for a mid-stream
/// failure or a paid-but-corrupt delivery. See [`VoucherProgress`].
///
/// # Errors
///
/// Same as `stream_fetch`.
#[cfg(any(test, feature = "test-util"))]
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_tracked(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    deadlines: PullDeadlines,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    progress: &mut VoucherProgress,
) -> anyhow::Result<Bytes> {
    stream_fetch_tracked_with_progress(
        endpoint,
        target,
        ctx,
        slash_domain,
        expected_signer,
        hash,
        namespace_id,
        byte_offset,
        timestamp_us,
        deadlines,
        max_blob_size_bytes,
        max_rate_per_mb,
        progress,
        None,
    )
    .await
}

/// Delivery-progress callback: invoked with `(received, expected)` after each
/// verified leaf lands, so a caller (e.g. `decdn fetch`) can render a progress
/// bar. Both slots are **content** bytes: `received` is the content position the
/// verifying decoder has reached and `expected` is the blob's content
/// `total_bytes`, constant across the pull — so a bar keyed on the two fills to
/// exactly 100%. `drive` / `multi_source_fetch` (the resumable [`crate::driver`]
/// path the CLI `fetch` and `bundle pull` use) additionally emit the
/// already-present resume base (`base_present`) once before streaming begins; the
/// `test-util` `stream_fetch_tracked_with_progress` wrapper reports the same
/// unit from its in-memory decoder.
///
/// It must not panic (it runs inside the hot receive loop) and must be `Send +
/// Sync` so the pull future stays spawnable.
pub type ProgressCallback = dyn Fn(u64, u64) + Send + Sync;

/// Like [`stream_fetch_tracked`], but also reports per-chunk delivery progress
/// through `on_progress` (see [`ProgressCallback`]) — the byte-progress hook the
/// watermark-only [`VoucherProgress`] out-param does not provide. `None` behaves
/// exactly like [`stream_fetch_tracked`].
///
/// # Errors
///
/// Same as `stream_fetch`.
#[cfg(any(test, feature = "test-util"))]
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_tracked_with_progress(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    deadlines: PullDeadlines,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    progress: &mut VoucherProgress,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<Bytes> {
    // One-shot ledger seeded from the channel's prior cumulative state. A single
    // (non-shared) pull owns its ledger; concurrent shared-channel pulls use
    // `stream_fetch_shared` with a caller-owned ledger instead.
    let ledger = Arc::new(ctx.new_ledger());
    let result = with_hard_cap(
        deadlines.hard_cap,
        fetch_inner(
            endpoint,
            target,
            ctx,
            slash_domain,
            expected_signer,
            hash,
            namespace_id,
            byte_offset,
            timestamp_us,
            max_blob_size_bytes,
            max_rate_per_mb,
            deadlines,
            &ledger,
            on_progress,
        ),
    )
    .await;
    // Copy the watermark back into `progress` on EVERY return path (Ok, Err,
    // timeout) BEFORE returning, so the latest totals survive a mid-stream failure
    // or a paid-but-corrupt delivery (#852).
    //
    // Read it from the LEDGER rather than a copied-back cumulative, so the figure
    // is `settlement` and not `committed` — matching the two sibling persist paths
    // (`UpstreamPull::progress`, the `node_origin` drop guard). A voucher left
    // armed by an ambiguous send is owed, and the error arm here is exactly where
    // that happens. On the `Ok` arm nothing is armed, so the two agree.
    *progress = VoucherProgress::from_ledger(&ledger, ctx.prior_amount);
    result
}

/// Apply the optional overall wall-clock cap of a [`PullDeadlines`] to `fut`.
///
/// `None` runs the pull uncapped — which is safe precisely because the `stall`
/// bound inside bounds every streaming read. A pull with no cap cannot hang; it
/// can only take as long as the upstream keeps feeding it bytes, which is the
/// point (#1134).
#[cfg(any(test, feature = "test-util"))]
async fn with_hard_cap<F>(hard_cap: Option<Duration>, fut: F) -> anyhow::Result<Bytes>
where
    F: std::future::Future<Output = anyhow::Result<Bytes>>,
{
    match hard_cap {
        Some(cap) => tokio::time::timeout(cap, fut)
            .await
            .map_err(|_| anyhow::Error::new(PullTimeout { after: cap }))?,
        None => fut.await,
    }
}

/// Like `stream_fetch`, but issues vouchers through a caller-owned shared
/// [`PoolLedger`] so multiple concurrent pulls on ONE payment channel coordinate.
///
/// The bug this fixes: each `stream_fetch`/`stream_fetch_tracked` call seeds its
/// own voucher state from `ctx.prior_*`, so N concurrent pulls on the same channel
/// each compute the next cumulative `amount` independently and collide — the node
/// accepts exactly one and rejects the rest as `AmountRegression`. Passing every
/// concurrent caller the SAME `&Arc<PoolLedger>` (shared across
/// `tokio::spawn`/`join!`) serializes their voucher issuance through the
/// ledger's mutex: each issue advances the cumulative `amount`/`bytes_delivered`
/// in turn, the channel advances monotonically, and all pulls succeed.
///
/// The caller owns the ledger's lifetime and persists what the channel paid from it
/// directly (this entrypoint does not surface a [`VoucherProgress`] — the shared ledger
/// IS the watermark). Persist via [`PoolLedger::settlement`], NOT `snapshot`:
/// `snapshot`/`committed` report only ACKED vouchers, so a voucher left in the ack wait
/// (the drop the node's `SettleOnDrop` guard handles) is under-reported and its deposit
/// stranded — `settlement` adds the in-flight voucher back (#1122/#1145). `snapshot` is
/// also `async`, so a `Drop` guard cannot call it at all.
///
/// # Errors
///
/// Same as `stream_fetch`.
#[cfg(any(test, feature = "test-util"))]
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_shared(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    ledger: &Arc<PoolLedger>,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    deadlines: PullDeadlines,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
) -> anyhow::Result<Bytes> {
    with_hard_cap(
        deadlines.hard_cap,
        fetch_inner(
            endpoint,
            target,
            ctx,
            slash_domain,
            expected_signer,
            hash,
            // Shared-channel pulls model the daemon as buyer on a node-to-node
            // fill; the requester already discovered the holder, so no namespace.
            decdn_protocol::client::NO_NAMESPACE,
            byte_offset,
            timestamp_us,
            max_blob_size_bytes,
            max_rate_per_mb,
            deadlines,
            ledger,
            // Shared concurrent pulls interleave many blobs on one channel; a
            // single unified byte-progress readout would be meaningless, so this
            // path never reports progress.
            None,
        ),
    )
    .await
}

/// Notified with a weak handle to each upstream connection at the moment it is
/// dialled, before any handshake step that could fail with the connection already
/// live on the caller's runtime.
///
/// For a caller that runs a pull on a runtime it is about to drop — `decdn-node`'s
/// per-serve pull-leg runtimes — and must first observe every connection it dialled
/// reach its drained state. The handle is weak by construction, so observing can
/// never delay the close it watches, and a caller with no such hazard (the
/// publisher CLI, one long-lived runtime) passes `None` and pays nothing.
pub type DialObserver<'a> = dyn Fn(iroh::endpoint::WeakConnectionHandle) + Send + Sync + 'a;

/// Where an [`open_stream`] gets its QUIC connection.
enum ConnSource<'a> {
    /// Dial a fresh one-shot connection to `target`. The pull owns it and closes
    /// it on its terminal method (`finish`/`abort`/drop).
    Dial {
        endpoint: &'a Endpoint,
        target: EndpointAddr,
    },
    /// Reuse a caller-owned [`WarmConnection`]'s connection. The pull borrows it
    /// and leaves it open for the next hash; the [`WarmConnection`] closes it once.
    Reuse(&'a iroh::endpoint::Connection),
}

/// The OPEN stage of a `cdn/client/v1` pull, the single request site behind
/// [`open_progressive_pull`]: get the connection from `source` (dial a fresh one,
/// or reuse a warm one), open the bi-stream, send the [`StreamRequest`], then read
/// and verify the signed [`StreamResponse`]. Returns the live connection, its
/// streams, and the verified response for the caller to stream from.
///
/// **Bounded as a whole by `open`** (#1134), and that bound lives HERE rather than
/// in the caller for a reason worth stating: the production pull paths run with no
/// overall wall-clock cap, so that a blob of any size can complete as long as bytes
/// keep arriving. Leaving the open to "whatever the caller wraps us in" therefore
/// means leaving it *unbounded* — and this is precisely the stage where a peer can
/// accept a connection, take our request, and then say nothing at all. Such a peer
/// would hang the pull until the QUIC idle timeout.
///
/// The stall bound cannot cover this stage: it measures inactivity BETWEEN bytes,
/// and here no byte has arrived yet. A wall clock is the right tool because the
/// open is bounded work whose duration does not scale with the blob.
///
/// A `slash_sig` that does not recover to `expected_signer`, a mismatched echoed
/// field, or a zero rate fails here (see [`verify_response`]) — before any byte is
/// paid for. A refusal (`body.ok == false`) is returned to the caller intact, since
/// only the caller knows how to classify it.
#[allow(clippy::too_many_arguments)]
async fn open_stream(
    source: ConnSource<'_>,
    ctx: &PoolContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    byte_len: u64,
    timestamp_us: u64,
    open: Duration,
    on_connect: Option<&DialObserver<'_>>,
) -> anyhow::Result<(
    iroh::endpoint::Connection,
    SendStream,
    RecvStream,
    StreamResponse,
    StreamResponseExt,
)> {
    tokio::time::timeout(open, async move {
        let conn = match source {
            ConnSource::Dial { endpoint, target } => endpoint
                .connect(target, ALPN_CLIENT)
                .await
                .map_err(|e| rate_limited::transport_error("connect failed", e))?,
            // Already dialled and warm — reuse the handle. A fresh `open_bi` below
            // gives this hash its own stream.
            ConnSource::Reuse(conn) => conn.clone(),
        };
        // Hand the caller its handle HERE, before the handshake — every step below
        // can fail with the connection already dialled and its driver already on
        // this runtime, and a caller that must observe the drain needs those too.
        if let Some(observe) = on_connect {
            observe(conn.weak_handle());
        }
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| rate_limited::transport_error("open_bi failed", e))?;

        let req = StreamRequest {
            hash,
            // The serving node routes on this for its pull-through authorized-origin
            // gate and origin-directory fallback (ADR 005 §Namespace routing). A
            // client fetch passes the namespace it published under; a node-to-node
            // pull to a DHT-discovered holder passes `NO_NAMESPACE` (0) — the holder
            // already has the bytes (ADR 002 §Retrieval by namespace) — while a pull
            // to a directory-discovered cold origin passes the served namespace so
            // that origin's gate resolves and it can fill from its own backend.
            namespace_id,
            pool_id: ctx.pool_id.into(),
            byte_offset,
            // `0` = whole-tail fetch; a caller requesting a bounded middle gap
            // (the gap-driven driver, #1608, via `PeerSource`) passes the gap's
            // length so the server scopes both the serve and the payment to it.
            byte_len,
            timestamp_us,
        };
        // Two-phase encode (ADR 005): attach the client identity binding when the
        // context carries one, so the serving node can prove channel ownership and
        // authorize a cache-miss origin pull (#1115). Absent ⇒ no ext bytes, exactly
        // the pre-#1115 wire (unbound node-to-node / registered-client path).
        let ext = client_binding_ext(ctx);
        let payload = decdn_protocol::encode_stream_request(&req, ext.as_ref())
            .map_err(|e| anyhow::anyhow!("encode stream request: {e}").context(LocalPullFault))?;
        write_frame(&mut send, &payload)
            .await
            .map_err(|e| rate_limited::transport_error("write stream request", e))?;

        let (resp, resp_ext) = read_stream_response(&mut recv).await?;
        verify_response(
            &resp,
            &resp_ext,
            slash_domain,
            expected_signer,
            hash,
            ctx.pool_id,
            timestamp_us,
        )?;
        Ok((conn, send, recv, resp, resp_ext))
    })
    .await
    .map_err(|_| anyhow::Error::new(PullTimeout { after: open }))?
}

/// Wallet-less resume (issue #1481 §5): the maximum number of times a fetch
/// will reopen a fresh stream after a gated, bundled
/// `AmountRegression`/`BytesRegression`/`SpendingCapExhausted`
/// rejection. Bounds a node that keeps rejecting (a buggy or adversarial
/// peer echoing a bundle that never lets the client catch up) to a handful
/// of round trips rather than looping forever; a healthy self-heal needs
/// exactly one.
///
/// Public because the STREAMING fetch (#1120) drives its own reopen loop — it
/// owns the output file and must rewind it before each retry, which this crate
/// cannot do for it — and both loops must agree on the bound.
pub const MAX_RESUME_ATTEMPTS: u32 = 3;

/// Reactive graduation (#1497): the maximum number of times the STREAMING fetch
/// (`crates/cli/src/commands/fetch.rs`) will `topUp` a channel toward its
/// `working_deposit` after a genuine mid-fetch `SpendingCapExhausted` (validated
/// against the buyer's own ledger via [`genuine_exhaustion`]) and resume at the
/// PAID FRONTIER ([`sink::content_paid_frontier`]) — not at the failed leg's own
/// offset, which would re-pay for the credited-but-unpaid tail. Separate from [`MAX_RESUME_ATTEMPTS`]: a top-up is a funding
/// action with its own on-chain cost and failure mode (a delegate key that
/// cannot fund, an allowance that fails to land), not a wallet-less resume, so
/// it is bounded on its own budget rather than sharing/competing with the resume
/// attempts.
pub const MAX_TOPUP_ATTEMPTS: u32 = 3;

/// In-memory fetch with wallet-less resume (issue #1481 §5): if a mid-stream
/// voucher rejection carries a signer-verified [`WatermarkBundle`] for one of
/// the four regression/exhaustion reasons, reseed `ledger` from it and reopen
/// the pull — at the **same** `byte_offset` the caller originally requested,
/// not `bundle.bytes_delivered` — instead of surfacing the rejection as
/// terminal.
///
/// `bundle.bytes_delivered` is deliberately NOT used as the retry's wire
/// `byte_offset`, even though that is what makes a `WatermarkBundle` look
/// resumable at a glance. The two are different axes: `bytes_delivered` is
/// the CHANNEL's cumulative payment counter (used to reconstruct each
/// voucher's EIP-712 `bytesDelivered`, ADR 005 — [`Voucher`] carries no such
/// field on the wire), not a position within THIS blob's byte range. A
/// channel can fund many blobs; jumping the wire offset ahead to the
/// channel's cumulative would, for a caller that requested `byte_offset == 0`,
/// silently return a TRUNCATED tail instead of the full blob the caller is
/// relying on getting back. The bytes already decoded in the failed attempt
/// were dropped with it, so there is nothing to legitimately splice a jump
/// with. Retrying at the caller's original offset is always safe and is the
/// only thing this bundle field is actually needed for: fixing the ledger's
/// amount/bytes BASELINE so the resumed stream's vouchers verify against what
/// the node now expects, not re-deriving where in the blob to resume.
///
/// This is the retry loop for this case on the in-memory path —
/// [`UpstreamPull::pay_one`] never retries itself, because the stream it holds
/// is already dead by the time a mid-stream `VoucherRejected` reaches it (the
/// node finishes its send side before replying,
/// `handlers/client/wire.rs::write_reject`); a fresh stream can only be opened
/// by whoever owns the connection, which is here.
///
/// Every `stream_fetch*` wrapper goes through here, so a resumable rejection is
/// retried transparently and the caller only ever sees the FINAL outcome
/// (success, or the original terminal error once [`MAX_RESUME_ATTEMPTS`] is
/// exhausted or the reason/bundle isn't eligible).
///
/// The production callers do not use this wrapper: the gap-driven
/// [`driver::drive`] reaches the same reseed loop around [`open_progressive_pull`],
/// and additionally answers a genuine `SpendingCapExhausted` with an on-chain
/// top-up through its [`Funder`] — the thing a from-zero retry could never do
/// without re-paying for the delivered prefix.
///
/// A bundle-less rejection, a non-gated reason, or a bundle that fails
/// [`WatermarkBundle::validate`] (malformed `last_signature` length) is
/// never treated as resumable and is returned to the caller unchanged on
/// the first attempt.
#[cfg(any(test, feature = "test-util"))]
#[allow(clippy::too_many_arguments)]
async fn fetch_inner(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    deadlines: PullDeadlines,
    ledger: &Arc<PoolLedger>,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<Bytes> {
    for attempt in 0..=MAX_RESUME_ATTEMPTS {
        let result = fetch_in_memory_once(
            endpoint,
            target.clone(),
            ctx,
            Arc::clone(ledger),
            slash_domain,
            expected_signer,
            hash,
            namespace_id,
            byte_offset,
            timestamp_us,
            max_blob_size_bytes,
            max_rate_per_mb,
            deadlines,
            on_progress,
        )
        .await;
        let Err(e) = result else {
            return result;
        };
        if attempt == MAX_RESUME_ATTEMPTS {
            return Err(e);
        }
        // A bundle that proves no desync (see `heal_watermark_desync`) is not worth
        // the resume budget: surface the real error instead.
        let healed = match rejection_watermark(&e, ctx) {
            Some(watermark) => heal_watermark_desync(&e, watermark, ledger).await,
            None => None,
        };
        let Some(healed) = healed else {
            return Err(e);
        };
        tracing::debug!(
            attempt,
            byte_offset,
            ?healed,
            "voucher rejection carried an authenticated watermark bundle; retrying at the same \
             byte_offset"
        );
    }
    // Unreachable: the loop above always returns on both the `Ok` and every `Err`
    // branch (either directly or after `attempt == MAX_RESUME_ATTEMPTS`
    // triggers on the final iteration). Kept as a typed bail rather than
    // `unreachable!()`/`panic!()` per the workspace anti-panic policy.
    Err(anyhow::anyhow!("resume loop exited without returning"))
}

/// One attempt of the in-memory `stream_fetch*` path: open a progressive pull,
/// feed its wire bytes through the bao verifying decoder AS THEY ARRIVE, and
/// return the decoded plaintext span `[byte_offset, total_bytes)`.
///
/// This is the same receive loop the production callers drive
/// ([`UpstreamPull::next_chunk`] under a [`PullReader`]) with an in-memory `Vec`
/// standing in for the ranged store / cache admit; there is no second receive
/// implementation. A corrupt chunk group therefore aborts the pull at that group
/// (ADR 038 §Receive side), before the rest of the stream is pulled or paid for —
/// so a peer that signs an inflated `total_bytes` and streams framed garbage can
/// bill at most the bytes that arrived before the first unverifiable group, never
/// its claim.
///
/// # Errors
///
/// Everything [`open_progressive_pull`] raises, plus [`HashMismatch`] for a
/// paid-but-corrupt delivery (including a non-empty root claimed against an empty
/// blob, #1054), the typed pull faults [`PullReader`] stashes (stall, refusal,
/// voucher rejection), a truncated stream, or a short delivery at `finish`.
#[cfg(any(test, feature = "test-util"))]
#[allow(clippy::too_many_arguments)]
async fn fetch_in_memory_once(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    ledger: Arc<PoolLedger>,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    deadlines: PullDeadlines,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<Bytes> {
    let (header, pull) = open_progressive_pull(
        endpoint,
        target,
        ctx,
        ledger,
        slash_domain,
        expected_signer,
        hash,
        namespace_id,
        byte_offset,
        timestamp_us,
        max_blob_size_bytes,
        max_rate_per_mb,
        deadlines,
        // Whole tail: the in-memory wrapper has no store to compute gaps against.
        0,
        // One long-lived test runtime: nothing to strand, so no dial observer.
        None,
    )
    .await?;
    let total_bytes = header.total_bytes;
    // A 0-byte blob (#1054) aligns to an empty range the decoder cannot anchor, so
    // the empty stream must be proven against the empty root explicitly rather than
    // accepted for an arbitrary one (see [`reject_empty_claim_for_nonempty_root`]).
    if total_bytes == 0 {
        reject_empty_claim_for_nonempty_root(total_bytes, hash)?;
        pull.finish().await?;
        return Ok(Bytes::new());
    }
    let aligned = align_range(byte_offset, 0, total_bytes)
        .map_err(|e| anyhow::anyhow!("range alignment: {e}").context(LocalPullFault))?;
    let reader = PullReader::new(pull);
    let (plaintext, reader) =
        decode_to_vec(hash, total_bytes, &aligned, reader, on_progress).await?;
    // Wire completeness (the full promised bao wire size arrived before
    // `StreamEnd`) and a clean close; the watermark it returns is read back from
    // the shared ledger by the caller, so it is not needed here.
    reader.into_inner().finish().await?;
    // The decoded buffer spans the aligned superset `[fetch_start, total_bytes)`;
    // trim back to the caller's requested span. Guard the bound explicitly:
    // `Bytes::slice` panics out of range, and a short decode must surface as a
    // clean error.
    let lead = usize::try_from(byte_offset.saturating_sub(aligned.fetch_start()))?;
    let bytes = Bytes::from(plaintext);
    if lead > bytes.len() {
        anyhow::bail!("decoded range shorter than requested span");
    }
    Ok(bytes.slice(lead..))
}

/// Like [`fetch_in_memory_once`], but opens the pull on a caller-owned
/// [`WarmConnection`] so the in-memory `stream_fetch_on` wrapper can prove
/// connection reuse across hashes (#1848 T1).
///
/// # Errors
///
/// The same set as [`fetch_in_memory_once`].
#[cfg(any(test, feature = "test-util"))]
#[allow(clippy::too_many_arguments)]
async fn fetch_in_memory_once_on(
    warm: &WarmConnection,
    ctx: &PoolContext,
    ledger: Arc<PoolLedger>,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    deadlines: PullDeadlines,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<Bytes> {
    let (header, pull) = open_progressive_pull_on(
        warm,
        ctx,
        ledger,
        slash_domain,
        expected_signer,
        hash,
        namespace_id,
        byte_offset,
        timestamp_us,
        max_blob_size_bytes,
        max_rate_per_mb,
        deadlines,
        // Whole tail: the in-memory wrapper has no store to compute gaps against.
        0,
        // One long-lived test runtime: nothing to strand, so no dial observer.
        None,
    )
    .await?;
    let total_bytes = header.total_bytes;
    if total_bytes == 0 {
        reject_empty_claim_for_nonempty_root(total_bytes, hash)?;
        pull.finish().await?;
        return Ok(Bytes::new());
    }
    let aligned = align_range(byte_offset, 0, total_bytes)
        .map_err(|e| anyhow::anyhow!("range alignment: {e}").context(LocalPullFault))?;
    let reader = PullReader::new(pull);
    let (plaintext, reader) =
        decode_to_vec(hash, total_bytes, &aligned, reader, on_progress).await?;
    reader.into_inner().finish().await?;
    let lead = usize::try_from(byte_offset.saturating_sub(aligned.fetch_start()))?;
    let bytes = Bytes::from(plaintext);
    if lead > bytes.len() {
        anyhow::bail!("decoded range shorter than requested span");
    }
    Ok(bytes.slice(lead..))
}

/// Feed a bao interleaved wire stream through the verifying decoder, appending
/// each verified leaf to an in-memory buffer, and return the buffer plus the
/// reader for the caller to finish. Every chunk group is checked against the
/// content-hash root `root` as it is decoded, so a corrupt group — at any offset,
/// including a resumed tail — is rejected at that group without needing earlier
/// bytes or the rest of the stream (ADR 038 §Receive side).
///
/// The decoded buffer spans the chunk-group-aligned **superset**
/// `[aligned.fetch_start(), aligned.fetch_end())` the server serves (a bao proof
/// anchors whole 16 KiB groups); the caller trims the lead. The buffer is grown
/// leaf by leaf rather than pre-sized: `aligned` derives from the peer's
/// unverified `total_bytes`, so a capacity hint would let an inflated claim drive
/// an over-allocation before a single byte verifies.
///
/// `on_progress`, when set, is called with `(content_position, total_bytes)` after
/// each verified leaf lands — the same content-byte unit [`driver::drive`] reports.
///
/// Same loop shape as [`ClientRangedStore::ingest_stream`], minus the durable
/// checkpointing; a typed fault the reader stashed ([`StashedFault`]) takes
/// precedence over the decoder's own complaint, and a decode failure is
/// classified by [`sink::classify_decode_error`] ([`HashMismatch`] for a
/// verification failure, a truncation error for a short stream).
///
/// # Errors
///
/// The reader's stashed typed fault, [`HashMismatch`], or a truncation / IO fault.
#[cfg(any(test, feature = "test-util"))]
async fn decode_to_vec<R>(
    root: [u8; 32],
    total_bytes: u64,
    aligned: &AlignedRange,
    reader: R,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<(Vec<u8>, R)>
where
    R: iroh_io::AsyncStreamReader + StashedFault + Send,
{
    let tree = BaoTree::new(total_bytes, IROH_BLOCK_SIZE);
    let root = blake3::Hash::from_bytes(root);
    let mut decoder = ResponseDecoder::new(root, aligned.chunk_ranges().clone(), tree, reader);
    let mut out = Vec::new();
    loop {
        match decoder.next().await {
            ResponseDecoderNext::More((rest, Ok(BaoContentItem::Leaf(leaf)))) => {
                out.extend_from_slice(&leaf.data);
                if let Some(cb) = on_progress {
                    let position = aligned
                        .fetch_start()
                        .saturating_add(u64::try_from(out.len()).unwrap_or(u64::MAX));
                    cb(position, total_bytes);
                }
                decoder = rest;
            }
            ResponseDecoderNext::More((rest, Ok(BaoContentItem::Parent(_)))) => decoder = rest,
            ResponseDecoderNext::More((rest, Err(decode_err))) => {
                let mut r = rest.finish();
                if let Some(fault) = r.take_fault() {
                    return Err(fault);
                }
                return Err(sink::classify_decode_error(decode_err));
            }
            ResponseDecoderNext::Done(mut r) => {
                if let Some(fault) = r.take_fault() {
                    return Err(fault);
                }
                return Ok((out, r));
            }
        }
    }
}

/// The security-critical core shared by every "did WE sign this?" check: does `signature` recover
/// to `expected` over `voucher` under `domain`?
///
/// A node's claim about a lane's watermark arrives over an unauthenticated application-level
/// message — a mid-stream `StreamError::VoucherRejected` on the fetch path (#1042) — so the claimed
/// `amount`/`bytes_delivered` are attacker-controllable on the wire. Without this check a malicious
/// or buggy upstream could hand back an inflated watermark and have the client act on it: `reseed`
/// its ledger and then sign a voucher for the echoed `amount + delta` on the retried pull. That is
/// a voucher this client genuinely holds the key to sign and the node can redeem on-chain up to the
/// deposit, draining the pool while delivering ~nothing.
///
/// The client's OWN last-accepted voucher signature closes that hole: a node can only echo back a
/// signature the client itself produced, so a tuple that verifies is by construction one this client
/// already committed to. That the node's stored signature always matches its stored tuple is the
/// write-side invariant of the seller-side lane state, which writes `last_amount`/`last_bytes` and
/// the accepted signature together and only after verifying the signature against the lane's pinned
/// capability `signer`. A signature that does not recover to `expected` is treated as a hostile or
/// corrupt echo, never as evidence.
///
/// Callers MUST verify over the exact tuple they are about to adopt as their new committed baseline
/// — never a tuple derived from it — so there is no window in which the proof covers one state and
/// the action commits another. Taking the whole [`Voucher`] rather than its fields loose is what
/// lets a caller verify and then act on *the same value*.
#[must_use]
pub(crate) fn voucher_signed_by(
    voucher: &Voucher,
    signature: &[u8],
    expected: Address,
    domain: &Eip712Domain,
) -> bool {
    let Ok(signature) = Signature::try_from(signature) else {
        return false;
    };
    SignedVoucher {
        voucher: voucher.clone(),
        signature,
    }
    .recover_signer(domain)
    .is_ok_and(|recovered| recovered == expected)
}

/// Extract a resumable, AUTHENTICATED [`WatermarkBundle`] from a pull error, or `None` if the
/// error is not an `UpstreamVoucherRejected`, its reason is not
/// [`VoucherRejectReason::is_watermark_gated`], it carries no bundle, the bundle fails
/// [`WatermarkBundle::validate`] (malformed `last_signature` length), or either of the two
/// security-critical checks fails: `last_signature` must recover to this client's OWN
/// voucher-signing address over the bundle's anchor and chain section, and the bundle's `tip`
/// must PROVE its `verified_index`, by hashing forward to the root that signature covers.
///
/// Both halves of the bundle are folded into money a resuming signer signs, so both have to be
/// evidence. The anchor half is the client's own signature ([`voucher_signed_by`]; see its doc
/// for why acting on an unverified watermark is a channel-draining hole rather than a robustness
/// nicety). The chain half carries no signature at all, which is what makes the tip check the
/// only thing standing between an inflated `verified_index` and a client that signs it.
pub(crate) fn resumable_watermark<'a>(
    err: &'a anyhow::Error,
    ctx: &PoolContext,
) -> Option<&'a WatermarkBundle> {
    let rejected = err.downcast_ref::<UpstreamVoucherRejected>()?;
    if !rejected.reason.is_watermark_gated() {
        return None;
    }
    let bundle = rejected.bundle.as_ref()?;
    if bundle.validate().is_err() {
        return None;
    }
    let claimed = Voucher {
        pool_id: ctx.pool_id,
        signer: ctx.client_signer.address(),
        provider: ctx.provider,
        amount: U256::from(bundle.amount),
        bytes_delivered: U256::from(bundle.bytes_delivered),
        chain_root: B256::from(bundle.chain_root),
        chunk_price: U256::from(bundle.chunk_price),
    };
    if !voucher_signed_by(
        &claimed,
        &bundle.last_signature,
        ctx.client_signer.address(),
        &ctx.voucher_domain,
    ) {
        return None;
    }
    frontier_is_proved(bundle).then_some(bundle)
}

/// How [`heal_watermark_desync`] resolved a rejection's authenticated watermark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Healed {
    /// The bundle was AHEAD of our committed watermark: the node holds a voucher
    /// we lost. [`PoolLedger::reseed`] moved us up to it.
    Reseeded,
    /// An `Underpaid` bundle was BEHIND our committed watermark: we hold vouchers
    /// the node never accepted. [`PoolLedger::rebase`] moved us down to it.
    Rebased,
    /// An `Underpaid` for a voucher signed before the latest rebase. The ledger
    /// has already healed; nothing moved, and the pull retries from the healed
    /// anchor.
    Stale,
}

/// The authenticated watermark a voucher rejection carries, as the cumulative a
/// ledger re-anchors to, or `None` when [`resumable_watermark`] refuses it.
#[must_use]
pub(crate) fn rejection_watermark(err: &anyhow::Error, ctx: &PoolContext) -> Option<Cumulative> {
    resumable_watermark(err, ctx).map(Cumulative::from)
}

/// Heal the watermark desync a voucher rejection reports, from `watermark` — the
/// authenticated bundle [`rejection_watermark`] extracted — and say how, or
/// `None` when it proves no desync and the caller must surface the real error.
///
/// A bundle ahead of our committed watermark reseeds, whatever the reason. A
/// bundle behind it heals only on [`VoucherRejectReason::Underpaid`], and only
/// for a voucher signed under the ledger's current generation; see
/// [`PoolLedger::rebase`]. A bundle that merely echoes our own watermark proves
/// no desync: the node attaches one to every watermark-gated rejection once a
/// voucher is accepted, so an exhausted lane echoes it straight back.
pub(crate) async fn heal_watermark_desync(
    err: &anyhow::Error,
    watermark: Cumulative,
    ledger: &PoolLedger,
) -> Option<Healed> {
    if ledger.reseed(watermark) {
        tracing::debug!(
            amount = %watermark.amount,
            bytes = %watermark.bytes,
            "voucher rejection carried a watermark ahead of ours; reseeded"
        );
        return Some(Healed::Reseeded);
    }
    let rejected = err.downcast_ref::<UpstreamVoucherRejected>()?;
    if rejected.reason != VoucherRejectReason::Underpaid {
        return None;
    }
    let outcome = ledger.rebase(watermark, rejected.proof_generation).await;
    rebase_outcome(outcome, watermark, rejected.proof_generation)
}

/// Log what a [`PoolLedger::rebase`] did and say how the heal resolved.
fn rebase_outcome(
    outcome: Rebase,
    watermark: Cumulative,
    proof_generation: Option<u64>,
) -> Option<Healed> {
    match outcome {
        Rebase::Rebased { from } => {
            tracing::warn!(
                from_amount = %from.amount,
                from_bytes = %from.bytes,
                to_amount = %watermark.amount,
                to_bytes = %watermark.bytes,
                "upstream refused our vouchers as underpaid: our lane watermark was ahead of \
                 the one it accepted; rebased down to it"
            );
            Some(Healed::Rebased)
        }
        Rebase::Stale => {
            tracing::debug!(
                ?proof_generation,
                "underpaid rejection of a voucher signed before the latest rebase; retrying \
                 from the healed anchor"
            );
            Some(Healed::Stale)
        }
        Rebase::Refused => None,
    }
}

/// Whether the bundle's `tip` proves the `verified_index` it reports:
/// `keccak^verified_index(tip) == chain_root`.
///
/// A bundle reports two things and the resuming signer folds BOTH into the amount it re-signs.
/// The anchor half is covered by the client's own signature. The chain half is covered by
/// nothing — `verified_index` is a number the node writes, and the signature it echoes is over a
/// voucher that contains no index. A node that reported 255 where the client released 3 would
/// otherwise collect `252 × chunk_price` for chunks it never delivered, from a client that signed
/// for them itself.
///
/// The tip closes that, and it is the only thing that can: a preimage is unforgeable, so the
/// deepest one the node can present is the deepest one it was actually GIVEN, and hashing it
/// forward to a root the client's own signature commits to turns `verified_index` from a claim
/// into a proof. The walk is `verified_index` keccaks against local state — no signer, no round
/// trip.
///
/// `verified_index == 0` folds nothing, so it needs no proof and carries no tip (the node sends
/// all-zero). That is the never-metered and sealed case, and the only one where an all-zero tip
/// is legitimate.
#[must_use]
fn frontier_is_proved(bundle: &WatermarkBundle) -> bool {
    bundle.verified_index == 0
        || decdn_incentive::chain::verify_forward(
            B256::from(bundle.tip),
            bundle.verified_index,
            B256::from(bundle.chain_root),
        )
}

/// True iff `err` is a `SpendingCapExhausted` voucher rejection that the buyer's OWN ledger
/// corroborates as genuine exhaustion, AND the buyer's remaining spendable deposit is below the
/// cost of the next voucher. A node claiming `SpendingCapExhausted` while the buyer's ledger
/// still shows headroom is NOT corroborated (returns `false`) — the caller must refuse to fund
/// it.
///
/// `committed` is the caller's OWN `ledger.committed()` at the moment of the rejection — the
/// watermark this genuinely-exhausted-or-not decision is judged against.
///
/// The healable-desync carve-out is watermark-ADVANCEMENT-based, not bundle-PRESENCE-based.
/// The node attaches an authenticated [`WatermarkBundle`] to EVERY watermark-gated rejection for
/// which it has a prior accepted voucher to echo (`watermark_bundle_for_reject` in
/// `crates/node/src/handlers/client/voucher.rs`) — including a rejection caused by perfectly
/// ordinary, real exhaustion on a lane that has already had some vouchers accepted. Treating
/// bundle PRESENCE alone as "this is a desync" would misroute every such real exhaustion into
/// the resync path (which cannot fix it — the deposit is actually short — and eventually fails
/// after burning `MAX_RESUME_ATTEMPTS`) instead of the top-up path that could. The bundle is only
/// evidence of desync when it reports a watermark AHEAD of what we already hold — i.e. the node
/// knows about a voucher we do not, which reseeding can heal. A bundle that merely echoes back
/// our OWN already-committed watermark (at or behind `committed`) proves nothing about desync;
/// it is the node correctly reporting the state we already agree on, and the exhaustion is real.
pub fn genuine_exhaustion(
    err: &anyhow::Error,
    ctx: &PoolContext,
    committed: Cumulative,
    remaining_spendable: U256,
    next_voucher_cost: U256,
) -> bool {
    let Some(rejected) = err.downcast_ref::<UpstreamVoucherRejected>() else {
        return false;
    };
    if rejected.reason != VoucherRejectReason::SpendingCapExhausted {
        return false;
    }
    // An authenticated bundle that ADVANCES our committed amount is a healable desync — let the
    // resume loop reseed instead of adding funds. A bundle that is absent, or present but at or
    // behind `committed`, proves no desync (see the doc comment above), so exhaustion can still
    // be genuine.
    if let Some(bundle) = resumable_watermark(err, ctx)
        && Cumulative::from(bundle).amount > committed.amount
    {
        return false;
    }
    // Validate the node's claim against our OWN accounting: only genuine if we truly cannot
    // cover the next voucher. Otherwise the node is lying/buggy and we refuse to fund it.
    remaining_spendable < next_voucher_cost
}

/// Whether a failed open/resume is consistent with the resume offset being wrong
/// — i.e. the upstream refused it as a range past the end, or with the ambiguous
/// `NotFound` its range gate collapses `byte_offset >= total_bytes` into.
///
/// This is the shared predicate the CLI's streaming fetch loop and the #1608
/// gap-driven [`driver::drive`] both key their post-top-up settle-wait on: right
/// after an on-chain `topUp`, the serving node's chain watcher may not yet have
/// observed the new deposit, so its pre-serve deposit gate (#1518) refuses the
/// resumed open with exactly this shape. Retrying the OPEN is money-safe (no
/// vouchers are sent and the offset is unchanged), so the caller waits briefly
/// for the watcher rather than treating it as terminal.
///
/// Two signals qualify, and the second is unavoidably ambiguous:
///
/// - [`ResumeOffsetPastEnd`] — the node signed a response whose `total_bytes` is
///   at or below our offset. Unambiguous, but only reachable against a
///   non-conforming server.
/// - `NotFound` — what an honest node actually sends. Its range gate refuses
///   `byte_offset >= total_bytes` *before* signing, and that deliberately
///   collapses to `NotFound` on the wire alongside a cache miss and an unknown
///   channel (`ServeRejectReason::wire_error`), so the client cannot separate
///   "your offset is past the end" from "I don't have this blob".
///
/// Everything else — stalls, resets, hash mismatches, local flush failures — must
/// NOT qualify. Those say nothing about the offset.
#[must_use]
pub fn resume_may_be_stale(err: &anyhow::Error) -> bool {
    if err.downcast_ref::<ResumeOffsetPastEnd>().is_some() {
        return true;
    }
    err.downcast_ref::<UpstreamRefused>()
        .is_some_and(|refused| matches!(refused.error(), StreamError::NotFound))
}

/// Whether `err` is an **open-time** [`StreamError::InsufficientDeposit`] refusal
/// (ADR 003 §Pool solvency, option 2 / #2013): the serving node proved us the
/// authenticated pool owner and told us its refundable floor `M` outruns our
/// pool's remaining deposit, so the pool cannot cover a credit window here.
///
/// `InsufficientDeposit` is a delivery-side code — a legitimate one rides ONLY in
/// the signed open-stage `StreamResponse { ok: false }`, never mid-stream. So this
/// gates on open-stage evidence ([`UpstreamRefused::evidence`] present): a
/// protocol-violating peer that emits a bare mid-stream `StreamError::InsufficientDeposit`
/// (no signed response) is NOT honored as a floor refusal and never drives the
/// top-up loop.
///
/// The driver routes an open-stage refusal into its fund-and-retry loop the same
/// way it routes a ledger-corroborated exhaustion: it tops the deposit up toward
/// the buyer's own `working_deposit` ceiling and re-opens, so a node's
/// larger-than-estimated `M` no longer dead-ends a fetch on an ambiguous
/// `NotFound`. Unlike [`genuine_exhaustion`], this needs no ledger corroboration —
/// our own numbers say we CAN afford the next voucher; only the node's private `M`
/// (which we cannot compute) is higher. The buyer's ceiling is the sole clamp on
/// how much a (possibly lying) node can make us escrow, so trusting the owner-only
/// refusal is money-safe.
#[must_use]
pub fn is_insufficient_deposit(err: &anyhow::Error) -> bool {
    err.downcast_ref::<UpstreamRefused>()
        .is_some_and(|refused| {
            matches!(refused.error(), StreamError::InsufficientDeposit)
                && refused.evidence().is_some()
        })
}

/// The wire-byte bound for a fetch of `[byte_offset, byte_offset + byte_len)`
/// (`byte_len == 0` meaning "to end") of a `total_bytes` blob: the bao-encoded
/// size of the chunk-group-aligned range (content plus interleaved proof, ADR
/// 038 §Payment metering), exactly the byte count the server emits. The server
/// widens the request to enclosing 16 KiB groups; [`align_range`] /
/// [`AlignedRange::wire_len`](decdn_bao_range::AlignedRange::wire_len)
/// reproduce that, keeping encoder and receiver in lock-step. The one site every
/// pull derives its wire bound (and its received-byte ceiling) through.
fn aligned_wire_len(byte_offset: u64, byte_len: u64, total_bytes: u64) -> anyhow::Result<u64> {
    let aligned = align_range(byte_offset, byte_len, total_bytes)
        .map_err(|e| anyhow::anyhow!("range alignment: {e}").context(LocalPullFault))?;
    Ok(aligned.wire_len())
}

/// How often the throughput floor samples the byte counter: a fraction of the window, so a
/// stall is detected within roughly one extra sample period beyond the window, floored at
/// 100 ms so a tiny window cannot spin the sampler.
pub(crate) fn stall_sample_period(window: Duration) -> Duration {
    (window / 8).max(Duration::from_millis(100))
}

/// How long the receive loop waits for a terminal message after a voucher write
/// fails (below). The node writes `StreamEnd` (or a `StreamError`) *before* the
/// teardown that stops our send, so the terminal signal is already in flight and
/// arrives at once; the bound only stops a peer that stops our send and then goes
/// silent from pinning this recovery read. It is an error-path bound, not a
/// steady-state one.
const TERMINAL_AFTER_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Header fields from the upstream `StreamResponse`, surfaced by
/// [`open_progressive_pull`] before the first chunk so the fused serve path
/// (#856) knows `total_bytes` up front — it must sign its OWN downstream
/// `StreamResponse` (which commits to a `total_bytes`) before forwarding a byte.
#[derive(Debug, Clone, Copy)]
pub struct UpstreamPullHeader {
    /// Whole-blob size the upstream promised (echoed into our downstream
    /// `StreamResponse`).
    pub total_bytes: u64,
    /// Upstream rate; informational for the caller (the buyer pays it inside
    /// [`UpstreamPull::next_chunk`]).
    pub rate_per_mb: u64,
    /// Upstream metering quantum in bytes: the buyer releases one hash-chain
    /// preimage per chunk as they arrive. Always `CHUNK_BYTES` on a paid
    /// upstream — it is a protocol constant, not a negotiated value — and `0`
    /// on an unpaid source, which is the only reason it is carried at all.
    pub interval_bytes: u64,
    /// Observed time-to-first-byte in milliseconds: wall-clock elapsed between
    /// dialling `target` and this signed [`StreamResponse`] verifying. `0.0` for
    /// a source with no real network round trip (a local origin re-encode, or a
    /// test double). Fed into [`crate::PeerStore::record_sample`] by callers that
    /// track peer knowledge (`decdn fetch`), superseding a probe-only latency
    /// with the real per-fetch figure.
    pub ttfb_ms: f64,
}

/// A live, progressive `cdn/client/v1` pull (#856) — the ONE receive loop for
/// paid delivery. Opened by [`open_progressive_pull`] (which has already done the
/// handshake and verified the response), driven chunk-by-chunk via
/// [`Self::next_chunk`], and closed by [`Self::finish`] (a wire-completeness
/// check — integrity is verified per bao chunk group by the consumer's decoder
/// as the bytes land, not by a whole-blob re-hash) or [`Self::abort`].
///
/// It pays the upstream per chunk *inside* `next_chunk` and yields each chunk to
/// the caller — a [`sink::PullReader`] feeding a verifying decoder into the ranged
/// store, the cache admit, or (`test-util`) memory — instead of buffering the
/// whole blob. This is what lets the serving node cap its speculative exposure to
/// a bounded window rather than fronting the entire upstream cost before any
/// downstream voucher arrives, and what bounds a lying peer's bill to the bytes
/// that arrived before its first unverifiable chunk group.
///
/// The acked voucher watermark is OWNED here (not threaded as a `&mut`
/// out-param) and read back via [`Self::progress`] /
/// returned by `finish`/`abort` — the caller (`node_origin`) persists it. On any
/// exit, the caller MUST call `progress`/`finish`/`abort` to recover the
/// watermark for `record_progress` (#852); a [`Drop`] guard closes the
/// connection if none ran, but cannot return the watermark, so the obligation
/// stands.
///
/// **Deadlines.** The *open* (handshake) phase is bounded inside
/// [`open_progressive_pull`] by `PullDeadlines::open`, via the shared `open_stream`
/// helper — NOT by the caller (#1134). `decdn-node` does additionally wrap the open
/// in its per-candidate budget, but that is belt-and-braces: a bound left to the
/// caller is a bound a caller can forget.
///
/// The streaming `next_chunk`/`finish` reads are bounded here, by a THROUGHPUT FLOOR
/// (#1797): bytes are counted off the QUIC stream sub-frame, and a read is abandoned when
/// the bytes across the trailing `window` fall below `floor_bps · window`. The detector
/// persists across reads, so its window spans the whole stream rather than one call, and a
/// self-inflicted window-pacing pause is excluded from the window.
///
/// A wall clock would be the wrong bound to reach for here: this type exists to
/// stream blobs of any size, so any fixed deadline would either kill a healthy
/// large transfer or be too loose to catch a dead one. A byte-throughput floor is
/// indifferent to size and link speed, and frame-size-independent.
pub struct UpstreamPull {
    conn: iroh::endpoint::Connection,
    /// Whether this pull OWNS its connection (a one-shot dial, closed on the
    /// pull's terminal method) or merely BORROWS a [`WarmConnection`]'s (left open
    /// for the next hash, closed once by the warm connection's own `Drop`).
    owns_conn: bool,
    send: SendStream,
    recv: RecvStream,
    ctx: PoolContext,
    /// The channel's voucher ledger, SHARED with every other concurrent pull on this
    /// channel (#1145 review). Not a per-pull one-shot: that made two concurrent pulls
    /// each compute the same next cumulative `amount` independently and collide
    /// (`AmountRegression`).
    ledger: Arc<PoolLedger>,
    hash: [u8; 32],
    rate_per_mb: u64,
    /// This stream's chain anchor — which epoch it has told the upstream about.
    meter: StreamMeter,
    /// Throughput-floor window for every streaming read (#1797). Names the `after` on a
    /// [`PullStalled`] / [`PullTimeout`]; the floor itself owns the window and floor rate.
    window: Duration,
    /// Bytes read off the QUIC stream so far, tallied sub-frame by the `ProgressReader`
    /// each `next_chunk`/`finish` read wraps `recv` in. The floor reads this counter.
    progress_counter: Arc<std::sync::atomic::AtomicU64>,
    /// The byte-progress stall detector, persisted across reads so its window spans the
    /// whole stream, not one `next_chunk` call.
    floor: progress::ThroughputFloor,
    /// The floor's sampling tick.
    sampler: tokio::time::Interval,
    /// Promised **wire** bytes for this stream: the bao-encoded size of the
    /// chunk-group-aligned range (content plus interleaved proof, ADR 038), not
    /// the content-byte remainder. Bounds the receive loop and the closing voucher.
    expected_wire_bytes: u64,
    /// The buyer's received-byte ceiling (#1895) as a WIRE bound: the wire size of a
    /// ceiling-sized blob's content from this stream's offset, so `next_chunk` can
    /// enforce `max_blob_size_bytes` on the bytes that ACTUALLY arrive without
    /// decoding. `0` = unlimited.
    max_received_wire: u64,
    /// Wire bytes received so far on this stream.
    cumulative: u64,
    /// Wire bytes received but not yet covered by a proof.
    unproved: u64,
    /// `StreamEnd` seen — `next_chunk` returns `None` and `finish` skips the
    /// drain.
    ended: bool,
}

impl std::fmt::Debug for UpstreamPull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamPull")
            .field("hash", &blake3::Hash::from_bytes(self.hash))
            .field("expected_wire_bytes", &self.expected_wire_bytes)
            .field("cumulative", &self.cumulative)
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

/// Open a progressive `cdn/client/v1` pull (#856): connect, send the
/// [`StreamRequest`], read and verify the signed [`StreamResponse`] (so
/// `total_bytes` is known up front), and return its header plus a live
/// [`UpstreamPull`] to drive. Zero-rate rejection, `slash_sig` recovery, echoed
/// field checks, the buyer's rate ceiling, and the `total_bytes >= byte_offset`
/// floor are all enforced BEFORE the first chunk. The `max_blob_size_bytes`
/// ceiling is NOT one of them (#1895): the peer's `total_bytes` claim is
/// unverified, so the ceiling is enforced on the bytes that ACTUALLY arrive,
/// inside [`UpstreamPull::next_chunk`], as [`BlobTooLarge`]. `0` = unlimited.
///
/// # Errors
///
/// Connect / transport faults, a refused or zero-rate response, a bad `slash_sig`,
/// a mismatched echoed field, a [`RateAboveCeiling`] quote, a
/// [`ResumeOffsetPastEnd`] offset, or a resume offset already at/past the size
/// ceiling ([`BlobTooLarge`]). The size ceiling otherwise surfaces later, as a
/// [`BlobTooLarge`] abort once received bytes cross it.
///
/// The open stage is bounded by `deadlines.open`, inside the shared `open_stream`
/// helper — NOT left to the caller (#1134). `node_origin` additionally wraps this
/// call in its per-candidate budget, which is belt-and-braces rather than the sole
/// bound. `deadlines.window` and `deadlines.floor_bps` are the throughput floor the returned
/// [`UpstreamPull`] carries into every streaming read; `deadlines.hard_cap` is not consulted
/// here (the caller owns the streaming lifetime on this path).
///
/// `ledger` is the CHANNEL's voucher ledger, not this pull's: pass the same
/// `Arc<PoolLedger>` to every concurrent pull on one channel, or they will each
/// compute the same next cumulative `amount` independently and collide
/// (`AmountRegression`, #1145 review). The caller reads what to
/// persist from it — including after a drop — via [`PoolLedger::settlement`].
///
/// Runs inside an `open_progressive_pull` span that covers the dial and the
/// signed-response handshake — the time to first byte. Its `hash`, `pool_id`,
/// `byte_offset`, `peer` and `local_node_id` fields match the serving node's
/// `serve_stream` span, with `peer` and `local_node_id` swapped.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    name = "open_progressive_pull",
    skip_all,
    fields(
        error = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.kind = "client",
        peer = %target.id,
        local_node_id = %endpoint.id(),
        hash = %decdn_protocol::ContentHash::from_bytes(hash),
        pool_id = %ctx.pool_id,
        byte_offset = byte_offset,
        byte_len = byte_len,
    )
)]
pub async fn open_progressive_pull(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    ledger: Arc<PoolLedger>,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    deadlines: PullDeadlines,
    byte_len: u64,
    on_connect: Option<&DialObserver<'_>>,
) -> anyhow::Result<(UpstreamPullHeader, UpstreamPull)> {
    open_progressive_pull_impl(
        ConnSource::Dial { endpoint, target },
        // One-shot dial: the pull owns this connection and closes it on teardown.
        true,
        ctx,
        ledger,
        slash_domain,
        expected_signer,
        hash,
        namespace_id,
        byte_offset,
        timestamp_us,
        max_blob_size_bytes,
        max_rate_per_mb,
        deadlines,
        byte_len,
        on_connect,
    )
    .await
}

/// Like [`open_progressive_pull`], but opens the pull on a caller-owned
/// [`WarmConnection`] instead of dialling a fresh connection (#1848). The warm
/// connection is reused across many hashes — one dial, a fresh bi-stream per hash
/// (one stream = one hash, no wire change) — and stays open when this pull ends,
/// so the pull borrows the connection and never closes it. The [`WarmConnection`]
/// closes it once, on its own `Drop`.
///
/// Every other argument behaves exactly as on [`open_progressive_pull`]; see its
/// docs. There is no `endpoint`/`target` pair — the warm connection already names
/// its peer.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    name = "open_progressive_pull",
    skip_all,
    fields(
        error = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.kind = "client",
        peer = %warm.connection().remote_id(),
        hash = %decdn_protocol::ContentHash::from_bytes(hash),
        pool_id = %ctx.pool_id,
        byte_offset = byte_offset,
        byte_len = byte_len,
    )
)]
pub(crate) async fn open_progressive_pull_on(
    warm: &WarmConnection,
    ctx: &PoolContext,
    ledger: Arc<PoolLedger>,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    deadlines: PullDeadlines,
    byte_len: u64,
    on_connect: Option<&DialObserver<'_>>,
) -> anyhow::Result<(UpstreamPullHeader, UpstreamPull)> {
    open_progressive_pull_impl(
        ConnSource::Reuse(warm.connection()),
        // Borrowed warm connection: leave it open for the next hash.
        false,
        ctx,
        ledger,
        slash_domain,
        expected_signer,
        hash,
        namespace_id,
        byte_offset,
        timestamp_us,
        max_blob_size_bytes,
        max_rate_per_mb,
        deadlines,
        byte_len,
        on_connect,
    )
    .await
}

/// The shared body behind [`open_progressive_pull`] (dial) and
/// [`open_progressive_pull_on`] (reuse). `owns_conn` records which one opened it,
/// so the returned [`UpstreamPull`] knows whether its terminal method closes the
/// connection (owned, one-shot) or leaves it open for the next hash (borrowed,
/// warm).
#[allow(clippy::too_many_arguments)]
async fn open_progressive_pull_impl(
    source: ConnSource<'_>,
    owns_conn: bool,
    ctx: &PoolContext,
    ledger: Arc<PoolLedger>,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    deadlines: PullDeadlines,
    // Upper bound on the requested range: `[byte_offset, byte_offset + byte_len)`.
    // `0` means "to end" (the pre-#1608 whole-tail behavior, unchanged for every
    // existing caller). A gap-driven caller (`source::PeerSource`, #1608) passes
    // the exact gap length so the server scopes both the serve and the payment
    // to it, rather than streaming the whole remainder.
    byte_len: u64,
    // Notified with a weak handle to the connection the instant it is dialled, so a
    // caller on a runtime it is about to drop can wait for that connection to reach
    // drained — including on the handshake failures below, which return with the
    // connection already live. `None` for a caller with no such hazard.
    on_connect: Option<&DialObserver<'_>>,
) -> anyhow::Result<(UpstreamPullHeader, UpstreamPull)> {
    let result: anyhow::Result<(UpstreamPullHeader, UpstreamPull)> = async {
        let window = deadlines.window;
        let floor_bps = deadlines.floor_bps;
        // TTFB boundary (#1906-series peer store): measured from immediately before
        // dial to the moment the signed `StreamResponse` verifies inside
        // `open_stream`, so it captures the real send-to-first-byte round trip a
        // probe cannot — a probe measures only its own tiny response, not the
        // paid-stream handshake this fetch actually pays for.
        let started = std::time::Instant::now();
        let (conn, send, recv, resp, resp_ext) = open_stream(
            source,
            ctx,
            slash_domain,
            expected_signer,
            hash,
            // Node-to-node pull. `NO_NAMESPACE` (0) for a DHT-discovered *holder* — it
            // already holds the bytes, so the downstream node needs no namespace (ADR
            // 002 §Retrieval by namespace). But a directory-discovered *cold origin* is
            // reached with the served request's namespace: it must fill from its own
            // backend, and its pull-through authorized-origin gate resolves on that
            // namespace (ADR 005 §Namespace routing). The caller passes whichever
            // applies.
            namespace_id,
            byte_offset,
            byte_len,
            timestamp_us,
            deadlines.open,
            on_connect,
        )
        .await?;
        if !resp.body.ok {
            return Err(UpstreamRefused::open(resp, &resp_ext));
        }
        // The peer's signed `total_bytes` never drives a refusal (#1895): it is
        // peer-controlled and unverified (`StreamResponse::validate()` does not bound
        // it), so the `max_blob_size_bytes` ceiling is enforced on the bytes that
        // ACTUALLY arrive, inside `UpstreamPull::next_chunk`, not on the claim. Refusing
        // on an inflated claim would let a holder centralise a small blob's traffic
        // across every finite-ceiling relay; a lie cannot produce bytes that verify
        // against the true root, and an honest giant is streamed and paid for only up
        // to one ceiling.
        // Buyer-side rate ceiling (#1375): refuse an over-ceiling quote before the
        // first paid interval, carrying the signed quote out as rate-manipulation
        // evidence. `0` = unbounded.
        if max_rate_per_mb > 0 && resp.body.rate_per_mb > max_rate_per_mb {
            return Err(RateAboveCeiling::over_ceiling(resp, max_rate_per_mb));
        }
        // A resume offset the blob cannot satisfy. `<=` rather than `<`: an offset
        // exactly AT the end has no chunk group to anchor either, and `align_range`
        // would reject it a few lines later with an untyped fault — this way both
        // land on the same typed sentinel. Guarded on `byte_offset > 0` so a 0-byte
        // blob fetched from 0 (#1054) is untouched.
        if resp.body.total_bytes <= byte_offset && byte_offset > 0 {
            return Err(anyhow::Error::new(ResumeOffsetPastEnd {
                total_bytes: resp.body.total_bytes,
                byte_offset,
            }));
        }

        let rate_per_mb = resp.body.rate_per_mb;
        // Wire-byte bound (bao-encoded size of the aligned range), not content bytes —
        // the window path forwards this stream verbatim and pays the upstream in wire
        // bytes (ADR 038 §Payment metering).
        let total_bytes = resp.body.total_bytes;
        let expected_wire_bytes = aligned_wire_len(byte_offset, byte_len, total_bytes)?;
        // Received-byte ceiling (#1895), expressed as a WIRE bound so `next_chunk` can
        // enforce it without decoding: the wire size of a ceiling-sized blob's content
        // from this offset. Enforced on the bytes that ACTUALLY arrive, never on the
        // peer's unverified `total_bytes` claim. A resume offset already at/past the
        // ceiling means the blob is genuinely oversized — abort before the first chunk.
        // `0` = unlimited.
        let max_received_wire = if max_blob_size_bytes == 0 {
            0
        } else if byte_offset >= max_blob_size_bytes {
            return Err(anyhow::Error::new(BlobTooLarge {
                received: byte_offset,
                ceiling: max_blob_size_bytes,
            }));
        } else {
            aligned_wire_len(byte_offset, 0, max_blob_size_bytes)?
        };
        let ttfb_ms = started.elapsed().as_secs_f64() * 1000.0;
        let header = UpstreamPullHeader {
            total_bytes,
            rate_per_mb,
            interval_bytes: CHUNK_BYTES,
            ttfb_ms,
        };
        let progress_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let floor = progress::ThroughputFloor::new(
            progress::FloorConfig { window, floor_bps },
            Arc::clone(&progress_counter),
            tokio::time::Instant::now(),
        );
        let mut sampler = tokio::time::interval(stall_sample_period(window));
        sampler.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let pull = UpstreamPull {
            window,
            progress_counter,
            floor,
            sampler,
            conn,
            owns_conn,
            send,
            recv,
            ctx: ctx.clone(),
            ledger,
            hash,
            rate_per_mb,
            meter: StreamMeter::default(),
            expected_wire_bytes,
            max_received_wire,
            cumulative: 0,
            unproved: 0,
            ended: false,
        };
        Ok((header, pull))
    }
    .await;
    record_open_result(&tracing::Span::current(), &result);
    result
}

impl UpstreamPull {
    /// Wire bytes this stream will deliver: the bao-encoded size of the
    /// chunk-group-aligned range (content plus interleaved proof, ADR 038). The
    /// window serve loop uses it as the pull budget `total` — it must be wire
    /// bytes, since `pulled`/`served_paid` count forwarded wire bytes.
    #[must_use]
    pub const fn expected_wire_bytes(&self) -> u64 {
        self.expected_wire_bytes
    }

    /// The watermark to PERSIST on any exit — including an error from `next_chunk`, and
    /// including a DROP (#852, #1122).
    ///
    /// Reads [`PoolLedger::settlement`], not a copied-back field. A field can only be
    /// updated on a path that RUNS, and this pull's does not always run: the serve loop
    /// drives it as a future on the `accept` task, which is dropped on shutdown or a
    /// downstream reset, and `settlement` additionally covers a voucher left in flight when
    /// that happened. See the ledger's docs.
    #[must_use]
    pub fn progress(&self) -> VoucherProgress {
        VoucherProgress::from_ledger(&self.ledger, self.ctx.prior_amount)
    }

    /// Send one voucher for `delta_bytes` newly delivered since the last voucher,
    /// through the CHANNEL's ledger — shared with every other concurrent pull on it, so
    /// their vouchers are serialized into strict cumulative-amount order rather than
    /// colliding. Sends optimistically (#1484): the ack is
    /// read back by [`Self::next_chunk`] / [`Self::finish`], not awaited here.
    async fn pay_one(&mut self, unproved: u64, complete: bool) -> anyhow::Result<u64> {
        let ledger = Arc::clone(&self.ledger);
        self.meter
            .pay(
                &mut self.send,
                &self.ctx,
                &ledger,
                self.rate_per_mb,
                unproved,
                complete,
            )
            .await
    }

    /// Read one message under the persistent throughput floor (#1797). Wraps `recv` in a
    /// `ProgressReader` feeding the shared counter, and races the read against
    /// the floor's sampling tick: a read slow enough to lose the race is judged, and a
    /// sub-floor window aborts with [`PullTimeout`] before the first byte (our own
    /// blob-size-dependent budget) or [`PullStalled`] after it — neither scores the peer.
    async fn read_under_floor(&mut self) -> anyhow::Result<ClientMessage> {
        let cumulative = self.cumulative;
        let window = self.window;
        let recv = &mut self.recv;
        let sampler = &mut self.sampler;
        let floor = &mut self.floor;
        let mut reader = progress::ProgressReader::new(recv, Arc::clone(&self.progress_counter));
        // Pin ONE read future and poll it across ticks. `read_client_message` is not
        // cancellation-safe — `read_frame` fills a frame with `read_exact`, so dropping the
        // future mid-frame loses the bytes already consumed and desynchronises the stream.
        // Recreating it per tick would corrupt every frame that spans a tick; the pinned
        // future is dropped only when the floor actually aborts (#1797). `biased` cannot
        // starve the floor: a ready read arm means a whole frame arrived, which is byte
        // progress the counter already holds, so a skipped tick only ever coincides with
        // throughput the floor would clear.
        let read = read_client_message(&mut reader);
        tokio::pin!(read);
        loop {
            tokio::select! {
                biased;
                r = &mut read => return r,
                _ = sampler.tick() => {
                    if let progress::FloorVerdict::Stalled =
                        floor.evaluate(tokio::time::Instant::now())
                    {
                        return Err(if cumulative == 0 {
                            anyhow::Error::new(PullTimeout { after: window })
                        } else {
                            anyhow::Error::new(PullStalled { after: window })
                        });
                    }
                }
            }
        }
    }

    /// Read the next `ChunkData`, paying the upstream at each voucher-interval
    /// boundary (and a closing voucher once all promised bytes have arrived),
    /// and return the chunk for the caller's verifying decoder (and, on the node's
    /// serve leg, to forward downstream).
    /// Returns `Ok(None)` on `StreamEnd`.
    ///
    /// # Errors
    ///
    /// More bytes than promised, received bytes crossing the buyer's size ceiling
    /// ([`BlobTooLarge`], raised before the crossing chunk is paid), a mid-stream `StreamError`
    /// (typed [`UpstreamRefused`]), an unexpected message, a [`UpstreamVoucherRejected`] /
    /// transport error while paying, or —
    /// on the throughput floor — [`PullStalled`] once bytes have flowed, or [`PullTimeout`] if
    /// the floor trips before the first byte (`cumulative == 0`). A malformed `ChunkData` is
    /// not raised here: an empty payload is rejected at decode by `ChunkData`'s
    /// `serde(try_from)` (#1088) and an oversized frame by the framing layer's
    /// `MAX_MESSAGE_SIZE` before it allocates, so both surface out of `read_client_message`.
    pub async fn next_chunk(&mut self) -> anyhow::Result<Option<Bytes>> {
        if self.ended {
            return Ok(None);
        }
        // Read exactly one message. In the pool model there is no positive ack to
        // consume (acceptance is implicit — continued delivery IS acceptance, ADR
        // 005), so every message either carries a chunk, ends the stream, or is a
        // mid-stream error/unexpected frame; none is skipped.
        {
            // Byte-progress bound (#1797): the read is judged by the persistent throughput
            // floor, which counts bytes off the QUIC stream sub-frame. A `ChunkData` carries
            // at least one byte (#1088 bans empty frames), so every frame is a unit of
            // progress. The floor's window spans the whole stream, not this call, and the
            // caller's downstream-forward time is not charged against it — the counter only
            // moves on bytes the reader pulls.
            let msg = self.read_under_floor().await?;
            match msg {
                ClientMessage::ChunkData(chunk) => {
                    // The payload is bounded on both sides by construction (#1088): the
                    // ceiling caps per-frame allocation, and the non-empty floor ties every
                    // frame to payload, so a peer cannot advance this loop with a run of
                    // empty frames that move neither `cumulative` nor the voucher
                    // accounting. This path has no belt-and-braces payload check behind
                    // that floor, and does not need one now the floor is structural.
                    let chunk_len = chunk.bytes().len() as u64;
                    self.cumulative = self.cumulative.saturating_add(chunk_len);
                    if self.cumulative > self.expected_wire_bytes {
                        anyhow::bail!(
                            "server sent {} bytes, more than the {} promised",
                            self.cumulative,
                            self.expected_wire_bytes
                        );
                    }
                    // Received-byte ceiling (#1895): enforce the size cap on the bytes
                    // that ACTUALLY arrive, never on the peer's unverified `total_bytes`
                    // claim. `max_received_wire` is the wire size of a ceiling-sized
                    // blob's content, so this is the wire form of "received content >
                    // ceiling" — distinct from the claim-derived `expected_wire_bytes`
                    // overrun guard above. Abort BEFORE paying for the chunk that
                    // crosses it, so the spend stays bounded to roughly one ceiling.
                    // `0` = unlimited.
                    if self.max_received_wire > 0 && self.cumulative > self.max_received_wire {
                        return Err(anyhow::Error::new(BlobTooLarge {
                            received: self.cumulative,
                            ceiling: self.max_received_wire,
                        }));
                    }
                    // No per-chunk hashing here: this stream's bytes are bao wire
                    // bytes the caller feeds to a verifying decoder — the cache's
                    // (`admit_bao_stream`), the ranged store's, or the in-memory
                    // test wrapper's — which checks every chunk group against the
                    // root as it lands (ADR 038). A reveal therefore pays for wire
                    // bytes that have been RECEIVED but not yet verified; the decoder
                    // aborts the pull at the first bad group, so what a lying peer
                    // can bill is bounded by one framed message plus one metering
                    // interval, never by its `total_bytes` claim.
                    self.unproved = self.unproved.saturating_add(chunk_len);
                    // One reveal per whole chunk, plus one closing signature for
                    // the residual once every promised byte has arrived. The
                    // sends commit optimistically; only a rejection comes back,
                    // on a later `next_chunk`/`finish` read.
                    let unproved = self.unproved;
                    let complete = self.cumulative >= self.expected_wire_bytes;
                    // Gate the floor across our own payment: while we owe the covering proof
                    // the upstream legitimately pauses (ADR 005 §Payment pacing), so that
                    // pause is self-inflicted, not a sender stall (#1797).
                    self.floor.pause(tokio::time::Instant::now());
                    let paid = self.pay_one(unproved, complete).await;
                    self.floor.resume(tokio::time::Instant::now());
                    match paid {
                        Ok(remaining) => self.unproved = remaining,
                        // A voucher write that failed at end-of-stream may only mean the
                        // node stopped our send after it finished (or is rejecting us):
                        // prefer the terminal signal it left to the opaque write failure.
                        // Boxed so this cold error-path future does not enlarge the steady
                        // receive loop's future (`clippy::large_futures`); the allocation
                        // only happens on the failure path. A confirmed voucher covers
                        // every byte this stream received, so nothing stays unproved.
                        Err(write_err) => {
                            if Box::pin(self.terminal_after_write_failure(write_err)).await? {
                                self.unproved = 0;
                            }
                        }
                    }
                    Ok(Some(Bytes::from(chunk.into_bytes())))
                }
                // A mid-stream `StreamError` is either a `VoucherRejected` (our
                // payment fault, with the self-heal bundle) or a refusal; both
                // surface as errors.
                ClientMessage::StreamError(e) => {
                    Err(voucher_rejection(&self.ledger, &self.meter, e))
                }
                ClientMessage::StreamEnd => {
                    self.ended = true;
                    Ok(None)
                }
                other => {
                    anyhow::bail!("unexpected message mid-delivery: {}", variant_name(&other))
                }
            }
        }
    }

    /// Decide what a voucher write failure really means.
    ///
    /// A node stops our send half only once it needs nothing more from us: a
    /// completed delivery leaves a `StreamEnd` on the wire, a mid-stream rejection a
    /// `StreamError`. The write half and the read half run in lock-step in the
    /// receive loop, so a raw voucher-write failure — the end-of-stream
    /// `STOP_SENDING(0)` a node emits when it finishes and drops `recv` — would
    /// otherwise mask that terminal signal and abort a complete, paid fetch (or
    /// swallow a typed rejection the reactive top-up path keys on). Read the terminal
    /// signal, briefly, and prefer it.
    ///
    /// A `StreamEnd` marks the stream ended: the chunk that triggered the write is
    /// still delivered, and the next read returns `None`. When the failed write was
    /// a voucher, the `StreamEnd` also confirms it. An honest node ends a stream
    /// only once every interval, the closing one included, is credited. So
    /// payment-based completion ([`driver::drive`]) sees this leg paid through its
    /// end and does not re-pull and re-bill the tail. Confirming costs nothing
    /// extra, because [`PoolLedger::settlement`] already reports the armed voucher.
    ///
    /// A `StreamError` surfaces as the typed rejection. A write we caused ourselves
    /// (a [`LocalPullFault`]) is never masked, nor is a stream that yields no
    /// terminal signal before the bound.
    ///
    /// Returns whether a voucher was confirmed. A `StreamEnd` after a failed reveal
    /// or re-anchor write, or after a sibling's voucher replaced this stream's
    /// armed one, ends the stream with nothing confirmed.
    async fn terminal_after_write_failure(
        &mut self,
        write_err: anyhow::Error,
    ) -> anyhow::Result<bool> {
        if write_err.is::<LocalPullFault>() {
            return Err(write_err);
        }
        // Any outcome other than a terminal message means nothing terminal was
        // waiting, so the write failure stands as the honest outcome (its typed
        // cause still downcasts through the added context); what the recovery saw
        // rides along so the log can tell a silent peer from a chatty one.
        match tokio::time::timeout(TERMINAL_AFTER_WRITE_TIMEOUT, self.read_under_floor()).await {
            Ok(Ok(ClientMessage::StreamEnd)) => {
                self.ended = true;
                let attempted = write_err
                    .downcast_ref::<UnconfirmedVoucher>()
                    .map(|voucher| voucher.amount);
                let confirmed = match attempted {
                    Some(amount) => self.ledger.confirm_armed_stamped(amount).await,
                    None => None,
                };
                if let Some((confirmed, generation)) = confirmed {
                    tracing::debug!(
                        amount = %confirmed.amount,
                        error = %format_args!("{write_err:#}"),
                        "upstream ended the stream after a voucher write failed; confirmed \
                         the voucher"
                    );
                    self.meter.last_proof = Some(StreamProof::Voucher {
                        amount: confirmed.amount,
                        generation,
                    });
                    self.meter.anchored_root = self.ledger.chain_root();
                    Ok(true)
                } else {
                    tracing::warn!(
                        attempted = ?attempted,
                        error = %format_args!("{write_err:#}"),
                        "upstream ended the stream after a proof write failed; nothing \
                         confirmed, so the leg's tail stays unpaid"
                    );
                    Ok(false)
                }
            }
            Ok(Ok(ClientMessage::StreamError(e))) => {
                Err(voucher_rejection(&self.ledger, &self.meter, e))
            }
            Ok(Ok(other)) => Err(write_err.context(format!(
                "no terminal signal after the voucher write failed; peer sent {}",
                variant_name(&other)
            ))),
            Ok(Err(read_err)) => Err(write_err.context(format!(
                "no terminal signal after the voucher write failed; recovery read failed: \
                 {read_err:#}"
            ))),
            Err(_) => Err(write_err.context(format!(
                "no terminal signal within {TERMINAL_AFTER_WRITE_TIMEOUT:?} of the voucher \
                 write failing"
            ))),
        }
    }

    /// Finalize a completed pull: drain to `StreamEnd` if needed, enforce
    /// wire-byte completeness (the full promised bao wire size was received),
    /// close the connection cleanly, and return the final acked watermark to
    /// persist. Per ADR 038 this does not re-hash the whole blob — bao
    /// verification is the caller's decoder's job, group by group as the bytes
    /// land.
    ///
    /// # Errors
    ///
    /// A short delivery (fewer wire bytes than promised before `StreamEnd`), a stream/protocol
    /// error while draining, or — on the same inactivity bound as `next_chunk` — [`PullStalled`]
    /// if the upstream goes silent before `StreamEnd`.
    pub async fn finish(mut self) -> anyhow::Result<VoucherProgress> {
        while !self.ended {
            // Same byte-progress bound as `next_chunk` (#1797): an upstream that never sends
            // its `StreamEnd` must not hold the drain open forever.
            let msg = self.read_under_floor().await?;
            match msg {
                ClientMessage::StreamEnd => self.ended = true,
                ClientMessage::ChunkData(_) => {
                    anyhow::bail!("server sent ChunkData after the promised total")
                }
                // The closing voucher was committed optimistically when it was sent
                // (implicit acceptance, ADR 005), so the drain only needs to watch
                // for a late `VoucherRejected` / other `StreamError`, which still
                // surfaces as an error.
                ClientMessage::StreamError(e) => {
                    return Err(voucher_rejection(&self.ledger, &self.meter, e));
                }
                other => {
                    anyhow::bail!("unexpected message at stream end: {}", variant_name(&other))
                }
            }
        }
        // Completeness for every fetch (full and resumed): bao verification is
        // the caller's decoder's job, group by group as the bytes land, so
        // `finish` does not re-hash the whole blob. A truncated stream (fewer
        // wire bytes than promised) can't be decoded, so require the full
        // promised wire size as the completeness signal.
        if self.cumulative < self.expected_wire_bytes {
            self.close_transport(0, b"short-delivery");
            anyhow::bail!(
                "server sent {} of {} promised wire bytes before StreamEnd",
                self.cumulative,
                self.expected_wire_bytes
            );
        }
        self.close_transport(0, b"done");
        Ok(self.progress())
    }

    /// Abandon the pull (e.g. the downstream client dropped, so we stop pulling
    /// and paying). Closes the connection and returns the acked watermark so the
    /// caller can still persist what it paid (#852).
    #[must_use]
    pub fn abort(mut self) -> VoucherProgress {
        self.close_transport(0, b"client-abandoned");
        self.progress()
    }

    /// Tear down this pull's transport on any exit.
    ///
    /// An OWNED connection (a one-shot dial) is closed, which ends the QUIC
    /// connection so the upstream's serve task stops and the paid stream does not
    /// linger half-open. A BORROWED connection (a [`WarmConnection`] reused across
    /// hashes) is left open for the next hash — only THIS stream is torn down: the
    /// send half is finished (a clean FIN) and the recv half is stopped. The warm
    /// connection's own `Drop` closes the connection once, later.
    ///
    /// `Connection::close`, `SendStream::finish`, and `RecvStream::stop` are all
    /// first-wins / idempotent, so an explicit terminal method (`finish`/`abort`)
    /// keeps its richer reason and the `Drop` safety net becomes a no-op.
    fn close_transport(&mut self, code: u32, reason: &[u8]) {
        if self.owns_conn {
            self.conn.close(code.into(), reason);
        } else {
            let _ = self.send.finish();
            let _ = self.recv.stop(code.into());
        }
    }
}

impl Drop for UpstreamPull {
    /// Safety net for the "call a terminal method on every exit" contract: if a
    /// caller returns or panics without `finish`/`abort`, still tear this pull's
    /// transport down through `close_transport` so the QUIC stream and the
    /// upstream's server-side serve task don't linger and keep that paid stream
    /// half-open. `Connection::close` / `SendStream::finish` / `RecvStream::stop`
    /// are all first-wins and idempotent, so an explicit teardown in
    /// `finish`/`abort` keeps its richer reason and this is a no-op when one of
    /// them ran; it only takes effect on a dropped-without-finalize path.
    ///
    /// It tears down the transport and nothing else, and that is sufficient: the
    /// acked watermark lives in the channel's [`PoolLedger`], which OUTLIVES the
    /// pull (it is shared with the other pulls on the channel), not in a field of
    /// this struct. A dropped pull's caller reads it with [`PoolLedger::settlement`]
    /// and persists it — `node_origin` does exactly that from its own `Drop` guard
    /// (#1145 review).
    fn drop(&mut self) {
        self.close_transport(0, b"upstream-pull-dropped");
    }
}

/// Issue one cumulative voucher for `delta_bytes` newly delivered since the last
/// voucher and SEND it. Acceptance is implicit (ADR 005): continued delivery IS
/// acceptance, so there is no ack to wait for — the send itself commits.
///
/// Voucher issuance runs through the lane's [`PoolLedger`], which serializes the
/// compute → sign → send critical section across every concurrent stream drawing
/// on the lane — so vouchers reach the node in strict cumulative order — and
/// releases its issuance lock the instant the send returns. Parallel range
/// streams to one provider do not serialize their payments behind each other's
/// round trips, because there is no round trip: only a rejection comes back, and
/// it arrives as its own mid-stream `StreamError`.
///
/// Each voucher's own *delta* (`ceil(delta_bytes * rate / 1 MiB)`) covers its own
/// bytes at the advertised rate (the node checks deltas, not the rounded
/// cumulative). A successful send advances the committed watermark optimistically;
/// an ambiguous send failure leaves the voucher armed so [`PoolLedger::settlement`]
/// still reports it (settle high — the upstream persists a voucher before it would
/// reject it, ADR 003). The client never pays ahead of what it received (vouchers
/// are cumulative over delivered bytes).
///
/// Returns the sent voucher as the [`StreamProof`] the stream records, stamped
/// with the ledger generation it was signed under.
async fn send_voucher(
    send: &mut SendStream,
    ctx: &PoolContext,
    ledger: &PoolLedger,
    rate_per_mb: u64,
    delta_bytes: u64,
    epoch: EpochAction,
) -> anyhow::Result<StreamProof> {
    if ctx.provider.is_zero() {
        return Err(anyhow::anyhow!(
            "voucher provider is not pinned (Address::ZERO) — call PoolContext::with_provider \
             before signing"
        )
        .context(LocalPullFault));
    }
    let mut attempted = None;
    let issued = ledger
        .issue_stamped(delta_bytes, rate_per_mb, epoch, |next, chain| {
            attempted = Some(next.amount);
            sign_and_write_voucher(send, ctx, next, chain)
        })
        .await;
    issued
        .map(|(next, generation)| StreamProof::Voucher {
            amount: next.amount,
            generation,
        })
        .map_err(|err| match attempted {
            Some(amount) => err.context(UnconfirmedVoucher { amount }),
            None => err,
        })
}

/// Names the voucher a failed [`send_voucher`] left armed, so a terminal
/// `StreamEnd` read afterwards confirms that voucher and no other
/// ([`PoolLedger::confirm_armed_stamped`]). Carried as error context: it composes with,
/// and still downcasts through, the send error it wraps.
#[derive(Debug)]
struct UnconfirmedVoucher {
    /// The cumulative amount the armed voucher signs.
    amount: U256,
}

impl std::fmt::Display for UnconfirmedVoucher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "voucher for cumulative amount {} did not confirm",
            self.amount
        )
    }
}

impl std::error::Error for UnconfirmedVoucher {}

/// Sign one voucher over `next`/`chain` with the pool context's key and write it
/// to `send` — the body both [`send_voucher`] and [`PoolLedger::reanchor`] hand to
/// the ledger, so the two differ only in which cumulative the ledger hands back.
async fn sign_and_write_voucher(
    send: &mut SendStream,
    ctx: &PoolContext,
    next: Cumulative,
    chain: ChainCommit,
) -> anyhow::Result<()> {
    let signed = Voucher {
        pool_id: ctx.pool_id,
        signer: ctx.client_signer.address(),
        provider: ctx.provider,
        amount: next.amount,
        bytes_delivered: next.bytes,
        chain_root: chain.chain_root,
        chunk_price: chain.chunk_price,
    }
    .sign(ctx.client_signer.as_ref(), &ctx.voucher_domain)
    .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}").context(LocalPullFault))?;
    let wire_voucher = signed_to_wire_voucher(&signed)
        .map_err(|e| anyhow::anyhow!("voucher exceeds wire width: {e}").context(LocalPullFault))?;
    write_message(send, &ClientMessage::Voucher(wire_voucher)).await
}

/// One stream's view of the lane's hash chain (ADR 003 §Concurrent Streams).
///
/// The chain's scope is the **lane**, not the stream: every stream from one
/// signer to one node shares a single root and a single index, because a
/// released preimage advances the lane and cannot be attributed to whichever
/// stream happened to carry it. What IS per-stream is the *anchor* — which
/// epoch this stream has told the node about.
///
/// That distinction is the whole reason this type exists. A bare preimage names
/// no chain, so the node places it against the anchor of the stream it arrived
/// on. If a stream released a reveal before carrying that epoch's root voucher,
/// the node could not name the chain the reveal belongs to and would reject it
/// `UnanchoredPreimage`. So each stream re-anchors itself whenever the lane
/// rolls, rather than trusting a sibling to have done it — QUIC orders within a
/// stream, so a fast stream that has already adopted a new root can never
/// invalidate a slower sibling still finishing the old one.
#[derive(Debug, Default)]
struct StreamMeter {
    /// The chain this stream has already carried the root voucher for, named by
    /// its root. `None` until it has anchored to anything.
    anchored_root: Option<B256>,
    /// The last proof THIS stream claimed money with, so a `VoucherRejected`
    /// read here rewinds that proof and not whatever a sibling stream has done
    /// on the lane since (see [`PoolLedger::resolve_reject`]). `None` before the
    /// stream has claimed anything, and left alone by a re-anchor — which claims
    /// nothing and so has nothing to rewind.
    last_proof: Option<StreamProof>,
}

impl StreamMeter {
    /// Ensure this stream has told the node which chain its reveals belong to,
    /// opening a chain if the lane meters none, and sending that chain's root
    /// voucher first if this stream has not carried it.
    ///
    /// Called from one place — a stream about to release a reveal, which is the
    /// only thing that needs a chain to exist. Residual settlement commits to
    /// whatever the lane already meters against and goes straight to
    /// [`send_voucher`].
    async fn anchor(
        &mut self,
        send: &mut SendStream,
        ctx: &PoolContext,
        ledger: &PoolLedger,
        rate_per_mb: u64,
    ) -> anyhow::Result<()> {
        if ledger.chain_root().is_none() {
            self.last_proof =
                Some(send_voucher(send, ctx, ledger, rate_per_mb, 0, EpochAction::Open).await?);
            // Re-read rather than reuse the pre-send `None`: the send is what
            // opened the chain, so only the ledger knows which one.
            self.anchored_root = ledger.chain_root();
            return Ok(());
        }
        if ledger.chain_root().is_some() && self.anchored_root != ledger.chain_root() {
            // A sibling rolled the lane, or this is our first reveal on a chain a
            // sibling opened. Either way this stream must name the chain before it
            // can reveal against it (ADR 003 §Concurrent Streams, Rule 1).
            //
            // This must NOT go through `send_voucher`: every voucher the ledger
            // issues folds the chain's accrual, and a voucher that folds must also
            // roll — so re-anchoring that way would retire the chain it meant to
            // join, strand every sibling's in-flight reveals under the retired
            // root, and buy a signature plus a fresh keccak ladder each time.
            // `reanchor` re-states the signed anchor under the live root instead,
            // folding nothing.
            //
            // Take the root `reanchor` reports, not one read before the send: the
            // lane's live chain is only knowable while the issuance lock is held,
            // and storing a stale root would leave this stream re-anchoring on
            // every chunk.
            self.anchored_root = ledger
                .reanchor(|anchor, chain| sign_and_write_voucher(send, ctx, anchor, chain))
                .await?;
        }
        Ok(())
    }

    /// Meter one delivered chunk: release the next preimage on this stream,
    /// rolling the chain first if the epoch is spent.
    ///
    /// The reveal costs one hash and one 33-byte message — no signature, no
    /// acknowledgement, and nothing durable to write — which is the whole point
    /// of the chain. A rollover costs one signature, and happens once per 255
    /// chunks rather than once per chunk.
    async fn release_chunk(
        &mut self,
        send: &mut SendStream,
        ctx: &PoolContext,
        ledger: &PoolLedger,
        rate_per_mb: u64,
    ) -> anyhow::Result<()> {
        // Two attempts, never more: the first can find the epoch spent, and the
        // roll that follows opens a chain with index 1 available by
        // construction. A third pass would mean the ledger contradicted itself.
        for attempt in 0..2u8 {
            self.anchor(&mut *send, ctx, ledger, rate_per_mb).await?;
            // Reborrow per iteration: the closure takes the stream by unique
            // reference for the duration of the send, and the loop needs it back.
            let wire = &mut *send;
            match ledger
                .meter(|released: Released| async move {
                    write_message(
                        wire,
                        &ClientMessage::ChunkPreimage(ChunkPreimage {
                            preimage: released.preimage.into(),
                            index: released.index,
                        }),
                    )
                    .await
                })
                .await?
            {
                Metered::Released(released) => {
                    self.last_proof = Some(StreamProof::Reveal {
                        chain_root: released.chain_root,
                        index: released.index,
                    });
                    return Ok(());
                }
                Metered::Exhausted if attempt == 0 => {
                    // Roll. The new voucher's `amount` already folds this
                    // chain's frontier — every reveal advanced the committed
                    // cumulative as it went — so the fold is the frontier
                    // actually reached, never a flat 255 (ADR 003 §Rollover).
                    self.last_proof = Some(
                        send_voucher(&mut *send, ctx, ledger, rate_per_mb, 0, EpochAction::Roll)
                            .await?,
                    );
                    self.anchored_root = ledger.chain_root();
                }
                Metered::Exhausted => {
                    // A ledger self-contradiction, not a peer fault: marked as OURS so
                    // it is scored as a local fault and never rescued by the
                    // voucher-write recovery into a clean completion.
                    return Err(
                        anyhow::anyhow!("hash chain still exhausted after a rollover")
                            .context(LocalPullFault),
                    );
                }
            }
        }
        Ok(())
    }

    /// Settle a residual smaller than one chunk with a signed voucher.
    ///
    /// A preimage always advances the claim by a *whole* chunk, so a partial
    /// trailing chunk cannot be priced by one — it settles through a signature,
    /// exact to one token base unit.
    ///
    /// It asks for [`EpochAction::Keep`], and usually does not get it. Any
    /// transfer that released at least one reveal has accrual outstanding, and a
    /// voucher that folds must also roll ([`PoolLedger::issue`]) — so the typical
    /// close retires the chain and opens a fresh one, and a sibling stream's
    /// in-flight reveals under the old root land as superseded and re-anchor.
    /// That is the correct trade and not an oversight: the fold is what makes the
    /// residual exact, and a folded frontier must never coexist with the root
    /// that proved it. `Keep` is what a transfer smaller than one whole chunk
    /// gets — nothing accrued, nothing to fold — and there it commits the sealed
    /// section, since such a lane never opened a chain at all.
    async fn settle_residual(
        &mut self,
        send: &mut SendStream,
        ctx: &PoolContext,
        ledger: &PoolLedger,
        rate_per_mb: u64,
        residual_bytes: u64,
    ) -> anyhow::Result<()> {
        self.last_proof = Some(
            send_voucher(
                send,
                ctx,
                ledger,
                rate_per_mb,
                residual_bytes,
                EpochAction::Keep,
            )
            .await?,
        );
        self.anchored_root = ledger.chain_root();
        Ok(())
    }

    /// Emit the proofs `delivered` new bytes have earned: one reveal per whole
    /// chunk, plus a closing signature for any residual once the transfer is
    /// complete.
    ///
    /// Returns the bytes still unproved — always below one chunk.
    async fn pay(
        &mut self,
        send: &mut SendStream,
        ctx: &PoolContext,
        ledger: &PoolLedger,
        rate_per_mb: u64,
        mut unproved: u64,
        complete: bool,
    ) -> anyhow::Result<u64> {
        while unproved >= CHUNK_BYTES {
            self.release_chunk(send, ctx, ledger, rate_per_mb).await?;
            unproved -= CHUNK_BYTES;
        }
        if complete && unproved > 0 {
            self.settle_residual(send, ctx, ledger, rate_per_mb, unproved)
                .await?;
            unproved = 0;
        }
        Ok(unproved)
    }
}

/// Turn a mid-stream `StreamError` read off the receive loop into the typed error
/// the loop surfaces. There is no positive ack in the pool model — continued
/// delivery is acceptance (ADR 005) — so this handles only the rejection slot.
///
/// A `VoucherRejected` is OUR payment-side fault: rewind the proof `meter` last sent
/// (known-not-taken, so it must not be settled optimistically) and carry its typed
/// reason — plus the wallet-less-resume `bundle` (#1481) — so the orchestrator can
/// exonerate the provider (#857) and the reseed loops (`driver::fill_gap`, the
/// `test-util` `fetch_inner`) can self-heal from an authenticated watermark
/// instead of treating the rejection as terminal. Any
/// OTHER `StreamError` is the upstream refusing mid-stream; carry the typed wire
/// code as [`UpstreamRefused`] so an honest `Overloaded`/`NotFound` peer is scored
/// on its real code rather than the `Unreachable` catch-all (#1145 review).
fn voucher_rejection(
    ledger: &PoolLedger,
    meter: &StreamMeter,
    error: StreamError,
) -> anyhow::Error {
    match error {
        StreamError::VoucherRejected { reason, bundle } => {
            // Rewind the proof THIS stream sent. The lane is shared, so the proof
            // it last issued may belong to a sibling; naming ours is what keeps
            // the rejection from un-committing a voucher the node accepted.
            if let Some(proof) = meter.last_proof {
                ledger.resolve_reject(proof);
            }
            let proof_generation = match meter.last_proof {
                Some(StreamProof::Voucher { generation, .. }) => Some(generation),
                Some(StreamProof::Reveal { .. }) | None => None,
            };
            anyhow::Error::new(UpstreamVoucherRejected {
                reason,
                bundle,
                proof_generation,
            })
        }
        other => anyhow::Error::new(UpstreamRefused::mid_stream(other)),
    }
}

/// Validate + verify a `StreamResponse` on receive (ADR 005, ADR 014 §1, #252).
fn verify_response(
    resp: &StreamResponse,
    ext: &StreamResponseExt,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    pool_id: B256,
    timestamp_us: u64,
) -> anyhow::Result<()> {
    // #252 + slash_sig length/upper-bound checks on the frozen base...
    resp.validate()
        .map_err(|e| anyhow::anyhow!("invalid stream response: {e}"))?;
    // ...and the ok/error agreement, which spans the signed base and the unsigned
    // extension, so neither validator can see it alone (ADR 013 §Tier 1).
    ext.validate(resp.body.ok)
        .map_err(|e| anyhow::anyhow!("invalid stream response: {e}"))?;
    // Echoed-field correlation (ADR 005).
    if resp.body.hash != hash {
        anyhow::bail!("response hash does not match request");
    }
    if resp.body.pool_id != pool_id.as_slice() {
        anyhow::bail!("response pool_id does not match request");
    }
    if resp.body.timestamp_us != timestamp_us {
        anyhow::bail!("response timestamp_us not echoed");
    }
    // slash_sig must recover to the delivering node's Ethereum address.
    let sig = Signature::try_from(resp.slash_sig.as_slice())
        .map_err(|e| anyhow::anyhow!("slash_sig parse: {e}"))?;
    StreamSlashData::from_response_body(&resp.body)
        .verify_signer(&sig, expected_signer, slash_domain)
        .map_err(|e| anyhow::anyhow!("slash_sig verification failed: {e}"))?;
    Ok(())
}

async fn write_message(send: &mut SendStream, msg: &ClientMessage) -> anyhow::Result<()> {
    // The encode is ours; the write below is the peer's connection.
    let payload = encode_message(msg)
        .map_err(|e| anyhow::anyhow!("encode failed: {e}").context(LocalPullFault))?;
    // A mid-stream voucher write, past the open handshake: `APP_ERR_RATE_LIMITED`
    // (`0x10`) is only ever sent before any application stream exists (ADR 013
    // §Application Error Codes), so it cannot reach this site. Keep the plain text
    // rather than route through `transport_error` — typing a shed here would trust
    // that invariant instead of scoping the recovery to the open-stage sites.
    write_frame(send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write failed: {e}"))
}

/// Read the open-stage `StreamResponse` together with its trailing
/// [`StreamResponseExt`] (ADR 013 §Tier 1, two-phase).
///
/// Separate from [`read_client_message`] because only this message carries an
/// extension: every mid-stream variant is a single postcard value, and widening
/// the shared reader would push an always-`default()` ext onto all of them.
async fn read_stream_response(
    recv: &mut RecvStream,
) -> anyhow::Result<(StreamResponse, StreamResponseExt)> {
    let frame = read_frame(recv)
        .await
        .map_err(|e| rate_limited::transport_error("frame read failed", e))?;
    let (msg, tail) = decode_message::<ClientMessage>(&frame)
        .map_err(|e| anyhow::anyhow!("decode failed: {e}"))?;
    let ClientMessage::StreamResponse(response) = msg else {
        anyhow::bail!("expected StreamResponse, got {}", variant_name(&msg));
    };
    let ext = decdn_protocol::parse_stream_response_ext(tail)
        .map_err(|e| anyhow::anyhow!("decode stream response ext: {e}"))?;
    Ok((response, ext))
}

async fn read_client_message<R: tokio::io::AsyncRead + Unpin>(
    recv: &mut R,
) -> anyhow::Result<ClientMessage> {
    // A mid-stream read, past the open handshake. `0x10` reaches only the
    // open-stage sites (`connect` / `open_bi` / the initial request write and
    // [`read_stream_response`]), never here — the limiters shed before any stream
    // exists (ADR 013 §Application Error Codes). Plain text, not `transport_error`.
    let frame = read_frame(recv)
        .await
        .map_err(|e| anyhow::anyhow!("frame read failed: {e}"))?;
    let (msg, _rest) = decode_message::<ClientMessage>(&frame)
        .map_err(|e| anyhow::anyhow!("decode failed: {e}"))?;
    Ok(msg)
}

const fn variant_name(msg: &ClientMessage) -> &'static str {
    match msg {
        ClientMessage::StreamRequest(_) => "StreamRequest",
        ClientMessage::StreamResponse(_) => "StreamResponse",
        ClientMessage::ChunkData(_) => "ChunkData",
        ClientMessage::Voucher(_) => "Voucher",
        ClientMessage::ChunkPreimage(_) => "ChunkPreimage",
        ClientMessage::StreamEnd => "StreamEnd",
        ClientMessage::StreamError(_) => "StreamError",
    }
}

/// Record a failed [`open_progressive_pull`] on its span once: the error text
/// and an error status, so a failed dial or handshake is not exported as a
/// slow success.
fn record_open_result<T>(span: &tracing::Span, result: &anyhow::Result<T>) {
    if let Err(e) = result {
        span.record("error", tracing::field::display(format_args!("{e:#}")));
        span.record("otel.status_code", "ERROR");
    }
}

#[cfg(test)]
mod tests {
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use decdn_bao_range::{IROH_BLOCK_SIZE, align_range, encode_verified_range};

    use bytes::Bytes;

    use super::{
        Cumulative, HashMismatch, Healed, LocalPullFault, PoolContext, PoolLedger, U256,
        UpstreamVoucherRejected, Voucher, VoucherRejectReason, WatermarkBundle, aligned_wire_len,
        decode_to_vec, genuine_exhaustion, heal_watermark_desync, rejection_watermark,
        resumable_watermark,
    };

    /// The `LocalPullFault` marker must ride out on the errors the range helpers ACTUALLY
    /// raise — not on one a test hand-built (#1145 review).
    ///
    /// This distinction is the whole point of the test: a test that calls the real
    /// `sign_client_binding`, throws away its `Ok` result, and hand-builds
    /// `anyhow!("...").context(LocalPullFault)` before asserting the ladder finds
    /// `LocalPullFault` in it is true by construction. Attaching the marker and then
    /// finding it proves nothing — such a test cannot fail even if every
    /// `.context(LocalPullFault)` call site is stripped from the crate, so a synthetic
    /// `anyhow!(...)` passes it while production silently fails to score the peer.
    ///
    /// So: real functions, real errors, marker never touched by the test. `align_range`
    /// rejects an offset at or past the end of the blob (never clamps — ADR 005), which is
    /// the one local-fault trigger reachable without mocking a signer, and
    /// `aligned_wire_len` is the one site every pull passes it through.
    ///
    /// The stakes, and why an unguarded marker here is not cosmetic: every arm BELOW
    /// `LocalPullFault` in the ladder blames the peer to some degree, and the catch-all
    /// scores `Unreachable` in the local per-peer EWMA (ADR 008). A fault in this
    /// node is not evidence about a provider, and a node in this state meets every candidate
    /// in turn, so losing the marker does not mis-score one peer: it defames the whole
    /// candidate list on the strength of our own defect.
    /// `capped` must refuse a cap that cannot outlast its own stages (#1145 review).
    ///
    /// This is the enforcement of record. The CLI's `ClientFetchArgs::validate` restates the
    /// rule as `timeout > 2 × stall` to give the user an early error in their own flags, but
    /// that `2 ×` is only correct because both call sites set the open bound from the stall
    /// knob — an assumption the compiler does not hold and an `--open-timeout-ms` flag would
    /// break. Here the three values are all in hand, so the real relation can be checked, and
    /// a `PullDeadlines` whose stall bound could never fire simply cannot be constructed.
    #[test]
    fn a_cap_that_cannot_outlast_its_stages_is_refused() {
        use super::{DeadlineError, PullDeadlines};
        use std::time::Duration;

        let open = Duration::from_secs(5);
        let window = Duration::from_secs(5);
        let floor = 4096;

        // At and below `open + window` the cap always wins the race, so the throughput floor
        // — the signal that a healthy stream is still making progress — could never fire.
        for cap in [Duration::from_secs(1), window, open + window] {
            assert!(
                matches!(
                    PullDeadlines::capped(open, window, floor, cap),
                    Err(DeadlineError::CapCannotOutlastItsStages { .. })
                ),
                "a cap of {cap:?} against open {open:?} + window {window:?} leaves the floor \
                 unable to fire, and must not be constructible"
            );
        }

        // One tick past it, the throughput floor can actually fire.
        assert!(
            PullDeadlines::capped(
                open,
                window,
                floor,
                open + window + Duration::from_millis(1)
            )
            .is_ok(),
            "past open + window the floor can fire, so this is a legitimate pull"
        );

        // A zero budget elapses on its first poll: the stage it bounds can never run.
        assert!(matches!(
            PullDeadlines::capped(Duration::ZERO, window, floor, Duration::from_mins(1)),
            Err(DeadlineError::ZeroBudget)
        ));
        assert!(matches!(
            PullDeadlines::capped(open, Duration::ZERO, floor, Duration::from_mins(1)),
            Err(DeadlineError::ZeroBudget)
        ));
    }

    /// `new` must refuse a zero budget too — and it is the constructor that MATTERS, because
    /// it is the one every production pull takes (#1145 review).
    ///
    /// A zero `window` makes the throughput floor demand progress over no time at all, so the
    /// streaming stage can never satisfy it and every honest read aborts on its first poll.
    /// The floor rate itself may legitimately be zero — that is idle-detection mode (one byte
    /// per window) — so only the durations are checked.
    ///
    /// The invariant belongs to the type, not to a resolver in another crate that a caller
    /// has to remember to run.
    #[test]
    fn new_refuses_a_zero_budget_on_the_path_every_production_pull_takes() {
        use super::{DeadlineError, PullDeadlines};
        use std::time::Duration;

        assert!(
            matches!(
                PullDeadlines::new(Duration::ZERO, Duration::from_secs(20), 4096),
                Err(DeadlineError::ZeroBudget)
            ),
            "a zero open bound means the open stage can never complete"
        );
        assert!(
            matches!(
                PullDeadlines::new(Duration::from_secs(20), Duration::ZERO, 4096),
                Err(DeadlineError::ZeroBudget)
            ),
            "a zero window makes the throughput floor unsatisfiable on every read"
        );
        assert!(PullDeadlines::new(Duration::from_secs(20), Duration::from_secs(20), 4096).is_ok());
        assert!(
            PullDeadlines::new(Duration::from_secs(20), Duration::from_secs(20), 0).is_ok(),
            "a zero floor rate is idle-detection mode, not an invalid budget"
        );
    }

    /// `aligned_wire_len`'s new `byte_len` parameter must actually bound the
    /// quoted wire cost — not just be accepted and ignored. This is the
    /// construction-level proof that `open_progressive_pull`'s `byte_len`
    /// threads all the way to the wire-byte bound `PeerSource`'s callers price
    /// vouchers from (#1608): a middle-gap request must quote strictly less
    /// than the whole tail, and must match `align_range`'s own `wire_len` for the
    /// identical bounded span so the two can never drift.
    #[test]
    fn aligned_wire_len_is_bounded_by_the_requested_byte_len() {
        let total = 10 * decdn_bao_range::CHUNK_GROUP_BYTES;
        let whole_tail = aligned_wire_len(0, 0, total).unwrap_or(0);
        let one_group = aligned_wire_len(0, decdn_bao_range::CHUNK_GROUP_BYTES, total).unwrap_or(0);
        assert!(
            one_group > 0 && one_group < whole_tail,
            "a one-group byte_len must quote less than the whole 10-group tail: \
             one_group={one_group}, whole_tail={whole_tail}"
        );
        let via_align_range =
            align_range(0, decdn_bao_range::CHUNK_GROUP_BYTES, total).map_or(0, |r| r.wire_len());
        assert_eq!(
            one_group, via_align_range,
            "aligned_wire_len must reproduce align_range's own wire_len for the same \
             bounded span, or the two can silently drift"
        );
    }

    #[test]
    fn the_range_helpers_mark_their_own_faults_as_local() {
        // A 4 KiB blob cannot be resumed from byte 8192 — `align_range` errors rather than
        // clamping (ADR 005), and the caller must own that as OURS. The assertion covers
        // both halves at once: `None` here means the call wrongly SUCCEEDED, and a `Some`
        // without the marker means it failed and blamed the peer.
        let aligned = aligned_wire_len(8192, 0, 4096).err();
        assert!(
            aligned
                .as_ref()
                .is_some_and(|e| e.downcast_ref::<LocalPullFault>().is_some()),
            "aligned_wire_len must reject an out-of-range offset and mark it OUR fault; \
             without the marker it falls through every downcast to the catch-all and \
             scores the peer as unreachable. Got: {aligned:?}"
        );
    }

    /// `client_binding_ext` maps an unbound context to `None` (so
    /// `encode_stream_request` appends no ext bytes — byte-for-byte the pre-#1115
    /// wire) and a bound one to `Some` carrying exactly the binding at the default
    /// voucher cadence. This is the mapping the single request site
    /// (`open_stream`) relies on, so it guards a refactor that would silently
    /// drop the ext (#1115).
    #[test]
    fn client_binding_ext_reflects_binding_presence() -> anyhow::Result<()> {
        use std::sync::Arc;

        use alloy::primitives::{Address, B256, U256};
        use alloy::signers::local::PrivateKeySigner;

        use super::{PoolContext, client_binding_ext, sign_client_binding};

        let signer = PrivateKeySigner::random();
        let domain = decdn_incentive::bind_node_id_domain(1, Address::ZERO);
        let ctx = PoolContext {
            pool_id: B256::ZERO,
            provider: Address::ZERO,
            deposit: U256::ZERO,
            client_signer: Arc::new(signer.clone()),
            voucher_domain: domain.clone(),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        };
        // Unbound ⇒ no ext.
        anyhow::ensure!(
            client_binding_ext(&ctx).is_none(),
            "unbound ctx must yield no ext"
        );

        // Bound ⇒ ext carries exactly the binding.
        let binding = sign_client_binding(&signer, B256::repeat_byte(0xAB), &domain)?;
        let ctx = ctx.with_client_binding(binding.clone());
        let ext = client_binding_ext(&ctx)
            .ok_or_else(|| anyhow::anyhow!("bound ctx must yield an ext"))?;
        anyhow::ensure!(
            ext.binding == Some(binding),
            "ext must carry the exact binding"
        );
        anyhow::ensure!(
            ext.capability.is_none(),
            "no capability attached ⇒ ext must carry none"
        );

        // Capability attached ⇒ ext carries the wire-mapped capability, alongside
        // the still-present binding — the two fields are independent.
        let owner = PrivateKeySigner::random();
        let capability = decdn_incentive::Capability {
            signer: signer.address(),
            spending_cap: 10_000_000u64,
            pool_id: B256::ZERO,
            expiry: 1_900_000_000,
        }
        .sign(&owner, &domain)?;
        let ctx = ctx.with_capability(capability.clone());
        let ext = client_binding_ext(&ctx)
            .ok_or_else(|| anyhow::anyhow!("ctx with capability must yield an ext"))?;
        let wire_cap = ext
            .capability
            .ok_or_else(|| anyhow::anyhow!("ext must carry the capability"))?;
        anyhow::ensure!(
            wire_cap.spending_cap == capability.capability.spending_cap,
            "spending_cap must round-trip to wire form"
        );
        anyhow::ensure!(
            wire_cap.expiry == capability.capability.expiry,
            "expiry must round-trip unchanged"
        );
        anyhow::ensure!(
            wire_cap.owner_signature == capability.signature.as_bytes().to_vec(),
            "owner_signature must round-trip to wire bytes"
        );
        Ok(())
    }

    /// Build a test [`PoolContext`] signing with `signer` over the
    /// `(pool_id, provider)` lane, sharing the shape
    /// `client_binding_ext_reflects_binding_presence` already uses.
    fn resume_test_ctx(
        pool_id: alloy::primitives::B256,
        provider: alloy::primitives::Address,
        signer: &std::sync::Arc<alloy::signers::local::PrivateKeySigner>,
        domain: &alloy::dyn_abi::Eip712Domain,
    ) -> PoolContext {
        PoolContext {
            pool_id,
            provider,
            deposit: U256::ZERO,
            client_signer: std::sync::Arc::clone(signer),
            voucher_domain: domain.clone(),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        }
    }

    /// Build a `WatermarkBundle` whose `last_signature` is `signer`'s real EIP-712 voucher
    /// signature over `(pool_id, signer, provider, amount, bytes_delivered)` — i.e. a
    /// genuinely-signed bundle, the shape a caller must construct one from.
    fn signed_bundle(
        pool_id: alloy::primitives::B256,
        provider: alloy::primitives::Address,
        signer: &alloy::signers::local::PrivateKeySigner,
        domain: &alloy::dyn_abi::Eip712Domain,
        amount: U256,
        bytes_delivered: U256,
    ) -> anyhow::Result<WatermarkBundle> {
        // A SEALED watermark: zero root, zero price. The bundle's authentication
        // gate recovers `last_signature` over the full seven-field voucher, so
        // the chain half has to be exactly what was signed — building the bundle
        // and the signature from one shape is what keeps that honest.
        let voucher_signature = Voucher {
            pool_id,
            signer: signer.address(),
            provider,
            amount,
            bytes_delivered,
            chain_root: alloy::primitives::B256::ZERO,
            chunk_price: U256::ZERO,
        }
        .sign(signer, domain)
        .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}"))?;
        Ok(WatermarkBundle {
            amount: u64::try_from(amount)?,
            bytes_delivered: u64::try_from(bytes_delivered)?,
            chain_root: [0u8; 32],
            verified_index: 0,
            tip: [0u8; 32],
            chunk_price: 0,
            last_signature: voucher_signature.signature.as_bytes().to_vec(),
        })
    }

    /// A metered bundle: the same seven-field voucher, but committing a real chain root and
    /// echoing the frontier the node claims under it. `released` is the depth the client
    /// actually gave the node; `claimed` is the depth the node reports. An honest node sets
    /// them equal.
    fn metered_bundle(
        pool_id: alloy::primitives::B256,
        provider: alloy::primitives::Address,
        signer: &alloy::signers::local::PrivateKeySigner,
        domain: &alloy::dyn_abi::Eip712Domain,
        seed: alloy::primitives::B256,
        released: u8,
        claimed: u8,
    ) -> anyhow::Result<WatermarkBundle> {
        let chain_root = decdn_incentive::chain::root_from_seed(seed);
        let amount = U256::from(500u64);
        let bytes_delivered = U256::from(4096u64);
        let voucher_signature = Voucher {
            pool_id,
            signer: signer.address(),
            provider,
            amount,
            bytes_delivered,
            chain_root,
            chunk_price: U256::from(10u64),
        }
        .sign(signer, domain)
        .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}"))?;
        Ok(WatermarkBundle {
            amount: u64::try_from(amount)?,
            bytes_delivered: u64::try_from(bytes_delivered)?,
            chain_root: chain_root.into(),
            verified_index: claimed,
            // The deepest preimage the node was actually handed. It cannot fabricate a deeper
            // one, so this is the whole of what it can prove.
            tip: decdn_incentive::chain::preimage_at(seed, released).into(),
            chunk_price: 10,
            last_signature: voucher_signature.signature.as_bytes().to_vec(),
        })
    }

    /// The chain half of a bundle is covered by NO signature: `verified_index` is a number the
    /// node writes, and the voucher whose signature it echoes carries no index. So a node can
    /// claim any depth it likes — and the resuming client folds `verified_index × chunk_price`
    /// into the amount it re-signs. Here the client released 3 chunks and the node reports 255;
    /// without the tip check the client would sign away 252 chunks it never received.
    #[test]
    fn resumable_watermark_rejects_a_frontier_the_tip_does_not_prove() -> anyhow::Result<()> {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let our_signer = std::sync::Arc::new(PrivateKeySigner::random());
        let ctx = resume_test_ctx(channel_id, token, &our_signer, &domain);

        let bundle = metered_bundle(
            channel_id,
            token,
            &our_signer,
            &domain,
            B256::repeat_byte(0x5E),
            3,
            255,
        )?;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::AmountRegression,
            bundle: Some(bundle),
            proof_generation: None,
        });

        anyhow::ensure!(
            resumable_watermark(&err, &ctx).is_none(),
            "a bundle claiming a depth its tip does not reach must never be folded into money"
        );
        Ok(())
    }

    /// The positive twin: the node reports exactly the depth it was given, its tip hashes
    /// forward to the root the client's own signature commits to, and the frontier is folded.
    #[test]
    fn resumable_watermark_accepts_a_frontier_the_tip_proves() -> anyhow::Result<()> {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let our_signer = std::sync::Arc::new(PrivateKeySigner::random());
        let ctx = resume_test_ctx(channel_id, token, &our_signer, &domain);

        let bundle = metered_bundle(
            channel_id,
            token,
            &our_signer,
            &domain,
            B256::repeat_byte(0x5E),
            3,
            3,
        )?;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::AmountRegression,
            bundle: Some(bundle),
            proof_generation: None,
        });

        let got = resumable_watermark(&err, &ctx)
            .ok_or_else(|| anyhow::anyhow!("a genuinely proved frontier must be resumable"))?;
        // 500 + 3 × 10: the anchor plus the three chunks the tip proves.
        anyhow::ensure!(Cumulative::from(got).amount == U256::from(530u64));
        Ok(())
    }

    /// The security property this module exists to guard (post-review-round-2, #1481 §5): a
    /// mid-stream `StreamError` carries no signature of its own, so `WatermarkBundle.amount`/
    /// `nonce`/`bytes_delivered` are otherwise attacker-controllable by the upstream node.
    /// `resumable_watermark` MUST refuse to reseed the ledger from a bundle whose
    /// `last_signature` does not recover to THIS client's own `ctx.client_signer` — otherwise a
    /// malicious/buggy upstream could hand back an inflated watermark and have this client sign
    /// (and the node redeem) a voucher for money it never delivered.
    #[test]
    fn resumable_watermark_rejects_a_bundle_not_signed_by_our_own_key() -> anyhow::Result<()> {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let our_signer = std::sync::Arc::new(PrivateKeySigner::random());
        let attacker_signer = PrivateKeySigner::random();
        let ctx = resume_test_ctx(channel_id, token, &our_signer, &domain);

        // The upstream (or an attacker impersonating it) signs the SAME tuple with a
        // DIFFERENT key — exactly what a malicious node echoing a fabricated watermark
        // would have to do, since it does not hold our key.
        let bundle = signed_bundle(
            channel_id,
            token,
            &attacker_signer,
            &domain,
            U256::from(1_000_000u64), // an inflated amount our ledger never earned
            U256::from(4096u64),
        )?;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::AmountRegression,
            bundle: Some(bundle),
            proof_generation: None,
        });

        anyhow::ensure!(
            resumable_watermark(&err, &ctx).is_none(),
            "a bundle signed by a key other than ours must never be treated as resumable"
        );
        Ok(())
    }

    /// The companion attack shape: the SAME rejected-voucher signature bytes replayed
    /// alongside a TAMPERED `amount` field. Recovery is over the whole tuple, so any field
    /// mismatch (not just a wrong key) must also fail the check — `last_signature` binds the
    /// exact `(amount, nonce, bytes_delivered)` triple, not just "some voucher we once signed".
    #[test]
    fn resumable_watermark_rejects_a_bundle_with_a_tampered_amount() -> anyhow::Result<()> {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let our_signer = std::sync::Arc::new(PrivateKeySigner::random());
        let ctx = resume_test_ctx(channel_id, token, &our_signer, &domain);

        // Genuinely our own signature — but over amount 100, not the 1_000_000 the bundle
        // claims. A node that recorded 100 and echoes 1_000_000 (bug or malice) must not
        // slip through just because SOME real signature accompanies it.
        let mut bundle = signed_bundle(
            channel_id,
            token,
            &our_signer,
            &domain,
            U256::from(100u64),
            U256::from(4096u64),
        )?;
        bundle.amount = 1_000_000u64;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::AmountRegression,
            bundle: Some(bundle),
            proof_generation: None,
        });

        anyhow::ensure!(
            resumable_watermark(&err, &ctx).is_none(),
            "a bundle whose signature does not cover the claimed amount must never be treated \
             as resumable"
        );
        Ok(())
    }

    /// The positive twin: a bundle genuinely signed by OUR OWN key, over the tuple it claims,
    /// for a gated reason, passes every check and is returned so the caller can reseed.
    #[test]
    fn resumable_watermark_accepts_a_bundle_genuinely_signed_by_our_own_key() -> anyhow::Result<()>
    {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let our_signer = std::sync::Arc::new(PrivateKeySigner::random());
        let ctx = resume_test_ctx(channel_id, token, &our_signer, &domain);

        let bundle = signed_bundle(
            channel_id,
            token,
            &our_signer,
            &domain,
            U256::from(500u64),
            U256::from(4096u64),
        )?;
        let expected_bytes = bundle.bytes_delivered;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::AmountRegression,
            bundle: Some(bundle),
            proof_generation: None,
        });

        let got = resumable_watermark(&err, &ctx).ok_or_else(|| {
            anyhow::anyhow!(
                "a bundle genuinely signed by our own key over the claimed tuple must resolve"
            )
        })?;
        assert_eq!(got.bytes_delivered, expected_bytes);
        Ok(())
    }

    /// Build an `UpstreamVoucherRejected` carrying a bundle genuinely signed by
    /// `signer` at `(amount, bytes)`, for a voucher signed under `proof_generation`.
    fn rejected_with_bundle(
        reason: VoucherRejectReason,
        ctx: &PoolContext,
        signer: &alloy::signers::local::PrivateKeySigner,
        (amount, bytes): (u64, u64),
        proof_generation: Option<u64>,
    ) -> anyhow::Result<anyhow::Error> {
        let bundle = signed_bundle(
            ctx.pool_id,
            ctx.provider,
            signer,
            &ctx.voucher_domain,
            U256::from(amount),
            U256::from(bytes),
        )?;
        Ok(anyhow::Error::new(UpstreamVoucherRejected {
            reason,
            bundle: Some(bundle),
            proof_generation,
        }))
    }

    fn heal_test_ctx() -> (
        PoolContext,
        std::sync::Arc<alloy::signers::local::PrivateKeySigner>,
    ) {
        use alloy::primitives::{Address, B256};
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let signer = std::sync::Arc::new(alloy::signers::local::PrivateKeySigner::random());
        let ctx = resume_test_ctx(
            B256::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            &signer,
            &domain,
        );
        (ctx, signer)
    }

    /// Extract the rejection's watermark and heal from it, the way both resume
    /// loops do.
    async fn heal(err: &anyhow::Error, ctx: &PoolContext, ledger: &PoolLedger) -> Option<Healed> {
        let watermark = rejection_watermark(err, ctx)?;
        heal_watermark_desync(err, watermark, ledger).await
    }

    /// An authenticated `Underpaid` bundle BEHIND the ledger rebases it down to the
    /// node's watermark. A later `Underpaid` for a voucher signed before that
    /// rebase is stale: it retries and leaves the ledger alone.
    #[tokio::test]
    async fn heal_rebases_on_an_underpaid_bundle_behind_the_ledger() -> anyhow::Result<()> {
        let (ctx, signer) = heal_test_ctx();
        let ledger = PoolLedger::new(Cumulative {
            bytes: U256::from(9_000u64),
            amount: U256::from(90u64),
        });
        let err = rejected_with_bundle(
            VoucherRejectReason::Underpaid,
            &ctx,
            &signer,
            (60, 5_000),
            Some(0),
        )?;
        assert_eq!(heal(&err, &ctx, &ledger).await, Some(Healed::Rebased));
        assert_eq!(
            ledger.committed(),
            Cumulative {
                bytes: U256::from(5_000u64),
                amount: U256::from(60u64),
            }
        );

        let stale = rejected_with_bundle(
            VoucherRejectReason::Underpaid,
            &ctx,
            &signer,
            (50, 4_000),
            Some(0),
        )?;
        assert_eq!(heal(&stale, &ctx, &ledger).await, Some(Healed::Stale));
        assert_eq!(ledger.committed().amount, U256::from(60u64));
        Ok(())
    }

    /// Only `Underpaid` rebases down. An `AmountRegression` echo behind the ledger
    /// proves no desync, so the caller surfaces the real error.
    #[tokio::test]
    async fn heal_does_not_rebase_on_other_reasons() -> anyhow::Result<()> {
        let (ctx, signer) = heal_test_ctx();
        let seed = Cumulative {
            bytes: U256::from(9_000u64),
            amount: U256::from(90u64),
        };
        let ledger = PoolLedger::new(seed);
        let err = rejected_with_bundle(
            VoucherRejectReason::AmountRegression,
            &ctx,
            &signer,
            (60, 5_000),
            Some(0),
        )?;
        assert_eq!(heal(&err, &ctx, &ledger).await, None);
        assert_eq!(ledger.generation(), 0);
        assert_eq!(ledger.committed(), seed);
        Ok(())
    }

    /// A bundle not signed by our own key heals nothing, even on `Underpaid`: a
    /// node cannot talk the payer's ledger down to a watermark it never signed.
    #[tokio::test]
    async fn heal_refuses_an_underpaid_bundle_we_did_not_sign() -> anyhow::Result<()> {
        let (ctx, _signer) = heal_test_ctx();
        let stranger = alloy::signers::local::PrivateKeySigner::random();
        let seed = Cumulative {
            bytes: U256::from(9_000u64),
            amount: U256::from(90u64),
        };
        let ledger = PoolLedger::new(seed);
        let err = rejected_with_bundle(
            VoucherRejectReason::Underpaid,
            &ctx,
            &stranger,
            (60, 5_000),
            Some(0),
        )?;
        assert_eq!(heal(&err, &ctx, &ledger).await, None);
        assert_eq!(ledger.committed(), seed);
        Ok(())
    }

    /// True twin: `SpendingCapExhausted` with no bundle (nothing for `resumable_watermark` to
    /// reseed from) and our own ledger confirming we truly cannot cover the next voucher.
    #[test]
    fn genuine_exhaustion_true_when_insufficient_and_ledger_drained() {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let signer = std::sync::Arc::new(PrivateKeySigner::random());
        let ctx = resume_test_ctx(channel_id, token, &signer, &domain);

        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
            proof_generation: None,
        });
        // remaining 10 µUSDC, next voucher needs 1000 -> truly out.
        assert!(genuine_exhaustion(
            &err,
            &ctx,
            Cumulative::default(),
            U256::from(10u64),
            U256::from(1000u64)
        ));
    }

    /// A node crying `SpendingCapExhausted` while our OWN ledger still shows headroom is NOT
    /// corroborated — the caller must refuse to fund it (a lying or buggy node must not be
    /// able to solicit an unnecessary top-up).
    #[test]
    fn genuine_exhaustion_false_when_ledger_still_has_headroom() {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let signer = std::sync::Arc::new(PrivateKeySigner::random());
        let ctx = resume_test_ctx(channel_id, token, &signer, &domain);

        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
            proof_generation: None,
        });
        assert!(!genuine_exhaustion(
            &err,
            &ctx,
            Cumulative::default(),
            U256::from(5000u64),
            U256::from(1000u64)
        ));
    }

    /// Any rejection reason other than `SpendingCapExhausted` is never exhaustion, regardless
    /// of what the ledger shows.
    #[test]
    fn genuine_exhaustion_false_for_non_insufficient_reason() {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let signer = std::sync::Arc::new(PrivateKeySigner::random());
        let ctx = resume_test_ctx(channel_id, token, &signer, &domain);

        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::AmountRegression,
            bundle: None,
            proof_generation: None,
        });
        assert!(!genuine_exhaustion(
            &err,
            &ctx,
            Cumulative::default(),
            U256::ZERO,
            U256::from(1000u64)
        ));
    }

    /// A healable watermark desync — an authenticated bundle that ADVANCES our committed
    /// watermark (the node knows about a voucher nonce we do not) — is NOT genuine exhaustion,
    /// even if it rides on an `SpendingCapExhausted` rejection and even if the ledger looks
    /// drained: the caller should reseed and resume, not fund a top-up.
    #[test]
    fn genuine_exhaustion_false_when_healable_desync_bundle_advances_committed()
    -> anyhow::Result<()> {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let signer = std::sync::Arc::new(PrivateKeySigner::random());
        let ctx = resume_test_ctx(channel_id, token, &signer, &domain);
        // Our own ledger is still at amount 300; the bundle reports amount 500 — the node
        // holds a later voucher than we do, exactly the healable desync #1481 §5 exists to
        // catch.
        let committed = Cumulative {
            bytes: U256::from(2048u64),
            amount: U256::from(300u64),
        };

        let bundle = signed_bundle(
            channel_id,
            token,
            &signer,
            &domain,
            U256::from(500u64),
            U256::from(4096u64),
        )?;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: Some(bundle),
            proof_generation: None,
        });

        // Sanity: this bundle IS the healable-desync case `resumable_watermark` resolves, and
        // it genuinely advances past `committed`.
        let resolved = resumable_watermark(&err, &ctx).ok_or_else(|| {
            anyhow::anyhow!("test bundle must be a healable desync resumable_watermark accepts")
        })?;
        anyhow::ensure!(
            Cumulative::from(resolved).amount > committed.amount,
            "test bundle must advance past `committed` to exercise the desync branch"
        );
        assert!(!genuine_exhaustion(
            &err,
            &ctx,
            committed,
            U256::from(10u64),
            U256::from(1000u64)
        ));
        Ok(())
    }

    /// The case this whole redesign exists for: a bundle that is PRESENT and authenticated, but
    /// does NOT advance past `committed` — the node echoing back exactly the watermark we
    /// already hold, because there is nothing later for it to report. This is NOT a desync (there
    /// is nothing to reseed to), so a genuinely drained ledger IS genuine exhaustion — the top-up
    /// path must fire, not the resync path. Every watermark-gated rejection on a channel that has
    /// ever had a voucher accepted carries a bundle (`watermark_bundle_for_reject`), so bundle
    /// PRESENCE alone (the pre-redesign check) would have misrouted this into an unproductive
    /// resync loop that eventually fails outright.
    #[test]
    fn genuine_exhaustion_true_when_bundle_present_but_does_not_advance_committed()
    -> anyhow::Result<()> {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let domain = decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33));
        let signer = std::sync::Arc::new(PrivateKeySigner::random());
        let ctx = resume_test_ctx(channel_id, token, &signer, &domain);
        // Our ledger and the echoed bundle agree EXACTLY: amount 500 both sides.
        let committed = Cumulative {
            bytes: U256::from(4096u64),
            amount: U256::from(500u64),
        };

        let bundle = signed_bundle(
            channel_id,
            token,
            &signer,
            &domain,
            U256::from(500u64),
            U256::from(4096u64),
        )?;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: Some(bundle),
            proof_generation: None,
        });

        // Sanity: the bundle is still authenticated (resumable_watermark resolves it) — the
        // fix is NOT "stop trusting the bundle", it's "stop treating its mere presence as proof
        // of desync".
        anyhow::ensure!(
            resumable_watermark(&err, &ctx).is_some(),
            "test bundle must be authenticated for this to be a meaningful test"
        );
        // remaining 10 µUSDC, next voucher needs 1000 -> truly out, and the bundle does not
        // move us anywhere new.
        assert!(genuine_exhaustion(
            &err,
            &ctx,
            committed,
            U256::from(10u64),
            U256::from(1000u64)
        ));
        Ok(())
    }

    /// Deterministic pseudo-random blob spanning several 16 KiB chunk groups.
    fn make_blob(len: usize) -> Vec<u8> {
        let mut v = vec![0u8; len];
        let mut x: u32 = 0x9e37_79b9;
        for b in &mut v {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes().first().copied().unwrap_or(0);
        }
        v
    }

    fn sub(data: &[u8], start: u64, end: u64) -> anyhow::Result<Vec<u8>> {
        let s = usize::try_from(start)?;
        let e = usize::try_from(end)?;
        data.get(s..e)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| anyhow::anyhow!("range [{start}, {end}) out of bounds"))
    }

    /// Produce the **header-less** bao wire stream a server emits for
    /// `[byte_offset, byte_offset + byte_len)` of `blob` (`byte_len == 0` ⇒ to
    /// end), plus the content root. `encode_verified_range` yields the combined
    /// form (8-byte size header + interleaved stream); the wire drops the header.
    fn wire_for(
        blob: &[u8],
        byte_offset: u64,
        byte_len: u64,
    ) -> anyhow::Result<([u8; 32], Vec<u8>)> {
        let ob = PreOrderMemOutboard::create(blob, IROH_BLOCK_SIZE);
        let root = *ob.root.as_bytes();
        let blob_size = u64::try_from(blob.len())?;
        let aligned = align_range(byte_offset, byte_len, blob_size)?;
        let data = sub(blob, aligned.fetch_start(), aligned.fetch_end())?;
        let combined = encode_verified_range(root, &aligned, &data, ob.data.clone().into())?;
        let wire = combined
            .get(8..)
            .ok_or_else(|| anyhow::anyhow!("combined shorter than 8-byte header"))?
            .to_vec();
        Ok((root, wire))
    }

    /// Drive `decode_to_vec` over a fixed wire buffer (a `Bytes` reader stashes
    /// no fault, so the decoder's verdict is the whole story) and trim the
    /// aligned superset back to `[byte_offset, total_bytes)` — the receive side
    /// of `fetch_in_memory_once` with the live pull swapped for memory.
    async fn decode_wire(
        root: [u8; 32],
        total_bytes: u64,
        byte_offset: u64,
        wire: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        let aligned = align_range(byte_offset, 0, total_bytes)?;
        let (out, _reader) = decode_to_vec(
            root,
            total_bytes,
            &aligned,
            Bytes::copy_from_slice(wire),
            None,
        )
        .await?;
        let lead = usize::try_from(byte_offset.saturating_sub(aligned.fetch_start()))?;
        out.get(lead..)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| anyhow::anyhow!("decoded range shorter than requested span"))
    }

    #[tokio::test]
    async fn decode_to_vec_round_trips_whole_blob() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (root, wire) = wire_for(&blob, 0, 0)?;
        let out = decode_wire(root, u64::try_from(blob.len())?, 0, &wire).await?;
        anyhow::ensure!(out == blob, "whole-blob round-trip");
        Ok(())
    }

    /// A resumed fetch at a group-aligned offset self-verifies against the root —
    /// no dependency on the bytes before the offset (the old gap is closed).
    #[tokio::test]
    async fn decode_to_vec_resumed_group_aligned_offset() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let off = 64 * 1024; // 16 KiB-group aligned
        let (root, wire) = wire_for(&blob, off, 0)?;
        let out = decode_wire(root, u64::try_from(blob.len())?, off, &wire).await?;
        let want = sub(&blob, off, u64::try_from(blob.len())?)?;
        anyhow::ensure!(out == want, "resumed tail self-verifies");
        Ok(())
    }

    /// A non-group-aligned resume offset: the server serves the aligned superset
    /// and the receiver trims the leading bytes back to the exact requested span.
    #[tokio::test]
    async fn decode_to_vec_trims_non_aligned_offset() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let off = 70 * 1024; // inside a group, not on a boundary
        let (root, wire) = wire_for(&blob, off, 0)?;
        let out = decode_wire(root, u64::try_from(blob.len())?, off, &wire).await?;
        let want = sub(&blob, off, u64::try_from(blob.len())?)?;
        anyhow::ensure!(out == want, "trimmed to requested offset");
        Ok(())
    }

    /// A corrupt tail byte is rejected at its chunk group with the typed
    /// `HashMismatch` — even on a resumed fetch with no earlier bytes (ADR 038 #1).
    #[tokio::test]
    async fn decode_to_vec_rejects_corrupt_tail() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let off = 64 * 1024;
        let (root, mut wire) = wire_for(&blob, off, 0)?;
        // Flip a byte near the end of the stream — inside the final leaf's data.
        let last = wire
            .len()
            .checked_sub(8)
            .ok_or_else(|| anyhow::anyhow!("wire too short"))?;
        if let Some(b) = wire.get_mut(last) {
            *b ^= 0xff;
        }
        let err = decode_wire(root, u64::try_from(blob.len())?, off, &wire)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected corrupt tail to be rejected"))?;
        anyhow::ensure!(
            err.downcast_ref::<HashMismatch>().is_some(),
            "corrupt tail must surface HashMismatch, got: {err}"
        );
        Ok(())
    }

    /// ADR 038 AC#2 (early rejection at the offending group): a corrupt MIDDLE
    /// group is rejected as `HashMismatch` even when everything AFTER it is
    /// missing — detection needs no tail, which is what lets the live receive
    /// loop stop pulling (and paying) at group *k*.
    #[tokio::test]
    async fn decode_to_vec_rejects_corrupt_middle_group_without_tail() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (root, mut wire) = wire_for(&blob, 0, 0)?;
        // Corrupt a byte ~55% in (inside a middle group's data), then TRUNCATE
        // everything after ~70% — the decoder must fail on the corrupt group,
        // never reaching (or needing) the missing tail.
        let corrupt_at = wire.len() * 55 / 100;
        let truncate_at = wire.len() * 70 / 100;
        if let Some(b) = wire.get_mut(corrupt_at) {
            *b ^= 0xff;
        }
        wire.truncate(truncate_at);
        let err = decode_wire(root, u64::try_from(blob.len())?, 0, &wire)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected corrupt middle group to be rejected"))?;
        anyhow::ensure!(
            err.downcast_ref::<HashMismatch>().is_some(),
            "corrupt middle group must surface HashMismatch (early rejection), got: {err}"
        );
        Ok(())
    }

    /// A truncated-but-clean stream is a transport-class failure, NOT
    /// corruption: it must NOT downcast to `HashMismatch`, because callers use
    /// that sentinel to score the provider `Corruption` (tarring a peer for a
    /// dropped connection would misattribute blame — #915 review).
    #[tokio::test]
    async fn decode_to_vec_truncation_is_not_hash_mismatch() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (root, mut wire) = wire_for(&blob, 0, 0)?;
        wire.truncate(wire.len() * 60 / 100);
        let err = decode_wire(root, u64::try_from(blob.len())?, 0, &wire)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected truncated stream to be rejected"))?;
        anyhow::ensure!(
            err.downcast_ref::<HashMismatch>().is_none(),
            "clean truncation must NOT be classified as corruption, got HashMismatch: {err}"
        );
        Ok(())
    }

    /// Decoding an honest stream against the WRONG root fails closed (the range
    /// can't be re-anchored), so a source serving a different blob is rejected.
    #[tokio::test]
    async fn decode_to_vec_rejects_wrong_root() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (_root, wire) = wire_for(&blob, 0, 0)?;
        let wrong = [0xABu8; 32];
        let err = decode_wire(wrong, u64::try_from(blob.len())?, 0, &wire)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected wrong-root rejection"))?;
        anyhow::ensure!(
            err.downcast_ref::<HashMismatch>().is_some(),
            "wrong root must surface HashMismatch, got: {err}"
        );
        Ok(())
    }

    /// An open-stage refusal must carry the upstream's **signed** `StreamResponse`
    /// out with it, not just the wire code (#1042).
    ///
    /// This is the seam that makes real daemon output admissible on-chain. The
    /// `slash_sig` here is produced by the same `StreamSlashData` signer the node
    /// uses and covers the same EIP-712 digest `SlashJudge._verifyPair` checks, so
    /// what the assertion actually proves is that the operator's self-incriminating
    /// attestation survives the client's error path intact — byte-for-byte, still
    /// recovering to the signer. Before #1042 `refusal()` took only
    /// `response.error` and dropped the body and signature on the floor, leaving
    /// `SlashJudge`'s rate path reachable only from a test that holds the
    /// operator's key and synthesises its own evidence.
    ///
    /// Deliberately routed through the private [`UpstreamRefused::open`] — the one
    /// constructor the open stage uses — rather
    /// than a hand-built value, which #1377's newtype now makes impossible outside
    /// this crate anyway.
    #[test]
    fn an_open_stage_refusal_preserves_the_signed_stream_response() -> anyhow::Result<()> {
        use alloy::signers::local::PrivateKeySigner;
        use decdn_incentive::slash_judge_domain;
        use decdn_incentive::stream_sig::StreamSlashData;
        use decdn_protocol::client::{StreamError, StreamResponse, StreamResponseBody};

        use super::UpstreamRefused;

        let operator = PrivateKeySigner::random();
        let domain = slash_judge_domain(31_337, alloy::primitives::Address::repeat_byte(0x11));
        // The wire shape of a signed refusal: the node signed `ok = false` for a
        // hash it had just announced (now inert as slash evidence).
        let body = StreamResponseBody {
            hash: [0x5Au8; 32],
            ok: false,
            rate_per_mb: 10,
            total_bytes: 0,
            pool_id: [0x77u8; 32],
            timestamp_us: 1_700_000_000_000_000,
        };
        let sig = StreamSlashData::from_response_body(&body).sign(&operator, &domain)?;
        let response = StreamResponse {
            body: body.clone(),
            slash_sig: sig.as_bytes().to_vec(),
        };
        let response_ext = decdn_protocol::StreamResponseExt {
            error: Some(StreamError::EvictedSinceProbe),
        };
        // Preconditions the real open stage enforces before ever calling `open`.
        response.validate()?;
        response_ext.validate(response.body.ok)?;

        let err = UpstreamRefused::open(response, &response_ext);
        let refused = err
            .downcast_ref::<UpstreamRefused>()
            .ok_or_else(|| anyhow::anyhow!("open() must stay a typed UpstreamRefused: {err:#}"))?;
        anyhow::ensure!(
            *refused.error() == StreamError::EvictedSinceProbe,
            "the wire code must survive unchanged, got {:?}",
            refused.error()
        );
        let preserved = refused.evidence().ok_or_else(|| {
            anyhow::anyhow!("the signed StreamResponse must survive on the refusal (#1042)")
        })?;
        anyhow::ensure!(
            preserved.body == body,
            "the preserved body must be the signed body verbatim"
        );
        // #1377: `error()` is now DERIVED from the evidence by `open()`, so the two
        // legs cannot desync by construction. This pins that they agree.
        anyhow::ensure!(
            response_ext.error.as_ref() == Some(refused.error()),
            "the derived wire code {:?} must match the code the extension carried {:?}",
            refused.error(),
            response_ext.error,
        );
        // The whole point: the surviving signature still recovers to the operator,
        // so it can be replayed to `SlashJudge` with no re-signing by the observer.
        let recovered = alloy::primitives::Signature::try_from(preserved.slash_sig.as_slice())?;
        StreamSlashData::from_response_body(&preserved.body).verify_signer(
            &recovered,
            operator.address(),
            &domain,
        )?;
        Ok(())
    }

    /// Option 2 / #2013: `is_insufficient_deposit` recognises an open-stage
    /// `InsufficientDeposit` (the node signed `ok: false` with the code in the ext),
    /// which is the only shape a legitimate floor refusal takes, and REJECTS a bare
    /// mid-stream `StreamError::InsufficientDeposit` — a protocol violation that must
    /// not drive the top-up loop.
    #[test]
    fn is_insufficient_deposit_matches_only_the_open_stage_refusal() -> anyhow::Result<()> {
        use decdn_protocol::client::{StreamError, StreamResponse, StreamResponseBody};

        use super::{UpstreamRefused, is_insufficient_deposit};

        let body = StreamResponseBody {
            hash: [0x5Au8; 32],
            ok: false,
            rate_per_mb: 10,
            total_bytes: 0,
            pool_id: [0x77u8; 32],
            timestamp_us: 1_700_000_000_000_000,
        };
        let response = StreamResponse {
            body,
            slash_sig: vec![0u8; decdn_protocol::message::SLASH_SIG_LEN],
        };
        let ext = decdn_protocol::StreamResponseExt {
            error: Some(StreamError::InsufficientDeposit),
        };
        let open = UpstreamRefused::open(response, &ext);
        anyhow::ensure!(
            is_insufficient_deposit(&open),
            "an open-stage InsufficientDeposit must be recognised"
        );

        let mid = anyhow::Error::new(UpstreamRefused::mid_stream(
            StreamError::InsufficientDeposit,
        ));
        anyhow::ensure!(
            !is_insufficient_deposit(&mid),
            "a mid-stream InsufficientDeposit is protocol-violating and must NOT be honored"
        );

        // A different open-stage code is not a floor refusal either.
        let other_body = StreamResponseBody {
            hash: [0x5Au8; 32],
            ok: false,
            rate_per_mb: 10,
            total_bytes: 0,
            pool_id: [0x77u8; 32],
            timestamp_us: 0,
        };
        let other = UpstreamRefused::open(
            StreamResponse {
                body: other_body,
                slash_sig: vec![0u8; decdn_protocol::message::SLASH_SIG_LEN],
            },
            &decdn_protocol::StreamResponseExt {
                error: Some(StreamError::NotFound),
            },
        );
        anyhow::ensure!(
            !is_insufficient_deposit(&other),
            "an open-stage NotFound is not a floor refusal"
        );
        Ok(())
    }

    /// #1377: `open()` on a `body.ok == false` response that carries no error code
    /// is a protocol violation (the `validate` invariant was bypassed), so it does
    /// NOT produce a typed `UpstreamRefused` that a challenger could act on — it
    /// surfaces as a plain error instead of laundering a malformed refusal.
    #[test]
    fn open_on_a_response_without_an_error_code_is_not_a_typed_refusal() {
        use decdn_protocol::client::{StreamResponse, StreamResponseBody};

        use super::UpstreamRefused;

        let response = StreamResponse {
            body: StreamResponseBody {
                hash: [0x5Au8; 32],
                ok: false,
                rate_per_mb: 10,
                total_bytes: 0,
                pool_id: [0x77u8; 32],
                timestamp_us: 1_700_000_000_000_000,
            },
            slash_sig: vec![0u8; decdn_protocol::message::SLASH_SIG_LEN],
        };
        let err = UpstreamRefused::open(response, &decdn_protocol::StreamResponseExt::default());
        assert!(
            err.downcast_ref::<UpstreamRefused>().is_none(),
            "a response with no error code must not become a typed refusal"
        );
    }

    /// #1375: the two buyer ceilings combine as a min with `0` meaning "unbounded"
    /// on each input, so a completed pull is always bounded by the LOWER of the
    /// probe-relative and absolute bounds — and unbounded only when both are.
    #[test]
    fn effective_rate_ceiling_is_min_with_zero_as_unbounded() {
        use super::effective_rate_ceiling;
        assert_eq!(
            effective_rate_ceiling(0, 0),
            0,
            "both unbounded => unbounded"
        );
        assert_eq!(
            effective_rate_ceiling(10, 0),
            10,
            "config unbounded => probe"
        );
        assert_eq!(
            effective_rate_ceiling(0, 900),
            900,
            "probe unbounded => config"
        );
        assert_eq!(effective_rate_ceiling(10, 900), 10, "min: probe binds");
        assert_eq!(effective_rate_ceiling(900, 10), 10, "min: config binds");
        assert_eq!(effective_rate_ceiling(42, 42), 42, "equal bounds");
    }

    /// A **mid-stream** refusal carries no signature, so it must never carry a
    /// `StreamResponse` either: the evidence contract promises a present value
    /// always recovers to the delivering node, and a synthesised one would hand an
    /// observer an unsigned artifact that breaks that promise (#1378). Routing all
    /// four mid-stream sites through [`UpstreamRefused::mid_stream`] makes
    /// `evidence() == None` a one-place decision; #1377 makes it a type property.
    #[test]
    fn a_mid_stream_refusal_never_carries_a_response() {
        use decdn_protocol::client::StreamError;

        use super::UpstreamRefused;

        let refused = UpstreamRefused::mid_stream(StreamError::Overloaded);
        assert!(
            refused.evidence().is_none(),
            "a mid-stream refusal has no signed response to carry"
        );
        assert_eq!(
            *refused.error(),
            StreamError::Overloaded,
            "the mid-stream wire code must survive unchanged"
        );
    }
}
