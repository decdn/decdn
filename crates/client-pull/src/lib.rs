//! Reusable `cdn/client/v1` paid-pull requester, shared by the node
//! (node-to-node miss pulls, #317) and the CLI (client fetch / bundle pull).
//!
//! `stream_fetch` performs one full delivery exchange against a remote node:
//! it sends a [`StreamRequest`], validates and verifies the signed
//! [`StreamResponse`], receives `ChunkData` while paying cumulative vouchers
//! at each `voucher_interval_mb` boundary, and returns the assembled blob on
//! `StreamEnd`. It is the receive-side call site for the #252 rule (reject a
//! `rate_per_mb == 0` response) and the `slash_sig` verification obligation
//! (ADR 014 §1).
//!
//! Mirrors [`probe::probe_once`] in spirit, but for the paid path: it signs
//! vouchers, so it needs the incentive layer and a signer.
//!
//! # Scope
//!
//! - **Redirects** (`StreamResponse.redirect`) are detected and rejected, not
//!   followed: resolving a redirect `NodeId` to a dialable address needs the
//!   provider-discovery layer (ADR 001 / 022), which is out of scope. A #317
//!   server always sends `redirect: None`.
//! - **Bao verified-range decoding** (ADR 038): the `ChunkData` payload is bao's
//!   interleaved verified-stream encoding, not raw bytes. The buffered path feeds
//!   the reassembled stream to a `bao-tree` verifying decoder that checks every
//!   chunk group against the requested content-hash root, so a range fetched at
//!   any `byte_offset > 0` self-verifies (a corrupt tail is rejected) with no
//!   dependency on earlier bytes — closing the old resume gap. The progressive
//!   (window pull-through) path forwards the bao stream verbatim and tees it into
//!   the cache's verifying decoder (`import_and_verify_stream`), which checks the
//!   cached copy against the same root.

/// Buyer-side `PaymentChannel` open kernel (#940), shared by the node service
/// and the CLI.
pub mod buyer_channel;
/// Client-initiated cooperative close (#971), shared by the CLI and the node.
pub mod cooperative_close;
/// Client-side node discovery (#936): read + select the active node set from
/// `CapacityBond.getRegisteredNodes`, then rank probed blob-holders.
pub mod discovery;
/// The #1608 gap-driven fetch driver: [`driver::drive`] fills only the
/// [`missing_ranges`](decdn_bao_range::RangedStore::missing_ranges) of a request,
/// paying the minimum, by folding the resume / top-up / settle-wait / reseed loop
/// behind the [`pacer::Pacer`] + [`source::Funder`] axes.
pub mod driver;
/// One-shot client `Endpoint` construction: relay + discovery resolution for
/// the `cdn/client/v1` and `cdn/probe/v1` dial paths (#935/#936).
pub mod endpoint;
mod ledger;
/// The pure pacing axis (#1608): [`pacer::Pacer`] / [`pacer::BudgetPacer`] decide
/// draw / top-up / wait / done / refuse for the gap-driven driver, with no I/O.
pub mod pacer;
/// Reusable `cdn/probe/v1` client.
pub mod probe;
/// Wallet-filled HTTP provider builder for opening/settling payment channels.
pub mod provider;
/// Client-side [`decdn_bao_range::RangedStore`] backend (#1621 P2): a
/// `.partial` + sidecar store built on `bao-tree`/`decdn-bao-range` only.
pub mod ranged_store;
pub mod rtt_map;
// Docs live in `sink.rs` as `//!`. Deliberately NOT documented here as well:
// rustdoc resolves intra-doc links on a `mod` item in THIS file's scope, so the
// module's own links (`ResponseDecoder`, `resume_offset`, …) would go unresolved
// and fail the `-D warnings` doc gate.
pub mod sink;
/// The two sourcing axes of the gap-driven driver (#1608): [`source::BlobSource`]
/// (raw-bao byte source for a range) and [`source::Funder`] (injected top-up
/// seam), plus scripted test doubles.
pub mod source;

pub use driver::drive;
pub use ledger::{ChannelLedger, Cumulative};
pub use pacer::{BudgetPacer, PaceDecision, PaceState, Pacer};
pub use ranged_store::ClientRangedStore;
pub use source::{BaoRangeReader, BlobSource, Funder, PeerSource};

#[cfg(any(test, feature = "test-util"))]
pub use source::{FakeFunder, ScriptedReader, ScriptedSource};

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use bao_tree::BaoTree;
use bao_tree::io::sync::DecodeResponseIter;
use bao_tree::io::{BaoContentItem, DecodeError};
use bytes::{Bytes, BytesMut};
use decdn_bao_range::{IROH_BLOCK_SIZE, align_range};
use decdn_incentive::{
    BuyerChannelState, EPHEMERAL_BINDING_NONCE, SignedVoucher, StreamSlashData, Voucher,
    binding_signing_hash, signed_to_wire_voucher,
};
use decdn_protocol::client::{
    ClientBinding, ClientMessage, StreamError, StreamRequest, StreamRequestExt, StreamResponse,
    VoucherRejectReason, WatermarkBundle,
};
use decdn_protocol::{
    ALPN_CLIENT, DEFAULT_VOUCHER_INTERVAL_MB, MB_BYTES, decode_message, encode_message, read_frame,
    write_frame,
};
use iroh::endpoint::{RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};

/// Per-channel context the requester needs to sign vouchers.
///
/// The `prior_*` fields capture the channel's cumulative state from earlier
/// streams so a **reused** channel resumes correctly: vouchers are cumulative
/// across the channel's lifetime, so the node's `last_nonce` /
/// `last_bytes_delivered` / `last_amount` are non-zero after the first stream.
/// Starting a fresh stream from zero would be rejected (`StaleNonce` /
/// `BytesRegression`). For a brand-new channel pass `U256::ZERO` for all three.
#[derive(Clone)]
pub struct ChannelContext {
    /// On-chain `channelId`.
    pub channel_id: B256,
    /// `ERC-20` token bound by the channel (`USDC`).
    pub token: Address,
    /// On-chain deposit (informational here; the node enforces it).
    pub deposit: U256,
    /// Client key that signs vouchers.
    pub client_signer: Arc<PrivateKeySigner>,
    /// `PaymentChannel` EIP-712 domain.
    pub voucher_domain: Eip712Domain,
    /// Nonce of the last voucher the client issued on this channel (the next
    /// voucher uses `prior_nonce + 1`). `ZERO` for a fresh channel.
    pub prior_nonce: U256,
    /// Cumulative bytes paid for on this channel before this stream.
    pub prior_bytes_delivered: U256,
    /// Cumulative amount paid on this channel before this stream.
    pub prior_amount: U256,
    /// Optional ADR 005 client identity binding (address + `BindNodeId`
    /// signature over the requester's own iroh `NodeId`, see
    /// [`sign_client_binding`]). Attached to every `cdn/client/v1` request's
    /// `ext` so the serving node can recover the buyer address, confirm it owns
    /// the channel (`pull_authorized`), and reactively populate from its
    /// configured origin (#1115). `None` ⇒ no binding is sent (an unconfigured
    /// `capacity_bond` on the client, or an on-chain/registered requester).
    pub client_binding: Option<ClientBinding>,
}

impl ChannelContext {
    /// Build a context for a buyer-held channel, resuming from its persisted
    /// cumulative voucher state (#744). The `prior_*` fields come straight from
    /// the stored [`BuyerChannelState`], so the next voucher continues the
    /// channel at `last_nonce + 1` rather than restarting from zero (which the
    /// upstream node would reject). For a freshly-opened channel the stored
    /// `last_*` are all `ZERO`, yielding a fresh-channel context.
    #[must_use]
    pub const fn for_buyer_channel(
        state: &BuyerChannelState,
        client_signer: Arc<PrivateKeySigner>,
        voucher_domain: Eip712Domain,
    ) -> Self {
        Self {
            channel_id: state.channel_id,
            token: state.token,
            deposit: state.deposit,
            client_signer,
            voucher_domain,
            prior_nonce: state.last_nonce,
            prior_bytes_delivered: state.last_bytes_delivered,
            prior_amount: state.last_amount,
            client_binding: None,
        }
    }

    /// Attach an ADR 005 client identity binding so this context's
    /// `cdn/client/v1` requests prove channel ownership to the serving node,
    /// enabling reactive cache-miss origin pull-through (#1115). Pass a binding
    /// produced by [`sign_client_binding`]; a hand-built `ClientBinding` whose
    /// `ethereum_address` and signature don't correspond (or that doesn't own the
    /// channel) is rejected by the serving node, so this only ever hurts the
    /// caller itself.
    #[must_use]
    pub fn with_client_binding(mut self, binding: ClientBinding) -> Self {
        self.client_binding = Some(binding);
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
/// identity binding, or `None` when the context is unbound — in which case
/// `encode_stream_request` appends no ext bytes, byte-for-byte the pre-#1115
/// wire. Shared by `fetch_inner` and `open_progressive_pull`. `voucher_interval_mb`
/// stays `None` so both sides keep negotiating the default cadence.
fn client_binding_ext(ctx: &ChannelContext) -> Option<StreamRequestExt> {
    ctx.client_binding.as_ref().map(|binding| StreamRequestExt {
        voucher_interval_mb: None,
        binding: Some(binding.clone()),
    })
}

impl std::fmt::Debug for ChannelContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelContext")
            .field("channel_id", &self.channel_id)
            .field("token", &self.token)
            .field("deposit", &self.deposit)
            .finish_non_exhaustive()
    }
}

/// The channel's acked voucher watermark, threaded through [`stream_fetch_tracked`]
/// as an out-param so the caller can persist what it paid (#852).
///
/// [`stream_fetch_tracked`] drives the pull through a one-shot [`ChannelLedger`]
/// seeded from the channel's prior cumulative state, then copies the ledger's
/// acked cumulative back into this watermark (`set_from_cumulative`) before
/// returning — so it always holds the
/// **absolute** cumulative totals of the last *acked* voucher (not per-stream
/// deltas), exactly the triple `BuyerChannelService::record_progress` expects.
/// Because the copy-back runs on every return path (including the `Err`/timeout
/// arms), the latest acked totals survive a mid-stream failure or a
/// paid-but-corrupt delivery, recorded against the upstream's committed
/// watermark (ADR 003).
///
/// **Acked only.** The watermark tracks vouchers the upstream *acknowledged*. If
/// the upstream commits a voucher (ADR 003: commit precedes the ack) but the ack
/// is then lost — a dropped connection while reading it — the watermark lags by
/// that one voucher; the next reuse re-signs a stale nonce and is rejected until
/// the channel rotates. That residual is inherent to one-sided ack loss and is
/// not closed here (it would need a reconciliation read of the upstream's
/// committed nonce on the next open).
///
/// Read the persistable totals via [`VoucherProgress::acked`], which yields `None`
/// when nothing was acked on this stream (so there is nothing to persist).
#[derive(Clone, Copy, Debug, Default)]
pub struct VoucherProgress {
    /// Nonce of the last acked voucher (the channel's prior nonce until the first
    /// ack on this stream).
    nonce: U256,
    /// Cumulative channel bytes paid for as of the last acked voucher.
    bytes_delivered: U256,
    /// Cumulative channel amount paid as of the last acked voucher.
    amount: U256,
    /// Count of vouchers acked on this stream.
    vouchers_sent: u64,
}

impl VoucherProgress {
    /// Build the watermark from a ledger [`Cumulative`] plus the channel's seed
    /// nonce. `vouchers_sent` is the number of vouchers acked since the seed
    /// (`cum.nonce - prior_nonce`, saturated to `u64`); `acked()` only checks it is
    /// `> 0`, so this preserves the "acked iff the nonce advanced past the seed"
    /// contract even when the ledger was shared across concurrent streams.
    ///
    /// Public because a caller that owns its ledger persists from it directly rather
    /// than through the `&mut VoucherProgress` out-param — including from a `Drop`,
    /// where nothing can be awaited and [`ChannelLedger::committed`] is the only
    /// readable source (#1145 review).
    #[must_use]
    pub fn from_cumulative(cum: Cumulative, prior_nonce: U256) -> Self {
        Self {
            nonce: cum.nonce,
            bytes_delivered: cum.bytes,
            amount: cum.amount,
            vouchers_sent: u64::try_from(cum.nonce.saturating_sub(prior_nonce)).unwrap_or(u64::MAX),
        }
    }

    fn set_from_cumulative(&mut self, cum: Cumulative, prior_nonce: U256) {
        *self = Self::from_cumulative(cum, prior_nonce);
    }

    /// The cumulative `(nonce, bytes_delivered, amount)` to persist via
    /// `record_progress`, or `None` if no voucher was acked on this stream
    /// (nothing new was paid, so there is nothing to record).
    #[must_use]
    pub fn acked(&self) -> Option<(U256, U256, U256)> {
        (self.vouchers_sent > 0).then_some((self.nonce, self.bytes_delivered, self.amount))
    }
}

/// The upstream delivered bytes that failed bao verification against the
/// requested content root — a paid-but-corrupt delivery (the content-addressing
/// invariant, ADR 014/038). Under ADR 038 the verifier is the per-chunk-group
/// `bao-tree` decoder (`decode_verified_range`), not a whole-blob re-hash, so
/// this fires the moment any group's proof mismatches. Returned (via `anyhow`)
/// by `stream_fetch` so callers can `downcast_ref` to classify corruption
/// (e.g. a reputation `Corruption` outcome) without matching on the error
/// message string. The `Display` text is kept stable for logs and the existing
/// requester tests.
#[derive(Debug)]
pub struct HashMismatch;

impl std::fmt::Display for HashMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("received bytes do not match requested hash")
    }
}

impl std::error::Error for HashMismatch {}

/// Typed sentinel for a server that claimed a `total_bytes` above the buyer's
/// `max_blob_size_bytes` ceiling (#840). Returned (not a bare string) so the
/// pull orchestrator can `downcast_ref` and classify it as a buyer-side policy
/// rejection — distinct from a hash mismatch or an unreachable peer — rather
/// than mis-attributing it to the provider's reputation. `Display` carries
/// `BlobTooLarge` so logs and the existing requester tests can match on it.
#[derive(Debug)]
pub struct BlobTooLargeClaim {
    pub claimed: u64,
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

impl std::fmt::Display for BlobTooLargeClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "server claimed {} bytes, exceeding max_blob_size {} bytes (BlobTooLarge)",
            self.claimed, self.ceiling
        )
    }
}

impl std::error::Error for BlobTooLargeClaim {}

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
    /// and the SINGLE construction path (both `fetch_inner` and
    /// `open_progressive_pull` route through it), so `quoted_rate_per_mb` is always
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
/// not tar the provider's reputation locally or over gossip. `Display` keeps the
/// stable `timed out` text for logs (and for the `!contains("timed out")`
/// negative assertion in `node_to_node_pull_through`'s deadline test).
///
/// Since #1134 it is raised by OUR OWN wall clocks only, and the message is
/// deliberately stage-NEUTRAL because there are THREE of them: the shared
/// `open_stream` open bound (`PullDeadlines::open`, on both the buffered and the
/// progressive path), the optional overall `hard_cap`, and the stall clock elapsing
/// before the FIRST byte (`cumulative == 0`) in either streaming loop — where it is
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
/// verified the rejected voucher recovered to the channel's pinned
/// `voucher_signer` before attaching it. A caller that sees `Some` alongside
/// `StaleNonce`/`AmountRegression`/`BytesRegression` can self-heal — re-seed
/// its ledger to the bundle's watermark ([`crate::ledger::Cumulative::from`])
/// and resume from `bytes_delivered` — rather than treating the rejection as
/// terminal. `InsufficientDeposit` with `bundle: None` means there is no
/// signer-verified watermark to resume from (or, more commonly, that a
/// wallet-less delegate simply has no local means to add deposit) — the
/// caller must surface that to the app rather than loop.
#[derive(Debug)]
pub struct UpstreamVoucherRejected {
    pub reason: VoucherRejectReason,
    pub bundle: Option<WatermarkBundle>,
}

impl std::fmt::Display for UpstreamVoucherRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The `{:?}` rendering of the reason is load-bearing: the loopback tests
        // assert `.contains("RetryLater")` / `.contains("Expired")` on this string.
        // A custom `Display` for `VoucherRejectReason` would have to reproduce the
        // variant names verbatim, so keep the Debug rendering here.
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
    /// *from* `response.error` and `response` always recovers to
    /// `expected_signer`.
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
    /// the wire `error` *from* `response.error` so the two can never disagree
    /// (#1377 invariant 2). Shared by the buffered [`fetch_inner`] and
    /// progressive [`open_progressive_pull`] open stages so they cannot drift in
    /// how they classify a refusal.
    ///
    /// Returns `anyhow::Error` rather than `Self` because the `None`-error arm is
    /// a protocol violation, not a refusal: callers MUST have run
    /// [`verify_response`] first, whose `StreamResponse::validate` rejects
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
    fn open(response: StreamResponse) -> anyhow::Error {
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
        match response.error.clone() {
            Some(error) => anyhow::Error::new(Self {
                kind: Kind::Open {
                    error,
                    response: Box::new(response),
                },
            }),
            None => anyhow::anyhow!(
                "delivery refused but the validated response carried no error code \
                 (StreamResponse::validate invariant bypassed)"
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
/// `classify_pull_failure` scores it as such — a local EWMA hit AND a gossiped observation.
///
/// # What the bound rests on
///
/// Two things, and it is worth being precise about both, because scoring a peer on a bound
/// that does not hold is how an honest node gets defamed network-wide.
///
/// **The non-empty-`ChunkData` invariant (#1088), on BOTH pull paths.** With empty frames
/// banned, "a frame arrived" and "bytes made progress" are the same statement, so a peer
/// cannot hold the deadline open with padding. Neither path has an independent progress
/// check behind that floor: the buffered loop (`receive_and_pay`) resets its deadline inside
/// the `ChunkData` arm, and the progressive path ([`UpstreamPull::next_chunk`]) re-arms a
/// fresh per-call `tokio::time::timeout` on every read. Both are safe because no frame a
/// peer can send makes zero progress, not because either verifies that it did. Since the
/// #1145 review the floor is structural rather than advisory — `ChunkData`'s field is
/// private, and its constructor and decode gate both reject an empty payload — so it cannot
/// be relaxed by forgetting to call a validator.
///
/// **At least one byte having already arrived.** The reset is what makes a stall the peer's
/// fault, so before the FIRST byte there has been no reset and the argument does not apply:
/// the clock is measuring the server's time-to-first-byte, which scales with blob size
/// (the serve path materialises the whole bao wire via `export_bao_range` before it can emit
/// chunk #1). Both loops therefore raise [`PullTimeout`] — exonerating — when the budget
/// elapses at `cumulative == 0`, and this sentinel only once bytes have flowed (#1145
/// review). Without that split, a 1 GiB blob off a cold disk gossiped an honest server as
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
/// anybody, and would previously walk the candidate list tarring every honest
/// provider it met with an `Unreachable` — a local EWMA hit; ADR 008 scoring is
/// local-only — on the strength of its own fault. Attach this marker at a
/// local-fault site and the classifier exonerates the peer and warns about us instead.
///
/// # It now also decides what a CLIENT is told (#1560)
///
/// Reputation is no longer the only consequence. A second consumer — the node crate's
/// `record_channel_open_failure` — reads this marker to choose between answering a
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

/// The bounds on a pull, each matched to the stage it governs (#1134).
///
/// The two are not interchangeable, and conflating them is the bug this type
/// exists to prevent. A single whole-transfer deadline — what this replaced — is
/// mostly useless as a health signal: it has to be sized against `blob size ×
/// link speed`, so it kills legitimate large or slow-but-healthy transfers while
/// a value small enough to catch a dead peer quickly cannot serve a big blob at
/// all. The operator ends up tuning a number that has nothing to do with node
/// health.
///
/// Split by stage instead:
///
/// - **`stall`** bounds the STREAMING stage by INACTIVITY. It resets on every byte
///   of progress, so it trips only when the provider goes unresponsive — the thing
///   a timeout should catch — and is indifferent to transfer size and link speed.
///   This is the primary mechanism.
/// - **`hard_cap`** is an optional overall wall-clock escape hatch, `None` by
///   default. It exists for a caller that must bound total runtime regardless; it
///   is not how a stalled peer is detected.
///
/// - **`open`** bounds the OPEN stage (dial → request → verified `StreamResponse`)
///   by WALL CLOCK. That stage is bounded work whose duration does not depend on
///   the blob, so a slow one really is a stall and a wall clock is the right tool.
///
/// Every stage must carry a bound of its own. It is not enough for a caller to
/// wrap the whole pull in a timeout and call the open "bounded": the buffered
/// path's handshake happens *inside* [`stream_fetch_tracked`], so a caller that
/// sets `hard_cap: None` would leave the `StreamResponse` read with no bound at
/// all, and a peer that accepts a connection and then says nothing would hang the
/// pull forever. `open` exists so that cannot be expressed.
/// # The relational invariant
///
/// `hard_cap`, when set, must STRICTLY EXCEED `open + stall`. Both clocks below run inside
/// the cap's, and in the worst case consecutively — the open can legitimately consume its
/// whole budget before the inactivity clock even starts — so a cap that does not outlast
/// both means the cap always fires first and [`PullStalled`] can never fire under it. The
/// pull then looks fully configured while its peer-health signal is dead.
///
/// [`Self::capped`] is fallible and the fields are private BECAUSE of that (#1145 review).
/// The invariant was previously enforced nowhere on this type: the fields were `pub`, both
/// production call sites built it with a struct literal, and the check lived in the CLI's
/// `ClientFetchArgs::validate` as a hardcoded `timeout > 2 × stall` — correct only because
/// those two call sites happened to set `open` from the same knob as `stall`. Adding an
/// `--open-timeout-ms` flag would have made it silently wrong, in the direction that reopens
/// the hole. The invariant belongs to the type that has the three values.
#[derive(Debug, Clone, Copy)]
pub struct PullDeadlines {
    /// Wall-clock bound on the open stage: dial, request, and the signed
    /// `StreamResponse`. Bounded work — a slow one is a stall.
    open: Duration,
    /// Inactivity bound on the streaming stage. Reset on every byte of progress.
    stall: Duration,
    /// Optional overall wall-clock cap on the whole exchange. `None` = uncapped;
    /// `open` and `stall` between them are what keep an uncapped pull from hanging.
    hard_cap: Option<Duration>,
}

/// A [`PullDeadlines`] whose bounds cannot do their job. Carries the three values so a CLI
/// can render the arithmetic back to the user rather than just saying "invalid".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineError {
    /// A zero budget elapses on its first poll, so the stage it bounds can never run.
    ZeroBudget,
    /// The cap does not outlast `open + stall`, so it always fires first and the stall
    /// bound — the only signal that says anything about the PEER — can never fire.
    CapCannotOutlastItsStages {
        open: Duration,
        stall: Duration,
        hard_cap: Duration,
    },
}

impl std::fmt::Display for DeadlineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroBudget => write!(f, "a deadline of zero elapses before any work can run"),
            Self::CapCannotOutlastItsStages {
                open,
                stall,
                hard_cap,
            } => write!(
                f,
                "the overall cap ({hard_cap:?}) must exceed the open bound ({open:?}) plus the \
                 stall bound ({stall:?}): both run inside it and in the worst case \
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
    /// [`DeadlineError::ZeroBudget`] for a zero `open` or `stall`.
    ///
    /// This constructor used to be infallible, on the reasoning that "a zero `open` or
    /// `stall` is still nonsense, but it is a bound that fires too EAGERLY — loud, and
    /// immediately obvious — rather than one that silently never fires". That is exactly
    /// backwards, and this same PR says so 800 lines away in `config`: a zero stall "trips
    /// `PullStalled` on the first poll of every streaming read … it would broadcast false
    /// `Unreachable` observations about every honest peer it touches."
    ///
    /// A zero stall is not loud. It is a node quietly gossiping defamation of the whole
    /// network, at full speed. The only thing standing between config and that state was a
    /// `> 0` check in a resolver in another crate — the same advisory-invariant shape
    /// `ChunkData` had before #1088, and the reason this type owns its bounds at all.
    pub const fn new(open: Duration, stall: Duration) -> Result<Self, DeadlineError> {
        if open.is_zero() || stall.is_zero() {
            return Err(DeadlineError::ZeroBudget);
        }
        Ok(Self {
            open,
            stall,
            hard_cap: None,
        })
    }

    /// As [`Self::new`], plus an overall wall-clock cap on the whole exchange.
    ///
    /// # Errors
    ///
    /// [`DeadlineError::CapCannotOutlastItsStages`] if `hard_cap` does not strictly exceed
    /// `open + stall`, and [`DeadlineError::ZeroBudget`] for a zero `open` or `stall`. See
    /// the type's own docs for why this is the one constructor that must be fallible.
    pub fn capped(
        open: Duration,
        stall: Duration,
        hard_cap: Duration,
    ) -> Result<Self, DeadlineError> {
        if open.is_zero() || stall.is_zero() {
            return Err(DeadlineError::ZeroBudget);
        }
        if hard_cap <= open.saturating_add(stall) {
            return Err(DeadlineError::CapCannotOutlastItsStages {
                open,
                stall,
                hard_cap,
            });
        }
        Ok(Self {
            open,
            stall,
            hard_cap: Some(hard_cap),
        })
    }

    /// The open-stage wall clock.
    #[must_use]
    pub const fn open(&self) -> Duration {
        self.open
    }

    /// The streaming-stage inactivity bound.
    #[must_use]
    pub const fn stall(&self) -> Duration {
        self.stall
    }

    /// The overall cap, if any.
    #[must_use]
    pub const fn hard_cap(&self) -> Option<Duration> {
        self.hard_cap
    }

    /// The legacy single-deadline shape: one budget serving as the open bound, the
    /// stall bound, AND the overall cap. **Test-only — do not reach for this in
    /// production.** That conflation is exactly what #1134 set out to remove, and the
    /// name reads far more like a legitimate policy choice than it is.
    ///
    /// Note what it quietly costs, beyond re-introducing the size-coupled deadline:
    /// because `hard_cap == stall`, and the cap's clock starts at the top of the whole
    /// exchange while the stall clock starts only once the open has completed, **the hard
    /// cap always elapses first — so [`PullStalled`] can never fire under it.** A pull
    /// built this way silently cannot detect a stalled peer, and so cannot score one.
    /// Every loopback test using this helper is exercising a pull with the stall
    /// signal disabled; the stall path is covered by
    /// `node_origin_mid_stream_silence_scores_stalled_upstream`, which builds its
    /// deadlines explicitly.
    ///
    /// That is a statement about the BUFFERED path, where `with_hard_cap` wraps the whole
    /// exchange. The progressive path never consults `hard_cap` at all (see
    /// [`open_progressive_pull`]), so a `whole_transfer` used there would leave `PullStalled`
    /// perfectly able to fire — which is not a reprieve, just a different reason not to
    /// reach for this (#1145 review).
    ///
    /// It is also the reason this constructor stays infallible while [`Self::capped`] is not:
    /// it deliberately builds the very state `capped` refuses.
    ///
    /// TEST-ONLY, and now unrepresentable in production by construction: gated behind the
    /// `test-util` feature (#1145 review), so a production caller cannot name it and reach for
    /// the zero-stall / uncapped state — it wants [`Self::new`] (stall-bounded) or
    /// [`Self::capped`] (stall-bounded with a leak guard). Used by the loopback helper
    /// `stream_fetch` and directly by the `client_loopback` suite, whose blobs are small
    /// enough that none of this matters.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub const fn whole_transfer(timeout: Duration) -> Self {
        Self {
            open: timeout,
            stall: timeout,
            hard_cap: Some(timeout),
        }
    }
}

/// Fetch `hash` from `target` over `cdn/client/v1`, paying as bytes arrive.
///
/// `expected_signer` is the delivering node's Ethereum address, used to verify
/// the response `slash_sig`. `byte_offset` resumes a partial fetch. Use
/// [`stream_fetch_tracked`] instead if you need to persist the voucher watermark
/// the channel reached (#852); this convenience wrapper discards it.
///
/// TEST-ONLY: gated behind the `test-util` feature alongside
/// [`PullDeadlines::whole_transfer`], the single-deadline shape it passes (#1145 review).
/// Production callers use [`stream_fetch_tracked`] directly with a split [`PullDeadlines`].
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
    ctx: &ChannelContext,
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
        // `stream_fetch` is a test/loopback convenience for node-to-node pulls; a
        // client that routes on a namespace calls `stream_fetch_tracked` directly.
        decdn_protocol::client::NO_NAMESPACE,
        byte_offset,
        timestamp_us,
        // The legacy single-deadline shape (#1134): this helper's callers are
        // loopback tests moving tiny blobs, for which one budget serving as both
        // the overall cap and the stall bound is harmless. Production paths take
        // `PullDeadlines` directly and split the two.
        PullDeadlines::whole_transfer(timeout),
        // No buyer-side blob-size or rate ceiling on this test/loopback helper. The
        // production pull path does not go through here — it calls
        // `stream_fetch_tracked` directly (`node_origin::pull_from_candidate`)
        // with its configured `max_blob_size_bytes` / `max_rate_per_mb`.
        0,
        0,
        &mut VoucherProgress::default(),
    )
    .await
}

/// Like `stream_fetch`, but reports the channel's acked voucher watermark via
/// the `progress` out-param so the caller can persist what it paid (#852).
///
/// `progress` is an out-param: on return it holds the cumulative `(nonce,
/// bytes_delivered, amount)` of the last *acked* voucher. Internally the pull
/// runs against a one-shot [`ChannelLedger`] seeded from `ctx.prior_*`; the
/// ledger's snapshot is copied back into `progress` on every return path — `Ok`,
/// `Err`, or timeout — so the caller can record progress even for a mid-stream
/// failure or a paid-but-corrupt delivery. See [`VoucherProgress`].
///
/// # Errors
///
/// Same as `stream_fetch`.
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_tracked(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
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

/// Delivery-progress callback: invoked with `(wire_bytes_received,
/// wire_bytes_expected)` after each chunk arrives, so a caller (e.g. `decdn
/// fetch`) can render a progress bar. Both counts are **wire** bytes — bao
/// content plus interleaved proof nodes (ADR 038 §Payment metering) — matching
/// the receive loop's own accounting; `wire_bytes_expected` is the aligned wire
/// length, known from the signed `StreamResponse` before the first chunk and
/// constant across the pull. It must not panic (it runs inside the hot receive
/// loop) and must be `Send + Sync` so the pull future stays spawnable.
pub type ProgressCallback = dyn Fn(u64, u64) + Send + Sync;

/// Like [`stream_fetch_tracked`], but also reports per-chunk delivery progress
/// through `on_progress` (see [`ProgressCallback`]) — the byte-progress hook the
/// watermark-only [`VoucherProgress`] out-param does not provide. `None` behaves
/// exactly like [`stream_fetch_tracked`].
///
/// # Errors
///
/// Same as `stream_fetch`.
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_tracked_with_progress(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
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
    let ledger = ChannelLedger::new(Cumulative {
        nonce: ctx.prior_nonce,
        bytes: ctx.prior_bytes_delivered,
        amount: ctx.prior_amount,
    });
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
            deadlines.open,
            deadlines.stall,
            &ledger,
            on_progress,
        ),
    )
    .await;
    // Copy the acked watermark back into `progress` on EVERY return path (Ok, Err,
    // timeout) BEFORE returning, so the latest acked totals survive a mid-stream
    // failure or a paid-but-corrupt delivery (#852). The ledger advances `committed`
    // only after an ack, so it is exactly the last acked cumulative. A completed pull
    // has read the ack for every voucher it sent (the closing ack precedes `StreamEnd`
    // on the wire), so `committed` carries the whole transfer here.
    progress.set_from_cumulative(ledger.committed(), ctx.prior_nonce);
    result
}

/// Apply the optional overall wall-clock cap of a [`PullDeadlines`] to `fut`.
///
/// `None` runs the pull uncapped — which is safe precisely because the `stall`
/// bound inside bounds every streaming read. A pull with no cap cannot hang; it
/// can only take as long as the upstream keeps feeding it bytes, which is the
/// point (#1134).
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
/// [`ChannelLedger`] so multiple concurrent pulls on ONE payment channel coordinate.
///
/// The bug this fixes: each `stream_fetch`/`stream_fetch_tracked` call seeds its
/// own voucher state from `ctx.prior_*`, so N concurrent pulls on the same channel
/// all sign the next voucher at `prior_nonce + 1` and collide — the node accepts
/// exactly one and rejects the rest as `StaleNonce`. Passing every concurrent
/// caller the SAME `&ChannelLedger` (typically an `Arc<ChannelLedger>` shared
/// across `tokio::spawn`/`join!`) serializes their voucher issuance through the
/// ledger's mutex: each issues the next nonce in turn, the channel advances
/// monotonically, and all pulls succeed.
///
/// The caller owns the ledger's lifetime and persists what the channel paid from it
/// directly (this entrypoint does not surface a [`VoucherProgress`] — the shared ledger
/// IS the watermark). Persist via [`ChannelLedger::settlement`], NOT `snapshot`:
/// `snapshot`/`committed` report only ACKED vouchers, so a voucher left in the ack wait
/// (the drop the node's `SettleOnDrop` guard handles) is under-reported and its deposit
/// stranded — `settlement` adds the in-flight voucher back (#1122/#1145). `snapshot` is
/// also `async`, so a `Drop` guard cannot call it at all.
///
/// # Errors
///
/// Same as `stream_fetch`.
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_shared(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    ledger: &ChannelLedger,
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
            // Shared-channel pulls are node-to-node cache-miss fills (the daemon
            // as buyer); the requester already discovered the holder.
            decdn_protocol::client::NO_NAMESPACE,
            byte_offset,
            timestamp_us,
            max_blob_size_bytes,
            max_rate_per_mb,
            deadlines.open,
            deadlines.stall,
            ledger,
            // Shared concurrent pulls interleave many blobs on one channel; a
            // single unified byte-progress readout would be meaningless, so this
            // path never reports progress.
            None,
        ),
    )
    .await
}

/// The OPEN stage of a `cdn/client/v1` pull, shared by the buffered
/// [`fetch_inner`] and the progressive [`open_progressive_pull`] so the two cannot
/// drift: dial, open the bi-stream, send the [`StreamRequest`], and read + verify
/// the signed [`StreamResponse`]. Returns the live connection, its streams, and the
/// verified response; the caller decides whether to buffer or stream from there.
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
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    byte_len: u64,
    timestamp_us: u64,
    open: Duration,
) -> anyhow::Result<(
    iroh::endpoint::Connection,
    SendStream,
    RecvStream,
    StreamResponse,
)> {
    tokio::time::timeout(open, async move {
        let conn = endpoint
            .connect(target, ALPN_CLIENT)
            .await
            .map_err(|e| anyhow::anyhow!("connect failed: {e}"))?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi failed: {e}"))?;

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
            channel_id: ctx.channel_id.into(),
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
            .map_err(|e| anyhow::anyhow!("write stream request: {e}"))?;

        let resp = match read_client_message(&mut recv).await? {
            ClientMessage::StreamResponse(r) => r,
            other => anyhow::bail!("expected StreamResponse, got {}", variant_name(&other)),
        };
        verify_response(
            &resp,
            slash_domain,
            expected_signer,
            hash,
            ctx.channel_id,
            timestamp_us,
        )?;
        Ok((conn, send, recv, resp))
    })
    .await
    .map_err(|_| anyhow::Error::new(PullTimeout { after: open }))?
}

/// Wallet-less resume (issue #1481 §5): the maximum number of times a fetch
/// will reopen a fresh stream after a gated, bundled
/// `StaleNonce`/`AmountRegression`/`BytesRegression`/`InsufficientDeposit`
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
/// `working_deposit` after a genuine mid-fetch `InsufficientDeposit` (validated
/// against the buyer's own ledger via [`genuine_exhaustion`]) and resume at the
/// PAID FRONTIER ([`sink::content_paid_frontier`]) — not at the failed leg's own
/// offset, which would re-pay for the credited-but-unpaid tail. Separate from [`MAX_RESUME_ATTEMPTS`]: a top-up is a funding
/// action with its own on-chain cost and failure mode (a delegate key that
/// cannot fund, an allowance that fails to land), not a wallet-less resume, so
/// it is bounded on its own budget rather than sharing/competing with the resume
/// attempts.
pub const MAX_TOPUP_ATTEMPTS: u32 = 3;

/// Buffered fetch with wallet-less resume (issue #1481 §5): if a mid-stream
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
/// channel's cumulative would, for every caller that requested
/// `byte_offset == 0` (previously the CLI's buffered `fetch_blob` /
/// bundle-pull path; both CLI commands now stream through
/// `open_progressive_pull` instead, so only `stream_fetch`'s test-only
/// callers still reach this wrapper at that offset), silently return a
/// TRUNCATED tail instead of the full blob the caller is relying on getting
/// back. The bytes already streamed in the failed attempt were never decoded
/// (the error path never reaches `decode_verified_range`), so there is
/// nothing to legitimately splice a jump with. Retrying at the caller's
/// original offset is always safe and is the only thing this bundle field
/// is actually needed for: fixing the ledger's amount/nonce/bytes BASELINE
/// so the resumed stream's vouchers verify against what the node now
/// expects, not re-deriving where in the blob to resume.
///
/// This is the ONLY retry loop in the crate for this case —
/// [`UpstreamPull::pay_one`]/[`receive_and_pay`] never retry themselves, because the
/// stream they hold is already dead by the time a mid-stream
/// `VoucherRejected` reaches them (the node finishes its send side before
/// replying, `handlers/client/wire.rs::write_reject`); a fresh stream can
/// only be opened by whoever owns the connection, which is here.
///
/// Every caller of the BUFFERED path — `stream_fetch` (test-only),
/// [`stream_fetch_tracked`], [`stream_fetch_tracked_with_progress`], and
/// [`stream_fetch_shared`] — goes through this wrapper, so a resumable rejection
/// on any of those paths is retried transparently and the caller only ever sees
/// the FINAL outcome (success, or the original terminal error once
/// [`MAX_RESUME_ATTEMPTS`] is exhausted or the reason/bundle isn't eligible).
///
/// The daemon's node-to-node cache-miss buyer leg USED to be one of those callers
/// (via [`stream_fetch_shared`], always at `byte_offset == 0`). Since #1530 it does
/// not: it drives its own resume loop over [`open_progressive_pull`] in
/// `decdn-node`'s `node_origin/resume.rs`, which reseeds on the same contract and
/// additionally answers a genuine `InsufficientDeposit` with an on-chain top-up —
/// the thing a from-zero buffered retry could never do without re-paying for the
/// delivered prefix. Either way `pull_verdict` / `voucher_verdict` in `decdn-node`
/// see only the terminal outcome, so the `OurDeadChannel` classification there
/// stays correct as the fallback.
///
/// A bundle-less rejection, a non-gated reason, or a bundle that fails
/// [`WatermarkBundle::validate`] (malformed `last_signature` length) is
/// never treated as resumable and is returned to the caller unchanged on
/// the first attempt, exactly as before this feature existed.
#[allow(clippy::too_many_arguments)]
async fn fetch_inner(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    open: Duration,
    stall: Duration,
    ledger: &ChannelLedger,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<Bytes> {
    for attempt in 0..=MAX_RESUME_ATTEMPTS {
        let result = fetch_inner_once(
            endpoint,
            target.clone(),
            ctx,
            slash_domain,
            expected_signer,
            hash,
            namespace_id,
            byte_offset,
            timestamp_us,
            max_blob_size_bytes,
            max_rate_per_mb,
            open,
            stall,
            ledger,
            on_progress,
        )
        .await;
        let Err(e) = result else {
            return result;
        };
        if attempt == MAX_RESUME_ATTEMPTS {
            return Err(e);
        }
        match resumable_watermark(&e, ctx) {
            // A bundle that does not ADVANCE our committed watermark is not a
            // desync: the node attaches one to every watermark-gated rejection once
            // any voucher has been accepted, so an exhausted channel echoes our own
            // watermark straight back. Retrying against it is futile — and applying
            // it would regress the ledger — so surface the real error instead of
            // spending the resume budget on it.
            Some(bundle) if ledger.reseed(Cumulative::from(bundle)) => {
                tracing::debug!(
                    attempt,
                    byte_offset,
                    "voucher rejection carried an authenticated watermark bundle; reseeded and \
                     retrying at the same byte_offset"
                );
            }
            Some(_) | None => return Err(e),
        }
    }
    // Unreachable: the loop above always returns on both the `Ok` and every `Err`
    // branch (either directly or after `attempt == MAX_RESUME_ATTEMPTS`
    // triggers on the final iteration). Kept as a typed bail rather than
    // `unreachable!()`/`panic!()` per the workspace anti-panic policy.
    Err(anyhow::anyhow!("resume loop exited without returning"))
}

/// The security-critical core shared by every "did WE sign this?" check: does `signature` recover
/// to `expected` over `voucher` under `domain`?
///
/// A node's claim about a channel's watermark arrives over an unauthenticated application-level
/// message — a mid-stream `StreamError` on the fetch path (#1042), a `CooperativeCloseAuth` on the
/// close path (#1495) — so the claimed `amount`/`nonce`/`bytes_delivered` are attacker-controllable
/// on the wire. Without this check a malicious or buggy upstream could hand back an inflated
/// watermark and have the client act on it: `reseed` its ledger and then sign a voucher for the
/// echoed `amount + delta` on the retried pull, or sign away the echoed `amount` outright on a
/// cooperative close. Either is a voucher this client genuinely holds the key to sign and the node
/// can redeem on-chain up to the deposit, draining the channel while delivering ~nothing.
///
/// The client's OWN last-accepted voucher signature closes that hole: a node can only echo back a
/// signature the client itself produced, so a tuple that verifies is by construction one this client
/// already committed to. That the node's stored signature always matches its stored tuple is the
/// write-side invariant of `decdn_incentive::ChannelState::accept_voucher`, which writes
/// `last_amount`/`last_nonce`/`last_bytes_delivered`/`last_signature` together and only after
/// verifying the signature against the channel's pinned `voucher_signer`. A signature that does not
/// recover to `expected` is treated as a hostile or corrupt echo, never as evidence.
///
/// Callers MUST verify over the exact tuple they are about to adopt as their new committed baseline
/// — never a tuple derived from it — so there is no window in which the proof covers one state and
/// the action commits another. Taking the whole [`Voucher`] rather than its fields loose is what
/// lets a caller verify and then act on *the same value*: `prepare_close` builds one voucher, proves
/// it here, and signs that same voucher.
#[must_use]
pub fn voucher_signed_by(
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
/// [`WatermarkBundle::validate`] (malformed `last_signature` length), or — the security-critical
/// check — `last_signature` does not recover to this client's OWN voucher-signing address over
/// the bundle's `amount`/`nonce`/`bytes_delivered`.
///
/// That last check is [`voucher_signed_by`]; see its doc for why acting on an unverified watermark
/// is a channel-draining hole rather than a robustness nicety.
pub fn resumable_watermark<'a>(
    err: &'a anyhow::Error,
    ctx: &ChannelContext,
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
        channel_id: ctx.channel_id,
        amount: U256::from_be_bytes(bundle.amount),
        nonce: U256::from_be_bytes(bundle.nonce),
        bytes_delivered: U256::from_be_bytes(bundle.bytes_delivered),
        token: ctx.token,
    };
    voucher_signed_by(
        &claimed,
        &bundle.last_signature,
        ctx.client_signer.address(),
        &ctx.voucher_domain,
    )
    .then_some(bundle)
}

/// True iff `err` is an `InsufficientDeposit` voucher rejection that the buyer's OWN ledger
/// corroborates as genuine exhaustion, AND the buyer's remaining spendable deposit is below the
/// cost of the next voucher. A node claiming `InsufficientDeposit` while the buyer's ledger
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
/// ordinary, real exhaustion on a channel that has already had some vouchers accepted. Treating
/// bundle PRESENCE alone as "this is a desync" would misroute every such real exhaustion into
/// the resync path (which cannot fix it — the deposit is actually short — and eventually fails
/// after burning `MAX_RESUME_ATTEMPTS`) instead of the top-up path that could. The bundle is only
/// evidence of desync when it reports a watermark AHEAD of what we already hold — i.e. the node
/// knows about a voucher we do not, which reseeding can heal. A bundle that merely echoes back
/// our OWN already-committed watermark (at or behind `committed`) proves nothing about desync;
/// it is the node correctly reporting the state we already agree on, and the exhaustion is real.
pub fn genuine_exhaustion(
    err: &anyhow::Error,
    ctx: &ChannelContext,
    committed: Cumulative,
    remaining_spendable: U256,
    next_voucher_cost: U256,
) -> bool {
    let Some(rejected) = err.downcast_ref::<UpstreamVoucherRejected>() else {
        return false;
    };
    if rejected.reason != VoucherRejectReason::InsufficientDeposit {
        return false;
    }
    // An authenticated bundle that ADVANCES our committed nonce is a healable desync — let the
    // resume loop reseed instead of adding funds. A bundle that is absent, or present but at or
    // behind `committed`, proves no desync (see the doc comment above), so exhaustion can still
    // be genuine.
    if let Some(bundle) = resumable_watermark(err, ctx)
        && Cumulative::from(bundle).nonce > committed.nonce
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

#[allow(clippy::too_many_arguments)]
async fn fetch_inner_once(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
    max_rate_per_mb: u64,
    open: Duration,
    stall: Duration,
    ledger: &ChannelLedger,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<Bytes> {
    let (conn, mut send, mut recv, resp) = open_stream(
        endpoint,
        target,
        ctx,
        slash_domain,
        expected_signer,
        hash,
        // A client fetch routes on the namespace it published under (ADR 005
        // §Namespace routing). The node-to-node callers that reach *this*
        // function (shared-channel / loopback fills) pass NO_NAMESPACE — the
        // requester already discovered a holder, so the downstream node needs no
        // hint (ADR 002 §Retrieval by namespace). The directory-discovered
        // cold-origin leg, which *does* carry a real namespace, goes through
        // `open_progressive_pull` → `open_stream` directly, not here.
        namespace_id,
        byte_offset,
        // Whole-tail fetch; a bounded range is plumbed via `open_progressive_pull`
        // (the gap-driven driver, #1608), not this buffered/loopback path.
        0,
        timestamp_us,
        open,
    )
    .await?;

    if !resp.body.ok {
        return Err(UpstreamRefused::open(resp));
    }
    if resp.body.redirect.is_some() {
        anyhow::bail!("server returned a redirect; following redirects is out of scope (#317)");
    }
    // Reject an oversized server-claimed `total_bytes` before allocating or
    // entering the receive loop — `total_bytes` is server-controlled and
    // `StreamResponse::validate()` does not bound it, so the in-loop
    // `cumulative > expected` guard alone would let one inflated promise drive
    // us toward OOM. Mirrors the serving-side `BlobTooLarge` gate
    // (handlers/client.rs); `0` = unlimited (#840). Typed sentinel so the pull
    // orchestrator classifies it as a buyer-side policy rejection, not provider
    // misbehavior.
    if max_blob_size_bytes > 0 && resp.body.total_bytes > max_blob_size_bytes {
        return Err(anyhow::Error::new(BlobTooLargeClaim {
            claimed: resp.body.total_bytes,
            ceiling: max_blob_size_bytes,
        }));
    }
    // Reject an over-ceiling rate before paying a single voucher (#1375). `resp`
    // is `ok == true` and already verified against `expected_signer`, so it is
    // the operator's own signed quote — carry it into the error as replayable
    // rate-manipulation evidence. `0` = unbounded.
    if max_rate_per_mb > 0 && resp.body.rate_per_mb > max_rate_per_mb {
        return Err(RateAboveCeiling::over_ceiling(resp, max_rate_per_mb));
    }
    // A `total_bytes` below `byte_offset` would underflow the wire bound to `0`
    // (saturating), so the loop ends on the first `StreamEnd` and returns an
    // empty buffer. An empty range decodes trivially (no chunk group to verify),
    // so that empty buffer would surface as success — a silent verification
    // bypass. A legitimate server always claims `total_bytes >= byte_offset`;
    // reject anything less before the loop. (A non-empty but *short* delivery is
    // caught by the completeness check after the loop.)
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
    let interval_bytes = resp
        .voucher_interval_mb
        .unwrap_or(DEFAULT_VOUCHER_INTERVAL_MB)
        .saturating_mul(MB_BYTES);
    // Paid/received bytes are **wire** bytes — content plus interleaved bao proof
    // nodes (ADR 038 §Payment metering) — not the content-byte remainder.
    let total_bytes = resp.body.total_bytes;
    let expected_wire = aligned_wire_len(byte_offset, 0, total_bytes)?;

    let (buf, cumulative) = receive_and_pay(
        &mut send,
        &mut recv,
        ctx,
        ledger,
        rate_per_mb,
        interval_bytes,
        expected_wire,
        stall,
        on_progress,
    )
    .await?;

    // A truncated stream (fewer wire bytes than the aligned range needs) cannot
    // decode; reject cleanly before the decoder hits an EOF mid-proof. There is no
    // whole-blob-hash fallback anymore, so this bound applies to every fetch
    // (full and resumed) rather than only resumes.
    if cumulative < expected_wire {
        conn.close(0u32.into(), b"short-delivery");
        anyhow::bail!(
            "server sent {cumulative} of {expected_wire} promised wire bytes before StreamEnd"
        );
    }
    // Decode the bao interleaved stream, verifying every chunk group against the
    // content-hash root, and trim to the requested span. A corrupt group at ANY
    // offset (including a resumed tail) yields `HashMismatch` — the resume gap is
    // closed. `HashMismatch` stays the typed sentinel so callers `downcast_ref` to
    // classify a paid-but-corrupt delivery.
    let blob = match decode_verified_range(hash, total_bytes, byte_offset, 0, buf.as_ref()) {
        Ok(blob) => blob,
        Err(e) => {
            conn.close(0u32.into(), b"verify-failed");
            return Err(e);
        }
    };
    conn.close(0u32.into(), b"done");
    Ok(blob)
}

/// The wire-byte bound for a fetch of `[byte_offset, byte_offset + byte_len)`
/// (`byte_len == 0` meaning "to end") of a `total_bytes` blob: the bao-encoded
/// size of the chunk-group-aligned range (content plus interleaved proof, ADR
/// 038 §Payment metering), exactly the byte count the server emits. The server
/// widens the request to enclosing 16 KiB groups; [`align_range`] /
/// [`AlignedRange::wire_len`](decdn_bao_range::AlignedRange::wire_len)
/// reproduce that, keeping encoder and receiver in lock-step. Shared by the
/// buffered [`fetch_inner`] and progressive [`open_progressive_pull`] paths so
/// the two can't drift.
fn aligned_wire_len(byte_offset: u64, byte_len: u64, total_bytes: u64) -> anyhow::Result<u64> {
    let aligned = align_range(byte_offset, byte_len, total_bytes)
        .map_err(|e| anyhow::anyhow!("range alignment: {e}").context(LocalPullFault))?;
    Ok(aligned.wire_len())
}

/// Drive the buffered receive loop: read `ChunkData` into a buffer, paying one
/// voucher per `interval_bytes` boundary (and a closing voucher once all
/// `expected_wire_bytes` have arrived) through the shared `ledger`, until
/// `StreamEnd`. Returns the assembled buffer and the cumulative byte count for
/// the caller's completeness check (integrity is verified per bao chunk group by
/// the decoder, not here). Enforces `ChunkData`'s bounds — the non-empty FLOOR and the
/// size ceiling (#1088) — plus the `cumulative <= expected_wire_bytes` overrun guard
/// (ADR 005 §`cdn/client/v1`). The floor is the load-bearing one: it is what lets the
/// inactivity deadline below rest on frame arrival, since an empty frame would refresh
/// the clock while advancing nothing.
///
/// `stall` bounds this loop by INACTIVITY (#1134): every read must land within
/// `stall` of the last byte of progress, so the loop is bounded no matter how
/// large the blob or how slow the link, and a silent upstream is abandoned
/// promptly rather than left to the QUIC idle timeout.
#[allow(clippy::too_many_arguments)]
async fn receive_and_pay(
    send: &mut SendStream,
    recv: &mut RecvStream,
    ctx: &ChannelContext,
    ledger: &ChannelLedger,
    rate_per_mb: u64,
    interval_bytes: u64,
    expected_wire_bytes: u64,
    stall: Duration,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<(BytesMut, u64)> {
    let mut buf = BytesMut::new();
    let mut cumulative: u64 = 0;
    // Bytes received but not yet covered by a voucher — the per-voucher *delta*
    // handed to the ledger (which accumulates deltas across this and any concurrent
    // streams on the channel, advancing only after the upstream acks).
    let mut bytes_since_voucher: u64 = 0;
    // The inactivity deadline (#1134). Reset only inside the `ChunkData` arm below —
    // never on a non-chunk frame — and a `ChunkData` that exists carries at least one
    // byte, because `ChunkData::new` and the decode gate are the only ways to obtain
    // one (#1088). So every refresh of this clock is paid for in bytes, which is the
    // property `PullStalled` rests on: a peer cannot hold the deadline open with padding.
    let mut deadline = tokio::time::Instant::now() + stall;

    loop {
        let msg = tokio::time::timeout_at(deadline, read_client_message(recv))
            .await
            // Which fault this is depends on whether a byte has EVER arrived (#1145 review).
            //
            // `PullStalled` scores the peer `Unreachable` — a local EWMA hit and a GOSSIPED
            // observation — and it earns that right from the reset above: a clock that
            // resets on every byte can only fire on a peer that stopped delivering. That
            // reasoning holds for every chunk but the first, where no byte has reset it yet
            // and the clock is measuring something else entirely.
            //
            // What it measures before the first chunk is the server's time-to-first-byte,
            // and that scales with BLOB SIZE: the serve path writes the `StreamResponse`
            // first, then materialises the whole bao wire encoding via `export_bao_range`
            // before it can emit chunk #1. A 1 GiB blob off a cold disk can exceed the 20 s
            // default — so an honest server, doing exactly what it was asked, got gossiped as
            // unreachable for being big.
            //
            // A wait on bounded-but-unpredictable server work is what the OPEN stage already
            // is, and the open bound already answers this the right way: it raises
            // `PullTimeout`, which is exonerating, on the grounds that our own budget
            // elapsing says nothing about the peer. This is the same wait one stage later, so
            // it gets the same answer. Nothing is lost that the open stage has not already
            // given up: a peer that accepts and then says nothing is unscored there too, and
            // it still fails the pull and yields the candidate slot.
            //
            // The alternative — keeping the peer-blaming verdict and widening the budget —
            // cannot work, because no fixed budget can separate "large blob, honest server"
            // from "dead peer" when the honest case is unbounded in blob size.
            //
            // But be precise about what this does and does not fix (#1145 review). It fixes
            // the ATTRIBUTION: an honest server with a slow first byte is no longer gossiped
            // as unreachable. It does NOT make that blob fetchable. The pull still fails —
            // a blob whose server-side materialisation exceeds the stall window is
            // unfetchable on this path. The real repair is on the SERVE side:
            // `export_bao_range` returns an
            // owned `Bytes`, materialising the entire bao encoding before chunk #1 goes out,
            // so TTFB scales with blob size by construction. Streaming it incrementally is
            // what would actually close #1122/#1132; until then this comment must not be read
            // as claiming the 708 MB blob now works.
            //
            // A `PullTimeout` here is metered and, since #1145, SUPPRESSED for
            // `REFUSAL_SUPPRESSION_TTL` — reputation-neutral, but it stops a peer that
            // accepts a stream and then says nothing from burning a candidate slot on every
            // miss forever.
            .map_err(|_| {
                if cumulative == 0 {
                    anyhow::Error::new(PullTimeout { after: stall })
                } else {
                    anyhow::Error::new(PullStalled { after: stall })
                }
            })??;
        match msg {
            ClientMessage::ChunkData(chunk) => {
                // The running total must not exceed what the response promised —
                // otherwise a malicious server could stream unbounded bytes (OOM) and
                // we would overpay (ADR 005 §`cdn/client/v1`). The 1..=CHUNK_SIZE bounds
                // needed no check here: the frame could not have been decoded otherwise.
                let chunk_len = chunk.bytes().len() as u64;
                cumulative = cumulative.saturating_add(chunk_len);
                if cumulative > expected_wire_bytes {
                    anyhow::bail!(
                        "server sent {cumulative} bytes, more than the {expected_wire_bytes} promised"
                    );
                }
                // Bytes arrived: the upstream is alive, so extend the inactivity
                // deadline. It also extends after a completed voucher round trip below;
                // both sites are inside this arm, and reaching this arm means bytes.
                deadline = tokio::time::Instant::now() + stall;
                buf.extend_from_slice(chunk.bytes());
                // Surface delivery progress after each chunk. `cumulative` and
                // `expected_wire_bytes` are both wire bytes, so the readout is
                // consistent (and can't overshoot — the guard above caps it).
                if let Some(cb) = on_progress {
                    cb(cumulative, expected_wire_bytes);
                }
                bytes_since_voucher = bytes_since_voucher.saturating_add(chunk_len);
                // Pay at each interval boundary, and a closing voucher once all
                // expected bytes have arrived — matching the node's pacing. Send the
                // voucher WITHOUT blocking on its ack (#1484): the ack arrives as its
                // own message later in this loop and is resolved into the ledger by the
                // `VoucherAck` arm below, so we keep reading bytes across the round trip
                // rather than stalling a full RTT per interval.
                let boundary = bytes_since_voucher >= interval_bytes && interval_bytes > 0;
                let closing = cumulative >= expected_wire_bytes && bytes_since_voucher > 0;
                if boundary || closing {
                    send_voucher(send, ctx, ledger, rate_per_mb, bytes_since_voucher).await?;
                    bytes_since_voucher = 0;
                }
            }
            // The ack for a voucher we sent optimistically (#1484). Resolve it into the
            // ledger's committed watermark and keep going; a `VoucherRejected` here
            // surfaces the typed payment fault, any other `StreamError` a mid-stream
            // refusal. The upstream is alive as of this message, so refresh the
            // inactivity deadline — but only on the ack, never on a bare frame that
            // carried no progress.
            ack @ (ClientMessage::VoucherAck | ClientMessage::StreamError(_)) => {
                resolve_voucher_slot(ledger, ack)?;
                deadline = tokio::time::Instant::now() + stall;
            }
            ClientMessage::StreamEnd => break,
            other => anyhow::bail!("unexpected message mid-delivery: {}", variant_name(&other)),
        }
    }

    Ok((buf, cumulative))
}

/// Decode and verify the reassembled bao interleaved stream `bao_wire` against the
/// content-hash root `hash`, returning the requested plaintext span
/// `[byte_offset, byte_offset + byte_len)` (`byte_len == 0` ⇒ to end). Every chunk
/// group is checked against the root as it is decoded, so a corrupt group — at any
/// offset, including a resumed tail — is rejected without needing earlier bytes
/// (ADR 038 §Receive side; closes the old `byte_offset > 0` gap).
///
/// The server serves the chunk-group-aligned **superset** of the request (a bao
/// proof anchors whole 16 KiB groups), so the decoder yields
/// `[align.fetch_start, align.fetch_end)` and we trim the leading bytes before
/// `byte_offset` here — the serve side never trims (trimming would break the
/// proof). `bao_wire` must be exactly the header-less response stream the server
/// emitted for `align_range(byte_offset, byte_len, total_bytes)`; the shared
/// [`align_range`]/[`AlignedRange::wire_len`](decdn_bao_range::AlignedRange::wire_len)
/// keep encoder and decoder in lock-step.
///
/// # Errors
///
/// [`HashMismatch`] if any chunk group or the root fails verification (a
/// paid-but-corrupt delivery); a decode/`Io` error (e.g. truncated stream) or an
/// out-of-range trim otherwise.
fn decode_verified_range(
    hash: [u8; 32],
    total_bytes: u64,
    byte_offset: u64,
    byte_len: u64,
    bao_wire: &[u8],
) -> anyhow::Result<Bytes> {
    let aligned = align_range(byte_offset, byte_len, total_bytes)
        .map_err(|e| anyhow::anyhow!("range alignment: {e}").context(LocalPullFault))?;
    // A 0-byte blob (#1054) aligns to an empty range: the decoder below has no
    // chunk group to anchor and would accept the empty stream for ANY root. This
    // is the same trivial-empty-range bypass `fetch_inner` guards against for the
    // `total_bytes < byte_offset` underflow (a different trigger, same root
    // cause). Prove the empty stream against the empty root explicitly; a
    // non-empty requested hash is a paid-but-wrong delivery, so surface the typed
    // `HashMismatch`.
    if total_bytes == 0 {
        if hash != *blake3::hash(&[]).as_bytes() {
            return Err(anyhow::Error::new(HashMismatch));
        }
        return Ok(Bytes::new());
    }
    let tree = BaoTree::new(total_bytes, IROH_BLOCK_SIZE);
    let root = blake3::Hash::from_bytes(hash);
    let chunk_ranges = aligned.chunk_ranges();
    let reader = std::io::Cursor::new(bao_wire);
    // Cap the capacity hint at the received wire length: decoded plaintext can
    // never exceed the bytes actually received, so an untrusted `total_bytes`
    // header (via `fetch_len`) can't drive an over-allocation / OOM.
    let cap = usize::try_from(aligned.fetch_len())
        .unwrap_or(0)
        .min(bao_wire.len());
    let mut plaintext = Vec::with_capacity(cap);
    for item in DecodeResponseIter::new(root, tree, reader, chunk_ranges.as_ref()) {
        match item {
            Ok(BaoContentItem::Leaf(leaf)) => plaintext.extend_from_slice(&leaf.data),
            Ok(BaoContentItem::Parent(_)) => {}
            // A group/leaf/root hash mismatch is the content-addressing violation
            // (ADR 014); surface the typed sentinel so callers classify corruption.
            Err(DecodeError::ParentHashMismatch(_) | DecodeError::LeafHashMismatch(_)) => {
                return Err(anyhow::Error::new(HashMismatch));
            }
            // A short/truncated stream or other IO fault — not provably corruption.
            Err(e) => anyhow::bail!("bao decode failed: {e}"),
        }
    }
    // The decoded buffer spans the aligned superset `[fetch_start, fetch_end)`;
    // trim back to the caller's requested span.
    let lead = usize::try_from(byte_offset.saturating_sub(aligned.fetch_start()))?;
    let want = if byte_len == 0 {
        plaintext.len().saturating_sub(lead)
    } else {
        usize::try_from(byte_len)?
    };
    // Take ownership of the decoded buffer as `Bytes` once, then trim with a
    // zero-copy `slice` view (no second allocation/copy). Guard the upper bound
    // explicitly: `Bytes::slice` panics out of range, and a short decode must
    // surface as a clean error (`lead <= end` always, since `want >= 0`).
    let end = lead.saturating_add(want);
    let bytes = Bytes::from(plaintext);
    if end > bytes.len() {
        anyhow::bail!("decoded range shorter than requested span");
    }
    if lead == 0 && end == bytes.len() {
        return Ok(bytes);
    }
    Ok(bytes.slice(lead..end))
}

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
    /// Upstream voucher cadence in bytes (the buyer pays one voucher per
    /// interval as chunks arrive).
    pub interval_bytes: u64,
}

/// A live, progressive `cdn/client/v1` pull (#856), the streaming counterpart of
/// the buffered `stream_fetch`. Opened by [`open_progressive_pull`] (which has
/// already done the handshake and verified the response), driven chunk-by-chunk
/// via [`Self::next_chunk`], and closed by [`Self::finish`] (a wire-completeness
/// check — integrity is verified per bao chunk group by the tee's decoder and the
/// downstream client's own decoder, not by a whole-blob re-hash) or
/// [`Self::abort`].
///
/// It pays the upstream per voucher interval *inside* `next_chunk` — identical
/// pacing to `stream_fetch` — but yields each chunk to the caller (which
/// forwards it to the paying downstream client and tees it into the cache)
/// instead of buffering the whole blob. This is what lets the serving node cap
/// its speculative exposure to a bounded window rather than fronting the entire
/// upstream cost before any downstream voucher arrives.
///
/// Unlike `stream_fetch_tracked`, the acked voucher watermark is OWNED here (not
/// threaded as a `&mut` out-param) and read back via [`Self::progress`] /
/// returned by `finish`/`abort` — the caller (`node_origin`) persists it. On any
/// exit, the caller MUST call `progress`/`finish`/`abort` to recover the
/// watermark for `record_progress` (#852); a [`Drop`] guard closes the
/// connection if none ran, but cannot return the watermark, so the obligation
/// stands.
///
/// **Deadlines.** The *open* (handshake) phase is bounded inside
/// [`open_progressive_pull`] by `PullDeadlines::open`, via the shared `open_stream`
/// helper — NOT by the caller (#1134). `decdn-node` does additionally wrap the open
/// in its per-candidate budget, but that is belt-and-braces: leaving the bound to
/// the caller is what let the buffered handshake ship unbounded once already.
///
/// The streaming `next_chunk`/`finish` reads are bounded here, by INACTIVITY: each
/// read must land within `stall` of the last byte of progress. Before that they had
/// no application-level bound at all — a silent upstream was left to the QUIC idle
/// timeout, with only the loop's window pacing (it recoups a downstream voucher
/// every window, so it cannot run unboundedly ahead of unpaid demand) standing
/// between a wedged peer and an indefinitely-held serve task.
///
/// A wall clock would be the wrong bound to reach for here: this type exists to
/// stream blobs of any size, so any fixed deadline would either kill a healthy
/// large transfer or be too loose to catch a dead one. Inactivity is indifferent
/// to size and link speed.
pub struct UpstreamPull {
    conn: iroh::endpoint::Connection,
    send: SendStream,
    recv: RecvStream,
    ctx: ChannelContext,
    /// The channel's voucher ledger, SHARED with every other concurrent pull on this
    /// channel (#1145 review). Not a per-pull one-shot: that made two concurrent pulls
    /// both sign `prior_nonce + 1` and collide — see [`stream_fetch_shared`], whose doc
    /// describes the same bug on the buffered path.
    ledger: Arc<ChannelLedger>,
    hash: [u8; 32],
    rate_per_mb: u64,
    interval_bytes: u64,
    /// Inactivity budget for every streaming read (#1134). Reset on byte progress.
    stall: Duration,
    /// Promised **wire** bytes for this stream: the bao-encoded size of the
    /// chunk-group-aligned range (content plus interleaved proof, ADR 038), not
    /// the content-byte remainder. Bounds the receive loop and the closing voucher.
    expected_wire_bytes: u64,
    /// Wire bytes received so far on this stream.
    cumulative: u64,
    /// Wire bytes received since the last voucher.
    unvouchered: u64,
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
/// [`UpstreamPull`] to drive. The same response-validation rules as
/// `stream_fetch` apply — zero-rate rejection, `slash_sig` recovery, echoed
/// field checks, the [`BlobTooLargeClaim`] ceiling, and the
/// `total_bytes >= byte_offset` floor — all enforced BEFORE the first chunk.
///
/// # Errors
///
/// Same set as `stream_fetch` for the handshake/response phase (connect /
/// transport, refused or zero-rate response, bad `slash_sig`, mismatched echoed
/// field, oversized `total_bytes`).
///
/// The open stage is bounded by `deadlines.open`, inside the shared `open_stream`
/// helper — NOT left to the caller (#1134). `node_origin` additionally wraps this
/// call in its per-candidate budget, which is belt-and-braces rather than the sole
/// bound. `deadlines.stall` is the INACTIVITY budget the returned [`UpstreamPull`]
/// carries into every streaming read; `deadlines.hard_cap` is not consulted here
/// (the caller owns the streaming lifetime on this path).
///
/// `ledger` is the CHANNEL's voucher ledger, not this pull's: pass the same
/// `Arc<ChannelLedger>` to every concurrent pull on one channel, exactly as with
/// [`stream_fetch_shared`], or they will each sign `prior_nonce + 1` and collide
/// (#1145 review). The caller reads what to persist from it — including after a drop —
/// via [`ChannelLedger::settlement`].
#[allow(clippy::too_many_arguments)]
pub async fn open_progressive_pull(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    ledger: Arc<ChannelLedger>,
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
) -> anyhow::Result<(UpstreamPullHeader, UpstreamPull)> {
    let stall = deadlines.stall;
    let (conn, send, recv, resp) = open_stream(
        endpoint,
        target,
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
    )
    .await?;
    if !resp.body.ok {
        return Err(UpstreamRefused::open(resp));
    }
    if resp.body.redirect.is_some() {
        anyhow::bail!("server returned a redirect; following redirects is out of scope (#317)");
    }
    // Same buyer-side ceiling as `fetch_inner`: reject an inflated `total_bytes`
    // before forwarding/allocating anything (#840). Typed sentinel so the pull
    // orchestrator classifies it as a buyer policy rejection, not provider fault.
    if max_blob_size_bytes > 0 && resp.body.total_bytes > max_blob_size_bytes {
        return Err(anyhow::Error::new(BlobTooLargeClaim {
            claimed: resp.body.total_bytes,
            ceiling: max_blob_size_bytes,
        }));
    }
    // Same buyer-side rate ceiling as `fetch_inner` (#1375): refuse an over-ceiling
    // quote before the first paid interval, carrying the signed quote out as
    // rate-manipulation evidence. `0` = unbounded.
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
    let interval_bytes = resp
        .voucher_interval_mb
        .unwrap_or(DEFAULT_VOUCHER_INTERVAL_MB)
        .saturating_mul(MB_BYTES);
    // Wire-byte bound (bao-encoded size of the aligned range), not content bytes —
    // the window path forwards this stream verbatim and pays the upstream in wire
    // bytes (ADR 038 §Payment metering). Same derivation as `fetch_inner`.
    let total_bytes = resp.body.total_bytes;
    let expected_wire_bytes = aligned_wire_len(byte_offset, byte_len, total_bytes)?;
    let header = UpstreamPullHeader {
        total_bytes,
        rate_per_mb,
        interval_bytes,
    };
    let pull = UpstreamPull {
        stall,
        conn,
        send,
        recv,
        ctx: ctx.clone(),
        ledger,
        hash,
        rate_per_mb,
        interval_bytes,
        expected_wire_bytes,
        cumulative: 0,
        unvouchered: 0,
        ended: false,
    };
    Ok((header, pull))
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
    /// Reads [`ChannelLedger::settlement`], not a copied-back field. A field can only be
    /// updated on a path that RUNS, and this pull's does not always run: the serve loop
    /// drives it as a future on the `accept` task, which is dropped on shutdown or a
    /// downstream reset, and `settlement` additionally covers a voucher left in flight when
    /// that happened. See the ledger's docs.
    #[must_use]
    pub fn progress(&self) -> VoucherProgress {
        VoucherProgress::from_cumulative(self.ledger.settlement(), self.ctx.prior_nonce)
    }

    /// Send one voucher for `delta_bytes` newly delivered since the last voucher,
    /// through the CHANNEL's ledger — shared with every other concurrent pull on it, so
    /// their vouchers are serialized into strict nonce order rather than colliding (see
    /// [`stream_fetch_shared`]). Sends optimistically (#1484): the ack is read back by
    /// [`Self::next_chunk`] / [`Self::finish`], not awaited here.
    async fn pay_one(&mut self, delta_bytes: u64) -> anyhow::Result<()> {
        let ledger = Arc::clone(&self.ledger);
        send_voucher(
            &mut self.send,
            &self.ctx,
            &ledger,
            self.rate_per_mb,
            delta_bytes,
        )
        .await
    }

    /// Read the next `ChunkData`, paying the upstream at each voucher-interval
    /// boundary (and a closing voucher once all promised bytes have arrived),
    /// and return the chunk for the caller to forward downstream + tee to cache.
    /// Returns `Ok(None)` on `StreamEnd`.
    ///
    /// # Errors
    ///
    /// More bytes than promised, a mid-stream `StreamError` (typed [`UpstreamRefused`]), an
    /// unexpected message, a [`UpstreamVoucherRejected`] / transport error while paying, or —
    /// on the inactivity clock — [`PullStalled`] once bytes have flowed, or [`PullTimeout`] if
    /// the stall budget elapses before the first byte (`cumulative == 0`). An empty or
    /// over-`CHUNK_SIZE` `ChunkData` is no longer raised here: it is rejected at decode by
    /// `ChunkData`'s `serde(try_from)` (#1088), so it surfaces out of `read_client_message`.
    pub async fn next_chunk(&mut self) -> anyhow::Result<Option<Bytes>> {
        if self.ended {
            return Ok(None);
        }
        // Loop past any optimistic `VoucherAck`s (#1484): a voucher we sent earlier is
        // acked on its own message, interleaved with chunks, and must be resolved into
        // the ledger rather than returned to the caller as a chunk. Every iteration
        // reads exactly one message; the loop only continues on an ack, and the acks it
        // will consume are bounded by the outstanding set, so a node cannot spin it with
        // a flood of bare acks (a spurious ack — none outstanding — bails).
        loop {
            // The inactivity bound (#1134). A per-read budget IS the stall budget here:
            // every read that succeeds either carries bytes (#1088 bans empty
            // `ChunkData`), resolves a voucher, or terminates the stream, so there is no
            // frame a peer can send to hold this open without making progress. The clock
            // starts when we begin waiting, not when the last chunk landed, so the
            // caller's downstream-forward time is not charged against the upstream's
            // budget.
            let msg = tokio::time::timeout(self.stall, read_client_message(&mut self.recv))
                .await
                // Before the first byte this is the server's time-to-first-byte, which
                // scales with blob size, not an inactivity signal — so it is OUR
                // deadline, not the peer's fault. Same reasoning, and the same split, as
                // the buffered loop in `receive_and_pay`; see the long comment there
                // (#1145 review).
                .map_err(|_| {
                    if self.cumulative == 0 {
                        anyhow::Error::new(PullTimeout { after: self.stall })
                    } else {
                        anyhow::Error::new(PullStalled { after: self.stall })
                    }
                })??;
            match msg {
                ClientMessage::ChunkData(chunk) => {
                    // The payload is bounded on both sides by construction (#1088): the
                    // ceiling caps per-frame allocation, and the non-empty floor keeps
                    // every frame a unit of progress, so a peer cannot spin this loop —
                    // or refresh the inactivity deadline above — with a run of empty
                    // frames. This path has no belt-and-braces byte-progress check behind
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
                    // No per-chunk hashing here: this stream's bytes are bao wire
                    // bytes forwarded verbatim downstream and teed into the cache's
                    // verifying decoder (`import_and_verify_stream`), which checks the
                    // cached copy against the root (ADR 038); the downstream client
                    // verifies its own copy with its decoder.
                    self.unvouchered = self.unvouchered.saturating_add(chunk_len);
                    let boundary =
                        self.unvouchered >= self.interval_bytes && self.interval_bytes > 0;
                    let closing =
                        self.cumulative >= self.expected_wire_bytes && self.unvouchered > 0;
                    if boundary || closing {
                        let delta = self.unvouchered;
                        // Send the voucher WITHOUT blocking on its ack; the ack is read
                        // back on a later iteration (or by `finish`).
                        self.pay_one(delta).await?;
                        self.unvouchered = 0;
                    }
                    return Ok(Some(Bytes::from(chunk.into_bytes())));
                }
                // The ack for a voucher we sent optimistically, or a mid-stream refusal.
                // Resolve it into the ledger and keep reading for the next chunk; a
                // `VoucherRejected` / other `StreamError` surfaces as an error.
                ack @ (ClientMessage::VoucherAck | ClientMessage::StreamError(_)) => {
                    resolve_voucher_slot(&self.ledger, ack)?;
                }
                ClientMessage::StreamEnd => {
                    self.ended = true;
                    return Ok(None);
                }
                other => {
                    anyhow::bail!("unexpected message mid-delivery: {}", variant_name(&other))
                }
            }
        }
    }

    /// Finalize a completed pull: drain to `StreamEnd` if needed, enforce
    /// wire-byte completeness (the full promised bao wire size was received),
    /// close the connection cleanly, and return the final acked watermark to
    /// persist. Per ADR 038 this no longer re-hashes the whole blob — bao
    /// verification is delegated to the tee's verifying decoder (cached copy) and
    /// the downstream client's own decoder.
    ///
    /// # Errors
    ///
    /// A short delivery (fewer wire bytes than promised before `StreamEnd`), a stream/protocol
    /// error while draining, or — on the same inactivity bound as `next_chunk` — [`PullStalled`]
    /// if the upstream goes silent before `StreamEnd`.
    pub async fn finish(mut self) -> anyhow::Result<VoucherProgress> {
        while !self.ended {
            // Same inactivity bound as `next_chunk` (#1134): an upstream that
            // never sends its `StreamEnd` must not hold the drain open forever.
            let msg = tokio::time::timeout(self.stall, read_client_message(&mut self.recv))
                .await
                .map_err(|_| anyhow::Error::new(PullStalled { after: self.stall }))??;
            match msg {
                ClientMessage::StreamEnd => self.ended = true,
                ClientMessage::ChunkData(_) => {
                    anyhow::bail!("server sent ChunkData after the promised total")
                }
                // The ack for the closing voucher arrives before `StreamEnd` on the wire
                // (the node collects and acks it, then sends `StreamEnd`), so the drain
                // must resolve it into the ledger's committed watermark — otherwise the
                // final voucher would settle high instead of committing (#1484). A
                // `VoucherRejected` / other `StreamError` still surfaces as an error.
                ack @ (ClientMessage::VoucherAck | ClientMessage::StreamError(_)) => {
                    resolve_voucher_slot(&self.ledger, ack)?;
                }
                other => {
                    anyhow::bail!("unexpected message at stream end: {}", variant_name(&other))
                }
            }
        }
        // Completeness for every fetch (full and resumed): bao verification is
        // delegated to the tee's verifying decoder (cached copy) and the
        // downstream client's own decoder, so `finish` no longer re-hashes the
        // whole blob. A
        // truncated stream (fewer wire bytes than promised) can't be decoded, so
        // require the full promised wire size as the completeness signal.
        if self.cumulative < self.expected_wire_bytes {
            self.conn.close(0u32.into(), b"short-delivery");
            anyhow::bail!(
                "server sent {} of {} promised wire bytes before StreamEnd",
                self.cumulative,
                self.expected_wire_bytes
            );
        }
        self.conn.close(0u32.into(), b"done");
        Ok(self.progress())
    }

    /// Abandon the pull (e.g. the downstream client dropped, so we stop pulling
    /// and paying). Closes the connection and returns the acked watermark so the
    /// caller can still persist what it paid (#852).
    #[must_use]
    pub fn abort(self) -> VoucherProgress {
        self.conn.close(0u32.into(), b"client-abandoned");
        self.progress()
    }
}

impl Drop for UpstreamPull {
    /// Safety net for the "call a terminal method on every exit" contract: if a
    /// caller returns or panics without `finish`/`abort`, still close the upstream
    /// connection so the QUIC stream and the upstream's server-side serve task
    /// don't linger and keep that paid stream half-open. `Connection::close` is
    /// first-wins and idempotent, so an explicit close in `finish`/`abort` keeps
    /// its richer reason and this is a no-op when one of them ran; it only takes
    /// effect on a dropped-without-finalize path.
    ///
    /// It closes the connection and nothing else, and that is now sufficient. It used to
    /// note that "the acked watermark cannot be recovered from `drop` (it can't be
    /// returned), so this bounds only the connection leak, not the #852 watermark loss" —
    /// which was true while the watermark lived in a field of this struct. It does not:
    /// the watermark lives in the channel's [`ChannelLedger`], which OUTLIVES the pull (it
    /// is shared with the other pulls on the channel). A dropped pull's caller reads it
    /// with [`ChannelLedger::settlement`] and persists it — `node_origin` does exactly that
    /// from its own `Drop` guard (#1145 review).
    fn drop(&mut self) {
        self.conn.close(0u32.into(), b"upstream-pull-dropped");
    }
}

/// Issue one cumulative voucher for `delta_bytes` newly delivered since the last
/// voucher and SEND it — WITHOUT waiting for the `VoucherAck` (#1484).
///
/// Voucher issuance runs through the channel's [`ChannelLedger`], which serializes
/// the compute → sign → send critical section across every concurrent stream on the
/// channel — so vouchers reach the node in strict nonce order — but releases its
/// issuance lock before any ack is read. That release is the point of #1484: the
/// blocking ack wait used to hold the shared ledger lock across a full round trip, so
/// parallel range streams to one provider serialized their payments behind each
/// other. The ack is now read off the critical path by the receive loop and fed back
/// through [`ChannelLedger::resolve_ack`] / [`ChannelLedger::resolve_reject`].
///
/// Each voucher's own *delta* (`ceil(delta_bytes * rate / 1 MiB)`) covers its own
/// bytes at the advertised rate (the node checks deltas, not the rounded cumulative).
/// The ledger advances its committed watermark only when the ack comes back, so a
/// voucher whose ack never arrives leaves the committed watermark unmoved while
/// [`ChannelLedger::settlement`] still reports it (settle high — the upstream persists
/// before it acks, ADR 003). The client never pays ahead of what it received (vouchers
/// are cumulative over delivered bytes), and a provider that delivers but never acks is
/// abandoned by the pull's `stall` timeout, so the optimistic loop needs no separate
/// cap on how many vouchers may be outstanding.
async fn send_voucher(
    send: &mut SendStream,
    ctx: &ChannelContext,
    ledger: &ChannelLedger,
    rate_per_mb: u64,
    delta_bytes: u64,
) -> anyhow::Result<()> {
    ledger
        .issue(delta_bytes, rate_per_mb, |next: Cumulative| async move {
            let signed = Voucher {
                channel_id: ctx.channel_id,
                amount: next.amount,
                nonce: next.nonce,
                bytes_delivered: next.bytes,
                token: ctx.token,
            }
            .sign(ctx.client_signer.as_ref(), &ctx.voucher_domain)
            .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}").context(LocalPullFault))?;
            write_message(
                send,
                &ClientMessage::Voucher(signed_to_wire_voucher(&signed)),
            )
            .await
        })
        .await
        .map(|_sent| ())
}

/// Dispatch a `VoucherAck` slot message read off the receive loop into the ledger
/// (#1484). `VoucherAck` advances the committed watermark by resolving the oldest
/// outstanding voucher; a `StreamError::VoucherRejected` disarms it WITHOUT committing
/// and surfaces the typed [`UpstreamVoucherRejected`]; any other `StreamError` is a
/// mid-stream refusal. Returns `Ok(())` when the message was an ack the loop should
/// keep going past. The caller is responsible for every OTHER message variant — this
/// only handles the voucher-resolution slot.
///
/// A `VoucherAck` with nothing outstanding is a protocol violation (the node acked a
/// voucher we never sent), and bounding the acks the loop will consume between chunks
/// to the outstanding set is also what keeps a node from spinning the loop with a
/// flood of bare acks — so a spurious ack is surfaced as an error rather than ignored.
fn resolve_voucher_slot(ledger: &ChannelLedger, ack: ClientMessage) -> anyhow::Result<()> {
    match ack {
        // The upstream persists before it acks (ADR 003), so the oldest outstanding
        // voucher is now accepted — advance the committed watermark to it.
        ClientMessage::VoucherAck => {
            if ledger.resolve_ack() {
                Ok(())
            } else {
                anyhow::bail!("upstream sent VoucherAck with no voucher outstanding")
            }
        }
        // A `VoucherRejected` is OUR payment-side fault. Disarm the rejected voucher
        // (known-not-taken, so it must not be settled optimistically) and carry its
        // typed reason — plus the wallet-less-resume `bundle` (#1481) — so the
        // orchestrator can exonerate the provider (#857) and `fetch_inner` can
        // self-heal from an authenticated watermark instead of treating the rejection
        // as terminal.
        ClientMessage::StreamError(StreamError::VoucherRejected { reason, bundle }) => {
            ledger.resolve_reject();
            Err(anyhow::Error::new(UpstreamVoucherRejected {
                reason,
                bundle,
            }))
        }
        // Any OTHER `StreamError` is the upstream refusing mid-stream. Carry the typed
        // wire code as `UpstreamRefused`, exactly as the mid-stream receive sites do
        // (#1145 review) — stringifying it dropped the code through every downcast to
        // the `Unreachable` catch-all, scoring an honest `Overloaded`/`NotFound` peer
        // as a dead node.
        ClientMessage::StreamError(e) => Err(anyhow::Error::new(UpstreamRefused::mid_stream(e))),
        other => anyhow::bail!("expected VoucherAck, got {}", variant_name(&other)),
    }
}

/// Validate + verify a `StreamResponse` on receive (ADR 005, ADR 014 §1, #252).
fn verify_response(
    resp: &StreamResponse,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    channel_id: B256,
    timestamp_us: u64,
) -> anyhow::Result<()> {
    // #252 + slash_sig length/upper-bound checks.
    resp.validate()
        .map_err(|e| anyhow::anyhow!("invalid stream response: {e}"))?;
    // Echoed-field correlation (ADR 005).
    if resp.body.hash != hash {
        anyhow::bail!("response hash does not match request");
    }
    if resp.body.channel_id != channel_id.as_slice() {
        anyhow::bail!("response channel_id does not match request");
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
    write_frame(send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write failed: {e}"))
}

async fn read_client_message(recv: &mut RecvStream) -> anyhow::Result<ClientMessage> {
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
        ClientMessage::VoucherAck => "VoucherAck",
        ClientMessage::StreamEnd => "StreamEnd",
        ClientMessage::StreamError(_) => "StreamError",
        ClientMessage::CooperativeCloseRequest(_) => "CooperativeCloseRequest",
        ClientMessage::CooperativeCloseAuth(_) => "CooperativeCloseAuth",
    }
}

#[cfg(test)]
mod tests {
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use decdn_bao_range::{IROH_BLOCK_SIZE, align_range, encode_verified_range};

    use super::{
        ChannelContext, Cumulative, HashMismatch, LocalPullFault, U256, UpstreamVoucherRejected,
        Voucher, VoucherRejectReason, WatermarkBundle, aligned_wire_len, decode_verified_range,
        genuine_exhaustion, resumable_watermark,
    };

    /// The `LocalPullFault` marker must ride out on the errors the range helpers ACTUALLY
    /// raise — not on one a test hand-built (#1145 review).
    ///
    /// This distinction is the whole point of the test, and the version it replaces got it
    /// backwards. That one called the real `sign_client_binding`, asserted it was `Ok`,
    /// threw the result away, and then hand-built `anyhow!("...").context(LocalPullFault)`
    /// before asserting the ladder found `LocalPullFault` in it. Attaching the marker and
    /// then finding it is true by construction: the test could not fail, and stripping every
    /// `.context(LocalPullFault)` from the crate left the whole suite green. Its own doc
    /// comment warned that "a synthetic `anyhow!(...)` would pass this test while production
    /// scored the peer" — and then did exactly that.
    ///
    /// So: real functions, real errors, marker never touched by the test. `align_range`
    /// rejects an offset at or past the end of the blob (never clamps — ADR 005), which is
    /// the one local-fault trigger reachable without mocking a signer, and it is shared by
    /// both the buffered and window-paced paths.
    ///
    /// The stakes, and why an unguarded marker here is not cosmetic: every arm BELOW
    /// `LocalPullFault` in the ladder blames the peer to some degree, and the catch-all
    /// scores `Unreachable` — a local EWMA hit AND a gossiped observation. A fault in this
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
        let stall = Duration::from_secs(5);

        // At and below `open + stall` the cap always wins the race, so `PullStalled` — the
        // only signal that says anything about the PEER — could never fire.
        for cap in [Duration::from_secs(1), stall, open + stall] {
            assert!(
                matches!(
                    PullDeadlines::capped(open, stall, cap),
                    Err(DeadlineError::CapCannotOutlastItsStages { .. })
                ),
                "a cap of {cap:?} against open {open:?} + stall {stall:?} leaves the stall \
                 bound unable to fire, and must not be constructible"
            );
        }

        // One tick past it, the inactivity deadline can actually fire.
        assert!(
            PullDeadlines::capped(open, stall, open + stall + Duration::from_millis(1)).is_ok(),
            "past open + stall the stall bound can fire, so this is a legitimate pull"
        );

        // A zero budget elapses on its first poll: the stage it bounds can never run.
        assert!(matches!(
            PullDeadlines::capped(Duration::ZERO, stall, Duration::from_mins(1)),
            Err(DeadlineError::ZeroBudget)
        ));
        assert!(matches!(
            PullDeadlines::capped(open, Duration::ZERO, Duration::from_mins(1)),
            Err(DeadlineError::ZeroBudget)
        ));
    }

    /// `new` must refuse a zero budget too — and it is the constructor that MATTERS, because
    /// it is the one every production pull takes (#1145 review).
    ///
    /// It was infallible, on the reasoning that a zero bound "fires too EAGERLY — loud, and
    /// immediately obvious". It is the opposite of loud. A zero `stall` trips `PullStalled`
    /// on the first poll of every streaming read, and `PullStalled` is the verdict that
    /// scores a peer `Unreachable` — locally AND over gossip. So the failure mode is not a
    /// node that visibly stops working; it is a node that quietly defames every honest peer
    /// it touches, as fast as it can dial them.
    ///
    /// `capped` refused this from the start. The invariant belongs to the type, not to a
    /// resolver in another crate that a caller has to remember to run.
    #[test]
    fn new_refuses_a_zero_budget_on_the_path_every_production_pull_takes() {
        use super::{DeadlineError, PullDeadlines};
        use std::time::Duration;

        assert!(
            matches!(
                PullDeadlines::new(Duration::ZERO, Duration::from_secs(20)),
                Err(DeadlineError::ZeroBudget)
            ),
            "a zero open bound means the open stage can never complete"
        );
        assert!(
            matches!(
                PullDeadlines::new(Duration::from_secs(20), Duration::ZERO),
                Err(DeadlineError::ZeroBudget)
            ),
            "a zero stall bound gossips `Unreachable` about every honest peer it touches"
        );
        assert!(PullDeadlines::new(Duration::from_secs(20), Duration::from_secs(20)).is_ok());
    }

    /// `aligned_wire_len`'s new `byte_len` parameter must actually bound the
    /// quoted wire cost — not just be accepted and ignored. This is the
    /// construction-level proof that `open_progressive_pull`'s `byte_len`
    /// threads all the way to the wire-byte bound `PeerSource`'s callers price
    /// vouchers from (#1608 A2): a middle-gap request must quote strictly less
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
        // clamping (ADR 005), and both callers must own that as OURS. Each assertion covers
        // both halves at once: `None` here means the call wrongly SUCCEEDED, and a `Some`
        // without the marker means it failed and blamed the peer.
        let aligned = aligned_wire_len(8192, 0, 4096).err();
        assert!(
            aligned
                .as_ref()
                .is_some_and(|e| e.downcast_ref::<LocalPullFault>().is_some()),
            "aligned_wire_len must reject an out-of-range offset and mark it OUR fault; \
             without the marker it falls through every downcast to the catch-all and \
             gossips the peer as unreachable. Got: {aligned:?}"
        );

        let decoded = decode_verified_range([0u8; 32], 4096, 8192, 0, &[]).err();
        assert!(
            decoded
                .as_ref()
                .is_some_and(|e| e.downcast_ref::<LocalPullFault>().is_some()),
            "decode_verified_range must reject an out-of-range offset and mark it OUR fault, \
             for the same reason. Got: {decoded:?}"
        );
    }

    /// `client_binding_ext` maps an unbound context to `None` (so
    /// `encode_stream_request` appends no ext bytes — byte-for-byte the pre-#1115
    /// wire) and a bound one to `Some` carrying exactly the binding at the default
    /// voucher cadence. This is the shared mapping BOTH request sites
    /// (`fetch_inner` and `open_progressive_pull`) rely on, so it guards a
    /// refactor that would silently drop the ext on either path (#1115).
    #[test]
    fn client_binding_ext_reflects_binding_presence() -> anyhow::Result<()> {
        use std::sync::Arc;

        use alloy::primitives::{Address, B256, U256};
        use alloy::signers::local::PrivateKeySigner;

        use super::{ChannelContext, client_binding_ext, sign_client_binding};

        let signer = PrivateKeySigner::random();
        let domain = decdn_incentive::bind_node_id_domain(1, Address::ZERO);
        let ctx = ChannelContext {
            channel_id: B256::ZERO,
            token: Address::ZERO,
            deposit: U256::ZERO,
            client_signer: Arc::new(signer.clone()),
            voucher_domain: domain.clone(),
            prior_nonce: U256::ZERO,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
        };
        // Unbound ⇒ no ext.
        anyhow::ensure!(
            client_binding_ext(&ctx).is_none(),
            "unbound ctx must yield no ext"
        );

        // Bound ⇒ ext carries exactly the binding, cadence left defaulted.
        let binding = sign_client_binding(&signer, B256::repeat_byte(0xAB), &domain)?;
        let ctx = ctx.with_client_binding(binding.clone());
        let ext = client_binding_ext(&ctx)
            .ok_or_else(|| anyhow::anyhow!("bound ctx must yield an ext"))?;
        anyhow::ensure!(
            ext.voucher_interval_mb.is_none(),
            "voucher cadence must stay defaulted"
        );
        anyhow::ensure!(
            ext.binding == Some(binding),
            "ext must carry the exact binding"
        );
        Ok(())
    }

    /// Build a test [`ChannelContext`] signing with `signer`, sharing the shape
    /// `client_binding_ext_reflects_binding_presence` already uses.
    fn resume_test_ctx(
        channel_id: alloy::primitives::B256,
        token: alloy::primitives::Address,
        signer: &std::sync::Arc<alloy::signers::local::PrivateKeySigner>,
        domain: &alloy::dyn_abi::Eip712Domain,
    ) -> ChannelContext {
        ChannelContext {
            channel_id,
            token,
            deposit: U256::ZERO,
            client_signer: std::sync::Arc::clone(signer),
            voucher_domain: domain.clone(),
            prior_nonce: U256::ZERO,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
        }
    }

    /// Build a `WatermarkBundle` whose `last_signature` is `signer`'s real EIP-712 voucher
    /// signature over `(channel_id, amount, nonce, bytes_delivered, token)` — i.e. a
    /// genuinely-signed bundle, the shape a caller must construct one from.
    fn signed_bundle(
        channel_id: alloy::primitives::B256,
        token: alloy::primitives::Address,
        signer: &alloy::signers::local::PrivateKeySigner,
        domain: &alloy::dyn_abi::Eip712Domain,
        amount: U256,
        nonce: U256,
        bytes_delivered: U256,
    ) -> anyhow::Result<WatermarkBundle> {
        let voucher_signature = Voucher {
            channel_id,
            amount,
            nonce,
            bytes_delivered,
            token,
        }
        .sign(signer, domain)
        .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}"))?;
        Ok(WatermarkBundle {
            amount: amount.to_be_bytes(),
            nonce: nonce.to_be_bytes(),
            bytes_delivered: bytes_delivered.to_be_bytes(),
            last_signature: voucher_signature.signature.as_bytes().to_vec(),
        })
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
            U256::from(1u64),
            U256::from(4096u64),
        )?;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::StaleNonce,
            bundle: Some(bundle),
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
            U256::from(1u64),
            U256::from(4096u64),
        )?;
        bundle.amount = U256::from(1_000_000u64).to_be_bytes();
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::StaleNonce,
            bundle: Some(bundle),
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
            U256::from(3u64),
            U256::from(4096u64),
        )?;
        let expected_bytes = bundle.bytes_delivered;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::StaleNonce,
            bundle: Some(bundle),
        });

        let got = resumable_watermark(&err, &ctx).ok_or_else(|| {
            anyhow::anyhow!(
                "a bundle genuinely signed by our own key over the claimed tuple must resolve"
            )
        })?;
        assert_eq!(got.bytes_delivered, expected_bytes);
        Ok(())
    }

    /// True twin: `InsufficientDeposit` with no bundle (nothing for `resumable_watermark` to
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
            reason: VoucherRejectReason::InsufficientDeposit,
            bundle: None,
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

    /// A node crying `InsufficientDeposit` while our OWN ledger still shows headroom is NOT
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
            reason: VoucherRejectReason::InsufficientDeposit,
            bundle: None,
        });
        assert!(!genuine_exhaustion(
            &err,
            &ctx,
            Cumulative::default(),
            U256::from(5000u64),
            U256::from(1000u64)
        ));
    }

    /// Any rejection reason other than `InsufficientDeposit` is never exhaustion, regardless
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
            reason: VoucherRejectReason::StaleNonce,
            bundle: None,
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
    /// even if it rides on an `InsufficientDeposit` rejection and even if the ledger looks
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
        // Our own ledger is still at nonce 2; the bundle reports nonce 3 — the node holds a
        // later voucher than we do, exactly the healable desync #1481 §5 exists to catch.
        let committed = Cumulative {
            nonce: U256::from(2u64),
            bytes: U256::from(2048u64),
            amount: U256::from(300u64),
        };

        let bundle = signed_bundle(
            channel_id,
            token,
            &signer,
            &domain,
            U256::from(500u64),
            U256::from(3u64),
            U256::from(4096u64),
        )?;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::InsufficientDeposit,
            bundle: Some(bundle),
        });

        // Sanity: this bundle IS the healable-desync case `resumable_watermark` resolves, and
        // it genuinely advances past `committed`.
        let resolved = resumable_watermark(&err, &ctx).ok_or_else(|| {
            anyhow::anyhow!("test bundle must be a healable desync resumable_watermark accepts")
        })?;
        anyhow::ensure!(
            Cumulative::from(resolved).nonce > committed.nonce,
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
        // Our ledger and the echoed bundle agree EXACTLY: nonce 3 both sides.
        let committed = Cumulative {
            nonce: U256::from(3u64),
            bytes: U256::from(4096u64),
            amount: U256::from(500u64),
        };

        let bundle = signed_bundle(
            channel_id,
            token,
            &signer,
            &domain,
            U256::from(500u64),
            U256::from(3u64),
            U256::from(4096u64),
        )?;
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::InsufficientDeposit,
            bundle: Some(bundle),
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

    #[test]
    fn decode_verified_range_round_trips_whole_blob() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (root, wire) = wire_for(&blob, 0, 0)?;
        let out = decode_verified_range(root, u64::try_from(blob.len())?, 0, 0, &wire)?;
        anyhow::ensure!(out.as_ref() == blob.as_slice(), "whole-blob round-trip");
        Ok(())
    }

    /// A resumed fetch at a group-aligned offset self-verifies against the root —
    /// no dependency on the bytes before the offset (the old gap is closed).
    #[test]
    fn decode_verified_range_resumed_group_aligned_offset() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let off = 64 * 1024; // 16 KiB-group aligned
        let (root, wire) = wire_for(&blob, off, 0)?;
        let out = decode_verified_range(root, u64::try_from(blob.len())?, off, 0, &wire)?;
        let want = sub(&blob, off, u64::try_from(blob.len())?)?;
        anyhow::ensure!(
            out.as_ref() == want.as_slice(),
            "resumed tail self-verifies"
        );
        Ok(())
    }

    /// A non-group-aligned resume offset: the server serves the aligned superset
    /// and the receiver trims the leading bytes back to the exact requested span.
    #[test]
    fn decode_verified_range_trims_non_aligned_offset() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let off = 70 * 1024; // inside a group, not on a boundary
        let (root, wire) = wire_for(&blob, off, 0)?;
        let out = decode_verified_range(root, u64::try_from(blob.len())?, off, 0, &wire)?;
        let want = sub(&blob, off, u64::try_from(blob.len())?)?;
        anyhow::ensure!(
            out.as_ref() == want.as_slice(),
            "trimmed to requested offset"
        );
        Ok(())
    }

    /// A corrupt tail byte is rejected at its chunk group with the typed
    /// `HashMismatch` — even on a resumed fetch with no earlier bytes (ADR 038 #1).
    #[test]
    fn decode_verified_range_rejects_corrupt_tail() -> anyhow::Result<()> {
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
        let err = decode_verified_range(root, u64::try_from(blob.len())?, off, 0, &wire)
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
    /// missing — detection needs no tail, so a streaming consumer can stop
    /// paying at group *k* instead of buffering to the end.
    #[test]
    fn decode_verified_range_rejects_corrupt_middle_group_without_tail() -> anyhow::Result<()> {
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
        let err = decode_verified_range(root, u64::try_from(blob.len())?, 0, 0, &wire)
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
    #[test]
    fn decode_verified_range_truncation_is_not_hash_mismatch() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (root, mut wire) = wire_for(&blob, 0, 0)?;
        wire.truncate(wire.len() * 60 / 100);
        let err = decode_verified_range(root, u64::try_from(blob.len())?, 0, 0, &wire)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected truncated stream to be rejected"))?;
        anyhow::ensure!(
            err.downcast_ref::<HashMismatch>().is_none(),
            "clean truncation must NOT be classified as corruption, got HashMismatch: {err}"
        );
        Ok(())
    }

    /// A 0-byte blob (#1054) delivers an empty stream that is proven against the
    /// empty root `blake3::hash(&[])`, decoding to empty bytes.
    #[test]
    fn decode_verified_range_empty_blob_accepts_empty_root() -> anyhow::Result<()> {
        let root = *blake3::hash(&[]).as_bytes();
        let out = decode_verified_range(root, 0, 0, 0, &[])?;
        anyhow::ensure!(out.is_empty(), "empty blob decodes to empty bytes");
        Ok(())
    }

    /// A server claiming `total_bytes == 0` for a NON-empty requested hash must be
    /// rejected: the empty stream must be proven against the empty root, never
    /// accepted for an arbitrary root. Without the explicit check the empty range
    /// decodes trivially (no chunk group to verify) and the bypass would surface
    /// as success — the exact hole `fetch_inner` warns about (#1054).
    #[test]
    fn decode_verified_range_empty_claim_rejects_wrong_root() -> anyhow::Result<()> {
        let blob = make_blob(4096);
        let root = *blake3::hash(&blob).as_bytes();
        let err = decode_verified_range(root, 0, 0, 0, &[])
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected wrong-root rejection for empty claim"))?;
        anyhow::ensure!(
            err.downcast_ref::<HashMismatch>().is_some(),
            "empty claim for a non-empty root must surface HashMismatch, got: {err}"
        );
        Ok(())
    }

    /// Decoding an honest stream against the WRONG root fails closed (the range
    /// can't be re-anchored), so a source serving a different blob is rejected.
    #[test]
    fn decode_verified_range_rejects_wrong_root() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (_root, wire) = wire_for(&blob, 0, 0)?;
        let wrong = [0xABu8; 32];
        let err = decode_verified_range(wrong, u64::try_from(blob.len())?, 0, 0, &wire)
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
    /// constructor both the buffered and progressive open stages share — rather
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
            channel_id: [0x77u8; 32],
            timestamp_us: 1_700_000_000_000_000,
            redirect: None,
        };
        let sig = StreamSlashData::from_response_body(&body).sign(&operator, &domain)?;
        let response = StreamResponse {
            body: body.clone(),
            error: Some(StreamError::EvictedSinceProbe),
            voucher_interval_mb: None,
            slash_sig: sig.as_bytes().to_vec(),
        };
        // Precondition the real open stage enforces before ever calling `open`.
        response.validate()?;

        let err = UpstreamRefused::open(response);
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
            preserved.error.as_ref() == Some(refused.error()),
            "the derived wire code {:?} must match the code inside the preserved evidence {:?}",
            refused.error(),
            preserved.error,
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
                channel_id: [0x77u8; 32],
                timestamp_us: 1_700_000_000_000_000,
                redirect: None,
            },
            error: None,
            voucher_interval_mb: None,
            slash_sig: vec![0u8; decdn_protocol::message::SLASH_SIG_LEN],
        };
        let err = UpstreamRefused::open(response);
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
