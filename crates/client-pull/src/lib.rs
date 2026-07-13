//! Reusable `cdn/client/v1` paid-pull requester, shared by the node
//! (node-to-node miss pulls, #317) and the CLI (client fetch / bundle pull).
//!
//! [`stream_fetch`] performs one full delivery exchange against a remote node:
//! it sends a [`StreamRequest`], validates and verifies the signed
//! [`StreamResponse`], receives `ChunkData` while paying cumulative vouchers
//! at each `voucher_interval_mb` boundary, and returns the assembled blob on
//! `StreamEnd`. It is the receive-side call site for the #252 rule (reject a
//! `rate_per_mb == 0` response) and the `slash_sig` verification obligation
//! (ADR 014 §1).
//!
//! Mirrors [`probe::probe_once`] in spirit, but for the paid path: it signs
//! vouchers (so it needs the incentive layer and a signer)
//! and, unlike probe, **never** attempts 0-RTT (ADR 015 forbids 0-RTT on
//! `cdn/client/v1`).
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
/// `CapacityBond.getActiveNodes`, then rank probed blob-holders.
pub mod discovery;
/// One-shot client `Endpoint` construction: relay + discovery resolution for
/// the `cdn/client/v1` and `cdn/probe/v1` dial paths (#935/#936).
pub mod endpoint;
mod ledger;
/// Reusable `cdn/probe/v1` client with QUIC 0-RTT (ADR 015).
pub mod probe;
/// Wallet-filled HTTP provider builder for opening/settling payment channels.
pub mod provider;

pub use ledger::{ChannelLedger, Cumulative};

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
    BuyerChannelState, EPHEMERAL_BINDING_NONCE, StreamSlashData, Voucher, binding_signing_hash,
    signed_to_wire_voucher,
};
use decdn_protocol::client::{
    ClientBinding, ClientMessage, StreamError, StreamRequest, StreamRequestExt, StreamResponse,
    VoucherRejectReason,
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
        .map_err(|e| anyhow::anyhow!("sign client binding: {e}"))?
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
    /// Seed the watermark from the channel's prior cumulative state (the last
    /// voucher acked on earlier streams). `vouchers_sent` starts at `0` — it
    /// counts acks on *this* stream.
    const fn seed(ctx: &ChannelContext) -> Self {
        Self {
            nonce: ctx.prior_nonce,
            bytes_delivered: ctx.prior_bytes_delivered,
            amount: ctx.prior_amount,
            vouchers_sent: 0,
        }
    }

    /// Set the watermark from a ledger [`Cumulative`] plus the channel's seed
    /// nonce. `vouchers_sent` is set to the number of vouchers acked since the
    /// seed (`cum.nonce - prior_nonce`, saturated to `u64`); `acked()` only checks
    /// it is `> 0`, so this preserves the "acked iff the nonce advanced past the
    /// seed" contract even when the ledger was shared across concurrent streams.
    fn set_from_cumulative(&mut self, cum: Cumulative, prior_nonce: U256) {
        self.nonce = cum.nonce;
        self.bytes_delivered = cum.bytes;
        self.amount = cum.amount;
        self.vouchers_sent =
            u64::try_from(cum.nonce.saturating_sub(prior_nonce)).unwrap_or(u64::MAX);
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
/// by [`stream_fetch`] so callers can `downcast_ref` to classify corruption
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

/// Typed sentinel for the buyer's own per-candidate pull deadline firing (#857).
/// Returned (not a bare string) so the pull orchestrator can `downcast_ref` and
/// recognize that the timeout is OUR local deadline — a possibly mis-sized
/// configuration value — not evidence the provider is unreachable, and so must
/// not tar the provider's reputation locally or over gossip. `Display` keeps the
/// stable `timed out` text for logs (and for the `!contains("timed out")`
/// negative assertion in `node_to_node_pull_through`'s deadline test).
///
/// Since #1134 it is raised by OUR OWN wall clocks only, and the message is
/// deliberately stage-NEUTRAL because there are two of them: the shared
/// `open_stream` open bound (`PullDeadlines::open`, on both the buffered and the
/// progressive path), and the optional overall `hard_cap`. `decdn-node` also wraps
/// `open_progressive_pull` in its per-candidate budget as belt-and-braces, which
/// raises the same sentinel.
///
/// It is NOT raised by the streaming loops — a mid-stream stall is [`PullStalled`],
/// which is a fact about the PEER and scores its reputation, whereas this is a fact
/// about our own possibly mis-sized budget and must not. Keeping the two apart is
/// the whole point of the split.
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
#[derive(Debug)]
pub struct UpstreamVoucherRejected {
    pub reason: VoucherRejectReason,
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
/// - `EvictedSinceProbe` / `Overloaded` / `BlobTooLarge` — likewise honest, and
///   the latter is deterministic (it recurs for this blob on any node).
/// - `InternalError` — the one code that IS evidence of a degraded peer; it means
///   "unexpected failure, do not retry THIS node" (#1129).
///
/// Only the wire code is recoverable here, never the finer server-side
/// `ServeRejectReason` — that collapse is intentional and must not be reversed.
/// `Display` keeps the stable `delivery refused: {code}` text that logs, the CLI's
/// cache-miss annotation, and the loopback tests match on.
#[derive(Debug)]
pub struct UpstreamRefused {
    pub error: StreamError,
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
        write!(f, "delivery refused: {:?}", self.error)
    }
}

impl std::error::Error for UpstreamRefused {}

/// Typed sentinel for an upstream that went SILENT mid-stream — no byte of
/// progress for the stall budget (#1134). Distinct from [`PullTimeout`], which is
/// our own overall wall-clock cap firing, and the distinction is load-bearing:
///
/// A whole-transfer deadline cannot tell "the provider is dead" from "this blob
/// is big" or "this link is slow", so `PullTimeout` must NOT tar the provider —
/// it fires on perfectly healthy transfers. A stall deadline resets on every byte
/// received, so it fires ONLY when a provider stops delivering while we wait.
/// That IS evidence the peer is unreachable, and `classify_pull_failure` scores it
/// as such.
///
/// The bound rests on the non-empty-`ChunkData` invariant (#1088): with empty
/// frames banned, a peer cannot hold the deadline open with padding that carries
/// no bytes. Belt-and-braces, the deadline is reset only when `cumulative`
/// actually advances — never merely because a frame arrived — so a run of
/// non-chunk frames cannot refresh it either.
#[derive(Debug)]
pub struct PullStalled {
    pub after: Duration,
}

impl std::fmt::Display for PullStalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upstream stalled: no progress for {:?}", self.after)
    }
}

impl std::error::Error for PullStalled {}

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
#[derive(Debug, Clone, Copy)]
pub struct PullDeadlines {
    /// Wall-clock bound on the open stage: dial, request, and the signed
    /// `StreamResponse`. Bounded work — a slow one is a stall.
    pub open: Duration,
    /// Inactivity bound on the streaming stage. Reset on every byte of progress.
    pub stall: Duration,
    /// Optional overall wall-clock cap on the whole exchange. `None` = uncapped;
    /// `open` and `stall` between them are what keep an uncapped pull from hanging.
    pub hard_cap: Option<Duration>,
}

impl PullDeadlines {
    /// The recommended shape: a wall clock on the open, inactivity on the stream,
    /// and no overall cap — so a pull of any size completes as long as the upstream
    /// keeps feeding it bytes.
    #[must_use]
    pub const fn new(open: Duration, stall: Duration) -> Self {
        Self {
            open,
            stall,
            hard_cap: None,
        }
    }

    /// As [`Self::new`], plus an overall wall-clock cap on the whole exchange.
    #[must_use]
    pub const fn capped(open: Duration, stall: Duration, hard_cap: Duration) -> Self {
        Self {
            open,
            stall,
            hard_cap: Some(hard_cap),
        }
    }

    /// The legacy single-deadline shape: one budget serving as the open bound, the
    /// stall bound, AND the overall cap. **Test-only — do not reach for this in
    /// production.** That conflation is exactly what #1134 set out to remove, and the
    /// name reads far more like a legitimate policy choice than it is.
    ///
    /// Note what it quietly costs, beyond re-introducing the size-coupled deadline:
    /// because `hard_cap == stall`, and the cap's clock starts at the top of the
    /// exchange while the stall clock only starts once bytes begin, **the hard cap
    /// always elapses first — so [`PullStalled`] can never fire under it.** A pull
    /// built this way silently cannot detect a stalled peer, and so cannot score one.
    /// Every loopback test using this helper is exercising a pull with the stall
    /// signal disabled; the stall path is covered by
    /// `node_origin_mid_stream_silence_scores_stalled_upstream`, which builds its
    /// deadlines explicitly.
    ///
    /// Retained only for the loopback helper [`stream_fetch`], whose callers move
    /// blobs small enough that none of this matters.
    #[doc(hidden)]
    #[must_use]
    pub const fn whole_transfer(timeout: Duration) -> Self {
        Self {
            open: timeout,
            stall: timeout,
            hard_cap: Some(timeout),
        }
    }
}

/// Build the [`UpstreamRefused`] error for a `body.ok == false` response, shared
/// by the buffered [`fetch_inner`] and progressive [`open_progressive_pull`] open
/// stages so the two cannot drift in how they classify a refusal.
///
/// Callers MUST have run [`verify_response`] first: its `StreamResponse::validate`
/// rejects `ok == false` with no error code (`MissingStreamError`), which is what
/// makes `error` guaranteed `Some` here. The `None` arm is therefore unreachable
/// on a validated response and is surfaced as the protocol violation it would be
/// — not defaulted to some invented code, which would launder a malformed refusal
/// into a plausible-looking one.
fn refusal(error: Option<StreamError>) -> anyhow::Error {
    match error {
        Some(error) => anyhow::Error::new(UpstreamRefused { error }),
        None => anyhow::anyhow!("delivery refused with no error code (unvalidated response?)"),
    }
}

/// Fetch `hash` from `target` over `cdn/client/v1`, paying as bytes arrive.
///
/// `expected_signer` is the delivering node's Ethereum address, used to verify
/// the response `slash_sig`. `byte_offset` resumes a partial fetch. Use
/// [`stream_fetch_tracked`] instead if you need to persist the voucher watermark
/// the channel reached (#852); this convenience wrapper discards it.
///
/// # Errors
///
/// Fails on connect/transport errors, an invalid or zero-rate response, a
/// `slash_sig` that does not recover to `expected_signer`, a mismatched echoed
/// field, a mid-stream `VoucherRejected`, a hash mismatch, or a timeout.
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
        byte_offset,
        timestamp_us,
        // The legacy single-deadline shape (#1134): this helper's callers are
        // loopback tests moving tiny blobs, for which one budget serving as both
        // the overall cap and the stall bound is harmless. Production paths take
        // `PullDeadlines` directly and split the two.
        PullDeadlines::whole_transfer(timeout),
        // No buyer-side blob-size ceiling on this test/loopback helper. The
        // production pull path does not go through here — it calls
        // `stream_fetch_tracked` directly (`node_origin::pull_from_candidate`)
        // with its configured `max_blob_size_bytes`.
        0,
        &mut VoucherProgress::default(),
    )
    .await
}

/// Like [`stream_fetch`], but reports the channel's acked voucher watermark via
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
/// Same as [`stream_fetch`].
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_tracked(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    deadlines: PullDeadlines,
    max_blob_size_bytes: u64,
    progress: &mut VoucherProgress,
) -> anyhow::Result<Bytes> {
    stream_fetch_tracked_with_progress(
        endpoint,
        target,
        ctx,
        slash_domain,
        expected_signer,
        hash,
        byte_offset,
        timestamp_us,
        deadlines,
        max_blob_size_bytes,
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
/// Same as [`stream_fetch`].
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_tracked_with_progress(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    deadlines: PullDeadlines,
    max_blob_size_bytes: u64,
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
            byte_offset,
            timestamp_us,
            max_blob_size_bytes,
            deadlines.open,
            deadlines.stall,
            &ledger,
            on_progress,
        ),
    )
    .await;
    // Copy the acked watermark back into `progress` on EVERY return path (Ok, Err,
    // timeout) BEFORE returning, so the latest acked totals survive a mid-stream
    // failure or a paid-but-corrupt delivery (#852). The ledger commits only after
    // an ack, so its snapshot is exactly the last acked cumulative.
    progress.set_from_cumulative(ledger.snapshot().await, ctx.prior_nonce);
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

/// Like [`stream_fetch`], but issues vouchers through a caller-owned shared
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
/// The caller owns the ledger's lifetime and reads its final cumulative via
/// [`ChannelLedger::snapshot`] to persist what the channel paid (this entrypoint
/// does not surface a [`VoucherProgress`] — the shared ledger IS the watermark).
///
/// # Errors
///
/// Same as [`stream_fetch`].
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
            byte_offset,
            timestamp_us,
            max_blob_size_bytes,
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
    byte_offset: u64,
    timestamp_us: u64,
    open: Duration,
) -> anyhow::Result<(
    iroh::endpoint::Connection,
    SendStream,
    RecvStream,
    StreamResponse,
)> {
    tokio::time::timeout(open, async move {
        // Full handshake — no 0-RTT on cdn/client/v1 (ADR 015).
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
            channel_id: ctx.channel_id.into(),
            byte_offset,
            // Whole-tail fetch; a bounded range is plumbed by the origin range-pull
            // path (ADR 037 §Origin-tier pull-through), not these node-to-node pulls.
            byte_len: 0,
            timestamp_us,
        };
        // Two-phase encode (ADR 005): attach the client identity binding when the
        // context carries one, so the serving node can prove channel ownership and
        // authorize a cache-miss origin pull (#1115). Absent ⇒ no ext bytes, exactly
        // the pre-#1115 wire (unbound node-to-node / registered-client path).
        let ext = client_binding_ext(ctx);
        let payload = decdn_protocol::encode_stream_request(&req, ext.as_ref())
            .map_err(|e| anyhow::anyhow!("encode stream request: {e}"))?;
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

#[allow(clippy::too_many_arguments)]
async fn fetch_inner(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
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
        byte_offset,
        timestamp_us,
        open,
    )
    .await?;

    if !resp.body.ok {
        return Err(refusal(resp.error));
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
    // A `total_bytes` below `byte_offset` would underflow the wire bound to `0`
    // (saturating), so the loop ends on the first `StreamEnd` and returns an
    // empty buffer. An empty range decodes trivially (no chunk group to verify),
    // so that empty buffer would surface as success — a silent verification
    // bypass. A legitimate server always claims `total_bytes >= byte_offset`;
    // reject anything less before the loop. (A non-empty but *short* delivery is
    // caught by the completeness check after the loop.)
    if resp.body.total_bytes < byte_offset {
        anyhow::bail!(
            "server claimed total_bytes ({}) below the requested byte_offset ({})",
            resp.body.total_bytes,
            byte_offset
        );
    }

    let rate_per_mb = resp.body.rate_per_mb;
    let interval_bytes = resp
        .voucher_interval_mb
        .unwrap_or(DEFAULT_VOUCHER_INTERVAL_MB)
        .saturating_mul(MB_BYTES);
    // Paid/received bytes are **wire** bytes — content plus interleaved bao proof
    // nodes (ADR 038 §Payment metering) — not the content-byte remainder.
    let total_bytes = resp.body.total_bytes;
    let expected_wire = aligned_wire_len(byte_offset, total_bytes)?;

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

/// The wire-byte bound for a resume at `byte_offset` of a `total_bytes` blob:
/// the bao-encoded size of the chunk-group-aligned range (content plus
/// interleaved proof, ADR 038 §Payment metering), exactly the byte count the
/// server emits. The server widens `byte_offset` to enclosing 16 KiB groups;
/// [`align_range`] / [`AlignedRange::wire_len`](decdn_bao_range::AlignedRange::wire_len)
/// reproduce that, keeping encoder and receiver in lock-step. Shared by the
/// buffered [`fetch_inner`] and progressive [`open_progressive_pull`] paths so
/// the two can't drift.
fn aligned_wire_len(byte_offset: u64, total_bytes: u64) -> anyhow::Result<u64> {
    let aligned = align_range(byte_offset, 0, total_bytes)
        .map_err(|e| anyhow::anyhow!("range alignment: {e}"))?;
    Ok(aligned.wire_len())
}

/// Drive the buffered receive loop: read `ChunkData` into a buffer, paying one
/// voucher per `interval_bytes` boundary (and a closing voucher once all
/// `expected_wire_bytes` have arrived) through the shared `ledger`, until
/// `StreamEnd`. Returns the assembled buffer and the cumulative byte count for
/// the caller's completeness check (integrity is verified per bao chunk group by
/// the decoder, not here). Enforces the chunk-size ceiling and the
/// `cumulative <= expected_wire_bytes` overrun guard (ADR 005 §`cdn/client/v1`).
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
    // The inactivity deadline (#1134). Reset ONLY where `cumulative` advances —
    // not on every frame — so neither a run of non-chunk frames nor (were #1088's
    // floor ever relaxed) a run of empty ones could hold it open without
    // delivering anything.
    let mut deadline = tokio::time::Instant::now() + stall;

    loop {
        let msg = tokio::time::timeout_at(deadline, read_client_message(recv))
            .await
            .map_err(|_| anyhow::Error::new(PullStalled { after: stall }))??;
        match msg {
            ClientMessage::ChunkData(chunk) => {
                // A chunk must carry 1..=CHUNK_SIZE bytes, and the running total
                // must not exceed what the response promised — otherwise a
                // malicious server could stream unbounded bytes (OOM) and we
                // would overpay (ADR 005 §`cdn/client/v1`). The non-empty floor
                // (#1088) is what makes every frame a unit of progress: an empty
                // one advances neither `cumulative` nor `bytes_since_voucher`, so
                // a run of them would spin this loop below the overrun guard (and
                // below the inactivity deadline, which resets only on bytes).
                chunk.validate()?;
                cumulative = cumulative.saturating_add(chunk.bytes.len() as u64);
                if cumulative > expected_wire_bytes {
                    anyhow::bail!(
                        "server sent {cumulative} bytes, more than the {expected_wire_bytes} promised"
                    );
                }
                // Bytes arrived: the upstream is alive, so extend the inactivity
                // deadline. It also extends after a completed voucher round trip
                // below — but both sites sit inside this `ChunkData` arm, past the
                // point where `cumulative` advanced, so only byte progress can ever
                // refresh it. That is the property `PullStalled` rests on.
                deadline = tokio::time::Instant::now() + stall;
                buf.extend_from_slice(&chunk.bytes);
                // Surface delivery progress after each chunk. `cumulative` and
                // `expected_wire_bytes` are both wire bytes, so the readout is
                // consistent (and can't overshoot — the guard above caps it).
                if let Some(cb) = on_progress {
                    cb(cumulative, expected_wire_bytes);
                }
                bytes_since_voucher = bytes_since_voucher.saturating_add(chunk.bytes.len() as u64);
                // Pay at each interval boundary, and a closing voucher once all
                // expected bytes have arrived — matching the node's pacing.
                let boundary = bytes_since_voucher >= interval_bytes && interval_bytes > 0;
                let closing = cumulative >= expected_wire_bytes && bytes_since_voucher > 0;
                if boundary || closing {
                    self_pay(
                        send,
                        recv,
                        ctx,
                        ledger,
                        rate_per_mb,
                        bytes_since_voucher,
                        stall,
                    )
                    .await?;
                    bytes_since_voucher = 0;
                    // The voucher exchange is a round trip we just completed, so
                    // the upstream is alive as of now — don't charge its latency
                    // against the next chunk's stall budget.
                    deadline = tokio::time::Instant::now() + stall;
                }
            }
            ClientMessage::StreamEnd => break,
            ClientMessage::StreamError(e) => anyhow::bail!("stream failed: {e:?}"),
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
        .map_err(|e| anyhow::anyhow!("range alignment: {e}"))?;
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
/// the buffered [`stream_fetch`]. Opened by [`open_progressive_pull`] (which has
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
    progress: VoucherProgress,
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
/// [`stream_fetch`] apply — zero-rate rejection, `slash_sig` recovery, echoed
/// field checks, the [`BlobTooLargeClaim`] ceiling, and the
/// `total_bytes >= byte_offset` floor — all enforced BEFORE the first chunk.
///
/// # Errors
///
/// Same set as [`stream_fetch`] for the handshake/response phase (connect /
/// transport, refused or zero-rate response, bad `slash_sig`, mismatched echoed
/// field, oversized `total_bytes`).
///
/// The open stage is bounded by `deadlines.open`, inside the shared `open_stream`
/// helper — NOT left to the caller (#1134). `node_origin` additionally wraps this
/// call in its per-candidate budget, which is belt-and-braces rather than the sole
/// bound. `deadlines.stall` is the INACTIVITY budget the returned [`UpstreamPull`]
/// carries into every streaming read; `deadlines.hard_cap` is not consulted here
/// (the caller owns the streaming lifetime on this path).
#[allow(clippy::too_many_arguments)]
pub async fn open_progressive_pull(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
    deadlines: PullDeadlines,
) -> anyhow::Result<(UpstreamPullHeader, UpstreamPull)> {
    let stall = deadlines.stall;
    let (conn, send, recv, resp) = open_stream(
        endpoint,
        target,
        ctx,
        slash_domain,
        expected_signer,
        hash,
        byte_offset,
        timestamp_us,
        deadlines.open,
    )
    .await?;
    if !resp.body.ok {
        return Err(refusal(resp.error));
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
    if resp.body.total_bytes < byte_offset {
        anyhow::bail!(
            "server claimed total_bytes ({}) below the requested byte_offset ({})",
            resp.body.total_bytes,
            byte_offset
        );
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
    let expected_wire_bytes = aligned_wire_len(byte_offset, total_bytes)?;
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
        progress: VoucherProgress::seed(ctx),
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

    /// The current acked voucher watermark — read it on any exit (including an
    /// error from `next_chunk`) to persist what was paid (#852).
    #[must_use]
    pub const fn progress(&self) -> VoucherProgress {
        self.progress
    }

    /// Issue one voucher for `delta_bytes` newly delivered since the last voucher
    /// through a one-shot ledger seeded from the current acked watermark, then copy
    /// the committed cumulative back into `self.progress`. The progressive pull owns
    /// a single stream, so there is no cross-stream contention to serialize here;
    /// the one-shot ledger just reuses the shared issue → sign → ack → commit path
    /// so the wire behavior matches `fetch_inner` exactly.
    async fn pay_one(&mut self, delta_bytes: u64) -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative {
            nonce: self.progress.nonce,
            bytes: self.progress.bytes_delivered,
            amount: self.progress.amount,
        });
        let result = self_pay(
            &mut self.send,
            &mut self.recv,
            &self.ctx,
            &ledger,
            self.rate_per_mb,
            delta_bytes,
            self.stall,
        )
        .await;
        // Copy back the committed watermark on every path: on Ok the ledger
        // advanced; on a rejected voucher it stayed put, so `progress` is unchanged.
        self.progress
            .set_from_cumulative(ledger.snapshot().await, self.ctx.prior_nonce);
        result
    }

    /// Read the next `ChunkData`, paying the upstream at each voucher-interval
    /// boundary (and a closing voucher once all promised bytes have arrived),
    /// and return the chunk for the caller to forward downstream + tee to cache.
    /// Returns `Ok(None)` on `StreamEnd`.
    ///
    /// # Errors
    ///
    /// An over-`CHUNK_SIZE` chunk, more bytes than promised, a mid-stream
    /// `StreamError`, an unexpected message, or a [`UpstreamVoucherRejected`] /
    /// transport error while paying.
    pub async fn next_chunk(&mut self) -> anyhow::Result<Option<Bytes>> {
        if self.ended {
            return Ok(None);
        }
        // The inactivity bound (#1134). A per-call budget IS the stall budget
        // here: every read that succeeds either carries bytes (#1088 bans empty
        // `ChunkData`) or terminates the stream (`StreamEnd` / `StreamError` /
        // anything else bails), so there is no frame a peer can send to hold this
        // open without making progress. The clock starts when we begin waiting,
        // not when the last chunk landed, so the caller's downstream-forward time
        // is not charged against the upstream's budget.
        let msg = tokio::time::timeout(self.stall, read_client_message(&mut self.recv))
            .await
            .map_err(|_| anyhow::Error::new(PullStalled { after: self.stall }))??;
        match msg {
            ClientMessage::ChunkData(chunk) => {
                // Bounds the payload on BOTH sides: the ceiling caps per-frame
                // allocation, and the non-empty floor keeps every frame a unit of
                // progress, so a peer cannot spin this loop (or refresh its
                // inactivity deadline) with an unbounded run of empty frames
                // (#1088).
                chunk.validate()?;
                self.cumulative = self.cumulative.saturating_add(chunk.bytes.len() as u64);
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
                self.unvouchered = self.unvouchered.saturating_add(chunk.bytes.len() as u64);
                let boundary = self.unvouchered >= self.interval_bytes && self.interval_bytes > 0;
                let closing = self.cumulative >= self.expected_wire_bytes && self.unvouchered > 0;
                if boundary || closing {
                    let delta = self.unvouchered;
                    self.pay_one(delta).await?;
                    self.unvouchered = 0;
                }
                Ok(Some(Bytes::from(chunk.bytes)))
            }
            ClientMessage::StreamEnd => {
                self.ended = true;
                Ok(None)
            }
            ClientMessage::StreamError(e) => anyhow::bail!("stream failed: {e:?}"),
            other => anyhow::bail!("unexpected message mid-delivery: {}", variant_name(&other)),
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
    /// A short delivery (fewer wire bytes than promised before `StreamEnd`), or a
    /// stream/protocol error while draining to the end of the stream.
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
                ClientMessage::StreamError(e) => anyhow::bail!("stream failed: {e:?}"),
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
        Ok(self.progress)
    }

    /// Abandon the pull (e.g. the downstream client dropped, so we stop pulling
    /// and paying). Closes the connection and returns the acked watermark so the
    /// caller can still persist what it paid (#852).
    #[must_use]
    pub fn abort(self) -> VoucherProgress {
        self.conn.close(0u32.into(), b"client-abandoned");
        self.progress
    }
}

impl Drop for UpstreamPull {
    /// Safety net for the "call a terminal method on every exit" contract: if a
    /// caller returns or panics without `finish`/`abort`, still close the upstream
    /// connection so the QUIC stream and the upstream's server-side serve task
    /// don't linger and keep that paid stream half-open. `Connection::close` is
    /// first-wins and idempotent, so an explicit close in `finish`/`abort` keeps
    /// its richer reason and this is a no-op when one of them ran; it only takes
    /// effect on a dropped-without-finalize path. The acked watermark cannot be
    /// recovered from `drop` (it can't be returned), so this bounds only the
    /// connection leak, not the #852 watermark loss the doc contract guards.
    fn drop(&mut self) {
        self.conn.close(0u32.into(), b"upstream-pull-dropped");
    }
}

/// Issue one cumulative voucher for `delta_bytes` newly delivered since the last
/// voucher, then await `VoucherAck`.
///
/// Voucher issuance runs through the channel's [`ChannelLedger`], which serializes
/// the compute → sign → send → await-ack → commit cycle across every concurrent
/// stream on the channel: the ledger holds its lock across the whole exchange, so
/// vouchers reach the node in strict nonce order even while byte transfers run in
/// parallel. Each voucher's own *delta* (`ceil(delta_bytes * rate / 1 MiB)`)
/// covers its own bytes at the advertised rate (the node checks deltas, not the
/// rounded cumulative). The ledger commits the advanced cumulative **only after**
/// the upstream acks, so a rejected voucher leaves the watermark at the last acked
/// value.
///
/// `stall` bounds the wait for the ack (#1134). Without it, an upstream that takes
/// our voucher and then goes silent would hang the pull forever: the caller's
/// inactivity deadline covers only the chunk reads, and the overall `hard_cap` is
/// off by default. Bounding it here is no more hazardous than the QUIC idle
/// timeout that used to be the sole backstop — the voucher is already on the wire
/// either way, and in both cases we leave without the ack, so the ledger does not
/// commit and our persisted watermark can lag what the node accepted. That desync
/// is pre-existing and tracked in #1122; this only makes the bound prompt and
/// application-level rather than a transport accident.
async fn self_pay(
    send: &mut SendStream,
    recv: &mut RecvStream,
    ctx: &ChannelContext,
    ledger: &ChannelLedger,
    rate_per_mb: u64,
    delta_bytes: u64,
    stall: Duration,
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
            .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}"))?;
            write_message(
                send,
                &ClientMessage::Voucher(signed_to_wire_voucher(&signed)),
            )
            .await?;
            let ack = tokio::time::timeout(stall, read_client_message(recv))
                .await
                .map_err(|_| anyhow::Error::new(PullStalled { after: stall }))??;
            match ack {
                // The upstream persists before it acks (ADR 003), so this is the
                // cumulative total it has accepted — let the ledger commit it.
                ClientMessage::VoucherAck => Ok(()),
                // Only a `VoucherRejected` is OUR payment-side fault. Carry its
                // typed reason so the orchestrator can exonerate the provider
                // (#857). Any OTHER `StreamError` here is the upstream violating
                // the ack protocol (only `VoucherAck`/`VoucherRejected` are valid
                // in reply to a voucher), so it stays a bare error and is
                // classified as provider-attributable upstream.
                ClientMessage::StreamError(StreamError::VoucherRejected { reason }) => {
                    Err(anyhow::Error::new(UpstreamVoucherRejected { reason }))
                }
                ClientMessage::StreamError(e) => {
                    anyhow::bail!("unexpected stream error awaiting voucher ack: {e:?}")
                }
                other => anyhow::bail!("expected VoucherAck, got {}", variant_name(&other)),
            }
        })
        .await
        .map(|_committed| ())
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
    let payload = encode_message(msg).map_err(|e| anyhow::anyhow!("encode failed: {e}"))?;
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

    use super::{HashMismatch, decode_verified_range};

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
}
