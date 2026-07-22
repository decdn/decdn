//! Client fixture: drives the real paid client path (`cdn/client/v1`) against a
//! [`crate::node::NodeFixture`] — open an on-chain `PaymentChannel`, dial the
//! daemon's QUIC endpoint, and run a voucher-signed `stream_fetch`, returning
//! the delivered (BLAKE3-verified) bytes.
//!
//! The client runs in-process (it is not a daemon, so it has none of the
//! global-state constraints that push the node fixture to a subprocess) and
//! dials the daemon over real QUIC by direct loopback address.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_client_pull::{
    BlobTooLargeClaim, ChannelContext, HashMismatch, PullDeadlines, UpstreamRefused,
    UpstreamVoucherRejected, VoucherProgress, sign_client_binding, stream_fetch_tracked,
};
use decdn_incentive::{bind_node_id_domain, slash_judge_domain, voucher_domain};
use decdn_protocol::client::StreamError;
use decdn_protocol::{ALPN_CLIENT, StreamRequest, StreamRequestExt, encode_stream_request};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};

use crate::assert::{channel_id, client_channel_nonce};
use crate::bindings::{Erc20, PaymentChannelOpen};
use crate::chain::ChainFixture;
use crate::node::NodeFixture;

/// Default channel deposit: 10 USDC (≥ the contract `minDeposit`). Public so a
/// journey can assert the daemon reports this exact deposit for the channel.
pub const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// Fixed slash-receipt timestamp (µs). The node does not gate freshness on this
/// path; a constant keeps the voucher deterministic (matches the settlement
/// e2e).
const TIMESTAMP_US: u64 = 0x00c0_ffe1;
/// How long [`ClientFixture::capture_delivery_wire`] waits for the next frame
/// before deciding the node has finished speaking. The tap never pays, so the
/// node's closing-voucher pause is the terminator on a successful delivery.
const WIRE_TAP_IDLE: Duration = Duration::from_secs(5);

/// One open payment channel to one node, reusable across several single-shot
/// fetches ([`ClientFixture::fetch_once`]) and the raw wire tap
/// ([`ClientFixture::capture_delivery_wire`]).
///
/// [`ClientFixture::fetch`] opens a throwaway channel per call and rides out the
/// node's pre-observation window by retrying — which is exactly what a journey
/// asserting on a *refusal* cannot do, since the window and a real refusal are
/// the same wire `NotFound`. A session pays that cost once, up front.
#[derive(Debug)]
pub struct ChannelSession {
    ctx: ChannelContext,
    target: EndpointAddr,
    slash_domain: Eip712Domain,
    /// The delivering node's Ethereum address; verifies the response `slash_sig`.
    provider: Address,
    channel_id: B256,
}

impl ChannelSession {
    /// On-chain `channelId` this session's vouchers are signed against.
    #[must_use]
    pub const fn channel_id(&self) -> B256 {
        self.channel_id
    }
}

/// Result of a paid fetch: the delivered bytes and the channel they were paid
/// through.
#[derive(Debug)]
pub struct FetchOutcome {
    /// Delivered, BLAKE3-verified payload.
    pub bytes: Vec<u8>,
    /// On-chain `channelId` the vouchers were signed against.
    pub channel_id: B256,
}

/// A funded buyer that pays nodes for delivery over `cdn/client/v1`.
#[derive(Debug)]
pub struct ClientFixture {
    signer: Arc<PrivateKeySigner>,
    endpoint: Endpoint,
}

impl ClientFixture {
    /// Create a funded client: fresh eth key with gas, a mock-USDC balance, and
    /// a max approval for the `PaymentChannel`, plus a loopback iroh endpoint.
    pub async fn new(chain: &ChainFixture) -> anyhow::Result<Self> {
        let signer = Arc::new(PrivateKeySigner::random());
        let addr = signer.address();
        chain.fund_eth(addr, 100).await?;
        chain
            .mint_usdc(addr, U256::from(1_000_000_000u64))
            .await
            .context("mint client USDC")?;

        // One-time max-ish approval so repeated opens don't each re-approve.
        let provider = chain.provider_for(&signer);
        let approve_receipt = Erc20::new(chain.usdc(), &provider)
            .approve(
                chain.addrs().payment_channel,
                U256::from(DEPOSIT_MICRO_USDC) * U256::from(100u64),
            )
            .send()
            .await
            .context("client approve PaymentChannel")?
            .get_receipt()
            .await
            .context("client approve receipt")?;
        crate::ensure_mined(&approve_receipt, "client approve")?;

        let endpoint = loopback_endpoint().await?;
        Ok(Self { signer, endpoint })
    }

    /// The buyer's Ethereum address (channel owner / voucher signer), for
    /// journeys that assert on client-side `PaymentChannel` state.
    #[must_use]
    pub fn address(&self) -> alloy::primitives::Address {
        self.signer.address()
    }

    /// Open a channel to `node`, then fetch `hash` over the paid path, retrying
    /// until the node's chain watcher has observed the `ChannelOpened` event and
    /// begins accepting the channel's vouchers. Verifies the delivered bytes
    /// hash to `hash`.
    pub async fn fetch(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        hash: Hash,
    ) -> anyhow::Result<FetchOutcome> {
        let mut session = self.open_channel(chain, node).await?;
        let cid = session.channel_id;
        let target = session.target.clone();
        let ctx = &mut session.ctx;
        let slash_domain = &session.slash_domain;

        // The node accepts vouchers only once its chain watcher has decoded the
        // ChannelOpened event (poll cadence ~500ms). Retry the paid fetch until
        // that catch-up completes or the budget expires.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        loop {
            // `stream_fetch_tracked` reports the acked voucher watermark via
            // `progress` on every return path (Ok/Err/timeout), so a retry after a
            // mid-stream failure that already consumed a voucher can resume from the
            // node's advanced nonce instead of replaying nonce 0 (#1062). No blob-size
            // ceiling on this loopback path — mirrors the plain `stream_fetch` wrapper.
            let mut progress = VoucherProgress::default();
            match stream_fetch_tracked(
                &self.endpoint,
                target.clone(),
                ctx,
                slash_domain,
                node.operator_addr(),
                *hash.as_bytes(),
                0,
                TIMESTAMP_US,
                // Loopback fixture: wall clock on the open, inactivity on the
                // stream, no overall cap (#1134). Both budgets are non-zero literals, so
                // the `ZeroBudget` arm is unreachable here — propagate rather than unwrap.
                PullDeadlines::new(Duration::from_secs(30), Duration::from_secs(30))?,
                0,
                &mut progress,
            )
            .await
            {
                Ok(bytes) => {
                    let got = bytes.as_ref().to_vec();
                    anyhow::ensure!(
                        Hash::new(&got) == hash,
                        "delivered bytes do not hash to the requested blob"
                    );
                    return Ok(FetchOutcome {
                        bytes: got,
                        channel_id: cid,
                    });
                }
                Err(e) if tokio::time::Instant::now() < deadline && is_retryable(&e) => {
                    // Fold whatever the node acked back into `ctx` so the next attempt
                    // signs the next nonce rather than replaying a stale one. `acked()`
                    // is `None` for the common pre-observation failure, leaving `ctx` at
                    // zero (correct for a never-observed channel).
                    if let Some((nonce, bytes_delivered, amount)) = progress.acked() {
                        ctx.prior_nonce = nonce;
                        ctx.prior_bytes_delivered = bytes_delivered;
                        ctx.prior_amount = amount;
                    }
                    tracing::debug!("paid fetch not ready ({e}); retrying after watcher catch-up");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                // Deadline expired, or a terminal error (`is_retryable` == false):
                // return the real cause immediately rather than spinning to the
                // deadline and misreporting a corruption/desync as a readiness timeout.
                Err(e) => return Err(e).context("paid fetch failed"),
            }
        }
    }

    /// Open a funded [`ChannelSession`] to `node` and wait until the daemon's
    /// settlement watcher has registered it, so every later single-shot fetch on
    /// the session is unambiguous — see [`Self::fetch_once`].
    pub async fn open_session(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
    ) -> anyhow::Result<ChannelSession> {
        let session = self.open_channel(chain, node).await?;
        node.wait_for_channel(session.channel_id, Duration::from_secs(60))
            .await?;
        Ok(session)
    }

    /// Run **one** paid fetch on `session` — no readiness retry loop — and return
    /// the delivered bytes (`byte_offset > 0` requests the tail from that offset,
    /// bao-verified against the whole-blob hash by the requester).
    ///
    /// The refusal is the point: [`Self::fetch`] retries a wire `NotFound` for 45s
    /// because it cannot tell the pre-observation window from a real refusal.
    /// `open_session` has already ruled the window out, so an error here is the
    /// node's actual verdict and reaches the caller typed (e.g. downcast to
    /// [`UpstreamRefused`]).
    ///
    /// # Errors
    ///
    /// Propagates whatever `stream_fetch_tracked` returns; the session's voucher
    /// watermark is advanced first on every path, so a later fetch on the same
    /// session signs the next nonce rather than replaying a stale one.
    pub async fn fetch_once(
        &self,
        session: &mut ChannelSession,
        hash: Hash,
        byte_offset: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let mut progress = VoucherProgress::default();
        let result = stream_fetch_tracked(
            &self.endpoint,
            session.target.clone(),
            &session.ctx,
            &session.slash_domain,
            session.provider,
            *hash.as_bytes(),
            byte_offset,
            TIMESTAMP_US,
            PullDeadlines::new(Duration::from_secs(30), Duration::from_secs(30))?,
            0,
            &mut progress,
        )
        .await;
        if let Some((nonce, bytes_delivered, amount)) = progress.acked() {
            session.ctx.prior_nonce = nonce;
            session.ctx.prior_bytes_delivered = bytes_delivered;
            session.ctx.prior_amount = amount;
        }
        Ok(result?.as_ref().to_vec())
    }

    /// Drive a raw `cdn/client/v1` delivery for `hash` on `session` and return
    /// every framed message the node sent, verbatim.
    ///
    /// This is the wire tap G-NODE-08 needs: no client-side decoding, no
    /// interpretation — the exact bytes a delivering node put on the QUIC stream,
    /// so a journey can assert an opaque backend's location is not among them.
    ///
    /// Deliberately never pays: the node streams `StreamResponse` + every
    /// `ChunkData` up to the voucher interval before pausing for payment, so for a
    /// sub-interval blob this captures the complete node→client message set. The
    /// capture ends when the node falls silent for `WIRE_TAP_IDLE` (the pause
    /// waiting for the voucher that never comes) or the stream closes.
    pub async fn capture_delivery_wire(
        &self,
        session: &ChannelSession,
        hash: Hash,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let conn = self
            .endpoint
            .connect(session.target.clone(), ALPN_CLIENT)
            .await
            .map_err(|e| anyhow::anyhow!("connect for wire tap: {e}"))?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi for wire tap: {e}"))?;
        let req = StreamRequest {
            hash: *hash.as_bytes(),
            channel_id: session.ctx.channel_id.into(),
            byte_offset: 0,
            byte_len: 0,
            timestamp_us: TIMESTAMP_US,
        };
        // The binding is what authorizes the node to spend on a cache-miss fill
        // (`pull_authorized`); without it the tap would only ever capture a refusal.
        let ext = StreamRequestExt {
            voucher_interval_mb: None,
            binding: session.ctx.client_binding.clone(),
        };
        let payload =
            encode_stream_request(&req, Some(&ext)).context("encode wire-tap StreamRequest")?;
        decdn_protocol::write_frame(&mut send, &payload)
            .await
            .context("write wire-tap StreamRequest")?;

        // Reads until the stream closes (a refusal, then `finish`) or the node
        // goes idle waiting for the voucher we never send: either way it has said
        // everything it is going to say.
        let mut frames = Vec::new();
        while let Ok(Ok(frame)) =
            tokio::time::timeout(WIRE_TAP_IDLE, decdn_protocol::read_frame(&mut recv)).await
        {
            frames.push(frame);
        }
        conn.close(0u32.into(), b"wire tap complete");
        anyhow::ensure!(
            !frames.is_empty(),
            "node sent nothing on the delivery stream"
        );
        Ok(frames)
    }

    /// Open and fund a payment channel to `node`, returning the session state a
    /// paid fetch needs. Does **not** wait for the node to observe the channel —
    /// [`Self::fetch`] rides that window out by retrying, [`Self::open_session`]
    /// waits it out explicitly.
    async fn open_channel(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
    ) -> anyhow::Result<ChannelSession> {
        let client_addr = self.signer.address();
        let provider = chain.provider_for(&self.signer);
        let pc = PaymentChannelOpen::new(chain.addrs().payment_channel, &provider);

        let nonce = client_channel_nonce(&provider, chain.addrs().payment_channel, client_addr)
            .await
            .context("read client channel nonce")?;
        let deposit = U256::from(DEPOSIT_MICRO_USDC);
        let open_receipt = pc
            .openChannel(node.operator_addr(), deposit)
            .send()
            .await
            .context("openChannel send")?
            .get_receipt()
            .await
            .context("openChannel receipt")?;
        crate::ensure_mined(&open_receipt, "openChannel")?;
        let cid = channel_id(
            client_addr,
            node.operator_addr(),
            u64::try_from(nonce).context("channel nonce overflow")?,
        );

        let ctx = ChannelContext {
            channel_id: cid,
            token: chain.usdc(),
            deposit,
            client_signer: Arc::clone(&self.signer),
            voucher_domain: voucher_domain(chain.chain_id(), chain.addrs().payment_channel),
            prior_nonce: U256::ZERO,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
        };
        let bind_domain = bind_node_id_domain(chain.chain_id(), chain.addrs().capacity_bond);
        let own_node_id = B256::from(*self.endpoint.id().as_bytes());
        let ctx = ctx.with_client_binding(sign_client_binding(
            &self.signer,
            own_node_id,
            &bind_domain,
        )?);
        let slash_domain = slash_judge_domain(chain.chain_id(), chain.addrs().slash_judge);
        let target = EndpointAddr::new(node.node_id()).with_ip_addr(SocketAddr::V4(
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, node.bind_port()),
        ));

        Ok(ChannelSession {
            ctx,
            target,
            slash_domain,
            provider: node.operator_addr(),
            channel_id: cid,
        })
    }

    /// Probe `node` for `hash` over `cdn/probe/v1` and return the signed
    /// response. Unpaid (no channel) — used to assert the daemon's probe handler
    /// reports `has_blob: false` after eviction (the phantom-blob slash seam).
    pub async fn probe(
        &self,
        node: &NodeFixture,
        hash: Hash,
    ) -> anyhow::Result<decdn_protocol::ProbeResponse> {
        let target = EndpointAddr::new(node.node_id()).with_ip_addr(SocketAddr::V4(
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, node.bind_port()),
        ));
        let (resp, _rtt) = decdn_client_pull::probe::probe_once(
            &self.endpoint,
            target,
            *hash.as_bytes(),
            TIMESTAMP_US,
            false, // full handshake keeps the probe deterministic
            None,
            Duration::from_secs(10),
        )
        .await
        .context("probe daemon")?;
        Ok(resp)
    }
}

/// Classify a `stream_fetch_tracked` error: `true` if it is the transient
/// watcher-catch-up condition the retry loop is meant to wait out, `false` if
/// re-running with the same channel would only spin to the deadline.
///
/// The loop exists to ride out the node's pre-observation window, during which it
/// refuses delivery up front — its channel is `UnknownChannel` until the chain
/// watcher decodes `ChannelOpened`, which reaches us as the wire `NotFound` that
/// `ServeRejectReason::wire_error` collapses seven reject reasons onto. That,
/// transport errors, a per-attempt `PullTimeout`, a `PullStalled` (#1134 — an upstream
/// that went silent mid-stream; retryable here because in a loopback fixture the node
/// is coming up, not dying), and the node's explicit `RetryLater` resend signal are all
/// retryable. `NotFound` and `RetryLater` are decided by explicit arms (the
/// `UpstreamRefused` and `UpstreamVoucherRejected` downcasts below); transport errors,
/// `PullTimeout`, and `PullStalled` reach the closing `true` by fallthrough.
///
/// Everything else is terminal: a corrupt delivery (`HashMismatch`), a buyer-side
/// size-cap rejection (`BlobTooLargeClaim`), any other mid-stream voucher
/// rejection (`UpstreamVoucherRejected` — e.g. a stale nonce left by one-sided ack
/// loss, deposit exhaustion, or an expired channel), or a refusal by which the
/// node reports itself degraded / the blob over its own ceiling. Retrying cannot
/// fix any of those, so we fail fast and surface the real cause.
///
/// The refusal arm is only this precise because the wire code now survives as a
/// typed `UpstreamRefused` (#1144); before that a refusal was an opaque string and
/// every one of them — including a hard `InternalError` — was retried to the
/// deadline.
fn is_retryable(err: &anyhow::Error) -> bool {
    if err.downcast_ref::<HashMismatch>().is_some()
        || err.downcast_ref::<BlobTooLargeClaim>().is_some()
    {
        return false;
    }
    if let Some(rejected) = err.downcast_ref::<UpstreamVoucherRejected>() {
        // `RetryLater` is a transient node-side persist failure that asks us to
        // resend the same voucher on a fresh stream; every other reason is a
        // terminal payment-state desync.
        return matches!(
            rejected.reason,
            decdn_protocol::VoucherRejectReason::RetryLater
        );
    }
    if let Some(refused) = err.downcast_ref::<UpstreamRefused>() {
        // `NotFound` is the pre-observation window this loop exists for (and, more
        // broadly, a node that may hold the blob on a later attempt).
        // `EvictedSinceProbe` / `Overloaded` are likewise transient. A node that
        // reports itself degraded, or the blob as over its ceiling, will say the
        // same thing on every attempt.
        return !matches!(
            refused.error,
            StreamError::InternalError | StreamError::BlobTooLarge
        );
    }
    true
}

/// Bind a loopback iroh endpoint with relays disabled (no ALPNs — client only
/// dials). Mirrors the node integration-test `support::local_endpoint` helper.
async fn loopback_endpoint() -> anyhow::Result<Endpoint> {
    Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::generate())
        .alpns(vec![])
        .relay_mode(RelayMode::Disabled)
        .bind_addr(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .map_err(|e| anyhow::anyhow!("bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("bind iroh endpoint: {e}"))
}
