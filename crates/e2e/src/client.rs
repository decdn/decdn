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
/// How long [`ClientFixture::capture_delivery_wire`] waits *between* frames
/// before deciding the node has finished speaking. The tap never pays, so the
/// node's closing-voucher pause is the terminator on a successful delivery.
///
/// Load-bearing coupling: this must stay below the node's `VOUCHER_READ_TIMEOUT`
/// (10s, `crates/node/src/handlers/client/mod.rs`), or the node gives up on the
/// voucher and resets the stream before the tap decides it has gone idle.
const WIRE_TAP_IDLE: Duration = Duration::from_secs(5);
/// How long the tap waits for the *first* frame. Far larger than
/// [`WIRE_TAP_IDLE`] because the first frame is gated on the node's entire
/// reactive backend fill (`try_local_populate`, budgeted at
/// `cache.node_pull_timeout_sec`), not on an idle pause — a tapped blob is
/// deliberately absent from the store.
const WIRE_TAP_FIRST_FRAME: Duration = Duration::from_secs(30);

/// One open payment channel to one node, reusable across several single-shot
/// fetches ([`ClientFixture::fetch_once`]) and the raw wire tap
/// ([`ClientFixture::capture_delivery_wire`]).
///
/// [`ClientFixture::fetch`] opens a throwaway channel per call and rides out the
/// node's pre-observation window by retrying — which is exactly what a journey
/// asserting on a *refusal* cannot do, since the window and a real refusal are
/// the same wire `NotFound`. A session pays that cost once, up front.
///
/// Never derive `Clone`: two sessions sharing one channel would fork the voucher
/// watermark and replay nonces against the node.
#[derive(Debug)]
pub struct ChannelSession {
    ctx: ChannelContext,
    target: EndpointAddr,
    slash_domain: Eip712Domain,
    /// The delivering node's operator address — the `expected_signer` that
    /// verifies the response `slash_sig`. Deliberately *not* called `provider`:
    /// in an alloy-facing file that word means the RPC handle.
    operator_addr: Address,
}

impl ChannelSession {
    /// On-chain `channelId` this session's vouchers are signed against.
    #[must_use]
    pub const fn channel_id(&self) -> B256 {
        self.ctx.channel_id
    }

    /// Fold whatever the node acked into the session's voucher watermark, so the
    /// next fetch signs the following nonce rather than replaying a stale one.
    ///
    /// `acked()` is `None` when nothing was acked (the common refusal path),
    /// which correctly leaves the watermark untouched.
    ///
    /// # Errors
    ///
    /// If the watermark would go backwards. That should be unreachable — the
    /// ledger only commits on ack — but a silent rewind would resurface much
    /// later as an opaque `UpstreamVoucherRejected { StaleNonce }` on an
    /// unrelated fetch, so it is worth naming at the point it happens.
    fn record_progress(&mut self, progress: &VoucherProgress) -> anyhow::Result<()> {
        let Some((nonce, bytes_delivered, amount)) = progress.acked() else {
            return Ok(());
        };
        anyhow::ensure!(
            nonce >= self.ctx.prior_nonce,
            "voucher watermark regressed: acked nonce {nonce} < prior {}",
            self.ctx.prior_nonce
        );
        self.ctx.prior_nonce = nonce;
        self.ctx.prior_bytes_delivered = bytes_delivered;
        self.ctx.prior_amount = amount;
        Ok(())
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
        namespace_id: alloy::primitives::U256,
    ) -> anyhow::Result<FetchOutcome> {
        let mut session = self.open_channel(chain, node).await?;
        let cid = session.channel_id();
        let target = session.target.clone();

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
                &session.ctx,
                &session.slash_domain,
                session.operator_addr,
                *hash.as_bytes(),
                namespace_id.to_be_bytes(),
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
                    // `acked()` is `None` for the common pre-observation failure,
                    // leaving the watermark at zero (correct for a never-observed
                    // channel).
                    session.record_progress(&progress)?;
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

    /// Open a funded [`ChannelSession`] to `node` and return it only once the
    /// node's **serve path** has registered it, so every later
    /// [`Self::fetch_once`] on the session is unambiguous.
    ///
    /// Two separate facts have to hold, and only the second one matters to a
    /// refusal assertion:
    ///
    /// 1. [`crate::node::NodeFixture::wait_for_channel`] polls the admin API,
    ///    which reads the *persisted* channel store.
    /// 2. `serve_stream` / `pull_authorized` gate on `ClientHandler`'s in-memory
    ///    map, which `register_open_channel` populates only *after* awaiting the
    ///    store fsync. So (1) can be true while the serve path still answers
    ///    `UnknownChannel` → wire `NotFound`.
    ///
    /// Waiting on (1) alone would leave every session's first single-shot fetch
    /// riding that gap. So this also fetches `warmup` — a blob the node is known
    /// to hold in cache — retrying until it succeeds. A served blob is proof the
    /// live map is populated, because the serve path had to read it. The warm-up
    /// bytes are returned so a caller can keep asserting on them rather than
    /// paying for a throwaway delivery.
    ///
    /// # Errors
    ///
    /// If the channel never reaches the store, or the warm-up fetch never
    /// succeeds within the readiness budget (or fails terminally — a
    /// non-retryable error is returned immediately with its real cause).
    pub async fn open_session(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        warmup: Hash,
    ) -> anyhow::Result<(ChannelSession, Vec<u8>)> {
        let mut session = self.open_channel(chain, node).await?;
        node.wait_for_channel(session.channel_id(), Duration::from_secs(60))
            .await?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        loop {
            match self
                .fetch_once(&mut session, warmup, 0, alloy::primitives::U256::ZERO)
                .await
            {
                Ok(bytes) => return Ok((session, bytes)),
                Err(e) if tokio::time::Instant::now() < deadline && is_retryable(&e) => {
                    tracing::debug!("session warm-up not ready ({e}); retrying");
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Err(e) => {
                    return Err(e).context(
                        "session warm-up fetch never succeeded; the node's serve path \
                         never registered this channel",
                    );
                }
            }
        }
    }

    /// Run **one** paid fetch on `session` — no readiness retry loop — and return
    /// the delivered bytes (`byte_offset > 0` requests the tail from that offset,
    /// bao-verified against the whole-blob hash by the requester).
    ///
    /// The refusal is the point: [`Self::fetch`] retries a wire `NotFound` for 45s
    /// because it cannot tell the pre-observation window from a real refusal.
    /// [`Self::open_session`] has already ruled that window out — its warm-up
    /// fetch proves the serve path holds the channel — so an error here is the
    /// node's verdict and reaches the caller typed (e.g. downcast to
    /// [`UpstreamRefused`]).
    ///
    /// # Errors
    ///
    /// Propagates whatever `stream_fetch_tracked` returns. The session's voucher
    /// watermark is folded in first on every path — but only *advances* when the
    /// node actually acked a voucher, so a refusal leaves it untouched.
    ///
    /// Note the residual hazard on the error path: `stream_fetch_tracked` reports
    /// the **acked** watermark, and the node commits a voucher before writing its
    /// ack (ADR 003). If an ack is lost mid-stream the session falls one nonce
    /// behind the node, and the *next* fetch on this session fails as
    /// `UpstreamVoucherRejected { StaleNonce }`. Open a fresh session after any
    /// mid-delivery failure rather than reusing this one.
    pub async fn fetch_once(
        &self,
        session: &mut ChannelSession,
        hash: Hash,
        byte_offset: u64,
        namespace_id: alloy::primitives::U256,
    ) -> anyhow::Result<Vec<u8>> {
        let mut progress = VoucherProgress::default();
        let result = stream_fetch_tracked(
            &self.endpoint,
            session.target.clone(),
            &session.ctx,
            &session.slash_domain,
            session.operator_addr,
            *hash.as_bytes(),
            namespace_id.to_be_bytes(),
            byte_offset,
            TIMESTAMP_US,
            PullDeadlines::new(Duration::from_secs(30), Duration::from_secs(30))?,
            0,
            &mut progress,
        )
        .await;
        session.record_progress(&progress)?;
        Ok(result?.as_ref().to_vec())
    }

    /// Drive a raw `cdn/client/v1` delivery for `hash` on `session` and return
    /// every framed message the node sent, verbatim.
    ///
    /// This is the wire tap G-NODE-08 needs: no client-side interpretation — the
    /// frame payloads a delivering node put on the QUIC stream, so a journey can
    /// assert an opaque backend's location is not among them. (Payloads, not raw
    /// stream bytes: `read_frame` consumes the varint length prefix.)
    ///
    /// Deliberately never pays, which bounds what it can see. The node streams
    /// `StreamResponse` + every `ChunkData` up to the voucher interval and then
    /// blocks on payment, so for a sub-interval blob this captures **every
    /// message the node emits before it blocks** — but never `VoucherAck` or
    /// `StreamEnd`, which are emitted only after a voucher arrives. The capture
    /// ends when the node falls silent for a few seconds (that payment pause) or
    /// the stream closes cleanly.
    ///
    /// Side effect: tapping a blob the node does not hold drives a real reactive
    /// backend fill, leaving the blob **warmed in the node's store**. Run this
    /// after any assertion that depends on the blob being absent, and treat it as
    /// the last operation on `session` — the node is left blocked awaiting a
    /// voucher that never comes.
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
            // Wire-tap of the backend-fill path; no namespace routing needed
            // (the node serves from its own origin backend by hash).
            namespace_id: decdn_protocol::client::NO_NAMESPACE,
            channel_id: session.ctx.channel_id.into(),
            byte_offset: 0,
            byte_len: 0,
            timestamp_us: TIMESTAMP_US,
        };
        // `pull_authorized` gates every backend-fill tier on the client binding;
        // without it the tap would only ever capture a refusal. (It is framed
        // upstream as authorizing the node to *spend* on the fill — true of a
        // metered origin like S3; a local `fs` backend costs nothing, but the
        // same gate still has to pass.)
        let ext = StreamRequestExt {
            voucher_interval_mb: None,
            binding: session.ctx.client_binding.clone(),
        };
        let payload =
            encode_stream_request(&req, Some(&ext)).context("encode wire-tap StreamRequest")?;
        decdn_protocol::write_frame(&mut send, &payload)
            .await
            .context("write wire-tap StreamRequest")?;

        // Reads until the stream closes cleanly (a refusal, then `finish`) or the
        // node goes idle waiting for the voucher we never send: either way it has
        // said everything it is going to say.
        //
        // A genuine transport/framing fault is NOT a terminator — folding it into
        // the idle case would silently return a truncated capture, and every
        // "the backend is absent from these frames" assertion downstream would be
        // that much weaker without saying so.
        let mut frames: Vec<Vec<u8>> = Vec::new();
        loop {
            let budget = if frames.is_empty() {
                WIRE_TAP_FIRST_FRAME
            } else {
                WIRE_TAP_IDLE
            };
            match tokio::time::timeout(budget, decdn_protocol::read_frame(&mut recv)).await {
                Ok(Ok(frame)) => frames.push(frame),
                // Idle: the expected terminator on a successful delivery.
                Err(_) => break,
                // Clean end of stream.
                Ok(Err(decdn_protocol::FrameError::Io(e)))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break;
                }
                Ok(Err(e)) => {
                    let captured: usize = frames.iter().map(Vec::len).sum();
                    conn.close(0u32.into(), b"wire tap failed");
                    return Err(anyhow::anyhow!(
                        "wire tap read failed after {} frames ({captured} bytes): {e}",
                        frames.len()
                    ));
                }
            }
        }
        conn.close(0u32.into(), b"wire tap complete");
        anyhow::ensure!(
            !frames.is_empty(),
            "node sent nothing on the delivery stream within {WIRE_TAP_FIRST_FRAME:?}"
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
            operator_addr: node.operator_addr(),
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
        self.probe_at(node, hash, TIMESTAMP_US).await
    }

    /// [`Self::probe`] with a caller-chosen `timestamp_us`.
    ///
    /// The probe timestamp is requester-generated and echoed back *inside* the
    /// signed body (ADR 005), so it is what anchors the pair on-chain: the
    /// `SlashJudge` 30s window and the evidence-staleness bound are both computed
    /// from it. The default `TIMESTAMP_US` is a fixed sentinel and would read as
    /// 1970 to the judge, so any journey feeding a real response to `SlashJudge`
    /// must stamp it near chain time — and any journey testing the *window* stamps
    /// it deliberately far from the stream's (#1042).
    pub async fn probe_at(
        &self,
        node: &NodeFixture,
        hash: Hash,
        timestamp_us: u64,
    ) -> anyhow::Result<decdn_protocol::ProbeResponse> {
        let (resp, _rtt) = decdn_client_pull::probe::probe_once(
            &self.endpoint,
            Self::target(node),
            *hash.as_bytes(),
            timestamp_us,
            false, // full handshake keeps the probe deterministic
            None,
            Duration::from_secs(10),
        )
        .await
        .context("probe daemon")?;
        Ok(resp)
    }

    /// Run **one** paid-stream open against `node` on an already-open
    /// `channel_id` and return the daemon's signed refusal (#1042).
    ///
    /// Errors unless the node refused up front with an `ok == false`
    /// `StreamResponse` — a delivery, a transport failure, or a mid-stream
    /// (unsigned) `StreamError` are all failures of the journey's setup, not
    /// results, and are surfaced as such rather than silently yielding no
    /// evidence.
    ///
    /// Why an *existing* channel rather than a fresh one: for the ~seconds after
    /// `openChannel` the node's chain watcher has not decoded `ChannelOpened`, so
    /// the channel is unrecognized. That does **not** change the refusal code here
    /// — `serve_stream` resolves the channel only *after* the blob-availability
    /// gate (`handlers::client::dispatch`), so both refusals this helper captures
    /// return before the lookup. What it does change is the *reason*:
    /// `handlers::client::fill::pull_authorized` returns `false` for an
    /// unrecognized channel, suppressing the range / local-origin / node-to-node
    /// fill tiers — so a fresh channel can turn a fillable miss into a `NotFound`
    /// for a reason unrelated to the journey. Reusing a channel a successful
    /// [`Self::fetch`] already proved the node accepts removes that confound. No
    /// voucher is exchanged on a refusal, so the zeroed `prior_*` watermark cannot
    /// go stale here.
    pub async fn refused_stream(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        channel_id: B256,
        hash: Hash,
        timestamp_us: u64,
    ) -> anyhow::Result<decdn_protocol::client::StreamResponse> {
        let ctx = self.channel_context(chain, channel_id)?;
        let slash_domain = slash_judge_domain(chain.chain_id(), chain.addrs().slash_judge);
        let err = match stream_fetch_tracked(
            &self.endpoint,
            Self::target(node),
            &ctx,
            &slash_domain,
            node.operator_addr(),
            *hash.as_bytes(),
            // Refusal tap: the point is that the node refuses; no namespace routing.
            decdn_protocol::client::NO_NAMESPACE,
            0,
            timestamp_us,
            PullDeadlines::new(Duration::from_secs(30), Duration::from_secs(30))?,
            0,
            &mut VoucherProgress::default(),
        )
        .await
        {
            Ok(bytes) => anyhow::bail!(
                "expected a refusal, but the node delivered {} bytes",
                bytes.len()
            ),
            Err(e) => e,
        };
        let refused = err
            .downcast_ref::<UpstreamRefused>()
            .ok_or_else(|| anyhow::anyhow!("expected a typed refusal, got: {err:#}"))?;
        // `response` is `None` only for a mid-stream `StreamError` frame, which is
        // unsigned and therefore useless as evidence — a setup failure here.
        refused.response.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "refusal carried no signed StreamResponse (mid-stream {:?}?)",
                refused.error
            )
        })
    }

    /// The loopback dial target for `node`.
    fn target(node: &NodeFixture) -> EndpointAddr {
        EndpointAddr::new(node.node_id()).with_ip_addr(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            node.bind_port(),
        )))
    }

    /// A [`ChannelContext`] bound to an existing `channel_id`, at a zero voucher
    /// watermark and carrying this client's identity binding.
    fn channel_context(
        &self,
        chain: &ChainFixture,
        channel_id: B256,
    ) -> anyhow::Result<ChannelContext> {
        let bind_domain = bind_node_id_domain(chain.chain_id(), chain.addrs().capacity_bond);
        let own_node_id = B256::from(*self.endpoint.id().as_bytes());
        Ok(ChannelContext {
            channel_id,
            token: chain.usdc(),
            deposit: U256::from(DEPOSIT_MICRO_USDC),
            client_signer: Arc::clone(&self.signer),
            voucher_domain: voucher_domain(chain.chain_id(), chain.addrs().payment_channel),
            prior_nonce: U256::ZERO,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
        }
        .with_client_binding(sign_client_binding(
            &self.signer,
            own_node_id,
            &bind_domain,
        )?))
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
