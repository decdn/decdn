//! Client fixture: drives the real paid client path (`cdn/client/v1`) against a
//! [`crate::node::NodeFixture`] — open an on-chain `PaymentPool`, self-issue the
//! owner capability delegating spend to the buyer's own key, dial the daemon's
//! QUIC endpoint, and run a voucher-signed `stream_fetch`, returning the
//! delivered (BLAKE3-verified) bytes.
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
use decdn_client_pull::buyer_pool::open_pool;
use decdn_client_pull::{
    BlobTooLargeClaim, HashMismatch, PoolContext, PullDeadlines, UpstreamRefused,
    UpstreamVoucherRejected, VoucherProgress, sign_client_binding, stream_fetch_tracked,
};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{
    BuyerPoolState, SignedCapability, bind_node_id_domain, slash_judge_domain, voucher_domain,
};
use decdn_protocol::client::{ClientMessage, StreamError, StreamResponse, WireCapability};
use decdn_protocol::{
    ALPN_CLIENT, StreamRequest, StreamRequestExt, decode_message, encode_stream_request,
};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};

use crate::bindings::Erc20;
use crate::chain::ChainFixture;
use crate::node::NodeFixture;

/// Default pool deposit: 10 USDC (ADR 003 recommended minimum). Public so a
/// journey can assert the daemon reports this exact deposit for the pool.
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

/// One open payment pool, reusable across several single-shot fetches
/// ([`ClientFixture::fetch_once`]) and the raw wire tap
/// ([`ClientFixture::capture_delivery_wire`]). The session pins ONE delivering
/// provider (the fixture's node), so its `(signer, provider)` voucher lane is
/// the one it advances.
///
/// [`ClientFixture::fetch`] opens a throwaway pool per call and rides out the
/// node's readiness window by retrying — which is exactly what a journey
/// asserting on a *refusal* cannot do, since the window and a real refusal are
/// the same wire `NotFound`. A session pays that cost once, up front.
///
/// Never derive `Clone`: two sessions sharing one pool would fork the lane's
/// cumulative voucher watermark and replay it against the node.
#[derive(Debug)]
pub struct PoolSession {
    ctx: PoolContext,
    target: EndpointAddr,
    slash_domain: Eip712Domain,
    /// The delivering node's operator address — the `expected_signer` that
    /// verifies the response `slash_sig`, and the voucher lane's `provider`.
    /// Deliberately *not* called `provider` at the fixture level: in an
    /// alloy-facing file that word means the RPC handle.
    operator_addr: Address,
}

impl PoolSession {
    /// On-chain `poolId` this session's vouchers draw from.
    #[must_use]
    pub const fn pool_id(&self) -> B256 {
        self.ctx.pool_id
    }

    /// Fold whatever the node acked into the lane's cumulative voucher
    /// watermark, so the next fetch signs the following cumulative rather than
    /// replaying a stale one.
    ///
    /// [`VoucherProgress::advanced`] is `None` when nothing was paid (the common
    /// refusal path), which correctly leaves the watermark untouched.
    ///
    /// # Errors
    ///
    /// If the watermark would go backwards. That should be unreachable — the
    /// ledger only commits on ack — but a silent rewind would resurface much
    /// later as an opaque `UpstreamVoucherRejected { AmountRegression }` on an
    /// unrelated fetch, so it is worth naming at the point it happens.
    fn record_progress(&mut self, progress: &VoucherProgress) -> anyhow::Result<()> {
        let Some((bytes_delivered, amount)) = progress.advanced() else {
            return Ok(());
        };
        anyhow::ensure!(
            amount >= self.ctx.prior_amount,
            "voucher watermark regressed: acked amount {amount} < prior {}",
            self.ctx.prior_amount
        );
        self.ctx.prior_bytes_delivered = bytes_delivered;
        self.ctx.prior_amount = amount;
        // Carry the lane's chain epoch forward too. Every fetch on this session
        // builds a fresh ledger from `ctx`, so a stale counter re-derives the
        // seed the previous fetch already used — and the node, still holding
        // that root at a non-zero frontier, treats every reveal under it as
        // already covered. The delivery then stalls with nothing crediting it.
        self.ctx.prior_epoch = self.ctx.prior_epoch.max(progress.next_epoch());
        Ok(())
    }
}

/// Result of a paid fetch: the delivered bytes and the pool they were paid from.
#[derive(Debug)]
pub struct FetchOutcome {
    /// Delivered, BLAKE3-verified payload.
    pub bytes: Vec<u8>,
    /// On-chain `poolId` the vouchers drew from.
    pub pool_id: B256,
}

/// A funded buyer that pays nodes for delivery over `cdn/client/v1`.
#[derive(Debug)]
pub struct ClientFixture {
    signer: Arc<PrivateKeySigner>,
    endpoint: Endpoint,
}

impl ClientFixture {
    /// Create a funded client: fresh eth key with gas, a mock-USDC balance, and
    /// a max approval for the `PaymentPool`, plus a loopback iroh endpoint.
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
                chain.addrs().payment_pool,
                U256::from(DEPOSIT_MICRO_USDC) * U256::from(100u64),
            )
            .send()
            .await
            .context("client approve PaymentPool")?
            .get_receipt()
            .await
            .context("client approve receipt")?;
        crate::ensure_mined(&approve_receipt, "client approve")?;

        let endpoint = loopback_endpoint().await?;
        Ok(Self { signer, endpoint })
    }

    /// The buyer's Ethereum address (pool owner / voucher signer), for journeys
    /// that assert on client-side `PaymentPool` state.
    #[must_use]
    pub fn address(&self) -> alloy::primitives::Address {
        self.signer.address()
    }

    /// The buyer's voucher-signing key, for journeys that drive a `client-pull`
    /// entry point directly rather than through [`Self::fetch`].
    #[must_use]
    pub const fn signer(&self) -> &Arc<PrivateKeySigner> {
        &self.signer
    }

    /// The buyer's loopback iroh endpoint, for the same reason as
    /// [`Self::signer`].
    #[must_use]
    pub const fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Open a pool to pay `node`, then fetch `hash` over the paid path, retrying
    /// until the node's serve path accepts the pool's vouchers. Verifies the
    /// delivered bytes hash to `hash`.
    pub async fn fetch(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        hash: Hash,
        namespace_id: alloy::primitives::U256,
    ) -> anyhow::Result<FetchOutcome> {
        self.fetch_with_deadline(chain, node, hash, namespace_id, Duration::from_secs(45))
            .await
    }

    /// [`Self::fetch`] with a caller-chosen readiness-retry budget.
    ///
    /// A *negative* journey that expects the fetch to fail — a corrupt outboard
    /// the node can never serve, say — has no success to converge on, so it burns
    /// the whole budget spinning on the node's refusal before returning the error
    /// the assertion wants. Such a caller passes a budget tighter than the 45s
    /// [`Self::fetch`] default to keep its runtime bounded.
    ///
    /// Tightening the budget is safe **only for expect-failure callers**: with no
    /// success to miss, a shorter budget can only surface the expected failure
    /// sooner. A *positive* fetch is the opposite case — too tight a budget can
    /// expire in the window between opening the pool and the node's `getPool`
    /// view resolving it, failing a fetch the node would have served moments
    /// later. A positive caller must keep enough headroom above the node's
    /// serve-path catch-up.
    pub async fn fetch_with_deadline(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        hash: Hash,
        namespace_id: alloy::primitives::U256,
        retry_budget: Duration,
    ) -> anyhow::Result<FetchOutcome> {
        let mut session = self.open_pool_session(chain, node).await?;
        let pid = session.pool_id();
        let target = session.target.clone();

        // The node serves a pool's vouchers once its `getPool` view resolves the
        // freshly-opened pool. Retry the paid fetch until that catch-up completes
        // or the budget expires.
        let deadline = tokio::time::Instant::now() + retry_budget;
        loop {
            // `stream_fetch_tracked` reports the acked voucher watermark via
            // `progress` on every return path (Ok/Err/timeout), so a retry after a
            // mid-stream failure that already consumed a voucher can resume from the
            // lane's advanced cumulative instead of replaying from zero (#1062). No
            // blob-size ceiling on this loopback path.
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
                        pool_id: pid,
                    });
                }
                Err(e) if tokio::time::Instant::now() < deadline && is_retryable(&e) => {
                    // `advanced()` is `None` for the common pre-observation failure,
                    // leaving the watermark at zero (correct for a never-served pool).
                    session.record_progress(&progress)?;
                    tracing::debug!(
                        "paid fetch not ready ({e}); retrying after serve-path catch-up"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                // Deadline expired, or a terminal error (`is_retryable` == false):
                // return the real cause immediately rather than spinning to the
                // deadline and misreporting a corruption/desync as a readiness timeout.
                Err(e) => return Err(e).context("paid fetch failed"),
            }
        }
    }

    /// Open a funded [`PoolSession`] to `node` and return it only once the node's
    /// **serve path** has delivered on it, so every later [`Self::fetch_once`] on
    /// the session is unambiguous.
    ///
    /// A lane is created in the node's store only when it accepts the first
    /// voucher — the node registers no lane at pool-open time (it reads the pool
    /// live via `getPool`). So this waits by driving a retried warm-up fetch of
    /// `warmup` — a blob the node is known to hold — until it succeeds. A served
    /// blob is proof the serve path accepts this pool, because it had to deliver
    /// and take a voucher. The warm-up bytes are returned so a caller can keep
    /// asserting on them rather than paying for a throwaway delivery.
    ///
    /// # Errors
    ///
    /// If the pool never opens, or the warm-up fetch never succeeds within the
    /// readiness budget (or fails terminally — a non-retryable error is returned
    /// immediately with its real cause).
    pub async fn open_session(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        warmup: Hash,
    ) -> anyhow::Result<(PoolSession, Vec<u8>)> {
        self.open_session_in_namespace(chain, node, warmup, alloy::primitives::U256::ZERO)
            .await
    }

    /// [`Self::open_session`] with a caller-chosen `namespace_id` for the warm-up
    /// fetch.
    ///
    /// The namespace matters when `warmup` is a blob the node does **not** hold
    /// itself and must acquire through node-to-node pull-through: the serving
    /// node routes its own cache miss to a provider via the on-chain
    /// `OriginAssignment` directory, which is keyed on the request's namespace
    /// (`NO_NAMESPACE`/`U256::ZERO` resolves to no origins). A cross-node warm-up
    /// under `U256::ZERO` would therefore never discover the provider and the
    /// session would never register. Own-origin warm-ups are namespace-agnostic
    /// (the origin backend is hash-keyed), so [`Self::open_session`] keeps the
    /// zero default.
    pub async fn open_session_in_namespace(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        warmup: Hash,
        namespace_id: alloy::primitives::U256,
    ) -> anyhow::Result<(PoolSession, Vec<u8>)> {
        // No pre-fetch `wait_for_pool`: the pool model registers a lane only when
        // the node intakes the first voucher (it reads the pool live via `getPool`,
        // not at pool-open time), so the retried warm-up fetch below is itself the
        // readiness gate — a served warm-up blob proves the serve path accepts this
        // pool, because it had to deliver and take a voucher.
        let mut session = self.open_pool_session(chain, node).await?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        loop {
            match self.fetch_once(&mut session, warmup, 0, namespace_id).await {
                Ok(bytes) => return Ok((session, bytes)),
                Err(e) if tokio::time::Instant::now() < deadline && is_retryable(&e) => {
                    tracing::debug!("session warm-up not ready ({e}); retrying");
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Err(e) => {
                    return Err(e).context(
                        "session warm-up fetch never succeeded; the node's serve path \
                         never accepted this pool",
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
    /// because it cannot tell the readiness window from a real refusal.
    /// [`Self::open_session`] has already ruled that window out — its warm-up
    /// fetch proves the serve path accepts the pool — so an error here is the
    /// node's verdict and reaches the caller typed (e.g. downcast to
    /// [`UpstreamRefused`]).
    ///
    /// # Errors
    ///
    /// Propagates whatever `stream_fetch_tracked` returns. The lane's voucher
    /// watermark is folded in first on every path — but only *advances* when the
    /// node actually acked a voucher, so a refusal leaves it untouched.
    ///
    /// Note the residual hazard on the error path: `stream_fetch_tracked` reports
    /// the **acked** watermark, and the node commits a voucher before writing its
    /// ack (ADR 003). If an ack is lost mid-stream the session falls behind the
    /// node, and the *next* fetch on this session fails as
    /// `UpstreamVoucherRejected { AmountRegression }`. Open a fresh session after
    /// any mid-delivery failure rather than reusing this one.
    pub async fn fetch_once(
        &self,
        session: &mut PoolSession,
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
    /// `StreamResponse` + every `ChunkData` up to the chunk and then
    /// blocks on payment, so for a sub-interval blob this captures **every
    /// message the node emits before it blocks** — but never `StreamEnd`,
    /// which is emitted only after a voucher arrives. The capture
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
        session: &PoolSession,
        hash: Hash,
        namespace_id: alloy::primitives::U256,
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
            // The backend-fill gate keys on the request namespace, so the tap must
            // carry the ratified namespace the blob is published under — otherwise
            // the authorized-origin gate refuses and the tap captures a refusal
            // instead of a delivery. The origin backend itself remains hash-keyed
            // and opaque (it never sees the namespace).
            namespace_id: namespace_id.to_be_bytes(),
            pool_id: session.ctx.pool_id.into(),
            byte_offset: 0,
            byte_len: 0,
            timestamp_us: TIMESTAMP_US,
        };
        // `pull_authorized` gates every backend-fill tier on the client binding,
        // and the capability lets the node register the lane; without them the tap
        // would only ever capture a refusal.
        let ext = session.request_ext();
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

    /// Capture the daemon's signed `StreamResponse { ok: true }` for a **held**
    /// blob at a caller-chosen `timestamp_us` — the delivery side of a real
    /// rate-manipulation evidence pair (#1042).
    ///
    /// The node signs the open-stage `StreamResponse` (committing to deliver at
    /// its *current* rate) before any voucher is exchanged, so this reads only
    /// the first frame and never pays. Unlike [`Self::capture_delivery_wire`] the
    /// timestamp is caller-controlled, so the response lands inside the 30s
    /// probe↔stream slashing window; unlike [`Self::refused_stream`] it requires
    /// the node to actually deliver (`ok == true`) — a refusal here means the
    /// blob was not held or the pool was not yet served, a setup failure.
    pub async fn capture_delivery_response(
        &self,
        session: &PoolSession,
        hash: Hash,
        namespace_id: U256,
        timestamp_us: u64,
    ) -> anyhow::Result<StreamResponse> {
        let conn = self
            .endpoint
            .connect(session.target.clone(), ALPN_CLIENT)
            .await
            .map_err(|e| anyhow::anyhow!("connect for delivery capture: {e}"))?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi for delivery capture: {e}"))?;
        let req = StreamRequest {
            hash: *hash.as_bytes(),
            namespace_id: namespace_id.to_be_bytes(),
            pool_id: session.ctx.pool_id.into(),
            byte_offset: 0,
            byte_len: 0,
            timestamp_us,
        };
        let ext = session.request_ext();
        let payload = encode_stream_request(&req, Some(&ext))
            .context("encode delivery-capture StreamRequest")?;
        decdn_protocol::write_frame(&mut send, &payload)
            .await
            .context("write delivery-capture StreamRequest")?;

        let frame =
            tokio::time::timeout(WIRE_TAP_FIRST_FRAME, decdn_protocol::read_frame(&mut recv))
                .await
                .context("delivery capture: node sent no open frame")?
                .context("delivery capture: read frame")?;
        conn.close(0u32.into(), b"delivery capture complete");

        let (msg, _rest) = decode_message::<ClientMessage>(&frame)
            .context("decode delivery-capture open frame")?;
        match msg {
            ClientMessage::StreamResponse(resp) => {
                anyhow::ensure!(
                    resp.body.ok,
                    "delivery capture expected ok:true, got a refusal ({:?})",
                    resp.error
                );
                Ok(resp)
            }
            _ => anyhow::bail!("delivery capture expected a StreamResponse open frame"),
        }
    }

    /// Open and fund a payment pool for `node`, self-issue the owner capability
    /// delegating spend to the buyer's own key, and pin the delivering provider
    /// (the node's operator) plus the ADR 005 client binding, returning the
    /// session a paid fetch needs. Does **not** wait for the node to serve the
    /// pool — [`Self::fetch`] rides that window out by retrying,
    /// [`Self::open_session`] proves it with a warm-up fetch.
    async fn open_pool_session(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
    ) -> anyhow::Result<PoolSession> {
        let provider = chain.provider_for(&self.signer);
        let contract = PaymentPool::new(chain.addrs().payment_pool, provider);
        let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_pool);
        let deposit = U256::from(DEPOSIT_MICRO_USDC);

        // The shared open kernel escrows the deposit, decodes the authoritative
        // `poolId` from the `PoolOpened` receipt, and signs the self-owned
        // capability the node registers on the first redemption.
        let opened = open_pool(
            &contract,
            Arc::clone(&self.signer),
            &voucher_dom,
            chain.usdc(),
            self.signer.address(),
            deposit,
        )
        .await
        .context("open buyer pool")?;

        // Pin the delivering provider (voucher lane target), attach the client
        // identity binding (proves pool ownership for reactive origin fill), and
        // carry the owner capability so the node can register this signer on its
        // first on-chain redemption.
        let bind_domain = bind_node_id_domain(chain.chain_id(), chain.addrs().capacity_bond);
        let own_node_id = B256::from(*self.endpoint.id().as_bytes());
        let ctx = opened
            .ctx
            .with_provider(node.operator_addr(), U256::ZERO, U256::ZERO)
            .with_client_binding(sign_client_binding(
                &self.signer,
                own_node_id,
                &bind_domain,
            )?)
            .with_capability(opened.capability);

        let slash_domain = slash_judge_domain(chain.chain_id(), chain.addrs().slash_judge);
        // Pinned into the session at open time, at the identity the daemon is
        // serving under now. A session deliberately keeps that pin: its pool and
        // lane watermark belong to this connection, so it must not silently follow
        // the node onto a new identity mid-life.
        let target = Self::target(node).await?;

        Ok(PoolSession {
            ctx,
            target,
            slash_domain,
            operator_addr: node.operator_addr(),
        })
    }

    /// Probe `node` for `hash` over `cdn/probe/v1` and return the signed
    /// response. Unpaid (no pool) — used to assert the daemon's probe handler
    /// reports `has_blob: false` after a blacklist eviction (the
    /// blacklist-violation compliance seam).
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
            Self::target(node).await?,
            *hash.as_bytes(),
            timestamp_us,
            Duration::from_secs(10),
        )
        .await
        .context("probe daemon")?;
        Ok(resp)
    }

    /// Probe an **explicit** iroh identity at a loopback port, rather than
    /// whichever id a [`NodeFixture`] recorded at launch (#1034).
    ///
    /// The key-rotation journey needs both directions of this: the retired id
    /// must stop answering, and the new one must start. Neither is expressible
    /// through [`Self::probe`], which dials `NodeFixture::node_id()` — a value
    /// frozen at launch, and therefore the *old* id after a rotation.
    ///
    /// A failure here is a genuine "that identity is not reachable at this
    /// socket": iroh authenticates the peer's public key during the QUIC
    /// handshake, so dialing a retired id against the same address does not
    /// silently connect to whoever is listening — it fails to establish.
    pub async fn probe_node_id(
        &self,
        node_id: iroh::PublicKey,
        port: u16,
        hash: Hash,
    ) -> anyhow::Result<decdn_protocol::ProbeResponse> {
        let target = EndpointAddr::new(node_id)
            .with_ip_addr(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)));
        let (resp, _rtt) = decdn_client_pull::probe::probe_once(
            &self.endpoint,
            target,
            *hash.as_bytes(),
            TIMESTAMP_US,
            // Shorter than `probe_at`'s 10s on purpose: the interesting call is
            // the one that must NOT connect, and a retired id fails by timing
            // out on a handshake nobody answers. Ten seconds of that per
            // assertion is dead wall-clock.
            Duration::from_secs(5),
        )
        .await
        .context("probe explicit node id")?;
        Ok(resp)
    }

    /// Run **one** paid-stream open against `node` on an already-open `pool_id`
    /// and return the daemon's signed refusal (#1042).
    ///
    /// Errors unless the node refused up front with an `ok == false`
    /// `StreamResponse` — a delivery, a transport failure, or a mid-stream
    /// (unsigned) `StreamError` are all failures of the journey's setup, not
    /// results, and are surfaced as such rather than silently yielding no
    /// evidence.
    ///
    /// Why an *existing* pool rather than a fresh one: `serve_stream` resolves
    /// the pool only *after* the blob-availability gate
    /// (`handlers::client::dispatch`), so a `NotFound` refusal returns before the
    /// lookup. Reusing a pool a successful [`Self::fetch`] already proved the node
    /// serves removes the confound of an unresolved pool suppressing the fill
    /// tiers. No voucher is exchanged on a refusal, so the zeroed lane watermark
    /// cannot go stale here.
    pub async fn refused_stream(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        pool_id: B256,
        hash: Hash,
        timestamp_us: u64,
    ) -> anyhow::Result<decdn_protocol::client::StreamResponse> {
        let ctx = self.pool_context(chain, node, pool_id)?;
        let slash_domain = slash_judge_domain(chain.chain_id(), chain.addrs().slash_judge);
        let err = match stream_fetch_tracked(
            &self.endpoint,
            Self::target(node).await?,
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
        let Some(refused) = err.downcast_ref::<UpstreamRefused>() else {
            // A voucher rejection here is a *different* finding than a wrong
            // error type: it means the induced condition did not take — the node
            // served the open stage, then rejected the replayed voucher this
            // helper sends. Name that precisely (#1379), so a no-op eviction or an
            // unexpectedly-present hash surfaces as "served when it should have
            // refused" rather than "expected a typed refusal".
            if let Some(rejected) = err.downcast_ref::<UpstreamVoucherRejected>() {
                anyhow::bail!(
                    "node served when it should have refused: the open stage \
                     succeeded and the replayed voucher was rejected ({rejected})"
                );
            }
            anyhow::bail!("expected a typed refusal, got: {err:#}");
        };
        // `evidence()` is `None` only for a mid-stream `StreamError` frame, which is
        // unsigned and therefore useless as evidence — a setup failure here. #1377
        // makes this the ONLY way to obtain the signed response, so an unsigned
        // mid-stream refusal can no longer be mistaken for on-chain evidence.
        refused.evidence().cloned().ok_or_else(|| {
            anyhow::anyhow!(
                "refusal carried no signed StreamResponse (mid-stream {:?}?)",
                refused.error()
            )
        })
    }

    /// The loopback dial target for `node`, at the identity it is serving under
    /// **right now**.
    ///
    /// Async because it asks the daemon (`admin_v1_health`) rather than reading
    /// `NodeFixture::node_id()`, which is frozen at launch and therefore names
    /// the retired key after a rotation (#1034). Every dial in this fixture goes
    /// through here, so a rotation journey does not have to special-case the
    /// paid path — and no other journey can quietly start dialing a stale id.
    ///
    /// The extra admin round trip is paid once per pool open or probe, all of
    /// which already require a live daemon.
    async fn target(node: &NodeFixture) -> anyhow::Result<EndpointAddr> {
        Ok(
            EndpointAddr::new(node.current_node_id().await?).with_ip_addr(SocketAddr::V4(
                SocketAddrV4::new(Ipv4Addr::LOCALHOST, node.bind_port()),
            )),
        )
    }

    /// A [`PoolContext`] bound to an existing `pool_id`, at a zero lane watermark
    /// and carrying this client's identity binding, the delivering provider, and
    /// the self-owned capability so the node can register the signer on a first
    /// redemption.
    fn pool_context(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        pool_id: B256,
    ) -> anyhow::Result<PoolContext> {
        let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_pool);
        let state = BuyerPoolState::new(
            pool_id,
            self.signer.address(),
            chain.usdc(),
            U256::from(DEPOSIT_MICRO_USDC),
        );
        // Uncapped, matching production self-issue: the delegate IS the pool
        // owner, so the pool deposit — not the capability cap — is the real
        // spending bound.
        let capability = decdn_client_pull::buyer_pool::issue_self_capability(
            self.signer.as_ref(),
            pool_id,
            U256::MAX,
            u64::MAX,
            &voucher_dom,
        )
        .context("sign self capability")?;
        let bind_domain = bind_node_id_domain(chain.chain_id(), chain.addrs().capacity_bond);
        let own_node_id = B256::from(*self.endpoint.id().as_bytes());
        Ok(
            PoolContext::for_pool(&state, Arc::clone(&self.signer), voucher_dom)
                .with_provider(node.operator_addr(), U256::ZERO, U256::ZERO)
                .with_client_binding(sign_client_binding(
                    &self.signer,
                    own_node_id,
                    &bind_domain,
                )?)
                .with_capability(capability),
        )
    }
}

impl PoolSession {
    /// The trailing [`StreamRequestExt`] this session's manual-wire helpers send:
    /// the client identity binding plus the owner capability, so the node's
    /// serve/fill gates authorize the request and can register the lane. Mirrors
    /// what `client-pull` attaches to a `stream_fetch` request.
    fn request_ext(&self) -> StreamRequestExt {
        StreamRequestExt {
            binding: self.ctx.client_binding.clone(),
            capability: self.ctx.capability.as_ref().map(signed_to_wire_capability),
        }
    }
}

/// Lower a [`SignedCapability`] into its wire form for a manual `StreamRequestExt`
/// (`client-pull` does the same internally on the `stream_fetch` path).
fn signed_to_wire_capability(signed: &SignedCapability) -> WireCapability {
    WireCapability {
        spending_cap: signed.capability.spending_cap.to_be_bytes(),
        expiry: signed.capability.expiry,
        owner_signature: signed.signature.as_bytes().to_vec(),
    }
}

/// Classify a `stream_fetch_tracked` error: `true` if it is the transient
/// serve-path-catch-up condition the retry loop is meant to wait out, `false` if
/// re-running with the same pool would only spin to the deadline.
///
/// The loop exists to ride out the node's readiness window, during which it
/// refuses delivery up front — its `getPool` view has not resolved the pool yet,
/// which reaches us as the wire `NotFound` that `ServeRejectReason::wire_error`
/// collapses seven reject reasons onto. That, transport errors, a per-attempt
/// `PullTimeout`, and a `PullStalled` (#1134 — an upstream that went silent
/// mid-stream; retryable here because in a loopback fixture the node is coming
/// up, not dying) are all retryable. A node-side persist fault also lands here:
/// it aborts the stream cleanly with no in-band voucher rejection, so it reaches
/// us as one of these same transport/stall errors rather than a typed downcast,
/// and the client resends the same voucher on a fresh stream. `NotFound` is
/// decided by an explicit arm (the `UpstreamRefused` downcast below); transport
/// errors, `PullTimeout`, and `PullStalled` reach the closing `true` by
/// fallthrough.
///
/// Everything else is terminal: a corrupt delivery (`HashMismatch`), a buyer-side
/// size-cap rejection (`BlobTooLargeClaim`), any other mid-stream voucher
/// rejection (`UpstreamVoucherRejected` — e.g. an `AmountRegression` left by
/// one-sided ack loss, deposit exhaustion, or a `SpendingCapExhausted`), or a refusal by
/// which the node reports itself degraded / the blob over its own ceiling.
/// Retrying cannot fix any of those, so we fail fast and surface the real cause.
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
    if err.downcast_ref::<UpstreamVoucherRejected>().is_some() {
        // Every mid-stream voucher rejection is a terminal payment-state desync
        // — a node-side persist fault aborts the stream instead of surfacing
        // here, so it never reaches this arm.
        return false;
    }
    if let Some(refused) = err.downcast_ref::<UpstreamRefused>() {
        // Allowlist the genuinely transient refusals: `NotFound` is the readiness
        // window this loop exists for (and, more broadly, a node that may hold the
        // blob on a later attempt); `EvictedSinceProbe` / `Overloaded` are
        // likewise transient. Everything else is terminal and says the same thing
        // on every attempt — a node that reports itself degraded
        // (`InternalError`), the blob as over its ceiling (`BlobTooLarge`), or a
        // governance/legal takedown of the content (`HashBlacklisted` /
        // `OriginBlacklisted`). Retrying those only spins the loop to its
        // deadline, so fail fast and surface the real cause.
        //
        // This is an allowlist rather than a denylist of terminal codes so a new
        // terminal `StreamError` variant defaults to fail-fast, not retry-to-45s.
        return matches!(
            refused.error(),
            StreamError::NotFound | StreamError::EvictedSinceProbe | StreamError::Overloaded
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
