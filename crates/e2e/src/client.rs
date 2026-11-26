//! Client fixture: drives the real paid client path (`cdn/client/v1`) against a
//! [`crate::node::NodeFixture`] — open an on-chain `PaymentPool`, self-issue the
//! owner capability delegating spend to the buyer's own key, dial the daemon's
//! QUIC endpoint, and run a voucher-signed `stream_fetch_tracked` (the
//! `test-util` in-memory driver of the one receive loop), returning the
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
use decdn_client::buyer_pool::{OwnedPayer, open_pool, self_owned_lane_ctx};
use decdn_client::{
    BlobTooLarge, Connections, HashMismatch, OpenedStream, PoolContext, PullDeadlines,
    UpstreamRefused, UpstreamVoucherRejected, VoucherProgress, sign_client_binding,
    stream_fetch_tracked,
};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{BuyerPoolState, Deployment, capacity_bond_domain};
use decdn_protocol::client::StreamError;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};

use crate::bindings::Erc20;
use crate::chain::ChainFixture;
use crate::node::NodeFixture;

/// Default pool deposit: 10 USDC (ADR 003 recommended minimum). Public so a
/// journey can assert the daemon reports this exact deposit for the pool.
pub const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// Fresh request timestamp (µs): the host's wall clock, which is also the
/// daemon's. The node refuses any request whose `timestamp_us` sits beyond
/// its clock-skew bound, so every fixture request stamps a current read; a
/// journey that needs a particular timestamp (evidence pairing) passes its
/// own through the `*_at` variants, staying inside that bound at capture
/// time. The daemon's own clock helper, so the fixture and the gate it must
/// satisfy cannot drift apart.
use decdn_node::handlers::timestamp_skew::now_us as timestamp_now_us;
/// How long [`ClientFixture::discovery_open`] waits for the dial, and again
/// for the answer.
const DISCOVERY_OPEN: Duration = Duration::from_secs(10);

/// One open payment pool, reusable across several single-shot fetches
/// ([`ClientFixture::fetch_once`]). The session pins ONE delivering
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
    stream_domain: Eip712Domain,
    /// The delivering node's operator address — the `expected_signer` that
    /// verifies the response `operator_sig`, and the voucher lane's `provider`.
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
        // The cumulative is all there is to carry. Every fetch on this session
        // builds a fresh ledger from `ctx`, and a fresh ledger draws a fresh
        // chain — so what the next fetch needs from this one is the money owed,
        // which its first voucher folds into the amount it signs.
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
    // The `PaymentPool` this client approves, opens pools on, and signs
    // vouchers for.
    payment_pool: Address,
}

impl ClientFixture {
    /// Create a funded client: fresh eth key with gas, a mock-USDC balance, and
    /// an approval for 100 deposits on the snapshot `PaymentPool`, plus a
    /// loopback iroh endpoint.
    pub async fn new(chain: &ChainFixture) -> anyhow::Result<Self> {
        Self::new_on_payment_pool(chain, chain.addrs().payment_pool).await
    }

    /// Like [`Self::new`], but paying through the `PaymentPool` at
    /// `payment_pool` instead of the snapshot's — a client of a node repointed
    /// at a redeployed pool (see [`ChainFixture::redeploy_payment_pool`]).
    pub async fn new_on_payment_pool(
        chain: &ChainFixture,
        payment_pool: Address,
    ) -> anyhow::Result<Self> {
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
                payment_pool,
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
        Ok(Self {
            signer,
            endpoint,
            payment_pool,
        })
    }

    /// The buyer's Ethereum address (pool owner / voucher signer), for journeys
    /// that assert on client-side `PaymentPool` state.
    #[must_use]
    pub fn address(&self) -> alloy::primitives::Address {
        self.signer.address()
    }

    /// The buyer's voucher-signing key, for journeys that drive a `decdn-client`
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
                &session.stream_domain,
                session.operator_addr,
                *hash.as_bytes(),
                namespace_id.to_be_bytes(),
                0,
                timestamp_now_us(),
                // Loopback fixture: wall clock on the open, inactivity on the
                // stream, no overall cap (#1134). Both budgets are non-zero literals, so
                // the `ZeroBudget` arm is unreachable here — propagate rather than unwrap.
                PullDeadlines::new(Duration::from_secs(30), Duration::from_secs(30), 4096)?,
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
            &session.stream_domain,
            session.operator_addr,
            *hash.as_bytes(),
            namespace_id.to_be_bytes(),
            byte_offset,
            timestamp_now_us(),
            PullDeadlines::new(Duration::from_secs(30), Duration::from_secs(30), 4096)?,
            0,
            0,
            &mut progress,
        )
        .await;
        session.record_progress(&progress)?;
        Ok(result?.as_ref().to_vec())
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
        let contract = PaymentPool::new(self.payment_pool, provider);
        let deposit = U256::from(DEPOSIT_MICRO_USDC);

        // The shared open kernel escrows the deposit and decodes the
        // authoritative `poolId` from the `PoolOpened` receipt.
        let deployment = self.deployment(chain);
        // With no handle, nothing reads the pending entry the guard holds.
        let (opened, _unrecorded) = open_pool(
            &contract,
            deployment,
            chain.usdc(),
            self.signer.address(),
            deposit,
            None,
            None,
            None,
        )
        .await
        .context("open buyer pool")?;

        // Pin the delivering provider (voucher lane target), carry the owner
        // capability so the node can register this signer on its first
        // on-chain redemption, and attach the client identity binding (ties the
        // request to the lane's signer for reactive origin fill). The fixture's
        // one key owns the pool and signs its lanes.
        let bind_domain = capacity_bond_domain(chain.chain_id(), chain.addrs().capacity_bond);
        let own_node_id = B256::from(*self.endpoint.id().as_bytes());
        let ctx = self_owned_lane_ctx(
            &opened.state,
            &OwnedPayer::sole(Arc::clone(&self.signer)),
            &deployment.voucher_domain(),
            node.operator_addr(),
            U256::ZERO,
            U256::ZERO,
        )
        .context("sign self capability")?
        .with_client_binding(sign_client_binding(
            &self.signer,
            own_node_id,
            &bind_domain,
        )?);

        let stream_domain = capacity_bond_domain(chain.chain_id(), chain.addrs().capacity_bond);
        // Pinned into the session at open time, at the identity the daemon is
        // serving under now. A session deliberately keeps that pin: its pool and
        // lane watermark belong to this connection, so it must not silently follow
        // the node onto a new identity mid-life.
        let target = Self::target(node).await?;

        Ok(PoolSession {
            ctx,
            target,
            stream_domain,
            operator_addr: node.operator_addr(),
        })
    }

    /// Open `hash` on `node`, at the identity it serves under now, with a
    /// voucher-less discovery open ([`decdn_client::discovery_open`]) that asks
    /// no pull-through, and return the stream with its verified answer.
    ///
    /// The open names a placeholder pool under this client's key: a node answers
    /// an open that does no fill work before it reads the pool, so the answer
    /// needs no funded pool. The node holds the stream, and pins the blob, until
    /// the caller cancels or drops it.
    pub async fn discovery_open(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        hash: Hash,
    ) -> anyhow::Result<OpenedStream> {
        self.discovery_open_to(chain, node, Self::target(node).await?, hash, DISCOVERY_OPEN)
            .await
    }

    /// [`Self::discovery_open`] to an **explicit** iroh identity at `node`'s
    /// loopback port, rather than whichever id the daemon serves under now
    /// (#1034).
    ///
    /// The key-rotation journey needs both directions of this: the retired id
    /// must stop answering, and the new one must start. A failure here is a
    /// genuine "that identity is not reachable at this socket": iroh
    /// authenticates the peer's public key during the QUIC handshake, so dialing
    /// a retired id against the same address does not silently connect to
    /// whoever is listening — it fails to establish.
    pub async fn discovery_open_at(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        node_id: iroh::PublicKey,
        hash: Hash,
    ) -> anyhow::Result<OpenedStream> {
        let target = EndpointAddr::new(node_id).with_ip_addr(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            node.bind_port(),
        )));
        // Shorter than `discovery_open`'s bound on purpose: the interesting call
        // is the one that must NOT connect, and a retired id fails by timing out
        // on a handshake nobody answers.
        self.discovery_open_to(chain, node, target, hash, Duration::from_secs(5))
            .await
    }

    /// The discovery open behind [`Self::discovery_open`] and
    /// [`Self::discovery_open_at`]: `open` bounds the dial, and again the
    /// exchange.
    async fn discovery_open_to(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        target: EndpointAddr,
        hash: Hash,
        open: Duration,
    ) -> anyhow::Result<OpenedStream> {
        let placeholder =
            decdn_client::funding::predicted_pool_id(self.signer.address(), U256::ZERO);
        let ctx = self.pool_context(chain, node, placeholder)?;
        let stream_domain = capacity_bond_domain(chain.chain_id(), chain.addrs().capacity_bond);
        decdn_client::discovery_open(
            &Connections::new(self.endpoint.clone()),
            &target,
            node.operator_addr(),
            &ctx,
            &stream_domain,
            *hash.as_bytes(),
            decdn_protocol::client::NO_NAMESPACE,
            (0, 0),
            false,
            open,
        )
        .await
        .context("discovery open")
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
    /// The extra admin round trip is paid once per pool open or discovery open,
    /// all of which already require a live daemon.
    async fn target(node: &NodeFixture) -> anyhow::Result<EndpointAddr> {
        Ok(
            EndpointAddr::new(node.current_node_id().await?).with_ip_addr(SocketAddr::V4(
                SocketAddrV4::new(Ipv4Addr::LOCALHOST, node.bind_port()),
            )),
        )
    }

    /// The `PaymentPool` deployment this client buys on: `chain`'s id and the
    /// client's configured `PaymentPool`.
    const fn deployment(&self, chain: &ChainFixture) -> Deployment {
        Deployment {
            chain_id: chain.chain_id(),
            payment_pool: self.payment_pool,
        }
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
        let deployment = self.deployment(chain);
        let voucher_dom = deployment.voucher_domain();
        let state = BuyerPoolState::new(
            pool_id,
            deployment,
            self.signer.address(),
            chain.usdc(),
            U256::from(DEPOSIT_MICRO_USDC),
        );
        // Uncapped, matching production self-issue. The fixture's one key owns
        // the pool and signs its lanes (`OwnedPayer::sole`, unlike the CLI's
        // derived lane key), so the pool deposit — not the capability cap — is
        // the real spending bound.
        let capability = decdn_client::buyer_pool::issue_self_capability(
            &OwnedPayer::sole(Arc::clone(&self.signer)),
            pool_id,
            decdn_client::buyer_pool::SELF_CAPABILITY_CAP,
            u64::MAX,
            &voucher_dom,
        )
        .context("sign self capability")?;
        let bind_domain = capacity_bond_domain(chain.chain_id(), chain.addrs().capacity_bond);
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

/// Classify a `stream_fetch_tracked` error: `true` if it is the transient
/// serve-path-catch-up condition the retry loop is meant to wait out, `false` if
/// re-running with the same pool would only spin to the deadline.
///
/// The loop exists to ride out the node's readiness window, during which it
/// refuses delivery up front — its `getPool` view has not resolved the pool yet,
/// which reaches us as the wire `NotFound` that `ServeRejectReason::wire_error`
/// collapses every not-now reject reason onto. That, transport errors, a per-attempt
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
/// received-byte size-cap abort (`BlobTooLarge`), any other mid-stream voucher
/// rejection (`UpstreamVoucherRejected` — e.g. an `AmountRegression` left by
/// one-sided ack loss, deposit exhaustion, or a `SpendingCapExhausted`), or a refusal by
/// which the node reports itself degraded / the blob over its own ceiling.
/// Retrying cannot fix any of those, so we fail fast and surface the real cause.
///
/// The refusal arm is only this precise because the wire code now survives as a
/// typed `UpstreamRefused` (#1144).
fn is_retryable(err: &anyhow::Error) -> bool {
    if err.downcast_ref::<HashMismatch>().is_some() || err.downcast_ref::<BlobTooLarge>().is_some()
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
        // Only `NotFound` is transient: it is the readiness window this loop
        // exists for, and a node that may serve on a later attempt. `Declined`
        // and `Unfunded` say the same thing on every attempt of this fetch, so
        // fail fast and surface the real cause.
        return matches!(refused.error(), StreamError::NotFound);
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
