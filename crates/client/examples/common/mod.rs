//! The setup both examples share: a buyer identity and payment pool, an iroh
//! endpoint, the holders of one blob that answer a discovery open, and the
//! paid lanes to them.
//!
//! Every value comes from an environment variable, so the examples read the same
//! way against any deployment. See [`Env::from_env`] for the list.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use anyhow::{Context, Result};
use decdn_client::buyer_pool::{
    OwnedPayer, ProgressWrite, WatermarkRow, ensure_allowance, open_pool, self_owned_lane_ctx,
};
use decdn_client::discovery::{self, NodeCandidate};
use decdn_client::lanes::LanePayer;
use decdn_client::session::ChainConfig;
use decdn_client::{
    Connections, Funder, PeerSource, PoolContext, PoolLedger, PullDeadlines, Recovery,
    SourceFuture, StreamCandidate, VoucherProgress, discovery_open, endpoint, provider,
};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity::load_signer;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{
    BuyerPoolState, BuyerPoolStore, Deployment, LaneKey, bind_node_id_domain, slash_judge_domain,
};
use decdn_protocol::Coverage;
use iroh::{Endpoint, EndpointAddr};

/// How long one discovery open may take, the dial and again the exchange,
/// before the node is skipped.
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);
/// How many registry nodes to open for the blob.
const OPEN_SAMPLE: usize = 16;

/// One verified holder of the blob: the node, the ranges it holds, and the
/// per-MB rate it signed in its answer.
pub(crate) struct Holder {
    candidate: NodeCandidate,
    coverage: Coverage,
    rate_per_mb: u64,
}

/// The deployment and the buyer, read from the environment.
pub(crate) struct Env {
    rpc_url: String,
    chain_id: u64,
    payment_pool: Address,
    slash_judge: Address,
    capacity_bond: Address,
    keystore: PathBuf,
    keystore_password: String,
    data_dir: PathBuf,
    deposit: U256,
}

impl Env {
    /// Read `DECDN_RPC_URL`, `DECDN_CHAIN_ID`, `DECDN_PAYMENT_POOL`,
    /// `DECDN_SLASH_JUDGE`, `DECDN_CAPACITY_BOND`, `DECDN_KEYSTORE`,
    /// `DECDN_KEYSTORE_PASSWORD`, `DECDN_DATA_DIR`, and `DECDN_DEPOSIT` (the pool
    /// deposit in USDC base units, used only when no pool exists yet).
    pub(crate) fn from_env() -> Result<Self> {
        fn var(name: &str) -> Result<String> {
            std::env::var(name).with_context(|| format!("set {name}"))
        }
        Ok(Self {
            rpc_url: var("DECDN_RPC_URL")?,
            chain_id: var("DECDN_CHAIN_ID")?.parse()?,
            payment_pool: var("DECDN_PAYMENT_POOL")?.parse()?,
            slash_judge: var("DECDN_SLASH_JUDGE")?.parse()?,
            capacity_bond: var("DECDN_CAPACITY_BOND")?.parse()?,
            keystore: var("DECDN_KEYSTORE")?.into(),
            keystore_password: var("DECDN_KEYSTORE_PASSWORD")?,
            data_dir: var("DECDN_DATA_DIR")?.into(),
            deposit: var("DECDN_DEPOSIT")?.parse()?,
        })
    }
}

/// Everything a paid fetch borrows: the buyer, its pool, and the endpoint.
pub(crate) struct Buyer {
    env: Env,
    /// The pool owner key, and the lane key this data dir signs vouchers with.
    payer: OwnedPayer,
    store: RedbBuyerPoolStore,
    pool: BuyerPoolState,
    voucher_domain: Eip712Domain,
    slash_domain: Eip712Domain,
    bind_domain: Eip712Domain,
    endpoint: Endpoint,
}

impl Buyer {
    /// Load the buyer key, reuse the pool the local store records (or open one
    /// and record it), and bring up an iroh endpoint.
    pub(crate) async fn connect(env: Env) -> Result<Self> {
        let signer = Arc::new(load_signer(&env.keystore, &env.keystore_password)?);
        let owner = signer.address();
        let rpc = provider::build_provider(&env.rpc_url, &signer)?;
        let contract = PaymentPool::new(env.payment_pool, rpc.clone());
        let deployment = Deployment {
            chain_id: env.chain_id,
            payment_pool: env.payment_pool,
        };
        let voucher_domain = deployment.voucher_domain();
        let slash_domain = slash_judge_domain(env.chain_id, env.slash_judge);
        let bind_domain = bind_node_id_domain(env.chain_id, env.capacity_bond);

        // The store is the buyer's only record of its pool and of what each lane
        // has paid. Keep it for the life of the pool.
        let store = RedbBuyerPoolStore::open(&env.data_dir)?;
        // The vouchers sign under a key derived from the owner key and this
        // store's salt, so another data dir on the same wallet signs on its
        // own lanes of the same pool.
        let payer = OwnedPayer::for_store(Arc::clone(&signer), &store)?;
        // A row from another deployment names a pool this contract never saw.
        let reusable = store
            .get_by_owner(owner)?
            .filter(|pool| pool.is_on(deployment));
        let pool = if let Some(pool) = reusable {
            pool
        } else {
            let token = contract.usdc().call().await?;
            ensure_allowance(&rpc, token, owner, env.payment_pool, Some(env.deposit)).await?;
            // With no handle, nothing reads the pending entry the guard holds.
            let (opened, _unrecorded) = open_pool(
                &contract,
                deployment,
                token,
                owner,
                env.deposit,
                None,
                Some(decdn_incentive::tx::RECEIPT_TIMEOUT),
            )
            .await?;
            // The deposit is escrowed once `open_pool` returns. A failed
            // record leaves it untracked, so name the transaction.
            store
                .record(&opened.state)
                .with_context(|| format!("record the pool opened in {}", opened.tx))?;
            opened.state
        };

        let relays = endpoint::resolve_relays(None, None)?;
        let discovery = endpoint::client_discovery(None)?;
        let endpoint = endpoint::client_endpoint(&relays, &discovery).await?;

        Ok(Self {
            env,
            payer,
            store,
            pool,
            voucher_domain,
            slash_domain,
            bind_domain,
            endpoint,
        })
    }

    /// Find the registered nodes that hold `hash`, and the blob's size.
    ///
    /// Each node gets one voucher-less `cdn/client/v1` open of the whole blob
    /// that names the buyer's pool and asks no pull-through: a holder answers
    /// its signed quote, its proven size and its coverage, and a node that
    /// misses answers `NotFound` at once. Every answer is verified before it
    /// counts (ADR 014 §1); one that does not verify is dropped. Each stream
    /// is cancelled once it has answered, so nothing is paid here.
    pub(crate) async fn holders(
        &self,
        hash: [u8; 32],
    ) -> Result<(Vec<Holder>, decdn_client::ProvenSize)> {
        let bootstrap = discovery::bootstrap_nodes(
            &self.env.rpc_url,
            self.env.capacity_bond,
            &self.env.data_dir,
            OPEN_TIMEOUT,
        )
        .await?;
        if let Some(warning) = bootstrap.warning() {
            tracing::warn!("{warning}");
        }
        let sample = discovery::select_candidates(bootstrap.into_peers(), None, OPEN_SAMPLE);

        // The opens carry the buyer's pool, lane key and identity binding like
        // any open; the node admits the payer only at the first voucher.
        let chain = ChainConfig {
            rpc_url: self.env.rpc_url.clone(),
            chain_id: self.env.chain_id,
            payment_pool: self.env.payment_pool,
            slash_judge: self.env.slash_judge,
            capacity_bond: Some(self.env.capacity_bond),
        };
        let contract = PaymentPool::new(
            self.env.payment_pool,
            provider::build_provider(&self.env.rpc_url, self.payer.owner())?,
        );
        let ctx = decdn_client::funding::discovery_ctx(
            &self.store,
            &contract,
            LanePayer::Owned(&self.payer),
            &chain,
            &self.endpoint,
            &self.voucher_domain,
            Some(self.pool.pool_id),
        )
        .await?;
        let conns = Connections::new(self.endpoint.clone());

        let mut holders = Vec::new();
        let mut total_bytes = None;
        for candidate in sample {
            let Ok(opened) = discovery_open(
                &conns,
                &dial_target(&candidate),
                candidate.eth_address,
                &ctx,
                &self.slash_domain,
                hash,
                decdn_protocol::client::NO_NAMESPACE,
                (0, 0),
                false,
                OPEN_TIMEOUT,
            )
            .await
            else {
                continue;
            };
            // A served answer carries a size its proof proved.
            if opened.served() {
                total_bytes = total_bytes.or(opened.proven_size());
                holders.push(Holder {
                    candidate,
                    coverage: opened.coverage().clone(),
                    rate_per_mb: opened.rate_per_mb(),
                });
            }
            opened.cancel();
        }
        let total_bytes = total_bytes.context("no verified holder reported the blob's size")?;
        Ok((holders, total_bytes))
    }

    /// One paid lane per holder: a [`PoolContext`] pinned to that provider, a
    /// voucher ledger resumed from what the lane has already paid, and the
    /// [`PeerSource`] that pulls from it.
    pub(crate) async fn lanes(
        &self,
        holders: Vec<Holder>,
    ) -> Result<(Vec<StreamCandidate<PeerSource<'_>>>, Vec<Lane>)> {
        // `decdn get`'s defaults: a 30 s open and stall budget and a 4 KiB/s
        // throughput floor.
        let deadlines = PullDeadlines::new(Duration::from_secs(30), Duration::from_secs(30), 4096)?;
        let contract = PaymentPool::new(
            self.env.payment_pool,
            provider::build_provider(&self.env.rpc_url, self.payer.owner())?,
        );

        let mut candidates = Vec::with_capacity(holders.len());
        let mut lanes = Vec::with_capacity(holders.len());
        for holder in holders {
            let provider = holder.candidate.eth_address;
            let lane = LaneKey {
                pool_id: self.pool.pool_id,
                signer: self.payer.lane().address(),
                provider,
            };
            let (prior_bytes, prior_amount) = prior_payment(&self.pool, &contract, lane).await?;
            // The binding, signed by the lane key, ties this endpoint to the
            // lane, so a node that misses the blob in its cache may pull it
            // from its origin for us. Without it, such a node refuses the open.
            let ctx = self_owned_lane_ctx(
                &self.pool,
                &self.payer,
                &self.voucher_domain,
                provider,
                prior_bytes,
                prior_amount,
            )?
            .bind_endpoint(&self.endpoint, &self.bind_domain)?;
            let ledger = Arc::new(ctx.new_ledger());
            let ctx: Arc<Mutex<PoolContext>> = Arc::new(Mutex::new(ctx));
            let source = PeerSource::new(
                &self.endpoint,
                dial_target(&holder.candidate),
                Arc::clone(&ctx),
                Arc::clone(&ledger),
                &self.slash_domain,
                provider,
                decdn_protocol::client::NO_NAMESPACE,
                // No received-byte ceiling beyond the proven size.
                0,
                // Refuse a stream quote above the rate the node signed in its
                // discovery answer. `0` would set no ceiling at all.
                holder.rate_per_mb,
                deadlines,
                None,
            );
            lanes.push(Lane {
                key: lane,
                prior_amount,
                ledger: Arc::clone(&ledger),
            });
            let mut candidate = StreamCandidate::new(source, ctx, ledger);
            candidate.coverage = Some(holder.coverage);
            candidates.push(candidate);
        }
        Ok((candidates, lanes))
    }

    /// Record what every lane paid, whatever the fetch's outcome.
    ///
    /// The faces do not persist. A lane that is not recorded resumes from a
    /// stale watermark on the next run, and the provider rejects its vouchers.
    pub(crate) fn record_payments(&self, lanes: &[Lane]) {
        for lane in lanes {
            // Take the rebase anchor before the settlement read, so the
            // settlement is at or above it.
            let anchor = lane.ledger.take_unsaved_rebase();
            let progress =
                VoucherProgress::from_cumulative(lane.ledger.settlement(), lane.prior_amount)
                    .with_rebase_anchor(anchor);
            let Some(write) = ProgressWrite::of(&progress) else {
                continue;
            };
            let row = WatermarkRow::Owned(self.payer.owner().address());
            if let Err(err) = write.apply(&self.store, row, lane.key) {
                tracing::warn!(provider = %lane.key.provider, error = %err, "lane not recorded");
            }
        }
    }
}

/// What one lane needs after the fetch to record its payment.
pub(crate) struct Lane {
    key: LaneKey,
    prior_amount: U256,
    ledger: Arc<PoolLedger>,
}

/// A [`Funder`] that never adds funds: a fetch whose sources all refuse the
/// deposit fails "funding needed" ([`decdn_client::NoAffordableSource`]). A
/// caller that wants funding recovery implements `recover` with
/// `decdn_client::buyer_pool::top_up`.
pub(crate) struct NoFunding;

impl Funder for NoFunding {
    fn recover(&self, _remaining: U256) -> SourceFuture<'_, Recovery> {
        Box::pin(async { Ok(Recovery::Unavailable) })
    }
}

/// The lane's cumulative `(bytes, amount)` already paid: the store's record, or
/// the chain's watermark for a lane the store has never seen.
///
/// Never resume a lane below the chain watermark. A voucher at or below it
/// redeems nothing, so the provider streams bytes it can never cash.
async fn prior_payment<P: Provider + Clone>(
    pool: &BuyerPoolState,
    contract: &PaymentPool::PaymentPoolInstance<P>,
    lane: LaneKey,
) -> Result<(U256, U256)> {
    if let Some(progress) = pool.lane_progress(lane) {
        return Ok((progress.last_bytes, progress.last_amount));
    }
    let onchain = contract
        .getWatermark(lane.pool_id, lane.signer, lane.provider)
        .call()
        .await
        .context("read the lane's on-chain watermark")?;
    Ok((
        U256::from(onchain.bytesDelivered),
        U256::from(onchain.amount),
    ))
}

/// The node's iroh address, with its registry-published direct addresses as
/// dial hints so a reachable node connects without a relay.
fn dial_target(candidate: &NodeCandidate) -> EndpointAddr {
    discovery::with_dial_addrs(EndpointAddr::new(candidate.node_id), candidate)
}
