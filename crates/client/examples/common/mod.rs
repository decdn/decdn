//! The setup both examples share: a buyer identity and payment pool, an iroh
//! endpoint, the probed holders of one blob, and the paid lanes to them.
//!
//! Every value comes from an environment variable, so the examples read the same
//! way against any deployment. See [`Env::from_env`] for the list.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use decdn_client::buyer_pool::{ProgressWrite, ensure_allowance, open_pool, self_owned_lane_ctx};
use decdn_client::discovery::{self, NodeCandidate};
use decdn_client::source::{Funder, SourceFuture};
use decdn_client::{
    PeerSource, PoolContext, PoolLedger, PullDeadlines, StreamCandidate, VoucherProgress,
    effective_rate_ceiling, endpoint, probe, provider,
};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity::load_signer;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{
    BuyerPoolState, BuyerPoolStore, DepositOutcome, LaneKey, slash_judge_domain, voucher_domain,
};
use decdn_protocol::Coverage;
use iroh::{Endpoint, EndpointAddr};

/// How long one probe round trip may take before the holder is skipped.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// How many registry nodes to probe for the blob.
const PROBE_SAMPLE: usize = 16;

/// One verified holder of the blob: the node, the ranges it holds, and the
/// per-MB rate it signed in its probe answer.
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
    signer: Arc<PrivateKeySigner>,
    store: RedbBuyerPoolStore,
    pool: BuyerPoolState,
    voucher_domain: Eip712Domain,
    slash_domain: Eip712Domain,
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
        let voucher_domain = voucher_domain(env.chain_id, env.payment_pool);
        let slash_domain = slash_judge_domain(env.chain_id, env.slash_judge);

        // The store is the buyer's only record of its pool and of what each lane
        // has paid. Keep it for the life of the pool.
        let store = RedbBuyerPoolStore::open(&env.data_dir)?;
        let pool = if let Some(pool) = store.get_by_owner(owner)? {
            pool
        } else {
            let token = contract.usdc().call().await?;
            ensure_allowance(&rpc, token, owner, env.payment_pool, Some(env.deposit)).await?;
            let opened = open_pool(
                &contract,
                Arc::clone(&signer),
                &voucher_domain,
                token,
                owner,
                env.deposit,
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
            signer,
            store,
            pool,
            voucher_domain,
            slash_domain,
            endpoint,
        })
    }

    /// Find the registered nodes that hold `hash`, and the blob's size.
    ///
    /// Every probe answer is verified before it counts (ADR 014 §1). An answer
    /// whose signature does not verify, or whose `has_blob` and coverage
    /// disagree, is dropped.
    pub(crate) async fn holders(&self, hash: [u8; 32]) -> Result<(Vec<Holder>, u64)> {
        let bootstrap = discovery::bootstrap_nodes(
            &self.env.rpc_url,
            self.env.capacity_bond,
            &self.env.data_dir,
            PROBE_TIMEOUT,
        )
        .await?;
        if let Some(warning) = bootstrap.warning() {
            tracing::warn!("{warning}");
        }
        let sample = discovery::select_candidates(bootstrap.into_peers(), None, PROBE_SAMPLE);

        let mut holders = Vec::new();
        let mut total_bytes = None;
        for candidate in sample {
            let timestamp_us = now_us()?;
            let Ok((resp, ext, _rtt_ms)) = probe::probe_once(
                &self.endpoint,
                dial_target(&candidate),
                hash,
                timestamp_us,
                PROBE_TIMEOUT,
            )
            .await
            else {
                continue;
            };
            let verified = probe::verify_probe_response(
                &resp,
                candidate.eth_address,
                &self.slash_domain,
                hash,
                timestamp_us,
            );
            if verified.is_err() || !ext.consistent_with(resp.body.has_blob) || !resp.body.has_blob
            {
                continue;
            }
            total_bytes = total_bytes.or(ext.total_bytes);
            holders.push(Holder {
                candidate,
                coverage: ext.coverage,
                rate_per_mb: resp.body.rate_per_mb,
            });
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
        // `decdn fetch`'s defaults: a 30 s open and stall budget and a 4 KiB/s
        // throughput floor.
        let deadlines = PullDeadlines::new(Duration::from_secs(30), Duration::from_secs(30), 4096)?;
        let contract = PaymentPool::new(
            self.env.payment_pool,
            provider::build_provider(&self.env.rpc_url, &self.signer)?,
        );

        let mut candidates = Vec::with_capacity(holders.len());
        let mut lanes = Vec::with_capacity(holders.len());
        for holder in holders {
            let provider = holder.candidate.eth_address;
            let lane = LaneKey {
                pool_id: self.pool.pool_id,
                signer: self.signer.address(),
                provider,
            };
            let (prior_bytes, prior_amount) = prior_payment(&self.pool, &contract, lane).await?;
            let ctx = self_owned_lane_ctx(
                &self.pool,
                &self.signer,
                &self.voucher_domain,
                provider,
                prior_bytes,
                prior_amount,
            )?;
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
                // No received-byte ceiling beyond the probed size.
                0,
                // Refuse a stream quote above the rate the node signed in its
                // probe answer. `0` would set no ceiling at all.
                effective_rate_ceiling(holder.rate_per_mb, 0),
                deadlines,
            );
            lanes.push(Lane {
                key: lane,
                prior_amount,
                ledger: Arc::clone(&ledger),
            });
            candidates.push(StreamCandidate {
                source,
                ctx,
                ledger,
                coverage: Some(holder.coverage),
                first_unit: None,
            });
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
            let owner = self.signer.address();
            if let Err(err) = write.apply(&self.store, owner, lane.key.pool_id, lane.key) {
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

/// A [`Funder`] that never tops the pool up: a fetch that runs the deposit dry
/// fails with [`decdn_client::PoolExhausted`]. A caller that wants reactive
/// top-ups implements `top_up` with `decdn_client::buyer_pool::top_up`.
pub(crate) struct NoTopUp;

impl Funder for NoTopUp {
    fn max_topups(&self) -> u32 {
        0
    }

    fn top_up(&self, _additional: U256) -> SourceFuture<'_, DepositOutcome> {
        Box::pin(async { anyhow::bail!("this buyer does not top up its pool") })
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

/// The probe timestamp: microseconds since the Unix epoch. The node echoes it,
/// and the verifier checks the echo.
fn now_us() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros(),
    )?)
}
