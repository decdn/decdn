//! What a paid pull runs under: the deployment it pays on, where it keeps its
//! local state, how it funds its pool, and how it finds the holders.
//!
//! Each value is resolved once by the caller and read by every lane the pull
//! builds. Nothing here touches the network.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use alloy::primitives::{Address, U256};
use decdn_incentive::Deployment;
use iroh::PublicKey;

/// The governance chain a pull reads the node registry from, and whose id and
/// contracts bind every signature that is not a payment (ADR 016 § Deployment
/// Tiers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernanceChain {
    /// The JSON-RPC endpoint of the governance chain: every registry read
    /// goes through it.
    pub rpc_url: String,
    /// The governance chain id. The `SlashJudge` domain (probe answers and
    /// stream responses) and the `CapacityBond` domain (the client identity
    /// binding) are built for it.
    pub chain_id: u64,
    /// The `SlashJudge` whose domain every probe answer and stream response
    /// verifies against.
    pub slash_judge: Address,
    /// The `CapacityBond` registry discovery reads, and the domain of the
    /// client identity binding. `None` disables both: the pull needs a pinned
    /// node, and a node serves it only content it already holds.
    pub capacity_bond: Option<Address>,
}

/// One `PaymentPool` contract a pull can pay through, and the RPC endpoint of
/// its payment chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentPoolChain {
    /// The JSON-RPC endpoint of the payment chain: every pool read and
    /// transaction on this contract goes through it.
    pub rpc_url: String,
    /// The payment chain id and the `PaymentPool` contract. The voucher and
    /// capability domains are built for it, and a buyer row is reused only on
    /// it.
    pub deployment: Deployment,
}

/// The chains a pull runs on: the governance chain, and the `PaymentPool`
/// contracts it can pay through, in the order the configuration lists them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainConfig {
    /// The governance chain: the registry and every non-payment domain.
    pub governance: GovernanceChain,
    /// The `PaymentPool` contracts the pull can pay through, first configured
    /// first. A fetch pays each node through one of them
    /// ([`crate::pool_route::PoolRoute`]).
    pub payment_pools: Vec<PaymentPoolChain>,
}

impl ChainConfig {
    /// The chains a pull runs on, with at least one payment pool and no pool
    /// listed twice.
    ///
    /// # Errors
    ///
    /// `payment_pools` is empty, or lists one deployment twice.
    pub fn new(
        governance: GovernanceChain,
        payment_pools: Vec<PaymentPoolChain>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !payment_pools.is_empty(),
            "no payment pool configured: set at least one [[blockchain.payment_chains]] entry"
        );
        for (i, pool) in payment_pools.iter().enumerate() {
            anyhow::ensure!(
                !payment_pools
                    .iter()
                    .skip(i.saturating_add(1))
                    .any(|other| other.deployment == pool.deployment),
                "payment pool {} is configured twice",
                pool.deployment.pool_account()
            );
        }
        Ok(Self {
            governance,
            payment_pools,
        })
    }

    /// The first configured payment pool: the pool a node with no
    /// `payment_pools` hint is paid through, and the one a delegated
    /// capability draws from.
    ///
    /// # Errors
    ///
    /// No payment pool is configured.
    pub fn first_pool(&self) -> anyhow::Result<&PaymentPoolChain> {
        self.payment_pools
            .first()
            .ok_or_else(|| anyhow::anyhow!("no payment pool configured"))
    }

    /// The configured payment pool on `deployment`, if any.
    #[must_use]
    pub fn pool(&self, deployment: Deployment) -> Option<&PaymentPoolChain> {
        self.payment_pools
            .iter()
            .find(|pool| pool.deployment == deployment)
    }

    /// Every configured deployment, first configured first.
    #[must_use]
    pub fn deployments(&self) -> Vec<Deployment> {
        self.payment_pools
            .iter()
            .map(|pool| pool.deployment)
            .collect()
    }
}

/// Where a pull keeps its local state, and where its client sits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullSite {
    /// The directory of the peer store, and of the scratch files a stream
    /// keeps while it runs.
    pub data_dir: PathBuf,
    /// The client's region, for region-first discovery ordering. `None` skips
    /// the ordering.
    pub region: Option<String>,
    /// The regions a fetch's discovery may probe. Empty allows every region.
    /// It narrows the candidates and never ranks them, and widens back to
    /// every region when too few candidates remain. A bundle pull applies it
    /// to each entry's sample of the run's registry list, with the same
    /// widening. A list the peer store supplied in place of a failed registry
    /// read is not narrowed.
    pub region_allowlist: Vec<decdn_protocol::Region>,
}

/// Whether a buy may adopt a live pool the wallet already owns on chain when
/// the buyer store has no row for it.
///
/// Adoption assumes the wallet has one buyer: the newest live pool it owns is
/// taken to be this buyer's own. A wallet shared with another buyer breaks
/// that, because adopting the other buyer's pool puts a second, independent
/// voucher series on its lanes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainAdoption {
    /// The wallet's pools are this buyer's own, so a lost row is recovered
    /// from chain rather than escrowing a second deposit.
    Allowed,
    /// The wallet belongs to a `decdn-node` daemon, and its newest live pool
    /// is the one the daemon pays from. The buy opens its own pool instead.
    Refused,
}

/// Whether a buy may open a new pool when it has no open pool to reuse or
/// adopt.
///
/// The gate covers every `openPool` of the run: a lane build's, and a funding
/// recovery step's replacement of the run's pool. A `topUp` of a reused or
/// adopted pool (its low-water refill, or a funding recovery step's) is not an
/// open, so the gate leaves it as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolOpen {
    /// The buy opens a pool at the working deposit when it has none.
    Allowed,
    /// The buy sends no `openPool`. With no open pool to reuse or adopt, a
    /// lane build ends the command with
    /// [`PoolOpenRefused`](crate::PoolOpenRefused), before any allowance or
    /// `openPool`; so does a funding recovery step that would replace the
    /// run's pool. [`PoolOpenRefused::find`](crate::PoolOpenRefused::find)
    /// detects it in a pull's error.
    Refused,
}

/// How a self-owned pull funds its pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepositPolicy {
    /// The deposit a new pool opens at, and the target a reused pool's
    /// low-water refill and a funding recovery step restore toward. Zero for a
    /// delegated signer, which owns no pool it could fund.
    pub working_deposit: U256,
    /// Approve an unlimited standing USDC allowance instead of the exact
    /// amount each deposit escrows.
    pub max_approve: bool,
    /// Whether a buyer store with no row may adopt the wallet's live pool.
    pub adoption: ChainAdoption,
    /// Whether the buy may open a pool when it has none to reuse or adopt.
    pub open: PoolOpen,
}

/// The proxy-warming rule (ADR 037): route through a nearer non-holder when
/// the best holder is distant.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProxyWarmingParams {
    /// Whether the rule engages at all.
    pub enabled: bool,
    /// The best holder's RTT at or above which a proxy is considered, in ms.
    pub rtt_threshold_ms: f64,
    /// How much nearer than the best holder a proxy must be, in ms.
    pub margin_ms: f64,
}

impl Default for ProxyWarmingParams {
    /// The rule on: route through a non-holder at least 30 ms nearer than a
    /// best holder 150 ms or more away.
    fn default() -> Self {
        Self {
            enabled: true,
            rtt_threshold_ms: 150.0,
            margin_ms: 30.0,
        }
    }
}

/// One node every fetch of a pull is pinned to, instead of discovering the
/// holders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinnedNode {
    /// The node's iroh id.
    pub node_id: PublicKey,
    /// The node operator's on-chain provider address, which its lane pays.
    pub provider: Address,
    /// A direct address to dial the node at, beside its discovery records.
    pub addr: Option<SocketAddr>,
}

/// How a pull finds a blob's holders.
#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryOptions {
    /// The one node to fetch from. `None` discovers the holders.
    pub pinned: Option<PinnedNode>,
    /// Read the registry and probe, even when the peer store holds a fresh
    /// candidate set.
    pub rediscover: bool,
    /// How many holders one fetch stripes across at once.
    pub max_sources: usize,
    /// The bound on one registry read. The peer-store fallback still runs
    /// when the read times out.
    pub registry_timeout: Duration,
    /// The proxy-warming rule.
    pub proxy_warming: ProxyWarmingParams,
}

impl Default for DiscoveryOptions {
    /// Discover the holders, from the peer store when it holds a fresh
    /// candidate set; stripe each fetch across up to 4 of them; bound a
    /// registry read by one hour; and warm proxies by the default rule.
    fn default() -> Self {
        Self {
            pinned: None,
            rediscover: false,
            max_sources: 4,
            registry_timeout: Duration::from_hours(1),
            proxy_warming: ProxyWarmingParams::default(),
        }
    }
}

/// The points of a fetch's path to its first byte that the pull reports to its
/// caller, for a time-to-first-byte log. Each is reported every time it is
/// reached; the caller keeps the first.
#[doc(hidden)]
pub trait FetchMarks: Send + Sync {
    /// Discovery starts a probe round.
    fn probe_started(&self);
    /// A lane's pool is open or reused, and its source is ready.
    fn lane_built(&self);
    /// A holder joined the running fetch from a probe that answered late.
    fn holder_joined(&self);
}

#[cfg(test)]
pub(crate) mod test_support;
