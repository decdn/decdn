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

/// The chain a pull pays on: its RPC endpoint and the deCDN contracts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainConfig {
    /// The JSON-RPC endpoint every chain read and transaction goes through.
    pub rpc_url: String,
    /// The chain id the EIP-712 domains are built for.
    pub chain_id: u64,
    /// The `PaymentPool` the pull's pool lives on.
    pub payment_pool: Address,
    /// The `SlashJudge` whose domain every probe answer and stream response
    /// verifies against.
    pub slash_judge: Address,
    /// The `CapacityBond` registry discovery reads, and the domain of the
    /// client identity binding. `None` disables both: the pull needs a pinned
    /// node, and a node serves it only content it already holds.
    pub capacity_bond: Option<Address>,
}

impl ChainConfig {
    /// The `PaymentPool` deployment this pull runs against: the chain id and
    /// the contract address. A buyer row is reused only on this deployment.
    #[must_use]
    pub const fn deployment(&self) -> Deployment {
        Deployment {
            chain_id: self.chain_id,
            payment_pool: self.payment_pool,
        }
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
    /// The regions discovery may probe. Empty allows every region. It
    /// narrows the candidates and never ranks them.
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
