//! What a pull costs before it pays a byte (#2600).
//!
//! Every node signs its per-MiB rate into its answer to each `cdn/client/v1`
//! open, so the price of a pull is known from one round of voucher-less opens
//! ([`crate::discovery_open`]), each cancelled once it has answered. [`census`]
//! opens every node a pull can draw from for each hash and keeps every
//! verified answer. [`HashCensus::serve_order`] orders those answers the way
//! the pull orders its holders, and [`price`] turns the rates into a low, high
//! and likely cost.
//!
//! The reference node fixes its rate at startup, so one rate holds for every
//! hash it serves. The rates a sample of a bundle's blobs gets therefore stand
//! in for the rest of the bundle ([`CENSUS_ENTRY_SAMPLE`]).

use std::collections::HashMap;

use alloy::primitives::{Address, U256};
use decdn_incentive::capacity_bond_domain;
use decdn_incentive::rate::min_payment;
use futures_util::StreamExt as _;
use iroh::{EndpointAddr, PublicKey, RelayUrl};

use crate::discovery::{self, NodeCandidate};
use crate::session::{ChainConfig, DiscoveryOptions, ProxyWarmingParams, PullSite};
use crate::targets::{
    Dropouts, OpenOutcome, OpenSpec, candidate_target, classify_open, failover_order,
    no_serve_target_error, open_at, pinned_target, select_up_to, spawn_harvest,
};
use crate::{Connections, PoolContext};

/// How many of a bundle's distinct blobs a quote asks about: the largest ones
/// by planned download. Each sampled blob costs every node one open, one
/// signed answer and one size proof, and pins the blob on the node until the
/// quote cancels the stream. Rates are per node, so more blobs add that cost
/// and no rate the sample does not already carry; four of the largest also
/// carry most of the bytes the estimate prices.
pub const CENSUS_ENTRY_SAMPLE: usize = 4;

/// How many opens of a census are in flight at once, across every node: the
/// dials, streams and answers this machine handles at a time. What one node
/// sees is bounded per connection beside it (a [`Connections`] map holds at
/// most eight discovery opens on one node), so a census of many hashes waits
/// for its own opens rather than having a node reset them.
const CENSUS_CONCURRENCY: usize = 32;

/// One node's verified answer to a census open.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "test-util"), non_exhaustive)]
pub struct NodeAnswer {
    /// The node that answered.
    pub candidate: NodeCandidate,
    /// The path RTT to the node, in milliseconds, measured after its answer.
    pub rtt_ms: f64,
    /// The rate the node signed, in micro-USDC per MiB.
    pub rate_per_mb: u64,
    /// Whether the node holds the blob: it served the open, or refused it
    /// with the blocks it holds of a blob it holds in part. A node that does
    /// not can still serve the blob through its own origin.
    pub has_blob: bool,
    /// The blob size the node proved in its answer; `None` from a non-holder
    /// and from a partial holder, whose refusal proves no size.
    pub total_bytes: Option<crate::ProvenSize>,
    /// The discovery blocks the node holds. Unsigned; checked against the
    /// signed answer before the census keeps it, and empty exactly when
    /// `has_blob` is false, except for the empty blob, which has no block.
    pub coverage: decdn_protocol::Coverage,
    /// The `PaymentPool` contracts the node accepts, from its answer's
    /// `payment_pools` hint; `None` when it named none (ADR 005 § Payment
    /// pool routing). Unsigned, like `coverage`.
    pub payment_pools: Option<Vec<decdn_protocol::PoolAccount>>,
}

/// Every answer one hash got in a [`census`], and how many opens got none.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "test-util"), non_exhaustive)]
pub struct HashCensus {
    /// The blob the census asked about.
    pub hash: [u8; 32],
    /// The verified answers, holders and non-holders, in answer order.
    pub answers: Vec<NodeAnswer>,
    /// Opens that got no answer.
    pub unreachable: usize,
    /// Opens the node shed.
    pub rate_limited: usize,
    /// Opens the node refused for clock skew: its clock and this machine's
    /// differ by more than 30 seconds.
    pub clock_skewed: usize,
    /// Answers that did not verify: a signature that did not recover to the
    /// node's operator, a size with no valid size proof, or a coverage that
    /// disagrees with the signed answer.
    pub unverifiable: usize,
    /// Verified answers the census cannot use: the node declined the blob, or
    /// refused the open for a reason that says nothing of the blob.
    pub unusable: usize,
}

impl HashCensus {
    /// How many answering nodes hold the blob.
    #[must_use]
    pub fn holders(&self) -> usize {
        self.answers.iter().filter(|a| a.has_blob).count()
    }

    /// The proven blob size from the nearest holder that stated one. Only the
    /// true size proves, so every holder that states a size states this one.
    #[must_use]
    pub fn size_hint(&self) -> Option<crate::ProvenSize> {
        self.answers
            .iter()
            .filter(|a| a.has_blob && a.total_bytes.is_some())
            .min_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms))
            .and_then(|a| a.total_bytes)
    }

    /// The answers a pull that opened every node would draw from, in the
    /// order it tries them: proxy-warming non-holders first when warming
    /// engages, then the holders nearest RTT first, or the non-holders
    /// nearest first as pull-through sources when no node holds the blob.
    /// Only the first node of each operator is kept, as the pull keeps one
    /// lane per operator. A kept node above `ceiling` (micro-USDC per MiB;
    /// `0` sets none) is then left out, as the pull refuses its stream at
    /// open; the operator's other nodes do not take its place.
    #[must_use]
    pub fn serve_order(&self, ceiling: u64, warming: ProxyWarmingParams) -> Vec<&NodeAnswer> {
        let mut holders = Vec::new();
        let mut non_holders = Vec::new();
        for answer in &self.answers {
            if answer.has_blob {
                holders.push(discovery::DiscoveredHolder {
                    candidate: answer.candidate.clone(),
                    rtt_ms: answer.rtt_ms,
                    rate_per_mb: answer.rate_per_mb,
                    total_bytes: answer.total_bytes,
                    coverage: answer.coverage.clone(),
                    payment_pools: answer.payment_pools.clone(),
                });
            } else {
                non_holders.push(discovery::WarmingCandidate {
                    node_id: answer.candidate.node_id,
                    eth_address: answer.candidate.eth_address,
                    rtt_ms: answer.rtt_ms,
                    multiaddrs: answer.candidate.multiaddrs.clone(),
                    payment_pools: answer.payment_pools.clone(),
                });
            }
        }
        let by_node: HashMap<PublicKey, &NodeAnswer> = self
            .answers
            .iter()
            .map(|a| (a.candidate.node_id, a))
            .collect();
        let order = failover_order(holders, &non_holders, warming).order;
        let all = order.len();
        discovery::admit_sources(order, all)
            .iter()
            .filter_map(|c| by_node.get(&c.node_id).copied())
            .filter(|a| within_ceiling(a.rate_per_mb, ceiling))
            .collect()
    }
}

/// Whether `rate` is at or below `ceiling`, where a `0` ceiling sets none.
const fn within_ceiling(rate: u64, ceiling: u64) -> bool {
    ceiling == 0 || rate <= ceiling
}

/// What a [`census`] learned: every answer for each hash it asked about.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "test-util"), non_exhaustive)]
pub struct Census {
    /// How many nodes it opened for each hash.
    pub opened: usize,
    /// One entry per hash, in the order given.
    pub hashes: Vec<HashCensus>,
    /// The node list is the registry-outage fallback: the peer store's
    /// last-known nodes, because the registry read failed.
    pub outage_fallback: bool,
}

/// The nodes a [`census`] opens: every node a pull can draw from, with each
/// one's dial target.
#[derive(Debug, Clone)]
pub struct CensusNodes {
    /// Each node, with its dial target.
    nodes: Vec<(NodeCandidate, EndpointAddr)>,
    /// The list is the registry-outage fallback.
    outage_fallback: bool,
}

impl CensusNodes {
    /// How many nodes the census opens for each hash.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the census has no node to open.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// The nodes a pull of `discovery` can draw from, for a [`census`]: a bundle
/// pull's list ([`crate::bundle::select_nodes`]), the peer store's
/// identity-fresh records, else one `CapacityBond` read. The site's region
/// allowlist narrows the list, with a single fetch's widening rule, unless it
/// is the registry-outage fallback; the list is not sampled down. A pinned
/// node is the list alone. Resolve the nodes before you unlock a key, so a
/// failed discovery costs no password prompt.
///
/// # Errors
///
/// Discovery fails ([`crate::bundle::select_nodes`]).
pub async fn census_nodes(
    (chain, site): (&ChainConfig, &PullSite),
    relays: &[RelayUrl],
    discovery: &DiscoveryOptions,
) -> anyhow::Result<CensusNodes> {
    let relay = relays.first().cloned();
    if let Some(pinned) = discovery.pinned {
        return Ok(CensusNodes {
            nodes: vec![pinned_target(pinned, relay)],
            outage_fallback: false,
        });
    }
    let (registry, outage_fallback) = crate::bundle::registry_nodes(chain, site, discovery).await?;
    let allow: &[decdn_protocol::Region] = if outage_fallback {
        &[]
    } else {
        site.region_allowlist.as_slice()
    };
    let selected = select_up_to(
        registry,
        site.region.as_deref(),
        allow,
        &crate::StoreConfig::default(),
        usize::MAX,
    );
    let nodes = selected
        .into_iter()
        .map(|c| {
            let target = candidate_target(&c, relay.clone());
            (c, target)
        })
        .collect();
    Ok(CensusNodes {
        nodes,
        outage_fallback,
    })
}

/// Open each of `nodes` for each of `hashes`, and keep every verified answer.
///
/// Each open is a voucher-less `cdn/client/v1` open of the whole blob that
/// asks no pull-through ([`crate::discovery_open`]), on `conns`, and names
/// `ctx`'s pool, signer, binding and capability like a pull's discovery: a
/// holder answers its signed quote, its proven size and its coverage, and a
/// node that misses answers a signed `NotFound` at once. The census cancels
/// every stream once it has answered, so it pays nothing. `namespace_id` is
/// the routing hint each open carries. Every answer is verified against its
/// operator's address under `chain`'s `CapacityBond` domain before it counts
/// (ADR 005 § Signer binding), and every verified answer is recorded in `site`'s peer store, so a
/// pull that follows ranks on fresh RTTs.
///
/// # Errors
///
/// `hashes` is not empty and no node answered any open; the error names how
/// the opens failed.
pub async fn census(
    nodes: &CensusNodes,
    (conns, ctx): (&Connections, &PoolContext),
    (chain, site): (&ChainConfig, &PullSite),
    hashes: &[[u8; 32]],
    namespace_id: [u8; 32],
) -> anyhow::Result<Census> {
    let CensusNodes {
        nodes,
        outage_fallback,
    } = nodes;
    let outage_fallback = *outage_fallback;
    let slash_dom = capacity_bond_domain(chain.governance.chain_id, chain.governance.capacity_bond);
    // The whole blob, with no rate ceiling: the census keeps every answer,
    // and `serve_order` applies the ceiling the pull would.
    let spec = OpenSpec {
        conns,
        ctx,
        namespace_id,
        span: (0, 0),
        max_rate_per_mb: 0,
    };
    let opens = hashes.iter().enumerate().flat_map(|(at, &hash)| {
        nodes
            .iter()
            .map(move |(cand, target)| (at, hash, cand, target))
    });
    let outcomes: Vec<(usize, NodeCandidate, OpenOutcome)> = futures_util::stream::iter(opens)
        .map(|(at, hash, cand, target)| {
            let slash_dom = &slash_dom;
            async move {
                // Each open stamps its own timestamp once it holds its
                // node's permit: a node refuses one more than 30 s from its
                // clock, and a census can run longer. The open bounds its own
                // dial and exchange.
                let res = open_at(target, cand, spec, slash_dom, hash, false).await;
                let (outcome, stream) = classify_open(cand, res, 0);
                if let Some(stream) = stream {
                    stream.cancel();
                }
                (at, cand.clone(), outcome)
            }
        })
        .buffer_unordered(CENSUS_CONCURRENCY)
        .collect()
        .await;
    let census = tally(nodes.len(), hashes, outcomes, outage_fallback);
    // Awaited, not detached: the command exits next, and the pull that
    // follows it ranks on these samples.
    if let Err(e) = spawn_harvest(&site.data_dir, Vec::new(), census.samples()).await {
        tracing::warn!("could not record the quote's answer samples in the peer store: {e}");
    }
    match unanswered(&census) {
        Some(err) => Err(err),
        None => Ok(census),
    }
}

/// The error for a census of at least one hash in which no node answered any
/// open, or `None`. The counts are the first hash's, one per node: every
/// hash went unanswered, and a configuration fault fails them all alike.
pub(crate) fn unanswered(census: &Census) -> Option<anyhow::Error> {
    let first = census.hashes.first()?;
    if census.hashes.iter().any(|h| !h.answers.is_empty()) {
        return None;
    }
    Some(no_serve_target_error(Dropouts {
        asked: census.opened,
        unreachable: first.unreachable,
        rate_limited: first.rate_limited,
        unverifiable: first.unverifiable,
        clock_skewed: first.clock_skewed,
        declined: 0,
        over_ceiling: 0,
    }))
}

/// Sort `outcomes`, each tagged with the index of its hash in `hashes`, into
/// one [`HashCensus`] per hash. `opened` is the number of nodes opened for
/// each hash.
pub(crate) fn tally(
    opened: usize,
    hashes: &[[u8; 32]],
    outcomes: Vec<(usize, NodeCandidate, OpenOutcome)>,
    outage_fallback: bool,
) -> Census {
    let mut per_hash: Vec<HashCensus> = hashes
        .iter()
        .map(|&hash| HashCensus {
            hash,
            answers: Vec::new(),
            unreachable: 0,
            rate_limited: 0,
            clock_skewed: 0,
            unverifiable: 0,
            unusable: 0,
        })
        .collect();
    for (at, candidate, outcome) in outcomes {
        let Some(entry) = per_hash.get_mut(at) else {
            continue;
        };
        match outcome {
            OpenOutcome::Holder(holder, (_, _, rate_per_mb)) => entry.answers.push(NodeAnswer {
                candidate,
                rtt_ms: holder.rtt_ms,
                rate_per_mb,
                has_blob: true,
                total_bytes: holder.total_bytes,
                coverage: holder.coverage,
                payment_pools: holder.payment_pools,
            }),
            OpenOutcome::NonHolder(warming, (_, _, rate_per_mb)) => {
                entry.answers.push(NodeAnswer {
                    candidate,
                    rtt_ms: warming.rtt_ms,
                    rate_per_mb,
                    has_blob: false,
                    total_bytes: None,
                    coverage: decdn_protocol::Coverage::empty(),
                    payment_pools: warming.payment_pools,
                });
            }
            OpenOutcome::Unreachable => entry.unreachable += 1,
            OpenOutcome::RateLimited => entry.rate_limited += 1,
            OpenOutcome::ClockSkewed => entry.clock_skewed += 1,
            OpenOutcome::Unverifiable => entry.unverifiable += 1,
            OpenOutcome::Unusable | OpenOutcome::Declined | OpenOutcome::OverCeiling => {
                entry.unusable += 1;
            }
        }
    }
    Census {
        opened,
        hashes: per_hash,
        outage_fallback,
    }
}

impl Census {
    /// One peer-store sample per answering node, `(node_id, rtt_ms,
    /// rate_per_mb)`, with the lowest RTT the node answered at.
    fn samples(&self) -> Vec<(PublicKey, f64, u64)> {
        let mut best = HashMap::new();
        keep_nearest(
            &mut best,
            self.hashes
                .iter()
                .flat_map(|h| &h.answers)
                .map(|a| (a.candidate.node_id, a.rtt_ms, a.rate_per_mb)),
        );
        best.into_iter()
            .map(|(node_id, (rtt, rate))| (node_id, rtt, rate))
            .collect()
    }
}

/// Fold `(node_id, rtt_ms, rate_per_mb)` samples into `best`, keeping each
/// node's sample at the lowest RTT it answered at.
pub(crate) fn keep_nearest(
    best: &mut HashMap<PublicKey, (f64, u64)>,
    samples: impl IntoIterator<Item = (PublicKey, f64, u64)>,
) {
    for (node_id, rtt_ms, rate) in samples {
        let slot = best.entry(node_id).or_insert((rtt_ms, rate));
        if rtt_ms < slot.0 {
            *slot = (rtt_ms, rate);
        }
    }
}

/// The bytes a pull of `content` bytes pays for: the content and its
/// interleaved bao proof, which nodes meter on the wire (ADR 038 § Payment
/// metering).
///
/// # Errors
///
/// Never for a whole blob; the range check is the bao-range helper's.
pub fn wire_bytes(content: u64) -> anyhow::Result<u64> {
    Ok(decdn_bao_range::align_range_clamped(0, content, content)?.wire_len())
}

/// A pull's cost in micro-USDC: on the cheapest node, on the most expensive, and
/// spread over the nodes the pull would draw from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PriceRange {
    /// Every byte from the cheapest node.
    pub low: U256,
    /// Every byte from the most expensive node.
    pub high: U256,
    /// The bytes split evenly over the first `max_sources` nodes of the
    /// order, as a pull splits its first segments; tail-stealing then moves
    /// bytes toward the faster nodes. Between `low` and `high`.
    pub likely: U256,
}

/// One blob to price: its wire bytes and the rates of the nodes a pull
/// would draw from, in the order it tries them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricedBlob {
    /// The blob's wire bytes ([`wire_bytes`]).
    pub wire: u64,
    /// The rates, in micro-USDC per MiB, in serve order.
    pub rates: Vec<u64>,
}

/// The cost of pulling `blobs`, plus `unsampled_wire` more wire bytes
/// served at the same rates. A blob with no rate counts as unsampled bytes.
/// `low` and `high` price every byte at the lowest and highest rate any
/// blob saw. `likely` sums each rated blob's even split over its first
/// `max_sources` rates (at least one), and prices the unsampled bytes at the
/// rated blobs' mean likely rate; it never exceeds `high`. `None` when no
/// blob has a rate.
#[must_use]
pub fn price(blobs: &[PricedBlob], unsampled_wire: u64, max_sources: usize) -> Option<PriceRange> {
    let rates = || blobs.iter().flat_map(|b| b.rates.iter().copied());
    let lowest = rates().min()?;
    let highest = rates().max()?;
    let lanes = max_sources.max(1);
    let mut sampled_wire = 0u64;
    let mut unsampled_wire = unsampled_wire;
    let mut likely = U256::ZERO;
    for blob in blobs {
        if blob.rates.is_empty() {
            unsampled_wire = unsampled_wire.saturating_add(blob.wire);
            continue;
        }
        sampled_wire = sampled_wire.saturating_add(blob.wire);
        likely = likely.saturating_add(split_cost(blob.wire, &blob.rates, lanes));
    }
    let total_wire = sampled_wire.saturating_add(unsampled_wire);
    if unsampled_wire > 0 && sampled_wire > 0 {
        let extra = likely
            .saturating_mul(U256::from(unsampled_wire))
            .div_ceil(U256::from(sampled_wire));
        likely = likely.saturating_add(extra);
    } else if unsampled_wire > 0 {
        likely = likely.saturating_add(min_payment(unsampled_wire, lowest));
    }
    let high = min_payment(total_wire, highest);
    // Each lane's share rounds up on its own, so the sum can pass `high` by
    // a few micro-USDC.
    Some(PriceRange {
        low: min_payment(total_wire, lowest),
        high,
        likely: likely.min(high),
    })
}

/// `wire` bytes split evenly over the first `lanes` of `rates`, the
/// remainder on the first, each share priced as a lane pays it.
fn split_cost(wire: u64, rates: &[u64], lanes: usize) -> U256 {
    let used = rates.len().min(lanes);
    let n = u64::try_from(used).unwrap_or(u64::MAX).max(1);
    let share = wire / n;
    let remainder = wire % n;
    rates
        .iter()
        .take(lanes)
        .enumerate()
        .map(|(at, &rate)| {
            let bytes = if at == 0 { share + remainder } else { share };
            min_payment(bytes, rate)
        })
        .fold(U256::ZERO, U256::saturating_add)
}

/// One node's rate as a pull already knows it: from its own discovery round,
/// or from the peer store. What `decdn get` prices its one-line estimate from.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateHint {
    /// The node operator's provider address.
    pub provider: Address,
    /// The rate the node last signed, in micro-USDC per MiB.
    pub rate_per_mb: u64,
}

/// A node's `(rtt_ms, rate_per_mb)` as the peer store last recorded them,
/// with an unknown latency as infinite, or `None` when the store knows no
/// rate for it or its last failure still suppresses it.
pub(crate) fn store_rate(
    store: &crate::PeerStore,
    node_id: &PublicKey,
    now_secs: u64,
) -> Option<(f64, u64)> {
    let record = store.get(node_id)?;
    if record.failure_suppressed(now_secs, &crate::StoreConfig::default()) {
        return None;
    }
    Some((
        record.latency_ms.unwrap_or(f64::INFINITY),
        record.rate_per_mb?,
    ))
}

/// The rates of `hints`, in the order given, keeping the first hint of each
/// operator and then leaving out those above `ceiling` (`0` sets none): the
/// [`PricedBlob::rates`] of a pull that draws from them in that order, as
/// [`HashCensus::serve_order`] ranks them.
#[doc(hidden)]
#[must_use]
pub fn hint_rates(hints: &[RateHint], ceiling: u64) -> Vec<u64> {
    let mut seen = std::collections::HashSet::new();
    hints
        .iter()
        .filter(|h| seen.insert(h.provider))
        .filter(|h| within_ceiling(h.rate_per_mb, ceiling))
        .map(|h| h.rate_per_mb)
        .collect()
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
