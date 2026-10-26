//! What a pull costs before it pays a byte (#2600).
//!
//! Every node signs its per-MiB rate into each `cdn/probe/v1` answer, so the
//! price of a pull is known from one unpaid probe round. [`census`] probes
//! every node a pull can draw from for each hash and keeps every verified
//! answer. [`HashCensus::serve_order`] orders those answers the way the pull
//! orders its holders, and [`price`] turns the rates into a low, high and
//! likely cost.
//!
//! The reference node fixes its rate at startup, so one rate holds for every
//! hash it serves. The rates a sample of a bundle's blobs gets therefore stand
//! in for the rest of the bundle ([`CENSUS_ENTRY_SAMPLE`]).

use std::collections::HashMap;

use alloy::primitives::{Address, U256};
use decdn_incentive::rate::min_payment;
use decdn_incentive::slash_judge_domain;
use futures_util::StreamExt as _;
use iroh::{Endpoint, PublicKey, RelayUrl};

use crate::discovery::{self, NodeCandidate};
use crate::session::{ChainConfig, DiscoveryOptions, ProxyWarmingParams, PullSite};
use crate::targets::{
    ProbeOutcome, classify_probe, failover_order, micros_now, no_serve_target_error, pinned_target,
    probe_candidate, probe_target, select_up_to, spawn_harvest,
};

/// How many of a bundle's distinct blobs a quote probes: the largest ones by
/// planned download. A node's default per-peer probe limit is a burst of
/// five (ADR 005 § Probe rate limiting). A quote that first fetches its
/// manifest has already spent one probe per node on the same endpoint, so
/// four keeps the round inside the burst. Rates are per node, so the
/// sample's rates stand in for the rest of the bundle.
pub const CENSUS_ENTRY_SAMPLE: usize = 4;

/// How many probes of a census are in flight at once.
const CENSUS_CONCURRENCY: usize = 32;

/// One node's verified answer to a census probe.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "test-util"), non_exhaustive)]
pub struct NodeAnswer {
    /// The node that answered.
    pub candidate: NodeCandidate,
    /// The probe's round-trip time, in milliseconds.
    pub rtt_ms: f64,
    /// The rate the node signed, in micro-USDC per MiB.
    pub rate_per_mb: u64,
    /// Whether the node holds the blob in its cache. A node that does not
    /// can still serve it through its own origin.
    pub has_blob: bool,
    /// The blob size the node reported. Unsigned, so a hint only; `None`
    /// from a non-holder, or from a holder that reported no size.
    pub total_bytes: Option<u64>,
    /// The byte ranges the node holds. Unsigned; empty exactly when
    /// `has_blob` is false, which the census checks before it keeps an
    /// answer.
    pub coverage: decdn_protocol::Coverage,
}

/// Every answer one hash got in a [`census`], and how many probes got none.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "test-util"), non_exhaustive)]
pub struct HashCensus {
    /// The probed blob.
    pub hash: [u8; 32],
    /// The verified answers, holders and non-holders, in answer order.
    pub answers: Vec<NodeAnswer>,
    /// Probes that got no answer.
    pub unreachable: usize,
    /// Probes the node kept shedding.
    pub rate_limited: usize,
    /// Answers whose signature did not recover to the node's operator.
    pub unverifiable: usize,
    /// Answers whose `has_blob` and coverage disagree.
    pub unusable: usize,
}

impl HashCensus {
    /// How many answering nodes hold the blob.
    #[must_use]
    pub fn holders(&self) -> usize {
        self.answers.iter().filter(|a| a.has_blob).count()
    }

    /// The blob size the nearest holder reported, as a pull takes its first
    /// size claim.
    #[must_use]
    pub fn size_hint(&self) -> Option<u64> {
        self.answers
            .iter()
            .filter(|a| a.has_blob && a.total_bytes.is_some())
            .min_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms))
            .and_then(|a| a.total_bytes)
    }

    /// The answers a pull that probed every node would draw from, in the
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
                holders.push(discovery::Probed {
                    candidate: answer.candidate.clone(),
                    rtt_ms: answer.rtt_ms,
                    total_bytes: answer.total_bytes,
                    coverage: answer.coverage.clone(),
                });
            } else {
                non_holders.push(discovery::WarmingCandidate {
                    node_id: answer.candidate.node_id,
                    eth_address: answer.candidate.eth_address,
                    rtt_ms: answer.rtt_ms,
                    multiaddrs: answer.candidate.multiaddrs.clone(),
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

/// What a [`census`] learned: every answer for each hash it probed.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "test-util"), non_exhaustive)]
pub struct Census {
    /// How many nodes it probed for each hash.
    pub probed: usize,
    /// One entry per probed hash, in the order given.
    pub hashes: Vec<HashCensus>,
    /// The node list is the registry-outage fallback: the peer store's
    /// last-known nodes, because the registry read failed.
    pub outage_fallback: bool,
}

/// Probe every node a pull of `hashes` can draw from, and keep every
/// verified answer.
///
/// The nodes are a bundle pull's list ([`crate::bundle::select_nodes`]):
/// the peer store's identity-fresh records, else one `CapacityBond` read.
/// The site's region allowlist narrows the list, with a single fetch's
/// widening rule, unless it is the registry-outage fallback; the list is not
/// sampled down. A pinned node is probed alone. Every answer is verified against its operator's
/// address before it counts (ADR 014 §1), and every verified answer is
/// recorded in the peer store, so a pull that follows ranks on fresh RTTs.
/// The probes are unpaid.
///
/// # Errors
///
/// Discovery fails ([`crate::bundle::select_nodes`]), or `hashes` is not
/// empty and no node answered any probe; the error names how the probes
/// failed.
pub async fn census(
    endpoint: &Endpoint,
    (chain, site): (&ChainConfig, &PullSite),
    relays: &[RelayUrl],
    discovery: &DiscoveryOptions,
    hashes: &[[u8; 32]],
) -> anyhow::Result<Census> {
    let relay = relays.first().cloned();
    let (nodes, outage_fallback) = if let Some(pinned) = discovery.pinned {
        (vec![pinned_target(pinned, relay)], false)
    } else {
        let (registry, outage_fallback) =
            crate::bundle::registry_nodes(chain, site, discovery).await?;
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
                let target = probe_target(&c, relay.clone());
                (c, target)
            })
            .collect();
        (nodes, outage_fallback)
    };
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
    let timestamp_us = micros_now();
    let probes = hashes.iter().enumerate().flat_map(|(at, &hash)| {
        nodes
            .iter()
            .map(move |(cand, target)| (at, hash, cand, target))
    });
    let outcomes: Vec<(usize, NodeCandidate, ProbeOutcome)> = futures_util::stream::iter(probes)
        .map(|(at, hash, cand, target)| {
            let slash_dom = &slash_dom;
            async move {
                let res = probe_candidate(endpoint, target.clone(), hash, timestamp_us).await;
                let outcome = classify_probe(cand, res, hash, timestamp_us, slash_dom);
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
        tracing::warn!("could not record the quote's probe samples in the peer store: {e}");
    }
    match unanswered(&census) {
        Some(err) => Err(err),
        None => Ok(census),
    }
}

/// The error for a census of at least one hash in which no node answered any
/// probe, or `None`. The counts are the first hash's, one per node: every
/// hash went unanswered, and a configuration fault fails them all alike.
pub(crate) fn unanswered(census: &Census) -> Option<anyhow::Error> {
    let first = census.hashes.first()?;
    if census.hashes.iter().any(|h| !h.answers.is_empty()) {
        return None;
    }
    Some(no_serve_target_error(
        census.probed,
        first.unreachable,
        first.rate_limited,
        first.unverifiable,
    ))
}

/// Sort `outcomes`, each tagged with the index of its hash in `hashes`, into
/// one [`HashCensus`] per hash. `probed` is the number of nodes probed for
/// each hash.
pub(crate) fn tally(
    probed: usize,
    hashes: &[[u8; 32]],
    outcomes: Vec<(usize, NodeCandidate, ProbeOutcome)>,
    outage_fallback: bool,
) -> Census {
    let mut per_hash: Vec<HashCensus> = hashes
        .iter()
        .map(|&hash| HashCensus {
            hash,
            answers: Vec::new(),
            unreachable: 0,
            rate_limited: 0,
            unverifiable: 0,
            unusable: 0,
        })
        .collect();
    for (at, candidate, outcome) in outcomes {
        let Some(entry) = per_hash.get_mut(at) else {
            continue;
        };
        match outcome {
            ProbeOutcome::Holder(probed, (_, _, rate_per_mb)) => entry.answers.push(NodeAnswer {
                candidate,
                rtt_ms: probed.rtt_ms,
                rate_per_mb,
                has_blob: true,
                total_bytes: probed.total_bytes,
                coverage: probed.coverage,
            }),
            ProbeOutcome::NonHolder(warming, (_, _, rate_per_mb)) => {
                entry.answers.push(NodeAnswer {
                    candidate,
                    rtt_ms: warming.rtt_ms,
                    rate_per_mb,
                    has_blob: false,
                    total_bytes: None,
                    coverage: decdn_protocol::Coverage::empty(),
                });
            }
            ProbeOutcome::Unreachable => entry.unreachable += 1,
            ProbeOutcome::RateLimited => entry.rate_limited += 1,
            ProbeOutcome::Unverifiable => entry.unverifiable += 1,
            ProbeOutcome::Unusable => entry.unusable += 1,
        }
    }
    Census {
        probed,
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

/// One node's rate as a pull already knows it: from its own probe round, or
/// from the peer store. What `decdn get` prices its one-line estimate from.
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
