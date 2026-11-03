//! `OriginAssignment` seat watcher (ADR 022 § Origin Attestation).
//!
//! Fills the node's [`SeatedOriginSet`] — every operator that holds an origin
//! seat in at least one namespace — and keeps it current. A DHT record is
//! valid only when its attestation recovers to an operator in that set.
//!
//! Bootstrap reads `assignedNamespaces` and `getOrigins` for each namespace at
//! one pinned snapshot block, because the contract's swap-and-pop removal
//! reorders the namespace list between blocks. The live tail then follows
//! `OriginAdded`, `OriginRemoved` and `InactiveOriginPruned` from that block on
//! the shared poller, and a periodic authoritative re-read replaces the set,
//! so a missed log heals without a restart.
//!
//! The set fails closed: a boot read that cannot finish leaves it empty, and an
//! empty set makes no record valid.

use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::eips::BlockId;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::eth::Log;
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::origin_assignment::OriginAssignment;
use tracing::{info, warn};

use crate::chain_events::boot_retry::BootRetry;
use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{CursorStart, LogSink, clear_cadence_on_recovery};
use crate::chain_events::shared_head::{HeadSource, snapshot_block};
use crate::chain_events::timed;
use crate::dht::seated_origins::{SeatedOriginSet, SeatedOrigins};
use crate::metrics::Metrics;

/// How often the sink replaces the set from an authoritative re-read.
const RESEED_INTERVAL: Duration = Duration::from_mins(30);

/// Namespaces read per `assignedNamespaces` page.
const PAGE_SIZE: u64 = 256;

/// The chain reads the watcher makes, behind a trait so the sink is testable
/// without a provider.
pub(crate) trait SeatReads: Send + Sync {
    /// The block to pin a full read at.
    fn snapshot_block(&self) -> impl Future<Output = Result<u64>> + Send;

    /// Every `(namespace, operator)` seat at block `at`.
    fn seats_at(&self, at: u64) -> impl Future<Output = Result<Vec<(U256, Address)>>> + Send;
}

struct ContractReads<P: Provider + Clone> {
    origin: OriginAssignment::OriginAssignmentInstance<P>,
    head: Arc<dyn HeadSource>,
}

impl<P: Provider + Clone> SeatReads for ContractReads<P> {
    async fn snapshot_block(&self) -> Result<u64> {
        snapshot_block(self.origin.provider(), &*self.head, *self.origin.address()).await
    }

    async fn seats_at(&self, at: u64) -> Result<Vec<(U256, Address)>> {
        let block = BlockId::number(at);
        let count = timed(
            None,
            "assignedNamespaceCount",
            self.origin.assignedNamespaceCount().block(block).call(),
        )
        .await?;
        let count = u64::try_from(count).context("assignedNamespaceCount exceeds u64")?;
        let mut seats = Vec::new();
        let mut offset = 0u64;
        while offset < count {
            let page = timed(
                None,
                "assignedNamespaces",
                self.origin
                    .assignedNamespaces(U256::from(offset), U256::from(PAGE_SIZE))
                    .block(block)
                    .call(),
            )
            .await
            .with_context(|| format!("assignedNamespaces(offset={offset})"))?;
            if page.is_empty() {
                break;
            }
            offset = offset.saturating_add(page.len() as u64);
            for namespace in page {
                let operators = timed(
                    None,
                    "getOrigins",
                    self.origin.getOrigins(namespace).block(block).call(),
                )
                .await
                .with_context(|| format!("getOrigins(namespace={namespace})"))?;
                seats.extend(operators.into_iter().map(|op| (namespace, op)));
            }
        }
        Ok(seats)
    }
}

/// Applies seat events to the set and re-reads it on a cadence.
struct SeatSink<R: SeatReads> {
    reads: R,
    seated: Arc<SeatedOriginSet>,
    metrics: Arc<Metrics>,
    interval: Duration,
    last_reseed: Option<Instant>,
}

impl<R: SeatReads> SeatSink<R> {
    fn publish(&self) {
        self.metrics.seated_origins(self.seated.len());
    }
}

/// A seat change one log carries: `(seated, namespace, operator)`.
type SeatChange = (bool, U256, Address);

/// Decode the seat change `log` carries, or `None` for a log of another
/// topic. An undecodable seat event is logged and skipped, never retried: a
/// re-scan would decode it no better.
fn seat_change(log: &Log) -> Option<SeatChange> {
    let data = &log.inner.data;
    let decoded = match log.topic0().copied()? {
        t if t == OriginAssignment::OriginAdded::SIGNATURE_HASH => {
            OriginAssignment::OriginAdded::decode_log_data(data)
                .map(|ev| (true, ev.namespaceId, ev.operator))
        }
        t if t == OriginAssignment::OriginRemoved::SIGNATURE_HASH => {
            OriginAssignment::OriginRemoved::decode_log_data(data)
                .map(|ev| (false, ev.namespaceId, ev.operator))
        }
        t if t == OriginAssignment::InactiveOriginPruned::SIGNATURE_HASH => {
            OriginAssignment::InactiveOriginPruned::decode_log_data(data)
                .map(|ev| (false, ev.namespaceId, ev.operator))
        }
        _ => return None,
    };
    decoded
        .inspect_err(|err| warn!(error = %err, "origin seats: undecodable seat event; skipping"))
        .ok()
}

impl<R: SeatReads> LogSink for SeatSink<R> {
    async fn apply(&mut self, log: Log) -> Result<()> {
        match seat_change(&log) {
            Some((true, namespace, operator)) => self.seated.seat(namespace, operator),
            Some((false, namespace, operator)) => self.seated.unseat(namespace, operator),
            None => {}
        }
        self.publish();
        Ok(())
    }

    async fn on_tick_complete(&mut self) -> Result<()> {
        let now = Instant::now();
        if self
            .last_reseed
            .is_some_and(|last| now.duration_since(last) < self.interval)
        {
            return Ok(());
        }
        // Stamp before the read, so a failing read retries on the cadence and
        // not on every tick.
        self.last_reseed = Some(now);
        let read = async {
            let at = self.reads.snapshot_block().await?;
            self.reads.seats_at(at).await
        };
        match read.await {
            Ok(seats) => {
                self.seated.replace(seats);
                self.publish();
            }
            Err(err) => warn!(
                error = %sanitize_err_chain(&err),
                "origin seats: authoritative re-read failed; keeping the current set"
            ),
        }
        Ok(())
    }

    fn on_recovered(&mut self) {
        clear_cadence_on_recovery(&mut self.last_reseed, self.interval);
    }
}

/// Read every seat once at a pinned block into `seated`, and build the route
/// that keeps it current.
///
/// # Errors
///
/// The boot read's error once the shared boot budget is spent, or at once on
/// a deterministic fault.
pub async fn bootstrap<P>(
    provider: P,
    origin_assignment_addr: Address,
    head: Arc<dyn HeadSource>,
    seated: Arc<SeatedOriginSet>,
    metrics: Arc<Metrics>,
    boot: &BootRetry,
) -> Result<Route>
where
    P: Provider + Clone + 'static,
{
    let reads = ContractReads {
        origin: OriginAssignment::new(origin_assignment_addr, provider),
        head,
    };
    let (at, seats) = boot
        .run("OriginAssignment seat snapshot", || async {
            let at = reads.snapshot_block().await?;
            let seats = reads
                .seats_at(at)
                .await
                .with_context(|| format!("read origin seats at {origin_assignment_addr}"))?;
            Ok((at, seats))
        })
        .await?;
    seated.replace(seats);
    metrics.seated_origins(seated.len());
    info!(
        seated = seated.len(),
        snapshot_block = at,
        %origin_assignment_addr,
        "origin seat bootstrap complete"
    );
    Ok(route_from(
        reads,
        origin_assignment_addr,
        at,
        seated,
        metrics,
    ))
}

fn route_from<R: SeatReads + 'static>(
    reads: R,
    origin_assignment_addr: Address,
    at: u64,
    seated: Arc<SeatedOriginSet>,
    metrics: Arc<Metrics>,
) -> Route {
    let sink = SeatSink {
        reads,
        seated,
        metrics,
        interval: RESEED_INTERVAL,
        // The bootstrap read just ran.
        last_reseed: Some(Instant::now()),
    };
    Route {
        addresses: vec![origin_assignment_addr],
        topic0s: vec![
            OriginAssignment::OriginAdded::SIGNATURE_HASH,
            OriginAssignment::OriginRemoved::SIGNATURE_HASH,
            OriginAssignment::InactiveOriginPruned::SIGNATURE_HASH,
        ],
        // The bootstrap read covers every event up to its snapshot block.
        start: CursorStart::Seeded { at },
        sink: SinkSource::Ready(Box::new(sink)),
        label: "origin-seats",
        on_established: None,
        on_backoff: None,
        on_tick_success: None,
        on_task_panic: None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
