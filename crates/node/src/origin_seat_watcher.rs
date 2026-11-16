//! `OriginAssignment` seat snapshot (ADR 022 § Origin Attestation).
//!
//! Fills the node's [`SeatedOriginSet`] — every operator that holds an origin
//! seat in at least one namespace — and keeps it current. A DHT record is
//! valid only when its attestation recovers to an operator in that set.
//!
//! The snapshot reads `assignedNamespaces` and `getOrigins` for each namespace
//! at one pinned block, because the contract's swap-and-pop removal reorders
//! the namespace list between blocks. The node re-reads it every
//! `SEAT_REFRESH_INTERVAL`, and early when an attestation names an operator
//! the set does not hold, at most once per `UNKNOWN_SEAT_REFRESH_GAP`. It
//! reads no event log.
//!
//! The set fails closed: a boot read that cannot finish leaves it empty, and an
//! empty set makes no record valid.

use std::sync::Arc;
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use anyhow::{Context, Result};
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::origin_assignment::OriginAssignment;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::chain_events::boot_retry::BootRetry;
use crate::chain_events::refresh::run_refresh_loop;
use crate::chain_events::snapshot::snapshot_block;
use crate::chain_events::timed;
use crate::dht::capacity_bond_registry::RefreshTask;
use crate::dht::seated_origins::{SeatedOriginSet, SeatedOrigins};
use crate::metrics::Metrics;

/// How often the seat set is re-read. Ten minutes bounds how long an unseated
/// origin's records stay valid, at one full read per interval.
const SEAT_REFRESH_INTERVAL: Duration = Duration::from_mins(10);

/// The least time between two seat reads that an unknown origin pulls forward.
/// One minute admits a newly seated origin quickly while a flood of records
/// naming unseated operators costs at most one read a minute.
const UNKNOWN_SEAT_REFRESH_GAP: Duration = Duration::from_mins(1);

/// Namespaces read per `assignedNamespaces` page.
const PAGE_SIZE: u64 = 256;

/// The chain reads the snapshot makes, behind a trait so the refresh is
/// testable without a provider.
pub(crate) trait SeatReads: Send + Sync {
    /// The block to pin a full read at.
    fn snapshot_block(&self) -> impl Future<Output = Result<u64>> + Send;

    /// Every `(namespace, operator)` seat at block `at`.
    fn seats_at(&self, at: u64) -> impl Future<Output = Result<Vec<(U256, Address)>>> + Send;
}

struct ContractReads<P: Provider + Clone> {
    origin: OriginAssignment::OriginAssignmentInstance<P>,
}

impl<P: Provider + Clone> SeatReads for ContractReads<P> {
    async fn snapshot_block(&self) -> Result<u64> {
        snapshot_block(self.origin.provider(), *self.origin.address()).await
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

/// Re-read every seat at a pinned block and replace the set. A failed read
/// keeps the current set.
async fn reseed<R: SeatReads>(reads: &R, seated: &SeatedOriginSet, metrics: &Metrics) {
    let read = async {
        let at = reads.snapshot_block().await?;
        reads.seats_at(at).await
    };
    match read.await {
        Ok(seats) => {
            seated.replace(seats);
            metrics.seated_origins(seated.len());
        }
        Err(err) => warn!(
            error = %sanitize_err_chain(&err),
            "origin seats: re-read failed; keeping the current set"
        ),
    }
}

/// Read every seat once at a pinned block into `seated`, and return the
/// refresh loop that keeps it current until `shutdown` is cancelled.
///
/// # Errors
///
/// The boot read's error once the shared boot budget is spent, or at once on
/// a deterministic fault.
pub async fn bootstrap<P>(
    provider: P,
    origin_assignment_addr: Address,
    seated: Arc<SeatedOriginSet>,
    metrics: Arc<Metrics>,
    boot: &BootRetry,
    shutdown: CancellationToken,
) -> Result<RefreshTask>
where
    P: Provider + Clone + 'static,
{
    let reads = ContractReads {
        origin: OriginAssignment::new(origin_assignment_addr, provider),
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
    Ok(refresh_task(reads, seated, metrics, shutdown))
}

fn refresh_task<R: SeatReads + 'static>(
    reads: R,
    seated: Arc<SeatedOriginSet>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> RefreshTask {
    let trigger = seated.refresh_trigger();
    let reads = Arc::new(reads);
    Box::pin(run_refresh_loop(
        SEAT_REFRESH_INTERVAL,
        UNKNOWN_SEAT_REFRESH_GAP,
        Some(trigger),
        shutdown,
        move || {
            let (reads, seated, metrics) = (
                Arc::clone(&reads),
                Arc::clone(&seated),
                Arc::clone(&metrics),
            );
            async move { reseed(&*reads, &seated, &metrics).await }
        },
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
