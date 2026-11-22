//! Refresh of the origin attestations this node holds (ADR 022 § Origin
//! Attestation).
//!
//! A node serves and publishes a hash only while it holds a live attestation
//! for it, and an attestation expires. From half its lifetime on, plus a
//! per-hash jitter of up to a quarter of the lifetime
//! ([`crate::attestations::Attestations::refresh_due`]), the node replaces it,
//! trying each source in turn:
//!
//! 1. **Self.** Its operator holds an origin seat and its own backend holds
//!    the hash: it signs a fresh one.
//! 2. **DHT.** The freshest valid attestation in the DHT records for the hash,
//!    when that one is still short of its own half-life. Every holder of a
//!    popular hash refreshes from the others this way, so the origin signs
//!    about once per hash per half-life, not once per holder.
//! 3. **Origin.** A voucher-less discovery open to a seated origin of the
//!    attestation's namespace: the attesting origin first, then the
//!    namespace's origin directory. The node keeps the attestation the served
//!    answer carries and cancels the stream, so the open costs the origin one
//!    signature and no egress.
//!
//! A refresh runs only while the node holds the blob. An attestation for a
//! blob the node no longer holds, or never landed, is dropped instead.
//!
//! A refresh that finds nothing fresher retries after
//! [`crate::attestations::REFRESH_RETRY_US`]. Once the held attestation has
//! expired with nothing fresher found, the node stops serving and publishing
//! the hash. It keeps the expired attestation and keeps retrying, so the hash
//! serves again once an origin of its namespace attests it.
//!
//! The task also looks up each wanted hash: one the node holds with no
//! attestation at all ([`crate::attestations::Attestations::want`]). It keeps
//! the freshest valid attestation the DHT records carry.
//!
//! Each tick runs up to [`MAX_REFRESHES_PER_TICK`] jobs, the due refreshes
//! first, [`REFRESH_CONCURRENCY`] at a time. A stop cancels the jobs in
//! flight.

use std::sync::{Arc, OnceLock};

use futures_util::StreamExt as _;
use std::time::Duration;

use alloy::primitives::U256;
use iroh::PublicKey;
use iroh_blobs::Hash;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::{NodeOriginDeps, OpenSpec, discovery_answer, discovery_ctx, fallback_origins};
use crate::attestations::{Attestations, RefreshDue, RefreshFailed};
use crate::dht::negative_cache::Hash as DhtHash;
use crate::handlers::timestamp_skew::now_us;
use crate::metrics::Metrics;

/// How often the task looks for due refreshes.
pub(crate) const REFRESH_TICK: Duration = Duration::from_secs(60);

/// The most refreshes and wanted lookups one tick runs. The rest stay due for
/// the next tick. At [`REFRESH_CONCURRENCY`] jobs at a time, this keeps up
/// with tens of thousands of hashes at the shortest attestation lifetime.
pub(crate) const MAX_REFRESHES_PER_TICK: usize = 256;

/// How many refreshes and wanted lookups run at once.
pub(crate) const REFRESH_CONCURRENCY: usize = 8;

/// The most seated origins one refresh opens a stream to.
const MAX_ORIGIN_OPENS: usize = 3;

/// What the refresh task reads and writes.
pub(crate) struct Refresher {
    /// The attestations the node holds.
    pub(crate) attestations: Arc<Attestations>,
    /// The cache engine, for the own-backend check before a self-signed
    /// refresh.
    pub(crate) cache: decdn_cache::CacheEngine,
    /// Node metrics.
    pub(crate) metrics: Arc<Metrics>,
    /// The node-to-node pull dependencies, once provisioned. Without them the
    /// node refreshes only what it signs itself.
    pub(crate) pull: Option<Arc<OnceLock<NodeOriginDeps>>>,
}

/// Where a refresh found its fresher attestation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Own,
    Dht,
    Origin,
}

/// One unit of refresh work.
enum Job {
    /// A held attestation whose refresh is due.
    Due(RefreshDue),
    /// A hash the node holds with no attestation.
    Wanted([u8; 32]),
}

/// Run refresh ticks until `stop` fires. A stop also cancels the jobs of a
/// tick in flight.
pub(crate) async fn run(refresher: Refresher, stop: CancellationToken) {
    let mut tick = tokio::time::interval(REFRESH_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _ = tick.tick() => {}
        }
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = refresher.refresh_due() => {}
        }
    }
}

impl Refresher {
    /// Run the refreshes due now and then the wanted lookups, up to
    /// [`MAX_REFRESHES_PER_TICK`] jobs in all, [`REFRESH_CONCURRENCY`] at a
    /// time.
    pub(crate) async fn refresh_due(&self) {
        let now = now_us();
        let due = self.attestations.refresh_due(now);
        let room = MAX_REFRESHES_PER_TICK.saturating_sub(due.len());
        let wanted = self.attestations.wanted_due(now, room);
        let jobs = due
            .into_iter()
            .take(MAX_REFRESHES_PER_TICK)
            .map(Job::Due)
            .chain(wanted.into_iter().map(Job::Wanted));
        futures_util::stream::iter(jobs)
            .for_each_concurrent(REFRESH_CONCURRENCY, |job| async move {
                match job {
                    Job::Due(due) => self.refresh(due).await,
                    Job::Wanted(hash) => self.recover(hash).await,
                }
            })
            .await;
    }

    async fn refresh(&self, due: RefreshDue) {
        let hash = Hash::from_bytes(due.hash);
        if !self.holds_blob(hash).await {
            if self
                .attestations
                .forget_if_held(&due.hash, &due.attestation)
            {
                debug!(%hash, "dropped the origin attestation of a blob the node does not hold");
            }
            return;
        }
        match self.fresher(&due).await {
            Some(Source::Own) => self.metrics.attestation_refreshed_self(),
            Some(Source::Dht) => self.metrics.attestation_refreshed_dht(),
            Some(Source::Origin) => self.metrics.attestation_refreshed_origin(),
            None => {
                self.metrics.attestation_refresh_failed();
                if self.attestations.refresh_failed(due.hash, now_us())
                    == (RefreshFailed::Lapsed { first: true })
                {
                    debug!(
                        %hash,
                        "origin attestation expired with no fresher one found; no longer serving the hash"
                    );
                    self.metrics.attestation_expired();
                }
            }
        }
    }

    /// Look up an attestation for `hash`, which the node holds with no
    /// attestation at all, and keep the freshest valid one the DHT records
    /// carry. A hash the node no longer holds stops being wanted.
    async fn recover(&self, hash: [u8; 32]) {
        if !self.holds_blob(Hash::from_bytes(hash)).await {
            self.attestations.unwant(&hash);
            return;
        }
        let Some(deps) = self.pull.as_ref().and_then(|pull| pull.get()) else {
            return;
        };
        if freshest_in_dht(deps, hash)
            .await
            .is_some_and(|attestation| deps.attestations.keep(hash, attestation))
        {
            self.metrics.attestation_refreshed_dht();
        }
    }

    /// Whether the node holds any of `hash`: its cache holds a byte of it, or
    /// its own origin backend holds it. A store fault counts as held, so a
    /// fault never drops an attestation the node may still serve under.
    async fn holds_blob(&self, hash: Hash) -> bool {
        self.cache.origin_held_size(hash).is_some()
            || !matches!(self.cache.coverage(hash).await, Ok(coverage) if coverage.is_empty())
    }

    /// Find and keep an attestation that expires after `due`'s, and say where
    /// it came from.
    async fn fresher(&self, due: &RefreshDue) -> Option<Source> {
        if self.attestations.can_attest_own()
            && matches!(
                self.cache
                    .origin_probe_presence(Hash::from_bytes(due.hash))
                    .await,
                decdn_cache::OriginPresence::Present(_)
            )
            && self
                .attestations
                .attest_own(due.hash)
                .is_some_and(|signed| signed.expires_at_us() > due.attestation.expires_at_us())
        {
            return Some(Source::Own);
        }
        let deps = self.pull.as_ref()?.get()?;
        if from_dht(deps, due).await {
            return Some(Source::Dht);
        }
        from_origin(deps, due).await.then_some(Source::Origin)
    }
}

/// The freshest valid attestation the DHT records for `hash` carry.
async fn freshest_in_dht(
    deps: &NodeOriginDeps,
    hash: [u8; 32],
) -> Option<decdn_protocol::OriginAttestation> {
    crate::dht::find_providers(
        &deps.dht_connections,
        &deps.routing_table,
        &deps.staker_set,
        deps.attestations.verifier(),
        &deps.negative_cache,
        deps.self_id,
        DhtHash::from_bytes(hash),
        deps.config.lookup,
        Some(&deps.metrics),
    )
    .await
    .freshest
}

/// Keep the freshest valid attestation the DHT records for `due.hash` carry,
/// when it expires after `due`'s and is still short of its own half-life.
async fn from_dht(deps: &NodeOriginDeps, due: &RefreshDue) -> bool {
    freshest_in_dht(deps, due.hash)
        .await
        .is_some_and(|attestation| {
            attestation.expires_at_us() > due.attestation.expires_at_us()
                && now_us() < attestation.half_life_at_us()
                && deps.attestations.keep(due.hash, attestation)
        })
}

/// Ask a seated origin of `due`'s namespace for a fresh attestation over a
/// voucher-less discovery open, and keep it when it expires after `due`'s.
async fn from_origin(deps: &NodeOriginDeps, due: &RefreshDue) -> bool {
    let namespace = U256::from_be_bytes(due.attestation.namespace_id);
    let attesting: Vec<_> = deps
        .attestations
        .verifier()
        .recover(due.hash, &due.attestation)
        .map(|signer| deps.origin_directory.origin_nodes(&[signer]))
        .unwrap_or_default();
    let origins = fallback_origins(deps, namespace, &attesting, None).await;
    if origins.is_empty() {
        return false;
    }
    let Ok(ctx) = discovery_ctx(deps).await else {
        return false;
    };
    let spec = OpenSpec::new(ctx, due.attestation.namespace_id, (0, 0));
    for origin in origins.into_iter().take(MAX_ORIGIN_OPENS) {
        let Ok(pk) = PublicKey::from_bytes(origin.as_bytes()) else {
            continue;
        };
        let Some(provider_addr) = deps.addr_resolver.address_of(&origin) else {
            continue;
        };
        let Some(Ok(opened)) = discovery_answer(deps, pk, provider_addr, due.hash, &spec).await
        else {
            continue;
        };
        let offered = opened
            .served()
            .then(|| opened.ext().origin_attestation.clone())
            .flatten();
        opened.cancel();
        if offered.is_some_and(|attestation| {
            attestation.expires_at_us() > due.attestation.expires_at_us()
                && deps.attestations.keep(due.hash, attestation)
        }) {
            return true;
        }
    }
    false
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
