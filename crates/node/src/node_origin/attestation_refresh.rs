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
//! A refresh that finds nothing fresher retries after
//! [`crate::attestations::REFRESH_RETRY_US`]. Once the held attestation has
//! expired with nothing fresher found, the node stops serving and publishing
//! the hash. While it still holds the blob, it keeps the expired attestation
//! and keeps retrying, so the hash serves again once an origin of its
//! namespace attests it. Once it no longer holds the blob, it drops the
//! attestation.

use std::sync::{Arc, OnceLock};
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

/// The most refreshes one tick runs. The rest stay due for the next tick.
pub(crate) const MAX_REFRESHES_PER_TICK: usize = 32;

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

/// Run refresh ticks until `stop` fires.
pub(crate) async fn run(refresher: Refresher, stop: CancellationToken) {
    let mut tick = tokio::time::interval(REFRESH_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _ = tick.tick() => refresher.refresh_due().await,
        }
    }
}

impl Refresher {
    /// Refresh every attestation due now, up to [`MAX_REFRESHES_PER_TICK`].
    pub(crate) async fn refresh_due(&self) {
        let due = self.attestations.refresh_due(now_us());
        for due in due.into_iter().take(MAX_REFRESHES_PER_TICK) {
            self.refresh(due).await;
        }
    }

    async fn refresh(&self, due: RefreshDue) {
        match self.fresher(&due).await {
            Some(Source::Own) => self.metrics.attestation_refreshed_self(),
            Some(Source::Dht) => self.metrics.attestation_refreshed_dht(),
            Some(Source::Origin) => self.metrics.attestation_refreshed_origin(),
            None => {
                self.metrics.attestation_refresh_failed();
                let RefreshFailed::Lapsed { first } =
                    self.attestations.refresh_failed(due.hash, now_us())
                else {
                    return;
                };
                let hash = Hash::from_bytes(due.hash);
                if first {
                    debug!(
                        %hash,
                        "origin attestation expired with no fresher one found; no longer serving the hash"
                    );
                    self.metrics.attestation_expired();
                }
                if !self.holds_blob(hash).await
                    && self.attestations.forget_lapsed(&due.hash, now_us())
                {
                    debug!(%hash, "dropped the expired origin attestation of a blob the node no longer holds");
                }
            }
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

/// Keep the freshest valid attestation the DHT records for `due.hash` carry,
/// when it expires after `due`'s and is still short of its own half-life.
async fn from_dht(deps: &NodeOriginDeps, due: &RefreshDue) -> bool {
    let lookup = crate::dht::find_providers(
        &deps.dht_connections,
        &deps.routing_table,
        &deps.staker_set,
        deps.attestations.verifier(),
        &deps.negative_cache,
        deps.self_id,
        DhtHash::from_bytes(due.hash),
        deps.config.lookup,
        Some(&deps.metrics),
    )
    .await;
    lookup.freshest.is_some_and(|attestation| {
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
