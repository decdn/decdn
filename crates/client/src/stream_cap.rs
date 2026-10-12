//! The run's per-provider cap on concurrent streams ([`LaneStreamCap`]): how
//! many streams one `(pool, signer, provider)` lane runs at once across every
//! fetch of a run, and in what order lanes that wait get a freed permit.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};

use alloy::primitives::Address;

/// Per-provider cap on concurrent streams to one `(pool, signer, provider)`
/// lane. A lane has one shared [`LaneLedgers`](crate::LaneLedgers) voucher watermark; concurrent
/// streams on the same lane draw on it together, and the serving node credits
/// each stream's delivered bytes from that shared watermark fairly — a slower
/// stream, whose reveal for its own chunk can land below a faster sibling's
/// frontier, is paid from the lane headroom the sibling opened rather than
/// stalled. So same-lane concurrency is safe; this only bounds how many streams
/// touch a given provider at once. `--max-lane-streams` sets the cap (default 4):
/// a `Semaphore(N)` admits N concurrent same-lane streams, and N == 1 runs a
/// single ordered voucher sequence per lane. Cross-lane parallelism (distinct
/// providers) is never bounded here — only by `--jobs`.
pub struct LaneStreamCap {
    /// Per-provider streams, created on first use. The `tokio::sync::Mutex`
    /// guards the map so the cap is `Sync` and shareable across the entry futures.
    map: tokio::sync::Mutex<HashMap<Address, Arc<ProviderStreams>>>,
    /// Concurrent-stream permits per provider (at least 1). At 1 a `Semaphore(1)`
    /// serializes same-lane streams exactly like a mutex.
    n: usize,
    /// The id the next lane's [`ExtraPermits`] gets.
    next_lane: AtomicU64,
}

impl std::fmt::Debug for LaneStreamCap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneStreamCap")
            .field("per_provider", &self.n)
            .finish_non_exhaustive()
    }
}

impl LaneStreamCap {
    /// A cap admitting `n` concurrent streams per provider (clamped to at least 1).
    pub fn new(n: usize) -> Self {
        Self {
            map: tokio::sync::Mutex::new(HashMap::new()),
            n: n.max(1),
            next_lane: AtomicU64::new(0),
        }
    }

    /// The per-provider streams, created on first use.
    async fn streams(&self, provider: Address) -> Arc<ProviderStreams> {
        let mut map = self.map.lock().await;
        Arc::clone(
            map.entry(provider)
                .or_insert_with(|| Arc::new(ProviderStreams::new(self.n))),
        )
    }

    /// One stream permit for `provider` if one is free now, else `None`. It
    /// never waits, so a caller that already holds another provider's permit
    /// cannot deadlock against a sibling that holds this one. It does not
    /// look at the provider's claims ([`ExtraPermits::grant`]): a lane's first
    /// stream takes any free permit, and a lane that finds none backs off and
    /// holds nothing.
    pub(crate) async fn try_permit(
        &self,
        provider: Address,
    ) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.streams(provider).await.semaphore)
            .try_acquire_owned()
            .ok()
    }

    /// How a lane to `provider` takes a stream beyond its lease
    /// ([`crate::LaneWiden`]): an extra stream for a queued range no
    /// idle lane takes, or the stream it starts again on. `grow` takes only
    /// a permit of `provider` that is free now and never waits, and `release`
    /// gives one back. The lane's first stream holds its permit as its lease,
    /// so the extra streams fit in the rest of the cap. At a cap of 3 or more,
    /// an extra stream never takes the provider's last free permit, so a
    /// sibling entry's first stream to `provider` can still open; the extra
    /// stream holds its permit for a whole range. A smaller cap has no room
    /// for both, and an extra stream takes any free permit. The acquire gives
    /// the lease back when the lane's own worker ends, and the lane starts
    /// again only on a permit `grow` takes; that start may take the last
    /// permit that no claim holds back.
    ///
    /// A lane that `grow` refuses claims the provider's next free permit,
    /// ahead of every later `grow` ask, and a lane that runs no stream goes
    /// ahead of the claims of lanes that run one ([`ProviderStreams::take`],
    /// #2341). A lane's first stream ([`Self::try_permit`]) still takes any
    /// free permit.
    pub(crate) async fn widen(&self, provider: Address) -> crate::LaneWiden {
        let extras = Arc::new(self.extra_permits(provider).await);
        let released = Arc::clone(&extras);
        crate::LaneWiden::new(
            move |kind| extras.grant(kind),
            move || released.release_one(),
        )
    }

    /// A new lane's grants of `provider`'s permits beside its lease, under a
    /// lane id no other lane of this cap has.
    async fn extra_permits(&self, provider: Address) -> ExtraPermits {
        ExtraPermits {
            streams: self.streams(provider).await,
            keep: self.extra_keep(),
            lane: self.next_lane.fetch_add(1, Ordering::Relaxed),
            held: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// The free permits of a provider an extra stream leaves for a sibling
    /// entry's first stream: one at a cap of 3 or more, where the lease, an
    /// extra stream and the kept permit fit, and none below.
    const fn extra_keep(&self) -> usize {
        if self.n > 2 { 1 } else { 0 }
    }
}

/// How long a lane's claim on a provider's next free permit holds without a
/// new ask. The acquire asks a refused `grow` again within
/// [`crate::GROWTH_RETRY`] while it still wants the stream, and each
/// ask renews the claim. So a lane that still waits keeps its claim, and the
/// claim of a lane that stopped asking, such as one whose own worker took
/// the range, ends soon after. A lane whose node refused its extra streams
/// is not asked while it waits out that refusal, so its claim lapses too:
/// the node would refuse the stream anyway.
const CLAIM_TTL: std::time::Duration = crate::GROWTH_RETRY.saturating_mul(3);

/// One provider's stream permits and the claims of the lanes that wait for
/// one.
struct ProviderStreams {
    /// The provider's per-lane stream semaphore.
    semaphore: Arc<tokio::sync::Semaphore>,
    /// The lanes [`Self::take`] refused, at most one claim per lane, in the
    /// order they first claimed. A renewal keeps a lane's place.
    claims: std::sync::Mutex<Vec<Claim>>,
}

/// A lane's claim on its provider's next free permit.
struct Claim {
    /// The claiming lane's [`ExtraPermits::lane`].
    lane: u64,
    /// The free permits the lane's last ask leaves after its own.
    keep: usize,
    /// Whether the lane ran no stream at its last ask.
    bare: bool,
    /// When the lane last asked.
    at: tokio::time::Instant,
}

impl ProviderStreams {
    /// `n` permits and no claims.
    fn new(n: usize) -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(n)),
            claims: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// A permit for `lane` if one is free now that no claim holds back,
    /// never waiting. `keep` is the free permits the ask leaves after its
    /// own, and `bare` says the lane runs no stream.
    ///
    /// Each claim ahead of `lane`'s own, or every claim when it has none,
    /// holds back one free permit, and the largest `keep` among the ask and
    /// those claims stays free after them. A bare ask counts only the bare
    /// claims ahead, so a lane that runs no stream goes ahead of a lane that
    /// waits for one more: every lane gets its first stream before any lane
    /// gets a second. So a lane that asks the instant a permit frees, such
    /// as one whose short legs restart beside its extra streams, cannot take
    /// it from a lane that asks only once per growth pass (#2341).
    ///
    /// A refused ask claims the next free permit, or renews the lane's claim
    /// in its place. A grant ends the claim. Claims older than [`CLAIM_TTL`]
    /// do not count. Every entry is polled on one task and this never
    /// awaits, so no sibling takes a permit between the count and the take.
    fn take(
        &self,
        lane: u64,
        keep: usize,
        bare: bool,
    ) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let now = tokio::time::Instant::now();
        let mut claims = self.claims.lock().unwrap_or_else(PoisonError::into_inner);
        claims.retain(|claim| now.saturating_duration_since(claim.at) < CLAIM_TTL);
        let own = claims.iter().position(|claim| claim.lane == lane);
        let (ahead, kept) = claims
            .get(..own.unwrap_or(claims.len()))
            .unwrap_or_default()
            .iter()
            .filter(|claim| !bare || claim.bare)
            .fold((0_usize, keep), |(n, kept), claim| {
                (n.saturating_add(1), kept.max(claim.keep))
            });
        if self.semaphore.available_permits() <= ahead.saturating_add(kept) {
            let claim = Claim {
                lane,
                keep,
                bare,
                at: now,
            };
            match own.and_then(|i| claims.get_mut(i)) {
                Some(renewed) => *renewed = claim,
                None => claims.push(claim),
            }
            return None;
        }
        match Arc::clone(&self.semaphore).try_acquire_owned() {
            Ok(permit) => {
                if let Some(i) = own {
                    claims.remove(i);
                }
                Some(permit)
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => None,
            Err(tokio::sync::TryAcquireError::Closed) => {
                // Nothing closes the semaphore: a closed one grants no
                // stream, and says so.
                tracing::warn!("a lane's stream semaphore is closed; it grants no extra stream");
                None
            }
        }
    }

    /// End `lane`'s claim, if it has one.
    fn forget(&self, lane: u64) {
        self.claims
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|claim| claim.lane != lane);
    }
}

/// The stream permits one lane holds beside its lease: its extra streams',
/// and its own stream's once it starts again without the lease
/// ([`LaneStreamCap::widen`]).
struct ExtraPermits {
    /// The provider's permits and claims.
    streams: Arc<ProviderStreams>,
    /// The free permits an extra stream leaves for a sibling entry's first
    /// stream.
    keep: usize,
    /// The lane's id in its provider's claims.
    lane: u64,
    /// The permits granted and not yet given back.
    held: std::sync::Mutex<Vec<tokio::sync::OwnedSemaphorePermit>>,
}

impl ExtraPermits {
    /// Take one permit if it is free now and no claim holds it back
    /// ([`ProviderStreams::take`]), never waiting, and return whether one
    /// was taken. An extra stream leaves `keep` free permits for a sibling
    /// entry's first stream; a restart may take the last one. The acquire
    /// asks a restart only once the lane's lease is given back, so a lane
    /// that asks one and holds no grant runs no stream.
    fn grant(&self, kind: crate::GrowFor) -> bool {
        let (keep, bare) = match kind {
            crate::GrowFor::Restart => (0, self.lock_held().is_empty()),
            crate::GrowFor::Extra => (self.keep, false),
        };
        let Some(permit) = self.streams.take(self.lane, keep, bare) else {
            return false;
        };
        self.lock_held().push(permit);
        true
    }

    /// Give back one held permit. The acquire gives back only what `grant`
    /// granted, so a call with none held breaks that rule: it does nothing
    /// and says so.
    fn release_one(&self) {
        let permit = self.lock_held().pop();
        debug_assert!(
            permit.is_some(),
            "a stream was given back that was never granted"
        );
        if permit.is_none() {
            tracing::warn!("a lane gave back a stream permit it does not hold");
        }
        drop(permit);
    }

    /// The held permits, locked.
    fn lock_held(&self) -> std::sync::MutexGuard<'_, Vec<tokio::sync::OwnedSemaphorePermit>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for ExtraPermits {
    /// An ended lane claims nothing.
    fn drop(&mut self) {
        self.streams.forget(self.lane);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
