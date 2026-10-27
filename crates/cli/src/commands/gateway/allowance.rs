//! The public gateway's spend bound: the bytes it delivers per UTC day from
//! its pool.
//!
//! The bound is the only thing between an anonymous `GET` and the operator's
//! wallet. It is metered on the bytes the gateway delivers, in memory: the
//! pool's balance is the hard stop, so a restart that forgives at most one
//! day's allowance costs nothing worth a store. A request whose size is known
//! (an entry's declared size, or a raw blob's size a pull verified) and
//! does not fit is refused before any pull; a stream whose size nobody knew
//! is cut once the bound is reached, so the gateway never delivers more
//! than the allowance plus the frames in flight. The next request is refused
//! until the day turns.
//!
//! One shared bucket takes every request, whatever namespace hint it
//! carries: the hint is the client's to set (ADR 002), so it buys no bucket
//! of its own. A namespace the operator gave an allowance of its own
//! (`--allowance <ns>=<bytes>`) has a bucket that replaces the shared one,
//! for the requests its pointer serves and those alone: a pointer is
//! owner-signed, so a request on `<ns>.<host>` is the namespace's, where a
//! `?ns=` or a `<ns>.<hash32>.` label is anyone's.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use serde::Serialize;

/// Seconds in the UTC day the allowance counts over.
const DAY_SECS: u64 = 86_400;

/// The seconds from `now_secs` until the UTC day turns.
const fn secs_to_next_day(now_secs: u64) -> u64 {
    (now_secs / DAY_SECS + 1)
        .saturating_mul(DAY_SECS)
        .saturating_sub(now_secs)
}

/// Bytes delivered in one day.
struct Used {
    day: u64,
    bytes: u64,
}

/// One allowance: the bytes per day, and what was delivered today.
struct Bucket {
    /// The bytes per day. `u64::MAX` is no bound.
    per_day: u64,
    used: Mutex<Used>,
}

impl Bucket {
    const fn new(per_day: u64) -> Self {
        Self {
            per_day,
            used: Mutex::new(Used { day: 0, bytes: 0 }),
        }
    }

    /// The bytes delivered on the UTC day of `now_secs`.
    fn spent_today(&self, now_secs: u64) -> u64 {
        let used = self.used.lock().unwrap_or_else(PoisonError::into_inner);
        if used.day == now_secs / DAY_SECS {
            used.bytes
        } else {
            0
        }
    }

    fn remaining(&self, now_secs: u64) -> Result<u64, u64> {
        if self.per_day == u64::MAX {
            return Ok(u64::MAX);
        }
        let left = self.per_day.saturating_sub(self.spent_today(now_secs));
        if left > 0 {
            Ok(left)
        } else {
            Err(secs_to_next_day(now_secs))
        }
    }

    /// What the bucket delivered today and has left, for `namespace`'s row.
    fn view(&self, namespace: Option<u64>, now_secs: u64) -> BucketView {
        let resets_in_secs = secs_to_next_day(now_secs);
        if self.per_day == u64::MAX {
            return BucketView {
                namespace,
                per_day: None,
                used: 0,
                remaining: None,
                resets_in_secs,
            };
        }
        let used = self.spent_today(now_secs);
        BucketView {
            namespace,
            per_day: Some(self.per_day),
            used,
            remaining: Some(self.per_day.saturating_sub(used)),
            resets_in_secs,
        }
    }

    fn charge(&self, bytes: u64, now_secs: u64) -> bool {
        if self.per_day == u64::MAX {
            return true;
        }
        let day = now_secs / DAY_SECS;
        let mut used = self.used.lock().unwrap_or_else(PoisonError::into_inner);
        if used.day != day {
            *used = Used { day, bytes: 0 };
        }
        used.bytes = used.bytes.saturating_add(bytes);
        used.bytes < self.per_day
    }
}

/// One bucket as `/_status` reports it.
#[derive(Debug, Serialize)]
pub(super) struct BucketView {
    /// The namespace the bucket is for; `None` for the shared one.
    pub(super) namespace: Option<u64>,
    /// The bytes per UTC day; `None` when there is no bound.
    pub(super) per_day: Option<u64>,
    /// The bytes delivered today. A bucket with no bound counts nothing.
    pub(super) used: u64,
    /// The bytes it may still deliver today; `None` when there is no bound.
    pub(super) remaining: Option<u64>,
    /// The seconds until the UTC day turns and the count starts again.
    pub(super) resets_in_secs: u64,
}

/// The bytes the gateway may deliver per day: one bucket shared by every
/// request, and one of its own per namespace the operator set one for.
pub(super) struct Allowance {
    shared: Bucket,
    namespaces: HashMap<u64, Bucket>,
}

impl Allowance {
    /// An allowance of `per_day` bytes per UTC day, shared by every request,
    /// and for each namespace in `namespaces` a bucket of its own bytes per
    /// day in place of it.
    pub(super) fn with_namespaces(
        per_day: u64,
        namespaces: impl IntoIterator<Item = (u64, u64)>,
    ) -> Self {
        Self {
            shared: Bucket::new(per_day),
            namespaces: namespaces
                .into_iter()
                .map(|(ns, per_day)| (ns, Bucket::new(per_day)))
                .collect(),
        }
    }

    /// The bucket a request spends: the namespace's own when the request is
    /// a pointer's (`namespace`) and that namespace has one, else the shared
    /// bucket.
    fn bucket(&self, namespace: Option<u64>) -> &Bucket {
        namespace
            .and_then(|ns| self.namespaces.get(&ns))
            .unwrap_or(&self.shared)
    }

    /// The bytes the gateway may still deliver today for a request of
    /// `namespace`'s pointer (`None` for any other), or, when none, the
    /// seconds until the UTC day turns and it may again.
    pub(super) fn remaining(&self, namespace: Option<u64>, now_secs: u64) -> Result<u64, u64> {
        self.bucket(namespace).remaining(now_secs)
    }

    /// Every bucket at `now_secs`: the shared one first, then each
    /// namespace's in ascending order.
    pub(super) fn view(&self, now_secs: u64) -> Vec<BucketView> {
        let mut namespaces: Vec<_> = self.namespaces.iter().collect();
        namespaces.sort_by_key(|(ns, _)| **ns);
        std::iter::once(self.shared.view(None, now_secs))
            .chain(
                namespaces
                    .into_iter()
                    .map(|(ns, bucket)| bucket.view(Some(*ns), now_secs)),
            )
            .collect()
    }

    /// Record `bytes` delivered now for a request of `namespace`'s pointer
    /// (`None` for any other). `false` once the day's allowance is reached:
    /// the caller stops delivering.
    pub(super) fn charge(&self, namespace: Option<u64>, bytes: u64, now_secs: u64) -> bool {
        self.bucket(namespace).charge(bytes, now_secs)
    }
}

#[cfg(test)]
mod tests;
