//! The lane ramp-credit pool (ADR 003 §Credit window).
//!
//! A stream's credit window ramps with `paid / credit_ramp_divisor`. The `paid`
//! input is the stream's own confirmed payment plus the credit it carries from
//! its lane. Each lane keeps one [`RampPool`] of paid content bytes: the content
//! of the requests its fully paid streams served. Content is never more than the
//! wire bytes that paid for it, so the pool never overstates the lane's revenue,
//! and the pull leg's `RampPacer` uses it directly against its content
//! frontiers. A new stream takes the whole pool as a [`RampCarry`], so a payer
//! that pulls many ranges back to back keeps its ramp instead of starting each
//! request at the floor.
//!
//! Only a stream that ends fully paid returns credit: its carry plus the content
//! of its request, just before its `StreamEnd`. A stream that ends any other
//! way after its first byte (abandoned, rejected, faulted) forfeits its carry
//! and its own payment. A stream refused before its first byte returns its
//! carry whole. Each paid byte therefore opens credit on at most one stream that
//! leaves bytes unpaid, so the lane's unpaid exposure over its whole life stays
//! at `paid / credit_ramp_divisor` plus one floor for each stream. Two live
//! streams never hold the same paid bytes, because a take empties the pool. The
//! pool holds at most `credit_max × credit_ramp_divisor`, which is the credit
//! that opens one window at the ceiling. The pool lives in memory only: a
//! restart, or a forgotten lane, starts the lane at the floor again.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// One lane's banked ramp credit, in content bytes the lane's streams paid for.
///
/// Atomic, not guarded by the lane's `tokio` mutex, so a carry can return its
/// credit from a synchronous `Drop`.
#[derive(Debug, Default)]
pub(crate) struct RampPool {
    credit: AtomicU64,
}

impl RampPool {
    /// Remove and return all banked credit, at most `cap`.
    fn take(&self, cap: u64) -> u64 {
        let prev = self
            .credit
            .update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                c.saturating_sub(cap)
            });
        prev.min(cap)
    }

    /// Add `bytes` of credit, holding the pool at `cap`.
    fn bank(&self, bytes: u64, cap: u64) {
        if bytes == 0 || cap == 0 {
            return;
        }
        self.credit
            .update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                c.saturating_add(bytes).min(cap)
            });
    }

    /// The credit the pool holds now.
    #[cfg(test)]
    pub(crate) fn credit(&self) -> u64 {
        self.credit.load(Ordering::Relaxed)
    }
}

/// The ramp credit one stream took from its lane, in paid content bytes.
///
/// The stream returns it, with the content it paid for itself, through
/// [`Self::return_paid`] when it ends fully paid. A carry dropped before
/// [`Self::start_delivery`] returns its credit untouched: the stream sent no
/// byte, so it left nothing unpaid. A carry dropped after that forfeits its
/// credit.
#[derive(Debug)]
pub(crate) struct RampCarry {
    pool: Arc<RampPool>,
    cap: u64,
    carried: u64,
    delivering: bool,
}

impl RampCarry {
    /// Take the whole of `pool`, capped at `cap`, for one new stream. A `cap` of
    /// `0` (a disabled ramp, whose window is already at the ceiling) takes and
    /// returns nothing.
    pub(crate) fn take(pool: Arc<RampPool>, cap: u64) -> Self {
        let carried = pool.take(cap);
        Self {
            pool,
            cap,
            carried,
            delivering: false,
        }
    }

    /// A carry with no lane behind it: it carries nothing and returns nowhere.
    pub(crate) fn detached() -> Self {
        Self::take(Arc::new(RampPool::default()), 0)
    }

    /// The paid content bytes this stream carries from its lane.
    pub(crate) const fn carried(&self) -> u64 {
        self.carried
    }

    /// The ramp input for a stream that confirmed `own_paid` bytes itself.
    /// The serve loops pass wire bytes, which are at least the content they
    /// carry, so the sum never overstates what the lane paid.
    pub(crate) const fn ramp_paid(&self, own_paid: u64) -> u64 {
        self.carried.saturating_add(own_paid)
    }

    /// Mark the first byte of the stream as about to go out. From here on, a
    /// dropped carry forfeits its credit.
    pub(crate) const fn start_delivery(&mut self) {
        self.delivering = true;
    }

    /// Return `carried + paid_content` to the lane's pool. Call it only when the
    /// stream has delivered, and been paid for, all `paid_content` bytes of its
    /// request.
    pub(crate) fn return_paid(mut self, paid_content: u64) {
        self.pool
            .bank(self.carried.saturating_add(paid_content), self.cap);
        self.carried = 0;
    }
}

impl Drop for RampCarry {
    fn drop(&mut self) {
        if !self.delivering {
            self.pool.bank(self.carried, self.cap);
        }
    }
}

#[cfg(test)]
mod tests;
