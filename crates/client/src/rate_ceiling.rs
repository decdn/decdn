//! The per-MiB rate a lane pays its node at most in one fetch
//! ([`RateCeiling`]).

use std::sync::{Arc, OnceLock};

use crate::RateAboveCeiling;

/// The most a lane pays per MiB on each stream of its node in one fetch
/// (ADR 005 § Rate self-enforcement).
///
/// The ceiling is the node's first verified quote in the fetch, and also the
/// payer's absolute `max_rate_per_mb` when that is set. The first quote is
/// the node's discovery answer. When no discovery answer quoted the node, it
/// is the first stream a lane opens on the node. A later stream that quotes
/// above the ceiling is refused unpaid with [`RateAboveCeiling`]. So a node
/// that wins ranking at one quote cannot raise its rate for the rest of the
/// fetch.
///
/// Clones share one first quote. Every lane and source of one node in one
/// fetch holds a clone of the same ceiling.
#[derive(Debug, Clone, Default)]
pub struct RateCeiling {
    /// The payer's absolute ceiling, `0` for none.
    max_rate_per_mb: u64,
    /// The node's first verified quote in the fetch, once known.
    first_quote: Arc<OnceLock<u64>>,
}

impl RateCeiling {
    /// A ceiling with no first quote yet, under the payer's absolute
    /// `max_rate_per_mb` (`0` for none).
    #[must_use]
    pub fn new(max_rate_per_mb: u64) -> Self {
        Self {
            max_rate_per_mb,
            first_quote: Arc::new(OnceLock::new()),
        }
    }

    /// A ceiling whose first quote is `first_quote`, a quote the node
    /// signed in a verified answer: its discovery answer.
    #[must_use]
    pub fn quoted(max_rate_per_mb: u64, first_quote: u64) -> Self {
        let ceiling = Self::new(max_rate_per_mb);
        ceiling.note_quote(first_quote);
        ceiling
    }

    /// Record `rate_per_mb`, a quote the node signed in a verified answer, as
    /// its first quote. A ceiling that has a first quote keeps it.
    pub fn note_quote(&self, rate_per_mb: u64) {
        let _ = self.first_quote.get_or_init(|| rate_per_mb);
    }

    /// The payer's absolute ceiling, `0` for none.
    #[must_use]
    pub const fn max_rate_per_mb(&self) -> u64 {
        self.max_rate_per_mb
    }

    /// The node's first verified quote in the fetch, or `None` before one.
    #[must_use]
    pub fn first_quote(&self) -> Option<u64> {
        self.first_quote.get().copied()
    }

    /// The ceiling in force: the lower of the first quote and the absolute
    /// ceiling, or `None` when neither is set.
    #[must_use]
    pub fn bound(&self) -> Option<u64> {
        let absolute = (self.max_rate_per_mb > 0).then_some(self.max_rate_per_mb);
        match (self.first_quote(), absolute) {
            (Some(first), Some(absolute)) => Some(first.min(absolute)),
            (first, absolute) => first.or(absolute),
        }
    }

    /// Admit a stream that quotes `rate_per_mb`, before the payer pays it.
    /// The first admitted quote becomes the first quote when none is set.
    ///
    /// # Errors
    ///
    /// [`RateAboveCeiling`] when the quote is above the ceiling in force.
    pub(crate) fn admit(&self, rate_per_mb: u64) -> anyhow::Result<()> {
        if let Some(bound) = self.bound()
            && rate_per_mb > bound
        {
            return Err(RateAboveCeiling::over_ceiling(rate_per_mb, bound));
        }
        // Two streams can race to set the first quote: the lower one wins only
        // when it sets it first, so the loser checks against the winner.
        let first = *self.first_quote.get_or_init(|| rate_per_mb);
        if rate_per_mb > first {
            return Err(RateAboveCeiling::over_ceiling(rate_per_mb, first));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
