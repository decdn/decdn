//! Command-wide health of each provider (ADR 039 § Failure handling:
//! reassign-only tail).
//!
//! One `PeerHealth` spans a whole command, so a bundle pull cools a flaky
//! provider once for every entry. A source is never removed: a delivery fault
//! cools it for a while that doubles with each consecutive fault, and a
//! refusal on price parks it until the pool deposit rises.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use alloy::primitives::{Address, U256};
use tokio::time::{Duration, Instant};

use crate::fault::Fault;

/// The first cooldown after a delivery fault.
pub(crate) const COOL_BASE: Duration = Duration::from_secs(2);
/// The longest cooldown, reached after consecutive faults.
pub(crate) const COOL_CAP: Duration = Duration::from_mins(1);

/// One provider's health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum Health {
    /// Usable. `streak` counts delivery faults since its last verified byte.
    Healthy {
        /// Consecutive delivery faults without progress.
        streak: u32,
    },
    /// Not usable before `until`.
    Cooling {
        /// When the source is usable again.
        until: Instant,
        /// Consecutive delivery faults without progress.
        streak: u32,
    },
    /// Not usable until the pool deposit exceeds `at_deposit`.
    Unaffordable {
        /// The deposit the source refused at.
        at_deposit: U256,
    },
}

impl Health {
    const fn streak(self) -> u32 {
        match self {
            Self::Healthy { streak } | Self::Cooling { streak, .. } => streak,
            Self::Unaffordable { .. } => 0,
        }
    }
}

/// Every provider's [`Health`] for one command.
#[derive(Debug, Default)]
pub struct PeerHealth {
    map: Mutex<HashMap<Address, Health>>,
}

impl PeerHealth {
    /// `provider`'s health. An unseen provider is healthy.
    #[must_use]
    #[doc(hidden)]
    pub fn health(&self, provider: Address) -> Health {
        self.lock()
            .get(&provider)
            .copied()
            .unwrap_or(Health::Healthy { streak: 0 })
    }

    /// Whether `provider` may take work at `now` against `deposit`.
    #[must_use]
    #[doc(hidden)]
    pub fn usable(&self, provider: Address, now: Instant, deposit: U256) -> bool {
        match self.health(provider) {
            Health::Healthy { .. } => true,
            Health::Cooling { until, .. } => now >= until,
            Health::Unaffordable { at_deposit } => deposit > at_deposit,
        }
    }

    /// Record a fault `provider` raised at `now` while the deposit was `deposit`.
    #[doc(hidden)]
    pub fn record(&self, provider: Address, fault: Fault, now: Instant, deposit: U256) {
        let mut map = self.lock();
        let current = map
            .get(&provider)
            .copied()
            .unwrap_or(Health::Healthy { streak: 0 });
        let next = match fault {
            Fault::Source => {
                let streak = current.streak().saturating_add(1);
                Health::Cooling {
                    until: now + cool_for(streak),
                    streak,
                }
            }
            Fault::Unaffordable => Health::Unaffordable {
                at_deposit: deposit,
            },
            Fault::Fatal(_) | Fault::Transient => current,
        };
        map.insert(provider, next);
    }

    /// Record a verified byte from `provider`: it is healthy with no streak.
    #[doc(hidden)]
    pub fn record_progress(&self, provider: Address) {
        self.lock().insert(provider, Health::Healthy { streak: 0 });
    }

    /// When `provider` stops cooling, if it is cooling at `now`.
    #[must_use]
    #[doc(hidden)]
    pub fn cooling_until(&self, provider: Address, now: Instant) -> Option<Instant> {
        match self.health(provider) {
            Health::Cooling { until, .. } if until > now => Some(until),
            _ => None,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Address, Health>> {
        self.map.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The cooldown after the `streak`-th consecutive fault: 2 s, doubling, capped at 60 s.
fn cool_for(streak: u32) -> Duration {
    COOL_BASE
        .saturating_mul(2u32.saturating_pow(streak.saturating_sub(1)))
        .min(COOL_CAP)
}

#[cfg(test)]
mod tests;
