//! The delegate's voucher credential and its funding signal (ADR 039 § Client
//! SDK funding signal, ADR 003 § Funding recovery).
//!
//! A delegated client pays from a pool it does not own, under a capability the
//! pool owner signed for its voucher key. It cannot top the pool up. When the
//! capability runs low, the application gets a new one from the owner and
//! pushes it into the fetch's [`CredentialSlot`]. The fetch then retires every
//! lane on the old key at a voucher boundary and opens new lanes under the new
//! key, from the delivered frontier. Nothing is fetched or paid twice.
//!
//! The fetch computes the signal itself, with no chain read: the capability's
//! headroom against the vouchers this run signed under it, the money the
//! remaining work needs at the highest rate a node quoted, and the time to the
//! capability's expiry ([`FundingEvent::RunningLow`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::primitives::{B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::SignedCapability;

use crate::PoolContext;

/// How long a delegated fetch, at the exhausted candidate set, waits for the
/// application to swap in a new credential before it ends with
/// [`FundingNeeded::NewCapability`]. One minute is time for an application to
/// ask a sponsor gateway for a fresh capability; a fetch with no one to ask sets
/// a zero wait ([`CredentialSlot::with_swap_wait`]).
pub const CREDENTIAL_SWAP_WAIT: Duration = Duration::from_mins(1);

/// The node's capability-expiry margin at the default redeem interval: inside
/// it a node accepts no voucher for the capability (ADR 003 § Revocation).
pub(crate) const NODE_EXPIRY_MARGIN_SECS: u64 = decdn_common::config::capability_expiry_margin_secs(
    decdn_common::config::DEFAULT_REDEEM_INTERVAL_SECS,
);

/// One voucher credential: the key that signs vouchers and the capability the
/// pool owner signed for it.
#[derive(Clone, Debug)]
pub struct Credential {
    /// The voucher-signing key the capability names.
    pub signer: Arc<PrivateKeySigner>,
    /// The owner-signed capability (the `dcap1` grant) that lets `signer`
    /// spend from the pool.
    pub capability: SignedCapability,
}

/// The current credential and how many swaps produced it.
#[derive(Clone, Debug)]
struct Current {
    generation: u64,
    credential: Credential,
}

#[derive(Debug)]
struct SlotInner {
    current: tokio::sync::watch::Sender<Current>,
    events: tokio::sync::watch::Sender<Option<FundingEvent>>,
    swap_wait: Duration,
}

/// A delegated fetch's voucher credential, which the application can swap at
/// any time. Clones share one slot.
///
/// A source provider reads [`Self::current`] when it builds a lane, and puts
/// its key and capability in the lane's [`PoolContext`]. After a
/// [`Self::swap`], the fetch ends every live lane on the old key at its next
/// voucher boundary, paid for what it received, and opens new lanes under the
/// new credential. A registered signer's terms are write-once on chain, so a
/// new capability names a new signer key.
#[derive(Clone, Debug)]
pub struct CredentialSlot {
    inner: Arc<SlotInner>,
}

impl CredentialSlot {
    /// A slot that holds `capability` for `signer`, with the
    /// [`CREDENTIAL_SWAP_WAIT`] wait.
    #[must_use]
    pub fn new(signer: Arc<PrivateKeySigner>, capability: SignedCapability) -> Self {
        Self::build(Credential { signer, capability }, CREDENTIAL_SWAP_WAIT)
    }

    /// A new slot with this slot's credential that waits `wait` for a swap at
    /// the exhausted candidate set. Clones of this slot stay on this slot.
    #[must_use]
    pub fn with_swap_wait(self, wait: Duration) -> Self {
        Self::build(self.current(), wait)
    }

    fn build(credential: Credential, swap_wait: Duration) -> Self {
        let (current, _) = tokio::sync::watch::channel(Current {
            generation: 0,
            credential,
        });
        let (events, _) = tokio::sync::watch::channel(None);
        Self {
            inner: Arc::new(SlotInner {
                current,
                events,
                swap_wait,
            }),
        }
    }

    /// Put `capability` for `signer` in the slot. Every fetch that holds the
    /// slot retires its lanes on the old key and goes on under this one.
    pub fn swap(&self, signer: Arc<PrivateKeySigner>, capability: SignedCapability) {
        self.inner.current.send_modify(|current| {
            current.generation = current.generation.saturating_add(1);
            current.credential = Credential { signer, capability };
        });
    }

    /// The credential a new lane signs under.
    #[must_use]
    pub fn current(&self) -> Credential {
        self.inner.current.borrow().credential.clone()
    }

    /// How many swaps the slot has taken.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.inner.current.borrow().generation
    }

    /// The pool the current capability draws from.
    #[must_use]
    pub fn pool_id(&self) -> B256 {
        self.inner
            .current
            .borrow()
            .credential
            .capability
            .capability
            .pool_id
    }

    /// How long a fetch at the exhausted candidate set waits for a swap.
    #[must_use]
    pub fn swap_wait(&self) -> Duration {
        self.inner.swap_wait
    }

    /// The fetch's funding signal: the latest [`FundingEvent`], or `None`
    /// while the credential covers the work. The value changes when the
    /// signal changes.
    #[must_use]
    pub fn funding_events(&self) -> tokio::sync::watch::Receiver<Option<FundingEvent>> {
        self.inner.events.subscribe()
    }

    /// Publish `event` when it differs from the last one.
    pub(crate) fn report(&self, event: Option<FundingEvent>) {
        self.inner.events.send_if_modified(|last| {
            let changed = *last != event;
            if changed {
                *last = event;
            }
            changed
        });
    }

    /// Wait until the slot takes a swap past `generation`.
    pub(crate) async fn swapped_since(&self, generation: u64) {
        let mut rx = self.inner.current.subscribe();
        // The sender lives as long as the slot, so the wait ends only on a swap.
        let _ = rx.wait_for(|current| current.generation > generation).await;
    }

    /// Whether `ctx` signs under a key other than the current credential's.
    pub(crate) fn is_stale(&self, ctx: &std::sync::Mutex<PoolContext>) -> bool {
        let current = self.inner.current.borrow().credential.signer.address();
        ctx.lock()
            .map_or(true, |ctx| ctx.client_signer.address() != current)
    }
}

/// A delegated fetch's funding signal ([`CredentialSlot::funding_events`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FundingEvent {
    /// The capability is about to stop paying: its headroom is short of the
    /// remaining work plus one credit window, or its expiry is inside the
    /// node's expiry margin. The application swaps in a new credential.
    RunningLow {
        /// The capability's spending cap less every voucher this run signed
        /// under it, in micro-USDC.
        headroom: U256,
        /// The remaining work's bytes at the highest rate a node quoted, in
        /// micro-USDC.
        projected_need: U256,
        /// The capability's expiry, in Unix seconds.
        expires_at: u64,
    },
}

/// Why a delegated fetch needs a new capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CapabilityCause {
    /// The vouchers signed under it reached its spending cap.
    CapSpent,
    /// It expires inside the node's expiry margin.
    Expired,
    /// Nodes refuse it although its cap and expiry still cover the work.
    Revoked,
}

impl std::fmt::Display for CapabilityCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::CapSpent => "has spent its cap",
            Self::Expired => "has expired",
            Self::Revoked => "was revoked",
        })
    }
}

/// The typed end of a delegated fetch that no node serves at its funding.
/// It is fatal to the command ([`crate::classify`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FundingNeeded {
    /// The capability no longer pays, and no swap came within the slot's
    /// wait. The pool owner issues a new capability for a new signer key.
    NewCapability {
        /// The pool the capability draws from.
        pool: B256,
        /// Why the capability no longer pays.
        cause: CapabilityCause,
    },
    /// The capability still covers the work, but nodes refuse the pool's
    /// funding. Only the pool owner can top the pool up.
    PublisherPool {
        /// The pool that cannot pay.
        pool: B256,
    },
}

impl std::fmt::Display for FundingNeeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewCapability { pool, cause } => write!(
                f,
                "funding needed: the capability for pool {pool} {cause}, and no new credential \
                 came"
            ),
            Self::PublisherPool { pool } => write!(
                f,
                "funding needed: the capability covers the work, but the nodes refuse the \
                 funding of pool {pool}; its owner must top it up"
            ),
        }
    }
}

impl std::error::Error for FundingNeeded {}

/// The highest rate and voucher interval any lane of one fetch was quoted.
#[derive(Debug, Default)]
#[doc(hidden)]
pub struct QuoteMax {
    rate_per_mb: AtomicU64,
    interval_bytes: AtomicU64,
}

impl QuoteMax {
    /// Fold one lane's quote in.
    pub(crate) fn observe(&self, rate_per_mb: u64, interval_bytes: u64) {
        self.rate_per_mb.fetch_max(rate_per_mb, Ordering::Relaxed);
        self.interval_bytes
            .fetch_max(interval_bytes, Ordering::Relaxed);
    }

    /// The highest rate quoted, in micro-USDC per MB.
    pub(crate) fn rate_per_mb(&self) -> u64 {
        self.rate_per_mb.load(Ordering::Relaxed)
    }

    /// The longest voucher interval quoted, in bytes.
    pub(crate) fn interval_bytes(&self) -> u64 {
        self.interval_bytes.load(Ordering::Relaxed)
    }
}

/// The cost of `bytes` at `rate_per_mb`, rounded up.
fn cost(bytes: u64, rate_per_mb: u64) -> U256 {
    U256::from(bytes)
        .saturating_mul(U256::from(rate_per_mb))
        .div_ceil(U256::from(decdn_protocol::MB_BYTES))
}

/// The local view of one capability against the work left.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CredentialView {
    /// The capability's spending cap less what this run signed under it.
    pub(crate) headroom: U256,
    /// The remaining work at the highest quoted rate.
    pub(crate) projected_need: U256,
    /// One credit window (one voucher interval) at the highest quoted rate.
    pub(crate) window: U256,
    /// The capability's expiry, in Unix seconds.
    pub(crate) expires_at: u64,
    /// Whether `now` lies inside the node's expiry margin.
    pub(crate) expiring: bool,
}

impl CredentialView {
    /// The view of `credential` after `signed` micro-USDC of vouchers under
    /// it, with `remaining_bytes` of work left at `quotes`, at Unix second
    /// `now`.
    pub(crate) fn of(
        credential: &Credential,
        signed: U256,
        remaining_bytes: u64,
        quotes: &QuoteMax,
        now: u64,
    ) -> Self {
        let capability = &credential.capability.capability;
        let rate = quotes.rate_per_mb();
        Self {
            headroom: U256::from(capability.spending_cap).saturating_sub(signed),
            projected_need: cost(remaining_bytes, rate),
            window: cost(quotes.interval_bytes(), rate),
            expires_at: capability.expiry,
            expiring: decdn_common::config::inside_capability_expiry_margin(
                capability.expiry,
                NODE_EXPIRY_MARGIN_SECS,
                now,
            ),
        }
    }

    /// Whether the headroom falls short of the remaining work plus one
    /// credit window.
    pub(crate) fn short(&self) -> bool {
        self.headroom < self.projected_need.saturating_add(self.window)
    }

    /// The running-low signal, or `None` while the capability covers the
    /// work.
    pub(crate) fn event(&self) -> Option<FundingEvent> {
        (self.short() || self.expiring).then_some(FundingEvent::RunningLow {
            headroom: self.headroom,
            projected_need: self.projected_need,
            expires_at: self.expires_at,
        })
    }

    /// Why the capability no longer pays, from the local view: expiry first,
    /// then a spent cap. `None` when both still cover the work.
    pub(crate) fn cause(&self) -> Option<CapabilityCause> {
        if self.expiring {
            Some(CapabilityCause::Expired)
        } else if self.short() {
            Some(CapabilityCause::CapSpent)
        } else {
            None
        }
    }
}

/// The current Unix second, or `0` when the clock reads before the epoch.
pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
