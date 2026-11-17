//! The delegate keys a gateway holds and the capabilities registered to them,
//! in memory only.
//!
//! At most one key is pending: no capability claims it yet, and
//! [`Delegates::pending`] returns it to every caller until one does. A key a
//! capability claimed holds at most one capability per pool. A capability is
//! scoped to a list of namespaces, or unscoped. The gateway pays with the
//! capabilities in the order they were registered ([`Delegates::paying`]):
//! for a request of a namespace, the capabilities scoped to that namespace
//! first, then the unscoped ones; for any other request, the unscoped ones
//! alone. The gateway decides which namespace a request is of. It skips one a funding stop retired
//! ([`Delegates::retire`]) and one inside the expiry margin nodes apply.

use std::collections::HashMap;
use std::sync::Arc;

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use decdn_common::config::inside_capability_expiry_margin;
use decdn_incentive::{CapabilityGrant, PoolId};

use super::super::fetch::Credential;

/// Why a capability does not register.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Refusal {
    /// It names a key the gateway does not hold.
    UnknownKey,
    /// Its key already holds a capability from its pool.
    PoolTaken,
}

impl Refusal {
    /// The answer's text.
    pub(super) const fn text(&self) -> &'static str {
        match self {
            Self::UnknownKey => {
                "the capability names no delegate this gateway is holding: get one from \
                 GET /_delegate\n"
            }
            Self::PoolTaken => {
                "that delegate already holds a capability from this pool: get a new one from \
                 GET /_delegate\n"
            }
        }
    }
}

/// One registered capability as `GET /_capability` and `/_status` list it.
/// The fields are declared in sorted key order, which is the body's key
/// order: the SDKs read this body, and its shape is a contract
/// (`the_capability_list_keeps_its_shape`). `namespaces` appears on a scoped
/// capability alone.
#[derive(Debug, serde::Serialize)]
pub(super) struct Capability {
    /// The spending cap, in micro-USDC.
    pub(super) cap: u64,
    /// The capability's expiry, Unix seconds.
    pub(super) expiry: u64,
    /// The namespaces it pays for; absent when it pays for any request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) namespaces: Option<Vec<u64>>,
    /// The pool it spends, `0x` hex.
    pub(super) pool: String,
    /// The delegate key, EIP-55 hex.
    pub(super) signer: String,
    /// Whether a pull stopped for funding on it.
    pub(super) spent: bool,
}

/// A capability registered to a held key.
struct Registered {
    grant: CapabilityGrant,
    /// The namespaces it pays for, or `None` for any request.
    namespaces: Option<Vec<u64>>,
    /// Its place in registration order.
    order: u64,
    /// Whether a pull stopped for funding on it.
    spent: bool,
}

/// A key a capability claimed, with every capability registered to it.
struct Delegate {
    signer: Arc<PrivateKeySigner>,
    /// At most one per pool.
    capabilities: Vec<Registered>,
}

/// The delegate keys a gateway holds.
#[derive(Default)]
pub(super) struct Delegates {
    /// The one key no capability claims yet.
    pending: Option<Arc<PrivateKeySigner>>,
    /// The keys capabilities claimed, by address.
    held: HashMap<Address, Delegate>,
    /// The order the next registration takes.
    next: u64,
}

impl Delegates {
    /// The pending key's address, created when there is none.
    pub(super) fn pending(&mut self) -> Address {
        self.pending
            .get_or_insert_with(|| Arc::new(PrivateKeySigner::random()))
            .address()
    }

    /// Whether `grant` would register: its key is one the gateway holds, and
    /// that key holds no capability from its pool. The check before the
    /// owner's signature is read on-chain.
    pub(super) fn admits(&self, grant: &CapabilityGrant) -> Result<(), Refusal> {
        if self
            .pending
            .as_ref()
            .is_some_and(|key| key.address() == grant.signer)
        {
            return Ok(());
        }
        let delegate = self.held.get(&grant.signer).ok_or(Refusal::UnknownKey)?;
        if delegate
            .capabilities
            .iter()
            .any(|r| r.grant.pool_id == grant.pool_id)
        {
            return Err(Refusal::PoolTaken);
        }
        Ok(())
    }

    /// Register `grant` to its key, scoped to `namespaces` or, with `None`,
    /// to any request. A pending key becomes held, and the next
    /// [`Self::pending`] creates a fresh one.
    pub(super) fn register(
        &mut self,
        grant: CapabilityGrant,
        namespaces: Option<Vec<u64>>,
    ) -> Result<(), Refusal> {
        self.admits(&grant)?;
        let registered = Registered {
            grant,
            namespaces,
            order: self.next,
            spent: false,
        };
        self.next = self.next.saturating_add(1);
        let signer = registered.grant.signer;
        if let Some(delegate) = self.held.get_mut(&signer) {
            delegate.capabilities.push(registered);
            return Ok(());
        }
        let key = self.pending.take().ok_or(Refusal::UnknownKey)?;
        self.held.insert(
            signer,
            Delegate {
                signer: key,
                capabilities: vec![registered],
            },
        );
        Ok(())
    }

    /// Drop every capability expired at `now`, and every held key left with
    /// none.
    pub(super) fn prune(&mut self, now: u64) {
        self.held.retain(|_, d| {
            d.capabilities.retain(|r| r.grant.expiry > now);
            !d.capabilities.is_empty()
        });
    }

    /// The capability the next pull pays with at `now`, for a request the
    /// pointer of `namespace` serves, or for any other request with `None`:
    /// among those that no funding stop retired and that are outside
    /// `margin_secs` of their expiry, the earliest registered scoped to
    /// `namespace`, else the earliest registered unscoped one.
    pub(super) fn paying(
        &self,
        now: u64,
        margin_secs: u64,
        namespace: Option<u64>,
    ) -> Option<Credential> {
        self.held
            .values()
            .flat_map(|d| d.capabilities.iter().map(move |r| (d, r)))
            .filter(|(_, r)| {
                !r.spent && !inside_capability_expiry_margin(r.grant.expiry, margin_secs, now)
            })
            .filter_map(|(d, r)| match &r.namespaces {
                None => Some((1u8, d, r)),
                Some(scope) => namespace
                    .is_some_and(|ns| scope.contains(&ns))
                    .then_some((0u8, d, r)),
            })
            .min_by_key(|(rank, _, r)| (*rank, r.order))
            .map(|(_, d, r)| Credential {
                signer: Arc::clone(&d.signer),
                grant: r.grant.clone(),
            })
    }

    /// Retire the capability `signer` holds from `pool` after a funding stop.
    /// It stays listed, as spent, until it expires.
    pub(super) fn retire(&mut self, signer: Address, pool: PoolId) {
        let registered = self
            .held
            .get_mut(&signer)
            .and_then(|d| d.capabilities.iter_mut().find(|r| r.grant.pool_id == pool));
        if let Some(registered) = registered {
            registered.spent = true;
        }
    }

    /// Every capability, in registration order.
    pub(super) fn list(&self) -> Vec<Capability> {
        let mut all: Vec<_> = self
            .held
            .values()
            .flat_map(|d| d.capabilities.iter())
            .collect();
        all.sort_by_key(|r| r.order);
        all.into_iter()
            .map(|r| Capability {
                cap: r.grant.spending_cap,
                expiry: r.grant.expiry,
                namespaces: r.namespaces.clone(),
                pool: r.grant.pool_id.to_string(),
                signer: r.grant.signer.to_string(),
                spent: r.spent,
            })
            .collect()
    }

    /// The `(pool, signer)` of every capability, whose lanes `/_status` reads.
    pub(super) fn lanes(&self) -> Vec<(PoolId, Address)> {
        self.held
            .values()
            .flat_map(|d| d.capabilities.iter())
            .map(|r| (r.grant.pool_id, r.grant.signer))
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
