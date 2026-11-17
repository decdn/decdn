//! Which `PaymentPool` contract a fetch pays each node through (ADR 005 §
//! Payment pool routing).
//!
//! A client can pay through several contracts, on one or more payment chains.
//! A node accepts its own list of contracts and names them in the
//! `payment_pools` hint of its probe answer. [`PoolRoute`] picks one contract
//! for each node: the first contract in the client's preference order that the
//! node's hint lists. A node with no hint gets the first configured contract,
//! the one the node knows from the client's pinned configuration.
//!
//! A fetch pays through one contract at a time, because its acquire loop gates
//! every lane on one pool deposit. [`PoolPasses`] runs one pass for each
//! contract that at least one node is routed to, in the client's preference
//! order. A node that refuses a contract as `Declined` names the contracts it
//! accepts. The fetch tries that node once more, in the pass of the first
//! listed contract the client can pay through. A node gets one such retry in a
//! fetch, so a node that refuses every pass cannot keep the fetch running.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, PoisonError};

use alloy::primitives::Address;
use decdn_incentive::{BuyerPoolStore, Deployment};
use decdn_protocol::PoolAccount;

use crate::session::ChainConfig;

/// The client's `PaymentPool` contracts in preference order, and the rule
/// that picks one for each node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolRoute {
    /// The first configured contract: a node with no hint gets it.
    configured_first: Deployment,
    /// Every contract the client can pay through, preferred first.
    order: Vec<Deployment>,
}

impl PoolRoute {
    /// The route over `configured`, first configured first, with every
    /// contract `held` says the client already holds an open pool in moved
    /// ahead of the rest. Each group keeps its configured order, so the route
    /// is the same for the same configuration and store. `None` when
    /// `configured` is empty.
    #[must_use]
    pub fn new(configured: &[Deployment], held: impl Fn(Deployment) -> bool) -> Option<Self> {
        let configured_first = *configured.first()?;
        let (mut order, rest): (Vec<_>, Vec<_>) =
            configured.iter().copied().partition(|&d| held(d));
        order.extend(rest);
        Some(Self {
            configured_first,
            order,
        })
    }

    /// The route of a buyer over `chain`'s payment pools. An `owner` that pays
    /// from its own pools prefers each contract where `store` records a pool
    /// it owns. A delegated buyer (`owner` is `None`) pays only the first
    /// configured contract, where its capability's pool lives. A store that
    /// cannot be read prefers nothing.
    ///
    /// # Errors
    ///
    /// `chain` configures no payment pool.
    pub fn for_buyer(
        chain: &ChainConfig,
        store: &dyn BuyerPoolStore,
        owner: Option<Address>,
    ) -> anyhow::Result<Self> {
        let first = chain.first_pool()?.deployment;
        let Some(owner) = owner else {
            return Ok(Self::single(first));
        };
        Ok(Self::new(&chain.deployments(), |deployment| {
            matches!(store.get_by_owner(deployment, owner), Ok(Some(_)))
        })
        .unwrap_or_else(|| Self::single(first)))
    }

    /// The route over one contract.
    #[must_use]
    pub fn single(deployment: Deployment) -> Self {
        Self {
            configured_first: deployment,
            order: vec![deployment],
        }
    }

    /// Every contract of the route, preferred first.
    #[must_use]
    pub fn order(&self) -> &[Deployment] {
        &self.order
    }

    /// The contract a node with the `payment_pools` `hint` is paid through:
    /// the first contract of the route that `hint` lists, or the first
    /// configured contract when the node gave no hint. `None` when the hint
    /// lists no contract of the route.
    #[must_use]
    pub fn choose(&self, hint: Option<&[PoolAccount]>) -> Option<Deployment> {
        let Some(hint) = hint else {
            return Some(self.configured_first);
        };
        let listed = listed(hint);
        self.order.iter().copied().find(|d| listed.contains(d))
    }

    /// The contract a node that refused `refused` as `Declined` is tried
    /// again on: the first contract of the route, other than `refused`, that
    /// the node's `accepted` list names. `None` when it names none.
    #[must_use]
    pub fn retry(&self, refused: Deployment, accepted: &[PoolAccount]) -> Option<Deployment> {
        let listed = listed(accepted);
        self.order
            .iter()
            .copied()
            .find(|&d| d != refused && listed.contains(&d))
    }
}

/// The EVM deployments `accounts` names. An account in another namespace, or
/// one that is not an EVM contract address, names none.
fn listed(accounts: &[PoolAccount]) -> HashSet<Deployment> {
    accounts
        .iter()
        .filter_map(Deployment::from_pool_account)
        .collect()
}

/// The contract passes of one fetch: which contract each node is paid
/// through, and which contracts still have a node to pay.
#[derive(Debug)]
pub struct PoolPasses {
    route: PoolRoute,
    state: Mutex<PassState>,
}

/// The mutable part of [`PoolPasses`].
#[derive(Debug, Default)]
struct PassState {
    /// The contracts whose pass has started.
    started: HashSet<Deployment>,
    /// Each node seen in a pass of another contract, by provider, with the
    /// contract it is routed to.
    routed: HashMap<Address, Deployment>,
    /// Each node that refused a contract and named one the client can pay
    /// through, by provider, with that contract.
    retry: HashMap<Address, Deployment>,
    /// The nodes that already took their one retry, or refused with no
    /// contract left to retry on.
    retried: HashSet<Address>,
    /// Each node a pass pays, by provider, with that pass's contract, until
    /// the node refuses it.
    admitted: HashMap<Address, Deployment>,
    /// Each node, by provider, with each contract it refused.
    refused: HashSet<(Address, Deployment)>,
}

impl PoolPasses {
    /// The passes of a fetch over `route`.
    #[must_use]
    pub fn new(route: PoolRoute) -> Self {
        Self {
            route,
            state: Mutex::new(PassState::default()),
        }
    }

    /// The route the passes follow.
    #[must_use]
    pub const fn route(&self) -> &PoolRoute {
        &self.route
    }

    /// The contract of the fetch's first pass, given the `hints` of the
    /// holders known before it: the first contract of the route that one of
    /// them is routed to. With no holder routed anywhere, the first configured
    /// contract.
    pub fn first<'h>(
        &self,
        hints: impl IntoIterator<Item = Option<&'h [PoolAccount]>>,
    ) -> Deployment {
        let chosen: HashSet<Deployment> = hints
            .into_iter()
            .filter_map(|hint| self.route.choose(hint))
            .collect();
        self.route
            .order
            .iter()
            .copied()
            .find(|d| chosen.contains(d))
            .unwrap_or(self.route.configured_first)
    }

    /// Mark the pass on `deployment` as started. Each contract has one pass
    /// in a fetch.
    pub fn start(&self, deployment: Deployment) {
        self.lock().started.insert(deployment);
    }

    /// Whether the pass on `pass` pays the node at `provider`, whose probe
    /// answer gave `hint`. It does when the node is routed to `pass`, or when
    /// the node refused another contract and `pass` is its retry. A node
    /// routed to another contract is kept for that contract's pass. A node
    /// routed to no contract of the route, or that refused `pass`, is never
    /// paid in it.
    pub fn admits(
        &self,
        pass: Deployment,
        provider: Address,
        hint: Option<&[PoolAccount]>,
    ) -> bool {
        let mut state = self.lock();
        if state.refused.contains(&(provider, pass)) {
            return false;
        }
        if state.admitted.get(&provider) == Some(&pass) {
            return true;
        }
        if state.retry.get(&provider) == Some(&pass) {
            state.retry.remove(&provider);
            state.retried.insert(provider);
            state.admitted.insert(provider, pass);
            return true;
        }
        if state.retried.contains(&provider) {
            return false;
        }
        match self.route.choose(hint) {
            Some(chosen) if chosen == pass => {
                state.admitted.insert(provider, pass);
                true
            }
            Some(chosen) => {
                state.routed.insert(provider, chosen);
                false
            }
            None => false,
        }
    }

    /// Record that the node at `provider` refused the contract `refused` as
    /// `Declined` and named `accepted`. The node gets one retry, in the pass
    /// of the first contract of the route that `accepted` names and whose
    /// pass has not started. A node that already took its retry gets none.
    pub fn declined(&self, refused: Deployment, provider: Address, accepted: &[PoolAccount]) {
        let mut state = self.lock();
        state.admitted.remove(&provider);
        state.refused.insert((provider, refused));
        if state.retried.contains(&provider) {
            return;
        }
        state.routed.remove(&provider);
        let listed = listed(accepted);
        let retry = self
            .route
            .order
            .iter()
            .copied()
            .find(|d| *d != refused && listed.contains(d) && !state.started.contains(d));
        match retry {
            Some(deployment) => {
                state.retry.insert(provider, deployment);
            }
            None => {
                state.retried.insert(provider);
            }
        }
    }

    /// The contract of the next pass: the first contract of the route whose
    /// pass has not started and that a node is routed to or retried on.
    /// `None` when no such contract is left.
    #[must_use]
    pub fn next(&self) -> Option<Deployment> {
        let state = self.lock();
        self.route.order.iter().copied().find(|d| {
            !state.started.contains(d)
                && (state.routed.values().any(|r| r == d) || state.retry.values().any(|r| r == d))
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PassState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests;
