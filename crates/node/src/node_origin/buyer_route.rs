//! The `PaymentPool` this node's buyer leg pays its upstreams through (ADR 005
//! § Payment pool routing).
//!
//! The buyer leg pays through one pool: the first pool of the route over this
//! node's accepted pools, which prefers a pool where the buyer store already
//! holds the node's pool. An upstream is a candidate only when the route sends
//! it to that pool, by the rule a client applies
//! ([`decdn_client::pool_route::PoolRoute::choose`]).

use decdn_client::pool_route::PoolRoute;
use decdn_incentive::Deployment;
use decdn_protocol::PoolAccount;

/// The buyer leg's pool and the route that chose it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuyerPool {
    route: PoolRoute,
    deployment: Deployment,
}

impl BuyerPool {
    /// The buyer pool of a node that accepts `accepted`, first configured
    /// first: the first pool of the route that moves each pool `held` says
    /// the buyer already holds a pool in ahead of the rest. `None` when
    /// `accepted` is empty.
    #[must_use]
    pub fn choose(accepted: &[Deployment], held: impl Fn(Deployment) -> bool) -> Option<Self> {
        let route = PoolRoute::new(accepted, held)?;
        let deployment = *route.order().first()?;
        Some(Self { route, deployment })
    }

    /// The pool the buyer leg pays through.
    #[must_use]
    pub const fn deployment(&self) -> Deployment {
        self.deployment
    }

    /// Whether an upstream whose probe answer gave the `payment_pools` `hint`
    /// is paid through the buyer pool.
    #[must_use]
    pub fn pays(&self, hint: Option<&[PoolAccount]>) -> bool {
        self.route.choose(hint) == Some(self.deployment)
    }
}

#[cfg(test)]
mod tests;
