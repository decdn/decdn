//! `NodeId → registry-published direct dial addresses` (ADR 001 § Node
//! Discovery), mirrored into an iroh [`MemoryLookup`] that the runtime registers
//! on the node's endpoint.
//!
//! The `CapacityBond` registry watcher keeps this directory in step with each
//! node's on-chain `multiaddrs`. With its lookup on the endpoint, every
//! `EndpointAddr::new(node_id)` dial (DHT RPCs, upstream probes and pulls,
//! waiver requests) resolves the registry addresses at once, alongside the
//! pkarr lookup that supplies the peer's home relay URL and the public IPs it
//! publishes (`node_publish_filter`). iroh sends the handshake to every known
//! path and keeps the direct path when the node is reachable.
//!
//! The addresses are self-attested and additive. A stale or wrong address
//! loses the path race to the relay, and the QUIC handshake authenticates the
//! node id, so an address cannot redirect a dial to a different node.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use iroh::address_lookup::MemoryLookup;
use iroh::{EndpointAddr, PublicKey};
use tracing::debug;

use crate::dht::chain_projection::{with_read, with_write};
use crate::dht::routing::NodeId;

/// Provenance tag on every address this directory hands iroh, so a connection's
/// path source reads as the registry in iroh's own diagnostics.
const PROVENANCE: &str = "capacity_bond_registry";

/// Lock label for [`with_read`] / [`with_write`] poison recovery logs.
const LABEL: &str = "chain dial-address directory";

/// The registry's `NodeId → direct dial addresses` projection and the iroh
/// lookup it feeds. Cloning shares both.
#[derive(Debug, Clone)]
pub struct DialAddrDirectory {
    addrs: Arc<RwLock<HashMap<NodeId, Vec<SocketAddr>>>>,
    lookup: MemoryLookup,
}

impl Default for DialAddrDirectory {
    fn default() -> Self {
        Self {
            addrs: Arc::default(),
            lookup: MemoryLookup::with_provenance(PROVENANCE),
        }
    }
}

impl DialAddrDirectory {
    /// The iroh address lookup this directory feeds. The runtime adds it to the
    /// node's endpoint.
    #[must_use]
    pub fn lookup(&self) -> MemoryLookup {
        self.lookup.clone()
    }

    /// The direct addresses held for `node`, or `None` when it has none.
    #[must_use]
    pub fn get(&self, node: &NodeId) -> Option<Vec<SocketAddr>> {
        with_read(&self.addrs, LABEL, |m| m.get(node).cloned())
    }

    /// Replace `node`'s addresses. An empty list removes the node.
    pub(crate) fn set(&self, node: NodeId, addrs: Vec<SocketAddr>) {
        with_write(&self.addrs, LABEL, |m| {
            if addrs.is_empty() {
                m.remove(&node);
                self.lookup_remove(&node);
            } else {
                self.lookup_set(&node, &addrs);
                m.insert(node, addrs);
            }
        });
    }

    /// Remove `node` and its addresses.
    pub(crate) fn remove(&self, node: &NodeId) {
        with_write(&self.addrs, LABEL, |m| {
            m.remove(node);
            self.lookup_remove(node);
        });
    }

    /// Swap in `next` wholesale. A node absent from `next`, or present with an
    /// empty list, leaves the lookup.
    pub(crate) fn replace_all(&self, mut next: HashMap<NodeId, Vec<SocketAddr>>) {
        next.retain(|_, addrs| !addrs.is_empty());
        with_write(&self.addrs, LABEL, |m| {
            for gone in m.keys().filter(|id| !next.contains_key(id)) {
                self.lookup_remove(gone);
            }
            for (id, addrs) in &next {
                self.lookup_set(id, addrs);
            }
            *m = next;
        });
    }

    fn lookup_set(&self, node: &NodeId, addrs: &[SocketAddr]) {
        let Some(pk) = public_key(node) else {
            return;
        };
        let target = addrs
            .iter()
            .fold(EndpointAddr::new(pk), |t, sock| t.with_ip_addr(*sock));
        self.lookup.set_endpoint_info(target);
    }

    fn lookup_remove(&self, node: &NodeId) {
        if let Some(pk) = public_key(node) {
            self.lookup.remove_endpoint_info(pk);
        }
    }
}

/// `node` as an iroh key, or `None` (logged) when the registry id is not a
/// valid ed25519 point. Such a node is undialable, so it needs no lookup entry.
fn public_key(node: &NodeId) -> Option<PublicKey> {
    PublicKey::from_bytes(node.as_bytes())
        .inspect_err(|e| debug!(%node, error = %e, "registry node id is not a valid public key"))
        .ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
