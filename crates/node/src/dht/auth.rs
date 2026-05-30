//! The authenticated-identity trust boundary for the DHT handler.
//!
//! [`AuthenticatedNodeId`] wraps a [`NodeId`] that originated from a live QUIC
//! connection's `remote_id()` — i.e. an identity iroh's handshake has bound to
//! the connection, not one a peer asserted in a wire message body. It is
//! deliberately the *only* way to obtain a `NodeId` the handler trusts as "who
//! is actually on the other end".
//!
//! # Why this type exists
//!
//! ADR 022's STORE/FIND flows hinge on distinguishing two superficially
//! identical 32-byte values:
//!
//! - the **authenticated** peer id (`conn.remote_id()`), and
//! - **wire-provided** ids a peer puts in a request body (`StoreRequest.holder`,
//!   `FindNodeRequest.requester`, …).
//!
//! Conflating them is the routing-table-poisoning bug class caught in PR #643:
//! inserting `req.requester` into the routing table lets an authenticated peer
//! seed arbitrary (unreachable) ids it does not own. With this type the handler
//! signatures that update trusted state — [`super::routing::RoutingTable`]
//! insertion via `note_peer_seen`, and the `holder == authenticated id` checks
//! — take an `AuthenticatedNodeId`, so passing a wire value there is a *compile*
//! error. Extracting the inner id requires an explicit
//! [`AuthenticatedNodeId::node_id`] call,
//! which documents exactly where trust is being asserted.
//!
//! There is intentionally **no** `From<NodeId>` / `From<[u8; 32]>` impl, and the
//! only constructor takes a live [`Connection`] (not a bare `PublicKey`, which a
//! caller could mint from arbitrary bytes): the type cannot be fabricated from
//! wire data.
//!
//! # Scope
//!
//! This guards the *handler's direct trust decisions* — the per-request routing
//! refresh (`note_peer_seen`) and the `holder` checks — against substituting a
//! request-body id for the connected peer. It does **not** mean every routing
//! insert requires authentication: the iterative-lookup / bucket-refresh /
//! bootstrap paths legitimately learn peers from `FindNode` *responses* and
//! insert those plain [`NodeId`]s as probe candidates (Kademlia by design,
//! bounded by `MAX_CLOSER_NODES`-capped `closer_nodes` and the downstream
//! active-staker filter). The boundary closes the requester/holder-substitution
//! class specifically.

use iroh::endpoint::Connection;

use super::routing::NodeId;

/// A [`NodeId`] proven by the QUIC handshake to be the connected peer's
/// identity. Constructible only from an authenticated [`Connection`] — see the
/// module docs for the trust rationale.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct AuthenticatedNodeId(NodeId);

impl AuthenticatedNodeId {
    /// Lift the authenticated identity of a live QUIC connection into the trust
    /// boundary. Reads `conn.remote_id()` — the key iroh's handshake bound to
    /// the connection — so there is no way to inject a peer-chosen id (a
    /// `Connection` cannot be constructed with an arbitrary `remote_id`; only
    /// the handshake sets it).
    #[must_use]
    pub fn from_connection(conn: &Connection) -> Self {
        Self(NodeId::from_bytes(*conn.remote_id().as_bytes()))
    }

    /// The underlying [`NodeId`]. Calling this is the explicit, greppable
    /// point at which an authenticated identity is compared against, or
    /// inserted alongside, wire-provided ids.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.0
    }
}
