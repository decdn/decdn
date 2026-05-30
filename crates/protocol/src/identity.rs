//! Strongly-typed 32-byte identity newtypes for the deCDN wire protocol.
//!
//! [`NodeId`] (a 32-byte Ed25519 public key) and [`ContentHash`] (a 32-byte
//! BLAKE3 content address) are distinct types even though both wrap `[u8; 32]`.
//! Keeping them distinct turns a class of confusion — passing a content hash
//! where a node id is expected, or treating a wire-provided id as an
//! authenticated one — into a **compile error** rather than a silent logic bug
//! (see the routing-table-poisoning class behind ADR 022 §STORE Flow).
//!
//! # Wire compatibility
//!
//! Both newtypes are `#[repr(transparent)]` + `#[serde(transparent)]`, so they
//! lay out and (de)serialize byte-for-byte identically to a bare `[u8; 32]`.
//! Postcard sees the same 32 raw bytes — no discriminant moves, no ALPN bump.
//! `nodeid_postcard_identical_to_array` pins this invariant.
//!
//! # Trust boundary
//!
//! A plain [`NodeId`] is freely constructible from wire bytes *by design*:
//! Kademlia legitimately learns peer ids from `FindNode` responses and inserts
//! them as probe candidates. The authenticated-identity boundary is enforced
//! one layer up by `decdn_node::dht::AuthenticatedNodeId`, which can only be
//! minted from a live QUIC connection's `remote_id()`.

use serde::{Deserialize, Serialize};

/// Length in bytes of a [`NodeId`] or [`ContentHash`] — the size of both an
/// Ed25519 public key and a BLAKE3 digest.
pub const ID_LEN: usize = 32;

/// A 32-byte node identifier (Ed25519 public key).
///
/// Distinct from [`ContentHash`] at the type level; identical on the wire.
#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId([u8; ID_LEN]);

/// A 32-byte content address (BLAKE3 hash).
///
/// Distinct from [`NodeId`] at the type level; identical on the wire. Named
/// `ContentHash` (not `Hash`) to avoid colliding with `decdn_config_types::Hash`
/// and the `std::hash::Hash` trait.
#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentHash([u8; ID_LEN]);

macro_rules! id_newtype_impls {
    ($t:ty) => {
        impl $t {
            /// Wrap raw bytes. `const` so it is usable where the routing table
            /// is built in a `const fn` context.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; ID_LEN]) -> Self {
                Self(bytes)
            }

            /// Borrow the inner bytes. The explicit accessor (rather than a
            /// `Deref<Target = [u8; ID_LEN]>`) keeps panicking slice indexing
            /// off the type, which the workspace `indexing_slicing` lint forbids.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; ID_LEN] {
                &self.0
            }

            /// Consume into the inner bytes.
            #[must_use]
            pub const fn to_bytes(self) -> [u8; ID_LEN] {
                self.0
            }
        }

        impl From<[u8; ID_LEN]> for $t {
            fn from(bytes: [u8; ID_LEN]) -> Self {
                Self(bytes)
            }
        }

        impl From<$t> for [u8; ID_LEN] {
            fn from(value: $t) -> Self {
                value.0
            }
        }
    };
}

id_newtype_impls!(NodeId);
id_newtype_impls!(ContentHash);

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn nodeid_postcard_identical_to_array() -> Result<(), postcard::Error> {
        // `#[serde(transparent)]` must keep the wire bytes identical to a bare
        // `[u8; 32]` — otherwise this change would silently break the DHT ALPN.
        for raw in [[0u8; ID_LEN], [0xABu8; ID_LEN]] {
            let node_bytes = postcard::to_allocvec(&NodeId::from_bytes(raw))?;
            let hash_bytes = postcard::to_allocvec(&ContentHash::from_bytes(raw))?;
            let array_bytes = postcard::to_allocvec(&raw)?;
            assert_eq!(node_bytes, array_bytes);
            assert_eq!(hash_bytes, array_bytes);

            // ...and decode round-trips from the array encoding.
            let decoded: NodeId = postcard::from_bytes(&array_bytes)?;
            assert_eq!(decoded, NodeId::from_bytes(raw));
        }
        Ok(())
    }

    #[test]
    fn distinct_accessors_round_trip() {
        let raw = [7u8; ID_LEN];
        let id = NodeId::from_bytes(raw);
        assert_eq!(id.as_bytes(), &raw);
        assert_eq!(id.to_bytes(), raw);
        assert_eq!(<[u8; ID_LEN]>::from(id), raw);
        assert_eq!(NodeId::from(raw), id);
    }
}
