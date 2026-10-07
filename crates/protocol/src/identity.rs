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
            /// Wrap raw bytes. `const` to keep the newtype usable in const
            /// contexts (and consistent with the sibling accessors below).
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

        /// Lowercase hex with no prefix — the same rendering as
        /// `iroh::PublicKey` and `iroh_blobs::Hash`, so a log or span field
        /// written from this type matches one written from the iroh type.
        impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.iter().try_for_each(|b| write!(f, "{b:02x}"))
            }
        }
    };
}

id_newtype_impls!(NodeId);
id_newtype_impls!(ContentHash);

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
