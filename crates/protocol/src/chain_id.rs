//! The CAIP-2 chain identifier (ADR 005 § Pool identifier). It is the chain
//! part of the [`crate::PoolAccount`] that names a payment pool on the wire.
//!
//! A [`ChainId`] is a namespace, a colon and a reference, such as
//! `eip155:42161`. The namespace matches `[-a-z0-9]{3,8}` and the reference
//! matches `[-_a-zA-Z0-9]{1,32}`, so an id holds at most [`MAX_CHAIN_ID_LEN`]
//! bytes. Every constructor and the decoder check these rules, so a value of
//! this type is always well formed. An EVM chain uses the `eip155` namespace
//! and its EIP-155 chain id ([`ChainId::eip155`]).
//!
//! On the wire the id is a postcard string: a varint length and the ASCII
//! bytes.

use std::fmt;
use std::str::FromStr;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};

/// Largest encoded length of a [`ChainId`] in bytes: an 8-byte namespace, the
/// colon and a 32-byte reference.
pub const MAX_CHAIN_ID_LEN: usize = 41;

const NAMESPACE_LEN: std::ops::RangeInclusive<usize> = 3..=8;
const REFERENCE_LEN: std::ops::RangeInclusive<usize> = 1..=32;
const EIP155_NAMESPACE: &str = "eip155";

/// Why a string is not a CAIP-2 chain id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChainIdError {
    /// The string is longer than [`MAX_CHAIN_ID_LEN`] bytes.
    #[error("chain id is {len} bytes; a CAIP-2 chain id holds at most {MAX_CHAIN_ID_LEN}")]
    TooLong {
        /// The refused length in bytes.
        len: usize,
    },
    /// The string has no colon between a namespace and a reference.
    #[error("chain id has no `:` between namespace and reference")]
    MissingColon,
    /// The namespace is not 3 to 8 characters of `[-a-z0-9]`.
    #[error("chain id namespace must be 3-8 characters of [-a-z0-9]")]
    BadNamespace,
    /// The reference is not 1 to 32 characters of `[-_a-zA-Z0-9]`.
    #[error("chain id reference must be 1-32 characters of [-_a-zA-Z0-9]")]
    BadReference,
}

/// A CAIP-2 chain identifier, such as `eip155:42161` (ADR 005 § Pool
/// identifier).
///
/// The value is always well formed: [`ChainId::new`], [`FromStr`] and the
/// serde decoder refuse a string that breaks the CAIP-2 rules. It is `Copy`
/// and stores its bytes inline. Equality, ordering and hashing follow the
/// string.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChainId {
    /// Length of the id in `bytes`, at most [`MAX_CHAIN_ID_LEN`].
    len: u8,
    /// The ASCII id, zero past `len`.
    bytes: [u8; MAX_CHAIN_ID_LEN],
}

impl ChainId {
    /// Parse and check a CAIP-2 chain id.
    ///
    /// # Errors
    ///
    /// A [`ChainIdError`] that names the first rule the string breaks.
    pub fn new(id: &str) -> Result<Self, ChainIdError> {
        if id.len() > MAX_CHAIN_ID_LEN {
            return Err(ChainIdError::TooLong { len: id.len() });
        }
        let (namespace, reference) = id.split_once(':').ok_or(ChainIdError::MissingColon)?;
        if !NAMESPACE_LEN.contains(&namespace.len())
            || !namespace
                .bytes()
                .all(|b| b == b'-' || b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return Err(ChainIdError::BadNamespace);
        }
        if !REFERENCE_LEN.contains(&reference.len())
            || !reference
                .bytes()
                .all(|b| b == b'-' || b == b'_' || b.is_ascii_alphanumeric())
        {
            return Err(ChainIdError::BadReference);
        }
        Ok(Self::from_checked(id))
    }

    /// The id of an EVM chain: the `eip155` namespace and the EIP-155 chain
    /// id in decimal.
    #[must_use]
    pub fn eip155(chain_id: u64) -> Self {
        // `eip155:` and at most 20 decimal digits: 27 bytes of the allowed set.
        Self::from_checked(&format!("{EIP155_NAMESPACE}:{chain_id}"))
    }

    /// The EIP-155 chain id, when this is an `eip155` id whose reference is a
    /// canonical decimal `u64` (no sign, no leading zero).
    #[must_use]
    pub fn eip155_id(&self) -> Option<u64> {
        let reference = self
            .as_str()
            .strip_prefix(EIP155_NAMESPACE)?
            .strip_prefix(':')?;
        let canonical = reference.bytes().all(|b| b.is_ascii_digit())
            && (reference == "0" || !reference.starts_with('0'));
        if canonical {
            reference.parse().ok()
        } else {
            None
        }
    }

    /// The id as a string, such as `eip155:42161`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        let bytes = self.bytes.get(..usize::from(self.len)).unwrap_or_default();
        // Every constructor stores checked ASCII, so this never falls back.
        std::str::from_utf8(bytes).unwrap_or_default()
    }

    /// Store an id that already meets the CAIP-2 rules.
    fn from_checked(id: &str) -> Self {
        let mut bytes = [0u8; MAX_CHAIN_ID_LEN];
        let len = id.len().min(MAX_CHAIN_ID_LEN);
        if let (Some(dst), Some(src)) = (bytes.get_mut(..len), id.as_bytes().get(..len)) {
            dst.copy_from_slice(src);
        }
        // `len` is at most MAX_CHAIN_ID_LEN (41), so the conversion holds.
        let len = u8::try_from(len).unwrap_or(u8::MAX);
        Self { len, bytes }
    }
}

impl PartialOrd for ChainId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ChainId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl fmt::Debug for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ChainId").field(&self.as_str()).finish()
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ChainId {
    type Err = ChainIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl Serialize for ChainId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ChainId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ChainIdVisitor;

        impl Visitor<'_> for ChainIdVisitor {
            type Value = ChainId;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a CAIP-2 chain id such as `eip155:42161`")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<ChainId, E> {
                ChainId::new(v).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(ChainIdVisitor)
    }
}

#[cfg(test)]
mod tests;
