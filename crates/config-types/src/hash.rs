//! BLAKE3 content-address newtype and the operator-pinned-hash set.
//!
//! [`struct@Hash`] is a plain 32-byte container — it does *not* compute digests
//! (that needs `blake3`, which stays where blobs are handled). Its serde
//! and `Display`/`FromStr` forms are the lowercase 64-char hex string,
//! byte-identical to `iroh_blobs::Hash`'s hex form, so admin JSON-RPC and
//! config TOML stay wire-compatible after the #578 extraction.

use std::collections::HashSet;
use std::collections::hash_set::Iter;
use std::str::FromStr;
use std::sync::Arc;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const HEX: &[u8; 16] = b"0123456789abcdef";

/// 32-byte BLAKE3 content address. The serde representation is the
/// lowercase 64-char hex string — byte-identical to the form
/// `iroh_blobs::Hash` produces, so admin JSON-RPC and config TOML stay
/// wire-compatible. `decdn-cache` converts between this and
/// `iroh_blobs::Hash` at its public boundary (both are the same 32-byte
/// digest).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash([u8; 32]);

impl Hash {
    /// Wrap a raw 32-byte BLAKE3 digest.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the raw 32-byte digest.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Consume into the raw 32-byte digest.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Lowercase 64-char hex encoding. Panic-free: the nibble→char
    /// lookup is bounded by construction and falls back to `'0'` rather
    /// than indexing-panic if the table were ever shortened.
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for byte in &self.0 {
            let hi = usize::from(byte >> 4);
            let lo = usize::from(byte & 0x0f);
            s.push(HEX.get(hi).map_or('0', |c| char::from(*c)));
            s.push(HEX.get(lo).map_or('0', |c| char::from(*c)));
        }
        s
    }
}

/// Error returned when a string is not a valid 64-char lowercase-or-mixed
/// hex BLAKE3 digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashParseError {
    /// Input was not exactly 64 hex characters.
    BadLength {
        /// The actual length received.
        got: usize,
    },
    /// Input contained a non-hex character.
    NonHexChar,
}

impl std::fmt::Display for HashParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadLength { got } => {
                write!(f, "expected 64 hex characters, got {got}")
            }
            Self::NonHexChar => f.write_str("input contained a non-hex character"),
        }
    }
}

impl std::error::Error for HashParseError {}

const fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

impl FromStr for Hash {
    type Err = HashParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 64 {
            return Err(HashParseError::BadLength { got: s.len() });
        }
        let mut out = [0u8; 32];
        // `out.iter_mut()` yields exactly 32 slots; the `len == 64` check
        // above means `chunks_exact(2)` yields exactly 32 pairs with no
        // remainder. `zip` pairs them 1:1 — every output byte is written
        // exactly once, with no indexing, no `get_mut`, and no
        // write-only-if-present branch that could silently leave a slot
        // zeroed if the invariants ever drifted.
        for (slot, pair) in out.iter_mut().zip(s.as_bytes().chunks_exact(2)) {
            let (Some(hi), Some(lo)) = (
                pair.first().copied().and_then(hex_val),
                pair.get(1).copied().and_then(hex_val),
            ) else {
                return Err(HashParseError::NonHexChar);
            };
            *slot = (hi << 4) | lo;
        }
        Ok(Self(out))
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Write hex straight to the formatter — no per-call `String`
        // allocation. `{:02x}` is lowercase zero-padded, byte-identical
        // to `to_hex()`/the serde form (and to `iroh_blobs::Hash`),
        // which the `hash_bridge_tests` wire-compat test still pins.
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Serialize for Hash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

/// Operator-pinned blob hashes (#276). Hashes here are excluded from the
/// LRU eviction-candidate snapshot. Constructing this type is the only
/// way to feed pinned hashes into `CacheEngine::open_with_pinned` or
/// `CacheEngine::set_pinned`, so a future "blocklist" or similar
/// `HashSet<Hash>`-shaped feature can't be silently passed into the
/// pinning slot.
///
/// Held as `Arc<HashSet<Hash>>` internally so reload paths that swap the
/// active set don't need to clone the underlying map.
#[derive(Debug, Clone)]
pub struct PinnedHashes(Arc<HashSet<Hash>>);

impl PinnedHashes {
    /// Build a [`PinnedHashes`] from a freshly parsed set.
    #[must_use]
    pub fn new(set: HashSet<Hash>) -> Self {
        Self(Arc::new(set))
    }

    /// The empty pinned set.
    #[must_use]
    pub fn empty() -> Self {
        Self(Arc::new(HashSet::new()))
    }

    /// Number of pinned hashes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Is the pinned set empty?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Is `hash` pinned?
    #[must_use]
    pub fn contains(&self, hash: &Hash) -> bool {
        self.0.contains(hash)
    }

    /// Iterate over the pinned hashes.
    pub fn iter(&self) -> Iter<'_, Hash> {
        self.0.iter()
    }

    /// Compute counts of additions / removals between `prev` (older
    /// snapshot) and `self` (newer). Used by the SIGHUP reload path to
    /// log a diff line — operators pin/unpin individual hashes and want
    /// to see the delta in the success log without scraping the full set.
    #[must_use]
    pub fn diff(&self, prev: &Self) -> PinDiff {
        let added = self.0.iter().filter(|h| !prev.0.contains(*h)).count();
        let removed = prev.0.iter().filter(|h| !self.0.contains(*h)).count();
        PinDiff { added, removed }
    }
}

impl<'a> IntoIterator for &'a PinnedHashes {
    type Item = &'a Hash;
    type IntoIter = Iter<'a, Hash>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl Default for PinnedHashes {
    fn default() -> Self {
        Self::empty()
    }
}

/// Cheap diff of two [`PinnedHashes`] snapshots, for the reload log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinDiff {
    /// Number of hashes present in the new set but not the old.
    pub added: usize,
    /// Number of hashes present in the old set but not the new.
    pub removed: usize,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips() {
        let h = Hash::from_bytes([0xab; 32]);
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c == 'a' || c == 'b'));
        let back: Hash = hex.parse().expect("round-trip");
        assert_eq!(h, back);
    }

    #[test]
    fn hex_is_lowercase_and_full_width() {
        // Leading zero byte must still produce two chars (no trimming).
        let mut bytes = [0u8; 32];
        if let Some(last) = bytes.last_mut() {
            *last = 0x0f;
        }
        let h = Hash::from_bytes(bytes);
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.starts_with("00"));
        assert!(hex.ends_with("0f"));
    }

    #[test]
    fn serde_is_lowercase_hex_string() {
        let h = Hash::from_bytes([0xab; 32]);
        let json = serde_json::to_string(&h).expect("serialise");
        assert_eq!(json, format!("\"{}\"", "ab".repeat(32)));
        let back: Hash = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(h, back);
    }

    #[test]
    fn from_str_accepts_mixed_case() {
        let lower = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
        let upper = lower.to_uppercase();
        assert_eq!(
            lower.parse::<Hash>().expect("lower"),
            upper.parse::<Hash>().expect("upper")
        );
    }

    #[test]
    fn from_str_rejects_bad_length() {
        assert_eq!(
            "abcd".parse::<Hash>().unwrap_err(),
            HashParseError::BadLength { got: 4 }
        );
        assert!(matches!(
            "".parse::<Hash>(),
            Err(HashParseError::BadLength { got: 0 })
        ));
    }

    #[test]
    fn from_str_rejects_non_hex() {
        let bad = "z".repeat(64);
        assert_eq!(bad.parse::<Hash>().unwrap_err(), HashParseError::NonHexChar);
    }

    #[test]
    fn pinned_hashes_diff_counts_added_and_removed() {
        // Direct unit test of the diff helper, independent of any engine
        // swap path. Locks the API: a future caller stitching log
        // messages from `PinDiff` shouldn't break silently if the
        // counting changes shape.
        let h1 = Hash::from_bytes([1; 32]);
        let h2 = Hash::from_bytes([2; 32]);
        let h3 = Hash::from_bytes([3; 32]);

        let prev = PinnedHashes::new([h1, h2].into_iter().collect());
        let new = PinnedHashes::new([h2, h3].into_iter().collect());

        let diff = new.diff(&prev);
        assert!(diff.added == 1 && diff.removed == 1, "got {diff:?}");

        let no_change = new.diff(&new);
        assert!(no_change.added == 0 && no_change.removed == 0);
    }

    #[test]
    fn pinned_hashes_empty_and_contains() {
        let empty = PinnedHashes::empty();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        let h = Hash::from_bytes([7; 32]);
        let set = PinnedHashes::new([h].into_iter().collect());
        assert!(set.contains(&h));
        assert_eq!(set.len(), 1);
    }
}
