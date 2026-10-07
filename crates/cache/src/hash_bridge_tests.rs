//! The load-bearing #578 invariant: the `decdn-config-types` leaf
//! `Hash` (its hand-rolled hex codec + serde) is byte- and
//! hex-identical to `iroh_blobs::Hash`. `decdn-cache` is the only
//! crate that links *both* types, so this contract is pinned here.
//! Without this, a future change to the leaf hex codec would
//! silently make every operator's `cache.pinned_hashes` config
//! entry decode to the wrong 32 bytes — the pinned blob would not
//! be protected — with no other test failing.

use std::str::FromStr;

use super::{Hash as StoreHash, from_store_hash, to_store_hash};

/// Known-answer vector: BLAKE3 of the empty input. If the leaf hex
/// codec ever drifts (nibble order, casing, base32, a `0x` prefix),
/// this fails — exactly the operator-facing wire regression #578's
/// `Hash` extraction must never introduce.
const BLAKE3_EMPTY: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

#[test]
fn leaf_hash_hex_is_byte_identical_to_iroh_blobs_for_known_vector() {
    let store = StoreHash::new(b"");
    assert_eq!(
        store.to_string(),
        BLAKE3_EMPTY,
        "sanity: iroh_blobs hex of BLAKE3(\"\")"
    );
    let leaf = from_store_hash(store);
    // Leaf hex == iroh-blobs hex, and == the known vector.
    assert_eq!(leaf.to_hex(), BLAKE3_EMPTY);
    assert_eq!(leaf.to_hex(), store.to_string());
    // iroh-blobs can parse what the leaf produced, back to the
    // same store hash (operator config string → leaf → store).
    let reparsed = StoreHash::from_str(&leaf.to_hex()).expect("iroh parses leaf hex");
    assert_eq!(reparsed, store);
    // And the leaf parses iroh's hex form to the same bytes
    // (admin/JSON-RPC string → leaf → store match).
    let leaf_from_iroh_hex =
        decdn_config_types::Hash::from_str(&store.to_string()).expect("leaf parses iroh hex");
    assert_eq!(to_store_hash(leaf_from_iroh_hex), store);
}

#[test]
fn store_leaf_round_trip_is_lossless_both_directions() {
    for payload in [b"".as_slice(), b"pinned blob", &[0xff; 64]] {
        let store = StoreHash::new(payload);
        assert_eq!(
            to_store_hash(from_store_hash(store)),
            store,
            "store → leaf → store must be identity"
        );
        let leaf = from_store_hash(store);
        assert_eq!(
            from_store_hash(to_store_hash(leaf)),
            leaf,
            "leaf → store → leaf must be identity"
        );
    }
}
