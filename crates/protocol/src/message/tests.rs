use super::*;

#[test]
fn empty_root_is_the_blake3_hash_of_zero_bytes() {
    assert_eq!(EMPTY_ROOT, *blake3::hash(&[]).as_bytes());
}
