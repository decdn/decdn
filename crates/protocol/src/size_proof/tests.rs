use super::*;

#[test]
fn a_proof_at_the_bound_round_trips() {
    let proof = SizeProof::new(vec![7u8; MAX_SIZE_PROOF_BYTES]).unwrap();
    let bytes = postcard::to_allocvec(&proof).unwrap();
    let back: SizeProof = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(back, proof);
    assert_eq!(back.as_bytes().len(), MAX_SIZE_PROOF_BYTES);
}

#[test]
fn new_refuses_a_proof_past_the_bound() {
    let err = SizeProof::new(vec![0u8; MAX_SIZE_PROOF_BYTES + 1]).unwrap_err();
    assert_eq!(err.len, MAX_SIZE_PROOF_BYTES + 1);
}

/// A length prefix far past the bound, with no bytes behind it, is refused
/// with no allocation: postcard checks the prefix against the input it holds
/// before the visitor runs.
#[test]
#[allow(clippy::cast_possible_truncation)] // varint bytes are masked to 7 bits
fn decode_refuses_a_huge_declared_length() {
    // postcard varint of 2^40, then nothing.
    let mut bytes = Vec::new();
    let mut n: u64 = 1 << 40;
    while n >= 0x80 {
        bytes.push((n as u8) | 0x80);
        n >>= 7;
    }
    bytes.push(n as u8);
    let err = postcard::from_bytes::<SizeProof>(&bytes).unwrap_err();
    assert_eq!(err, postcard::Error::DeserializeUnexpectedEnd, "{err}");
}

/// A proof one byte past the bound, with every byte present, is refused by
/// the bound itself.
#[test]
fn decode_refuses_a_proof_past_the_bound() {
    // Encode the raw vector with the same layout as a `SizeProof`: one
    // length-prefixed byte vector.
    let long = vec![0u8; MAX_SIZE_PROOF_BYTES + 1];
    let bytes = postcard::to_allocvec(&long).unwrap();
    let err = postcard::from_bytes::<SizeProof>(&bytes).unwrap_err();
    assert_eq!(err, postcard::Error::SerdeDeCustom, "{err}");
}
