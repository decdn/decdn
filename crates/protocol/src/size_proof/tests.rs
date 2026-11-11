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

#[test]
fn decode_refuses_a_proof_past_the_bound() {
    // Encode the raw vector with the same layout as a `SizeProof`: one
    // length-prefixed byte vector.
    let long = vec![0u8; MAX_SIZE_PROOF_BYTES + 1];
    let bytes = postcard::to_allocvec(&long).unwrap();
    assert!(postcard::from_bytes::<SizeProof>(&bytes).is_err());
}
