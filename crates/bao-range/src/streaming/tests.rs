use super::*;
use crate::{CHUNK_GROUP_BYTES, align_range, encode_verified_range};

// A blob spanning several chunk groups plus a partial final group, so the
// tree has real interior nodes (matches the crate-root tests' rationale).
const BLOB_SIZE: u64 = 5 * CHUNK_GROUP_BYTES + 123;

fn blob() -> Vec<u8> {
    // Non-constant bytes so a byte-shuffling bug can't hide behind an
    // all-zero/all-same blob.
    (0..BLOB_SIZE).map(|i| (i % 251) as u8).collect()
}

#[test]
fn streamed_headerless_matches_reference() {
    let data = blob();
    let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let outboard = Bytes::from(ob.data.clone());

    let aligned = align_range(0, 0, BLOB_SIZE).expect("align whole blob");
    let reference =
        encode_verified_range(root, &aligned, &data, outboard.clone()).expect("reference encode");
    let reference_headerless = reference.get(8..).expect("reference has 8-byte header");

    let mut buf = Vec::new();
    encode_whole_blob_headerless(root, BLOB_SIZE, outboard, &data[..], &mut buf)
        .expect("streaming encode");

    assert_eq!(buf, reference_headerless);
}

#[test]
fn corrupt_outboard_fails() {
    let data = blob();
    let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let mut corrupt = ob.data.clone();
    if let Some(byte) = corrupt.get_mut(0) {
        *byte ^= 0xff;
    }
    let outboard = Bytes::from(corrupt);

    let mut buf = Vec::new();
    let err = encode_whole_blob_headerless(root, BLOB_SIZE, outboard, &data[..], &mut buf)
        .expect_err("corrupt outboard must fail verification");
    assert!(matches!(err, RangeVerifyError::Verification { .. }));
}

#[test]
fn wrong_root_fails() {
    let data = blob();
    let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    let outboard = Bytes::from(ob.data.clone());
    let wrong_root = [0xABu8; 32];

    let mut buf = Vec::new();
    let err = encode_whole_blob_headerless(wrong_root, BLOB_SIZE, outboard, &data[..], &mut buf)
        .expect_err("wrong root must fail verification");
    assert!(matches!(err, RangeVerifyError::Verification { .. }));
}
