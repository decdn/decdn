//! Guard the iroh-blobs-free leaf-crate constant against the real upstream
//! value. `decdn-cache` links both crates, so this is where the two can be
//! compared; `decdn-bao-range` and `decdn-client` cannot see iroh-blobs.
#[test]
fn leaf_block_size_matches_iroh_blobs() {
    assert_eq!(
        super::IROH_BLOCK_SIZE,
        iroh_blobs::store::IROH_BLOCK_SIZE,
        "decdn-bao-range IROH_BLOCK_SIZE diverged from iroh-blobs' canonical \
         block size — bao wire encoding is a protocol contract (ADR 038); \
         update the leaf-crate constant to match the iroh-blobs bump"
    );
}
