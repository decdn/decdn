//! Bao verified-range helpers — re-exported from the iroh-blobs-free
//! [`decdn_bao_range`] leaf crate (#578) so `decdn-client` can share the
//! exact same alignment / encoded-size / verify logic without linking
//! iroh-blobs. The serve (`engine::export_bao_range`), origin range encode
//! (`engine::origin_range_wire`), and client-receive paths therefore all agree
//! by construction. See [ADR 038 §Wire format](../../../adr/038-bao-verified-range-streaming.md).
//!
//! The block size [`IROH_BLOCK_SIZE`] is declared in the leaf crate from the
//! `bao-tree` primitive; the lock-step test below asserts it stays byte-identical
//! to `iroh_blobs::store::IROH_BLOCK_SIZE`, so an iroh-blobs bump that changed the
//! canonical block size — a protocol-contract change under ADR 038 — fails here.

pub use decdn_bao_range::{
    AlignedRange, IROH_BLOCK_SIZE, RangeVerifyError, align_range, bao_encoded_size,
    encode_verified_range,
};

#[cfg(test)]
mod lock_step {
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
}
