use super::held_seed_ranges;
use bao_tree::{ChunkNum, ChunkRanges};
use decdn_bao_range::CHUNK_GROUP_BYTES;

/// Chunk-group `g`'s chunk range (16 KiB groups of 1 KiB chunks).
fn group(g: u64) -> ChunkRanges {
    let per = CHUNK_GROUP_BYTES / 1024;
    ChunkRanges::from(ChunkNum(g * per)..ChunkNum((g + 1) * per))
}

#[test]
fn a_held_range_outside_the_request_is_not_seeded() {
    let total = 8 * CHUNK_GROUP_BYTES;
    let present = &group(0) | &group(6);
    // The request covers groups 2-3 only; neither held group is in it.
    let seeded = held_seed_ranges(
        &present,
        2 * CHUNK_GROUP_BYTES,
        2 * CHUNK_GROUP_BYTES,
        total,
    );
    assert!(seeded.is_empty(), "seeded {seeded:?}");
}

#[test]
fn only_the_held_part_of_the_request_is_seeded() {
    let total = 8 * CHUNK_GROUP_BYTES;
    let present = &(&group(0) | &group(3)) | &group(6);
    // An unaligned request inside groups 2-4 widens to those whole groups.
    let seeded = held_seed_ranges(
        &present,
        2 * CHUNK_GROUP_BYTES + 100,
        2 * CHUNK_GROUP_BYTES,
        total,
    );
    assert_eq!(seeded, group(3));
}

#[test]
fn a_to_end_request_seeds_every_held_range_from_its_start() {
    let total = 8 * CHUNK_GROUP_BYTES;
    let present = &(&group(0) | &group(3)) | &group(6);
    let seeded = held_seed_ranges(&present, 2 * CHUNK_GROUP_BYTES, 0, total);
    assert_eq!(seeded, &group(3) | &group(6));
}

#[test]
fn a_to_end_request_seeds_a_held_partial_tail_group() {
    // The blob ends 100 bytes into group 5, so its last chunk is chunk 80.
    let total = 5 * CHUNK_GROUP_BYTES + 100;
    let tail = ChunkRanges::from(ChunkNum(80)..ChunkNum(81));
    let present = &group(0) | &tail;
    let seeded = held_seed_ranges(&present, 2 * CHUNK_GROUP_BYTES, 0, total);
    assert_eq!(seeded, tail);
}

#[test]
fn a_length_past_the_blob_end_clamps_to_it() {
    let total = 8 * CHUNK_GROUP_BYTES;
    let present = &group(0) | &group(7);
    let seeded = held_seed_ranges(
        &present,
        6 * CHUNK_GROUP_BYTES,
        10 * CHUNK_GROUP_BYTES,
        total,
    );
    assert_eq!(seeded, group(7));
}

#[test]
fn a_request_at_or_past_the_blob_end_seeds_nothing() {
    let total = 8 * CHUNK_GROUP_BYTES;
    for offset in [total, total + CHUNK_GROUP_BYTES] {
        let seeded = held_seed_ranges(&group(7), offset, 0, total);
        assert!(seeded.is_empty(), "offset {offset} seeded {seeded:?}");
    }
}
