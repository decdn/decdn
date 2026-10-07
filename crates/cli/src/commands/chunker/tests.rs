use super::*;
use crate::commands::manifest::b3_hex_str;

// Deterministic pseudo-random bytes so chunk boundaries actually form.
fn pseudo(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn sizes() -> ChunkSizes {
    ChunkSizes::resolve(4 * 1024 * 1024, None, None).unwrap()
}

#[test]
fn whole_hash_and_total_match_direct_blake3() {
    let data = pseudo(1, 20 * 1024 * 1024);
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let cf = chunk_file(&data[..], &sizes(), |_h, b| {
        seen.push(b.to_vec());
        Ok(())
    })
    .unwrap();
    assert_eq!(cf.whole_hash, blake3::hash(&data));
    assert_eq!(cf.total_size, data.len() as u64);
    // sink saw the file in order, exactly once.
    assert_eq!(seen.concat(), data);
    // chunk sizes sum to total.
    assert_eq!(cf.chunks.iter().map(|c| c.size).sum::<u64>(), cf.total_size);
}

#[test]
fn deterministic_boundaries() {
    let data = pseudo(2, 20 * 1024 * 1024);
    let a = chunk_file(&data[..], &sizes(), |_h, _b| Ok(())).unwrap();
    let b = chunk_file(&data[..], &sizes(), |_h, _b| Ok(())).unwrap();
    let ha: Vec<_> = a.chunks.iter().map(|c| c.hash.clone()).collect();
    let hb: Vec<_> = b.chunks.iter().map(|c| c.hash.clone()).collect();
    assert_eq!(ha, hb);
    assert!(ha.len() >= 2, "expected multiple chunks, got {}", ha.len());
}

#[test]
fn shared_region_dedups_distinct_does_not() {
    // Two files sharing a big identical middle region share >=1 chunk hash;
    // a fully distinct file shares none.
    let shared = pseudo(3, 16 * 1024 * 1024);
    let mut f1 = pseudo(10, 4 * 1024 * 1024);
    f1.extend_from_slice(&shared);
    f1.extend_from_slice(&pseudo(11, 4 * 1024 * 1024));
    let mut f2 = pseudo(20, 4 * 1024 * 1024);
    f2.extend_from_slice(&shared);
    f2.extend_from_slice(&pseudo(21, 4 * 1024 * 1024));
    let distinct = pseudo(99, 24 * 1024 * 1024);
    let h = |d: &[u8]| -> std::collections::HashSet<String> {
        chunk_file(d, &sizes(), |_h, _b| Ok(()))
            .unwrap()
            .chunks
            .into_iter()
            .map(|c| c.hash)
            .collect()
    };
    let (s1, s2, sd) = (h(&f1), h(&f2), h(&distinct));
    assert!(
        s1.intersection(&s2).count() >= 1,
        "shared region should dedup"
    );
    assert_eq!(
        s1.intersection(&sd).count(),
        0,
        "distinct file should not dedup"
    );
    let _ = b3_hex_str; // keep the import used
}

#[test]
fn cdc_resyncs_past_a_length_shift() {
    // The shared region starts at different byte offsets in each file
    // because the prefixes differ in LENGTH (1 MiB vs 1 MiB + 7 bytes) and
    // in content. A fixed-size chunker would misalign every downstream
    // chunk boundary and share zero chunks; CDC re-syncs its
    // content-defined cut points once it re-enters the shared bytes, so
    // the interior and trailing chunks of `shared` still come out
    // identical in both files (only the chunk straddling the
    // prefix->shared seam differs).
    let shared = pseudo(3, 16 * 1024 * 1024);
    let mut f1 = pseudo(10, 1024 * 1024);
    f1.extend_from_slice(&shared);
    let mut f2 = pseudo(20, 1024 * 1024 + 7);
    f2.extend_from_slice(&shared);

    let h = |d: &[u8]| -> std::collections::HashSet<String> {
        chunk_file(d, &sizes(), |_h, _b| Ok(()))
            .unwrap()
            .chunks
            .into_iter()
            .map(|c| c.hash)
            .collect()
    };
    let (s1, s2) = (h(&f1), h(&f2));
    let shared_count = s1.intersection(&s2).count();
    assert!(
        shared_count >= 2,
        "CDC should re-sync past the length shift and share several \
         chunks of `shared`; got {shared_count} shared out of {} / {} \
         chunks",
        s1.len(),
        s2.len()
    );
    // The differing prefixes mean the files are not chunk-for-chunk
    // identical.
    assert!(s1 != s2, "prefixes differ, so the chunk sets must too");
}

#[test]
fn shared_head_divergent_tail_dedups_head_not_tail() {
    // A shared leading region followed by divergent tails: the head
    // chunks dedup (proving CDC finds the shared content), while each
    // file's tail produces chunks the other lacks (proving divergence
    // still yields distinct chunks, not spurious matches).
    let head = pseudo(5, 16 * 1024 * 1024);
    let mut f1 = head.clone();
    f1.extend_from_slice(&pseudo(30, 8 * 1024 * 1024));
    let mut f2 = head.clone();
    f2.extend_from_slice(&pseudo(40, 8 * 1024 * 1024));

    let h = |d: &[u8]| -> std::collections::HashSet<String> {
        chunk_file(d, &sizes(), |_h, _b| Ok(()))
            .unwrap()
            .chunks
            .into_iter()
            .map(|c| c.hash)
            .collect()
    };
    let (s1, s2) = (h(&f1), h(&f2));
    let shared_count = s1.intersection(&s2).count();
    assert!(
        shared_count >= 2,
        "shared head should dedup several chunks; got {shared_count}"
    );
    let f1_only: std::collections::HashSet<_> = s1.difference(&s2).cloned().collect();
    let f2_only: std::collections::HashSet<_> = s2.difference(&s1).cloned().collect();
    assert!(
        !f1_only.is_empty() && !f2_only.is_empty(),
        "divergent tails should each produce chunks the other file lacks \
         (f1_only={}, f2_only={})",
        f1_only.len(),
        f2_only.len()
    );
}

#[test]
fn empty_input_yields_empty_chunks_and_empty_hash() {
    let cf = chunk_file(&[][..], &sizes(), |_h, _b| Ok(())).unwrap();
    assert_eq!(cf.total_size, 0);
    assert!(cf.chunks.is_empty());
    assert_eq!(cf.whole_hash, blake3::hash(&[]));
}

#[test]
fn defaults_derive_rails_from_avg() {
    let s = ChunkSizes::resolve(4 * 1024 * 1024, None, None).unwrap();
    assert_eq!(s.min, 1024 * 1024);
    assert_eq!(s.avg, 4 * 1024 * 1024);
    assert_eq!(s.max, 8 * 1024 * 1024);
}

#[test]
fn rejects_non_power_of_two_avg() {
    assert!(ChunkSizes::resolve(3 * 1024 * 1024, None, None).is_err());
}

#[test]
fn rejects_min_below_one_mib() {
    assert!(ChunkSizes::resolve(4 * 1024 * 1024, Some(512 * 1024), None).is_err());
}

#[test]
fn rejects_avg_above_fastcdc_ceiling() {
    // 8 MiB avg exceeds AVERAGE_MAX (4 MiB).
    assert!(ChunkSizes::resolve(8 * 1024 * 1024, None, None).is_err());
}

#[test]
fn rejects_max_above_fastcdc_ceiling() {
    assert!(ChunkSizes::resolve(4 * 1024 * 1024, None, Some(32 * 1024 * 1024)).is_err());
}

#[test]
fn rejects_min_gt_avg_and_avg_gt_max() {
    assert!(ChunkSizes::resolve(2 * 1024 * 1024, Some(4 * 1024 * 1024), None).is_err());
    assert!(ChunkSizes::resolve(4 * 1024 * 1024, None, Some(2 * 1024 * 1024)).is_err());
}

#[test]
fn accepts_2mib_avg_floors_min_at_one_mib() {
    let s = ChunkSizes::resolve(2 * 1024 * 1024, None, None).unwrap();
    assert_eq!(
        (s.min, s.avg, s.max),
        (1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024)
    );
}
