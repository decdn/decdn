use super::*;
use bao_tree::io::outboard::PreOrderMemOutboard;

/// The streaming encoder must produce byte-identical root and outboard to
/// the in-memory `PreOrderMemOutboard::create` the cache tests seed with, so
/// a blob written by `origin import` is one `origin::fs` reads back verbatim.
#[test]
fn encode_outboard_matches_in_memory_encoder() {
    let payload = (0..200u32)
        .flat_map(|i| std::iter::repeat_n((i & 0xff) as u8, 1024))
        .collect::<Vec<_>>();
    let size = u64::try_from(payload.len()).unwrap();

    let streamed = encode_outboard(payload.as_slice(), size).unwrap();

    let mem = PreOrderMemOutboard::create(&payload, IROH_BLOCK_SIZE);
    assert_eq!(streamed.hash_hex, mem.root.to_hex().to_string());
    assert_eq!(streamed.outboard, mem.data);
}

/// The root hash equals the plain BLAKE3 hash of the content — the bao tree
/// root is the content address, so the data object's file name matches what
/// `origin import --dry-run` records for the same file.
#[test]
fn encode_outboard_root_is_content_blake3() {
    let payload = vec![7u8; 40 * 1024];
    let size = u64::try_from(payload.len()).unwrap();
    let got = encode_outboard(payload.as_slice(), size).unwrap();
    assert_eq!(
        got.hash_hex,
        bao_tree::blake3::hash(&payload).to_hex().to_string()
    );
}

/// A zero-length blob has an empty outboard and still yields a stable hash.
#[test]
fn encode_outboard_empty_blob() {
    let got = encode_outboard([].as_slice(), 0).unwrap();
    assert_eq!(got.hash_hex.len(), 64);
    assert!(got.outboard.is_empty());
}

#[test]
fn shard_prefix_takes_first_two_hex_chars() {
    assert_eq!(shard_prefix("abcdef"), "ab");
    assert_eq!(shard_prefix("a"), "");
    assert_eq!(shard_prefix(""), "");
}

#[test]
fn obao4_name_appends_suffix() {
    let ob = EncodedOutboard {
        hash_hex: "ab".repeat(32),
        outboard: Vec::new(),
    };
    assert_eq!(ob.shard(), "ab");
    assert_eq!(ob.obao4_name(), format!("{}{OBAO4_SUFFIX}", ob.hash_hex));
}
