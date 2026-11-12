use super::*;

fn hash(n: u8) -> Hash {
    Hash::from_bytes([n; 32])
}

#[test]
fn keeps_gets_marks_and_drops_a_proof() {
    let memo = SizeProofMemo::new(4);
    memo.insert(hash(1), 10, Bytes::from_static(b"p"), false);
    let kept = memo.get(&hash(1)).unwrap();
    assert_eq!(kept.size, 10);
    assert_eq!(kept.proof.as_ref(), b"p");
    assert!(!kept.stored);
    memo.mark_stored(&hash(1));
    assert!(memo.get(&hash(1)).unwrap().stored);
    memo.remove(&hash(1));
    assert!(memo.get(&hash(1)).is_none());
}

#[test]
fn the_oldest_proof_goes_when_full() {
    let memo = SizeProofMemo::new(2);
    memo.insert(hash(1), 1, Bytes::new(), false);
    memo.insert(hash(2), 2, Bytes::new(), false);
    memo.insert(hash(3), 3, Bytes::new(), false);
    assert!(memo.get(&hash(1)).is_none());
    assert!(memo.get(&hash(2)).is_some());
    assert!(memo.get(&hash(3)).is_some());
}

/// A stale queue position (the hash was removed, then inserted again) does
/// not evict the fresh entry: the oldest live entry goes instead.
#[test]
fn a_reinserted_proof_keeps_its_new_place() {
    let memo = SizeProofMemo::new(2);
    memo.insert(hash(0xA), 1, Bytes::new(), false);
    memo.remove(&hash(0xA));
    memo.insert(hash(0xB), 2, Bytes::new(), false);
    memo.insert(hash(0xC), 3, Bytes::new(), false);
    memo.insert(hash(0xA), 4, Bytes::new(), false);
    assert!(memo.get(&hash(0xB)).is_none(), "B is the oldest live entry");
    assert!(memo.get(&hash(0xC)).is_some());
    assert_eq!(memo.get(&hash(0xA)).map(|e| e.size), Some(4));
}

#[test]
fn churn_keeps_the_order_queue_bounded() {
    let memo = SizeProofMemo::new(2);
    for n in 0..200u8 {
        memo.insert(hash(n), u64::from(n), Bytes::new(), false);
        memo.remove(&hash(n));
    }
    assert!(memo.queue_len() <= 5);
}
