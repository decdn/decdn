use super::*;

fn hash(n: u8) -> Hash {
    Hash::from_bytes([n; 32])
}

#[test]
fn keeps_gets_and_drops_a_proof() {
    let memo = SizeProofMemo::new(4);
    memo.insert(hash(1), 10, Bytes::from_static(b"p"));
    let kept = memo.get(&hash(1)).unwrap();
    assert_eq!(kept.size, 10);
    assert_eq!(kept.proof.as_ref(), b"p");
    memo.remove(&hash(1));
    assert!(memo.get(&hash(1)).is_none());
}

#[test]
fn the_oldest_proof_goes_when_full() {
    let memo = SizeProofMemo::new(2);
    memo.insert(hash(1), 1, Bytes::new());
    memo.insert(hash(2), 2, Bytes::new());
    memo.insert(hash(3), 3, Bytes::new());
    assert!(memo.get(&hash(1)).is_none());
    assert!(memo.get(&hash(2)).is_some());
    assert!(memo.get(&hash(3)).is_some());
}

#[test]
fn churn_keeps_the_order_queue_bounded() {
    let memo = SizeProofMemo::new(2);
    for n in 0..200u8 {
        memo.insert(hash(n), u64::from(n), Bytes::new());
        memo.remove(&hash(n));
    }
    assert!(memo.lock().order.len() <= 4);
}
