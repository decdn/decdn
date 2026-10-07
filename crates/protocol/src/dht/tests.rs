use super::*;

/// A `NodeId` with every byte set to `b`.
fn nid(b: u8) -> NodeId {
    NodeId::from_bytes([b; 32])
}

/// A `ContentHash` with every byte set to `b`.
fn ch(b: u8) -> ContentHash {
    ContentHash::from_bytes([b; 32])
}

/// Wrap nodes in `CloserNodes`, asserting they fit the cap (test inputs do).
fn closer(nodes: Vec<NodeId>) -> CloserNodes {
    CloserNodes::try_new(nodes).expect("test closer_nodes within MAX_CLOSER_NODES")
}

/// A `Provider` holding `node` with a one-block-full `Coverage`.
fn provider(node: NodeId) -> Provider {
    Provider {
        node,
        coverage: Coverage::full(1),
    }
}

fn sample_find_value_request() -> FindValueRequest {
    FindValueRequest {
        hash: ch(0x11),
        requester: nid(0x22),
    }
}

fn sample_find_value_response() -> FindValueResponse {
    FindValueResponse {
        hash: ch(0x11),
        providers: vec![provider(nid(0x33)), provider(nid(0x44))],
        closer_nodes: closer(vec![nid(0x55)]),
    }
}

fn sample_store_request() -> StoreRequest {
    StoreRequest {
        hash: ch(0x66),
        holder: nid(0x77),
        coverage: Coverage::full(1),
    }
}

fn sample_store_ack() -> StoreAck {
    StoreAck {
        hash: ch(0x66),
        accepted: true,
    }
}

fn sample_find_node_request() -> FindNodeRequest {
    FindNodeRequest {
        target: nid(0x88),
        requester: nid(0x99),
    }
}

fn sample_find_node_response() -> FindNodeResponse {
    FindNodeResponse {
        target: nid(0x88),
        closer_nodes: closer(vec![nid(0xAA), nid(0xBB)]),
    }
}

fn sample_batch_store_request() -> BatchStoreRequest {
    BatchStoreRequest {
        entries: vec![(ch(0xCC), Coverage::full(1)), (ch(0xDD), Coverage::empty())],
        holder: nid(0xEE),
    }
}

fn sample_batch_store_ack() -> BatchStoreAck {
    BatchStoreAck {
        results: vec![true, false],
    }
}

#[test]
fn find_value_request_roundtrip() -> Result<(), postcard::Error> {
    let msg = sample_find_value_request();
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: FindValueRequest = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn find_value_response_roundtrip() -> Result<(), postcard::Error> {
    let msg = sample_find_value_response();
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: FindValueResponse = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn find_value_response_empty_providers_roundtrip() -> Result<(), postcard::Error> {
    let msg = FindValueResponse {
        hash: ch(0x11),
        providers: vec![],
        closer_nodes: closer(vec![nid(0x55)]),
    };
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: FindValueResponse = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn store_request_roundtrip() -> Result<(), postcard::Error> {
    let msg = sample_store_request();
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: StoreRequest = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn store_ack_roundtrip() -> Result<(), postcard::Error> {
    let msg = sample_store_ack();
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: StoreAck = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn store_ack_rejected_roundtrip() -> Result<(), postcard::Error> {
    let msg = StoreAck {
        hash: ch(0x66),
        accepted: false,
    };
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: StoreAck = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn find_node_request_roundtrip() -> Result<(), postcard::Error> {
    let msg = sample_find_node_request();
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: FindNodeRequest = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn find_node_response_roundtrip() -> Result<(), postcard::Error> {
    let msg = sample_find_node_response();
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: FindNodeResponse = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn dht_message_find_value_discriminant_is_zero() -> Result<(), postcard::Error> {
    let msg = DhtMessage::FindValue(sample_find_value_request());
    let bytes = postcard::to_allocvec(&msg)?;
    assert_eq!(bytes.first().copied(), Some(0u8));
    let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn dht_message_find_value_response_discriminant_is_one() -> Result<(), postcard::Error> {
    let msg = DhtMessage::FindValueResponse(sample_find_value_response());
    let bytes = postcard::to_allocvec(&msg)?;
    assert_eq!(bytes.first().copied(), Some(1u8));
    let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn dht_message_store_discriminant_is_two() -> Result<(), postcard::Error> {
    let msg = DhtMessage::Store(sample_store_request());
    let bytes = postcard::to_allocvec(&msg)?;
    assert_eq!(bytes.first().copied(), Some(2u8));
    let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn dht_message_store_ack_discriminant_is_three() -> Result<(), postcard::Error> {
    let msg = DhtMessage::StoreAck(sample_store_ack());
    let bytes = postcard::to_allocvec(&msg)?;
    assert_eq!(bytes.first().copied(), Some(3u8));
    let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn dht_message_batch_store_discriminant_is_four() -> Result<(), postcard::Error> {
    let msg = DhtMessage::BatchStore(sample_batch_store_request());
    let bytes = postcard::to_allocvec(&msg)?;
    assert_eq!(bytes.first().copied(), Some(4u8));
    let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn dht_message_batch_store_ack_discriminant_is_five() -> Result<(), postcard::Error> {
    let msg = DhtMessage::BatchStoreAck(sample_batch_store_ack());
    let bytes = postcard::to_allocvec(&msg)?;
    assert_eq!(bytes.first().copied(), Some(5u8));
    let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn dht_message_find_node_discriminant_is_six() -> Result<(), postcard::Error> {
    let msg = DhtMessage::FindNode(sample_find_node_request());
    let bytes = postcard::to_allocvec(&msg)?;
    assert_eq!(bytes.first().copied(), Some(6u8));
    let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn dht_message_find_node_response_discriminant_is_seven() -> Result<(), postcard::Error> {
    let msg = DhtMessage::FindNodeResponse(sample_find_node_response());
    let bytes = postcard::to_allocvec(&msg)?;
    assert_eq!(bytes.first().copied(), Some(7u8));
    let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

// Pins `TopLevelEnum::VARIANT_COUNT` to the highest discriminant so a future
// variant addition must update the count the ADR 013 unknown/known
// classifier relies on.
#[test]
fn dht_message_variant_count_matches_discriminants() -> Result<(), postcard::Error> {
    use crate::framing::TopLevelEnum;
    assert_eq!(DhtMessage::VARIANT_COUNT, 8);
    // The last declared variant (`FindNodeResponse`) must encode to
    // discriminant VARIANT_COUNT - 1. Compare against postcard's own varint
    // encoding of that index (not `bytes.first()`) so the pin survives a
    // future multi-byte discriminant (> 127 variants).
    let last = DhtMessage::FindNodeResponse(sample_find_node_response());
    let bytes = postcard::to_allocvec(&last)?;
    let expected_disc = postcard::to_allocvec(&(DhtMessage::VARIANT_COUNT - 1))?;
    assert!(bytes.starts_with(&expected_disc));
    Ok(())
}

#[test]
fn dht_message_unknown_discriminant_is_flagged_unsupported() {
    // Discriminant 8 is the first index past the known set → UNSUPPORTED.
    assert!(crate::is_unknown_variant::<DhtMessage>(&[8u8, 0, 0]));
    // A known in-range discriminant (7) with a bad payload stays MALFORMED.
    assert!(!crate::is_unknown_variant::<DhtMessage>(&[7u8, 0xFF]));
}

#[test]
fn batch_store_request_roundtrip() -> Result<(), postcard::Error> {
    let msg = sample_batch_store_request();
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: BatchStoreRequest = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn batch_store_ack_roundtrip() -> Result<(), postcard::Error> {
    let msg = sample_batch_store_ack();
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: BatchStoreAck = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn dht_message_rejects_unknown_discriminant() {
    // Discriminant 99 has no matching variant — silent-drop is a handler
    // concern; at the type level postcard surfaces a decode error.
    let bytes = [99u8, 0, 0, 0, 0];
    let r: Result<DhtMessage, _> = postcard::from_bytes(&bytes);
    assert!(r.is_err());
}

// ADR 022 §DHT Bandwidth Analysis pins the max FindValueResponse at
// ~2.3 KB on the wire (50 providers × 32 B + 20 closer_nodes × 32 B +
// framing). We measure the **fully wrapped** `DhtMessage` payload (one
// discriminant byte for the outer enum + the inner struct) PLUS the
// length-prefix varint added by `crate::framing::write_frame` so the
// bound matches the operator-observable wire shape — measuring only
// the inner struct would let drift creep in via discriminant or
// framing-layer changes.
#[test]
fn find_value_response_max_framed_size_under_ceiling() -> Result<(), postcard::Error> {
    let inner = FindValueResponse {
        hash: ch(0xFF),
        providers: (0..MAX_PROVIDERS_PER_HASH)
            .map(|_| provider(nid(0xCD)))
            .collect(),
        closer_nodes: closer(vec![nid(0xCD); MAX_CLOSER_NODES]),
    };
    let msg = DhtMessage::FindValueResponse(inner.clone());
    let payload = postcard::to_allocvec(&msg)?;
    // Account for the framing varint length prefix the writer prepends.
    // postcard `to_allocvec(&u32)` produces the same varint layout, so
    // its length is the right proxy for the prefix's byte count.
    let payload_len_u32: u32 =
        u32::try_from(payload.len()).expect("payload length must fit in u32 for framing varint");
    let length_prefix_len = postcard::to_allocvec(&payload_len_u32)?.len();
    let framed_len = payload.len() + length_prefix_len;
    // 1 (enum discriminant) + 32 (hash) + 1 (providers len varint, 50 fits
    // 1B) + 50×34 (providers: 32B `NodeId` + 2B `Coverage` — a
    // single-block-covering `Coverage` encodes as a 1-byte varint length
    // + 1 payload byte) = 1700 + 1 (closer len varint, 20 fits 1B) +
    // 20×32 (closer) = 640 → 1 + 32 + 1 + 1700 + 1 + 640 = 2375 B inner;
    // + ~2B varint length prefix = ~2377 B on the wire. Range-keyed
    // discovery (this module's `Provider.coverage` field) grew the
    // per-provider cost from 32B to 34B over the plain-`NodeId` shape
    // the original 2.3 KB ADR 022 ceiling assumed; round up to 2.45 KB.
    assert!(
        framed_len <= 2_450,
        "framed max DhtMessage::FindValueResponse = {framed_len} B exceeds the 2.45 KB ceiling"
    );
    // Lower-bound guard: a future regression that drops `providers` or
    // `closer_nodes` from the wire shape would shrink the encoded
    // size well below the modeled payload (~2.38 KB) without failing
    // the upper-bound assert. Pin the lower bound at 2.3 KB so any
    // accidental field removal trips the test loudly.
    assert!(
        framed_len > 2_300,
        "framed max DhtMessage::FindValueResponse shrank to {framed_len} B \
         — has the wire shape lost providers/closer_nodes/coverage?"
    );
    // Sanity: round-trips through the outer enum.
    let decoded: DhtMessage = postcard::from_bytes(&payload)?;
    assert_eq!(decoded, msg);
    Ok(())
}

#[test]
fn find_value_response_decode_rejects_oversize_providers() -> Result<(), postcard::Error> {
    // Serialize a response whose `providers` field exceeds the cap.
    // Decoding must fail with the `deserialize_with` hook's error
    // rather than allocate the oversize Vec.
    let msg = FindValueResponse {
        hash: ch(0xFF),
        providers: (0..=MAX_PROVIDERS_PER_HASH)
            .map(|_| provider(nid(0xCD)))
            .collect(),
        closer_nodes: CloserNodes::default(),
    };
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: Result<FindValueResponse, _> = postcard::from_bytes(&bytes);
    assert!(
        decoded.is_err(),
        "providers length {} > MAX_PROVIDERS_PER_HASH must reject at decode",
        MAX_PROVIDERS_PER_HASH + 1
    );
    Ok(())
}

#[test]
fn bounded_vec_giant_length_prefix_truncated_body_does_not_overallocate() {
    // #845: pin the allocation-safety assumption of `deserialize_bounded_vec`.
    // An adversarial peer can send a `providers` length prefix claiming
    // billions of elements with no element bytes behind it. Decoding must
    // return an error (EOF) promptly without pre-allocating a multi-GB Vec
    // from the untrusted length — serde's `cautious` capacity bounds the
    // up-front allocation and postcard streams elements, so the body runs
    // out before the cap check is ever reached. If this regressed to an
    // unbounded `with_capacity(len)`, this test would OOM rather than fail.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&[0xFFu8; 32]); // `hash` field
    // postcard LEB128 varint for u32::MAX (= 4_294_967_295) providers.
    bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    // No provider bytes follow: the body is truncated immediately.
    let decoded: Result<FindValueResponse, _> = postcard::from_bytes(&bytes);
    assert!(
        decoded.is_err(),
        "giant length prefix + truncated body must error at decode, not allocate"
    );
}

#[test]
fn find_value_response_decode_rejects_oversize_closer_nodes() -> Result<(), postcard::Error> {
    // `CloserNodes` cannot *hold* an over-cap list, so build the oversize
    // wire bytes directly from the field components. postcard encodes a
    // struct as the concatenation of its fields, so a `(hash, providers,
    // closer_nodes)` tuple produces bytes identical to an over-cap
    // `FindValueResponse` — exactly what an adversarial peer could send.
    let bytes = postcard::to_allocvec(&(
        [0xFFu8; 32],
        Vec::<[u8; 32]>::new(),
        vec![[0xCDu8; 32]; MAX_CLOSER_NODES + 1],
    ))?;
    let decoded: Result<FindValueResponse, _> = postcard::from_bytes(&bytes);
    assert!(decoded.is_err());
    Ok(())
}

#[test]
fn find_node_response_decode_rejects_oversize_closer_nodes() -> Result<(), postcard::Error> {
    // See the sibling test: hand-encode the over-cap wire form via a
    // `(target, closer_nodes)` tuple since `CloserNodes` rejects it.
    let bytes = postcard::to_allocvec(&([0u8; 32], vec![[0xCDu8; 32]; MAX_CLOSER_NODES + 1]))?;
    let decoded: Result<FindNodeResponse, _> = postcard::from_bytes(&bytes);
    assert!(decoded.is_err());
    Ok(())
}

#[test]
fn batch_store_request_decode_rejects_oversize_hashes() -> Result<(), postcard::Error> {
    let msg = BatchStoreRequest {
        entries: vec![(ch(0xCD), Coverage::empty()); MAX_BATCH_STORE_HASHES + 1],
        holder: nid(0),
    };
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: Result<BatchStoreRequest, _> = postcard::from_bytes(&bytes);
    assert!(
        decoded.is_err(),
        "ADR 022 §STORE Flow: BatchStore > 256 hashes must reject at decode"
    );
    Ok(())
}

#[test]
fn batch_store_ack_decode_rejects_oversize_results() -> Result<(), postcard::Error> {
    let msg = BatchStoreAck {
        results: vec![true; MAX_BATCH_STORE_HASHES + 1],
    };
    let bytes = postcard::to_allocvec(&msg)?;
    let decoded: Result<BatchStoreAck, _> = postcard::from_bytes(&bytes);
    assert!(decoded.is_err());
    Ok(())
}

// Caps are inclusive: exactly-MAX must round-trip cleanly to confirm
// the boundary check is on `>`, not `>=`.
#[test]
fn caps_accept_exactly_max() -> Result<(), postcard::Error> {
    let resp = FindValueResponse {
        hash: ch(0),
        providers: (0..MAX_PROVIDERS_PER_HASH)
            .map(|_| provider(nid(0)))
            .collect(),
        closer_nodes: closer(vec![nid(0); MAX_CLOSER_NODES]),
    };
    let bytes = postcard::to_allocvec(&resp)?;
    let decoded: FindValueResponse = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded.providers.len(), MAX_PROVIDERS_PER_HASH);
    assert_eq!(decoded.closer_nodes.len(), MAX_CLOSER_NODES);
    let batch = BatchStoreRequest {
        entries: vec![(ch(0), Coverage::empty()); MAX_BATCH_STORE_HASHES],
        holder: nid(0),
    };
    let bytes = postcard::to_allocvec(&batch)?;
    let decoded: BatchStoreRequest = postcard::from_bytes(&bytes)?;
    assert_eq!(decoded.entries.len(), MAX_BATCH_STORE_HASHES);
    Ok(())
}

// `CloserNodes::try_new` enforces the same cap as the decode path, but at
// construction. Pins the `>` boundary directly (decode tests cover the wire
// side; this covers the in-memory side the type's doc advertises).
#[test]
fn closer_nodes_try_new_enforces_cap() {
    // Exactly MAX is accepted.
    let at_cap = CloserNodes::try_new(vec![nid(0); MAX_CLOSER_NODES])
        .expect("exactly MAX_CLOSER_NODES must construct");
    assert_eq!(at_cap.len(), MAX_CLOSER_NODES);

    // One over MAX is rejected, and the error reports the offending length.
    let err = CloserNodes::try_new(vec![nid(0); MAX_CLOSER_NODES + 1])
        .expect_err("over-cap must be rejected at construction");
    assert_eq!(err.len, MAX_CLOSER_NODES + 1);

    // Empty is fine.
    assert!(
        CloserNodes::try_new(vec![])
            .expect("empty is valid")
            .is_empty()
    );
}

#[test]
fn dht_message_trailing_bytes_tolerated() -> Result<(), postcard::Error> {
    // ADR 013 Tier-1: extension bytes after the message are silently
    // tolerated by `take_from_bytes`. Confirms the DHT enum behaves the
    // same as ProbeMessage under unknown-extension data.
    let msg = DhtMessage::FindValue(sample_find_value_request());
    let mut bytes = postcard::to_allocvec(&msg)?;
    bytes.extend_from_slice(&[0xAAu8, 0xBB, 0xCC]);
    let (decoded, tail) = postcard::take_from_bytes::<DhtMessage>(&bytes)?;
    assert_eq!(decoded, msg);
    assert_eq!(tail, &[0xAAu8, 0xBB, 0xCC]);
    Ok(())
}
