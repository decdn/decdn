use super::*;

#[test]
fn display_is_lowercase_hex_without_prefix() {
    let mut raw = [0u8; ID_LEN];
    raw[0] = 0xAB;
    raw[ID_LEN - 1] = 0x0F;
    let expected = format!("ab{}0f", "00".repeat(ID_LEN - 2));
    assert_eq!(NodeId::from_bytes(raw).to_string(), expected);
    assert_eq!(ContentHash::from_bytes(raw).to_string(), expected);
}

#[test]
fn nodeid_postcard_identical_to_array() -> Result<(), postcard::Error> {
    // `#[serde(transparent)]` must keep the wire bytes identical to a bare
    // `[u8; 32]` — otherwise this change would silently break the DHT ALPN.
    for raw in [[0u8; ID_LEN], [0xABu8; ID_LEN]] {
        let node_bytes = postcard::to_allocvec(&NodeId::from_bytes(raw))?;
        let hash_bytes = postcard::to_allocvec(&ContentHash::from_bytes(raw))?;
        let array_bytes = postcard::to_allocvec(&raw)?;
        assert_eq!(node_bytes, array_bytes);
        assert_eq!(hash_bytes, array_bytes);

        // ...and both types decode from the bare-array encoding (the
        // no-ALPN-bump claim needs byte-identity in both directions for
        // both types — `ContentHash` is the type carried in `*.hash`).
        let decoded_node: NodeId = postcard::from_bytes(&array_bytes)?;
        assert_eq!(decoded_node, NodeId::from_bytes(raw));
        let decoded_hash: ContentHash = postcard::from_bytes(&array_bytes)?;
        assert_eq!(decoded_hash, ContentHash::from_bytes(raw));
    }
    Ok(())
}

#[test]
fn distinct_accessors_round_trip() {
    let raw = [7u8; ID_LEN];
    let id = NodeId::from_bytes(raw);
    assert_eq!(id.as_bytes(), &raw);
    assert_eq!(id.to_bytes(), raw);
    assert_eq!(<[u8; ID_LEN]>::from(id), raw);
    assert_eq!(NodeId::from(raw), id);
}
