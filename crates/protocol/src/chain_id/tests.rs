use super::*;

#[test]
fn accepts_well_formed_ids() -> Result<(), ChainIdError> {
    for id in [
        "eip155:1",
        "eip155:42161",
        "cosmos:cosmoshub-4",
        "bip122:000000000019d6689c085ae165831e93",
        "abc:x",
        "a-b-c-d0:Ref_With-Mixed_Case",
    ] {
        let parsed = ChainId::new(id)?;
        assert_eq!(parsed.as_str(), id);
        assert_eq!(parsed.to_string(), id);
        assert_eq!(id.parse::<ChainId>()?, parsed);
    }
    Ok(())
}

#[test]
fn rejects_a_missing_colon() {
    assert_eq!(ChainId::new("eip155"), Err(ChainIdError::MissingColon));
    assert_eq!(ChainId::new(""), Err(ChainIdError::MissingColon));
}

#[test]
fn rejects_bad_namespaces() {
    for id in [
        ":1",            // empty
        "ab:1",          // 2 characters
        "abcdefghi:1",   // 9 characters
        "EIP155:1",      // upper case
        "eip_155:1",     // underscore is reference-only
        "eip.155:1",     // punctuation
        "eip155 :1",     // whitespace
        "\u{e9}ip155:1", // non-ASCII
    ] {
        assert_eq!(
            ChainId::new(id),
            Err(ChainIdError::BadNamespace),
            "{id:?} must be refused"
        );
    }
}

#[test]
fn rejects_bad_references() {
    for id in [
        "eip155:",       // empty
        "eip155:1:2",    // a second colon
        "eip155:1.0",    // punctuation
        "eip155:1 ",     // whitespace
        "eip155:\u{e9}", // non-ASCII
    ] {
        assert_eq!(
            ChainId::new(id),
            Err(ChainIdError::BadReference),
            "{id:?} must be refused"
        );
    }
    // 33 characters: one past the CAIP-2 bound.
    let long = format!("eip155:{}", "1".repeat(33));
    assert_eq!(ChainId::new(&long), Err(ChainIdError::BadReference));
}

#[test]
fn bounds_the_length_at_41_bytes() -> Result<(), ChainIdError> {
    let longest = format!("{}:{}", "a".repeat(8), "B".repeat(32));
    assert_eq!(longest.len(), MAX_CHAIN_ID_LEN);
    assert_eq!(ChainId::new(&longest)?.as_str(), longest);

    let too_long = format!("{}:{}", "a".repeat(8), "B".repeat(33));
    assert_eq!(too_long.len(), MAX_CHAIN_ID_LEN + 1);
    assert_eq!(
        ChainId::new(&too_long),
        Err(ChainIdError::TooLong {
            len: MAX_CHAIN_ID_LEN + 1
        })
    );
    Ok(())
}

#[test]
fn eip155_builds_the_evm_id() -> Result<(), ChainIdError> {
    assert_eq!(ChainId::eip155(42161).as_str(), "eip155:42161");
    assert_eq!(ChainId::eip155(42161), ChainId::new("eip155:42161")?);
    assert_eq!(
        ChainId::eip155(u64::MAX).as_str(),
        "eip155:18446744073709551615"
    );
    assert_eq!(ChainId::eip155(0).as_str(), "eip155:0");
    Ok(())
}

#[test]
fn eip155_id_reads_back_the_evm_id() -> Result<(), ChainIdError> {
    assert_eq!(ChainId::eip155(421_614).eip155_id(), Some(421_614));
    assert_eq!(ChainId::eip155(u64::MAX).eip155_id(), Some(u64::MAX));
    assert_eq!(ChainId::eip155(0).eip155_id(), Some(0));
    assert_eq!(ChainId::new("cosmos:cosmoshub-4")?.eip155_id(), None);
    assert_eq!(ChainId::new("eip155:0x1")?.eip155_id(), None);
    assert_eq!(ChainId::new("eip155:01")?.eip155_id(), None);
    assert_eq!(
        ChainId::new("eip155:18446744073709551616")?.eip155_id(),
        None
    );
    Ok(())
}

#[test]
fn postcard_encodes_a_length_prefixed_string() -> Result<(), postcard::Error> {
    let id = ChainId::eip155(1);
    let bytes = postcard::to_allocvec(&id)?;
    let mut expected = vec![8u8];
    expected.extend_from_slice(b"eip155:1");
    assert_eq!(bytes, expected);
    assert_eq!(postcard::from_bytes::<ChainId>(&bytes)?, id);
    Ok(())
}

#[test]
fn decode_refuses_a_malformed_id() -> Result<(), postcard::Error> {
    for bad in ["eip155", "EIP155:1", "eip155:", "eip155:1:2"] {
        let bytes = postcard::to_allocvec(bad)?;
        assert!(
            postcard::from_bytes::<ChainId>(&bytes).is_err(),
            "{bad:?} must not decode"
        );
    }
    let too_long = format!("{}:{}", "a".repeat(8), "B".repeat(33));
    let bytes = postcard::to_allocvec(too_long.as_str())?;
    assert!(postcard::from_bytes::<ChainId>(&bytes).is_err());
    Ok(())
}

#[test]
fn ordering_follows_the_string() -> Result<(), ChainIdError> {
    let mut ids = [
        ChainId::new("eip155:9")?,
        ChainId::new("eip155:10")?,
        ChainId::new("cosmos:a")?,
    ];
    ids.sort();
    let sorted: Vec<&str> = ids.iter().map(ChainId::as_str).collect();
    assert_eq!(sorted, ["cosmos:a", "eip155:10", "eip155:9"]);
    Ok(())
}

#[test]
fn debug_shows_the_string() {
    assert_eq!(format!("{:?}", ChainId::eip155(5)), "ChainId(\"eip155:5\")");
}
