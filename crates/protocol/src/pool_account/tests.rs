use super::*;

const POOL: [u8; 20] = [
    0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xAA, 0xBB,
];
const POOL_HEX: &str = "0xabcdef012345678900112233445566778899aabb";

#[test]
fn eip155_renders_lowercase_hex() {
    let account = PoolAccount::eip155(42161, POOL);
    assert_eq!(account.to_string(), format!("eip155:42161:{POOL_HEX}"));
    assert_eq!(account.chain_id(), ChainId::eip155(42161));
    assert_eq!(account.address(), POOL_HEX);
    assert_eq!(account.eip155_address(), Some(POOL));
}

#[test]
fn accepts_well_formed_ids() -> Result<(), PoolAccountError> {
    for id in [
        "eip155:1:0xab16a96d359ec26a11e2c2b3d8f8b8942d5bfcdb",
        "cosmos:cosmoshub-4:cosmos1t2uflqwqe0fsj0shcfkrvpukewcw40yjj6hdc0",
        "bip122:000000000019d6689c085ae165831e93:128Lkh3S7CkDTBZ8W7BbpsN3YYizJMp8p6",
        "abc:x:a",
        "abc:x:%2E-.Z9",
    ] {
        let parsed = PoolAccount::new(id)?;
        assert_eq!(parsed.to_string(), id);
        assert_eq!(id.parse::<PoolAccount>()?, parsed);
    }
    Ok(())
}

/// An `eip155` address in checksum case is stored in lower case, so two
/// spellings of one contract are one pool account.
#[test]
fn eip155_hex_addresses_compare_case_insensitively() -> Result<(), PoolAccountError> {
    let mixed = PoolAccount::new("eip155:10:0xAbCdEf012345678900112233445566778899AaBb")?;
    assert_eq!(mixed, PoolAccount::eip155(10, POOL));
    assert_eq!(mixed.address(), POOL_HEX);
    // Another namespace keeps its case: CAIP-10 does not fold it.
    let other = PoolAccount::new("cosmos:hub:AbC")?;
    assert_eq!(other.address(), "AbC");
    Ok(())
}

#[test]
fn rejects_a_bad_chain_part() {
    assert_eq!(
        PoolAccount::new("EIP155:1:0xab"),
        Err(PoolAccountError::Chain(ChainIdError::BadNamespace))
    );
    assert_eq!(
        PoolAccount::new("eip155:1.0:0xab"),
        Err(PoolAccountError::Chain(ChainIdError::BadReference))
    );
    assert_eq!(
        PoolAccount::new("eip155"),
        Err(PoolAccountError::Chain(ChainIdError::MissingColon))
    );
}

#[test]
fn rejects_a_missing_address() {
    assert_eq!(
        PoolAccount::new("eip155:1"),
        Err(PoolAccountError::MissingAddress)
    );
}

#[test]
fn rejects_bad_addresses() {
    for id in [
        "eip155:1:",         // empty
        "eip155:1:0xab:cd",  // a further colon
        "eip155:1:0x ab",    // whitespace
        "eip155:1:0x_ab",    // underscore is not in the CAIP-10 set
        "eip155:1:0x\u{e9}", // non-ASCII
    ] {
        assert_eq!(
            PoolAccount::new(id),
            Err(PoolAccountError::BadAddress),
            "{id:?} must be refused"
        );
    }
}

#[test]
fn bounds_the_length_at_170_bytes() -> Result<(), PoolAccountError> {
    let longest = format!("{}:{}:{}", "a".repeat(8), "B".repeat(32), "c".repeat(128));
    assert_eq!(longest.len(), MAX_POOL_ACCOUNT_LEN);
    assert_eq!(PoolAccount::new(&longest)?.to_string(), longest);

    let too_long = format!("{}:{}:{}", "a".repeat(8), "B".repeat(32), "c".repeat(129));
    assert_eq!(
        PoolAccount::new(&too_long),
        Err(PoolAccountError::TooLong {
            len: MAX_POOL_ACCOUNT_LEN + 1
        })
    );
    Ok(())
}

#[test]
fn eip155_address_needs_an_evm_hex_address() -> Result<(), PoolAccountError> {
    assert_eq!(PoolAccount::new("eip155:1:0xab")?.eip155_address(), None);
    assert_eq!(PoolAccount::new("eip155:1:abcd")?.eip155_address(), None);
    let foreign = format!("cosmos:hub:0x{}", "ab".repeat(20));
    assert_eq!(PoolAccount::new(&foreign)?.eip155_address(), None);
    Ok(())
}

#[test]
fn postcard_encodes_a_length_prefixed_string() -> Result<(), Box<dyn std::error::Error>> {
    let account = PoolAccount::new("eip155:1:0xab")?;
    let bytes = postcard::to_allocvec(&account)?;
    let mut expected = vec![13u8];
    expected.extend_from_slice(b"eip155:1:0xab");
    assert_eq!(bytes, expected);
    assert_eq!(postcard::from_bytes::<PoolAccount>(&bytes)?, account);
    Ok(())
}

#[test]
fn decode_refuses_a_malformed_id() -> Result<(), postcard::Error> {
    for bad in ["eip155:1", "EIP155:1:0xab", "eip155:1:", "eip155:1:0xab:cd"] {
        let bytes = postcard::to_allocvec(bad)?;
        assert!(
            postcard::from_bytes::<PoolAccount>(&bytes).is_err(),
            "{bad:?} must not decode"
        );
    }
    Ok(())
}

#[test]
fn debug_shows_the_string() {
    assert_eq!(
        format!("{:?}", PoolAccount::eip155(5, [0x11; 20])),
        "PoolAccount(\"eip155:5:0x1111111111111111111111111111111111111111\")"
    );
}
