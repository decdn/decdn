use super::*;
use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;

const REGISTRY: Address = Address::repeat_byte(0x42);
const ROOT: [u8; 32] = [0xab; 32];

/// secp256k1 group order `n`, to build the malleable high-`s` twin of a
/// signature.
const SECP256K1N: U256 = U256::from_be_bytes([
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
    0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36, 0x41, 0x41,
]);

fn domain() -> Eip712Domain {
    pointer_domain(421_614, REGISTRY)
}

fn signed(signer: &PrivateKeySigner, namespace: u64, seq: u64) -> SignedPointer {
    SignedPointer::sign(signer, namespace, ROOT, seq, &domain()).unwrap()
}

#[test]
fn a_signed_pointer_recovers_its_signer() {
    let owner = PrivateKeySigner::random();
    let pointer = signed(&owner, 7, 1);
    assert_eq!(pointer.signer(&domain()).unwrap(), owner.address());
    assert_eq!(pointer.namespace, 7);
    assert_eq!(pointer.root.0, ROOT);
    assert_eq!(pointer.seq, 1);
}

/// The domain binds the pointer to one deployment: under another chain or
/// another registry the same bytes recover a stranger.
#[test]
fn another_chain_or_registry_recovers_a_stranger() {
    let owner = PrivateKeySigner::random();
    let pointer = signed(&owner, 7, 1);
    let other_chain = pointer_domain(1, REGISTRY);
    assert_ne!(pointer.signer(&other_chain).unwrap(), owner.address());
    let other_registry = pointer_domain(421_614, Address::repeat_byte(0x43));
    assert_ne!(pointer.signer(&other_registry).unwrap(), owner.address());
}

/// Every signed field is bound: a change to any one recovers a stranger.
#[test]
fn a_tampered_field_recovers_a_stranger() {
    let owner = PrivateKeySigner::random();
    let pointer = signed(&owner, 7, 1);
    let tampered = [
        SignedPointer {
            namespace: 8,
            ..pointer.clone()
        },
        SignedPointer {
            root: B256::repeat_byte(0xac),
            ..pointer.clone()
        },
        SignedPointer {
            seq: 2,
            ..pointer.clone()
        },
    ];
    for t in tampered {
        assert_ne!(t.signer(&domain()).unwrap(), owner.address(), "{t:?}");
    }
}

#[test]
fn a_malformed_or_high_s_signature_is_refused() {
    let owner = PrivateKeySigner::random();
    let pointer = signed(&owner, 7, 1);
    let short = SignedPointer {
        signature: Bytes::from(vec![0u8; 64]),
        ..pointer.clone()
    };
    assert!(short.signer(&domain()).is_err());
    let garbage = SignedPointer {
        signature: Bytes::from(vec![0xffu8; 65]),
        ..pointer.clone()
    };
    assert!(garbage.signer(&domain()).is_err());
    // The high-`s` twin recovers the same signer under alloy, and reverts
    // on-chain: the gateway refuses it as the contracts would.
    let sig = Signature::try_from(pointer.signature.as_ref()).unwrap();
    let twin = Signature::new(sig.r(), SECP256K1N - sig.s(), !sig.v());
    let high_s = SignedPointer {
        signature: Bytes::from(twin.as_bytes().to_vec()),
        ..pointer
    };
    assert!(high_s.signer(&domain()).is_err());
}

/// The JSON shape `decdn publish point` sends and the gateway reads: numbers
/// for the ids, `0x` hex for the root and the signature.
#[test]
fn the_json_shape_round_trips() {
    let owner = PrivateKeySigner::random();
    let pointer = signed(&owner, 7, 3);
    let json = serde_json::to_value(&pointer).unwrap();
    assert_eq!(json["namespace"], 7);
    assert_eq!(json["seq"], 3);
    assert_eq!(json["root"], format!("0x{}", alloy::hex::encode(ROOT)));
    let signature = json["signature"].as_str().unwrap();
    assert!(signature.starts_with("0x") && signature.len() == 2 + 130);
    let back: SignedPointer = serde_json::from_value(json).unwrap();
    assert_eq!(back, pointer);
    assert_eq!(back.signer(&domain()).unwrap(), owner.address());
}

fn pointer(namespace: u64, root: u8, seq: u64) -> SignedPointer {
    SignedPointer {
        namespace,
        root: B256::repeat_byte(root),
        seq,
        signature: Bytes::from(vec![0u8; 65]),
    }
}

/// The owner every store test's pointers are taken as signed by.
const OWNER: Address = Address::repeat_byte(0x11);

#[test]
fn a_newer_pointer_replaces_and_an_older_or_equal_one_is_stale() {
    let dir = tempfile::tempdir().unwrap();
    let pointers = Pointers::load(dir.path()).unwrap();
    assert_eq!(pointers.root(7), None);
    assert_eq!(pointers.held(7), None);
    pointers.set(pointer(7, 0x01, 5), OWNER).unwrap();
    assert_eq!(pointers.root(7), Some([0x01; 32]));
    assert_eq!(pointers.held(7), Some((5, OWNER)));
    for seq in [5, 4] {
        match pointers.set(pointer(7, 0x02, seq), OWNER) {
            Err(SetError::Stale { current }) => assert_eq!(current, 5),
            other => panic!("seq {seq}: {other:?}"),
        }
    }
    assert_eq!(pointers.root(7), Some([0x01; 32]));
    pointers.set(pointer(7, 0x03, 6), OWNER).unwrap();
    assert_eq!(pointers.root(7), Some([0x03; 32]));
    // Another namespace has its own sequence.
    pointers.set(pointer(8, 0x04, 1), OWNER).unwrap();
    assert_eq!(pointers.root(8), Some([0x04; 32]));
    assert_eq!(pointers.root(7), Some([0x03; 32]));
}

/// A namespace changes hands on-chain: the new owner's first pointer
/// replaces the old owner's whatever its `seq`, so a former owner's high
/// `seq` cannot hold the host. The caller has verified the new signer owns
/// the namespace.
#[test]
fn another_signer_replaces_a_pointer_at_any_seq() {
    let dir = tempfile::tempdir().unwrap();
    let pointers = Pointers::load(dir.path()).unwrap();
    pointers.set(pointer(7, 0x01, u64::MAX), OWNER).unwrap();
    let new_owner = Address::repeat_byte(0x22);
    pointers.set(pointer(7, 0x02, 1), new_owner).unwrap();
    assert_eq!(pointers.root(7), Some([0x02; 32]));
    assert_eq!(pointers.held(7), Some((1, new_owner)));
    // From then on the new owner's own sequence holds.
    assert!(matches!(
        pointers.set(pointer(7, 0x03, 1), new_owner),
        Err(SetError::Stale { current: 1 })
    ));
}

#[test]
fn pointers_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let pointers = Pointers::load(dir.path()).unwrap();
        pointers.set(pointer(7, 0x01, 5), OWNER).unwrap();
        pointers.set(pointer(9, 0x02, 2), OWNER).unwrap();
    }
    let reloaded = Pointers::load(dir.path()).unwrap();
    assert_eq!(reloaded.root(7), Some([0x01; 32]));
    assert_eq!(reloaded.held(7), Some((5, OWNER)));
    assert_eq!(reloaded.root(9), Some([0x02; 32]));
    assert!(matches!(
        reloaded.set(pointer(7, 0x03, 5), OWNER),
        Err(SetError::Stale { current: 5 })
    ));
}

#[test]
fn a_file_that_does_not_parse_or_is_misnamed_fails_the_load() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("7.json"), b"not json").unwrap();
    assert!(Pointers::load(dir.path()).is_err());
    std::fs::remove_file(dir.path().join("7.json")).unwrap();
    let record = serde_json::to_vec(&Record {
        pointer: pointer(7, 0x01, 1),
        signer: OWNER,
    })
    .unwrap();
    std::fs::write(dir.path().join("8.json"), &record).unwrap();
    assert!(Pointers::load(dir.path()).is_err());
    std::fs::remove_file(dir.path().join("8.json")).unwrap();
    // A temp file a crash left behind is not a record.
    std::fs::write(dir.path().join(".tmpabc"), b"half").unwrap();
    std::fs::write(dir.path().join("7.json"), &record).unwrap();
    let pointers = Pointers::load(dir.path()).unwrap();
    assert_eq!(pointers.root(7), Some([0x01; 32]));
}

#[cfg(unix)]
#[test]
fn the_pointer_dir_is_created_owner_only() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let inner = dir.path().join("pointers");
    let pointers = Pointers::load(&inner).unwrap();
    assert_eq!(
        std::fs::metadata(&inner).unwrap().permissions().mode() & 0o777,
        0o700
    );
    pointers.set(pointer(7, 0x01, 1), OWNER).unwrap();
    let entries: Vec<_> = std::fs::read_dir(&inner)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(entries, ["7.json"], "no temp file is left behind");
}
