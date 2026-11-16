use super::*;

fn addr(byte: u8) -> Address {
    Address::from([byte; 20])
}

fn content(origins: &[Address]) -> ResolvedContent {
    ResolvedContent {
        denied_origins: origins.iter().copied().collect(),
        ..ResolvedContent::default()
    }
}

fn chain(origins: &[Address]) -> HashSet<Address> {
    origins.iter().copied().collect()
}

#[test]
fn empty_denies_nothing() {
    assert!(!ContentDenylist::empty().is_origin_denied(&addr(1)));
}

#[test]
fn local_origins_are_denied() {
    let deny = ContentDenylist::new(&content(&[addr(9)]));
    assert!(deny.is_origin_denied(&addr(9)));
    assert!(!deny.is_origin_denied(&addr(10)));
}

/// The reload path must not clobber what the blacklist sync learned, and the
/// sync must not clobber the operator's local list. This is the whole reason
/// the two live in separate slots.
#[test]
fn local_reload_and_chain_updates_are_independent() {
    let deny = ContentDenylist::new(&content(&[addr(1)]));
    deny.set_chain_origins(chain(&[addr(2)]));
    assert!(deny.is_origin_denied(&addr(1)));
    assert!(deny.is_origin_denied(&addr(2)));

    // A reload that drops the local entry leaves the chain entry standing.
    deny.set_local_origins(&content(&[]));
    assert!(!deny.is_origin_denied(&addr(1)));
    assert!(deny.is_origin_denied(&addr(2)));

    // ...and a chain removal leaves a re-added local entry standing.
    deny.set_local_origins(&content(&[addr(1)]));
    deny.set_chain_origins(chain(&[]));
    assert!(deny.is_origin_denied(&addr(1)));
    assert!(!deny.is_origin_denied(&addr(2)));
}

/// An address on BOTH lists must survive removal from one. A naive single
/// set would drop it and silently resume serving a blacklisted origin.
#[test]
fn origin_on_both_lists_survives_removal_from_one() {
    let deny = ContentDenylist::new(&content(&[addr(3)]));
    deny.set_chain_origins(chain(&[addr(3)]));
    deny.set_chain_origins(chain(&[]));
    assert!(deny.is_origin_denied(&addr(3)), "local entry still stands");
}

#[test]
fn set_chain_origins_replaces_wholesale() {
    let deny = ContentDenylist::empty();
    deny.set_chain_origins(chain(&[addr(5)]));
    deny.set_chain_origins(chain(&[addr(6)]));
    assert!(!deny.is_origin_denied(&addr(5)));
    assert!(deny.is_origin_denied(&addr(6)));
    assert_eq!(deny.chain_origin_count(), 1);
}
