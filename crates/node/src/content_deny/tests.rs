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

/// The reload path must not clobber what the chain watcher learned, and the
/// watcher must not clobber the operator's local list. This is the whole
/// reason the two live in separate slots.
#[test]
fn local_reload_and_chain_updates_are_independent() {
    let deny = ContentDenylist::new(&content(&[addr(1)]));
    deny.apply_chain_origin(addr(2), true);
    assert!(deny.is_origin_denied(&addr(1)));
    assert!(deny.is_origin_denied(&addr(2)));

    // A reload that drops the local entry leaves the chain entry standing.
    deny.set_local_origins(&content(&[]));
    assert!(!deny.is_origin_denied(&addr(1)));
    assert!(deny.is_origin_denied(&addr(2)));

    // ...and a chain removal leaves a re-added local entry standing.
    deny.set_local_origins(&content(&[addr(1)]));
    deny.apply_chain_origin(addr(2), false);
    assert!(deny.is_origin_denied(&addr(1)));
    assert!(!deny.is_origin_denied(&addr(2)));
}

/// An address on BOTH lists must survive removal from one. A naive single
/// set would drop it and silently resume serving a blacklisted origin.
#[test]
fn origin_on_both_lists_survives_removal_from_one() {
    let deny = ContentDenylist::new(&content(&[addr(3)]));
    deny.apply_chain_origin(addr(3), true);
    deny.apply_chain_origin(addr(3), false);
    assert!(deny.is_origin_denied(&addr(3)), "local entry still stands");
}

#[test]
fn apply_chain_origin_reports_whether_it_changed_anything() {
    let deny = ContentDenylist::empty();
    assert!(deny.apply_chain_origin(addr(4), true));
    assert!(!deny.apply_chain_origin(addr(4), true), "replay is a no-op");
    assert!(deny.apply_chain_origin(addr(4), false));
    assert!(!deny.apply_chain_origin(addr(4), false));
}

#[test]
fn set_chain_origins_replaces_wholesale() {
    let deny = ContentDenylist::empty();
    deny.apply_chain_origin(addr(5), true);
    deny.set_chain_origins([addr(6)].into_iter().collect());
    assert!(!deny.is_origin_denied(&addr(5)));
    assert!(deny.is_origin_denied(&addr(6)));
    assert_eq!(deny.chain_origin_count(), 1);
}
