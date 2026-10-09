use super::*;
use std::assert_matches;

/// `buyer.redb` is one of the daemon markers, and the client's own store
/// is not a marker at all. The rest of the set is covered by
/// `a_node_data_dir_whose_buyer_store_was_deleted_is_still_a_node_s`.
#[test]
fn classify_buyer_store_keys_on_a_daemon_file_not_the_client_one() {
    let dir = tempfile::tempdir().unwrap();
    let client = || BuyerStoreOwner::Client {
        data_dir: dir.path().to_path_buf(),
    };
    assert_eq!(classify_buyer_store(dir.path()).unwrap(), client());

    // The client's own store does not make it a node data dir.
    std::fs::write(dir.path().join("buyer-pools.redb"), b"x").unwrap();
    assert_eq!(classify_buyer_store(dir.path()).unwrap(), client());

    std::fs::write(dir.path().join("buyer.redb"), b"x").unwrap();
    assert_eq!(
        classify_buyer_store(dir.path()).unwrap(),
        BuyerStoreOwner::Node {
            data_dir: dir.path().to_path_buf(),
            marker: decdn_common::data_dir::NODE_BUYER_DB_FILE,
        }
    );
}

/// A buy from a node's data dir never adopts, and a client dir always may.
///
/// The keystore in a node's dir is the daemon's operator key, so the
/// wallet's newest live pool is the one the daemon is paying from. Adopting
/// it would put a second, independent voucher series on the daemon's lanes.
/// Keyed on the same markers as `classify_buyer_store`, so a dir whose
/// `buyer.redb` was deleted mid-recovery is still refused.
#[test]
fn chain_adoption_is_refused_in_a_node_data_dir() {
    let client = tempfile::tempdir().unwrap();
    let client_key = client.path().join("keystore.json");
    assert_eq!(
        ChainAdoption::for_buy(client.path(), &client_key).unwrap(),
        ChainAdoption::Allowed
    );

    let node = tempfile::tempdir().unwrap();
    std::fs::write(node.path().join("lanes.redb"), b"x").unwrap();
    assert!(!node.path().join("buyer.redb").exists());
    assert_eq!(
        ChainAdoption::for_buy(node.path(), &node.path().join("keystore.json")).unwrap(),
        ChainAdoption::Refused,
        "a node dir must refuse adoption even with its buyer store gone"
    );
}

/// A client dir signing with the node's operator key refuses adoption too.
///
/// `--keystore` resolves independently of `--data-dir`, so the store can be
/// a client's while the wallet is the daemon's. The wallet is what decides
/// whose pools it owns, so its key is checked as well as the store's dir.
#[test]
fn chain_adoption_is_refused_for_a_node_operator_key_used_from_a_client_dir() {
    let client = tempfile::tempdir().unwrap();
    let node = tempfile::tempdir().unwrap();
    std::fs::write(node.path().join("lanes.redb"), b"x").unwrap();
    assert_eq!(
        ChainAdoption::for_buy(client.path(), &node.path().join("keystore.json")).unwrap(),
        ChainAdoption::Refused,
        "a client dir must not adopt the daemon's pool through its operator key"
    );
}

/// A node data dir whose `buyer.redb` has been deleted is STILL a node's.
///
/// This is the reset that the whole rule is about, and the repo's own
/// `node_pull_pool_adopt` e2e performs it: remove the buyer store, restart,
/// let the daemon re-adopt. In the window before that restart the other
/// daemon stores are still on disk. Keying classification on the one file
/// that is missing would call it a client dir and let `pool open` escrow a
/// second deposit — recreating the bug exactly when an operator is
/// recovering from it.
#[test]
fn a_node_data_dir_whose_buyer_store_was_deleted_is_still_a_node_s() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("lanes.redb"), b"x").unwrap();
    std::fs::write(dir.path().join("settle.redb"), b"x").unwrap();
    // No `buyer.redb` — it was deleted to force re-adoption.
    assert!(!dir.path().join("buyer.redb").exists());

    let owner = classify_buyer_store(dir.path()).unwrap();
    assert_matches!(
        owner,
        BuyerStoreOwner::Node { .. },
        "a data dir with daemon stores but no buyer.redb must not be a client's"
    );
    // And the guards still bite, which is the point.
    assert!(owner.refuse_escrow("open a pool").is_err());
    assert!(owner.open_for_write().unwrap().is_none());
    assert!(
        !dir.path().join("buyer-pools.redb").exists(),
        "no client store may be created in a node data dir mid-recovery"
    );
}

/// `open` / `top-up` are refused on a node data dir, and the refusal names
/// both files — an operator who sees only "refused" cannot tell which of
/// the two stores the command was about to write.
#[test]
fn escrow_is_refused_on_a_node_data_dir() {
    let owner = BuyerStoreOwner::Node {
        data_dir: PathBuf::from("/var/lib/decdn"),
        marker: decdn_common::data_dir::NODE_BUYER_DB_FILE,
    };
    let err = owner.refuse_escrow("open a pool").unwrap_err().to_string();
    assert!(err.contains("/var/lib/decdn/buyer.redb"), "{err}");
    assert!(err.contains("/var/lib/decdn/buyer-pools.redb"), "{err}");
    assert!(err.contains("decdn node pools"), "{err}");
    // A client data dir is unaffected.
    BuyerStoreOwner::Client {
        data_dir: PathBuf::from("/tmp/client"),
    }
    .refuse_escrow("open a pool")
    .unwrap();
}

/// `--all` enumerates from chain, so on a node data dir it would close the
/// pool the daemon is paying from. Refused; the single-`--pool` recovery
/// path is named as the alternative.
#[test]
fn sweep_is_refused_on_a_node_data_dir() {
    let owner = BuyerStoreOwner::Node {
        data_dir: PathBuf::from("/var/lib/decdn"),
        marker: decdn_common::data_dir::NODE_BUYER_DB_FILE,
    };
    let err = owner.refuse_sweep("close").unwrap_err().to_string();
    assert!(err.contains("--pool"), "{err}");
    assert!(err.contains("/var/lib/decdn/buyer.redb"), "{err}");
    BuyerStoreOwner::Client {
        data_dir: PathBuf::from("/tmp/client"),
    }
    .refuse_sweep("close")
    .unwrap();
}

/// A node data dir yields no writable store, so nothing is created there.
#[test]
fn node_data_dir_opens_no_store() {
    let dir = tempfile::tempdir().unwrap();
    let owner = BuyerStoreOwner::Node {
        data_dir: dir.path().to_path_buf(),
        marker: decdn_common::data_dir::NODE_BUYER_DB_FILE,
    };
    assert!(owner.open_for_write().unwrap().is_none());
    assert!(
        !dir.path().join("buyer-pools.redb").exists(),
        "classifying a node data dir must not create the client store"
    );
}

/// The fetch rule (#2082): a node dir the operator did not name is refused,
/// the same dir named on the command line is allowed, and a client dir is
/// unaffected whatever the source.
#[test]
fn a_buy_is_refused_only_when_the_node_dir_was_not_named() {
    let owner = BuyerStoreOwner::Node {
        data_dir: PathBuf::from("/var/lib/decdn"),
        marker: decdn_common::data_dir::NODE_BUYER_DB_FILE,
    };
    for source in [DataDirSource::Config, DataDirSource::Default] {
        let err = owner
            .refuse_implicit_node_dir("fetch", source)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--data-dir"), "{err}");
        assert!(err.contains("/var/lib/decdn/buyer.redb"), "{err}");
        assert!(err.contains("/var/lib/decdn/buyer-pools.redb"), "{err}");
    }
    owner
        .refuse_implicit_node_dir("fetch", DataDirSource::Flag)
        .expect("an explicitly named node dir is the operator's call");

    for source in [
        DataDirSource::Flag,
        DataDirSource::Config,
        DataDirSource::Default,
    ] {
        BuyerStoreOwner::Client {
            data_dir: PathBuf::from("/tmp/client"),
        }
        .refuse_implicit_node_dir("fetch", source)
        .unwrap();
    }
}

/// The fused openers refuse before they create anything — the property
/// that makes them safe to reach for instead of a bare
/// `RedbBuyerPoolStore::open`.
#[test]
fn the_fused_openers_create_no_store_on_a_node_dir() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("lanes.redb"), b"x").unwrap();

    assert!(open_client_store_for_escrow(dir.path(), "open a pool").is_err());
    assert!(open_client_store_for_buy(dir.path(), DataDirSource::Config, "fetch content").is_err());
    assert!(
        !dir.path().join("buyer-pools.redb").exists(),
        "a refused open must not manufacture the client store"
    );
}

/// An owner that cannot be told refuses every guarded open instead of
/// passing as a client dir (#2086). A regular file in place of the data dir
/// makes each marker stat fail with `NotADirectory`, not `NotFound`.
#[test]
fn an_unknown_owner_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let not_a_dir = dir.path().join("data");
    std::fs::write(&not_a_dir, b"x").unwrap();

    let err = classify_buyer_store(&not_a_dir).unwrap_err();
    assert!(
        format!("{err:#}").contains("cannot tell whether"),
        "{err:#}"
    );
    // The refusal must come from the classification, not from a store open
    // that happens to fail on the same path.
    for err in [
        open_client_store_for_escrow(&not_a_dir, "open a pool").unwrap_err(),
        open_client_store_for_buy(&not_a_dir, DataDirSource::Flag, "fetch content").unwrap_err(),
    ] {
        assert!(
            format!("{err:#}").contains("cannot tell whether"),
            "{err:#}"
        );
    }
    assert!(ChainAdoption::for_buy(dir.path(), &not_a_dir.join("keystore.json")).is_err());
}
