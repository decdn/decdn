//! Which files live in a `data_dir`, and whose they are.
//!
//! A `decdn-node` daemon and a standalone client can be pointed at the same
//! directory, and they keep *different* stores there. The buyer pools are the
//! pair that matters: the daemon's [`NODE_BUYER_DB_FILE`] and the client's
//! [`CLIENT_BUYER_DB_FILE`] share one table format and nothing else — different
//! pools, different owners, and `redb` holds a process-exclusive lock on the
//! daemon's for its lifetime, so no other process can open it.
//!
//! Naming them here rather than in each crate is what lets an operator tool
//! tell the two apart before it writes to the wrong one (#2078).

use std::path::Path;

/// The daemon's buyer-pool store: buyer pool state and its owner index.
pub const NODE_BUYER_DB_FILE: &str = "buyer.redb";

/// The daemon's seller-side per-lane voucher store.
pub const NODE_LANES_DB_FILE: &str = "lanes.redb";

/// The daemon's pending-settle store (seller and buyer sets).
pub const NODE_SETTLE_DB_FILE: &str = "settle.redb";

/// The daemon's settlement-watcher checkpoint store.
pub const NODE_CHECKPOINT_DB_FILE: &str = "checkpoint.redb";

/// The standalone client's buyer-only pool store (`decdn fetch`, `decdn pool`).
pub const CLIENT_BUYER_DB_FILE: &str = "buyer-pools.redb";

/// Every store file a `decdn-node` daemon owns in its data dir.
///
/// The daemon's store opens all of them at bring-up, so any one of them present
/// means a daemon has run against this directory. Nothing a client runs creates
/// any of them.
pub const DAEMON_STORE_FILES: &[&str] = &[
    NODE_BUYER_DB_FILE,
    NODE_LANES_DB_FILE,
    NODE_SETTLE_DB_FILE,
    NODE_CHECKPOINT_DB_FILE,
];

/// The daemon-owned file that marks `data_dir` as a node's, if any.
///
/// Checks the whole [`DAEMON_STORE_FILES`] set rather than
/// [`NODE_BUYER_DB_FILE`] alone, because the buyer store is exactly the file an
/// operator recovering from a reset deletes or loses. A directory whose
/// `buyer.redb` is gone but whose `lanes.redb` remains is still a node's, and
/// treating it as a client's is how a second deposit gets escrowed into a store
/// the daemon never reads.
#[must_use]
pub fn daemon_marker(data_dir: &Path) -> Option<&'static str> {
    DAEMON_STORE_FILES
        .iter()
        .copied()
        .find(|name| data_dir.join(name).exists())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_dir_has_no_daemon_marker() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(daemon_marker(dir.path()), None);
    }

    /// The client's own store is not a daemon marker — otherwise every client
    /// data dir would classify as a node's after its first pool.
    #[test]
    fn the_client_store_is_not_a_daemon_marker() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CLIENT_BUYER_DB_FILE), b"x").unwrap();
        assert_eq!(daemon_marker(dir.path()), None);
    }

    /// The reset case this exists for: `buyer.redb` deleted, the rest of the
    /// daemon's stores still present. Still a node's data dir.
    #[test]
    fn a_data_dir_missing_only_the_buyer_store_is_still_a_node_s() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(NODE_LANES_DB_FILE), b"x").unwrap();
        std::fs::write(dir.path().join(NODE_SETTLE_DB_FILE), b"x").unwrap();
        assert_eq!(daemon_marker(dir.path()), Some(NODE_LANES_DB_FILE));
    }

    #[test]
    fn every_daemon_store_file_marks_the_dir() {
        for name in DAEMON_STORE_FILES {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join(name), b"x").unwrap();
            assert_eq!(daemon_marker(dir.path()), Some(*name), "{name}");
        }
    }
}
