//! Which buyer store a resolved `data_dir` belongs to, and what a command may
//! do with it.
//!
//! The client and the daemon keep their buyer pools in two different files in
//! the same directory — `buyer-pools.redb` and `buyer.redb` — sharing one table
//! format but nothing else. Every command that opens a buyer store asks here
//! first, so the answer is one rule rather than one rule per subcommand.

use std::path::{Path, PathBuf};

use anyhow::Context as _;

use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;

/// Which buyer store a resolved `data_dir` belongs to.
///
/// A command pointed at a daemon's `data_dir` would otherwise open (and, via
/// `Database::create`, *manufacture*) the client file and report its emptiness
/// as the node's state (#2078).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BuyerStoreOwner {
    /// No daemon store here, so the CLI owns `buyer-pools.redb` in this dir.
    Client {
        /// The dir this verdict is about. Carried so the verdict and the store
        /// it authorizes cannot be about two different directories.
        data_dir: PathBuf,
    },
    /// A `decdn-node` daemon owns this data dir. The CLI owns only the client
    /// store, so it must not create a second, unrelated store beside the
    /// daemon's. Classification is file presence, not liveness: while that
    /// daemon runs, redb's process-exclusive lock also makes reading its
    /// `buyer.redb` from disk impossible, which is why the read goes over the
    /// admin RPC.
    Node {
        /// The dir itself, so both store paths can be named in a message —
        /// which file produced a `pools=0` must never be ambiguous.
        data_dir: PathBuf,
        /// The daemon-owned file that gave it away. Named in the refusal so an
        /// operator whose `buyer.redb` is missing can see WHY the dir was
        /// still judged a node's.
        marker: &'static str,
    },
}

/// Where a resolved `data_dir` came from.
///
/// Only [`DataDirSource::Flag`] is a deliberate choice of directory. On a node
/// host `identity.data_dir` in `~/.decdn/node.toml` is already the node's, and
/// a bare `decdn fetch` resolves to it without the operator naming anything —
/// which is how a client store, under the node's own operator address, lands
/// beside the daemon's (#2082).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataDirSource {
    /// An explicit `--data-dir` on the command line.
    Flag,
    /// `identity.data_dir` from the config file.
    Config,
    /// The built-in client default (`~/.decdn/client`).
    Default,
}

/// The daemon's buyer store within `data_dir`.
pub(crate) fn node_buyer_db(data_dir: &Path) -> PathBuf {
    data_dir.join(decdn_common::data_dir::NODE_BUYER_DB_FILE)
}

/// The client's buyer store within `data_dir`.
pub(crate) fn client_buyer_db(data_dir: &Path) -> PathBuf {
    data_dir.join(decdn_common::data_dir::CLIENT_BUYER_DB_FILE)
}

/// Whether a buy may adopt a pool its wallet already owns on chain, when the
/// client store has no row for it.
///
/// Adoption assumes the wallet has one buyer: the newest live pool it owns is
/// taken to be this client's own. A wallet shared with another buyer breaks
/// that, because adopting the other buyer's pool puts a second, independent
/// voucher series on its lanes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChainAdoption {
    /// Neither the store nor the key belongs to a node, so the wallet's pools
    /// are this client's own and a lost row is recovered from chain rather than
    /// escrowing a second deposit.
    Allowed,
    /// The store or the key belongs to a `decdn-node` daemon, so the wallet is
    /// the node's operator and its newest live pool is the one the daemon is
    /// paying from. The buy opens its own pool instead.
    Refused,
}

impl ChainAdoption {
    /// The rule for a buy whose client store lives in `data_dir` and which
    /// signs with the keystore at `keystore`.
    ///
    /// Both are checked, because they resolve independently: `--data-dir`
    /// (#2082) puts a client store in a node's dir, and `--keystore` or
    /// `blockchain.eth_keystore` points a client dir at a node's operator key.
    /// Either way the wallet is the daemon's. The keystore is judged by the
    /// directory that holds it, read against the same daemon markers as
    /// [`classify_buyer_store`].
    ///
    /// This recognises a key that lives in a node's data dir. It cannot
    /// recognise one copied out of it, or a wallet two clients share — nothing
    /// local distinguishes those from a wallet with one buyer.
    ///
    /// # Errors
    ///
    /// Errors when either directory's owner cannot be told, because a stat of
    /// one of its daemon markers failed for a reason other than `NotFound`.
    pub(crate) fn for_buy(data_dir: &Path, keystore: &Path) -> anyhow::Result<Self> {
        let node_owned = |dir: &Path| -> anyhow::Result<bool> {
            Ok(matches!(
                classify_buyer_store(dir)?,
                BuyerStoreOwner::Node { .. }
            ))
        };
        let key_dir = keystore.parent().unwrap_or(Path::new(""));
        Ok(if node_owned(data_dir)? || node_owned(key_dir)? {
            Self::Refused
        } else {
            Self::Allowed
        })
    }
}

/// Classify `data_dir` by whether a `decdn-node` daemon owns it.
///
/// Keys on ANY of the daemon's store files, not `buyer.redb` alone. The buyer
/// store is precisely the file that goes missing in the reset this whole rule
/// is about — a moved volume, a re-provisioned host, or an operator deleting it
/// to force re-adoption — and a dir whose `buyer.redb` is gone but whose
/// `lanes.redb` remains is still a node's. Keying on the missing file would
/// classify it `Client` and escrow a second deposit into a store the daemon
/// never reads, which is the bug, recreated at exactly the moment an operator
/// is recovering from it.
///
/// `node.secret` is deliberately not a marker — a client keygen writes one too.
///
/// # Errors
///
/// Errors when a daemon marker's stat fails for a reason other than `NotFound`.
/// The owner is then unknown, and every guard built on this verdict refuses
/// rather than treating the directory as a client's (#2086).
pub(crate) fn classify_buyer_store(data_dir: &Path) -> anyhow::Result<BuyerStoreOwner> {
    let marker = decdn_common::data_dir::daemon_marker(data_dir).with_context(|| {
        format!(
            "cannot tell whether {} belongs to a decdn-node daemon",
            data_dir.display()
        )
    })?;
    Ok(match marker {
        Some(marker) => BuyerStoreOwner::Node {
            data_dir: data_dir.to_path_buf(),
            marker,
        },
        None => BuyerStoreOwner::Client {
            data_dir: data_dir.to_path_buf(),
        },
    })
}

impl BuyerStoreOwner {
    /// Refuse a command that would escrow or credit a deposit into a store the
    /// daemon never reads.
    ///
    /// `open` and `top-up` are the two that *create* the stranded-deposit
    /// condition: the USDC leaves the wallet, the row lands in the client file,
    /// and the daemon opens a second pool on its next miss. There is no
    /// honest way to do this half-correctly, so it does not run at all.
    ///
    /// # Errors
    ///
    /// Errors when this is [`BuyerStoreOwner::Node`].
    pub(crate) fn refuse_escrow(&self, verb: &str) -> anyhow::Result<()> {
        let Self::Node { data_dir, marker } = self else {
            return Ok(());
        };
        anyhow::bail!(
            "refusing to {verb}: {} belongs to a decdn-node daemon (it holds {marker}), and \
             `decdn pool` writes a separate client store ({}) the daemon never reads. The \
             escrowed USDC would be invisible to the node, which would then open a second pool \
             of its own. The daemon manages its own pool — it opens one at first miss and tops \
             it up from blockchain.buyer_working_deposit_micro_usdc. Run `decdn node pools` to \
             see what it holds ({} is its copy), or pass --data-dir <client dir> to act as a \
             separate buyer.",
            data_dir.display(),
            client_buyer_db(data_dir).display(),
            node_buyer_db(data_dir).display(),
        )
    }

    /// Refuse a chain-wide `--all` sweep against a node data dir.
    ///
    /// `close --all` / `reclaim --all` enumerate from chain by keystore
    /// address, not from the local store, so on a node host they would close
    /// the pool the daemon is actively paying from while the local forget
    /// silently no-ops. A single `--pool <id>` is the stranded-pool recovery
    /// path and stays available. `list --all` sends no transaction and writes
    /// no pool record, so it is not refused.
    ///
    /// # Errors
    ///
    /// Errors when this is [`BuyerStoreOwner::Node`].
    pub(crate) fn refuse_sweep(&self, verb: &str) -> anyhow::Result<()> {
        let Self::Node { data_dir, .. } = self else {
            return Ok(());
        };
        anyhow::bail!(
            "refusing to {verb} --all: this data dir belongs to a decdn-node daemon ({}), and \
             --all enumerates every pool this keystore owns ON CHAIN — including the one the \
             daemon is paying from right now. Run `decdn node pools` to see which pool that is, \
             then {verb} the stranded ones individually with --pool <poolId>.",
            node_buyer_db(data_dir).display(),
        )
    }

    /// Refuse a buy that would escrow into a node data dir nobody named.
    ///
    /// `decdn fetch` and `decdn bundle pull` are legitimately separate buyers
    /// when a human runs them on a node host, so an explicit `--data-dir` is a
    /// real answer here in a way it is not for `pool open`. What is not an
    /// answer is arriving at the node's dir by default: `identity.data_dir`
    /// from `~/.decdn/node.toml` resolves there with nothing on the command
    /// line, and the keystore defaults to the same dir — so the pool is opened
    /// under the node's OWN operator address. The daemon then reports it as a
    /// stranded deposit, and on a store reset can adopt it, which puts two
    /// independent watermarks on one lane (#2082).
    ///
    /// # Errors
    ///
    /// Errors when this is [`BuyerStoreOwner::Node`] and `source` is not
    /// [`DataDirSource::Flag`].
    pub(crate) fn refuse_implicit_node_dir(
        &self,
        verb: &str,
        source: DataDirSource,
    ) -> anyhow::Result<()> {
        let Self::Node { data_dir, marker } = self else {
            return Ok(());
        };
        if source == DataDirSource::Flag {
            return Ok(());
        }
        // `Flag` returned above, so the two remaining steps are the whole
        // match — no wildcard, so a fourth ladder step fails to compile here
        // rather than silently printing one of these.
        let named = match source {
            DataDirSource::Flag => return Ok(()),
            DataDirSource::Config => "identity.data_dir in the config file",
            DataDirSource::Default => "the default client data dir",
        };
        anyhow::bail!(
            "refusing to {verb}: {} came from {named}, and it belongs to a decdn-node daemon (it \
             holds {marker}). Buying from here opens a client pool ({}) under the node's own \
             operator keystore, beside the daemon's own store ({}) — the daemon would report \
             that deposit as stranded, and could adopt it and sign a second voucher series on \
             one lane. Pass --data-dir <client dir> to buy as a separate client, or pass \
             --data-dir {} to say you really mean the node's dir.",
            data_dir.display(),
            client_buyer_db(data_dir).display(),
            node_buyer_db(data_dir).display(),
            data_dir.display(),
        )
    }

    /// The store handle the mutating commands should use: `Some` for a client
    /// data dir, `None` for a node's — nothing may be written there.
    ///
    /// Takes no path. The dir it opens is the dir it classified, so a caller
    /// cannot hold a `Client` verdict about one directory and open a store in
    /// another — which is #2078 with an extra step.
    ///
    /// # Errors
    ///
    /// Propagates the store open error for a client data dir.
    pub(crate) fn open_for_write(&self) -> anyhow::Result<Option<RedbBuyerPoolStore>> {
        match self {
            Self::Client { data_dir } => Ok(Some(RedbBuyerPoolStore::open(data_dir)?)),
            Self::Node { .. } => Ok(None),
        }
    }
}

/// Classify, refuse, and open in one step, for a command that escrows.
///
/// The guard and the open are one call, so a new escrowing subcommand cannot
/// forget the check: there is no way to reach the store without passing it.
///
/// # Errors
///
/// Errors when `data_dir` belongs to a daemon or its owner cannot be told, or
/// when the client store will not open.
pub(crate) fn open_client_store_for_escrow(
    data_dir: &Path,
    verb: &str,
) -> anyhow::Result<RedbBuyerPoolStore> {
    let owner = classify_buyer_store(data_dir)?;
    owner.refuse_escrow(verb)?;
    owner
        .open_for_write()?
        .ok_or_else(|| anyhow::anyhow!("a node data dir has no writable client store"))
}

/// Classify, refuse an unnamed node dir, and open, for a command that buys.
///
/// The fetch-side counterpart of [`open_client_store_for_escrow`]: same fusing,
/// different rule — an explicit `--data-dir` is accepted here.
///
/// # Errors
///
/// Errors when `data_dir` belongs to a daemon and was not named on the command
/// line, when its owner cannot be told, or when the client store will not open.
pub(crate) fn open_client_store_for_buy(
    data_dir: &Path,
    source: DataDirSource,
    verb: &str,
) -> anyhow::Result<RedbBuyerPoolStore> {
    let owner = classify_buyer_store(data_dir)?;
    owner.refuse_implicit_node_dir(verb, source)?;
    // A named node dir is the operator's call, so the buy proceeds there — the
    // one place a `Node` verdict still opens a client store.
    match owner.open_for_write()? {
        Some(store) => Ok(store),
        None => Ok(RedbBuyerPoolStore::open(data_dir)?),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;

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
        assert!(
            matches!(owner, BuyerStoreOwner::Node { .. }),
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
        assert!(
            open_client_store_for_buy(dir.path(), DataDirSource::Config, "fetch content").is_err()
        );
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
        assert!(open_client_store_for_escrow(&not_a_dir, "open a pool").is_err());
        assert!(
            open_client_store_for_buy(&not_a_dir, DataDirSource::Flag, "fetch content").is_err()
        );
        assert!(ChainAdoption::for_buy(dir.path(), &not_a_dir.join("keystore.json")).is_err());
    }
}
