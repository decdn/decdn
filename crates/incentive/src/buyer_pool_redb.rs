//! `redb`-backed persistent [`BuyerPoolStore`] for client-side use (#940).
//!
//! A client (`decdn fetch`) has no seller state, so it gets a **buyer-only**
//! store: its own `buyer-pools.redb`, one table. The node keeps its buyer table
//! in its own `buyer.redb` — one of the per-family files its lane store opens,
//! so a buyer top-up commits on a writer slot separate from the seller lane
//! flush.
//!
//! That file ownership — plus the guards on [`RedbBuyerPoolStore::open`] — is
//! **all** this module contributes. The record codec and every table
//! operation live in [`crate::buyer_pool_table`], which the node's store
//! delegates to as well, so the two cannot drift in on-disk format or in
//! advance/deposit semantics (#1246). Before that, this module carried its
//! own byte-identical copy of both, kept in sync by hand.
//!
//! Gated behind the `redb` feature so non-client consumers of
//! `decdn-incentive` (the contracts/voucher logic) don't link `redb`. The
//! shared table sits behind `buyer-store-core`, which this feature implies.

use std::path::Path;

use alloy::primitives::{Address, U256};
use redb::{Database, ReadOnlyDatabase};

use crate::buyer_pool::{
    AdvanceOutcome, BuyerLaneProgress, BuyerLoad, BuyerPoolState, BuyerPoolStore, DepositOutcome,
};
use crate::buyer_pool_table::{BuyerPoolTable, load_all_from};
use crate::lane::{LaneKey, PoolId};
use crate::store::StoreError;

/// File name of the buyer-pool redb database within the data dir. Named in
/// [`decdn_common::data_dir`] beside the daemon's own stores, so a tool can
/// tell a client data dir from a node's before it writes to either.
const BUYER_POOLS_DB_FILE: &str = decdn_common::data_dir::CLIENT_BUYER_DB_FILE;

/// Buyer-only `redb`-backed [`BuyerPoolStore`]. One redb file, one table;
/// every mutating call fsyncs on commit (`Durability::Immediate`).
#[derive(Debug)]
pub struct RedbBuyerPoolStore {
    db: Database,
}

impl RedbBuyerPoolStore {
    /// Open (or create) the buyer-pool store at `<data_dir>/buyer-pools.redb`,
    /// creating `data_dir` with hardened permissions first.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the data dir cannot be created/hardened or
    /// redb cannot open the database.
    pub fn open(data_dir: &Path) -> Result<Self, StoreError> {
        decdn_common::identity::ensure_data_dir(data_dir)
            .map_err(|err| StoreError::Backend(format!("ensure data dir: {err}")))?;
        let path = data_dir.join(BUYER_POOLS_DB_FILE);
        // Reject a zero-length file. `redb::Database::create` treats both
        // "absent" and "present but empty" as "create a fresh database", so
        // a `truncate -s 0` or a filesystem rollback that nukes content but
        // keeps the inode would silently wipe the persisted watermark — the
        // next reuse would re-sign stale totals, or a second pool would
        // open and escrow another deposit. After the first commit the file
        // is non-zero forever in normal operation, so a zero length means
        // the store was deliberately or accidentally wiped. (Mirrors the
        // node store's guard.)
        if std::fs::metadata(&path).is_ok_and(|m| m.len() == 0) {
            return Err(StoreError::Corrupt {
                pool_id: None,
                detail: format!(
                    "buyer pool store at {} is empty (length 0) — a truncation or filesystem \
                     rollback would silently wipe the voucher watermark. Restore from backup, or \
                     delete the file deliberately to start fresh (forfeiting pool reuse).",
                    path.display()
                ),
            });
        }
        // A second concurrent opener (another `decdn fetch`/`bundle pull`
        // on the same data dir) trips redb's process-exclusive write lock.
        // Surface that as a clear, dedicated error instead of a raw backend
        // string — two processes must not share one pool's voucher
        // watermark (#942).
        let db = Database::create(&path).map_err(|err| match err {
            redb::DatabaseError::DatabaseAlreadyOpen => StoreError::AlreadyOpen { path },
            other => StoreError::Backend(format!("open buyer pool db: {other}")),
        })?;
        Ok(Self { db })
    }

    /// Bind the shared table operations to this store's database.
    const fn table(&self) -> BuyerPoolTable<'_> {
        BuyerPoolTable::new(&self.db)
    }

    /// Test-only corruption seam used to verify consumer handling of an
    /// undecodable buyer row.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] if the raw row cannot be committed durably.
    #[cfg(feature = "test-util")]
    pub fn insert_raw_buyer_record(&self, pool_id: PoolId, bytes: &[u8]) -> Result<(), StoreError> {
        self.table().insert_raw(pool_id, bytes)
    }
}

/// A buyer-pool store opened read-only, by file path.
///
/// The reader for any buyer store this process must not write: a **stopped**
/// `decdn-node` daemon's `buyer.redb` under post-mortem inspection (#2084), or
/// the client's own `buyer-pools.redb` when a command only needs to look.
///
/// `redb` holds its process-exclusive lock for the lifetime of an open
/// [`Database`], so the lock doubles as a liveness signal: a successful open
/// proves no process holds the file, and [`StoreError::AlreadyOpen`] proves one
/// does.
///
/// Two differences from [`RedbBuyerPoolStore`] are load-bearing:
///
/// - It takes a **file path**, not a data dir. A directory holds two unrelated
///   buyer stores, and a reader must not have to guess which one it means.
/// - It **never creates**. [`redb::ReadOnlyDatabase::open`] cannot, which is
///   what keeps #2078 closed: pointed at a path with no store it reports
///   [`StoreError::Absent`], rather than manufacturing an empty store and
///   reporting its emptiness as state.
///
/// Clearing a row is a separate decision and would need the write lock this
/// type deliberately does not take.
pub struct ReadOnlyBuyerPoolStore {
    db: ReadOnlyDatabase,
    path: std::path::PathBuf,
}

/// Hand-written because `redb::ReadOnlyDatabase` has no `Debug`. The path is
/// the identifying fact anyway — the handle itself prints nothing useful.
impl std::fmt::Debug for ReadOnlyBuyerPoolStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOnlyBuyerPoolStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl ReadOnlyBuyerPoolStore {
    /// Open the buyer-pool database at `path` read-only.
    ///
    /// # Errors
    ///
    /// - [`StoreError::AlreadyOpen`] when a process holds the write lock, which
    ///   means a daemon is running against this file.
    /// - [`StoreError::NeedsRepair`] when the file was not closed cleanly. A
    ///   read-only open cannot run redb's repair pass, so the file reads only
    ///   after a writable open has repaired it.
    /// - [`StoreError::Absent`] when no file exists at `path`. A routine state,
    ///   not a fault: a data dir whose store was deleted to force re-adoption
    ///   has none, and so does a dir no buyer has used.
    /// - [`StoreError::Backend`] for anything else.
    pub fn open_file(path: &Path) -> Result<Self, StoreError> {
        let db = ReadOnlyDatabase::open(path).map_err(|err| match err {
            redb::DatabaseError::DatabaseAlreadyOpen => StoreError::AlreadyOpen {
                path: path.to_path_buf(),
            },
            redb::DatabaseError::RepairAborted => StoreError::NeedsRepair {
                path: path.to_path_buf(),
            },
            // An absent file is the one `other` worth naming: it is a routine
            // state whose honest report is "there is no store here", and
            // leaving it as a backend errno makes a caller infer it.
            redb::DatabaseError::Storage(redb::StorageError::Io(ref io))
                if io.kind() == std::io::ErrorKind::NotFound =>
            {
                StoreError::Absent {
                    path: path.to_path_buf(),
                }
            }
            other => StoreError::Backend(format!(
                "open buyer pool db read-only at {}: {other}",
                path.display()
            )),
        })?;
        Ok(Self {
            db,
            path: path.to_path_buf(),
        })
    }

    /// The file this store reads. Callers name it in their own output, because
    /// which file produced a listing must never be ambiguous.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load every persisted buyer pool, with the same decode-and-skip semantics
    /// a live store applies.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the table or its iterator is unreadable.
    pub fn load_all(&self) -> Result<BuyerLoad, StoreError> {
        load_all_from(&self.db)
    }
}

/// Every method delegates to [`crate::buyer_pool_table`]; this store owns
/// the file, not the logic.
impl BuyerPoolStore for RedbBuyerPoolStore {
    fn load_all(&self) -> Result<BuyerLoad, StoreError> {
        self.table().load_all()
    }

    fn record(&self, state: &BuyerPoolState) -> Result<(), StoreError> {
        self.table().record(state)
    }

    fn forget(&self, owner: Address) -> Result<(), StoreError> {
        self.table().forget(owner)
    }

    fn forget_if_pool(&self, owner: Address, pool_id: PoolId) -> Result<bool, StoreError> {
        self.table().forget_if_pool(owner, pool_id)
    }

    fn get_by_pool_id(&self, pool_id: PoolId) -> Result<Option<BuyerPoolState>, StoreError> {
        self.table().get_by_pool_id(pool_id)
    }

    fn get_by_owner(&self, owner: Address) -> Result<Option<BuyerPoolState>, StoreError> {
        self.table().get_by_owner(owner)
    }

    fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.table()
            .advance_progress(owner, pool_id, lane, bytes, amount)
    }

    fn rebase_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        anchor: BuyerLaneProgress,
        totals: BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.table()
            .rebase_progress(owner, pool_id, lane, anchor, totals)
    }

    fn seed_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.table()
            .seed_progress(owner, pool_id, lane, bytes, amount)
    }

    fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        self.table().add_deposit(owner, pool_id, additional)
    }
}

#[cfg(test)]
mod tests;
