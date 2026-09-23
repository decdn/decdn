//! The buyer-pool `redb` table: its on-disk record codec and its operations
//! (#1246).
//!
//! **This module is the single definition of the buyer table's on-disk
//! format and transactional semantics.** Two stores persist buyer pools, and
//! they differ only in *which file they own*:
//!
//! - `buyer_pool_redb::RedbBuyerPoolStore` owns a buyer-only
//!   `buyer-pools.redb` for the client (`decdn fetch`, #940). (Code span,
//!   not a link: that module exists only under the `redb` feature, so
//!   linking it would break a `buyer-store-core`-only doc build.)
//! - `decdn-node`'s `channel_store::PersistentPoolStateStore` owns the buyer
//!   table in its own `buyer.redb`, one of the per-family redb files it opens
//!   under `data_dir` (the seller lane, pending-settle, and watcher-checkpoint
//!   families each get their own file too, so no family's commit waits on
//!   another's writer slot).
//!
//! That is a file-ownership difference, not a logic difference, so both
//! stores supply only their own `Database` and delegate the actual work to
//! [`BuyerPoolTable`]. Both stores share this code, so their on-disk format is
//! byte-identical by construction, and `tests::encode_is_byte_stable` pins the bytes.
//!
//! The operations here never assume they own the file or that the buyer
//! table is the only table in it — that is what makes the node's
//! multi-table layout safe to serve from the same code.

use alloy::primitives::{Address, U256};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::buyer_pool::{
    AdvanceOutcome, BuyerLaneProgress, BuyerLoad, BuyerPoolState, BuyerProgressError,
    DepositOutcome,
};
use crate::lane::{LaneKey, PoolId};
use crate::store::StoreError;

/// The buyer table: name, key type, and value type.
///
/// **The whole thing is a frozen on-disk identifier**, and it is
/// deliberately private and non-configurable. Changing the name orphans
/// every existing record in both the node's `buyer.redb` and the client's
/// `buyer-pools.redb`; changing the key/value types breaks them harder
/// still, because `redb` persists key/value *type names* in the table
/// metadata and refuses to open a table whose types don't match — at
/// runtime, not compile time.
///
/// Callers supply the `Database` (the one thing they legitimately differ
/// on) and nothing else.
///
/// **`_v4`**: the primary key is `pool_id` (32 bytes); the value carries the
/// `PaymentPool` address the pool lives on plus a variable-length per-lane
/// progress table. A layout change that is not a trailing addition bumps this
/// suffix.
///
/// A file written under an older suffix holds no table of this name, so
/// `open_table` reports `TableDoesNotExist` and every read path treats the
/// store as empty: the old rows are ignored, never misread and never
/// live-migrated. **A suffix bump therefore orphans every row written before
/// it** — including rows for pools on the configured contract, which the node
/// then re-adopts from chain. (The key/value *type-name* check `redb` persists
/// per table is a separate guard, and it fires on a type change, not on this
/// rename: the key/value types here are unchanged.)
const BUYER_POOL_TABLE: TableDefinition<'static, &'static [u8; 32], &'static [u8]> =
    TableDefinition::new("buyer_pool_state_v4");

/// Secondary index: `owner (20 bytes) → pool_id (32 bytes)`. Maintained
/// alongside [`BUYER_POOL_TABLE`] on every `record`/`forget`/
/// `forget_if_pool` so `get_by_owner` — the open-pool trigger's reuse-lookup
/// hot path — stays a one-hop lookup instead of a table scan.
///
/// **This is a reuse hint, not an enumeration.** If two pools ever share an
/// owner (e.g. mid-rotate), the index holds only the most-recently-recorded
/// `pool_id`; the other pool still exists in [`BUYER_POOL_TABLE`] and is
/// only visible via [`BuyerPoolTable::load_all`] (the reclaim sweep's path)
/// — never via [`BuyerPoolTable::get_by_owner`].
const BUYER_OWNER_INDEX_TABLE: TableDefinition<'static, &'static [u8; 20], &'static [u8; 32]> =
    TableDefinition::new("buyer_pool_owner_index_v4");

/// The buyer-record `schema_version` this binary reads and writes.
///
/// Matched exactly, not as a ceiling. The fields are positional, so a record
/// written under a different version does not decode into these fields — it
/// decodes into the wrong ones, silently. Refusing anything that is not this
/// exact layout is the only answer that cannot mis-map.
const BUYER_SUPPORTED_SCHEMA_VERSION: u32 = 2;

/// Sanity ceiling on trailing bytes per record. Trailing bytes are tolerated
/// (forward-compat with additive schema changes), but a `remainder.len()`
/// above this is logged so an honest schema-skew incident or a malicious
/// padding attempt is observable in operator logs without re-introducing
/// the strict-decoding regression issue #527's reviewers warned against.
const SANE_TRAILER_MAX_BYTES: usize = 256;

/// On-disk per-lane progress entry. Fixed-size big-endian fields, same
/// convention as the pool-level record.
#[derive(Debug, Serialize, Deserialize)]
struct StoredLane {
    signer: [u8; 20],
    provider: [u8; 20],
    last_amount: [u8; 32],
    last_bytes: [u8; 32],
}

/// On-disk buyer-pool record. All numeric fields are fixed-size big-endian
/// byte arrays so the encoded width per field is stable across postcard
/// versions; `lanes` is the one variable-length part, encoded as a postcard
/// `Vec` (length-prefixed).
/// `schema_version` lives in the value, not the key. A field appended at the
/// END ships under a bumped `schema_version` alone — decode uses
/// [`postcard::take_from_bytes`], which tolerates trailing bytes. A field
/// inserted anywhere else shifts every field after it, so it needs the
/// table-name suffix bumped too, or an older record would decode into the wrong
/// fields.
///
/// **The field order is the wire order.** Postcard encodes struct fields
/// positionally and unnamed, so reordering or retyping a field silently
/// rewrites every record with no compile error. `encode_is_byte_stable`
/// guards this. `lanes` is always encoded sorted by `(signer, provider)` so
/// the golden bytes are deterministic regardless of `HashMap` iteration
/// order.
///
/// Deliberately private: the encoded shape is unnameable outside this
/// module, so no downstream crate can construct or reorder it.
#[derive(Debug, Serialize, Deserialize)]
struct StoredBuyerPoolState {
    schema_version: u32,
    pool_id: [u8; 32],
    payment_pool: [u8; 20],
    owner: [u8; 20],
    token: [u8; 20],
    deposit: [u8; 32],
    lanes: Vec<StoredLane>,
}

impl From<&BuyerPoolState> for StoredBuyerPoolState {
    fn from(state: &BuyerPoolState) -> Self {
        let mut lanes: Vec<StoredLane> = state
            .lanes()
            .map(|(key, progress)| StoredLane {
                signer: key.signer.into(),
                provider: key.provider.into(),
                last_amount: progress.last_amount.to_be_bytes(),
                last_bytes: progress.last_bytes.to_be_bytes(),
            })
            .collect();
        // Deterministic order: the pool_id is fixed for the whole record, so
        // (signer, provider) alone is a total order over the lanes.
        lanes.sort_by_key(|l| (l.signer, l.provider));
        Self {
            schema_version: BUYER_SUPPORTED_SCHEMA_VERSION,
            pool_id: state.pool_id.into(),
            payment_pool: state.payment_pool.into(),
            owner: state.owner.into(),
            token: state.token.into(),
            deposit: state.deposit.to_be_bytes(),
            lanes,
        }
    }
}

impl StoredBuyerPoolState {
    fn into_state(self, pool_id: PoolId) -> Result<BuyerPoolState, StoreError> {
        if self.schema_version != BUYER_SUPPORTED_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema {
                found: self.schema_version,
                supported: BUYER_SUPPORTED_SCHEMA_VERSION,
            });
        }
        let lanes = self
            .lanes
            .into_iter()
            .map(|l| {
                let key = LaneKey {
                    pool_id,
                    signer: Address::from(l.signer),
                    provider: Address::from(l.provider),
                };
                let progress = BuyerLaneProgress {
                    last_amount: U256::from_be_bytes(l.last_amount),
                    last_bytes: U256::from_be_bytes(l.last_bytes),
                };
                (key, progress)
            })
            .collect();
        Ok(BuyerPoolState::hydrate(
            pool_id,
            Address::from(self.payment_pool),
            Address::from(self.owner),
            Address::from(self.token),
            U256::from_be_bytes(self.deposit),
            lanes,
        ))
    }
}

/// Encode one buyer record. The single encoder shared by every crate that
/// persists a buyer pool.
fn encode_record(state: &BuyerPoolState) -> Result<Vec<u8>, StoreError> {
    postcard::to_allocvec(&StoredBuyerPoolState::from(state))
        .map_err(|err| StoreError::Codec(format!("buyer record postcard encode: {err}")))
}

/// Decode one buyer record into a [`BuyerPoolState`], validating that the
/// embedded `pool_id` matches the table's primary key (additive-forward-compat
/// via `take_from_bytes`; bounded trailing-bytes warning).
fn decode_record(key_bytes: [u8; 32], value_bytes: &[u8]) -> Result<BuyerPoolState, StoreError> {
    let pool_id = PoolId::from(key_bytes);
    let (stored, remainder): (StoredBuyerPoolState, &[u8]) = postcard::take_from_bytes(value_bytes)
        .map_err(|err| StoreError::Corrupt {
            pool_id: Some(pool_id),
            detail: format!("buyer record postcard decode failed (pool_id {pool_id}): {err}"),
        })?;
    if stored.pool_id != key_bytes {
        return Err(StoreError::Corrupt {
            pool_id: Some(pool_id),
            detail: format!("buyer record pool_id {pool_id} does not match table key"),
        });
    }
    if remainder.len() > SANE_TRAILER_MAX_BYTES {
        tracing::warn!(
            %pool_id,
            remainder = remainder.len(),
            limit = SANE_TRAILER_MAX_BYTES,
            event = "buyer_pool_store_excess_trailer",
            "buyer pool record has unusually large trailing bytes; possible malicious padding \
             or large-additive-field schema skew",
        );
    }
    stored.into_state(pool_id)
}

/// Load every persisted buyer pool from any readable `redb` database.
///
/// Generic over [`redb::ReadableDatabase`] so one decode-and-skip
/// implementation serves both a read-write [`Database`] — the daemon and the
/// client store while their process owns the file — and a
/// [`redb::ReadOnlyDatabase`] opened against a stopped daemon's file for
/// post-mortem inspection (#2084). The skip semantics documented on
/// [`BuyerPoolTable::load_all`] are the point: they must not fork between the
/// two callers.
///
/// # Errors
///
/// [`StoreError::Backend`] if the table or its iterator is unreadable.
pub(crate) fn load_all_from<D: ReadableDatabase>(db: &D) -> Result<BuyerLoad, StoreError> {
    let read_txn = db
        .begin_read()
        .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
    let table = match read_txn.open_table(BUYER_POOL_TABLE) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(BuyerLoad::default()),
        Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
    };
    let mut out = Vec::new();
    let mut skipped = Vec::new();
    let iter = table
        .iter()
        .map_err(|err| StoreError::Backend(format!("table iter: {err}")))?;
    for entry in iter {
        let (key_guard, value_guard) =
            entry.map_err(|err| StoreError::Backend(format!("iter entry: {err}")))?;
        let key_bytes: [u8; 32] = *key_guard.value();
        match decode_record(key_bytes, value_guard.value()) {
            Ok(state) => out.push(state),
            Err(err) => {
                let pool_id = PoolId::from(key_bytes);
                skipped.push(pool_id);
                tracing::error!(
                    %pool_id,
                    error = %err,
                    event = "buyer_pool_store_skip_undecodable_record",
                    "buyer pool hydration: skipping an undecodable record; its escrowed \
                     deposit is untracked and will not be auto-reclaimed until the record is \
                     repaired (other pools remain healthy)",
                );
            }
        }
    }
    Ok(BuyerLoad {
        pools: out,
        skipped,
    })
}

/// `db` viewed as the buyer-pool table: a typed capability over a
/// caller-owned [`Database`]. Zero-cost — it borrows and owns nothing, and
/// does no I/O until a method is called. Construct one per operation.
///
/// The *file* is the only thing the two buyer stores legitimately differ
/// on, so it is the only thing this takes. Everything else about the table
/// — name, key type, value type, record codec — is frozen in this module
/// (see the private `BUYER_POOL_TABLE`).
#[derive(Debug)]
pub struct BuyerPoolTable<'a> {
    db: &'a Database,
}

impl<'a> BuyerPoolTable<'a> {
    /// View `db` as the buyer-pool table.
    #[must_use]
    pub const fn new(db: &'a Database) -> Self {
        Self { db }
    }

    /// Begin a write transaction with fsync-on-commit durability.
    ///
    /// `Durability::Immediate` is `redb`'s current default but is set
    /// explicitly so a future default change cannot silently weaken the
    /// persistence guarantee the buyer watermark depends on. No-row
    /// operations open the table in this transaction and return without
    /// committing; dropping the transaction aborts it, rolls back the
    /// implicit table creation, and avoids both a preflight read
    /// transaction and a no-op fsync. Callers therefore commit only after
    /// an actual mutation.
    fn begin_durable_write(&self) -> Result<redb::WriteTransaction, StoreError> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        Ok(write_txn)
    }

    /// Load every persisted buyer pool.
    ///
    /// **Undecodable rows are logged and skipped, never propagated.** This
    /// is deliberately asymmetric with the seller `load_all` — whose error
    /// aborts startup, because a corrupt voucher record reopening the #527
    /// replay window is unsafe to run past. Buyer bootstrap is *non-fatal*:
    /// a propagated error here would not just disable new buys, it would
    /// stop the reclaim sweep from ever spawning, stranding every *other*
    /// tracked pool's deposit as unreclaimable (PR #753 review,
    /// alpergundogdu). One bad row must not take the others down. **Do not
    /// "unify" this with the seller path.**
    ///
    /// Logged at `error!`, not `warn!`: the skipped row's deposit stays
    /// escrowed-but-unreclaimable until an operator repairs the record, so
    /// it warrants action (and must not be filtered out of alerting). The
    /// `pool_id` is the table's primary key, so it is always recoverable
    /// even when the value bytes are not — it is the repair handle this
    /// offers (the owner, by contrast, lives inside the undecodable value
    /// bytes and is not).
    ///
    /// The returned [`BuyerLoad`] also carries every skipped pool id. This
    /// lets callers expose the condition through their own user or operator
    /// surface without coupling the incentive crate to a CLI or node
    /// metrics backend.
    ///
    /// [`BuyerPoolStore`]: crate::buyer_pool::BuyerPoolStore
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the table or its iterator is unreadable.
    pub fn load_all(&self) -> Result<BuyerLoad, StoreError> {
        load_all_from(self.db)
    }

    /// Persist (insert or overwrite) the state for one pool, keyed by
    /// `state.pool_id` (primary), maintaining `state.owner →
    /// state.pool_id` in the secondary reuse index. Durable (fsynced)
    /// before returning `Ok`.
    ///
    /// Creates both tables if absent — unlike the no-row operations, this
    /// one intends to write. If a *different* `pool_id` was previously
    /// indexed for `state.owner`, that older pool's primary row is left in
    /// place (untouched, no longer reachable via [`Self::get_by_owner`]) —
    /// only [`Self::load_all`] still enumerates it, which is what lets the
    /// reclaim sweep still recover its deposit. See the
    /// `BUYER_OWNER_INDEX_TABLE` doc comment for the secondary-index
    /// reuse-hint contract.
    ///
    /// # Errors
    ///
    /// [`StoreError::Codec`] on encode failure, [`StoreError::Backend`] if
    /// the write or fsync fails.
    pub fn record(&self, state: &BuyerPoolState) -> Result<(), StoreError> {
        let encoded = encode_record(state)?;
        let primary_key: [u8; 32] = state.pool_id.into();
        let index_key: [u8; 20] = state.owner.into();

        let write_txn = self.begin_durable_write()?;
        {
            let mut table = write_txn
                .open_table(BUYER_POOL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(&primary_key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        {
            let mut index = write_txn
                .open_table(BUYER_OWNER_INDEX_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table (index): {err}")))?;
            index
                .insert(&index_key, &primary_key)
                .map_err(|err| StoreError::Backend(format!("insert (index): {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    /// Drop the persisted entry the owner index currently maps `owner` to,
    /// removing it from both the primary table and the index. A no-op if no
    /// index entry exists for `owner`, or if neither table was ever
    /// written. An actual deletion is committed durably.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the delete or durable commit fails.
    pub fn forget(&self, owner: Address) -> Result<(), StoreError> {
        let index_key: [u8; 20] = owner.into();
        let write_txn = self.begin_durable_write()?;
        let primary_key = {
            let mut index = write_txn
                .open_table(BUYER_OWNER_INDEX_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table (index): {err}")))?;
            let Some(value_guard) = index
                .get(&index_key)
                .map_err(|err| StoreError::Backend(format!("get (index): {err}")))?
            else {
                return Ok(());
            };
            let primary_key: [u8; 32] = *value_guard.value();
            drop(value_guard);
            index
                .remove(&index_key)
                .map_err(|err| StoreError::Backend(format!("remove (index): {err}")))?;
            primary_key
        };
        {
            let mut table = write_txn
                .open_table(BUYER_POOL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .remove(&primary_key)
                .map_err(|err| StoreError::Backend(format!("remove: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    /// Point-lookup the live pool by its primary key, or `None` if none is
    /// tracked.
    ///
    /// Unlike [`Self::load_all`], a corrupt row here is **propagated**: this
    /// is not the bootstrap path, so surfacing the precise error is safe
    /// and diagnostic.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if unreadable, [`StoreError::Corrupt`] /
    /// [`StoreError::UnsupportedSchema`] if the record cannot be decoded.
    pub fn get_by_pool_id(&self, pool_id: PoolId) -> Result<Option<BuyerPoolState>, StoreError> {
        let key: [u8; 32] = pool_id.into();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(BUYER_POOL_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let Some(value_guard) = table
            .get(&key)
            .map_err(|err| StoreError::Backend(format!("get: {err}")))?
        else {
            return Ok(None);
        };
        Ok(Some(decode_record(key, value_guard.value())?))
    }

    /// Point-lookup the live pool for `owner` via the secondary reuse index
    /// (one hop to the `pool_id`, then a primary lookup), or `None` if none
    /// is tracked.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if unreadable, [`StoreError::Corrupt`] /
    /// [`StoreError::UnsupportedSchema`] if the record cannot be decoded.
    pub fn get_by_owner(&self, owner: Address) -> Result<Option<BuyerPoolState>, StoreError> {
        let index_key: [u8; 20] = owner.into();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let index = match read_txn.open_table(BUYER_OWNER_INDEX_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(StoreError::Backend(format!("open_table (index): {err}"))),
        };
        let Some(idx_guard) = index
            .get(&index_key)
            .map_err(|err| StoreError::Backend(format!("get (index): {err}")))?
        else {
            return Ok(None);
        };
        let primary_key: [u8; 32] = *idx_guard.value();
        drop(idx_guard);
        let table = match read_txn.open_table(BUYER_POOL_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let Some(value_guard) = table
            .get(&primary_key)
            .map_err(|err| StoreError::Backend(format!("get: {err}")))?
        else {
            return Ok(None);
        };
        Ok(Some(decode_record(primary_key, value_guard.value())?))
    }

    /// Compare-and-delete inside a single write transaction: remove
    /// `owner`'s indexed row only if the owner index still maps it to
    /// `pool_id`. Returns whether a row was deleted.
    ///
    /// Guards the reclaim sweep against a lost update: between the sweep
    /// loading a closed pool and forgetting it, a concurrent
    /// `open_or_reuse` may have opened a replacement under the same owner
    /// key. An unconditional [`Self::forget`] would delete the live
    /// replacement.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the read, delete, or durable commit
    /// fails.
    pub fn forget_if_pool(&self, owner: Address, pool_id: PoolId) -> Result<bool, StoreError> {
        let index_key: [u8; 20] = owner.into();
        let primary_key: [u8; 32] = pool_id.into();
        let write_txn = self.begin_durable_write()?;
        {
            let mut index = write_txn
                .open_table(BUYER_OWNER_INDEX_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table (index): {err}")))?;
            // Read the current index entry inside the same (serialised)
            // write txn so the match-and-remove is atomic against a
            // concurrent replace.
            let Some(value_guard) = index
                .get(&index_key)
                .map_err(|err| StoreError::Backend(format!("get (index): {err}")))?
            else {
                return Ok(false);
            };
            let matches = *value_guard.value() == primary_key;
            drop(value_guard);
            if !matches {
                return Ok(false);
            }
            index
                .remove(&index_key)
                .map_err(|err| StoreError::Backend(format!("remove (index): {err}")))?;
        }
        {
            let mut table = write_txn
                .open_table(BUYER_POOL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .remove(&primary_key)
                .map_err(|err| StoreError::Backend(format!("remove: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(true)
    }

    /// Atomically advance the committed progress for `lane` inside
    /// `owner`'s pool, inside a single write transaction (owner-index CAS →
    /// advance against the committed lane watermark → write).
    ///
    /// Closes the lost-update / watermark-regression race that a separate
    /// `get_by_owner` → mutate → [`Self::record`] sequence exposes when a
    /// concurrent writer (e.g. [`Self::add_deposit`]) touches the same row
    /// in the gap (#838).
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] / [`StoreError::Codec`] on I/O or codec
    /// failure.
    pub fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.write_lane(owner, pool_id, |state| {
            state.advance_lane(lane, bytes, amount)
        })
    }

    /// Atomically overwrite `lane`'s committed progress in `owner`'s pool
    /// ([`BuyerPoolState::rebase_lane`]), inside the same single write
    /// transaction and owner-index CAS as [`Self::advance_progress`].
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] / [`StoreError::Codec`] on I/O or codec
    /// failure.
    pub fn rebase_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        anchor: BuyerLaneProgress,
        totals: BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.write_lane(owner, pool_id, |state| {
            state.rebase_lane(lane, anchor, totals);
            Ok(())
        })
    }

    /// The shared body of [`Self::advance_progress`] and
    /// [`Self::rebase_progress`]: owner-index CAS, then `apply` to the
    /// committed row and write it back, all in one durable write transaction.
    fn write_lane(
        &self,
        owner: Address,
        pool_id: PoolId,
        apply: impl FnOnce(&mut BuyerPoolState) -> Result<(), BuyerProgressError>,
    ) -> Result<AdvanceOutcome, StoreError> {
        let index_key: [u8; 20] = owner.into();
        let primary_key: [u8; 32] = pool_id.into();
        let write_txn = self.begin_durable_write()?;
        {
            let index = write_txn
                .open_table(BUYER_OWNER_INDEX_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table (index): {err}")))?;
            let Some(idx_guard) = index
                .get(&index_key)
                .map_err(|err| StoreError::Backend(format!("get (index): {err}")))?
            else {
                return Ok(AdvanceOutcome::UnknownPool);
            };
            let mapped: [u8; 32] = *idx_guard.value();
            drop(idx_guard);
            if mapped != primary_key {
                return Ok(AdvanceOutcome::PoolMismatch);
            }
        }
        {
            let mut table = write_txn
                .open_table(BUYER_POOL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            // Read the committed row inside the same (serialised) write
            // txn so the advance is checked against — and written over —
            // the committed watermark, never a stale in-memory snapshot.
            let Some(value_guard) = table
                .get(&primary_key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            else {
                return Ok(AdvanceOutcome::UnknownPool);
            };
            let mut state = decode_record(primary_key, value_guard.value())?;
            // Drop the borrow of `table` held by `value_guard` before
            // mutating.
            drop(value_guard);
            if let Err(err) = apply(&mut state) {
                return Ok(AdvanceOutcome::Regressed(err));
            }
            let encoded = encode_record(&state)?;
            table
                .insert(&primary_key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(AdvanceOutcome::Advanced)
    }

    /// Atomically add `additional` to the committed deposit for `owner`'s
    /// pool inside a single write transaction.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] / [`StoreError::Codec`] on I/O or codec
    /// failure.
    pub fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        let index_key: [u8; 20] = owner.into();
        let primary_key: [u8; 32] = pool_id.into();
        let write_txn = self.begin_durable_write()?;
        {
            let index = write_txn
                .open_table(BUYER_OWNER_INDEX_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table (index): {err}")))?;
            let Some(idx_guard) = index
                .get(&index_key)
                .map_err(|err| StoreError::Backend(format!("get (index): {err}")))?
            else {
                return Ok(DepositOutcome::UnknownPool);
            };
            let mapped: [u8; 32] = *idx_guard.value();
            drop(idx_guard);
            if mapped != primary_key {
                return Ok(DepositOutcome::PoolMismatch);
            }
        }
        let new_deposit = {
            let mut table = write_txn
                .open_table(BUYER_POOL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            let Some(value_guard) = table
                .get(&primary_key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            else {
                return Ok(DepositOutcome::UnknownPool);
            };
            let mut state = decode_record(primary_key, value_guard.value())?;
            drop(value_guard);
            state.deposit = state.deposit.saturating_add(additional);
            let encoded = encode_record(&state)?;
            table
                .insert(&primary_key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
            state.deposit
        };
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(DepositOutcome::Added(new_deposit))
    }

    /// Write raw value bytes under `pool_id`'s primary key, **bypassing the
    /// encoder**, to simulate a row left undecodable by a binary downgrade.
    ///
    /// Seeds corruption against a live store for the tolerance tests here
    /// and, through store-specific test seams, for cross-crate consumer
    /// tests. Does NOT touch the owner index — callers that need
    /// `get_by_owner` to resolve to the corrupt row must index it
    /// themselves (there is no encoded state to read an owner from).
    ///
    /// Gated behind `test-util` rather than `#[cfg(test)]` because a
    /// cross-crate `cfg(test)` does not propagate: node's seam could not
    /// reach a `cfg(test)`-only method here. The feature keeps a
    /// corruption-seeding writer out of the shipped API; enable it from
    /// `[dev-dependencies]` (the same pattern `decdn-client`'s
    /// `test-util` uses).
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the write or durable commit fails.
    #[cfg(any(test, feature = "test-util"))]
    pub fn insert_raw(&self, pool_id: PoolId, bytes: &[u8]) -> Result<(), StoreError> {
        let key: [u8; 32] = pool_id.into();
        let write_txn = self.begin_durable_write()?;
        {
            let mut table = write_txn
                .open_table(BUYER_POOL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(&key, bytes)
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use tempfile::TempDir;

    use super::*;

    /// A bare `redb` database in a temp dir. These tests drive the shared
    /// table ops directly, so they need no store, no data-dir hardening,
    /// and no file layout — which is the point: the ops are agnostic about
    /// who owns the file.
    fn db() -> anyhow::Result<(TempDir, Database)> {
        let dir = tempfile::tempdir()?;
        let db = Database::create(dir.path().join("t.redb"))?;
        Ok((dir, db))
    }

    fn tbl(db: &Database) -> BuyerPoolTable<'_> {
        BuyerPoolTable::new(db)
    }

    /// `true` if the buyer table was never created. Distinguishes "no row"
    /// from "no table" so the no-row ops can be held to not creating one.
    fn table_absent(db: &Database) -> anyhow::Result<bool> {
        let read_txn = db.begin_read()?;
        Ok(matches!(
            read_txn.open_table(BUYER_POOL_TABLE),
            Err(redb::TableError::TableDoesNotExist(_))
        ))
    }

    /// Every `byte` gets a **distinct** `pool_id` too (derived from the same
    /// byte): `pool_id` is the store's primary key, so two fixtures sharing
    /// one `pool_id` would collide in the primary table instead of
    /// coexisting as two independent pools. One lane, keyed off `owner` as
    /// both signer and provider seed byte, tracks the sample's cumulative
    /// progress.
    fn state(byte: u8) -> BuyerPoolState {
        let mut id = [0u8; 32];
        id[31] = byte;
        let mut own = [0u8; 20];
        own[19] = byte;
        let owner = Address::from(own);
        let pool_id = PoolId::from(id);
        let mut s = BuyerPoolState::new(
            pool_id,
            Address::repeat_byte(0x9c),
            owner,
            address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            U256::from(10_000_000u64),
        );
        let lane = LaneKey {
            pool_id,
            signer: owner,
            provider: address!("00000000000000000000000000000000000000b2"),
        };
        let _ = s.advance_lane(
            lane,
            U256::from(byte) * U256::from(1_024u64),
            U256::from(byte) * U256::from(1_000u64),
        );
        s
    }

    /// Mirrors `state(1)`, named for the round-trip test below.
    fn sample_state() -> BuyerPoolState {
        state(1)
    }

    const OTHER_POOL: PoolId =
        b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

    /// Golden fixture: two distinct lanes with distinct byte patterns in
    /// every field. Postcard writes fields (and vec elements) positionally,
    /// so a golden can only catch a reordering if the swapped fields encode
    /// differently — every field here holds a unique byte pattern.
    fn golden_state() -> anyhow::Result<BuyerPoolState> {
        let pool_id = B256::repeat_byte(0x11);
        let owner = Address::repeat_byte(0x22);
        let lane_a = LaneKey {
            pool_id,
            signer: Address::repeat_byte(0x66),
            provider: Address::repeat_byte(0x77),
        };
        let lane_b = LaneKey {
            pool_id,
            signer: Address::repeat_byte(0x88),
            provider: Address::repeat_byte(0x99),
        };
        let mut s = BuyerPoolState::new(
            pool_id,
            Address::repeat_byte(0x9c),
            owner,
            Address::repeat_byte(0x33),
            U256::from(0xAAAA_AAAA_AAAA_AAAAu64),
        );
        s.advance_lane(
            lane_a,
            U256::from(0xDDDD_DDDD_DDDD_DDDDu64),
            U256::from(0xBBBB_BBBB_BBBB_BBBBu64),
        )?;
        s.advance_lane(
            lane_b,
            U256::from(0xEEEE_EEEE_EEEE_EEEEu64),
            U256::from(0xCCCC_CCCC_CCCC_CCCCu64),
        )?;
        Ok(s)
    }

    use alloy::primitives::B256;

    /// Postcard encoding of [`golden_state`] (schema v2). Two lanes, sorted
    /// by `(signer, provider)` for a deterministic encoding regardless of
    /// `HashMap` iteration order.
    ///
    /// The lane row is the two cumulatives and nothing else. A chain hangs off
    /// the anchor in its own opening voucher (ADR 003 §One chain per lane), so
    /// `last_amount` already says which chain the lane resumes on — there is no
    /// counter here to keep, and none to get wrong.
    const GOLDEN_RECORD_HEX: &str = concat!(
        "02",                                                               // schema_version (varint)
        "1111111111111111111111111111111111111111111111111111111111111111", // pool_id
        "9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c",                         // payment_pool
        "2222222222222222222222222222222222222222",                         // owner
        "3333333333333333333333333333333333333333",                         // token
        "000000000000000000000000000000000000000000000000aaaaaaaaaaaaaaaa", // deposit
        "02",                                       // lanes: length prefix (2 entries)
        "6666666666666666666666666666666666666666", // lane a: signer
        "7777777777777777777777777777777777777777", // lane a: provider
        "000000000000000000000000000000000000000000000000bbbbbbbbbbbbbbbb", // lane a: last_amount
        "000000000000000000000000000000000000000000000000dddddddddddddddd", // lane a: last_bytes
        "8888888888888888888888888888888888888888", // lane b: signer
        "9999999999999999999999999999999999999999", // lane b: provider
        "000000000000000000000000000000000000000000000000cccccccccccccccc", // lane b: last_amount
        "000000000000000000000000000000000000000000000000eeeeeeeeeeeeeeee", // lane b: last_bytes
    );

    /// Lowercase hex of `bytes`. `fold` + `write!` rather than the obvious
    /// `map(format!).collect()`, which trips `clippy::format_collect`.
    fn hex_of(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        bytes
            .iter()
            .fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
                // Infallible: writing to a String never errors.
                let _ = write!(acc, "{b:02x}");
                acc
            })
    }

    /// The buyer record is a **frozen on-disk format**, shared by the
    /// node's `buyer.redb` and the client's `buyer-pools.redb`. Postcard
    /// encodes struct fields positionally and unnamed, so reordering,
    /// retyping, or inserting a field rewrites the bytes with no compile
    /// error and no other test failure — every store the suites build is a
    /// fresh tempdir, so they would all still pass while every record an
    /// older binary wrote was silently orphaned.
    ///
    /// This golden is the tripwire for that, and the only test here that
    /// would fail on such a change.
    #[test]
    fn encode_is_byte_stable() -> anyhow::Result<()> {
        let hex = hex_of(&encode_record(&golden_state()?)?);
        anyhow::ensure!(
            hex == GOLDEN_RECORD_HEX,
            "the buyer record's on-disk encoding changed — this orphans every existing record in \
             both `buyer.redb` and `buyer-pools.redb`.\n  got:  {hex}\n  want: {GOLDEN_RECORD_HEX}",
        );
        Ok(())
    }

    /// The frozen bytes must also *decode* back to the fixture — the
    /// direction that actually matters (an older binary's record still
    /// loads).
    #[test]
    fn golden_bytes_still_decode() -> anyhow::Result<()> {
        let bytes: Vec<u8> = (0..GOLDEN_RECORD_HEX.len() / 2)
            .map(|i| {
                GOLDEN_RECORD_HEX
                    .get(i * 2..i * 2 + 2)
                    .ok_or_else(|| anyhow::anyhow!("odd-length golden hex"))
                    .and_then(|b| u8::from_str_radix(b, 16).map_err(Into::into))
            })
            .collect::<anyhow::Result<_>>()?;
        let want = golden_state()?;
        let got = decode_record(want.pool_id.into(), &bytes)?;
        anyhow::ensure!(
            got == want,
            "frozen bytes no longer decode:\n {got:?}\n {want:?}"
        );
        Ok(())
    }

    #[test]
    fn open_empty_store_returns_no_entries() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        anyhow::ensure!(tbl(&db).load_all()?.pools.is_empty());
        anyhow::ensure!(tbl(&db).get_by_owner(state(1).owner)?.is_none());
        anyhow::ensure!(tbl(&db).get_by_pool_id(state(1).pool_id)?.is_none());
        Ok(())
    }

    #[test]
    fn record_round_trips_and_keys_by_pool_id() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = sample_state();
        tbl(&db).record(&s)?;

        let by_id = tbl(&db)
            .get_by_pool_id(s.pool_id)?
            .ok_or_else(|| anyhow::anyhow!("missing by pool_id"))?;
        anyhow::ensure!(by_id == s);

        let by_owner = tbl(&db)
            .get_by_owner(s.owner)?
            .ok_or_else(|| anyhow::anyhow!("missing by owner"))?;
        anyhow::ensure!(by_owner.pool_id == s.pool_id);
        anyhow::ensure!(by_owner == s);
        Ok(())
    }

    #[test]
    fn record_get_and_persist_across_reopen() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("t.redb");
        let a = state(1);
        let b = state(2);
        {
            let db = Database::create(&path)?;
            tbl(&db).record(&a)?;
            tbl(&db).record(&b)?;
            let got = tbl(&db)
                .get_by_owner(a.owner)?
                .ok_or_else(|| anyhow::anyhow!("missing a"))?;
            anyhow::ensure!(got == a, "get_by_owner must round-trip");
        }
        // Reopen the same file: records survive.
        let db = Database::create(&path)?;
        let mut load = tbl(&db).load_all()?;
        load.pools.sort_by_key(|s| s.owner);
        anyhow::ensure!(load.pools.len() == 2);
        anyhow::ensure!(*load.pools.first().ok_or_else(|| anyhow::anyhow!("[0]"))? == a);
        anyhow::ensure!(*load.pools.get(1).ok_or_else(|| anyhow::anyhow!("[1]"))? == b);
        Ok(())
    }

    #[test]
    fn record_overwrites_by_owner() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let mut s = state(5);
        tbl(&db).record(&s)?;
        s.deposit = U256::from(20_000_000u64);
        tbl(&db).record(&s)?;
        anyhow::ensure!(
            tbl(&db).load_all()?.pools.len() == 1,
            "same owner overwrites"
        );
        let only = tbl(&db)
            .get_by_owner(s.owner)?
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(only.deposit == U256::from(20_000_000u64));
        Ok(())
    }

    #[test]
    fn load_all_returns_every_recorded_pool() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        for byte in 1..=4u8 {
            tbl(&db).record(&state(byte))?;
        }
        let mut got: Vec<_> = tbl(&db).load_all()?.pools.iter().map(|s| s.owner).collect();
        got.sort_unstable();
        let want: Vec<_> = (1..=4u8).map(|b| state(b).owner).collect();
        anyhow::ensure!(got == want, "load_all must return every owner, got {got:?}");
        Ok(())
    }

    #[test]
    fn forget_removes_entry_and_is_noop_when_absent() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(3);
        tbl(&db).record(&s)?;
        tbl(&db).forget(s.owner)?;
        anyhow::ensure!(tbl(&db).load_all()?.pools.is_empty());
        // forget on an unknown owner is a no-op.
        tbl(&db).forget(address!("00000000000000000000000000000000000000ff"))?;
        Ok(())
    }

    /// A buyer record stamped with a future schema version must NOT fail
    /// hydration — that would disable the whole buyer path and the reclaim
    /// sweep (PR #753 review). `load_all` skips it; the point lookup still
    /// surfaces the precise error.
    /// An OLDER record is rejected, not decoded.
    ///
    /// This is why the version is matched exactly rather than as a ceiling. The
    /// fields are positional and `payment_pool` sits third, so a v1 record —
    /// which has no such field — would decode `owner` into `payment_pool`,
    /// `token` into `owner`, and so on: every field after `pool_id` shifted by
    /// one, with no error. A silently wrong deployment tag is the one outcome
    /// this whole change exists to prevent, so the reader refuses it instead.
    #[test]
    fn older_schema_version_is_rejected_not_misdecoded() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(1);
        let mut stored = StoredBuyerPoolState::from(&s);
        stored.schema_version = 1;
        let encoded = postcard::to_allocvec(&stored)?;
        tbl(&db).insert_raw(s.pool_id, &encoded)?;

        let load = tbl(&db).load_all()?;
        anyhow::ensure!(
            load.pools.is_empty(),
            "an older-schema record must be skipped by load_all, never hydrated",
        );
        anyhow::ensure!(load.skipped == vec![s.pool_id]);
        let err = tbl(&db)
            .get_by_pool_id(s.pool_id)
            .err()
            .ok_or_else(|| anyhow::anyhow!("an older schema must reject on get_by_pool_id"))?;
        anyhow::ensure!(
            matches!(
                err,
                StoreError::UnsupportedSchema { found, supported }
                    if found == 1 && supported == BUYER_SUPPORTED_SCHEMA_VERSION
            ),
            "expected UnsupportedSchema for the older record, got {err:?}"
        );
        Ok(())
    }

    #[test]
    fn future_schema_version_skipped_on_hydration() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(1);
        let mut stored = StoredBuyerPoolState::from(&s);
        stored.schema_version = BUYER_SUPPORTED_SCHEMA_VERSION + 1;
        let encoded = postcard::to_allocvec(&stored)?;
        tbl(&db).insert_raw(s.pool_id, &encoded)?;

        let load = tbl(&db).load_all()?;
        anyhow::ensure!(
            load.pools.is_empty(),
            "future-schema record must be skipped, not propagated, by load_all",
        );
        anyhow::ensure!(load.skipped == vec![s.pool_id]);
        let err = tbl(&db)
            .get_by_pool_id(s.pool_id)
            .err()
            .ok_or_else(|| anyhow::anyhow!("future schema must reject on get_by_pool_id"))?;
        anyhow::ensure!(
            matches!(
                err,
                StoreError::UnsupportedSchema { found, supported }
                    if found == BUYER_SUPPORTED_SCHEMA_VERSION + 1
                        && supported == BUYER_SUPPORTED_SCHEMA_VERSION,
            ),
            "expected UnsupportedSchema, got {err:?}",
        );
        Ok(())
    }

    /// Garbage value bytes under a real buyer key are skipped by `load_all`
    /// (one bad row must not strand every other pool's deposit — PR #753
    /// review), while a healthy record alongside it survives.
    #[test]
    fn corrupt_value_bytes_skipped_keeps_healthy() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let healthy = state(2);
        tbl(&db).record(&healthy)?;
        tbl(&db).insert_raw(state(3).pool_id, &[0u8; 8])?; // far too short

        let load = tbl(&db).load_all()?;
        anyhow::ensure!(
            load.pools.len() == 1 && load.pools.first() == Some(&healthy),
            "corrupt row must be skipped while the healthy row survives, got {load:?}",
        );
        anyhow::ensure!(
            load.skipped == vec![state(3).pool_id],
            "the skipped pool_id is the only repair handle, got {load:?}",
        );
        let err = tbl(&db)
            .get_by_pool_id(state(3).pool_id)
            .err()
            .ok_or_else(|| anyhow::anyhow!("garbage value must reject on get_by_pool_id"))?;
        anyhow::ensure!(
            matches!(&err, StoreError::Corrupt { detail, .. } if detail.contains("postcard decode")),
            "expected Corrupt(postcard decode), got {err:?}",
        );
        Ok(())
    }

    /// A record whose embedded `pool_id` doesn't match its table key is
    /// skipped by `load_all`; the point lookup still surfaces `Corrupt`.
    #[test]
    fn pool_id_key_mismatch_skipped_on_hydration() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s_for_a = state(0xAA);
        let encoded = postcard::to_allocvec(&StoredBuyerPoolState::from(&s_for_a))?;
        // Filed under a *different* pool_id's key.
        tbl(&db).insert_raw(state(0xBB).pool_id, &encoded)?;

        let load = tbl(&db).load_all()?;
        anyhow::ensure!(
            load.pools.is_empty(),
            "pool_id/key-mismatch record must be skipped by load_all",
        );
        anyhow::ensure!(load.skipped == vec![state(0xBB).pool_id]);
        let err = tbl(&db)
            .get_by_pool_id(state(0xBB).pool_id)
            .err()
            .ok_or_else(|| anyhow::anyhow!("pool_id/key mismatch must reject on get_by_pool_id"))?;
        anyhow::ensure!(
            matches!(&err, StoreError::Corrupt { detail, .. } if detail.contains("does not match table key")),
            "expected Corrupt(does not match table key), got {err:?}",
        );
        Ok(())
    }

    /// Forward-compat: a future writer's additive trailing bytes after the
    /// buyer prefix decode cleanly (`take_from_bytes`).
    #[test]
    fn extra_trailing_bytes_are_tolerated() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(0x42);
        let mut encoded = postcard::to_allocvec(&StoredBuyerPoolState::from(&s))?;
        encoded.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01]);
        tbl(&db).insert_raw(s.pool_id, &encoded)?;

        let load = tbl(&db).load_all()?;
        anyhow::ensure!(load.pools.len() == 1);
        let only = load
            .pools
            .first()
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(*only == s, "prefix must decode despite trailing bytes");
        Ok(())
    }

    /// `forget_if_pool` (compare-and-delete) deletes only the matching
    /// pool — the lost-update guard for the reclaim sweep.
    #[test]
    fn forget_if_pool_is_compare_and_delete() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        // CAS on a never-written store is a no-op (false).
        anyhow::ensure!(!tbl(&db).forget_if_pool(state(1).owner, state(1).pool_id)?);

        let s = state(4);
        tbl(&db).record(&s)?;
        // Wrong pool id → not deleted, row survives.
        anyhow::ensure!(
            !tbl(&db).forget_if_pool(s.owner, OTHER_POOL)?,
            "mismatched pool must not delete"
        );
        anyhow::ensure!(
            tbl(&db).get_by_owner(s.owner)?.is_some(),
            "row must survive"
        );
        // Matching pool id → deleted.
        anyhow::ensure!(tbl(&db).forget_if_pool(s.owner, s.pool_id)?);
        anyhow::ensure!(tbl(&db).get_by_owner(s.owner)?.is_none());
        Ok(())
    }

    /// #838: a stale `advance_progress` reporting totals below the
    /// committed watermark is rejected and leaves the committed watermark
    /// intact.
    #[test]
    fn advance_progress_cannot_regress_committed_watermark() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(8);
        tbl(&db).record(&s)?;
        let lane = s
            .lanes()
            .next()
            .map(|(k, _)| k)
            .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
        let progress = s
            .lane_progress(lane)
            .ok_or_else(|| anyhow::anyhow!("missing progress"))?;

        let outcome = tbl(&db).advance_progress(
            s.owner,
            s.pool_id,
            lane,
            progress.last_bytes - U256::from(1u64),
            progress.last_amount,
        )?;
        anyhow::ensure!(
            matches!(outcome, AdvanceOutcome::Regressed(_)),
            "stale progress must be rejected, got {outcome:?}"
        );
        let stored = tbl(&db)
            .get_by_owner(s.owner)?
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(
            stored.lane_progress(lane) == Some(progress),
            "committed watermark regressed"
        );
        Ok(())
    }

    /// A rebase overwrites the committed watermark DOWN (the ledger moved to the
    /// node's watermark), and keeps the same pool-id guard as an advance: it
    /// never clobbers a row replaced by a newer open.
    #[test]
    fn rebase_progress_overwrites_down_and_guards_the_pool() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(8);
        tbl(&db).record(&s)?;
        let lane = s
            .lanes()
            .next()
            .map(|(k, _)| k)
            .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
        let progress = s
            .lane_progress(lane)
            .ok_or_else(|| anyhow::anyhow!("missing progress"))?;
        let anchor = BuyerLaneProgress {
            last_amount: progress.last_amount - U256::from(10u64),
            last_bytes: progress.last_bytes - U256::from(10u64),
        };
        let totals = BuyerLaneProgress {
            last_amount: anchor.last_amount + U256::from(3u64),
            last_bytes: anchor.last_bytes + U256::from(3u64),
        };

        let wrong_pool = PoolId::repeat_byte(0xEE);
        let outcome = tbl(&db).rebase_progress(s.owner, wrong_pool, lane, anchor, totals)?;
        anyhow::ensure!(
            matches!(outcome, AdvanceOutcome::PoolMismatch),
            "a rebase against a replaced pool must not write, got {outcome:?}"
        );

        let outcome = tbl(&db).rebase_progress(s.owner, s.pool_id, lane, anchor, totals)?;
        anyhow::ensure!(
            matches!(outcome, AdvanceOutcome::Advanced),
            "a rebase must write, got {outcome:?}"
        );
        let stored = tbl(&db)
            .get_by_owner(s.owner)?
            .and_then(|row| row.lane_progress(lane))
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(
            stored == totals,
            "the rebase must overwrite down to the anchor and advance to the totals, got \
             {stored:?}"
        );

        // Totals below the anchor record the anchor alone.
        let low = BuyerLaneProgress {
            last_amount: anchor.last_amount - U256::from(1u64),
            last_bytes: anchor.last_bytes,
        };
        let outcome = tbl(&db).rebase_progress(s.owner, s.pool_id, lane, anchor, low)?;
        anyhow::ensure!(
            matches!(outcome, AdvanceOutcome::Advanced),
            "got {outcome:?}"
        );
        let stored = tbl(&db)
            .get_by_owner(s.owner)?
            .and_then(|row| row.lane_progress(lane))
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(stored == anchor, "got {stored:?}");
        Ok(())
    }

    /// #838: the atomic mutators are pool-id guarded (a row replaced by a
    /// newer open for the same owner is not clobbered) and report unknown
    /// owners.
    #[test]
    fn advance_and_deposit_guard_on_pool_and_owner() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let ghost = state(1);
        let ghost_lane = ghost
            .lanes()
            .next()
            .map(|(k, _)| k)
            .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
        anyhow::ensure!(
            tbl(&db).advance_progress(
                ghost.owner,
                ghost.pool_id,
                ghost_lane,
                U256::from(1u64),
                U256::from(1u64),
            )? == AdvanceOutcome::UnknownPool
        );
        anyhow::ensure!(
            tbl(&db).add_deposit(ghost.owner, ghost.pool_id, U256::from(1u64))?
                == DepositOutcome::UnknownPool
        );

        let s = state(9);
        tbl(&db).record(&s)?;
        let lane = s
            .lanes()
            .next()
            .map(|(k, _)| k)
            .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;

        // With the table now present, an unrecorded owner must still report
        // UnknownPool. Both these calls and the never-written-store calls
        // above return from inside the write transaction.
        let absent = state(0x5A);
        let absent_lane = absent
            .lanes()
            .next()
            .map(|(k, _)| k)
            .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
        anyhow::ensure!(
            tbl(&db).advance_progress(
                absent.owner,
                absent.pool_id,
                absent_lane,
                U256::from(1u64),
                U256::from(1u64),
            )? == AdvanceOutcome::UnknownPool,
            "advance_progress on an absent row of an existing table"
        );
        anyhow::ensure!(
            tbl(&db).add_deposit(absent.owner, absent.pool_id, U256::from(1u64))?
                == DepositOutcome::UnknownPool,
            "add_deposit on an absent row of an existing table"
        );

        anyhow::ensure!(
            tbl(&db).advance_progress(
                s.owner,
                OTHER_POOL,
                lane,
                U256::from(99u64),
                U256::from(99u64),
            )? == AdvanceOutcome::PoolMismatch
        );
        anyhow::ensure!(
            tbl(&db).add_deposit(s.owner, OTHER_POOL, U256::from(1u64))?
                == DepositOutcome::PoolMismatch
        );
        let stored = tbl(&db)
            .get_by_owner(s.owner)?
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(stored == s, "mismatched calls must not mutate the row");
        Ok(())
    }

    /// The no-row operations must not implicitly create the table on a
    /// never-written store. Each opens it inside a write transaction, then
    /// returns without committing; abort-on-drop must roll the creation
    /// back. Committing any of these no-write paths leaves the table behind
    /// and makes the corresponding assertion fail.
    #[test]
    fn no_row_ops_do_not_create_the_table() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(1);
        let lane = s
            .lanes()
            .next()
            .map(|(k, _)| k)
            .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;

        tbl(&db).forget(s.owner)?;
        anyhow::ensure!(table_absent(&db)?, "forget created the table");

        anyhow::ensure!(!tbl(&db).forget_if_pool(s.owner, s.pool_id)?);
        anyhow::ensure!(table_absent(&db)?, "forget_if_pool created the table");

        anyhow::ensure!(
            tbl(&db).advance_progress(
                s.owner,
                s.pool_id,
                lane,
                U256::from(1u64),
                U256::from(1u64),
            )? == AdvanceOutcome::UnknownPool
        );
        anyhow::ensure!(table_absent(&db)?, "advance_progress created the table");

        anyhow::ensure!(
            tbl(&db).add_deposit(s.owner, s.pool_id, U256::from(1u64))?
                == DepositOutcome::UnknownPool
        );
        anyhow::ensure!(table_absent(&db)?, "add_deposit created the table");

        // `record`, by contrast, is supposed to create it.
        tbl(&db).record(&s)?;
        anyhow::ensure!(!table_absent(&db)?, "record must create the table");
        Ok(())
    }

    /// Real deletions still honour the `Durability::Immediate` contract:
    /// both unconditional and compare-and-delete removals survive a
    /// database reopen.
    #[test]
    fn forget_deletions_survive_reopen() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("t.redb");
        let forgotten = state(0x31);
        let compare_deleted = state(0x32);
        let retained = state(0x33);
        {
            let db = Database::create(&path)?;
            tbl(&db).record(&forgotten)?;
            tbl(&db).record(&compare_deleted)?;
            tbl(&db).record(&retained)?;

            tbl(&db).forget(forgotten.owner)?;
            anyhow::ensure!(
                tbl(&db).forget_if_pool(compare_deleted.owner, compare_deleted.pool_id)?
            );
        }

        let db = Database::create(&path)?;
        anyhow::ensure!(tbl(&db).get_by_owner(forgotten.owner)?.is_none());
        anyhow::ensure!(tbl(&db).get_by_owner(compare_deleted.owner)?.is_none());
        anyhow::ensure!(
            tbl(&db).get_by_owner(retained.owner)? == Some(retained),
            "unrelated row must survive both durable deletions"
        );
        Ok(())
    }

    /// #838: the atomic mutators honour the `Durability::Immediate` "MUST
    /// commit durably" contract — an `advance_progress` + `add_deposit`
    /// survive a close/reopen, while a no-write `PoolMismatch` persists
    /// nothing.
    #[test]
    fn advance_and_deposit_survive_reopen() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("t.redb");
        let s = state(2);
        let lane = s
            .lanes()
            .next()
            .map(|(k, _)| k)
            .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
        let progress = s
            .lane_progress(lane)
            .ok_or_else(|| anyhow::anyhow!("missing progress"))?;
        {
            let db = Database::create(&path)?;
            tbl(&db).record(&s)?;
            anyhow::ensure!(
                tbl(&db).advance_progress(
                    s.owner,
                    s.pool_id,
                    lane,
                    progress.last_bytes + U256::from(3_000u64),
                    progress.last_amount + U256::from(30u64),
                )? == AdvanceOutcome::Advanced
            );
            anyhow::ensure!(
                tbl(&db).add_deposit(s.owner, s.pool_id, U256::from(40u64))?
                    == DepositOutcome::Added(s.deposit + U256::from(40u64))
            );
            // A mismatched (no-write) call must leave nothing extra to
            // persist.
            anyhow::ensure!(
                tbl(&db).add_deposit(s.owner, OTHER_POOL, U256::from(1u64))?
                    == DepositOutcome::PoolMismatch
            );
        }
        let db = Database::create(&path)?;
        let reopened = tbl(&db)
            .get_by_owner(s.owner)?
            .ok_or_else(|| anyhow::anyhow!("row vanished across reopen"))?;
        let reopened_progress = reopened
            .lane_progress(lane)
            .ok_or_else(|| anyhow::anyhow!("lane vanished across reopen"))?;
        anyhow::ensure!(reopened_progress.last_bytes == progress.last_bytes + U256::from(3_000u64));
        anyhow::ensure!(reopened_progress.last_amount == progress.last_amount + U256::from(30u64));
        anyhow::ensure!(
            reopened.deposit == s.deposit + U256::from(40u64),
            "mismatched call must not have altered the deposit"
        );
        Ok(())
    }

    /// #838: interleaving `add_deposit` with `advance_progress` on the same
    /// owner row must lose neither the deposit accrual nor the watermark
    /// advance. The pre-fix `get → mutate → record` (read outside the write
    /// txn) would clobber one writer with the other's stale snapshot; the
    /// atomic in-txn mutators serialise correctly.
    #[test]
    fn concurrent_top_up_and_progress_preserve_both() -> anyhow::Result<()> {
        const N: u64 = 300;
        let (_d, db) = db()?;
        let db = std::sync::Arc::new(db);
        let mut base = BuyerPoolState::new(
            PoolId::repeat_byte(6),
            Address::repeat_byte(0x9c),
            Address::repeat_byte(6),
            address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            U256::from(1_000u64),
        );
        let lane = LaneKey {
            pool_id: base.pool_id,
            signer: base.owner,
            provider: address!("00000000000000000000000000000000000000b6"),
        };
        base.advance_lane(lane, U256::ZERO, U256::ZERO)?;
        tbl(&db).record(&base)?;
        let (owner, pool_id) = (base.owner, base.pool_id);

        let depositor = std::sync::Arc::clone(&db);
        let deposit_thread = std::thread::spawn(move || -> anyhow::Result<()> {
            for _ in 0..N {
                let outcome = tbl(&depositor).add_deposit(owner, pool_id, U256::from(1u64))?;
                anyhow::ensure!(
                    matches!(outcome, DepositOutcome::Added(_)),
                    "deposit outcome {outcome:?}"
                );
            }
            Ok(())
        });
        let advancer = std::sync::Arc::clone(&db);
        let progress_thread = std::thread::spawn(move || -> anyhow::Result<()> {
            for i in 1..=N {
                let outcome = tbl(&advancer).advance_progress(
                    owner,
                    pool_id,
                    lane,
                    U256::from(i) * U256::from(1_024u64),
                    U256::from(i) * U256::from(10u64),
                )?;
                anyhow::ensure!(
                    outcome == AdvanceOutcome::Advanced,
                    "advance outcome {outcome:?}"
                );
            }
            Ok(())
        });
        deposit_thread
            .join()
            .map_err(|_| anyhow::anyhow!("deposit thread panicked"))??;
        progress_thread
            .join()
            .map_err(|_| anyhow::anyhow!("progress thread panicked"))??;

        let final_row = tbl(&db)
            .get_by_owner(owner)?
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(
            final_row.deposit == U256::from(1_000u64) + U256::from(N),
            "lost a top-up: deposit = {}",
            final_row.deposit
        );
        let final_progress = final_row
            .lane_progress(lane)
            .ok_or_else(|| anyhow::anyhow!("lane vanished"))?;
        anyhow::ensure!(
            final_progress.last_amount == U256::from(N) * U256::from(10u64),
            "watermark not fully advanced: last_amount = {}",
            final_progress.last_amount
        );
        Ok(())
    }
}
