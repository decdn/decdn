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
use crate::deployment::Deployment;
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
/// The primary key is `pool_id` (32 bytes); the value carries the
/// [`Deployment`] the pool lives on (chain id and `PaymentPool` address), a
/// variable-length per-lane progress table, and the redeemed spend no tracked
/// lane accounts for ([`BuyerPoolState::redeemed_elsewhere`]). The exact-match
/// `schema_version` refuses a row in any other layout, and a point lookup that
/// errors fails the fetch, where a missing table reads as no row.
///
/// **A layout change bumps [`BUYER_SUPPORTED_SCHEMA_VERSION`] and this
/// table-name suffix together**, an appended field included. Bumping the
/// version alone leaves every existing row in place to fail its point lookup,
/// which stops the buyer path until the store is moved aside; renaming the
/// table makes the old rows unreachable instead.
///
/// A file that holds no table of this name reads as empty: `open_table`
/// reports `TableDoesNotExist` and every read path treats the store as having
/// no rows. The caller (the node, or `decdn fetch` / `bundle pull`) then
/// re-adopts a live pool on the configured contract from chain where its
/// adoption rules allow.
const BUYER_POOL_TABLE: TableDefinition<'static, &'static [u8; 32], &'static [u8]> =
    TableDefinition::new("buyer_pool_state_v1");

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
    TableDefinition::new("buyer_pool_owner_index_v1");

/// The buyer-record `schema_version` this binary reads and writes.
///
/// Matched exactly, not as a ceiling. The fields are positional, so a record
/// written under a different version does not decode into these fields — it
/// decodes into the wrong ones, silently. Refusing anything that is not this
/// exact layout is the only answer that cannot mis-map.
const BUYER_SUPPORTED_SCHEMA_VERSION: u32 = 1;

/// Sanity ceiling on trailing bytes per record. Trailing bytes are tolerated
/// so decode does not fail on padding (a layout change still bumps
/// `schema_version`), but a `remainder.len()`
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
/// `schema_version` lives in the value, not the key, and decode uses
/// [`postcard::take_from_bytes`], which tolerates trailing bytes. Any layout
/// change, an appended field included, bumps `schema_version` and the
/// table-name suffix (see [`BUYER_POOL_TABLE`]): the exact-match version check
/// refuses a row in any other layout, and a refused row fails the point
/// lookup. A field inserted anywhere but the end shifts every field after it.
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
    /// [`Deployment::chain_id`], big-endian.
    chain_id: [u8; 8],
    payment_pool: [u8; 20],
    owner: [u8; 20],
    token: [u8; 20],
    deposit: [u8; 32],
    lanes: Vec<StoredLane>,
    /// [`BuyerPoolState::redeemed_elsewhere`], big-endian.
    redeemed_elsewhere: [u8; 32],
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
            chain_id: state.deployment.chain_id.to_be_bytes(),
            payment_pool: state.deployment.payment_pool.into(),
            owner: state.owner.into(),
            token: state.token.into(),
            deposit: state.deposit.to_be_bytes(),
            lanes,
            redeemed_elsewhere: state.redeemed_elsewhere().to_be_bytes(),
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
            Deployment {
                chain_id: u64::from_be_bytes(self.chain_id),
                payment_pool: Address::from(self.payment_pool),
            },
            Address::from(self.owner),
            Address::from(self.token),
            U256::from_be_bytes(self.deposit),
            lanes,
            U256::from_be_bytes(self.redeemed_elsewhere),
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

    /// Atomically record `lane`'s on-chain watermark in `owner`'s pool
    /// ([`BuyerPoolState::seed_lane`]), inside the same single write
    /// transaction and owner-index CAS as [`Self::advance_progress`].
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] / [`StoreError::Codec`] on I/O or codec
    /// failure.
    pub fn seed_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.write_lane(owner, pool_id, |state| state.seed_lane(lane, bytes, amount))
    }

    /// The shared body of [`Self::advance_progress`],
    /// [`Self::seed_progress`] and [`Self::rebase_progress`]: owner-index CAS, then `apply` to the
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
mod tests;
