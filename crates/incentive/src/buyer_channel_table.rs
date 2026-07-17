//! The buyer-channel `redb` table: its on-disk record codec and its operations
//! (#1246).
//!
//! **This module is the single definition of the buyer table's on-disk format
//! and transactional semantics.** Two stores persist buyer channels, and they
//! differ only in *which file they own*:
//!
//! - `buyer_channel_redb::RedbBuyerChannelStore` owns a buyer-only
//!   `buyer-channels.redb` for the client (`decdn fetch`, #940). (Code span, not
//!   a link: that module exists only under the `redb` feature, so linking it
//!   would break a `buyer-store-core`-only doc build.)
//! - `decdn-node`'s `channel_store::PersistentChannelStateStore` owns a combined
//!   `channels.redb` holding the seller, buyer, pending-settle, and
//!   watcher-checkpoint tables — one file, because `redb` forbids two
//!   `Database` handles on the same file.
//!
//! That is a file-ownership difference, not a logic difference, so both stores
//! supply only their own `Database` and delegate the actual work to
//! [`BuyerChannelTable`]. Before #1246 each store carried its own copy of this
//! code, and the byte-compatibility contract between them was asserted only by a
//! doc comment; now it is the same code, and `tests::encode_is_byte_stable` pins
//! the bytes.
//!
//! The operations here never assume they own the file or that the buyer table is
//! the only table in it — that is what makes the node's multi-table layout safe
//! to serve from the same code.

use alloy::primitives::{Address, B256, U256};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::buyer_channel::{AdvanceOutcome, BuyerChannelState, DepositOutcome};
use crate::channel::ChannelId;
use crate::store::StoreError;

/// The buyer table: name, key type, and value type.
///
/// **The whole thing is a frozen on-disk identifier**, and it is deliberately
/// private and non-configurable. Changing the name orphans every existing record
/// in both the node's `channels.redb` and the client's `buyer-channels.redb`;
/// changing the key/value types breaks them harder still, because `redb`
/// persists key/value *type names* in the table metadata and refuses to open a
/// table whose types don't match — at runtime, not compile time.
///
/// Callers supply the `Database` (the one thing they legitimately differ on) and
/// nothing else. An earlier draft let each store pass its own `TableDefinition`;
/// that parameter had exactly one valid inhabitant, and a typo'd name would have
/// compiled, read an empty table, and told the reclaim sweep there was nothing to
/// reclaim. A frozen global is not a per-call knob.
///
/// The `_v1` suffix is a version tag: a future breaking layout change ships as
/// `_v2` with a one-shot migration on open, while additive changes stay on `_v1`
/// (decode tolerates unknown trailing bytes).
const BUYER_CHANNEL_TABLE: TableDefinition<'static, &'static [u8; 20], &'static [u8]> =
    TableDefinition::new("buyer_channel_state_v1");

/// Highest buyer-record `schema_version` this binary can decode. Independent of
/// the node's seller table — the buyer table is new in #744 with no legacy
/// records, so it starts at 1 and carries `expires_at` inline rather than as a
/// trailing segment.
const BUYER_SUPPORTED_SCHEMA_VERSION: u32 = 1;

/// Sanity ceiling on trailing bytes per record. Trailing bytes are tolerated
/// (forward-compat with additive schema changes), but a `remainder.len()` above
/// this is logged so an honest schema-skew incident or a malicious padding
/// attempt is observable in operator logs without re-introducing the
/// strict-decoding regression issue #527's reviewers warned against.
const SANE_TRAILER_MAX_BYTES: usize = 256;

/// On-disk buyer-channel record. All numeric fields are fixed-size big-endian
/// byte arrays so the encoded width is stable across postcard versions.
/// `schema_version` lives in the value (not the key) so a future additive field
/// can ship without renaming the table — decode uses
/// [`postcard::take_from_bytes`], tolerating trailing bytes.
///
/// **The field order is the wire order.** Postcard encodes struct fields
/// positionally and unnamed, so reordering or retyping a field silently
/// rewrites every record with no compile error. `encode_is_byte_stable` guards
/// this.
///
/// Deliberately private: the encoded shape is unnameable outside this module, so
/// no downstream crate can construct or reorder it.
#[derive(Debug, Serialize, Deserialize)]
struct StoredBuyerChannelState {
    schema_version: u32,
    channel_id: [u8; 32],
    provider: [u8; 20],
    token: [u8; 20],
    deposit: [u8; 32],
    last_amount: [u8; 32],
    last_nonce: [u8; 32],
    last_bytes_delivered: [u8; 32],
    expires_at: u64,
}

impl From<&BuyerChannelState> for StoredBuyerChannelState {
    fn from(state: &BuyerChannelState) -> Self {
        Self {
            schema_version: BUYER_SUPPORTED_SCHEMA_VERSION,
            channel_id: state.channel_id.into(),
            provider: state.provider.into(),
            token: state.token.into(),
            deposit: state.deposit.to_be_bytes(),
            last_amount: state.last_amount.to_be_bytes(),
            last_nonce: state.last_nonce.to_be_bytes(),
            last_bytes_delivered: state.last_bytes_delivered.to_be_bytes(),
            expires_at: state.expires_at,
        }
    }
}

impl StoredBuyerChannelState {
    fn into_state(self) -> Result<BuyerChannelState, StoreError> {
        if self.schema_version > BUYER_SUPPORTED_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema {
                found: self.schema_version,
                supported: BUYER_SUPPORTED_SCHEMA_VERSION,
            });
        }
        Ok(BuyerChannelState {
            channel_id: B256::from(self.channel_id),
            provider: Address::from(self.provider),
            token: Address::from(self.token),
            deposit: U256::from_be_bytes(self.deposit),
            last_amount: U256::from_be_bytes(self.last_amount),
            last_nonce: U256::from_be_bytes(self.last_nonce),
            last_bytes_delivered: U256::from_be_bytes(self.last_bytes_delivered),
            expires_at: self.expires_at,
        })
    }
}

/// Encode one buyer record. The single home for what used to be six inlined
/// copies of this across two crates.
fn encode_record(state: &BuyerChannelState) -> Result<Vec<u8>, StoreError> {
    postcard::to_allocvec(&StoredBuyerChannelState::from(state))
        .map_err(|err| StoreError::Codec(format!("buyer record postcard encode: {err}")))
}

/// Decode one buyer record into a [`BuyerChannelState`], validating that the
/// embedded `provider` matches the table key (additive-forward-compat via
/// `take_from_bytes`; bounded trailing-bytes warning).
fn decode_record(key_bytes: [u8; 20], value_bytes: &[u8]) -> Result<BuyerChannelState, StoreError> {
    let provider = Address::from(key_bytes);
    let (stored, remainder): (StoredBuyerChannelState, &[u8]) =
        postcard::take_from_bytes(value_bytes).map_err(|err| StoreError::Corrupt {
            channel_id: None,
            detail: format!("buyer record postcard decode failed (provider {provider}): {err}"),
        })?;
    if stored.provider != key_bytes {
        return Err(StoreError::Corrupt {
            channel_id: None,
            detail: format!("buyer record provider {provider} does not match table key"),
        });
    }
    if remainder.len() > SANE_TRAILER_MAX_BYTES {
        tracing::warn!(
            %provider,
            remainder = remainder.len(),
            limit = SANE_TRAILER_MAX_BYTES,
            event = "buyer_channel_store_excess_trailer",
            "buyer channel record has unusually large trailing bytes; possible malicious padding \
             or large-additive-field schema skew",
        );
    }
    stored.into_state()
}

/// `db` viewed as the buyer-channel table: a typed capability over a caller-owned
/// [`Database`]. Zero-cost — it borrows and owns nothing, and does no I/O until a
/// method is called. Construct one per operation.
///
/// The *file* is the only thing the two buyer stores legitimately differ on, so
/// it is the only thing this takes. Everything else about the table — name, key
/// type, value type, record codec — is frozen in this module (see the private
/// `BUYER_CHANNEL_TABLE`).
#[derive(Debug)]
pub struct BuyerChannelTable<'a> {
    db: &'a Database,
}

impl<'a> BuyerChannelTable<'a> {
    /// View `db` as the buyer-channel table.
    #[must_use]
    pub const fn new(db: &'a Database) -> Self {
        Self { db }
    }

    /// `true` if the table exists, `false` if the store was never written. Lets
    /// the no-row operations avoid implicitly creating it —
    /// `WriteTransaction::open_table` would.
    ///
    /// Benign TOCTOU: a concurrent writer may create the table *and* insert a row
    /// after this returns `false`, so the caller reports `UnknownProvider` /
    /// `false` for a provider that by then has a row. Harmless — it linearizes as
    /// "our op ran first". (Note the caller returns early on `false`; it does not
    /// fall through to a `get` miss.)
    fn table_exists(&self) -> Result<bool, StoreError> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        match read_txn.open_table(BUYER_CHANNEL_TABLE) {
            Ok(_) => Ok(true),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(false),
            Err(err) => Err(StoreError::Backend(format!("open_table: {err}"))),
        }
    }

    /// Begin a write transaction with fsync-on-commit durability.
    ///
    /// `Durability::Immediate` is `redb`'s current default but is set explicitly
    /// so a future default change cannot silently weaken the persistence
    /// guarantee the buyer watermark depends on.
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

    /// Load every persisted buyer channel.
    ///
    /// **Undecodable rows are logged and skipped, never propagated.** This is
    /// deliberately asymmetric with the seller `load_all` — whose error aborts
    /// startup, because a corrupt voucher record reopening the #527 replay
    /// window is unsafe to run past. Buyer bootstrap is *non-fatal*: a
    /// propagated error here would not just disable new buys, it would stop the
    /// reclaim sweep from ever spawning, stranding every *other* tracked
    /// channel's deposit as unreclaimable (PR #753 review, alpergundogdu). One
    /// bad row must not take the others down. **Do not "unify" this with the
    /// seller path.**
    ///
    /// Logged at `error!`, not `warn!`: the skipped row's deposit stays
    /// escrowed-but-unreclaimable until an operator repairs the record, so it
    /// warrants action (and must not be filtered out of alerting). The channel
    /// id can't be named — it lives inside the undecodable bytes — so the
    /// on-chain provider key is the only handle it can offer.
    ///
    /// **That log only reaches a `decdn-node` operator.** The node installs a
    /// `tracing` subscriber; the `decdn` CLI — the sole consumer of
    /// `RedbBuyerChannelStore` — does not, and has no `tracing` dependency at
    /// all, so for the client store this skip is entirely silent: `decdn channel
    /// list` omits the row and `decdn channel clean` can report "no tracked
    /// channels to clean" while the deposit is still escrowed. Pre-existing (both
    /// stores behaved this way before #1246 unified them), and not fixable from
    /// here — it needs `load_all` to *return* the skipped providers so each
    /// caller can surface them, which changes the [`BuyerChannelStore`] trait.
    /// Tracked as follow-up; do not read the paragraph above as a claim that a
    /// CLI user is told anything.
    ///
    /// [`BuyerChannelStore`]: crate::buyer_channel::BuyerChannelStore
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the table or its iterator is unreadable.
    pub fn load_all(&self) -> Result<Vec<BuyerChannelState>, StoreError> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(BUYER_CHANNEL_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let mut out = Vec::new();
        let iter = table
            .iter()
            .map_err(|err| StoreError::Backend(format!("table iter: {err}")))?;
        for entry in iter {
            let (key_guard, value_guard) =
                entry.map_err(|err| StoreError::Backend(format!("iter entry: {err}")))?;
            let key_bytes: [u8; 20] = *key_guard.value();
            match decode_record(key_bytes, value_guard.value()) {
                Ok(state) => out.push(state),
                Err(err) => tracing::error!(
                    provider = %Address::from(key_bytes),
                    %err,
                    event = "buyer_channel_store_skip_undecodable_record",
                    "buyer channel hydration: skipping an undecodable record; its escrowed deposit \
                     is untracked and will not be auto-reclaimed until the record is repaired \
                     (other channels remain healthy)",
                ),
            }
        }
        Ok(out)
    }

    /// Persist (insert or overwrite) the state for one channel, keyed by
    /// `state.provider`. Durable (fsynced) before returning `Ok`.
    ///
    /// Creates the table if absent — unlike the no-row operations, this one
    /// intends to write.
    ///
    /// # Errors
    ///
    /// [`StoreError::Codec`] on encode failure, [`StoreError::Backend`] if the
    /// write or fsync fails.
    pub fn record(&self, state: &BuyerChannelState) -> Result<(), StoreError> {
        let encoded = encode_record(state)?;
        let key: [u8; 20] = state.provider.into();

        let write_txn = self.begin_durable_write()?;
        {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(&key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    /// Drop the persisted entry for `provider`. A no-op if no record exists or
    /// the table was never written. Commits durably.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the delete or durable commit fails.
    pub fn forget(&self, provider: Address) -> Result<(), StoreError> {
        let key: [u8; 20] = provider.into();
        if !self.table_exists()? {
            return Ok(());
        }

        let write_txn = self.begin_durable_write()?;
        {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .remove(&key)
                .map_err(|err| StoreError::Backend(format!("remove: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    /// Point-lookup the live channel for `provider`, or `None` if none is
    /// tracked.
    ///
    /// Unlike [`Self::load_all`], a corrupt row here is **propagated**: this is
    /// not the bootstrap path, so surfacing the precise error is safe and
    /// diagnostic.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if unreadable, [`StoreError::Corrupt`] /
    /// [`StoreError::UnsupportedSchema`] if the record cannot be decoded.
    pub fn get_by_provider(
        &self,
        provider: Address,
    ) -> Result<Option<BuyerChannelState>, StoreError> {
        let key: [u8; 20] = provider.into();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(BUYER_CHANNEL_TABLE) {
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

    /// Compare-and-delete inside a single write transaction: remove `provider`'s
    /// row only if the stored `channel_id` still matches. Returns whether a row
    /// was deleted.
    ///
    /// Guards the reclaim sweep against a lost update: between the sweep loading
    /// an expired channel and forgetting it, a concurrent `open_or_reuse` may
    /// have opened a replacement under the same provider key. An unconditional
    /// [`Self::forget`] would delete the live replacement.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the read, delete, or durable commit fails.
    pub fn forget_if_channel(
        &self,
        provider: Address,
        channel_id: ChannelId,
    ) -> Result<bool, StoreError> {
        let key: [u8; 20] = provider.into();
        if !self.table_exists()? {
            return Ok(false);
        }

        let write_txn = self.begin_durable_write()?;
        let deleted = {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            // Read the current row inside the same (serialised) write txn so the
            // match-and-remove is atomic against a concurrent replace.
            let matches = match table
                .get(&key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            {
                Some(value_guard) => {
                    decode_record(key, value_guard.value())?.channel_id == channel_id
                }
                None => false,
            };
            if matches {
                table
                    .remove(&key)
                    .map_err(|err| StoreError::Backend(format!("remove: {err}")))?;
            }
            matches
        };
        // NOTE: unlike `advance_progress`/`add_deposit`, this commits even when
        // nothing matched (an empty txn + fsync). Pre-existing behaviour in both
        // stores; preserved deliberately rather than "fixed" here.
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(deleted)
    }

    /// Atomically advance the committed progress for `provider`'s channel inside
    /// a single write transaction (read → channel-id guard → advance against the
    /// committed watermark → write).
    ///
    /// Closes the lost-update / watermark-regression race that a separate
    /// `get_by_provider` → mutate → [`Self::record`] sequence exposes when a
    /// concurrent writer (e.g. [`Self::add_deposit`]) touches the same row in the
    /// gap (#838).
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] / [`StoreError::Codec`] on I/O or codec failure.
    pub fn advance_progress(
        &self,
        provider: Address,
        channel_id: ChannelId,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        let key: [u8; 20] = provider.into();
        if !self.table_exists()? {
            return Ok(AdvanceOutcome::UnknownProvider);
        }

        let write_txn = self.begin_durable_write()?;
        {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            // Read the committed row inside the same (serialised) write txn so
            // the advance is checked against — and written over — the committed
            // watermark, never a stale in-memory snapshot.
            let Some(value_guard) = table
                .get(&key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            else {
                // No-write outcomes return early so the uncommitted write txn is
                // aborted on drop — avoiding a pointless `Durability::Immediate`
                // fsync on a transaction that changed nothing.
                return Ok(AdvanceOutcome::UnknownProvider);
            };
            let mut state = decode_record(key, value_guard.value())?;
            // Drop the borrow of `table` held by `value_guard` before mutating.
            drop(value_guard);
            if state.channel_id != channel_id {
                return Ok(AdvanceOutcome::ChannelMismatch);
            }
            if let Err(err) = state.advance(nonce, bytes_delivered, amount) {
                return Ok(AdvanceOutcome::Regressed(err));
            }
            let encoded = encode_record(&state)?;
            table
                .insert(&key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(AdvanceOutcome::Advanced)
    }

    /// Atomically add `additional` to the committed deposit for `provider`'s
    /// channel inside a single write transaction.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] / [`StoreError::Codec`] on I/O or codec failure.
    pub fn add_deposit(
        &self,
        provider: Address,
        channel_id: ChannelId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        let key: [u8; 20] = provider.into();
        if !self.table_exists()? {
            return Ok(DepositOutcome::UnknownProvider);
        }

        let write_txn = self.begin_durable_write()?;
        let new_deposit = {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            let Some(value_guard) = table
                .get(&key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            else {
                // No-write outcome returns early so the uncommitted write txn is
                // aborted on drop — no pointless `Durability::Immediate` fsync.
                return Ok(DepositOutcome::UnknownProvider);
            };
            let mut state = decode_record(key, value_guard.value())?;
            drop(value_guard);
            if state.channel_id != channel_id {
                return Ok(DepositOutcome::ChannelMismatch);
            }
            state.deposit = state.deposit.saturating_add(additional);
            let encoded = encode_record(&state)?;
            table
                .insert(&key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
            state.deposit
        };
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(DepositOutcome::Added(new_deposit))
    }

    /// Write raw value bytes under `provider`'s key, **bypassing the encoder**,
    /// to simulate a row left undecodable by a binary downgrade.
    ///
    /// Seeds corruption against a live store for the tolerance tests here, and —
    /// via `decdn-node`'s `insert_raw_buyer_record` — for its buyer
    /// reconciliation + mixed-reclaim e2e (#763), which lives in a separate
    /// integration-test crate.
    ///
    /// Gated behind `test-util` rather than `#[cfg(test)]` because a cross-crate
    /// `cfg(test)` does not propagate: node's seam could not reach a
    /// `cfg(test)`-only method here. The feature keeps a corruption-seeding
    /// writer out of the shipped API; enable it from `[dev-dependencies]` (the
    /// same pattern `decdn-client-pull`'s `test-util` uses).
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the write or durable commit fails.
    #[cfg(any(test, feature = "test-util"))]
    pub fn insert_raw(&self, provider: Address, bytes: &[u8]) -> Result<(), StoreError> {
        let key: [u8; 20] = provider.into();
        let write_txn = self.begin_durable_write()?;
        {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
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

    /// A bare `redb` database in a temp dir. These tests drive the shared table
    /// ops directly, so they need no store, no data-dir hardening, and no file
    /// layout — which is the point: the ops are agnostic about who owns the file.
    fn db() -> anyhow::Result<(TempDir, Database)> {
        let dir = tempfile::tempdir()?;
        let db = Database::create(dir.path().join("t.redb"))?;
        Ok((dir, db))
    }

    fn tbl(db: &Database) -> BuyerChannelTable<'_> {
        BuyerChannelTable::new(db)
    }

    /// `true` if the buyer table was never created. Distinguishes "no row" from
    /// "no table" so the no-row ops can be held to not creating one.
    fn table_absent(db: &Database) -> anyhow::Result<bool> {
        let read_txn = db.begin_read()?;
        Ok(matches!(
            read_txn.open_table(BUYER_CHANNEL_TABLE),
            Err(redb::TableError::TableDoesNotExist(_))
        ))
    }

    fn state(byte: u8) -> BuyerChannelState {
        let mut id = [0u8; 32];
        id[31] = byte;
        let mut prov = [0u8; 20];
        prov[19] = byte;
        BuyerChannelState {
            channel_id: id.into(),
            provider: Address::from(prov),
            token: address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            deposit: U256::from(10_000_000u64),
            last_amount: U256::from(byte) * U256::from(1_000u64),
            last_nonce: U256::from(byte),
            last_bytes_delivered: U256::from(byte) * U256::from(1_024u64),
            expires_at: 1_900_000_000 + u64::from(byte),
        }
    }

    const OTHER_CHANNEL: ChannelId =
        b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

    /// Postcard encoding of [`golden_state`].
    ///
    /// Produced by running the **pre-#1246 encoder** (on `main`, before the codec
    /// moved) over this fixture, so it pins the format across the hoist and not
    /// merely against itself. Hex rather than a 206-byte array literal so a diff
    /// shows exactly which field moved.
    const GOLDEN_RECORD_HEX: &str = concat!(
        "01",                                                               // schema_version (varint)
        "1111111111111111111111111111111111111111111111111111111111111111", // channel_id
        "2222222222222222222222222222222222222222",                         // provider
        "3333333333333333333333333333333333333333",                         // token
        "000000000000000000000000000000000000000000000000aaaaaaaaaaaaaaaa", // deposit
        "000000000000000000000000000000000000000000000000bbbbbbbbbbbbbbbb", // last_amount
        "000000000000000000000000000000000000000000000000cccccccccccccccc", // last_nonce
        "000000000000000000000000000000000000000000000000dddddddddddddddd", // last_bytes_delivered
        "eeddbbf70e",                                                       // expires_at (varint)
    );

    /// The frozen bytes must also *decode* back to the fixture.
    ///
    /// [`encode_is_byte_stable`] pins the encoder, but the direction that
    /// actually matters — an older binary's record still loads — runs the other
    /// way, and every other test in this suite round-trips through the current
    /// codec, so a matched encoder/decoder drift would pass all of them.
    /// `GOLDEN_RECORD_HEX` is a source literal no running code produced, so this
    /// holds `take_from_bytes`, the provider/key cross-check, and the
    /// schema-version gate against the format as it was written on disk.
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
        let want = golden_state();
        let got = decode_record(want.provider.into(), &bytes)?;
        anyhow::ensure!(
            got == want,
            "frozen bytes no longer decode:\n {got:?}\n {want:?}"
        );
        Ok(())
    }

    /// The fixture behind [`GOLDEN_RECORD_HEX`].
    ///
    /// Every field carries a **distinct** byte pattern, and that is the whole
    /// point: postcard writes fields positionally, so a golden can only catch a
    /// reordering if the swapped fields encode differently. The obvious fixture
    /// (`state(7)`) fails that — its `channel_id` is 32 bytes ending `07` and its
    /// `last_nonce` is `U256::from(7)`, i.e. *the same 32 bytes*, so swapping the
    /// two is invisible. Verified: with `state(7)` this test passed a
    /// `channel_id`/`last_nonce` swap, which would mis-decode every record on
    /// disk. Keep all eight values distinct.
    fn golden_state() -> BuyerChannelState {
        BuyerChannelState {
            channel_id: B256::repeat_byte(0x11),
            provider: Address::repeat_byte(0x22),
            token: Address::repeat_byte(0x33),
            deposit: U256::from(0xAAAA_AAAA_AAAA_AAAAu64),
            last_amount: U256::from(0xBBBB_BBBB_BBBB_BBBBu64),
            last_nonce: U256::from(0xCCCC_CCCC_CCCC_CCCCu64),
            last_bytes_delivered: U256::from(0xDDDD_DDDD_DDDD_DDDDu64),
            expires_at: 0xEEEE_EEEEu64,
        }
    }

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

    /// The buyer record is a **frozen on-disk format**, shared by the node's
    /// `channels.redb` and the client's `buyer-channels.redb`. Postcard encodes
    /// struct fields positionally and unnamed, so reordering, retyping, or
    /// inserting a field rewrites the bytes with no compile error and no other
    /// test failure — every store the suites build is a fresh tempdir, so they
    /// would all still pass while every record an older binary wrote was
    /// silently orphaned.
    ///
    /// This golden is the tripwire for that, and the only test here that would
    /// fail on such a change. Captured from the pre-#1246 encoder, so it pins the
    /// format across the hoist itself — and, because both crates encoded this
    /// identically beforehand, across the two implementations it replaced.
    ///
    /// See [`golden_state`] for why every field must hold a distinct value.
    #[test]
    fn encode_is_byte_stable() -> anyhow::Result<()> {
        let hex = hex_of(&encode_record(&golden_state())?);
        anyhow::ensure!(
            hex == GOLDEN_RECORD_HEX,
            "the buyer record's on-disk encoding changed — this orphans every existing record in \
             both `channels.redb` and `buyer-channels.redb`.\n  got:  {hex}\n  want: \
             {GOLDEN_RECORD_HEX}",
        );
        Ok(())
    }

    #[test]
    fn open_empty_store_returns_no_entries() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        anyhow::ensure!(tbl(&db).load_all()?.is_empty());
        anyhow::ensure!(tbl(&db).get_by_provider(state(1).provider)?.is_none());
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
                .get_by_provider(a.provider)?
                .ok_or_else(|| anyhow::anyhow!("missing a"))?;
            anyhow::ensure!(got == a, "get_by_provider must round-trip");
        }
        // Reopen the same file: records survive.
        let db = Database::create(&path)?;
        let mut all = tbl(&db).load_all()?;
        all.sort_by_key(|s| s.provider);
        anyhow::ensure!(all.len() == 2);
        anyhow::ensure!(*all.first().ok_or_else(|| anyhow::anyhow!("[0]"))? == a);
        anyhow::ensure!(*all.get(1).ok_or_else(|| anyhow::anyhow!("[1]"))? == b);
        Ok(())
    }

    #[test]
    fn record_overwrites_by_provider() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let mut s = state(5);
        tbl(&db).record(&s)?;
        s.last_nonce = U256::from(99u64);
        s.deposit = U256::from(20_000_000u64);
        tbl(&db).record(&s)?;
        anyhow::ensure!(tbl(&db).load_all()?.len() == 1, "same provider overwrites");
        let only = tbl(&db)
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(only.last_nonce == U256::from(99u64));
        anyhow::ensure!(only.deposit == U256::from(20_000_000u64));
        Ok(())
    }

    #[test]
    fn load_all_returns_every_recorded_channel() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        for byte in 1..=4u8 {
            tbl(&db).record(&state(byte))?;
        }
        let mut got: Vec<_> = tbl(&db).load_all()?.iter().map(|s| s.provider).collect();
        got.sort_unstable();
        let want: Vec<_> = (1..=4u8).map(|b| state(b).provider).collect();
        anyhow::ensure!(
            got == want,
            "load_all must return every provider, got {got:?}"
        );
        Ok(())
    }

    #[test]
    fn forget_removes_entry_and_is_noop_when_absent() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(3);
        tbl(&db).record(&s)?;
        tbl(&db).forget(s.provider)?;
        anyhow::ensure!(tbl(&db).load_all()?.is_empty());
        // forget on an unknown provider is a no-op.
        tbl(&db).forget(address!("00000000000000000000000000000000000000ff"))?;
        Ok(())
    }

    /// A buyer record stamped with a future schema version must NOT fail
    /// hydration — that would disable the whole buyer path and the reclaim sweep
    /// (PR #753 review). `load_all` skips it; the point lookup still surfaces the
    /// precise error.
    #[test]
    fn future_schema_version_skipped_on_hydration() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(1);
        let mut stored = StoredBuyerChannelState::from(&s);
        stored.schema_version = BUYER_SUPPORTED_SCHEMA_VERSION + 1;
        let encoded = postcard::to_allocvec(&stored)?;
        tbl(&db).insert_raw(s.provider, &encoded)?;

        anyhow::ensure!(
            tbl(&db).load_all()?.is_empty(),
            "future-schema record must be skipped, not propagated, by load_all",
        );
        let err = tbl(&db)
            .get_by_provider(s.provider)
            .err()
            .ok_or_else(|| anyhow::anyhow!("future schema must reject on get_by_provider"))?;
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

    /// Garbage value bytes under a real buyer key are skipped by `load_all` (one
    /// bad row must not strand every other channel's deposit — PR #753 review),
    /// while a healthy record alongside it survives.
    #[test]
    fn corrupt_value_bytes_skipped_keeps_healthy() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let healthy = state(2);
        tbl(&db).record(&healthy)?;
        tbl(&db).insert_raw(state(3).provider, &[0u8; 8])?; // far too short

        let all = tbl(&db).load_all()?;
        anyhow::ensure!(
            all.len() == 1 && all.first() == Some(&healthy),
            "corrupt row must be skipped while the healthy row survives, got {all:?}",
        );
        let err = tbl(&db)
            .get_by_provider(state(3).provider)
            .err()
            .ok_or_else(|| anyhow::anyhow!("garbage value must reject on get_by_provider"))?;
        anyhow::ensure!(
            matches!(&err, StoreError::Corrupt { detail, .. } if detail.contains("postcard decode")),
            "expected Corrupt(postcard decode), got {err:?}",
        );
        Ok(())
    }

    /// A record whose embedded `provider` doesn't match its table key is skipped
    /// by `load_all`; the point lookup still surfaces `Corrupt`.
    #[test]
    fn provider_key_mismatch_skipped_on_hydration() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s_for_a = state(0xAA);
        let encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(&s_for_a))?;
        // Filed under a *different* provider's key.
        tbl(&db).insert_raw(state(0xBB).provider, &encoded)?;

        anyhow::ensure!(
            tbl(&db).load_all()?.is_empty(),
            "provider/key-mismatch record must be skipped by load_all",
        );
        let err = tbl(&db)
            .get_by_provider(state(0xBB).provider)
            .err()
            .ok_or_else(|| {
                anyhow::anyhow!("provider/key mismatch must reject on get_by_provider")
            })?;
        anyhow::ensure!(
            matches!(&err, StoreError::Corrupt { detail, .. } if detail.contains("does not match table key")),
            "expected Corrupt(does not match table key), got {err:?}",
        );
        Ok(())
    }

    /// Forward-compat: a future writer's additive trailing bytes after the buyer
    /// prefix decode cleanly (`take_from_bytes`).
    #[test]
    fn extra_trailing_bytes_are_tolerated() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(0x42);
        let mut encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(&s))?;
        encoded.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01]);
        tbl(&db).insert_raw(s.provider, &encoded)?;

        let all = tbl(&db).load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(*only == s, "prefix must decode despite trailing bytes");
        Ok(())
    }

    /// `forget_if_channel` (compare-and-delete) deletes only the matching
    /// channel — the lost-update guard for the reclaim sweep.
    #[test]
    fn forget_if_channel_is_compare_and_delete() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        // CAS on a never-written store is a no-op (false).
        anyhow::ensure!(!tbl(&db).forget_if_channel(state(1).provider, state(1).channel_id)?);

        let s = state(4);
        tbl(&db).record(&s)?;
        // Wrong channel id → not deleted, row survives.
        anyhow::ensure!(
            !tbl(&db).forget_if_channel(s.provider, OTHER_CHANNEL)?,
            "mismatched channel must not delete"
        );
        anyhow::ensure!(
            tbl(&db).get_by_provider(s.provider)?.is_some(),
            "row must survive"
        );
        // Matching channel id → deleted.
        anyhow::ensure!(tbl(&db).forget_if_channel(s.provider, s.channel_id)?);
        anyhow::ensure!(tbl(&db).get_by_provider(s.provider)?.is_none());
        Ok(())
    }

    /// #838: a stale `advance_progress` reporting totals below the committed
    /// watermark is rejected and leaves the committed watermark intact.
    #[test]
    fn advance_progress_cannot_regress_committed_watermark() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let mut s = state(8);
        s.last_nonce = U256::from(9u64);
        s.last_bytes_delivered = U256::from(9_000u64);
        s.last_amount = U256::from(90u64);
        tbl(&db).record(&s)?;

        let outcome = tbl(&db).advance_progress(
            s.provider,
            s.channel_id,
            U256::from(5u64),
            U256::from(5_000u64),
            U256::from(50u64),
        )?;
        anyhow::ensure!(
            matches!(outcome, AdvanceOutcome::Regressed(_)),
            "stale progress must be rejected, got {outcome:?}"
        );
        let stored = tbl(&db)
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(
            stored.last_nonce == U256::from(9u64),
            "committed watermark regressed to {}",
            stored.last_nonce
        );
        Ok(())
    }

    /// #838: the atomic mutators are channel-id guarded (a row replaced by a
    /// newer open for the same provider is not clobbered) and report unknown
    /// providers.
    #[test]
    fn advance_and_deposit_guard_on_channel_and_provider() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let ghost = state(1);
        anyhow::ensure!(
            tbl(&db).advance_progress(
                ghost.provider,
                ghost.channel_id,
                U256::from(1u64),
                U256::from(1u64),
                U256::from(1u64)
            )? == AdvanceOutcome::UnknownProvider
        );
        anyhow::ensure!(
            tbl(&db).add_deposit(ghost.provider, ghost.channel_id, U256::from(1u64))?
                == DepositOutcome::UnknownProvider
        );

        let s = state(9);
        tbl(&db).record(&s)?;

        // With the table now present, an unrecorded provider must still report
        // UnknownProvider. This is a *different* branch from the `ghost` calls
        // above: those short-circuit at `table_exists()`, this one returns from
        // inside the write txn (aborting it on drop). Mutating that return to
        // ChannelMismatch otherwise passes the whole suite.
        let absent = state(0x5A);
        anyhow::ensure!(
            tbl(&db).advance_progress(
                absent.provider,
                absent.channel_id,
                U256::from(1u64),
                U256::from(1u64),
                U256::from(1u64)
            )? == AdvanceOutcome::UnknownProvider,
            "advance_progress on an absent row of an existing table"
        );
        anyhow::ensure!(
            tbl(&db).add_deposit(absent.provider, absent.channel_id, U256::from(1u64))?
                == DepositOutcome::UnknownProvider,
            "add_deposit on an absent row of an existing table"
        );

        anyhow::ensure!(
            tbl(&db).advance_progress(
                s.provider,
                OTHER_CHANNEL,
                U256::from(99u64),
                U256::from(99u64),
                U256::from(99u64)
            )? == AdvanceOutcome::ChannelMismatch
        );
        anyhow::ensure!(
            tbl(&db).add_deposit(s.provider, OTHER_CHANNEL, U256::from(1u64))?
                == DepositOutcome::ChannelMismatch
        );
        let stored = tbl(&db)
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(stored == s, "mismatched calls must not mutate the row");
        Ok(())
    }

    /// The no-row operations must not implicitly create the table on a
    /// never-written store — `WriteTransaction::open_table` would, so each one
    /// pre-checks. The node's suite asserted the outcomes but never the "no
    /// table" half of the claim; assert it here, since a dropped pre-check is
    /// exactly what this refactor could regress.
    #[test]
    fn no_row_ops_do_not_create_the_table() -> anyhow::Result<()> {
        let (_d, db) = db()?;
        let s = state(1);

        tbl(&db).forget(s.provider)?;
        anyhow::ensure!(table_absent(&db)?, "forget created the table");

        anyhow::ensure!(!tbl(&db).forget_if_channel(s.provider, s.channel_id)?);
        anyhow::ensure!(table_absent(&db)?, "forget_if_channel created the table");

        anyhow::ensure!(
            tbl(&db).advance_progress(
                s.provider,
                s.channel_id,
                U256::from(1u64),
                U256::from(1u64),
                U256::from(1u64)
            )? == AdvanceOutcome::UnknownProvider
        );
        anyhow::ensure!(table_absent(&db)?, "advance_progress created the table");

        anyhow::ensure!(
            tbl(&db).add_deposit(s.provider, s.channel_id, U256::from(1u64))?
                == DepositOutcome::UnknownProvider
        );
        anyhow::ensure!(table_absent(&db)?, "add_deposit created the table");

        // `record`, by contrast, is supposed to create it.
        tbl(&db).record(&s)?;
        anyhow::ensure!(!table_absent(&db)?, "record must create the table");
        Ok(())
    }

    /// #838: the atomic mutators honour the `Durability::Immediate` "MUST commit
    /// durably" contract — an `advance_progress` + `add_deposit` survive a
    /// close/reopen, while a no-write `ChannelMismatch` persists nothing.
    #[test]
    fn advance_and_deposit_survive_reopen() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("t.redb");
        let s = state(2);
        {
            let db = Database::create(&path)?;
            tbl(&db).record(&s)?;
            anyhow::ensure!(
                tbl(&db).advance_progress(
                    s.provider,
                    s.channel_id,
                    s.last_nonce + U256::from(3u64),
                    s.last_bytes_delivered + U256::from(3_000u64),
                    s.last_amount + U256::from(30u64),
                )? == AdvanceOutcome::Advanced
            );
            anyhow::ensure!(
                tbl(&db).add_deposit(s.provider, s.channel_id, U256::from(40u64))?
                    == DepositOutcome::Added(s.deposit + U256::from(40u64))
            );
            // A mismatched (no-write) call must leave nothing extra to persist.
            anyhow::ensure!(
                tbl(&db).add_deposit(s.provider, OTHER_CHANNEL, U256::from(1u64))?
                    == DepositOutcome::ChannelMismatch
            );
        }
        let db = Database::create(&path)?;
        let reopened = tbl(&db)
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("row vanished across reopen"))?;
        anyhow::ensure!(reopened.last_nonce == s.last_nonce + U256::from(3u64));
        anyhow::ensure!(
            reopened.last_bytes_delivered == s.last_bytes_delivered + U256::from(3_000u64)
        );
        anyhow::ensure!(reopened.last_amount == s.last_amount + U256::from(30u64));
        anyhow::ensure!(
            reopened.deposit == s.deposit + U256::from(40u64),
            "mismatched call must not have altered the deposit"
        );
        Ok(())
    }

    /// #838: interleaving `add_deposit` with `advance_progress` on the same
    /// provider row must lose neither the deposit accrual nor the watermark
    /// advance. The pre-fix `get → mutate → record` (read outside the write txn)
    /// would clobber one writer with the other's stale snapshot; the atomic
    /// in-txn mutators serialise correctly.
    #[test]
    fn concurrent_top_up_and_progress_preserve_both() -> anyhow::Result<()> {
        const N: u64 = 300;
        let (_d, db) = db()?;
        let db = std::sync::Arc::new(db);
        let mut base = state(6);
        base.deposit = U256::from(1_000u64);
        base.last_nonce = U256::ZERO;
        base.last_bytes_delivered = U256::ZERO;
        base.last_amount = U256::ZERO;
        tbl(&db).record(&base)?;
        let (provider, channel_id) = (base.provider, base.channel_id);

        let depositor = std::sync::Arc::clone(&db);
        let deposit_thread = std::thread::spawn(move || -> anyhow::Result<()> {
            for _ in 0..N {
                let outcome =
                    tbl(&depositor).add_deposit(provider, channel_id, U256::from(1u64))?;
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
                    provider,
                    channel_id,
                    U256::from(i),
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
            .get_by_provider(provider)?
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(
            final_row.deposit == U256::from(1_000u64) + U256::from(N),
            "lost a top-up: deposit = {}",
            final_row.deposit
        );
        anyhow::ensure!(
            final_row.last_nonce == U256::from(N),
            "watermark not fully advanced: last_nonce = {}",
            final_row.last_nonce
        );
        Ok(())
    }
}
