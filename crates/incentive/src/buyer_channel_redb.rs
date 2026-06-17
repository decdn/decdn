//! `redb`-backed persistent [`BuyerChannelStore`] for client-side use (#940).
//!
//! The node persists buyer channels in a *combined* seller+buyer redb file
//! (`node::channel_store`), deliberately sharing one open + one fsync across
//! both tables. A client (`decdn fetch`) has no seller state, so it gets this
//! **buyer-only** store: one redb file, one table, the same durable
//! (`Durability::Immediate`, fsync-on-commit) write discipline and the same
//! versioned `StoredBuyerChannelState` encoding + transactional advance/deposit
//! semantics as the node's buyer table, so a channel opened in one `fetch`
//! invocation is reused (resuming its voucher watermark) by the next.
//!
//! Gated behind the `redb` feature so non-client consumers of `decdn-incentive`
//! (the contracts/voucher logic) don't pull `redb`/`postcard`.

use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::buyer_channel::{AdvanceOutcome, BuyerChannelState, BuyerChannelStore, DepositOutcome};
use crate::channel::ChannelId;
use crate::store::StoreError;

/// File name of the buyer-channel redb database within the data dir.
const BUYER_CHANNELS_DB_FILE: &str = "buyer-channels.redb";

/// redb table holding buyer channel state, keyed by the 20-byte provider
/// address (one open channel per provider). Value: postcard-encoded
/// [`StoredBuyerChannelState`].
const BUYER_CHANNEL_TABLE: TableDefinition<&[u8; 20], &[u8]> =
    TableDefinition::new("buyer_channel_state_v1");

/// Highest buyer-record `schema_version` this binary can decode. Matches the
/// node's buyer table so the on-disk shape is identical.
const BUYER_SUPPORTED_SCHEMA_VERSION: u32 = 1;

/// Trailing-bytes warning threshold on decode (additive-forward-compat slack).
const SANE_TRAILER_MAX_BYTES: usize = 256;

/// On-disk buyer record. `schema_version` lives in the value (not the key) so a
/// future additive field ships without renaming the table; decode uses
/// [`postcard::take_from_bytes`], tolerating trailing bytes. Byte-compatible
/// with the node's buyer table.
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

fn decode_buyer_record(
    key_bytes: [u8; 20],
    value_bytes: &[u8],
) -> Result<BuyerChannelState, StoreError> {
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
            "buyer channel record has unusually large trailing bytes; possible schema skew",
        );
    }
    stored.into_state()
}

/// Buyer-only `redb`-backed [`BuyerChannelStore`]. One redb file, one table;
/// every mutating call fsyncs on commit (`Durability::Immediate`).
#[derive(Debug)]
pub struct RedbBuyerChannelStore {
    db: Database,
}

impl RedbBuyerChannelStore {
    /// Open (or create) the buyer-channel store at `<data_dir>/buyer-channels.redb`,
    /// creating `data_dir` with hardened permissions first.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the data dir cannot be created/hardened or
    /// redb cannot open the database.
    pub fn open(data_dir: &Path) -> Result<Self, StoreError> {
        decdn_common::identity::ensure_data_dir(data_dir)
            .map_err(|err| StoreError::Backend(format!("ensure data dir: {err}")))?;
        let path = data_dir.join(BUYER_CHANNELS_DB_FILE);
        let db = Database::create(&path)
            .map_err(|err| StoreError::Backend(format!("open buyer channel db: {err}")))?;
        Ok(Self { db })
    }

    fn write_state(&self, state: &BuyerChannelState) -> Result<(), StoreError> {
        let encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(state))
            .map_err(|err| StoreError::Codec(format!("buyer record postcard encode: {err}")))?;
        let key: [u8; 20] = state.provider.into();
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
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

    /// `true` if the buyer table exists, `false` if the store was never written.
    /// Lets the no-row methods avoid implicitly creating the table.
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
}

impl BuyerChannelStore for RedbBuyerChannelStore {
    fn load_all(&self) -> Result<Vec<BuyerChannelState>, StoreError> {
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
            // One undecodable row must not sink the whole load (mirrors the
            // node's non-fatal buyer hydration): log it and skip.
            match decode_buyer_record(key_bytes, value_guard.value()) {
                Ok(state) => out.push(state),
                Err(err) => tracing::error!(
                    provider = %Address::from(key_bytes),
                    %err,
                    event = "buyer_channel_store_skip_undecodable_record",
                    "skipping an undecodable buyer record; repair it to recover the channel",
                ),
            }
        }
        Ok(out)
    }

    fn record(&self, state: &BuyerChannelState) -> Result<(), StoreError> {
        self.write_state(state)
    }

    fn forget(&self, provider: Address) -> Result<(), StoreError> {
        if !self.table_exists()? {
            return Ok(());
        }
        let key: [u8; 20] = provider.into();
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
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

    fn forget_if_channel(
        &self,
        provider: Address,
        channel_id: ChannelId,
    ) -> Result<bool, StoreError> {
        if !self.table_exists()? {
            return Ok(false);
        }
        let key: [u8; 20] = provider.into();
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        let deleted = {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            // Read the row inside the same serialised write txn so match-and-remove
            // is atomic against a concurrent replace.
            let matches = match table
                .get(&key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            {
                Some(value_guard) => {
                    decode_buyer_record(key, value_guard.value())?.channel_id == channel_id
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
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(deleted)
    }

    fn get_by_provider(&self, provider: Address) -> Result<Option<BuyerChannelState>, StoreError> {
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
        Ok(Some(decode_buyer_record(key, value_guard.value())?))
    }

    fn advance_progress(
        &self,
        provider: Address,
        channel_id: ChannelId,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        if !self.table_exists()? {
            return Ok(AdvanceOutcome::UnknownProvider);
        }
        let key: [u8; 20] = provider.into();
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            // Read the committed row inside the same write txn so the advance is
            // checked against — and written over — the committed watermark.
            let Some(value_guard) = table
                .get(&key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            else {
                // No-write outcome: return early so the write txn aborts on drop.
                return Ok(AdvanceOutcome::UnknownProvider);
            };
            let mut state = decode_buyer_record(key, value_guard.value())?;
            drop(value_guard);
            if state.channel_id != channel_id {
                return Ok(AdvanceOutcome::ChannelMismatch);
            }
            if let Err(err) = state.advance(nonce, bytes_delivered, amount) {
                return Ok(AdvanceOutcome::Regressed(err));
            }
            let encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(&state))
                .map_err(|err| StoreError::Codec(format!("buyer record postcard encode: {err}")))?;
            table
                .insert(&key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(AdvanceOutcome::Advanced)
    }

    fn add_deposit(
        &self,
        provider: Address,
        channel_id: ChannelId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        if !self.table_exists()? {
            return Ok(DepositOutcome::UnknownProvider);
        }
        let key: [u8; 20] = provider.into();
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        let new_deposit = {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            let Some(value_guard) = table
                .get(&key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            else {
                return Ok(DepositOutcome::UnknownProvider);
            };
            let mut state = decode_buyer_record(key, value_guard.value())?;
            drop(value_guard);
            if state.channel_id != channel_id {
                return Ok(DepositOutcome::ChannelMismatch);
            }
            state.deposit = state.deposit.saturating_add(additional);
            let encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(&state))
                .map_err(|err| StoreError::Codec(format!("buyer record postcard encode: {err}")))?;
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
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    fn state(provider: u8, nonce: u64, bytes: u64, amount: u64) -> BuyerChannelState {
        BuyerChannelState {
            channel_id: B256::repeat_byte(provider),
            provider: Address::repeat_byte(provider),
            token: Address::repeat_byte(0xaa),
            deposit: U256::from(1_000_000u64),
            last_amount: U256::from(amount),
            last_nonce: U256::from(nonce),
            last_bytes_delivered: U256::from(bytes),
            expires_at: 9_999_999_999,
        }
    }

    #[test]
    fn record_then_reuse_resumes_watermark_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let provider = Address::repeat_byte(7);
        let s = state(7, 3, 3000, 300);
        {
            let store = RedbBuyerChannelStore::open(&dir.path().join("d")).unwrap();
            store.record(&s).unwrap();
        }
        // Reopen: the channel + its watermark must survive (this is what lets a
        // later `fetch` reuse the channel instead of opening a new one).
        let store = RedbBuyerChannelStore::open(&dir.path().join("d")).unwrap();
        let got = store
            .get_by_provider(provider)
            .unwrap()
            .expect("channel persisted");
        assert_eq!(got.channel_id, s.channel_id);
        assert_eq!(got.last_nonce, U256::from(3u64));
        assert_eq!(got.last_bytes_delivered, U256::from(3000u64));
    }

    #[test]
    fn advance_progress_guards_regression_and_channel_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let store = RedbBuyerChannelStore::open(&dir.path().join("d")).unwrap();
        let provider = Address::repeat_byte(7);
        let cid = B256::repeat_byte(7);
        store.record(&state(7, 1, 1000, 100)).unwrap();

        // Forward advance commits.
        assert!(matches!(
            store
                .advance_progress(
                    provider,
                    cid,
                    U256::from(2u64),
                    U256::from(2000u64),
                    U256::from(200u64)
                )
                .unwrap(),
            AdvanceOutcome::Advanced
        ));
        // Regression (lower nonce) is rejected without writing.
        assert!(matches!(
            store
                .advance_progress(
                    provider,
                    cid,
                    U256::from(1u64),
                    U256::from(1500u64),
                    U256::from(150u64)
                )
                .unwrap(),
            AdvanceOutcome::Regressed(_)
        ));
        // Wrong channel id for this provider → mismatch, no write.
        assert!(matches!(
            store
                .advance_progress(
                    provider,
                    B256::repeat_byte(9),
                    U256::from(3u64),
                    U256::from(3000u64),
                    U256::from(300u64)
                )
                .unwrap(),
            AdvanceOutcome::ChannelMismatch
        ));
        // The committed watermark is still the forward advance.
        let got = store.get_by_provider(provider).unwrap().unwrap();
        assert_eq!(got.last_nonce, U256::from(2u64));
    }

    #[test]
    fn unknown_provider_outcomes_on_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = RedbBuyerChannelStore::open(&dir.path().join("d")).unwrap();
        let p = Address::repeat_byte(1);
        assert!(store.get_by_provider(p).unwrap().is_none());
        assert!(matches!(
            store
                .advance_progress(p, B256::ZERO, U256::ZERO, U256::ZERO, U256::ZERO)
                .unwrap(),
            AdvanceOutcome::UnknownProvider
        ));
        assert!(!store.forget_if_channel(p, B256::ZERO).unwrap());
    }
}
