//! `redb`-backed persistent [`BuyerChannelStore`] for client-side use (#940).
//!
//! A client (`decdn fetch`) has no seller state, so it gets a **buyer-only**
//! store: its own `buyer-channels.redb`, one table. The node instead keeps its
//! buyer table inside a combined `channels.redb` alongside the seller,
//! pending-settle, and watcher-checkpoint tables, because `redb` forbids two
//! `Database` handles on one file.
//!
//! That file ownership — plus the guards on [`RedbBuyerChannelStore::open`] — is
//! **all**
//! this module contributes. The record codec and every table operation live in
//! [`crate::buyer_channel_table`], which the node's store delegates to as well,
//! so the two cannot drift in on-disk format or in advance/deposit semantics
//! (#1246). Before that, this module carried its own byte-identical copy of
//! both, kept in sync by hand.
//!
//! Gated behind the `redb` feature so non-client consumers of `decdn-incentive`
//! (the contracts/voucher logic) don't link `redb`. The shared table sits behind
//! `buyer-store-core`, which this feature implies.

use std::path::Path;

use alloy::primitives::{Address, U256};
use redb::Database;

use crate::buyer_channel::{AdvanceOutcome, BuyerChannelState, BuyerChannelStore, DepositOutcome};
use crate::buyer_channel_table::BuyerChannelTable;
use crate::channel::ChannelId;
use crate::store::StoreError;

/// File name of the buyer-channel redb database within the data dir.
const BUYER_CHANNELS_DB_FILE: &str = "buyer-channels.redb";

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
        // Reject a zero-length file. `redb::Database::create` treats both "absent"
        // and "present but empty" as "create a fresh database", so a `truncate -s 0`
        // or a filesystem rollback that nukes content but keeps the inode would
        // silently wipe the persisted watermark — the next reuse would re-sign a
        // stale nonce, or a second channel would open and escrow another deposit.
        // After the first commit the file is non-zero forever in normal operation,
        // so a zero length means the store was deliberately or accidentally wiped.
        // (Mirrors the node store's guard.)
        if std::fs::metadata(&path).is_ok_and(|m| m.len() == 0) {
            return Err(StoreError::Corrupt {
                channel_id: None,
                detail: format!(
                    "buyer channel store at {} is empty (length 0) — a truncation or filesystem \
                     rollback would silently wipe the voucher watermark. Restore from backup, or \
                     delete the file deliberately to start fresh (forfeiting channel reuse).",
                    path.display()
                ),
            });
        }
        // A second concurrent opener (another `decdn fetch`/`bundle pull` on the
        // same data dir) trips redb's process-exclusive write lock. Surface that
        // as a clear, dedicated error instead of a raw backend string — two
        // processes must not share one channel's voucher nonce (#942).
        let db = Database::create(&path).map_err(|err| match err {
            redb::DatabaseError::DatabaseAlreadyOpen => StoreError::AlreadyOpen { path },
            other => StoreError::Backend(format!("open buyer channel db: {other}")),
        })?;
        Ok(Self { db })
    }

    /// Bind the shared table operations to this store's database.
    const fn table(&self) -> BuyerChannelTable<'_> {
        BuyerChannelTable::new(&self.db)
    }
}

/// Every method delegates to [`crate::buyer_channel_table`]; this store owns the
/// file, not the logic.
impl BuyerChannelStore for RedbBuyerChannelStore {
    fn load_all(&self) -> Result<Vec<BuyerChannelState>, StoreError> {
        self.table().load_all()
    }

    fn record(&self, state: &BuyerChannelState) -> Result<(), StoreError> {
        self.table().record(state)
    }

    fn forget(&self, provider: Address) -> Result<(), StoreError> {
        self.table().forget(provider)
    }

    fn forget_if_channel(
        &self,
        provider: Address,
        channel_id: ChannelId,
    ) -> Result<bool, StoreError> {
        self.table().forget_if_channel(provider, channel_id)
    }

    fn get_by_provider(&self, provider: Address) -> Result<Option<BuyerChannelState>, StoreError> {
        self.table().get_by_provider(provider)
    }

    fn advance_progress(
        &self,
        provider: Address,
        channel_id: ChannelId,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.table()
            .advance_progress(provider, channel_id, nonce, bytes_delivered, amount)
    }

    fn add_deposit(
        &self,
        provider: Address,
        channel_id: ChannelId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        self.table().add_deposit(provider, channel_id, additional)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only what is specific to *this* store: `open()`'s guards, and that the
    /// trait delegations are wired to the right shared operation. The record
    /// codec and the table semantics are covered once, next to the shared
    /// implementation in `crate::buyer_channel_table`.
    fn store(dir: &tempfile::TempDir) -> anyhow::Result<RedbBuyerChannelStore> {
        // `open` hardens the data dir to 0o700 and rejects anything looser, so
        // point it at a subdir it creates rather than the tempdir root.
        Ok(RedbBuyerChannelStore::open(&dir.path().join("d"))?)
    }

    fn state(provider: u8, nonce: u64, bytes: u64, amount: u64) -> BuyerChannelState {
        BuyerChannelState {
            channel_id: alloy::primitives::B256::repeat_byte(provider),
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
    fn second_concurrent_open_is_already_open_not_raw_backend() -> anyhow::Result<()> {
        // #942: a second opener on the same data dir (a concurrent `decdn fetch`
        // / `bundle pull`) trips redb's process-exclusive write lock. It must
        // fail with the dedicated, user-facing `AlreadyOpen` — never a raw redb
        // backend string — and recover once the first handle drops.
        let dir = tempfile::tempdir()?;
        let data = dir.path().join("d");
        let first = RedbBuyerChannelStore::open(&data)?;

        let err = RedbBuyerChannelStore::open(&data)
            .err()
            .ok_or_else(|| anyhow::anyhow!("a second concurrent open must fail"))?;
        anyhow::ensure!(
            matches!(&err, StoreError::AlreadyOpen { path } if path == &data.join(BUYER_CHANNELS_DB_FILE)),
            "wrong error or path: {err:?}"
        );
        let msg = err.to_string();
        anyhow::ensure!(
            msg.contains("another decdn process is using") && msg.contains("--data-dir"),
            "message must be user-facing, got: {msg}"
        );

        // Releasing the first handle frees the lock; a fresh open then succeeds.
        drop(first);
        RedbBuyerChannelStore::open(&data)?;
        Ok(())
    }

    #[test]
    fn rejects_zero_length_db_file() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let data = dir.path().join("d");
        drop(store(&dir)?); // create, then release the lock
        // Truncate to zero: redb would treat this as "create fresh" and silently
        // wipe the watermark, so `open` must refuse.
        std::fs::write(data.join(BUYER_CHANNELS_DB_FILE), b"")?;
        let err = RedbBuyerChannelStore::open(&data)
            .err()
            .ok_or_else(|| anyhow::anyhow!("a zero-length db file must be rejected"))?;
        anyhow::ensure!(
            matches!(&err, StoreError::Corrupt { detail, .. } if detail.contains("empty (length 0)")),
            "expected Corrupt(empty), got {err:?}"
        );
        Ok(())
    }

    #[test]
    fn record_then_reuse_resumes_watermark_across_reopen() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let s = state(1, 5, 5_000, 50);
        {
            let store = store(&dir)?;
            store.record(&s)?;
        }
        // A later `decdn fetch` on the same data dir resumes the watermark
        // instead of re-signing a stale nonce (#940).
        let reopened = store(&dir)?;
        let got = reopened
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("row vanished across reopen"))?;
        anyhow::ensure!(got == s, "watermark must resume across invocations");
        Ok(())
    }

    /// Each of the seven trait methods must reach its matching shared operation
    /// with its arguments in the right order — the shared suite proves the
    /// operations are correct but cannot see this store's wiring.
    ///
    /// Method-level transpositions are mostly impossible (the types reject
    /// `forget` wired to `forget_if_channel`). The reachable mistake is *argument*
    /// order among `advance_progress`'s three `U256`s, which the compiler cannot
    /// catch and which silently corrupts a payment watermark — so assert the
    /// resulting **row**, not just the outcome enum.
    #[test]
    fn store_delegates_every_op() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = store(&dir)?;
        let s = state(2, 1, 1_000, 10);

        // record + get_by_provider + load_all
        store.record(&s)?;
        anyhow::ensure!(
            store.get_by_provider(s.provider)?.as_ref() == Some(&s),
            "record/get"
        );
        anyhow::ensure!(store.load_all()?.len() == 1, "load_all");

        // advance_progress
        anyhow::ensure!(
            store.advance_progress(
                s.provider,
                s.channel_id,
                U256::from(2u64),
                U256::from(2_000u64),
                U256::from(20u64),
            )? == AdvanceOutcome::Advanced,
            "advance_progress"
        );

        // add_deposit
        anyhow::ensure!(
            store.add_deposit(s.provider, s.channel_id, U256::from(7u64))?
                == DepositOutcome::Added(s.deposit + U256::from(7u64)),
            "add_deposit"
        );

        // The outcomes above are all reachable with the U256 arguments permuted;
        // only the row proves each landed in its own field.
        let row = store
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(
            row.last_nonce == U256::from(2u64),
            "last_nonce = {}",
            row.last_nonce
        );
        anyhow::ensure!(
            row.last_bytes_delivered == U256::from(2_000u64),
            "last_bytes_delivered = {}",
            row.last_bytes_delivered
        );
        anyhow::ensure!(
            row.last_amount == U256::from(20u64),
            "last_amount = {}",
            row.last_amount
        );
        anyhow::ensure!(
            row.deposit == s.deposit + U256::from(7u64),
            "deposit = {}",
            row.deposit
        );

        // forget_if_channel: wrong id leaves the row, right id deletes it.
        anyhow::ensure!(
            !store.forget_if_channel(s.provider, alloy::primitives::B256::repeat_byte(0xee))?,
            "forget_if_channel must not delete on a mismatched channel"
        );
        anyhow::ensure!(
            store.get_by_provider(s.provider)?.is_some(),
            "row must survive"
        );
        anyhow::ensure!(
            store.forget_if_channel(s.provider, s.channel_id)?,
            "forget_if_channel must delete on a match"
        );
        anyhow::ensure!(
            store.get_by_provider(s.provider)?.is_none(),
            "row must be gone"
        );

        // forget
        store.record(&s)?;
        store.forget(s.provider)?;
        anyhow::ensure!(store.load_all()?.is_empty(), "forget");
        Ok(())
    }
}
