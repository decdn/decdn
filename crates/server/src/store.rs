use crate::money::MicroUsdc;
use alloy::primitives::{Address, B256};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::path::Path;

// value encodings are fixed-width big-endian byte arrays for stable ordering.
// redb 4.x still has no `Value` impl for `Vec<u8>` (only `&[u8]`, fixed-size
// arrays, and a handful of primitives), so the channel record table keeps
// `&[u8]` as its value type — unchanged from the redb 2.x layout.
const CHANNELS: TableDefinition<[u8; 32], &[u8]> = TableDefinition::new("channels_v1");
const CLIENT_INDEX: TableDefinition<[u8; 52], [u8; 32]> = TableDefinition::new("client_index_v1"); // client(20)|node(32)
const CAP: TableDefinition<[u8; 24], u64> = TableDefinition::new("cap_v1"); // client(20)|bucket(4)

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelRecord {
    pub client: Address,
    pub provider: Address,
    pub node_id: [u8; 32],
    pub deposit_micro: u64,
    pub opened_unix: u64,
}

impl ChannelRecord {
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(20 + 20 + 32 + 8 + 8);
        v.extend_from_slice(self.client.as_slice());
        v.extend_from_slice(self.provider.as_slice());
        v.extend_from_slice(&self.node_id);
        v.extend_from_slice(&self.deposit_micro.to_be_bytes());
        v.extend_from_slice(&self.opened_unix.to_be_bytes());
        v
    }

    fn decode(b: &[u8]) -> anyhow::Result<ChannelRecord> {
        anyhow::ensure!(b.len() == 88, "bad channel record length {}", b.len());
        let client = Address::from_slice(b.get(0..20).ok_or_else(|| anyhow::anyhow!("client"))?);
        let provider =
            Address::from_slice(b.get(20..40).ok_or_else(|| anyhow::anyhow!("provider"))?);
        let node_slice = b.get(40..72).ok_or_else(|| anyhow::anyhow!("node"))?;
        let node_id: [u8; 32] = node_slice
            .try_into()
            .map_err(|_| anyhow::anyhow!("node length"))?;
        let deposit_bytes = b.get(72..80).ok_or_else(|| anyhow::anyhow!("dep"))?;
        let deposit_micro = u64::from_be_bytes(
            deposit_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("dep length"))?,
        );
        let ts_bytes = b.get(80..88).ok_or_else(|| anyhow::anyhow!("ts"))?;
        let opened_unix = u64::from_be_bytes(
            ts_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("ts length"))?,
        );
        Ok(ChannelRecord {
            client,
            provider,
            node_id,
            deposit_micro,
            opened_unix,
        })
    }
}

fn client_node_key(client: Address, node_id: [u8; 32]) -> [u8; 52] {
    let mut k = [0u8; 52];
    if let Some(dst) = k.get_mut(0..20) {
        dst.copy_from_slice(client.as_slice());
    }
    if let Some(dst) = k.get_mut(20..52) {
        dst.copy_from_slice(&node_id);
    }
    k
}

fn cap_key(client: Address, bucket: u32) -> [u8; 24] {
    let mut k = [0u8; 24];
    if let Some(dst) = k.get_mut(0..20) {
        dst.copy_from_slice(client.as_slice());
    }
    if let Some(dst) = k.get_mut(20..24) {
        dst.copy_from_slice(&bucket.to_be_bytes());
    }
    k
}

pub struct Store {
    db: Database,
}

impl Store {
    pub fn open(dir: &Path) -> anyhow::Result<Store> {
        std::fs::create_dir_all(dir)?;
        let db = Database::create(dir.join("sponsor.redb"))?;
        // create tables up front so read-only transactions don't fail on a
        // fresh database before any writer has touched them.
        let w = db.begin_write()?;
        {
            w.open_table(CHANNELS)?;
            w.open_table(CLIENT_INDEX)?;
            w.open_table(CAP)?;
        }
        w.commit()?;
        Ok(Store { db })
    }

    pub fn insert_channel(&self, id: B256, rec: &ChannelRecord) -> anyhow::Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut ch = w.open_table(CHANNELS)?;
            ch.insert(id.0, rec.encode().as_slice())?;
            let mut idx = w.open_table(CLIENT_INDEX)?;
            idx.insert(client_node_key(rec.client, rec.node_id), id.0)?;
        }
        w.commit()?;
        Ok(())
    }

    pub fn get_by_client_node(
        &self,
        client: Address,
        node_id: [u8; 32],
    ) -> anyhow::Result<Option<(B256, ChannelRecord)>> {
        let r = self.db.begin_read()?;
        let idx = r.open_table(CLIENT_INDEX)?;
        let Some(id) = idx.get(client_node_key(client, node_id))? else {
            return Ok(None);
        };
        let id = B256::from(id.value());
        let ch = r.open_table(CHANNELS)?;
        let Some(raw) = ch.get(id.0)? else {
            return Ok(None);
        };
        Ok(Some((id, ChannelRecord::decode(raw.value())?)))
    }

    pub fn get_by_channel(&self, id: B256) -> anyhow::Result<Option<ChannelRecord>> {
        let r = self.db.begin_read()?;
        let ch = r.open_table(CHANNELS)?;
        let Some(raw) = ch.get(id.0)? else {
            return Ok(None);
        };
        Ok(Some(ChannelRecord::decode(raw.value())?))
    }

    pub fn cap_spent(&self, client: Address, bucket: u32) -> anyhow::Result<MicroUsdc> {
        let r = self.db.begin_read()?;
        let cap = r.open_table(CAP)?;
        Ok(MicroUsdc(
            cap.get(cap_key(client, bucket))?
                .map(|v| v.value())
                .unwrap_or(0),
        ))
    }

    pub fn cap_add(
        &self,
        client: Address,
        bucket: u32,
        amount: MicroUsdc,
    ) -> anyhow::Result<MicroUsdc> {
        let w = self.db.begin_write()?;
        let new_total;
        {
            let mut cap = w.open_table(CAP)?;
            let prev = cap
                .get(cap_key(client, bucket))?
                .map(|v| v.value())
                .unwrap_or(0);
            new_total = prev.saturating_add(amount.0);
            cap.insert(cap_key(client, bucket), new_total)?;
        }
        w.commit()?;
        Ok(MicroUsdc(new_total))
    }

    pub fn cap_refund(
        &self,
        client: Address,
        bucket: u32,
        amount: MicroUsdc,
    ) -> anyhow::Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut cap = w.open_table(CAP)?;
            let prev = cap
                .get(cap_key(client, bucket))?
                .map(|v| v.value())
                .unwrap_or(0);
            cap.insert(cap_key(client, bucket), prev.saturating_sub(amount.0))?;
        }
        w.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{address, b256};

    #[test]
    fn insert_is_idempotent_lookup_and_cap_accumulates() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path()).unwrap();
        let client = address!("00000000000000000000000000000000000000aa");
        let id = b256!("11111111111111111111111111111111111111111111111111111111111111ff");
        let rec = ChannelRecord {
            client,
            provider: address!("00000000000000000000000000000000000000bb"),
            node_id: [7u8; 32],
            deposit_micro: 2_000_000,
            opened_unix: 1_769_904_000,
        };
        s.insert_channel(id, &rec).unwrap();
        let (got_id, got) = s.get_by_client_node(client, [7u8; 32]).unwrap().unwrap();
        assert_eq!(got_id, id);
        assert_eq!(got.deposit_micro, 2_000_000);

        let bucket = 42;
        assert_eq!(s.cap_spent(client, bucket).unwrap(), MicroUsdc(0));
        let total = s.cap_add(client, bucket, MicroUsdc(2_000_000)).unwrap();
        assert_eq!(total, MicroUsdc(2_000_000));
        let total = s.cap_add(client, bucket, MicroUsdc(2_000_000)).unwrap();
        assert_eq!(total, MicroUsdc(4_000_000));
        assert_eq!(s.cap_spent(client, bucket).unwrap(), MicroUsdc(4_000_000));
    }

    #[test]
    fn cap_refund_saturates() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path()).unwrap();
        let client = address!("00000000000000000000000000000000000000aa");
        let bucket = 42;

        // add some cap, then refund part of it
        let _total = s.cap_add(client, bucket, MicroUsdc(5_000_000)).unwrap();
        s.cap_refund(client, bucket, MicroUsdc(2_000_000)).unwrap();
        assert_eq!(s.cap_spent(client, bucket).unwrap(), MicroUsdc(3_000_000));

        // refund more than exists, should saturate at 0
        s.cap_refund(client, bucket, MicroUsdc(10_000_000)).unwrap();
        assert_eq!(s.cap_spent(client, bucket).unwrap(), MicroUsdc(0));
    }
}
