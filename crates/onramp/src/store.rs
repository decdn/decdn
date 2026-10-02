use alloy::primitives::Address;
use redb::{Database, ReadableDatabase, TableDefinition};
use std::path::Path;

// signer(20) -> encoded GrantRecord. `&[u8]` value: redb 4.x has no `Value`
// impl for `Vec<u8>`, so the variable-length record is stored as a byte slice.
const GRANTS: TableDefinition<[u8; 20], &[u8]> = TableDefinition::new("grants_v1");

/// A capability issued to `signer`, persisted so `POST /fund` is idempotent
/// and `GET /capability` can return the exact token the client was given.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantRecord {
    pub spending_cap: u64,
    pub expiry: u64,
    pub issued_unix: u64,
    pub token: String,
}

impl GrantRecord {
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(24 + self.token.len());
        v.extend_from_slice(&self.spending_cap.to_be_bytes());
        v.extend_from_slice(&self.expiry.to_be_bytes());
        v.extend_from_slice(&self.issued_unix.to_be_bytes());
        v.extend_from_slice(self.token.as_bytes());
        v
    }

    fn decode(b: &[u8]) -> anyhow::Result<GrantRecord> {
        anyhow::ensure!(b.len() >= 24, "bad grant record length {}", b.len());
        let cap_bytes = b.get(0..8).ok_or_else(|| anyhow::anyhow!("cap"))?;
        let spending_cap = u64::from_be_bytes(
            cap_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("cap len"))?,
        );
        let exp_bytes = b.get(8..16).ok_or_else(|| anyhow::anyhow!("expiry"))?;
        let expiry = u64::from_be_bytes(
            exp_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("expiry len"))?,
        );
        let iss_bytes = b.get(16..24).ok_or_else(|| anyhow::anyhow!("issued"))?;
        let issued_unix = u64::from_be_bytes(
            iss_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("issued len"))?,
        );
        let token_bytes = b.get(24..).ok_or_else(|| anyhow::anyhow!("token"))?;
        let token = String::from_utf8(token_bytes.to_vec())
            .map_err(|_| anyhow::anyhow!("grant token not utf8"))?;
        Ok(GrantRecord {
            spending_cap,
            expiry,
            issued_unix,
            token,
        })
    }
}

pub struct Store {
    db: Database,
}

impl Store {
    pub fn open(dir: &Path) -> anyhow::Result<Store> {
        std::fs::create_dir_all(dir)?;
        let db = Database::create(dir.join("sponsor.redb"))?;
        let w = db.begin_write()?;
        {
            w.open_table(GRANTS)?;
        }
        w.commit()?;
        Ok(Store { db })
    }

    pub fn put_grant(&self, signer: Address, rec: &GrantRecord) -> anyhow::Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(GRANTS)?;
            t.insert(signer.into_array(), rec.encode().as_slice())?;
        }
        w.commit()?;
        Ok(())
    }

    pub fn get_grant(&self, signer: Address) -> anyhow::Result<Option<GrantRecord>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(GRANTS)?;
        let Some(raw) = t.get(signer.into_array())? else {
            return Ok(None);
        };
        Ok(Some(GrantRecord::decode(raw.value())?))
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    #[test]
    fn put_then_get_roundtrips_and_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path()).unwrap();
        let signer = address!("00000000000000000000000000000000000000aa");
        assert_eq!(s.get_grant(signer).unwrap(), None);
        let rec = GrantRecord {
            spending_cap: 10_000_000,
            expiry: 1_769_904_000,
            issued_unix: 1_767_312_000,
            token: "dcap1:AAAA".to_string(),
        };
        s.put_grant(signer, &rec).unwrap();
        assert_eq!(s.get_grant(signer).unwrap(), Some(rec.clone()));
        // idempotent re-issue overwrites in place
        let rec2 = GrantRecord {
            token: "dcap1:BBBB".to_string(),
            ..rec
        };
        s.put_grant(signer, &rec2).unwrap();
        assert_eq!(s.get_grant(signer).unwrap(), Some(rec2));
    }
}
