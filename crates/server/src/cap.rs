use crate::money::{MicroUsdc, month_bucket};
use crate::store::Store;
use alloy::primitives::Address;

#[derive(Clone)]
pub struct Cap {
    pub monthly_limit: MicroUsdc,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CapDecision {
    Allowed { new_total: MicroUsdc },
    Exhausted { spent: MicroUsdc, limit: MicroUsdc },
}

impl Cap {
    pub fn check_and_reserve(
        &self,
        store: &Store,
        client: Address,
        now_unix: u64,
        want: MicroUsdc,
    ) -> anyhow::Result<CapDecision> {
        let bucket = month_bucket(now_unix);
        let spent = store.cap_spent(client, bucket)?;
        if spent.0.saturating_add(want.0) > self.monthly_limit.0 {
            return Ok(CapDecision::Exhausted {
                spent,
                limit: self.monthly_limit,
            });
        }
        let new_total = store.cap_add(client, bucket, want)?;
        Ok(CapDecision::Allowed { new_total })
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
    use crate::store::Store;
    use alloy::primitives::address;

    #[test]
    fn reserves_until_limit_then_exhausts() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let cap = Cap {
            monthly_limit: MicroUsdc(10_000_000),
        };
        let c = address!("00000000000000000000000000000000000000aa");
        let now = 1_769_904_000;
        // five $2 reservations fit
        for i in 1..=5 {
            match cap
                .check_and_reserve(&store, c, now, MicroUsdc(2_000_000))
                .unwrap()
            {
                CapDecision::Allowed { new_total } => assert_eq!(new_total.0, i * 2_000_000),
                CapDecision::Exhausted { .. } => panic!("should fit at {i}"),
            }
        }
        // sixth does not
        match cap
            .check_and_reserve(&store, c, now, MicroUsdc(2_000_000))
            .unwrap()
        {
            CapDecision::Exhausted { spent, limit } => {
                assert_eq!(spent.0, 10_000_000);
                assert_eq!(limit.0, 10_000_000);
            }
            CapDecision::Allowed { .. } => panic!("should be exhausted"),
        }
    }
}
