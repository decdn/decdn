//! Test doubles for code built on `sponsord-core`. Compiled for this
//! crate's own tests and, behind the `test-support` feature, for dependents'
//! tests.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256};
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use decdn_incentive::voucher_domain;

use crate::MicroUsdc;
use crate::issuer::Issuer;
use crate::pool::{Authorization, PoolChain};
use crate::sponsor::Sponsor;

pub const TEST_CHAIN_ID: u64 = 421_614;
pub const TEST_PAYMENT_POOL: Address = Address::repeat_byte(0x22);
pub const TEST_POOL_ID: B256 = B256::repeat_byte(0x11);

/// In-memory pool: fixed owner, a mutable remaining balance, and a
/// settable per-signer registration map.
pub struct FakePool {
    owner: Address,
    remaining: Mutex<u64>,
    registrations: Mutex<HashMap<Address, Authorization>>,
    fail_authorization: AtomicBool,
}

impl FakePool {
    #[must_use]
    pub fn new(owner: Address, remaining: u64) -> Self {
        Self {
            owner,
            remaining: Mutex::new(remaining),
            registrations: Mutex::new(HashMap::new()),
            fail_authorization: AtomicBool::new(false),
        }
    }

    /// Record `signer` as registered on-chain with `auth`.
    pub fn register(&self, signer: Address, auth: Authorization) {
        self.registrations.lock().unwrap().insert(signer, auth);
    }

    /// Make every `authorization` read fail (an RPC outage).
    pub fn fail_authorization_reads(&self, fail: bool) {
        self.fail_authorization.store(fail, Ordering::SeqCst);
    }
}

#[async_trait]
impl PoolChain for FakePool {
    fn owner_address(&self) -> Address {
        self.owner
    }
    async fn remaining(&self, _pool_id: B256) -> anyhow::Result<MicroUsdc> {
        let r = self
            .remaining
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned"))?;
        Ok(MicroUsdc(*r))
    }
    async fn top_up(&self, _pool_id: B256, additional: MicroUsdc) -> anyhow::Result<MicroUsdc> {
        let mut r = self
            .remaining
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned"))?;
        *r = r.saturating_add(additional.0);
        Ok(MicroUsdc(*r))
    }
    async fn pool_owner(&self, _pool_id: B256) -> anyhow::Result<Address> {
        Ok(self.owner)
    }
    async fn authorization(
        &self,
        _pool_id: B256,
        signer: Address,
    ) -> anyhow::Result<Option<Authorization>> {
        anyhow::ensure!(
            !self.fail_authorization.load(Ordering::SeqCst),
            "authorization read failed"
        );
        Ok(self.registrations.lock().unwrap().get(&signer).copied())
    }
}

/// A `Sponsor` with a random owner key over a `FakePool` that owns the
/// test pool. The returned pool is the same instance the sponsor reads,
/// so tests can register signers or fail reads on it.
pub async fn fake_sponsor(max_spending_cap: u64, max_ttl_secs: u64) -> (Sponsor, Arc<FakePool>) {
    let signer = PrivateKeySigner::random();
    let pool = Arc::new(FakePool::new(signer.address(), 100_000_000));
    let issuer = Issuer::new(
        signer,
        voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL),
        TEST_POOL_ID,
        max_spending_cap,
        max_ttl_secs,
    );
    let sponsor = Sponsor::from_parts(
        issuer,
        pool.clone() as Arc<dyn PoolChain>,
        TEST_CHAIN_ID,
        TEST_PAYMENT_POOL,
    )
    .await
    .unwrap();
    (sponsor, pool)
}
