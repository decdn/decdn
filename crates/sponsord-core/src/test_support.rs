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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256, TxHash};
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use decdn_client::buyer_pool::TopUpUnconfirmed;
use decdn_incentive::voucher_domain;
use sponsord_api::MicroUsdc;

use crate::issuer::{Issuer, Limits};
use crate::pool::{Authorization, PoolChain, TxState};
use crate::sponsor::Sponsor;

/// The chain id [`fake_sponsor`] signs for (Arbitrum Sepolia's).
pub const TEST_CHAIN_ID: u64 = 421_614;
/// The `PaymentPool` address [`fake_sponsor`] signs for.
pub const TEST_PAYMENT_POOL: Address = Address::repeat_byte(0x22);
/// The pool id [`fake_sponsor`] signs for.
pub const TEST_POOL_ID: B256 = B256::repeat_byte(0x11);
/// The RPC URL `FakePool` failures name, with an API key in its path, so
/// tests can assert it never reaches a log line.
pub const FAKE_RPC_URL: &str = "http://rpc.example/v3/FAKE-RPC-KEY";
/// The `topUp` transaction an unconfirmed `FakePool` top-up names.
pub const FAKE_TOPUP_TX: TxHash = TxHash::repeat_byte(0xAB);

/// In-memory pool: fixed owner, a mutable remaining balance, a settable
/// per-signer registration map, a settable transaction state and confirmed
/// nonce, and switches to fail reads or top-ups. A failure reads like a raw
/// reqwest transport error naming [`FAKE_RPC_URL`]: unlike `ChainPool`, it
/// does not redact, so tests exercise the log sites' own redaction.
#[derive(Debug)]
pub struct FakePool {
    owner: Address,
    remaining: Mutex<MicroUsdc>,
    registrations: Mutex<HashMap<Address, Authorization>>,
    fail_remaining: AtomicBool,
    fail_authorization: AtomicBool,
    fail_top_up: AtomicBool,
    unconfirm_top_up: AtomicBool,
    lose_top_up_submit: AtomicBool,
    fail_tx_reads: AtomicBool,
    tx_state: Mutex<TxState>,
    pub(crate) confirmed_nonce: AtomicU64,
    top_up_calls: AtomicU64,
}

impl FakePool {
    /// A pool owned by `owner` holding `remaining` micro-USDC, with no
    /// registrations and every failure switch off.
    #[must_use]
    pub fn new(owner: Address, remaining: u64) -> Self {
        Self {
            owner,
            remaining: Mutex::new(MicroUsdc(remaining)),
            registrations: Mutex::new(HashMap::new()),
            fail_remaining: AtomicBool::new(false),
            fail_authorization: AtomicBool::new(false),
            fail_top_up: AtomicBool::new(false),
            unconfirm_top_up: AtomicBool::new(false),
            lose_top_up_submit: AtomicBool::new(false),
            fail_tx_reads: AtomicBool::new(false),
            tx_state: Mutex::new(TxState::Unknown),
            confirmed_nonce: AtomicU64::new(0),
            top_up_calls: AtomicU64::new(0),
        }
    }

    /// Record `signer` as registered on-chain with `auth`.
    pub fn register(&self, signer: Address, auth: Authorization) {
        self.registrations.lock().unwrap().insert(signer, auth);
    }

    /// Make every `remaining` read fail (an RPC outage).
    pub fn fail_remaining_reads(&self, fail: bool) {
        self.fail_remaining.store(fail, Ordering::SeqCst);
    }

    /// Make every `authorization` read fail (an RPC outage).
    pub fn fail_authorization_reads(&self, fail: bool) {
        self.fail_authorization.store(fail, Ordering::SeqCst);
    }

    /// Make every `top_up` fail (an out-of-funds treasury).
    pub fn fail_top_ups(&self, fail: bool) {
        self.fail_top_up.store(fail, Ordering::SeqCst);
    }

    /// Make every `top_up` broadcast [`FAKE_TOPUP_TX`] but fail to read its
    /// receipt: the error carries decdn's `TopUpUnconfirmed`, and nothing is
    /// credited. Its nonce is the one `confirmed_nonce` reports, the
    /// treasury's next while nothing else is pending.
    pub fn unconfirm_top_ups(&self, unconfirm: bool) {
        self.unconfirm_top_up.store(unconfirm, Ordering::SeqCst);
    }

    /// Make every `top_up` submit fail in transport, as if the RPC node may
    /// have broadcast it without returning its hash: the error carries
    /// decdn's `TopUpUnconfirmed` with no transaction, at the nonce
    /// [`FakePool::unconfirm_top_ups`] uses, and nothing is credited.
    pub fn lose_top_up_submits(&self, lose: bool) {
        self.lose_top_up_submit.store(lose, Ordering::SeqCst);
    }

    /// Make every `transaction` and `confirmed_nonce` read fail (an RPC
    /// outage).
    pub fn fail_tx_reads(&self, fail: bool) {
        self.fail_tx_reads.store(fail, Ordering::SeqCst);
    }

    /// What `transaction` reports, for any hash. Starts [`TxState::Unknown`].
    pub fn set_tx_state(&self, state: TxState) {
        *self.tx_state.lock().unwrap() = state;
    }

    /// What `confirmed_nonce` reports. Starts at 0. Named for the RPC's
    /// transaction count: `CodeQL` takes a literal passed to a `*nonce*`
    /// function for a hard-coded cryptographic nonce.
    pub fn set_confirmed_tx_count(&self, count: u64) {
        self.confirmed_nonce.store(count, Ordering::SeqCst);
    }

    /// How many times `top_up` has been called, failed or not.
    #[must_use]
    pub fn top_up_calls(&self) -> u64 {
        self.top_up_calls.load(Ordering::SeqCst)
    }

    /// The pool's current remaining balance.
    #[must_use]
    pub fn remaining_now(&self) -> MicroUsdc {
        *self.remaining.lock().unwrap()
    }
}

#[async_trait]
impl PoolChain for FakePool {
    fn owner_address(&self) -> Address {
        self.owner
    }
    async fn remaining(&self, _pool_id: B256) -> anyhow::Result<MicroUsdc> {
        anyhow::ensure!(
            !self.fail_remaining.load(Ordering::SeqCst),
            "remaining read failed: error sending request for url ({FAKE_RPC_URL})"
        );
        Ok(self.remaining_now())
    }
    async fn top_up(&self, _pool_id: B256, additional: MicroUsdc) -> anyhow::Result<MicroUsdc> {
        self.top_up_calls.fetch_add(1, Ordering::SeqCst);
        let nonce = self.confirmed_nonce.load(Ordering::SeqCst);
        // Each shaped like the matching decdn `top_up` error.
        if self.lose_top_up_submit.load(Ordering::SeqCst) {
            return Err(
                anyhow::anyhow!("error sending request for url ({FAKE_RPC_URL})")
                    .context("submit topUp")
                    .context(TopUpUnconfirmed { tx: None, nonce }),
            );
        }
        if self.unconfirm_top_up.load(Ordering::SeqCst) {
            return Err(
                anyhow::anyhow!("error sending request for url ({FAKE_RPC_URL})")
                    .context("await topUp receipt")
                    .context(TopUpUnconfirmed {
                        tx: Some(FAKE_TOPUP_TX),
                        nonce,
                    }),
            );
        }
        anyhow::ensure!(
            !self.fail_top_up.load(Ordering::SeqCst),
            "top-up failed: error sending request for url ({FAKE_RPC_URL})"
        );
        let mut r = self.remaining.lock().unwrap();
        *r = r.saturating_add(additional);
        Ok(additional)
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
            "authorization read failed: error sending request for url ({FAKE_RPC_URL})"
        );
        Ok(self.registrations.lock().unwrap().get(&signer).copied())
    }
    async fn transaction(&self, _tx: TxHash) -> anyhow::Result<TxState> {
        anyhow::ensure!(
            !self.fail_tx_reads.load(Ordering::SeqCst),
            "transaction read failed: error sending request for url ({FAKE_RPC_URL})"
        );
        Ok(*self.tx_state.lock().unwrap())
    }
    async fn confirmed_nonce(&self) -> anyhow::Result<u64> {
        anyhow::ensure!(
            !self.fail_tx_reads.load(Ordering::SeqCst),
            "nonce read failed: error sending request for url ({FAKE_RPC_URL})"
        );
        Ok(self.confirmed_nonce.load(Ordering::SeqCst))
    }
}

/// A `Sponsor` with a random owner key over a `FakePool` that owns the test
/// pool. The returned pool is the same instance the sponsor reads, so tests
/// can register signers or fail reads on it.
pub async fn fake_sponsor(max_spending_cap: u64, max_ttl_secs: u64) -> (Sponsor, Arc<FakePool>) {
    let signer = PrivateKeySigner::random();
    let pool = Arc::new(FakePool::new(signer.address(), 100_000_000));
    let issuer = Issuer::new(
        signer,
        voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL),
        TEST_POOL_ID,
        Limits {
            max_spending_cap: MicroUsdc(max_spending_cap),
            max_ttl_secs,
        },
    );
    let sponsor = Sponsor::new(
        issuer,
        pool.clone() as Arc<dyn PoolChain>,
        TEST_CHAIN_ID,
        TEST_PAYMENT_POOL,
    )
    .await
    .unwrap();
    (sponsor, pool)
}

/// `tracing` output captured from the current thread, for asserting what a
/// test logged and what it must not (an RPC URL).
#[derive(Debug, Clone, Default)]
pub struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl CapturedLog {
    /// Send this thread's events here until the guard drops.
    /// `#[tokio::test]`'s current-thread runtime keeps the test's futures on
    /// this thread.
    #[must_use]
    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
        let sink = self.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(move || sink.clone())
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    /// Everything logged so far.
    #[must_use]
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
