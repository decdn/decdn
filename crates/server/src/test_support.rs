//! Fakes for the HTTP contract tests (`tests/http_contract.rs`).
//!
//! This file is **not** part of the `sponsord` lib/bin: it is pulled in only
//! by the integration test binary via
//! `#[path = "../src/test_support.rs"] mod test_support;` at the top of
//! `tests/http_contract.rs`. That keeps these fakes (and their `tempfile`
//! dependency, panicking constructors, etc.) out of the shipped `sponsord`
//! library and binary entirely — a normal `cargo build -p sponsord` never
//! compiles this file. All types referenced below are already `pub` in the
//! lib, so the test binary can build this module against them with no
//! feature flag. `#[allow(dead_code)]` keeps `cargo clippy --all-targets`
//! pristine for any fake surface a given test binary doesn't exercise.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256};
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;

use sponsord::cap::Cap;
use sponsord::captcha::CaptchaVerifier;
use sponsord::config::ServerConfig;
use sponsord::discovery::{NodePick, ProviderResolver};
use sponsord::money::MicroUsdc;
use sponsord::state::AppState;
use sponsord::store::Store;
use sponsord::treasury::Treasury;

/// In-memory `Treasury`: same fake shape as the `treasury` module's own
/// unit-test double, made `pub` so the HTTP contract tests can inject it.
#[derive(Default)]
pub struct FakeTreasury {
    deposit: Mutex<HashMap<B256, u64>>,
    next_id: AtomicU8,
}

#[async_trait]
impl Treasury for FakeTreasury {
    async fn open(
        &self,
        _provider: Address,
        _voucher_signer: Address,
        deposit: MicroUsdc,
    ) -> anyhow::Result<B256> {
        let n = self.next_id.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
        let id = B256::repeat_byte(n);
        let mut d = self
            .deposit
            .lock()
            .map_err(|_| anyhow::anyhow!("fake treasury mutex poisoned"))?;
        d.insert(id, deposit.0);
        Ok(id)
    }

    async fn deposit_of(&self, channel_id: B256) -> anyhow::Result<MicroUsdc> {
        let d = self
            .deposit
            .lock()
            .map_err(|_| anyhow::anyhow!("fake treasury mutex poisoned"))?;
        Ok(MicroUsdc(d.get(&channel_id).copied().unwrap_or(0)))
    }

    async fn top_up_to(
        &self,
        channel_id: B256,
        _provider: Address,
        target: MicroUsdc,
    ) -> anyhow::Result<MicroUsdc> {
        let mut d = self
            .deposit
            .lock()
            .map_err(|_| anyhow::anyhow!("fake treasury mutex poisoned"))?;
        let cur = d.get(&channel_id).copied().unwrap_or(0);
        let new = cur.max(target.0);
        d.insert(channel_id, new);
        Ok(MicroUsdc(new))
    }
}

/// Pass-through captcha whose verdict can be flipped after construction —
/// the 403 test builds one with `pass = false`.
pub struct FakeCaptcha {
    pass: AtomicBool,
}

impl FakeCaptcha {
    #[must_use]
    pub fn new(pass: bool) -> Self {
        Self {
            pass: AtomicBool::new(pass),
        }
    }
}

#[async_trait]
impl CaptchaVerifier for FakeCaptcha {
    async fn verify(&self, _token: &str, _remote_ip: Option<&str>) -> anyhow::Result<bool> {
        Ok(self.pass.load(Ordering::SeqCst))
    }
}

/// Always resolves to one fixed node/provider pair.
pub struct FakeResolver {
    pub pick: NodePick,
}

#[async_trait]
impl ProviderResolver for FakeResolver {
    async fn resolve(&self, _hash: [u8; 32]) -> anyhow::Result<Option<NodePick>> {
        Ok(Some(self.pick))
    }
}

/// A real signer the contract tests can use as a channel's `client` /
/// voucher-signer address, so `/topup`'s EIP-191 signature-verification path
/// can be exercised with a signature the recovered address actually matches
/// — the fixed `CLIENT` test constant has no known private key, so it can
/// never drive the `/topup` 200 happy path.
pub struct FakeClient {
    pub signer: PrivateKeySigner,
    pub address: Address,
}

impl FakeClient {
    /// A fresh random signer/address pair.
    #[must_use]
    pub fn random() -> Self {
        let signer = PrivateKeySigner::random();
        let address = signer.address();
        Self { signer, address }
    }
}

/// Knobs `app_state_with_options` exposes to the contract tests.
pub struct FakeOptions {
    /// Whether the injected `FakeCaptcha` reports success.
    pub captcha_passes: bool,
    /// The monthly cap the injected `Cap` policy enforces.
    pub monthly_cap: MicroUsdc,
    /// `ServerConfig::initial_deposit` — the deposit `/fund` opens a fresh
    /// channel with.
    pub initial_deposit: MicroUsdc,
    /// `ServerConfig::working_balance` — the target `/topup` funds a
    /// channel back up to. Set this above `initial_deposit` in a test that
    /// needs to exercise the real `top_up_to` delta path rather than
    /// `/topup`'s already-at-target short-circuit.
    pub working_balance: MicroUsdc,
}

impl Default for FakeOptions {
    fn default() -> Self {
        Self {
            captcha_passes: true,
            monthly_cap: MicroUsdc(10_000_000),
            initial_deposit: MicroUsdc(2_000_000),
            working_balance: MicroUsdc(2_000_000),
        }
    }
}

/// Build an `AppState` with a real (temp-dir-backed) `Store` — exercising
/// the same cap/idempotency persistence path production traffic hits — but
/// fake `Treasury`, `CaptchaVerifier`, and `ProviderResolver`.
///
/// The backing temp directory is intentionally leaked (`TempDir::keep`) so
/// it outlives this function; test binaries are short-lived processes and
/// the OS reclaims it on exit.
#[must_use]
pub fn app_state_with_options(opts: FakeOptions) -> AppState {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let data_dir = dir.keep();
    let store = Arc::new(Store::open(&data_dir).unwrap_or_else(|e| panic!("open store: {e}")));
    let cap = Cap {
        monthly_limit: opts.monthly_cap,
    };
    let treasury: Arc<dyn Treasury> = Arc::new(FakeTreasury::default());
    let turnstile: Arc<dyn CaptchaVerifier> = Arc::new(FakeCaptcha::new(opts.captcha_passes));
    let discovery: Arc<dyn ProviderResolver> = Arc::new(FakeResolver {
        pick: NodePick {
            node_id: [7u8; 32],
            provider: Address::repeat_byte(0xbb),
        },
    });
    let cfg = ServerConfig {
        bind: "127.0.0.1:0"
            .parse()
            .unwrap_or_else(|e| panic!("bind addr: {e}")),
        rpc_url: "http://localhost:8545".into(),
        chain_id: 421_614,
        payment_channel: Address::ZERO,
        capacity_bond: Address::ZERO,
        treasury_keystore: PathBuf::from("/dev/null"),
        initial_deposit: opts.initial_deposit,
        working_balance: opts.working_balance,
        monthly_cap: opts.monthly_cap,
        turnstile_secret: "secret".into(),
        turnstile_sitekey: "sitekey".into(),
        data_dir,
        topup_max_skew_secs: 120,
    };
    AppState {
        store,
        cap,
        treasury,
        turnstile,
        cfg: Arc::new(cfg),
        discovery,
    }
}

/// Default fakes: captcha passes, a generous monthly cap, one fixed node.
#[must_use]
pub fn app_state_with_fakes() -> AppState {
    app_state_with_options(FakeOptions::default())
}
