//! Fakes for the HTTP contract tests (`tests/http_contract.rs`).
//!
//! Deliberately **not** gated behind `#[cfg(test)]`: integration tests under
//! `tests/` are a separate crate that links against the `sponsord` *library*
//! target built without `cfg(test)` (only unit tests compiled via `cargo test
//! --lib` see `cfg(test)` items). So this module has to be a plain `pub mod`
//! for `cargo test -p sponsord` to run `http_contract.rs` with no extra
//! feature flag. `#[allow(dead_code)]` keeps a normal `cargo build`/`clippy`
//! pristine, since nothing in the production binary calls these symbols.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256};
use async_trait::async_trait;

use crate::cap::Cap;
use crate::captcha::CaptchaVerifier;
use crate::config::ServerConfig;
use crate::discovery::{NodePick, ProviderResolver};
use crate::money::MicroUsdc;
use crate::state::AppState;
use crate::store::Store;
use crate::treasury::Treasury;

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

/// Knobs `app_state_with_options` exposes to the contract tests.
pub struct FakeOptions {
    /// Whether the injected `FakeCaptcha` reports success.
    pub captcha_passes: bool,
    /// The monthly cap the injected `Cap` policy enforces.
    pub monthly_cap: MicroUsdc,
}

impl Default for FakeOptions {
    fn default() -> Self {
        Self {
            captcha_passes: true,
            monthly_cap: MicroUsdc(10_000_000),
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
        initial_deposit: MicroUsdc(2_000_000),
        working_balance: MicroUsdc(2_000_000),
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
