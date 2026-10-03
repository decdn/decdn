#![allow(dead_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use alloy::primitives::Address;
use async_trait::async_trait;

use sponsord_api::client::DaemonError;
use sponsord_api::daemon::{Info, IssueRequest, IssueResponse};
use sponsord_api::time::SystemClock;
use sponsord_api::{ErrorCode, IssuedCapability, MicroUsdc};
use sponsord_onramp::captcha::CaptchaVerifier;
use sponsord_onramp::config::{OnrampConfig, ReleasePin};
use sponsord_onramp::daemon::CapabilitySource;
use sponsord_onramp::state::AppState;
use sponsord_onramp::store::Store;

pub fn test_info() -> Info {
    Info {
        chain_id: 421_614,
        payment_pool: Address::repeat_byte(0x22),
        max_spending_cap: MicroUsdc(5_000_000),
        max_ttl_secs: 172_800,
    }
}

pub fn test_config(data_dir: PathBuf) -> OnrampConfig {
    OnrampConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        public_url: "https://up.decdn.org".into(),
        daemon_url: "http://127.0.0.1:8090".into(),
        daemon_token: "t".repeat(32).as_str().into(),
        rpc_url: "https://rpc.example".into(),
        capacity_bond: Address::ZERO,
        spending_cap: None,
        ttl_secs: None,
        turnstile_secret: "secret".into(),
        turnstile_sitekey: "TEST_SITEKEY".into(),
        data_dir,
        releases_base: "https://github.com/decdn".into(),
        decdn_release: ReleasePin {
            tag: "v0.1.0".into(),
            sums_sha256: "ab".repeat(32),
        },
        cli_release: ReleasePin {
            tag: "v0.2.0".into(),
            sums_sha256: "cd".repeat(32),
        },
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceBehavior {
    Issue,
    SignerExpired,
    Unavailable,
    Rejected,
}

/// In-memory daemon: hands out `dcap1:FAKE<n>` tokens with the requested
/// terms (omitted = `test_info()` maximums), or fails as configured.
pub struct FakeCapabilitySource {
    behavior: SourceBehavior,
    issued: AtomicUsize,
    last_request: Mutex<Option<IssueRequest>>,
}

impl FakeCapabilitySource {
    pub fn new(behavior: SourceBehavior) -> Self {
        Self {
            behavior,
            issued: AtomicUsize::new(0),
            last_request: Mutex::new(None),
        }
    }
    pub fn issued_count(&self) -> usize {
        self.issued.load(Ordering::SeqCst)
    }
    pub fn last_request(&self) -> Option<IssueRequest> {
        self.last_request.lock().unwrap().clone()
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[async_trait]
impl CapabilitySource for FakeCapabilitySource {
    async fn issue(&self, req: &IssueRequest) -> Result<IssueResponse, DaemonError> {
        *self.last_request.lock().unwrap() = Some(req.clone());
        match self.behavior {
            SourceBehavior::Issue => {
                let n = self.issued.fetch_add(1, Ordering::SeqCst) + 1;
                let info = test_info();
                Ok(IssueResponse {
                    capability: IssuedCapability {
                        token: format!("dcap1:FAKE{n}"),
                        spending_cap: req.spending_cap.unwrap_or(info.max_spending_cap),
                        expiry: now_unix() + req.ttl_secs.unwrap_or(info.max_ttl_secs),
                    },
                    registered: false,
                })
            }
            SourceBehavior::SignerExpired => Err(DaemonError::SignerExpired { expiry: 1_000 }),
            SourceBehavior::Unavailable => Err(DaemonError::Unavailable("down".into())),
            SourceBehavior::Rejected => Err(DaemonError::Rejected {
                status: 401,
                code: ErrorCode::Unauthorized,
            }),
        }
    }

    async fn info(&self) -> Result<Info, DaemonError> {
        Ok(test_info())
    }
}

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

pub struct FakeOptions {
    pub captcha_passes: bool,
    pub source: SourceBehavior,
    pub spending_cap: Option<MicroUsdc>,
    pub ttl_secs: Option<u64>,
}
impl Default for FakeOptions {
    fn default() -> Self {
        Self {
            captcha_passes: true,
            source: SourceBehavior::Issue,
            spending_cap: None,
            ttl_secs: None,
        }
    }
}

/// Build an `AppState` with a real temp-dir `Store` and fake daemon and
/// captcha, skipping `state::build`'s daemon round trip. The temp dir is
/// leaked so it outlives this call; test binaries are short-lived.
#[must_use]
pub fn app_state_with_options(opts: FakeOptions) -> (AppState, Arc<FakeCapabilitySource>) {
    let data_dir = tempfile::tempdir().unwrap().keep();
    let store = Arc::new(Store::open(&data_dir).unwrap());
    let source = Arc::new(FakeCapabilitySource::new(opts.source));
    let mut cfg = test_config(data_dir);
    cfg.spending_cap = opts.spending_cap;
    cfg.ttl_secs = opts.ttl_secs;
    let state = AppState {
        store,
        source: source.clone() as Arc<dyn CapabilitySource>,
        turnstile: Arc::new(FakeCaptcha::new(opts.captcha_passes)),
        cfg: Arc::new(cfg),
        chain: Arc::new(test_info()),
        clock: Arc::new(SystemClock),
    };
    (state, source)
}

#[must_use]
pub fn app_state_with_fakes() -> AppState {
    app_state_with_options(FakeOptions::default()).0
}
