//! Fakes for testing code built on `sponsord-onramp`: a daemon
//! ([`FakeCapabilitySource`]), a gate ([`FakeGate`]), and a ready
//! [`AppState`] over both. Compiled for this crate's tests and, behind the
//! `test-support` feature, for dependents'.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use alloy_primitives::Address;
use async_trait::async_trait;
use sponsord_api::client::DaemonError;
use sponsord_api::daemon::{Info, IssueRequest, IssueResponse};
use sponsord_api::time::{Clock, SystemClock};
use sponsord_api::{ErrorCode, IssuedCapability, MicroUsdc};

use crate::config::{GateKind, OnrampConfig, ReleasePin, TurnstileConfig};
use crate::daemon::CapabilitySource;
use crate::gate::{Gate, html_escape};
use crate::net::ClientIpSource;
use crate::state::{self, AppState};

pub fn test_info() -> Info {
    Info {
        chain_id: 421_614,
        payment_pool: Address::repeat_byte(0x22),
        max_spending_cap: MicroUsdc(5_000_000),
        max_ttl_secs: 172_800,
    }
}

pub fn test_config() -> OnrampConfig {
    OnrampConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        public_url: "https://up.example.org".into(),
        daemon_url: "http://127.0.0.1:8090".into(),
        daemon_token: "t".repeat(32).as_str().into(),
        rpc_url: "https://rpc.example".into(),
        capacity_bond: Address::repeat_byte(0x33),
        slash_judge: None,
        min_cli_version: None,
        spending_cap: None,
        ttl_secs: None,
        gate: GateKind::Turnstile,
        brand_name: "deCDN".into(),
        gate_template: None,
        turnstile: Some(TurnstileConfig {
            secret: "secret".into(),
            sitekey: "TEST_SITEKEY".into(),
        }),
        client_ip: ClientIpSource::Peer,
        fund_rate_per_min: 0,
        poll_rate_per_min: 0,
        releases_base: "https://github.com/decdn".into(),
        decdn_release: ReleasePin {
            tag: "v0.1.0".into(),
            version: "0.1.0".into(),
            sums_sha256: "ab".repeat(32),
        },
        cli_release: ReleasePin {
            tag: "decdn-sponsored-v0.2.0".into(),
            version: "0.2.0".into(),
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
                        expiry: SystemClock.now_unix() + req.ttl_secs.unwrap_or(info.max_ttl_secs),
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

/// A gate that passes or refuses every proof, and records the last client
/// address it was shown.
pub struct FakeGate {
    pass: AtomicBool,
    last_ip: Mutex<Option<IpAddr>>,
}

impl FakeGate {
    #[must_use]
    pub fn new(pass: bool) -> Self {
        Self {
            pass: AtomicBool::new(pass),
            last_ip: Mutex::new(None),
        }
    }

    pub fn last_ip(&self) -> Option<IpAddr> {
        *self.last_ip.lock().unwrap()
    }
}

#[async_trait]
impl Gate for FakeGate {
    fn page(&self, client: Address) -> String {
        // Escaped like `TurnstileGate::page`, so rust/xss sees a barrier.
        format!("<p>fake gate for {}</p>", html_escape(&client.to_string()))
    }

    async fn verify(
        &self,
        _client: Address,
        _proof: &str,
        client_ip: Option<IpAddr>,
    ) -> anyhow::Result<bool> {
        *self.last_ip.lock().unwrap() = client_ip;
        Ok(self.pass.load(Ordering::SeqCst))
    }
}

pub struct FakeOptions {
    pub gate_passes: bool,
    pub source: SourceBehavior,
    pub config: OnrampConfig,
}

impl Default for FakeOptions {
    fn default() -> Self {
        Self {
            gate_passes: true,
            source: SourceBehavior::Issue,
            config: test_config(),
        }
    }
}

/// The fakes behind an [`AppState`], for tests to inspect.
pub struct Fakes {
    pub source: Arc<FakeCapabilitySource>,
    pub gate: Arc<FakeGate>,
}

/// An `AppState` over a fake daemon and gate, built the way production
/// builds it (`state::build`).
pub async fn app_state_with_options(opts: FakeOptions) -> (AppState, Fakes) {
    let source = Arc::new(FakeCapabilitySource::new(opts.source));
    let gate = Arc::new(FakeGate::new(opts.gate_passes));
    let state = state::build(
        &opts.config,
        source.clone() as Arc<dyn CapabilitySource>,
        gate.clone() as Arc<dyn Gate>,
    )
    .await
    .unwrap();
    (state, Fakes { source, gate })
}

pub async fn app_state_with_fakes() -> AppState {
    app_state_with_options(FakeOptions::default()).await.0
}
