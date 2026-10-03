#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;

use sponsord_api::MicroUsdc;
use sponsord_api::client::DaemonClient;
use sponsord_onramp::daemon::CapabilitySource;
use sponsord_onramp::gate::Gate;
use sponsord_onramp::state;
use sponsord_onramp::test_support::{
    FakeCapabilitySource, FakeGate, SourceBehavior, test_config, test_info,
};

fn gate() -> Arc<dyn Gate> {
    Arc::new(FakeGate::new(true))
}

fn fake_source() -> Arc<dyn CapabilitySource> {
    Arc::new(FakeCapabilitySource::new(SourceBehavior::Issue))
}

#[tokio::test]
async fn build_takes_the_chain_from_the_daemon() {
    let st = state::build(&test_config(), fake_source(), gate())
        .await
        .unwrap();
    assert_eq!(st.profile.chain_id, test_info().chain_id);
    assert_eq!(st.profile.payment_pool, test_info().payment_pool);
}

#[tokio::test]
async fn build_refuses_terms_above_the_daemon_maximum() {
    let mut cfg = test_config();
    cfg.spending_cap = Some(MicroUsdc(5_000_001));
    let err = state::build(&cfg, fake_source(), gate())
        .await
        .err()
        .unwrap();
    assert!(
        err.to_string().contains("ONRAMP_SPENDING_CAP_MICRO_USDC"),
        "{err}"
    );

    let mut cfg = test_config();
    cfg.ttl_secs = Some(0);
    let err = state::build(&cfg, fake_source(), gate())
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("ONRAMP_TTL_SECS"), "{err}");
}

#[tokio::test]
async fn build_refuses_an_unreachable_daemon_and_names_it() {
    let mut cfg = test_config();
    cfg.daemon_url = "http://127.0.0.1:1".into();
    let source: Arc<dyn CapabilitySource> = Arc::new(DaemonClient::new(
        &cfg.daemon_url,
        cfg.daemon_token.clone(),
        reqwest::Client::new(),
    ));
    let err = state::build(&cfg, source, gate()).await.err().unwrap();
    assert!(err.to_string().contains("http://127.0.0.1:1"), "{err}");
}
