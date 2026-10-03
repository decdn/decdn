#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

#[path = "../src/test_support.rs"]
mod test_support;

use std::sync::Arc;

use sponsord_onramp::captcha::CaptchaVerifier;
use sponsord_onramp::daemon::{CapabilitySource, DaemonClient};
use sponsord_onramp::state;
use test_support::{FakeCapabilitySource, FakeCaptcha, SourceBehavior, test_config, test_info};

fn captcha() -> Arc<dyn CaptchaVerifier> {
    Arc::new(FakeCaptcha::new(true))
}

fn fake_source() -> Arc<dyn CapabilitySource> {
    Arc::new(FakeCapabilitySource::new(SourceBehavior::Issue))
}

#[tokio::test]
async fn build_keeps_the_daemon_info() {
    let cfg = test_config(tempfile::tempdir().unwrap().keep());
    let st = state::build(cfg, fake_source(), captcha()).await.unwrap();
    assert_eq!(*st.chain, test_info());
}

#[tokio::test]
async fn build_refuses_terms_above_the_daemon_maximum() {
    let mut cfg = test_config(tempfile::tempdir().unwrap().keep());
    cfg.spending_cap = Some(5_000_001);
    let err = state::build(cfg, fake_source(), captcha())
        .await
        .err()
        .unwrap();
    assert!(
        err.to_string().contains("ONRAMP_SPENDING_CAP_MICRO_USDC"),
        "{err}"
    );

    let mut cfg = test_config(tempfile::tempdir().unwrap().keep());
    cfg.ttl_secs = Some(0);
    let err = state::build(cfg, fake_source(), captcha())
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("ONRAMP_TTL_SECS"), "{err}");
}

#[tokio::test]
async fn build_refuses_an_unreachable_daemon_and_names_it() {
    let mut cfg = test_config(tempfile::tempdir().unwrap().keep());
    cfg.daemon_url = "http://127.0.0.1:1".into();
    let source: Arc<dyn CapabilitySource> = Arc::new(DaemonClient::new(
        &cfg.daemon_url,
        cfg.daemon_token.clone(),
        reqwest::Client::new(),
    ));
    let err = state::build(cfg, source, captcha()).await.err().unwrap();
    assert!(err.to_string().contains("http://127.0.0.1:1"), "{err}");
}
