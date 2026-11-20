use std::future::Future;
use std::pin::Pin;

use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{Origin, OriginFetch, OriginKind, OriginPullError};
use decdn_protocol::MIN_ATTESTATION_TTL_SEC;

use super::*;
use crate::attestations::{REFRESH_RETRY_US, SelfOrigin};
use crate::dht::attestation::test_support::{ORIGIN, attest_at, verifier};

const HOUR_US: u64 = 3_600_000_000;

/// An age past three quarters of the default 12 h lifetime, so the refresh is
/// due whatever its jitter, while the attestation is still live.
const AGED_US: u64 = 10 * HOUR_US;

/// An origin backend that holds exactly `hash`.
#[derive(Debug)]
struct Holds(Hash);

impl Origin for Holds {
    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(OriginFetch::NotFound) })
    }

    fn size(
        &self,
        hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, OriginPullError>> + Send + '_>> {
        let held = hash == self.0;
        Box::pin(async move { Ok(held.then_some(1_024)) })
    }

    fn kind(&self) -> OriginKind {
        OriginKind::Filesystem
    }
}

async fn refresher(
    origin: Option<SelfOrigin>,
    backend: Vec<Arc<dyn Origin>>,
) -> (Refresher, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cache = decdn_cache::CacheEngine::open(dir.path(), backend, 1)
        .await
        .unwrap();
    (
        Refresher {
            attestations: Arc::new(Attestations::in_memory(verifier(), origin)),
            cache,
            metrics: Arc::new(Metrics::new()),
            pull: None,
        },
        dir,
    )
}

fn seated() -> SelfOrigin {
    SelfOrigin {
        signer: Arc::new(ORIGIN.clone()),
        ttl_sec: decdn_protocol::DEFAULT_ATTESTATION_TTL_SEC,
    }
}

#[tokio::test]
async fn a_seated_origin_re_signs_its_own_content() {
    let hash = [5; 32];
    let backend: Arc<dyn Origin> = Arc::new(Holds(Hash::from_bytes(hash)));
    let (r, _dir) = refresher(Some(seated()), vec![backend]).await;
    let aged = attest_at(hash, now_us() - AGED_US);
    assert!(r.attestations.keep(hash, aged.clone()));
    r.refresh_due().await;
    let held = r.attestations.held(&hash).unwrap();
    assert!(held.timestamp_us > aged.timestamp_us, "re-signed");
    assert!(r.attestations.refresh_due(now_us()).is_empty());
}

#[tokio::test]
async fn a_seated_origin_does_not_attest_what_its_backend_lacks() {
    let hash = [5; 32];
    let backend: Arc<dyn Origin> = Arc::new(Holds(Hash::from_bytes([6; 32])));
    let (r, _dir) = refresher(Some(seated()), vec![backend]).await;
    let aged = attest_at(hash, now_us() - AGED_US);
    assert!(r.attestations.keep(hash, aged.clone()));
    r.refresh_due().await;
    assert_eq!(r.attestations.held(&hash), Some(aged), "not re-signed");
}

#[tokio::test]
async fn a_refresh_with_no_source_retries_later() {
    let hash = [5; 32];
    let (r, _dir) = refresher(None, Vec::new()).await;
    let aged = attest_at(hash, now_us() - AGED_US);
    assert!(r.attestations.keep(hash, aged.clone()));
    r.refresh_due().await;
    assert_eq!(r.attestations.held(&hash), Some(aged));
    assert!(r.attestations.refresh_due(now_us()).is_empty(), "deferred");
    assert_eq!(
        r.attestations
            .refresh_due(now_us() + REFRESH_RETRY_US)
            .len(),
        1
    );
}

#[tokio::test]
async fn an_expired_attestation_with_no_fresher_one_is_dropped() {
    let hash = [5; 32];
    let (r, _dir) = refresher(Some(seated()), Vec::new()).await;
    // Kept while live, then left to expire.
    let short = crate::dht::attestation::sign_attestation(
        &*ORIGIN,
        &crate::dht::attestation::test_support::domain(),
        hash,
        crate::dht::attestation::test_support::NAMESPACE,
        now_us() - HOUR_US + 2_000_000,
        MIN_ATTESTATION_TTL_SEC,
    )
    .unwrap();
    assert!(r.attestations.keep(hash, short));
    tokio::time::sleep(Duration::from_secs(3)).await;
    r.refresh_due().await;
    assert_eq!(r.attestations.held(&hash), None);
    assert_eq!(r.attestations.live(&hash), None);
}

#[tokio::test]
async fn an_unseated_node_never_self_signs_on_refresh() {
    let hash = [5; 32];
    let backend: Arc<dyn Origin> = Arc::new(Holds(Hash::from_bytes(hash)));
    let stranger = SelfOrigin {
        signer: Arc::new(PrivateKeySigner::random()),
        ttl_sec: MIN_ATTESTATION_TTL_SEC,
    };
    let (r, _dir) = refresher(Some(stranger), vec![backend]).await;
    let aged = attest_at(hash, now_us() - AGED_US);
    assert!(r.attestations.keep(hash, aged.clone()));
    r.refresh_due().await;
    assert_eq!(r.attestations.held(&hash), Some(aged));
}
