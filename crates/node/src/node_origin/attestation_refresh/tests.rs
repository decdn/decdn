use std::future::Future;
use std::pin::Pin;

use alloy::signers::local::PrivateKeySigner;
use bytes::Bytes;
use decdn_cache::{Origin, OriginKind, OriginPullError};
use decdn_protocol::MIN_ATTESTATION_TTL_SEC;

use super::*;
use crate::attestations::{REFRESH_RETRY_US, SelfOrigin};
use crate::dht::attestation::test_support::{ORIGIN, attest_at, verifier};

const HOUR_US: u64 = 3_600_000_000;

/// An age past three quarters of the default 12 h lifetime, so the refresh is
/// due whatever its jitter, while the attestation is still live.
const AGED_US: u64 = 10 * HOUR_US;

/// The one blob a [`Holds`] backend holds.
const BLOB: &[u8] = &[7; 1_024];

/// The content address of [`BLOB`].
fn blob_hash() -> [u8; 32] {
    *Hash::new(BLOB).as_bytes()
}

/// An origin backend that holds exactly its payload: it enumerates its hash and
/// serves its verifying `{H}.sizeproof`.
#[derive(Debug)]
struct Holds(&'static [u8]);

impl Origin for Holds {
    fn fetch_size_proof(
        &self,
        hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, OriginPullError>> + Send + '_>> {
        let object = (hash == Hash::new(self.0)).then(|| {
            let size = u64::try_from(self.0.len()).unwrap();
            Bytes::from(
                decdn_bao_range::encode_outboard(self.0, size)
                    .unwrap()
                    .size_proof_object(),
            )
        });
        Box::pin(async move { Ok(object) })
    }

    fn enumerate(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Hash>, OriginPullError>> + Send + '_>> {
        let held = Hash::new(self.0);
        Box::pin(async move { Ok(vec![held]) })
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
    let hash = blob_hash();
    let backend: Arc<dyn Origin> = Arc::new(Holds(BLOB));
    let (r, _dir) = refresher(Some(seated()), vec![backend]).await;
    r.cache.rescan_origins().await;
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
    let backend: Arc<dyn Origin> = Arc::new(Holds(BLOB));
    let (r, _dir) = refresher(Some(seated()), vec![backend]).await;
    r.cache.rescan_origins().await;
    let aged = attest_at(hash, now_us() - AGED_US);
    assert!(r.attestations.keep(hash, aged.clone()));
    r.refresh_due().await;
    assert_eq!(r.attestations.held(&hash), None, "dropped, never re-signed");
}

#[tokio::test]
async fn a_refresh_with_no_source_retries_later() {
    let hash = blob_hash();
    let backend: Arc<dyn Origin> = Arc::new(Holds(BLOB));
    let (r, _dir) = refresher(None, vec![backend]).await;
    r.cache.rescan_origins().await;
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

/// The value of `decdn_attestation_expired_total`.
fn expired_count(metrics: &Metrics) -> usize {
    let text = metrics.encode().unwrap();
    text.lines()
        .filter_map(|l| l.strip_prefix("decdn_attestation_expired_total "))
        .map(|v| v.parse().unwrap())
        .next()
        .unwrap()
}

/// A minimum-lifetime attestation for `hash` that expires two seconds from
/// now.
fn expiring_soon(hash: [u8; 32]) -> decdn_protocol::OriginAttestation {
    crate::dht::attestation::sign_attestation(
        &*ORIGIN,
        &crate::dht::attestation::test_support::domain(),
        hash,
        crate::dht::attestation::test_support::NAMESPACE,
        now_us() - HOUR_US + 2_000_000,
        MIN_ATTESTATION_TTL_SEC,
    )
    .unwrap()
}

/// An attestation for a blob the node does not hold is dropped when its
/// refresh comes due, live or expired, rather than refreshed: a pull that
/// kept it never landed, or the blob was evicted.
#[tokio::test]
async fn an_attestation_of_a_blob_the_node_lacks_is_dropped_not_refreshed() {
    let (r, _dir) = refresher(Some(seated()), Vec::new()).await;
    let live = [5; 32];
    let expired = [6; 32];
    assert!(
        r.attestations
            .keep(live, attest_at(live, now_us() - AGED_US))
    );
    assert!(r.attestations.keep(expired, expiring_soon(expired)));
    tokio::time::sleep(Duration::from_secs(3)).await;
    r.refresh_due().await;
    assert_eq!(r.attestations.held(&live), None);
    assert_eq!(r.attestations.held(&expired), None);
    assert_eq!(expired_count(&r.metrics), 0, "no refresh ran");
}

/// A wanted hash the node no longer holds stops being wanted. One it holds
/// stays wanted while no DHT is wired to look it up.
#[tokio::test]
async fn a_wanted_hash_is_dropped_once_the_blob_is_gone() {
    let held = blob_hash();
    let backend: Arc<dyn Origin> = Arc::new(Holds(BLOB));
    let (r, _dir) = refresher(None, vec![backend]).await;
    r.cache.rescan_origins().await;
    r.attestations.want(held);
    r.attestations.want([6; 32]);
    r.refresh_due().await;
    let retry = now_us() + REFRESH_RETRY_US;
    assert_eq!(r.attestations.wanted_due(retry, 8), vec![held]);
}

/// The node keeps the expired attestation of a blob it still holds, serves
/// nothing under it, and keeps retrying, so the hash serves again once an
/// origin returns. The lapse counts once.
#[tokio::test]
async fn an_expired_attestation_of_a_held_blob_stays_due_for_refresh() {
    let hash = blob_hash();
    let backend: Arc<dyn Origin> = Arc::new(Holds(BLOB));
    let (r, _dir) = refresher(None, vec![backend]).await;
    r.cache.rescan_origins().await;
    assert!(r.cache.origin_held_size(Hash::from_bytes(hash)).is_some());
    let short = expiring_soon(hash);
    assert!(r.attestations.keep(hash, short.clone()));
    tokio::time::sleep(Duration::from_secs(3)).await;
    r.refresh_due().await;
    assert_eq!(r.attestations.held(&hash), Some(short));
    assert_eq!(r.attestations.live(&hash), None);
    let retry = now_us() + REFRESH_RETRY_US;
    assert_eq!(r.attestations.refresh_due(retry).len(), 1, "retried later");
    assert_eq!(
        r.attestations.refresh_failed(hash, retry),
        RefreshFailed::Lapsed { first: false }
    );
    assert_eq!(expired_count(&r.metrics), 1);
}

#[tokio::test]
async fn an_unseated_node_never_self_signs_on_refresh() {
    let hash = blob_hash();
    let backend: Arc<dyn Origin> = Arc::new(Holds(BLOB));
    let stranger = SelfOrigin {
        signer: Arc::new(PrivateKeySigner::random()),
        ttl_sec: MIN_ATTESTATION_TTL_SEC,
    };
    let (r, _dir) = refresher(Some(stranger), vec![backend]).await;
    r.cache.rescan_origins().await;
    let aged = attest_at(hash, now_us() - AGED_US);
    assert!(r.attestations.keep(hash, aged.clone()));
    r.refresh_due().await;
    assert_eq!(r.attestations.held(&hash), Some(aged));
}
