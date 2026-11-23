use super::*;
use crate::commands::fetch::{MALFORMED_MANIFEST, MAX_MANIFEST_MB};
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::flights::PRUNE_FLOOR;
use http_body_util::Full;

fn manifest() -> Arc<BundleManifest> {
    Arc::new(BundleManifest::new([0; 32], Vec::new()))
}

fn hash(n: u8) -> [u8; 32] {
    [n; 32]
}

#[test]
fn manifest_cache_evicts_the_least_recently_used_past_the_count() {
    let mut cache = ManifestCache::default();
    for n in 0..u8::try_from(MAX_CACHED_MANIFESTS).unwrap() {
        cache.insert(hash(n), manifest(), 1);
    }
    // Reading the oldest makes it the newest, so the next insert evicts 1.
    assert!(cache.get(&hash(0)).is_some());
    cache.insert(hash(200), manifest(), 1);
    assert_eq!(cache.entries.len(), MAX_CACHED_MANIFESTS);
    assert!(cache.get(&hash(0)).is_some());
    assert!(cache.get(&hash(1)).is_none());
    assert!(cache.get(&hash(200)).is_some());
}

#[test]
fn manifest_cache_evicts_past_the_byte_budget() {
    let mut cache = ManifestCache::default();
    cache.insert(hash(1), manifest(), MANIFEST_CACHE_BYTES / 2);
    cache.insert(hash(2), manifest(), MANIFEST_CACHE_BYTES / 2);
    assert_eq!(cache.bytes, MANIFEST_CACHE_BYTES);
    cache.insert(hash(3), manifest(), 1);
    assert!(cache.get(&hash(1)).is_none());
    assert!(cache.get(&hash(2)).is_some());
    assert!(cache.get(&hash(3)).is_some());
    assert_eq!(cache.bytes, MANIFEST_CACHE_BYTES / 2 + 1);
}

#[test]
fn manifest_cache_keeps_the_newest_entry_even_over_budget() {
    let mut cache = ManifestCache::default();
    cache.insert(hash(1), manifest(), 1);
    cache.insert(hash(2), manifest(), MANIFEST_CACHE_BYTES + 1);
    assert_eq!(cache.entries.len(), 1);
    assert!(cache.get(&hash(2)).is_some());
}

#[test]
fn manifest_cache_reinsert_replaces_without_double_counting() {
    let mut cache = ManifestCache::default();
    cache.insert(hash(1), manifest(), 10);
    cache.insert(hash(1), manifest(), 30);
    assert_eq!(cache.entries.len(), 1);
    assert_eq!(cache.bytes, 30);
}

#[test]
fn not_manifests_evict_the_least_recently_used_past_the_count() {
    let verdict = || NotManifest::Plain("no".into());
    let mut known = NotManifests::default();
    for n in 0..MAX_NOT_MANIFESTS {
        let mut h = [0u8; 32];
        h[..8].copy_from_slice(&n.to_le_bytes());
        known.insert(h, verdict());
    }
    // Reading the oldest makes it the newest, so the next insert evicts 1.
    assert!(known.get(&[0; 32]).is_some());
    known.insert(hash(0xff), verdict());
    assert_eq!(known.entries.len(), MAX_NOT_MANIFESTS);
    assert!(known.get(&[0; 32]).is_some());
    let mut one = [0u8; 32];
    one[0] = 1;
    assert!(known.get(&one).is_none());
    // A repeat insert replaces, not duplicates.
    known.insert(hash(0xff), verdict());
    assert_eq!(known.entries.len(), MAX_NOT_MANIFESTS);
}

#[test]
fn verdict_text_is_clipped_on_a_character_boundary() {
    assert_eq!(clip("short".to_owned()), "short");
    let exact = "x".repeat(MAX_VERDICT_TEXT);
    assert_eq!(clip(exact.clone()), exact);
    // A multi-byte character straddles the cut.
    let long = format!("{}é{}", "x".repeat(MAX_VERDICT_TEXT - 4), "y".repeat(100));
    let clipped = clip(long);
    assert!(clipped.len() <= MAX_VERDICT_TEXT, "{}", clipped.len());
    assert!(clipped.ends_with('…'));
}

/// A capability from pool `pool` (every byte `pool`) to `signer`, with a cap
/// of one and `expiry`.
fn grant(signer: Address, pool: u8, expiry: u64) -> CapabilityGrant {
    CapabilityGrant {
        pool_id: alloy::primitives::B256::repeat_byte(pool),
        signer,
        spending_cap: 1,
        expiry,
        owner_signature: vec![0; 65],
    }
}

/// Register a capability from pool `pool` to the gateway's pending key, as a
/// `PUT /_capability` the chain accepts does, and return the key.
fn hold_capability(gateway: &Gateway, pool: u8) -> Address {
    let mut delegates = gateway.delegates();
    let signer = delegates.pending();
    delegates
        .register(grant(signer, pool, u64::MAX), TEST_POOL, None)
        .unwrap();
    signer
}

/// The one `PaymentPool` contract a test gateway is configured with.
const TEST_POOL: Deployment = Deployment {
    chain_id: 421_614,
    payment_pool: Address::repeat_byte(1),
};

/// A pull backend for gateways whose requests all stop before a pull.
struct NoPulls;

impl worker::Backend for NoPulls {
    type Host = ();
    type Payer = ();

    async fn open(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn payer(&self, _credential: Credential) -> anyhow::Result<()> {
        Ok(())
    }

    async fn pull(
        &self,
        (): &(),
        (): &(),
        _blob: &BlobFetch,
        _sink: &mut super::super::fetch::Sink,
    ) -> anyhow::Result<()> {
        anyhow::bail!("these tests start no pull")
    }

    async fn close(&self, (): ()) {}
}

async fn test_gateway() -> Arc<Gateway> {
    test_gateway_with(NoPulls, flags(&[])).await
}

async fn test_gateway_with(
    backend: impl worker::Backend,
    common: cli::ClientFetchArgs,
) -> Arc<Gateway> {
    test_gateway_slots(backend, common, 1).await
}

/// [`test_gateway_with`] whose worker runs `worker_slots` pulls at once, for
/// tests where a held pull must not take the only slot. A held pull keeps its
/// worker slot even after its request is dropped.
async fn test_gateway_slots(
    backend: impl worker::Backend,
    common: cli::ClientFetchArgs,
    worker_slots: usize,
) -> Arc<Gateway> {
    let sites = SiteDomain::Loopback {
        listen: "127.0.0.1:8484".parse().unwrap(),
    };
    test_gateway_in(backend, common, sites, None, worker_slots).await
}

/// The registry address a test's public gateway verifies pointers against.
const REGISTRY: Address = Address::repeat_byte(3);

/// What a test's public gateway holds: no pointers yet, in `pointers_dir`.
fn public_with(pointers_dir: &Path) -> Public {
    Public {
        pointers: Pointers::load(pointers_dir, 421_614, REGISTRY).unwrap(),
        registry: REGISTRY,
        lookups: std::sync::Mutex::new(Budgets::default()),
        pointer_writes: Mutex::new(()),
    }
}

/// A gateway on `backend` whose sites live under `sites`: a loopback gateway
/// with no `public`, or a public one with its `public`.
async fn test_gateway_in(
    backend: impl worker::Backend,
    common: cli::ClientFetchArgs,
    sites: SiteDomain,
    public: Option<Public>,
    worker_slots: usize,
) -> Arc<Gateway> {
    test_gateway_cached(backend, common, sites, public, worker_slots, None).await
}

/// [`test_gateway_in`] serving repeat requests out of `cache`.
async fn test_gateway_cached(
    backend: impl worker::Backend,
    common: cli::ClientFetchArgs,
    sites: SiteDomain,
    public: Option<Public>,
    worker_slots: usize,
    cache: Option<CacheEngine>,
) -> Arc<Gateway> {
    assert_eq!(matches!(sites, SiteDomain::Public { .. }), public.is_some());
    // Nothing here reaches the chain: every request below stops first.
    let chain = ChainConfig::new(
        decdn_client::session::GovernanceChain {
            rpc_url: "http://127.0.0.1:1".to_owned(),
            chain_id: 421_614,
            capacity_bond: Address::repeat_byte(2),
        },
        vec![decdn_client::session::PaymentPoolChain {
            rpc_url: "http://127.0.0.1:1".to_owned(),
            deployment: TEST_POOL,
        }],
    )
    .unwrap();
    Arc::new(Gateway {
        common,
        chain,
        delegates: std::sync::Mutex::default(),
        pulls: PullWorker::start(
            backend,
            worker_slots,
            Duration::from_secs(GIVE_UP_AFTER_SECS),
        )
        .await
        .unwrap(),
        cache,
        manifests: std::sync::Mutex::new(ManifestCache::default()),
        not_manifests: std::sync::Mutex::new(NotManifests::default()),
        manifest_fills: decdn_cache::Flights::default(),
        manifest_buffer: Semaphore::new(MANIFEST_BUFFER_BYTES),
        hosts: allowed_hosts("127.0.0.1:8484".parse().unwrap()),
        sites,
        metrics: Metrics::default(),
        active: std::sync::Mutex::new(HashMap::new()),
        next_pull: AtomicU64::new(0),
        started: Instant::now(),
        started_unix: now_secs(),
        listen: "127.0.0.1:8484".parse().unwrap(),
        public,
    })
}

async fn call(
    gateway: &Arc<Gateway>,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> StatusCode {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, "127.0.0.1:8484");
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    let req = req.body(Full::new(Bytes::from(body.to_owned()))).unwrap();
    Arc::clone(gateway).handle(req).await.status()
}

#[tokio::test]
async fn head_never_starts_a_pull() {
    let gateway = test_gateway().await;
    let status = call(
        &gateway,
        Method::HEAD,
        &format!("/{}", "ab".repeat(32)),
        &[],
        "",
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

/// `method path` with `headers` and `body`: the response's status, headers
/// and body as text.
async fn exchange(
    gateway: &Arc<Gateway>,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (StatusCode, HeaderMap, String) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, "127.0.0.1:8484");
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    let req = req.body(Full::new(Bytes::from(body.to_owned()))).unwrap();
    let response = Arc::clone(gateway).handle(req).await;
    let (parts, body) = response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    (
        parts.status,
        parts.headers,
        String::from_utf8(body.to_vec()).unwrap(),
    )
}

/// The address `GET /_delegate` returns, asked with no browser headers.
async fn pending_address(gateway: &Arc<Gateway>) -> Address {
    let (status, _, body) = exchange(gateway, Method::GET, "/_delegate", &[], "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    body["address"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn the_pending_key_answers_anyone_until_a_capability_claims_it() {
    let gateway = test_gateway().await;
    let pending = pending_address(&gateway).await;
    assert_eq!(pending_address(&gateway).await, pending, "one pending key");
    gateway
        .delegates()
        .register(grant(pending, 1, u64::MAX), TEST_POOL, None)
        .unwrap();
    let next = pending_address(&gateway).await;
    assert_ne!(next, pending, "a claimed key is no longer pending");
    assert_eq!(pending_address(&gateway).await, next);
}

/// `PUT /_capability` with `grant` for any request: the status and the body.
async fn put_capability(gateway: &Arc<Gateway>, grant: &CapabilityGrant) -> (StatusCode, String) {
    let body = serde_json::json!({ "capability": grant.to_token(), "scope": "any" }).to_string();
    let (status, _, body) = exchange(gateway, Method::PUT, "/_capability", &[], &body).await;
    (status, body)
}

#[tokio::test]
async fn a_capability_that_cannot_register_is_refused_before_the_chain_is_read() {
    // The gateway's RPC is unreachable, so every answer here comes before a
    // chain read.
    let gateway = test_gateway().await;
    let far = now_secs() + 86_400;
    let pending = pending_address(&gateway).await;

    let token = grant(pending, 1, far).to_token();
    for (body, why) in [
        (
            serde_json::json!({ "capability": token }),
            "missing field `scope`",
        ),
        (
            serde_json::json!({ "capability": token, "scope": "*" }),
            "a scope is \"any\"",
        ),
        (
            serde_json::json!({ "capability": token, "scope": "all" }),
            "a scope is \"any\"",
        ),
    ] {
        let (status, _, answer) = exchange(
            &gateway,
            Method::PUT,
            "/_capability",
            &[],
            &body.to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}: {answer}");
        assert!(answer.contains(why), "{body}: {answer}");
    }

    let mut zero = grant(pending, 1, far);
    zero.spending_cap = 0;
    let (status, body) = put_capability(&gateway, &zero).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains("cap is zero"), "{body}");

    let expiring = grant(pending, 1, now_secs() + EXPIRY_MARGIN_SECS);
    let (status, body) = put_capability(&gateway, &expiring).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains("inside the margin"), "{body}");

    let stranger = alloy::primitives::Address::repeat_byte(9);
    let (status, body) = put_capability(&gateway, &grant(stranger, 1, far)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    let held = hold_capability(&gateway, 1);
    let (status, body) = put_capability(&gateway, &grant(held, 1, far)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body.contains("from this pool"), "{body}");
    // Another pool passes the key checks and stops at the owner signature,
    // which this grant does not carry.
    let (status, body) = put_capability(&gateway, &grant(held, 2, far)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

#[tokio::test]
async fn a_loopback_gateway_answers_nothing_cross_origin() {
    let gateway = test_gateway().await;
    let page = [(header::ORIGIN.as_str(), "https://decdn.org")];
    for path in ["/_delegate", "/_capability", "/_status"] {
        let (status, _, _) = exchange(&gateway, Method::OPTIONS, path, &page, "").await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
    }
    let (status, headers, body) = exchange(&gateway, Method::GET, "/_delegate", &page, "").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
}

#[tokio::test]
async fn a_client_outside_a_browser_lists_the_capabilities_and_the_status() {
    let gateway = test_gateway().await;
    let first = hold_capability(&gateway, 1);
    let second = hold_capability(&gateway, 2);
    let (status, _, body) = exchange(&gateway, Method::GET, "/_capability", &[], "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let listed: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
    let signers: Vec<_> = listed
        .iter()
        .map(|c| c["signer"].as_str().unwrap())
        .collect();
    assert_eq!(
        signers,
        [first.to_string(), second.to_string()],
        "registration order"
    );
    let (status, _, body) = exchange(&gateway, Method::GET, "/_status", &[], "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// What [`Recording`] answers a pull of one blob with.
#[derive(Clone)]
enum Reply {
    /// The pull ends at its cap before any byte, as a blob that the store
    /// already shows to be past the cap does.
    TooLarge,
    /// The pull stops for funding before any byte.
    Funding,
    /// The pull writes nothing and never ends.
    Hold,
    /// The pull writes these bytes, then never ends.
    BytesThenHold(Vec<u8>),
    /// The pull waits for this notice, then fails as a blob no node holds.
    FailOn(Arc<tokio::sync::Notify>),
    /// The size proof never ends, nor does the pull.
    HoldProof,
    /// The pull writes these bytes, then ends at its cap, as a blob larger
    /// than the cap does once the cap's worth has landed.
    BytesThenTooLarge(Vec<u8>),
    /// The pull writes these bytes and succeeds.
    Bytes(Vec<u8>),
}

/// A shared, appended log of what the pulls carried.
type Log<T> = Arc<std::sync::Mutex<Vec<T>>>;

/// A pull backend that answers each blob by its hash and records the
/// `--max-blob-mb` each pull runs with, the size cap it carries and its
/// byte window. The sizes in `sizes` are the ones earlier pulls verified. A
/// size proof is logged in `proofs` apart from the pulls. A blob with no
/// reply fails its proof with "no holder", and a `Funding` reply with a
/// funding stop, as its pull would; otherwise the proof records the size
/// `proves` holds for it, if any, as verified.
struct Recording {
    caps: Log<u64>,
    size_caps: Log<Option<u64>>,
    ranges: Log<Option<(u64, u64)>>,
    proofs: Log<[u8; 32]>,
    replies: HashMap<[u8; 32], Reply>,
    sizes: std::sync::Mutex<HashMap<[u8; 32], u64>>,
    proves: HashMap<[u8; 32], u64>,
}

impl worker::Backend for Recording {
    type Host = ();
    type Payer = ();

    async fn open(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn payer(&self, _credential: Credential) -> anyhow::Result<()> {
        Ok(())
    }

    fn size(&self, (): &(), hash: [u8; 32]) -> Option<u64> {
        self.sizes.lock().unwrap().get(&hash).copied()
    }

    async fn pull(
        &self,
        (): &(),
        (): &(),
        blob: &BlobFetch,
        sink: &mut super::super::fetch::Sink,
    ) -> anyhow::Result<()> {
        if blob.size_only {
            self.proofs.lock().unwrap().push(blob.hash);
            match self.replies.get(&blob.hash) {
                None => anyhow::bail!("no node holds the blob"),
                Some(Reply::Funding) => {
                    return Err(anyhow::Error::new(
                        decdn_client::FundingNeeded::PublisherPool {
                            pool: alloy::primitives::B256::ZERO,
                        },
                    ));
                }
                Some(Reply::HoldProof) => std::future::pending().await,
                Some(_) => {}
            }
            if let Some(&size) = self.proves.get(&blob.hash) {
                self.sizes.lock().unwrap().insert(blob.hash, size);
            }
            return Ok(());
        }
        self.caps.lock().unwrap().push(blob.common.max_blob_mb);
        self.size_caps.lock().unwrap().push(blob.size_cap);
        self.ranges.lock().unwrap().push(blob.range);
        let too_large = || {
            anyhow::Error::new(decdn_client::BlobTooLarge {
                reached: (blob.common.max_blob_mb << 20) + 1,
                ceiling: blob.common.max_blob_mb << 20,
            })
        };
        let super::super::fetch::Sink::Writer(writer) = sink else {
            anyhow::bail!("the gateway pulls into a writer");
        };
        match self.replies.get(&blob.hash) {
            None => anyhow::bail!("no node holds the blob"),
            Some(Reply::TooLarge) => Err(too_large()),
            Some(Reply::Funding) => Err(anyhow::Error::new(
                decdn_client::FundingNeeded::PublisherPool {
                    pool: alloy::primitives::B256::ZERO,
                },
            )),
            Some(Reply::Hold | Reply::HoldProof) => std::future::pending().await,
            Some(Reply::BytesThenHold(bytes)) => {
                tokio::io::AsyncWriteExt::write_all(writer, bytes).await?;
                std::future::pending().await
            }
            Some(Reply::FailOn(notice)) => {
                notice.notified().await;
                anyhow::bail!("no node holds the blob")
            }
            Some(Reply::BytesThenTooLarge(bytes)) => {
                tokio::io::AsyncWriteExt::write_all(writer, bytes).await?;
                Err(too_large())
            }
            Some(Reply::Bytes(bytes)) => {
                // A pull of a window writes the window, as the client does.
                let window = match blob.range {
                    Some((offset, len)) => {
                        let start = usize::try_from(offset)?.min(bytes.len());
                        let end = usize::try_from(offset + len)?.min(bytes.len());
                        &bytes[start..end]
                    }
                    None => bytes.as_slice(),
                };
                tokio::io::AsyncWriteExt::write_all(writer, window).await?;
                Ok(())
            }
        }
    }

    async fn close(&self, (): ()) {}
}

/// The blob the single-reply gateways serve.
const BLOB: [u8; 32] = [0xab; 32];

/// A gateway on [`Recording`] that answers [`BLOB`] with `reply`, holding a
/// capability that pays for every host, and the caps its pulls record.
async fn recording_gateway(
    reply: Reply,
    args: &[&str],
) -> (Arc<Gateway>, Arc<std::sync::Mutex<Vec<u64>>>) {
    recording_gateway_of(HashMap::from([(BLOB, reply)]), args).await
}

/// A gateway on [`Recording`] that answers each blob in `replies`.
async fn recording_gateway_of(
    replies: HashMap<[u8; 32], Reply>,
    args: &[&str],
) -> (Arc<Gateway>, Arc<std::sync::Mutex<Vec<u64>>>) {
    recording_gateway_slots(replies, args, 1).await
}

/// [`recording_gateway_of`] whose worker runs `worker_slots` pulls at once.
async fn recording_gateway_slots(
    replies: HashMap<[u8; 32], Reply>,
    args: &[&str],
    worker_slots: usize,
) -> (Arc<Gateway>, Arc<std::sync::Mutex<Vec<u64>>>) {
    let (gateway, caps, _) =
        recording_gateway_proving(replies, HashMap::new(), args, worker_slots).await;
    (gateway, caps)
}

/// [`recording_gateway_of`] whose size proofs prove the sizes in `proves`,
/// and the blobs its proofs ran for.
async fn recording_gateway_proving(
    replies: HashMap<[u8; 32], Reply>,
    proves: HashMap<[u8; 32], u64>,
    args: &[&str],
    worker_slots: usize,
) -> (Arc<Gateway>, Log<u64>, Log<[u8; 32]>) {
    let (caps, proofs) = (Arc::default(), Log::default());
    let backend = Recording {
        caps: Arc::clone(&caps),
        size_caps: Arc::default(),
        ranges: Arc::default(),
        proofs: Arc::clone(&proofs),
        replies,
        sizes: std::sync::Mutex::default(),
        proves,
    };
    let gateway = test_gateway_slots(backend, flags(args), worker_slots).await;
    hold_capability(&gateway, 0);
    (gateway, caps, proofs)
}

/// A paid `GET path`: the status and the body as text.
async fn get(gateway: &Arc<Gateway>, path: &str) -> (StatusCode, String) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(path)
        .header(header::HOST, "127.0.0.1:8484")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = Arc::clone(gateway).handle(req).await;
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn a_manifest_pull_is_capped_and_an_oversize_blob_is_not_found() {
    let blob = alloy::hex::encode(BLOB);
    let (gateway, caps) = recording_gateway(Reply::TooLarge, &[]).await;
    let (status, body) = get(&gateway, &format!("/{blob}/?ns=7")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body.contains(&format!("fetch `/{blob}?ns=7` for the raw blob")),
        "the gateway serves the raw blob past the manifest cap: {body}"
    );
    // The verdict is remembered: the next request pulls nothing, and its
    // hint names its own namespace.
    let (status, body) = get(&gateway, &format!("/{blob}/a.bin?ns=9")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body.contains(&format!("fetch `/{blob}?ns=9` for the raw blob")),
        "{body}"
    );
    assert_eq!(*caps.lock().unwrap(), [MAX_MANIFEST_MB]);

    let (gateway, caps) = recording_gateway(Reply::TooLarge, &["--max-blob-mb", "10"]).await;
    let (status, body) = get(&gateway, &format!("/{blob}/")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        !body.contains("raw blob"),
        "the raw blob takes the same cap, so no hint names it: {body}"
    );
    assert_eq!(*caps.lock().unwrap(), [10], "a lower gateway cap stays");
}

/// The client ends a pull at its cap only after the cap's worth of bytes
/// has landed, so the stop arrives after the manifest bytes start.
#[tokio::test]
async fn a_cap_stop_after_the_first_bytes_is_not_found() {
    let reply = Reply::BytesThenTooLarge(b"{\"version\": 1, ".to_vec());
    let (gateway, caps) = recording_gateway(reply, &["--max-blob-mb", "10"]).await;
    let path = format!("/{}/", alloy::hex::encode(BLOB));
    let (status, body) = get(&gateway, &path).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (status, again) = get(&gateway, &path).await;
    assert_eq!(
        (status, again.as_str()),
        (StatusCode::NOT_FOUND, body.as_str())
    );
    assert_eq!(*caps.lock().unwrap(), [10], "the cap stop is remembered");
}

#[tokio::test]
async fn a_raw_blob_pull_keeps_the_gateway_cap() {
    let (gateway, caps) = recording_gateway(Reply::Bytes(b"raw".to_vec()), &[]).await;
    let (status, _) = get(&gateway, &format!("/{}", alloy::hex::encode(BLOB))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(*caps.lock().unwrap(), [flags(&[]).max_blob_mb]);
    assert_ne!(flags(&[]).max_blob_mb, MAX_MANIFEST_MB);
}

#[tokio::test]
async fn a_blob_that_is_not_a_manifest_is_not_found() {
    let (gateway, caps) = recording_gateway(Reply::Bytes(b"not json".to_vec()), &[]).await;
    let blob = alloy::hex::encode(BLOB);
    let (status, first) = get(&gateway, &format!("/{blob}/")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        first.contains(&format!("fetch `/{blob}` for the raw blob")),
        "{first}"
    );
    // The repeat request pays for no second pull, and its hint names its
    // own namespace.
    let (status, again) = get(&gateway, &format!("/{blob}/?ns=9")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        again,
        first.replace(&format!("`/{blob}`"), &format!("`/{blob}?ns=9`"))
    );
    assert_eq!(*caps.lock().unwrap(), [MAX_MANIFEST_MB]);
}

/// A pull that failed upstream says nothing of the blob, so the next request
/// pulls again.
#[tokio::test]
async fn a_failed_manifest_pull_is_not_remembered() {
    let (gateway, caps) = recording_gateway_of(HashMap::new(), &[]).await;
    let blob = alloy::hex::encode(BLOB);
    for _ in 0..2 {
        let (status, body) = get(&gateway, &format!("/{blob}/")).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    }
    assert_eq!(*caps.lock().unwrap(), [MAX_MANIFEST_MB; 2]);
}

/// A bundle reads its manifest under the manifest cap, once, and serves its
/// entries under the gateway's own cap.
#[tokio::test]
async fn a_bundle_serves_its_index_and_entries() {
    let entry = [0xe0; 32];
    let manifest = format!(
        r#"{{"version": 1, "entries": [{{"path": "a.bin", "hash": "{}", "size": 3}}]}}"#,
        alloy::hex::encode(entry)
    );
    let replies = HashMap::from([
        (BLOB, Reply::Bytes(manifest.into_bytes())),
        (entry, Reply::Bytes(b"abc".to_vec())),
    ]);
    let (gateway, caps) = recording_gateway_of(replies, &[]).await;
    let blob = alloy::hex::encode(BLOB);
    let (status, body) = get(&gateway, &format!("/{blob}/a.bin")).await;
    assert_eq!((status, body.as_str()), (StatusCode::OK, "abc"));
    let (status, body) = get(&gateway, &format!("/{blob}/")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("a.bin"), "{body}");
    assert_eq!(
        *caps.lock().unwrap(),
        [MAX_MANIFEST_MB, flags(&[]).max_blob_mb],
        "the index reads the cached manifest"
    );
}

/// A manifest with an entry hash that does not parse is malformed: its index
/// and every entry, a valid sibling too, answer 404 naming the bad entry with
/// no raw-blob hint, the index links nothing, and no pull for an entry starts.
#[tokio::test]
async fn a_malformed_entry_hash_makes_the_bundle_not_found() {
    let manifest = serde_json::json!({"version": 1, "entries": [
        {"path": "ok.bin", "hash": alloy::hex::encode([0xe0; 32]), "size": 3},
        {"path": "a.bin", "hash": "zz", "size": 3},
    ]});
    let (gateway, caps) =
        recording_gateway(Reply::Bytes(manifest.to_string().into_bytes()), &[]).await;
    let blob = alloy::hex::encode(BLOB);
    let paths = [
        format!("/{blob}/a.bin"),
        format!("/{blob}/ok.bin"),
        format!("/{blob}/"),
    ];
    for path in &paths {
        let (status, body) = get(&gateway, path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}: {body}");
        assert!(
            body.contains(r#"bundle entry "a.bin" has a malformed hash"#),
            "{path}: {body}"
        );
        assert!(body.contains(MALFORMED_MANIFEST), "{path}: {body}");
        assert!(!body.contains("raw blob"), "{path}: {body}");
        assert!(!body.contains("href"), "{path}: {body}");
    }
    assert_eq!(
        *caps.lock().unwrap(),
        [MAX_MANIFEST_MB],
        "the first request pulls only the manifest, and the rest none"
    );
}

/// A malformed manifest quotes its publisher's path and hash string, so the
/// remembered verdict, and the 404 that carries it, keep only the first
/// [`MAX_VERDICT_TEXT`] bytes.
#[tokio::test]
async fn a_malformed_manifest_with_huge_fields_is_remembered_clipped() {
    let path = "p".repeat(100 << 10);
    let manifest = serde_json::json!({"version": 1, "entries": [
        {"path": path, "hash": "z".repeat(100 << 10), "size": 3},
    ]});
    let (gateway, caps) =
        recording_gateway(Reply::Bytes(manifest.to_string().into_bytes()), &[]).await;
    let blob = alloy::hex::encode(BLOB);
    for _ in 0..2 {
        let (status, body) = get(&gateway, &format!("/{blob}/")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.starts_with(MALFORMED_MANIFEST), "{}", &body[..80]);
        assert!(body.len() <= MAX_VERDICT_TEXT + 1, "{}", body.len());
    }
    assert_eq!(*caps.lock().unwrap(), [MAX_MANIFEST_MB]);
    let kept = gateway.not_manifests.lock().unwrap();
    let NotManifest::Plain(text) = &kept.entries[0].1 else {
        panic!("a malformed manifest is a plain verdict");
    };
    assert!(text.len() <= MAX_VERDICT_TEXT);
}

/// A response's status, headers and body as text.
struct Answer {
    status: StatusCode,
    headers: HeaderMap,
    body: String,
}

impl Answer {
    fn header(&self, name: header::HeaderName) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

/// `GET path` with `Host: host` and `headers`.
async fn request(
    gateway: &Arc<Gateway>,
    host: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> Answer {
    send(gateway, Method::GET, host, path, headers).await
}

/// `method path` with `Host: host` and `headers`, and an empty body.
async fn send(
    gateway: &Arc<Gateway>,
    method: Method,
    host: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> Answer {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, host);
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    let response = Arc::clone(gateway)
        .handle(req.body(Full::new(Bytes::new())).unwrap())
        .await;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    Answer {
        status,
        headers,
        body: String::from_utf8(body.to_vec()).unwrap(),
    }
}

/// The browser headers of the site's own subresource request.
const SAME_ORIGIN: &[(&str, &str)] = &[
    ("sec-fetch-site", "same-origin"),
    ("sec-fetch-mode", "no-cors"),
    ("sec-fetch-dest", "style"),
];

/// The browser headers of a typed URL.
const TYPED: &[(&str, &str)] = &[
    ("sec-fetch-site", "none"),
    ("sec-fetch-mode", "navigate"),
    ("sec-fetch-dest", "document"),
];

/// A link click on the site's own page.
const CLICK: &[(&str, &str)] = &[
    ("sec-fetch-site", "same-origin"),
    ("sec-fetch-mode", "navigate"),
    ("sec-fetch-dest", "document"),
];

const PAGE: [u8; 32] = [0xe1; 32];
const STYLE: [u8; 32] = [0xe2; 32];
const DOCS: [u8; 32] = [0xe3; 32];
const FEED: [u8; 32] = [0xec; 32];
const EMPTY_FEED: [u8; 32] = [0xed; 32];
const OTHER: [u8; 32] = [0xcd; 32];

/// An RSS document that runs a script if a browser renders it as XML.
const RSS: &str = "<rss version=\"2.0\"><script xmlns=\"http://www.w3.org/1999/xhtml\">\
                   alert(1)</script></rss>";

/// A gateway serving [`BLOB`] as a site bundle with `index.html`,
/// `css/site.css`, `docs/index.html`, `feed.rss` and the empty `empty.rss`,
/// the blob [`OTHER`], and the host of that site.
async fn site_gateway() -> (Arc<Gateway>, String) {
    let other = alloy::hex::encode(OTHER);
    let manifest = serde_json::json!({"version": 1, "entries": [
        {"path": "index.html", "hash": alloy::hex::encode(PAGE)},
        {"path": "css/site.css", "hash": alloy::hex::encode(STYLE)},
        {"path": "docs/index.html", "hash": alloy::hex::encode(DOCS)},
        {"path": "feed.rss", "hash": alloy::hex::encode(FEED)},
        {"path": "empty.rss", "hash": alloy::hex::encode(EMPTY_FEED), "size": 0},
    ]});
    let page = format!(
        "<!doctype html><link rel=stylesheet href=\"css/site.css\">\
         <img src=\"decdn:{other}\"><a href=\"decdn:{other}/\">other</a>"
    );
    let replies = HashMap::from([
        (BLOB, Reply::Bytes(manifest.to_string().into_bytes())),
        (PAGE, Reply::Bytes(page.into_bytes())),
        (
            STYLE,
            Reply::Bytes(format!("a {{ background: url(decdn:{other}) }}").into_bytes()),
        ),
        (DOCS, Reply::Bytes(b"<p>docs</p>".to_vec())),
        (FEED, Reply::Bytes(RSS.as_bytes().to_vec())),
        (OTHER, Reply::Bytes(b"other bytes".to_vec())),
    ]);
    let (gateway, _) = recording_gateway_of(replies, &[]).await;
    let host = gateway.sites.site_host(&Site {
        hash: BLOB,
        namespace: None,
    });
    (gateway, host)
}

#[tokio::test]
async fn a_site_serves_its_index_as_html_with_uris_rewritten() {
    let (gateway, host) = site_gateway().await;
    let other = alloy::hex::encode(OTHER);
    let page = request(&gateway, &host, "/", TYPED).await;
    assert_eq!(page.status, StatusCode::OK, "{}", page.body);
    assert_eq!(
        page.header(header::CONTENT_TYPE),
        Some("text/html; charset=utf-8")
    );
    assert!(
        page.body.contains(&format!("src=\"/_decdn/{other}\"")),
        "{}",
        page.body
    );
    assert!(
        page.body.contains(&format!("href=\"/_decdn/{other}/\"")),
        "{}",
        page.body
    );
    assert!(!page.body.contains("decdn:"), "{}", page.body);
    assert!(!page.body.contains("8484"), "{}", page.body);
    assert_eq!(page.header(header::CONTENT_LENGTH), None);
    let policy = page.header(header::CONTENT_SECURITY_POLICY).unwrap();
    assert!(policy.starts_with("default-src 'self'"), "{policy}");

    let style = request(&gateway, &host, "/css/site.css", SAME_ORIGIN).await;
    assert_eq!(style.status, StatusCode::OK);
    assert_eq!(
        style.header(header::CONTENT_TYPE),
        Some("text/css; charset=utf-8")
    );
    assert_eq!(
        style.body,
        format!("a {{ background: url(/_decdn/{other}) }}")
    );
}

#[tokio::test]
async fn a_site_directory_serves_its_index() {
    let (gateway, host) = site_gateway().await;
    let docs = request(&gateway, &host, "/docs/", CLICK).await;
    assert_eq!(
        (docs.status, docs.body.as_str()),
        (StatusCode::OK, "<p>docs</p>")
    );
    let bare = request(&gateway, &host, "/docs", CLICK).await;
    assert_eq!(bare.status, StatusCode::FOUND);
    assert_eq!(bare.header(header::LOCATION), Some("docs/"));
    // The query belongs to the page, and the redirect keeps it.
    let queried = request(&gateway, &host, "/docs?tab=2", CLICK).await;
    assert_eq!(queried.status, StatusCode::FOUND);
    assert_eq!(queried.header(header::LOCATION), Some("docs/?tab=2"));
    let missing = request(&gateway, &host, "/nope.html", CLICK).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    assert!(
        missing
            .headers
            .contains_key(header::CONTENT_SECURITY_POLICY)
    );
}

/// A directory in a bundle under `/_decdn/` serves its `index.html`, as a
/// site's own directory does, and its redirect keeps the request's `ns=`.
#[tokio::test]
async fn a_directory_under_decdn_serves_its_index() {
    let (gateway, host) = site_gateway().await;
    let hex = alloy::hex::encode(BLOB);
    let bare = request(&gateway, &host, &format!("/_decdn/{hex}/docs"), SAME_ORIGIN).await;
    assert_eq!(bare.status, StatusCode::FOUND, "{}", bare.body);
    assert_eq!(bare.header(header::LOCATION), Some("docs/"));
    let docs = request(
        &gateway,
        &host,
        &format!("/_decdn/{hex}/docs/"),
        SAME_ORIGIN,
    )
    .await;
    assert_eq!(
        (docs.status, docs.body.as_str()),
        (StatusCode::OK, "<p>docs</p>")
    );
    let hinted = request(
        &gateway,
        &host,
        &format!("/_decdn/{hex}/docs?ns=7"),
        SAME_ORIGIN,
    )
    .await;
    assert_eq!(hinted.status, StatusCode::FOUND, "{}", hinted.body);
    assert_eq!(hinted.header(header::LOCATION), Some("docs/?ns=7"));
    let hinted_docs = request(
        &gateway,
        &host,
        &format!("/_decdn/{hex}/docs/?ns=7"),
        SAME_ORIGIN,
    )
    .await;
    assert_eq!(
        (hinted_docs.status, hinted_docs.body.as_str()),
        (StatusCode::OK, "<p>docs</p>")
    );
    // A navigation opens the directory itself on the bundle's site.
    let opened = request(&gateway, &host, &format!("/_decdn/{hex}/docs/"), CLICK).await;
    assert_eq!(opened.status, StatusCode::FOUND, "{}", opened.body);
    let docs_url = format!("{}/docs/", gateway.sites.origin(&host));
    assert_eq!(opened.header(header::LOCATION), Some(docs_url.as_str()));
    // A directory without an `index.html` has nothing to serve.
    let bare_dir = request(&gateway, &host, &format!("/_decdn/{hex}/css/"), SAME_ORIGIN).await;
    assert_eq!(bare_dir.status, StatusCode::NOT_FOUND, "{}", bare_dir.body);
}

/// A directory under `/_decdn/` names its `index.html`; the bundle root, a
/// file and the query stay as they are.
#[test]
fn directory_index_names_a_bundle_directorys_index() {
    let h = alloy::hex::encode(BLOB);
    for (rest, named) in [
        (format!("{h}/docs/"), format!("{h}/docs/index.html")),
        (
            format!("{h}/docs/?ns=7"),
            format!("{h}/docs/index.html?ns=7"),
        ),
        (format!("{h}/a/b/"), format!("{h}/a/b/index.html")),
        // An empty segment stays one, for the URI parser to refuse.
        (format!("{h}//"), format!("{h}//index.html")),
    ] {
        assert_eq!(directory_index(&rest), (named, true), "{rest}");
    }
    for same in [
        format!("{h}/"),
        h.clone(),
        format!("{h}/a.txt?ns=7"),
        format!("{h}/?ns=7"),
    ] {
        assert_eq!(directory_index(&same), (same.clone(), false));
    }
}

#[tokio::test]
async fn decdn_paths_serve_a_pages_requests_and_redirect_its_navigation() {
    let (gateway, host) = site_gateway().await;
    let other = alloy::hex::encode(OTHER);
    let image = request(&gateway, &host, &format!("/_decdn/{other}"), SAME_ORIGIN).await;
    assert_eq!(
        (image.status, image.body.as_str()),
        (StatusCode::OK, "other bytes")
    );

    let other_site = gateway.sites.site_host(&Site {
        hash: OTHER,
        namespace: Some(7),
    });
    let link = request(
        &gateway,
        &host,
        &format!("/_decdn/{other}/a%20b.html?ns=7"),
        CLICK,
    )
    .await;
    assert_eq!(link.status, StatusCode::FOUND);
    assert_eq!(
        link.header(header::LOCATION),
        Some(format!("http://{other_site}/a%20b.html").as_str())
    );

    let download = request(&gateway, &host, &format!("/_decdn/{other}"), CLICK).await;
    assert_eq!(download.status, StatusCode::OK);
    assert!(
        download
            .header(header::CONTENT_DISPOSITION)
            .unwrap()
            .starts_with("attachment"),
    );
    let policy = download.header(header::CONTENT_SECURITY_POLICY).unwrap();
    assert!(policy.starts_with("default-src 'self'"), "{policy}");
}

#[tokio::test]
async fn other_pages_cannot_make_a_site_spend() {
    let (gateway, host) = site_gateway().await;
    let image = [
        ("sec-fetch-site", "cross-site"),
        ("sec-fetch-mode", "no-cors"),
        ("sec-fetch-dest", "image"),
    ];
    let frame = [
        ("sec-fetch-site", "cross-site"),
        ("sec-fetch-mode", "navigate"),
        ("sec-fetch-dest", "iframe"),
    ];
    let sibling = [
        ("sec-fetch-site", "same-site"),
        ("sec-fetch-mode", "cors"),
        ("sec-fetch-dest", "empty"),
    ];
    for headers in [&image[..], &frame[..], &sibling[..]] {
        let answer = request(&gateway, &host, "/", headers).await;
        assert_eq!(answer.status, StatusCode::FORBIDDEN, "{headers:?}");
    }
    let link = [
        ("sec-fetch-site", "same-site"),
        ("sec-fetch-mode", "navigate"),
        ("sec-fetch-dest", "document"),
    ];
    let deep = request(&gateway, &host, "/docs/", &link).await;
    assert_eq!(
        deep.status,
        StatusCode::OK,
        "a link opens a site at any path"
    );
}

/// A client outside a browser sends no `Sec-Fetch-Site`, and a site spends
/// for it.
#[tokio::test]
async fn a_client_outside_a_browser_reads_a_site() {
    let (gateway, host) = site_gateway().await;
    for headers in [&[][..], &[("user-agent", "curl/8.0")][..]] {
        let answer = request(&gateway, &host, "/docs/", headers).await;
        assert_eq!(
            (answer.status, answer.body.as_str()),
            (StatusCode::OK, "<p>docs</p>"),
            "{headers:?}"
        );
    }
}

#[tokio::test]
async fn a_site_answers_no_gateway_api_and_a_bad_site_host_is_refused() {
    let (gateway, host) = site_gateway().await;
    for path in ["/_delegate", "/_capability"] {
        let answer = request(&gateway, &host, path, TYPED).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{path}");
    }
    let answer = request(&gateway, "127.0.0.1:8484", "/_delegate", &[]).await;
    assert_eq!(answer.status, StatusCode::OK, "the bare origin answers it");
    for bad in [
        format!("1{}", &host[1..]),
        host.replace("8484", "8485"),
        "evil.example:8484".to_owned(),
    ] {
        let answer = request(&gateway, &bad, "/", TYPED).await;
        assert_eq!(answer.status, StatusCode::FORBIDDEN, "{bad}");
    }
}

#[tokio::test]
async fn a_site_without_an_index_lists_its_entries_relatively() {
    let manifest = serde_json::json!({"version": 1, "entries": [
        {"path": "a b.txt", "hash": alloy::hex::encode([0xe0; 32])},
    ]});
    let (gateway, _) =
        recording_gateway(Reply::Bytes(manifest.to_string().into_bytes()), &[]).await;
    let host = gateway.sites.site_host(&Site {
        hash: BLOB,
        namespace: None,
    });
    let list = request(&gateway, &host, "/", TYPED).await;
    assert_eq!(list.status, StatusCode::OK);
    assert!(list.body.contains("href=\"./a%20b.txt\""), "{}", list.body);
}

#[tokio::test]
async fn a_navigation_on_the_bare_origin_opens_a_bundle_as_its_site() {
    let gateway = test_gateway().await;
    let blob = alloy::hex::encode(BLOB);
    let site = gateway.sites.site_host(&Site {
        hash: BLOB,
        namespace: Some(3),
    });
    let answer = request(&gateway, "127.0.0.1:8484", &format!("/{blob}/?ns=3"), TYPED).await;
    assert_eq!(answer.status, StatusCode::FOUND);
    assert_eq!(
        answer.header(header::LOCATION),
        Some(format!("http://{site}/").as_str())
    );
    let answer = request(&gateway, "127.0.0.1:8484", &format!("/{blob}"), TYPED).await;
    assert_eq!(
        answer.status,
        StatusCode::PAYMENT_REQUIRED,
        "a blob has no site"
    );
}

/// A bundle request that is no navigation is served at the bare origin, as
/// a listing of the bundle.
#[tokio::test]
async fn a_bundle_request_that_is_no_navigation_stays_on_the_bare_origin() {
    let (gateway, _) = site_gateway().await;
    let blob = alloy::hex::encode(BLOB);
    let cors = [
        ("sec-fetch-site", "same-origin"),
        ("sec-fetch-mode", "cors"),
        ("sec-fetch-dest", "empty"),
    ];
    for headers in [&[][..], &cors[..]] {
        let answer = request(&gateway, "127.0.0.1:8484", &format!("/{blob}/"), headers).await;
        assert_eq!(
            answer.status,
            StatusCode::OK,
            "{headers:?}: {}",
            answer.body
        );
        assert!(answer.body.contains("index.html"), "{}", answer.body);
        assert_eq!(answer.header(header::LOCATION), None);
    }
}

#[tokio::test]
async fn the_bare_origin_still_serves_markup_as_text() {
    let (gateway, _) = site_gateway().await;
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/{}/index.html", alloy::hex::encode(BLOB)))
        .header(header::HOST, "127.0.0.1:8484")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = Arc::clone(&gateway).handle(req).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/plain; charset=utf-8"
    );
    assert_eq!(
        response.headers()[header::CONTENT_SECURITY_POLICY],
        "sandbox"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        String::from_utf8_lossy(&body).contains("decdn:"),
        "not rewritten"
    );
}

/// An `.rss` entry on the bare origin is neutralised to plain text and
/// sandboxed, streamed or empty: a browser renders any `+xml` type as an XML
/// document, so an XHTML-namespaced script inside one would otherwise run on
/// the gateway's origin.
#[tokio::test]
async fn the_bare_origin_neutralises_and_sandboxes_an_rss_entry() {
    let (gateway, _) = site_gateway().await;
    let blob = alloy::hex::encode(BLOB);
    for (path, body) in [("feed.rss", RSS), ("empty.rss", "")] {
        let answer = request(&gateway, "127.0.0.1:8484", &format!("/{blob}/{path}"), &[]).await;
        assert_eq!(answer.status, StatusCode::OK, "{path}");
        assert_eq!(
            answer.header(header::CONTENT_TYPE),
            Some("text/plain; charset=utf-8"),
            "{path}"
        );
        assert_eq!(
            answer.header(header::CONTENT_SECURITY_POLICY),
            Some("sandbox"),
            "{path}"
        );
        assert_eq!(answer.body, body, "{path} not rewritten");
    }
}

#[test]
fn is_active_type_matches_the_xml_suffix_only() {
    for active in [
        "application/xml",
        "application/rss+xml",
        "image/svg+xml",
        "application/javascript",
    ] {
        assert!(is_active_type(active), "{active}");
    }
    for passive in [
        "application/epub+zip",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/pdf",
        "image/png",
    ] {
        assert!(!is_active_type(passive), "{passive}");
    }
}

#[test]
fn only_a_cap_stop_marks_a_manifest_pull_not_a_manifest() {
    let too_large = || {
        anyhow::Error::new(decdn_client::BlobTooLarge {
            reached: 2,
            ceiling: 1,
        })
    };
    assert!(matches!(
        over_manifest_cap(PullError::Other(too_large()).into()),
        ManifestStop::Not(NotManifest::OverCap(_))
    ));
    assert!(matches!(
        over_manifest_cap(PullError::Other(anyhow::anyhow!("no node")).into()),
        ManifestStop::Pull(PullError::Other(_))
    ));
    assert!(matches!(
        over_manifest_cap(PullError::Funding(too_large()).into()),
        ManifestStop::Pull(PullError::Funding(_))
    ));
}

#[derive(clap::Parser)]
struct Flags {
    #[command(flatten)]
    common: cli::ClientFetchArgs,
}

fn flags(args: &[&str]) -> cli::ClientFetchArgs {
    <Flags as clap::Parser>::try_parse_from(std::iter::once("gateway").chain(args.iter().copied()))
        .unwrap()
        .common
}

#[test]
fn the_gateway_drops_the_password_file_and_refuses_key_flags() {
    let common = fetch_flags(&flags(&["--keystore-password-file", "/pw"])).unwrap();
    assert!(common.keystore_password_file.is_none());
    for flag in [
        ["--keystore", "/ks"],
        ["--capability", "dcap1:x"],
        ["--capability-file", "/cap"],
    ] {
        assert!(fetch_flags(&flags(&flag)).is_err(), "{flag:?}");
    }
}

#[test]
fn classify_finds_funding_attached_as_context() {
    let funding = decdn_client::FundingNeeded::PublisherPool {
        pool: alloy::primitives::B256::ZERO,
    };
    let e = anyhow::anyhow!("refused").context(funding).context("outer");
    assert!(matches!(classify(e), PullError::Funding(_)));
    assert!(matches!(
        classify(anyhow::anyhow!("no node")),
        PullError::Other(_)
    ));
}

#[tokio::test]
async fn a_pull_that_fails_before_any_byte_is_an_error() {
    let (writer, mut reader) = tokio::io::duplex(64);
    let (done_tx, done_rx) = oneshot::channel();
    drop(writer);
    done_tx.send(Err(anyhow::anyhow!("not found"))).unwrap();
    let Err(PullError::Other(e)) = first_chunk(&mut reader, done_rx).await else {
        panic!("expected the pull's error");
    };
    assert_eq!(e.to_string(), "not found");
}

#[tokio::test]
async fn an_empty_successful_pull_is_an_empty_body() {
    let (writer, mut reader) = tokio::io::duplex(64);
    let (done_tx, done_rx) = oneshot::channel();
    drop(writer);
    done_tx.send(Ok(())).unwrap();
    let Ok((first, done)) = first_chunk(&mut reader, done_rx).await else {
        panic!("expected an empty body");
    };
    assert!(first.is_empty());
    assert!(finished(done).await.is_ok());
}

#[tokio::test]
async fn the_first_bytes_come_back_before_the_pull_ends() {
    let (mut writer, mut reader) = tokio::io::duplex(64);
    let (_done_tx, done_rx) = oneshot::channel::<anyhow::Result<()>>();
    tokio::io::AsyncWriteExt::write_all(&mut writer, b"abc")
        .await
        .unwrap();
    let Ok((first, _)) = first_chunk(&mut reader, done_rx).await else {
        panic!("expected the first bytes");
    };
    assert_eq!(&first[..], b"abc");
}

#[test]
fn content_type_by_extension_and_markup_as_text() {
    assert_eq!(content_type("Sintel.2010.4k.MKV"), "video/x-matroska");
    assert_eq!(content_type("clip.mp4"), "video/mp4");
    assert_eq!(
        content_type("weights.safetensors"),
        "application/octet-stream"
    );
    assert_eq!(content_type("image.PNG"), "image/png");
    assert_eq!(content_type("no-extension"), "application/octet-stream");
    for active in [
        "page.html",
        "page.htm",
        "page.xhtml",
        "logo.svg",
        "feed.xml",
        "feed.rss",
        "a.atom",
        "m.rdf",
        "m.mathml",
        "g.kml",
        "app.js",
        "app.mjs",
        "notes.txt",
        "style.css",
    ] {
        assert_eq!(
            content_type(active),
            "text/plain; charset=utf-8",
            "{active}"
        );
    }
}

#[test]
fn write_escaped_covers_markup_and_quotes() {
    let mut out = String::new();
    write_escaped(&mut out, r#"<a href="x">'&'</a>"#).unwrap();
    assert_eq!(out, "&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;");
}

/// The listing's markup, pinned whole for one small bundle: the heading, the
/// row shape, and the empty size cell of an entry without a size.
#[test]
fn a_listing_renders_its_rows_and_an_empty_size_cell() {
    let hash = [0x11; 32];
    let manifest = BundleManifest::new(
        hash,
        vec![
            decdn_client::bundle::ManifestEntry::new("a.txt", [0xe0; 32], Some(3)),
            decdn_client::bundle::ManifestEntry::new("c.bin", [0xe1; 32], None),
        ],
    );
    let uri = DecdnUri::new(hash, UriTarget::Bundle, None);
    let a = DecdnUri::new(hash, UriTarget::BundleEntry("a.txt".to_owned()), None);
    let c = DecdnUri::new(hash, UriTarget::BundleEntry("c.bin".to_owned()), None);
    let mut page = String::new();
    write_listing(&mut page, &manifest, hash, None, Present::Bare, &uri).unwrap();
    assert_eq!(
        page,
        format!(
            "<h1>Bundle</h1><p><code>{uri}</code></p><table>\
             <tr><td><a href=\"{a}\">a.txt</a></td><td>3 B</td></tr>\
             <tr><td><a href=\"{c}\">c.bin</a></td><td></td></tr></table>"
        )
    );
}

/// A streamed link is the link the gateway gives elsewhere: on a site, `./`
/// and [`entry_link`]; otherwise the entry's `decdn:` URI, namespace and all.
#[test]
fn a_streamed_link_matches_the_entry_link() {
    let hash = [0x22; 32];
    for path in [
        "a.txt",
        "dir/a b.txt",
        "it's & \"that\".bin",
        "ünïcødé/文件 ?#.bin",
    ] {
        for namespace in [None, Some(9)] {
            let mut site = String::new();
            write_link(&mut site, hash, path, namespace, Present::Site).unwrap();
            assert_eq!(site, format!("./{}", entry_link(hash, path)), "{path}");
            let mut bare = String::new();
            write_link(&mut bare, hash, path, namespace, Present::Bare).unwrap();
            let uri = DecdnUri::new(hash, UriTarget::BundleEntry(path.to_owned()), namespace);
            assert_eq!(bare, uri.to_string(), "{path} ns={namespace:?}");
        }
    }
}

/// The entry blob of the public gateway's bundle.
const ENTRY: [u8; 32] = [0xe4; 32];

/// What one public gateway's pulls carried, in order.
struct Logs {
    /// The `--max-blob-mb` of each pull.
    caps: Log<u64>,
    /// The size cap of each pull.
    size_caps: Log<Option<u64>>,
    /// The byte window of each pull.
    ranges: Log<Option<(u64, u64)>>,
    /// The blob of each size proof.
    proofs: Log<[u8; 32]>,
    /// Where the gateway keeps its pointers, for as long as the test runs.
    pointers_dir: tempfile::TempDir,
}

/// The blob whose pull stops for funding on the public gateway.
const UNFUNDED: [u8; 32] = [0xe5; 32];

/// A blob of several response frames on the public gateway.
const BIG: [u8; 32] = [0xe6; 32];

/// A blob whose pull writes a few bytes and then ends at its cap.
const PARTIAL: [u8; 32] = [0xe7; 32];

/// A bundle with no `index.html`, whose one entry has a space in its name.
const NOINDEX: [u8; 32] = [0xe9; 32];

/// A blob whose pull never writes a byte, and never ends.
const HELD: [u8; 32] = [0xea; 32];

/// A blob whose size proof never ends.
const HELD_PROOF: [u8; 32] = [0xeb; 32];

/// The [`NOINDEX`] bundle's manifest.
fn noindex_manifest() -> Vec<u8> {
    serde_json::json!({"version": 1, "entries": [
        {"path": "a b.txt", "hash": alloy::hex::encode(ENTRY), "size": 3},
    ]})
    .to_string()
    .into_bytes()
}

/// The length of [`BIG`].
const BIG_LEN: usize = 600 * 1024;

/// The empty entry of the public gateway's bundle.
const EMPTY: [u8; 32] = [0xe8; 32];

/// The public gateway's bundle manifest.
fn public_manifest() -> Vec<u8> {
    serde_json::json!({"version": 1, "entries": [
        {"path": "a.txt", "hash": alloy::hex::encode(ENTRY), "size": 3},
        {"path": "index.html", "hash": alloy::hex::encode(PAGE)},
        {"path": "empty.txt", "hash": alloy::hex::encode(EMPTY), "size": 0},
        {"path": "big.bin", "hash": alloy::hex::encode(BIG), "size": BIG_LEN},
    ]})
    .to_string()
    .into_bytes()
}

/// A gateway on [`Recording`] serving the public origin `https://decdn.test`,
/// with [`BLOB`] a bundle of `a.txt` (3 bytes) and `index.html`, the blob
/// [`OTHER`], and the caps its pulls record, plus the bundle's site host.
async fn public_gateway() -> (Arc<Gateway>, Logs, String) {
    public_gateway_sized(HashMap::new()).await
}

/// [`public_gateway`] whose earlier pulls verified the sizes in `found`.
async fn public_gateway_sized(found: HashMap<[u8; 32], u64>) -> (Arc<Gateway>, Logs, String) {
    public_gateway_proving(found, HashMap::new()).await
}

/// [`public_gateway_sized`] whose size proofs prove the sizes in `proves`.
async fn public_gateway_proving(
    found: HashMap<[u8; 32], u64>,
    proves: HashMap<[u8; 32], u64>,
) -> (Arc<Gateway>, Logs, String) {
    public_gateway_flagged(found, proves, flags(&[])).await
}

/// [`public_gateway_proving`] run with the fetch flags `common`, holding one
/// capability that pays.
async fn public_gateway_flagged(
    found: HashMap<[u8; 32], u64>,
    proves: HashMap<[u8; 32], u64>,
    common: cli::ClientFetchArgs,
) -> (Arc<Gateway>, Logs, String) {
    let (gateway, logs, host) = public_gateway_unpaid(found, proves, common).await;
    hold_capability(&gateway, 0);
    (gateway, logs, host)
}

/// [`public_gateway_flagged`] holding no capability.
async fn public_gateway_unpaid(
    found: HashMap<[u8; 32], u64>,
    proves: HashMap<[u8; 32], u64>,
    common: cli::ClientFetchArgs,
) -> (Arc<Gateway>, Logs, String) {
    let replies = HashMap::from([
        (BLOB, Reply::Bytes(public_manifest())),
        (ENTRY, Reply::Bytes(b"abc".to_vec())),
        (PAGE, Reply::Bytes(b"<p>hi</p>".to_vec())),
        (OTHER, Reply::Bytes(b"other bytes".to_vec())),
        (UNFUNDED, Reply::Funding),
        (BIG, Reply::Bytes(vec![0x42; BIG_LEN])),
        (PARTIAL, Reply::BytesThenTooLarge(b"partial".to_vec())),
        (NOINDEX, Reply::Bytes(noindex_manifest())),
        (HELD, Reply::Hold),
        (HELD_PROOF, Reply::HoldProof),
    ]);
    let logs = Logs {
        caps: Arc::default(),
        size_caps: Arc::default(),
        ranges: Arc::default(),
        proofs: Arc::default(),
        pointers_dir: tempfile::tempdir().unwrap(),
    };
    let backend = Recording {
        caps: Arc::clone(&logs.caps),
        size_caps: Arc::clone(&logs.size_caps),
        ranges: Arc::clone(&logs.ranges),
        proofs: Arc::clone(&logs.proofs),
        replies,
        sizes: std::sync::Mutex::new(found),
        proves,
    };
    let sites = SiteDomain::Public {
        origin: "https://decdn.test".parse().unwrap(),
    };
    let public = public_with(logs.pointers_dir.path());
    let gateway = test_gateway_in(backend, common, sites, Some(public), 1).await;
    let host = gateway.sites.site_host(&Site {
        hash: BLOB,
        namespace: None,
    });
    (gateway, logs, host)
}

/// `method path` with `Host: host` and `body`.
async fn send_body(
    gateway: &Arc<Gateway>,
    method: Method,
    host: &str,
    path: &str,
    body: &str,
) -> Answer {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, host)
        .body(Full::new(Bytes::from(body.to_owned())))
        .unwrap();
    let response = Arc::clone(gateway).handle(req).await;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    Answer {
        status,
        headers,
        body: String::from_utf8(body.to_vec()).unwrap(),
    }
}

/// A pointer from namespace `namespace` to the bundle `root`, with no real
/// signature: stored straight into the gateway, past `PUT /_pointer`.
fn unsigned_pointer(namespace: u64, root: [u8; 32], seq: u64) -> SignedPointer {
    SignedPointer {
        namespace,
        root: alloy::primitives::B256::from(root),
        seq,
        signature: alloy::primitives::Bytes::from(vec![0u8; 65]),
    }
}

fn pointers(gateway: &Gateway) -> &Pointers {
    &gateway.public.as_ref().unwrap().pointers
}

/// Store `pointer` as signed by `signer`, past `PUT /_pointer`.
fn hold(gateway: &Gateway, pointer: SignedPointer, signer: Address) {
    pointers(gateway).set(pointer, signer).unwrap();
}

/// A signer the gateway stored a pointer from before draws on a lookup
/// budget of its own per namespace, still read on-chain, so a flood of
/// strangers on the shared budget does not keep a publisher from updating.
/// That budget is per namespace, so one publisher's updates cannot spend
/// another's either.
#[tokio::test]
async fn a_known_signer_has_its_own_lookup_budget_per_namespace() {
    let (gateway, _logs, _) = public_gateway().await;
    let domain = pointer_domain(gateway.chain.governance.chain_id, REGISTRY);
    let owner = PrivateKeySigner::random();
    let public = gateway.public.as_ref().unwrap();
    hold(&gateway, unsigned_pointer(7, BLOB, 1), owner.address());
    hold(&gateway, unsigned_pointer(8, BLOB, 1), owner.address());
    let put = |ns: u64, seq: u64| {
        let pointer = SignedPointer::sign(&owner, ns, OTHER, seq, &domain).unwrap();
        let body = serde_json::to_string(&pointer).unwrap();
        let gateway = Arc::clone(&gateway);
        async move { send_body(&gateway, Method::PUT, "decdn.test", "/_pointer", &body).await }
    };
    let (shared_spent, known_spent) = loop {
        let now = now_secs();
        while public.take_lookup(None, now).is_ok() {}
        let shared_spent = put(7, 2).await;
        let mut seq = 3;
        while public.take_lookup(Some(7), now).is_ok() {
            seq += 1;
        }
        let known_spent = put(7, seq).await;
        if now_secs() / 60 == now / 60 {
            break (shared_spent, known_spent);
        }
    };
    // The holder's update reached the chain past the spent shared budget.
    assert_eq!(
        shared_spent.status,
        StatusCode::BAD_GATEWAY,
        "{}",
        shared_spent.body
    );
    // Its own budget runs out too, per namespace.
    assert_eq!(
        known_spent.status,
        StatusCode::TOO_MANY_REQUESTS,
        "{}",
        known_spent.body
    );
    let other_namespace = put(8, 2).await;
    assert_eq!(
        other_namespace.status,
        StatusCode::BAD_GATEWAY,
        "{}",
        other_namespace.body
    );
}

/// `PUT /_pointer` answers on the public bare host only: a loopback gateway
/// holds no pointers, and a site host has no API.
#[tokio::test]
async fn put_pointer_answers_on_the_public_bare_host_only() {
    let loopback = test_gateway().await;
    let answer = send_body(&loopback, Method::PUT, "127.0.0.1:8484", "/_pointer", "{}").await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
    let (gateway, _logs, host) = public_gateway().await;
    let answer = send_body(&gateway, Method::PUT, &host, "/_pointer", "{}").await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
    let answer = send_body(&gateway, Method::PUT, "7.decdn.test", "/_pointer", "{}").await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
}

/// A body that is too large, not the record, or carries no signature is
/// refused before anything is read on-chain.
#[tokio::test]
async fn put_pointer_refuses_a_bad_body_before_the_chain() {
    let (gateway, _logs, _) = public_gateway().await;
    let big = "x".repeat(MAX_BODY_BYTES + 1);
    let answer = send_body(&gateway, Method::PUT, "decdn.test", "/_pointer", &big).await;
    assert_eq!(answer.status, StatusCode::PAYLOAD_TOO_LARGE);
    let answer = send_body(&gateway, Method::PUT, "decdn.test", "/_pointer", "{").await;
    assert_eq!(answer.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(answer.body.contains("\"signature\""), "{}", answer.body);
    let short = serde_json::to_string(&SignedPointer {
        signature: alloy::primitives::Bytes::from(vec![0u8; 64]),
        ..unsigned_pointer(7, BLOB, 1)
    })
    .unwrap();
    let answer = send_body(&gateway, Method::PUT, "decdn.test", "/_pointer", &short).await;
    assert_eq!(answer.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(answer.body.contains("signature"), "{}", answer.body);
}

/// A pointer from the signer whose pointer is held, at a `seq` not above
/// it, is refused with the held `seq` before the chain is read. One from
/// another signer is not a replay: it goes to the chain, where only the
/// current owner passes, so a namespace that changed hands is not held by
/// its former owner's `seq`.
#[tokio::test]
async fn put_pointer_refuses_a_stale_seq_from_the_same_signer_before_the_chain() {
    let (gateway, _logs, _) = public_gateway().await;
    let owner = PrivateKeySigner::random();
    let domain = pointer_domain(gateway.chain.governance.chain_id, REGISTRY);
    hold(&gateway, unsigned_pointer(7, BLOB, 5), owner.address());
    for seq in [5, 4] {
        let pointer = SignedPointer::sign(&owner, 7, OTHER, seq, &domain).unwrap();
        let body = serde_json::to_string(&pointer).unwrap();
        let answer = send_body(&gateway, Method::PUT, "decdn.test", "/_pointer", &body).await;
        assert_eq!(answer.status, StatusCode::CONFLICT, "seq {seq}");
        let json: serde_json::Value = serde_json::from_str(&answer.body).unwrap();
        assert_eq!(json["seq"], 5);
    }
    let stranger = PrivateKeySigner::random();
    let pointer = SignedPointer::sign(&stranger, 7, OTHER, 1, &domain).unwrap();
    let body = serde_json::to_string(&pointer).unwrap();
    let answer = send_body(&gateway, Method::PUT, "decdn.test", "/_pointer", &body).await;
    assert_eq!(answer.status, StatusCode::BAD_GATEWAY, "{}", answer.body);
    assert_eq!(pointers(&gateway).root(7), Some(BLOB));
}

/// Each on-chain owner lookup a `PUT /_pointer` costs comes out of a budget
/// per minute, so an anonymous flood of well-formed pointers cannot turn the
/// gateway into an RPC amplifier: past it the answer is 429 with
/// `Retry-After`, and nothing is read.
#[tokio::test]
async fn put_pointer_owner_lookups_are_budgeted() {
    let (gateway, _logs, _) = public_gateway().await;
    let domain = pointer_domain(gateway.chain.governance.chain_id, REGISTRY);
    let signer = PrivateKeySigner::random();
    let public = gateway.public.as_ref().unwrap();
    // One lookup reaches the chain; the rest of the minute's budget is
    // spent directly, so the test does not straddle a minute boundary.
    let pointer = SignedPointer::sign(&signer, 1, BLOB, 1, &domain).unwrap();
    let body = serde_json::to_string(&pointer).unwrap();
    let answer = send_body(&gateway, Method::PUT, "decdn.test", "/_pointer", &body).await;
    assert_eq!(answer.status, StatusCode::BAD_GATEWAY, "{}", answer.body);
    let answer = loop {
        let now = now_secs();
        while public.take_lookup(None, now).is_ok() {}
        let pointer = SignedPointer::sign(&signer, 1_000, BLOB, 1, &domain).unwrap();
        let body = serde_json::to_string(&pointer).unwrap();
        let answer = send_body(&gateway, Method::PUT, "decdn.test", "/_pointer", &body).await;
        // The minute turned under the request: the budget is fresh again.
        if now_secs() / 60 == now / 60 {
            break answer;
        }
    };
    assert_eq!(
        answer.status,
        StatusCode::TOO_MANY_REQUESTS,
        "{}",
        answer.body
    );
    assert!(answer.header(header::RETRY_AFTER).is_some());
    // A replay still costs nothing and is still a replay.
    hold(&gateway, unsigned_pointer(7, BLOB, 5), signer.address());
    let replay = SignedPointer::sign(&signer, 7, OTHER, 5, &domain).unwrap();
    let body = serde_json::to_string(&replay).unwrap();
    let answer = send_body(&gateway, Method::PUT, "decdn.test", "/_pointer", &body).await;
    assert_eq!(answer.status, StatusCode::CONFLICT);
}

/// A well-formed pointer with a fresh `seq` is checked against the
/// namespace's owner on-chain: with no chain to ask, the answer is 502 and
/// nothing is stored.
#[tokio::test]
async fn put_pointer_with_a_valid_signature_asks_the_chain() {
    let (gateway, _logs, _) = public_gateway().await;
    let owner = PrivateKeySigner::random();
    let domain = pointer_domain(gateway.chain.governance.chain_id, REGISTRY);
    let pointer = SignedPointer::sign(&owner, 7, BLOB, 1, &domain).unwrap();
    let body = serde_json::to_string(&pointer).unwrap();
    // On the bare host, and on the gateway's own listen address.
    for host in ["decdn.test", "127.0.0.1:8484"] {
        let answer = send_body(&gateway, Method::PUT, host, "/_pointer", &body).await;
        assert_eq!(
            answer.status,
            StatusCode::BAD_GATEWAY,
            "{host}: {}",
            answer.body
        );
    }
    assert_eq!(pointers(&gateway).root(7), None);
}

/// A public site serves its entries to anyone: with no browser headers,
/// the capability the gateway holds pays.
#[tokio::test]
async fn a_public_site_serves_to_anyone() {
    let (gateway, _logs, host) = public_gateway().await;
    let answer = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!(
        (answer.status, answer.body.as_str()),
        (StatusCode::OK, "abc")
    );
    let page = request(&gateway, &host, "/", &[]).await;
    assert_eq!(page.status, StatusCode::OK);
    assert!(page.body.contains("<p>hi</p>"), "{}", page.body);
    assert_eq!(
        page.header(header::CONTENT_TYPE),
        Some("text/html; charset=utf-8")
    );
    let policy = page.header(header::CONTENT_SECURITY_POLICY).unwrap();
    assert!(policy.ends_with("connect-src 'self'"), "{policy}");
    assert!(!policy.contains("http"), "{policy}");
}

/// A public gateway answers its capability API on its bare host, to any
/// origin, and on no site host.
#[tokio::test]
async fn the_public_gateway_api_answers_on_the_bare_host_only() {
    let (gateway, logs, host) = public_gateway().await;
    for (method, path, bare) in [
        (Method::GET, "/_delegate", StatusCode::OK),
        (
            Method::PUT,
            "/_capability",
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (Method::GET, "/_capability", StatusCode::OK),
        (Method::OPTIONS, "/_delegate", StatusCode::NO_CONTENT),
        (Method::OPTIONS, "/_capability", StatusCode::NO_CONTENT),
    ] {
        let answer = send(&gateway, method.clone(), "decdn.test", path, &[]).await;
        assert_eq!(answer.status, bare, "{method} {path} on the bare host");
        assert!(readable_anywhere(&answer), "{method} {path}");
        let answer = send(&gateway, method.clone(), &host, path, &[]).await;
        assert_eq!(
            answer.status,
            StatusCode::NOT_FOUND,
            "{method} {path} on {host}"
        );
    }
    let landing = request(&gateway, "decdn.test", "/", &[]).await;
    assert_eq!(landing.status, StatusCode::OK);
    assert!(logs.caps.lock().unwrap().is_empty(), "nothing was pulled");
}

/// A public gateway pays with the capabilities it holds alone: with none it
/// answers 503, with one it serves, and one that runs out is retired and
/// leaves 503 behind it.
#[tokio::test]
async fn a_public_gateway_pays_with_capabilities() {
    let (gateway, _logs, _) =
        public_gateway_unpaid(HashMap::new(), HashMap::new(), flags(&[])).await;
    let other = format!("/{}", alloy::hex::encode(OTHER));
    let refused = request(&gateway, "decdn.test", &other, &[]).await;
    assert_eq!(
        refused.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        refused.body
    );
    assert!(refused.body.contains("cannot pay"), "{}", refused.body);
    assert_eq!(refused.header(header::RETRY_AFTER), Some("60"));

    hold_capability(&gateway, 1);
    let served = request(&gateway, "decdn.test", &other, &[]).await;
    assert_eq!(served.status, StatusCode::OK, "{}", served.body);
    assert_eq!(served.body, "other bytes");

    let unfunded = format!("/{}", alloy::hex::encode(UNFUNDED));
    let stopped = request(&gateway, "decdn.test", &unfunded, &[]).await;
    assert_eq!(
        stopped.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        stopped.body
    );
    let listed = gateway.capabilities();
    assert_eq!(listed.len(), 1);
    assert!(listed[0].spent, "the capability that ran out is retired");
    let after = request(&gateway, "decdn.test", &other, &[]).await;
    assert_eq!(
        after.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        after.body
    );
}

/// On the public bare host a bundle or an entry redirects to its site over
/// the public scheme, and a blob streams.
#[tokio::test]
async fn the_public_bare_host_redirects_bundles_and_streams_blobs() {
    let (gateway, _logs, host) = public_gateway().await;
    let blob = alloy::hex::encode(BLOB);
    let bundle = request(&gateway, "decdn.test", &format!("/{blob}/"), &[]).await;
    assert_eq!(bundle.status, StatusCode::FOUND);
    assert_eq!(
        bundle.header(header::LOCATION),
        Some(format!("https://{host}/").as_str())
    );
    let entry = request(&gateway, "decdn.test", &format!("/{blob}/a.txt"), &[]).await;
    assert_eq!(
        entry.header(header::LOCATION),
        Some(format!("https://{host}/a.txt").as_str())
    );
    let other = alloy::hex::encode(OTHER);
    let raw = request(&gateway, "decdn.test", &format!("/{other}"), &[]).await;
    assert_eq!(
        (raw.status, raw.body.as_str()),
        (StatusCode::OK, "other bytes")
    );
    assert_eq!(raw.header(header::CONTENT_SECURITY_POLICY), Some("sandbox"));
}

/// A public gateway answers `HEAD` from the bundle manifest: the entry's
/// length and hash, with no pull of the entry itself. A raw blob's `HEAD`
/// pays only for its size proof, and has no length when the proof proves
/// nothing.
#[tokio::test]
async fn a_public_head_is_answered_from_the_manifest() {
    let (gateway, logs, host) = public_gateway().await;
    let head = send(&gateway, Method::HEAD, &host, "/a.txt", &[]).await;
    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(head.header(header::CONTENT_LENGTH), Some("3"));
    assert_eq!(
        head.header(header::ETAG),
        Some(format!("\"{}\"", alloy::hex::encode(ENTRY)).as_str())
    );
    assert_eq!(
        head.header(header::CONTENT_TYPE),
        Some("text/plain; charset=utf-8")
    );
    assert_eq!(head.header(header::ACCEPT_RANGES), Some("bytes"));
    assert!(head.body.is_empty());
    assert_eq!(
        *logs.caps.lock().unwrap(),
        [MAX_MANIFEST_MB],
        "only the manifest is pulled"
    );

    let page = send(&gateway, Method::HEAD, &host, "/", &[]).await;
    assert_eq!(page.status, StatusCode::OK);
    assert_eq!(
        page.header(header::CONTENT_TYPE),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(
        page.header(header::CONTENT_LENGTH),
        None,
        "no size, and HTML is rewritten"
    );

    let other = alloy::hex::encode(OTHER);
    let raw = send(
        &gateway,
        Method::HEAD,
        "decdn.test",
        &format!("/{other}"),
        &[],
    )
    .await;
    assert_eq!(raw.status, StatusCode::OK);
    assert_eq!(
        raw.header(header::CONTENT_LENGTH),
        None,
        "a size no proof proved gives no length"
    );
    assert_eq!(raw.header(header::CONTENT_SECURITY_POLICY), Some("sandbox"));
    assert_eq!(*logs.proofs.lock().unwrap(), [OTHER]);
    assert_eq!(
        logs.caps.lock().unwrap().len(),
        1,
        "a raw blob's HEAD pulls nothing but its size proof"
    );

    let missing = send(&gateway, Method::HEAD, &host, "/nope.txt", &[]).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
}

/// A public gateway answers its domain, its sites and its own listen
/// address, and refuses every other `Host`.
#[tokio::test]
async fn a_public_gateway_refuses_unknown_hosts() {
    let (gateway, _logs, _) = public_gateway().await;
    for host in [
        "evil.test",
        "decdn.test.evil.test",
        "localhost:9999",
        "decdn.test:443",
    ] {
        let answer = request(&gateway, host, "/", &[]).await;
        assert_eq!(answer.status, StatusCode::FORBIDDEN, "{host}");
    }
    // Its own loopback names still reach the bare host, for a health check.
    for host in ["127.0.0.1:8484", "localhost:8484"] {
        let health = request(&gateway, host, "/", &[]).await;
        assert_eq!(health.status, StatusCode::OK, "{host}");
    }
}

/// A namespace host with no pointer on this gateway is not found.
#[tokio::test]
async fn a_public_namespace_without_a_pointer_is_not_found() {
    let (gateway, _logs, _) = public_gateway().await;
    let answer = request(&gateway, "7.decdn.test", "/a.txt", &[]).await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
    assert!(answer.body.contains("namespace 7"), "{}", answer.body);
}

/// When the capability the gateway holds cannot pay, the client did nothing
/// wrong: 503 with the URI and a retry hint, never 402.
#[tokio::test]
async fn a_capability_that_cannot_pay_answers_503() {
    let (gateway, _logs, _) = public_gateway().await;
    let unfunded = alloy::hex::encode(UNFUNDED);
    let answer = request(&gateway, "decdn.test", &format!("/{unfunded}"), &[]).await;
    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(answer.header(header::RETRY_AFTER), Some("60"));
    assert_eq!(
        answer.header(header::LINK),
        Some(format!("<decdn:{unfunded}>; rel=\"alternate\"").as_str())
    );
    let body: serde_json::Value = serde_json::from_str(&answer.body).unwrap();
    assert_eq!(body["uri"], format!("decdn:{unfunded}"));
}

/// A manifest pull no capability can pay for says nothing of the blob, so the
/// next request, paid by a fresh capability, pulls again.
#[tokio::test]
async fn a_manifest_pull_stopped_for_funding_is_not_remembered() {
    let (gateway, logs, _) = public_gateway().await;
    let host = gateway.sites.site_host(&Site {
        hash: UNFUNDED,
        namespace: None,
    });
    for round in 0..2 {
        if round > 0 {
            hold_capability(&gateway, round);
        }
        let answer = request(&gateway, &host, "/", &[]).await;
        assert_eq!(
            answer.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{}",
            answer.body
        );
    }
    assert_eq!(*logs.caps.lock().unwrap(), [MAX_MANIFEST_MB; 2]);
}

/// An entry's pull is capped at the size its manifest declares, so a
/// manifest that understates an entry cannot make the gateway pay for the
/// rest; a manifest pull and a raw blob, whose sizes nothing declares, run
/// under the gateway's cap alone.
#[tokio::test]
async fn an_entry_pull_is_capped_at_its_declared_size() {
    let (gateway, logs, host) = public_gateway().await;
    let before = logs.size_caps.lock().unwrap().len();
    let answer = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!(
        (answer.status, answer.body.as_str()),
        (StatusCode::OK, "abc")
    );
    let other = alloy::hex::encode(OTHER);
    let raw = request(&gateway, "decdn.test", &format!("/{other}"), &[]).await;
    assert_eq!(raw.status, StatusCode::OK);
    let caps = logs.size_caps.lock().unwrap();
    assert_eq!(
        &caps[before..],
        [None, Some(3), None],
        "the manifest, then the entry at its size, then the raw blob"
    );
}

/// An entry the manifest declares empty is answered empty, with no pull: a
/// declared size of zero buys no uncapped pull, and nothing to deliver.
#[tokio::test]
async fn an_empty_entry_is_served_without_a_pull() {
    let (gateway, logs, host) = public_gateway().await;
    let answer = request(&gateway, &host, "/empty.txt", &[]).await;
    assert_eq!((answer.status, answer.body.as_str()), (StatusCode::OK, ""));
    assert_eq!(answer.header(header::CONTENT_LENGTH), Some("0"));
    assert_eq!(
        answer.header(header::CONTENT_TYPE),
        Some("text/plain; charset=utf-8")
    );
    let head = send(&gateway, Method::HEAD, &host, "/empty.txt", &[]).await;
    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(head.header(header::CONTENT_LENGTH), Some("0"));
    assert_eq!(
        *logs.caps.lock().unwrap(),
        [MAX_MANIFEST_MB],
        "only the manifest was pulled"
    );
}

/// On a public site, a `/_decdn/` request with no `Sec-Fetch-*` headers comes
/// from outside a browser, or from one that sends none, and is taken as a
/// navigation: another bundle's pages never run on this origin, and a blob
/// is a download. The site's own subresource requests are served as before.
#[tokio::test]
async fn a_headerless_request_for_another_bundle_is_a_navigation() {
    let (gateway, _logs, host) = public_gateway().await;
    let blob = alloy::hex::encode(BLOB);
    let page = request(&gateway, &host, &format!("/_decdn/{blob}/a.txt"), &[]).await;
    assert_eq!(page.status, StatusCode::FOUND);
    assert_eq!(
        page.header(header::LOCATION),
        Some(format!("https://{host}/a.txt").as_str())
    );
    let other = alloy::hex::encode(OTHER);
    let download = request(&gateway, &host, &format!("/_decdn/{other}"), &[]).await;
    assert_eq!(download.status, StatusCode::OK);
    assert!(
        download
            .header(header::CONTENT_DISPOSITION)
            .unwrap()
            .starts_with("attachment")
    );
    let sub = request(
        &gateway,
        &host,
        &format!("/_decdn/{blob}/a.txt"),
        SAME_ORIGIN,
    )
    .await;
    assert_eq!((sub.status, sub.body.as_str()), (StatusCode::OK, "abc"));
}

/// `HEAD` resolves a path as `GET` does: percent-encoded names decode, and
/// the root of a bundle without an `index.html` is its listing, a page with
/// no fixed length.
#[tokio::test]
async fn a_public_head_decodes_its_path_and_knows_a_bundle_without_an_index() {
    let (gateway, _logs, _) = public_gateway().await;
    let host = gateway.sites.site_host(&Site {
        hash: NOINDEX,
        namespace: None,
    });
    let entry = send(&gateway, Method::HEAD, &host, "/a%20b.txt", &[]).await;
    assert_eq!(entry.status, StatusCode::OK);
    assert_eq!(entry.header(header::CONTENT_LENGTH), Some("3"));
    let root = send(&gateway, Method::HEAD, &host, "/", &[]).await;
    assert_eq!(root.status, StatusCode::OK);
    assert_eq!(
        root.header(header::CONTENT_TYPE),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(root.header(header::CONTENT_LENGTH), None);
}

/// `Range` → the byte window of a `size`-byte entry: one satisfiable range
/// is served, an unsatisfiable one is refused, and anything else (several
/// ranges, another unit, a malformed or inverted range) is ignored.
#[test]
fn a_range_header_names_one_window_or_none() {
    let window = |header: &str, size: u64| byte_window(header, size);
    assert_eq!(window("bytes=0-99", 1000), Ok(Some((0, 100))));
    assert_eq!(window("bytes=100-", 1000), Ok(Some((100, 900))));
    assert_eq!(window("bytes=-100", 1000), Ok(Some((900, 100))));
    assert_eq!(window("bytes=-5000", 1000), Ok(Some((0, 1000))));
    assert_eq!(window("bytes=0-1999", 1000), Ok(Some((0, 1000))));
    assert_eq!(window("bytes=999-999", 1000), Ok(Some((999, 1))));
    assert_eq!(window(" bytes = 0 - 9 ", 1000), Ok(Some((0, 10))));
    for unsatisfiable in ["bytes=1000-", "bytes=1000-2000", "bytes=-0"] {
        assert_eq!(window(unsatisfiable, 1000), Err(()), "{unsatisfiable}");
    }
    for ignored in [
        "bytes=5-2",
        "bytes=0-0,2-2",
        "bits=0-1",
        "bytes=",
        "bytes=a-b",
        "0-1",
    ] {
        assert_eq!(window(ignored, 1000), Ok(None), "{ignored}");
    }
}

/// A range on a sized entry is served as 206 over exactly that window: the
/// pull carries the window, the response its length and position, and the
/// entry's size still caps the pull.
#[tokio::test]
async fn a_range_on_an_entry_answers_206_with_the_window() {
    let (gateway, logs, host) = public_gateway().await;
    let before = logs.ranges.lock().unwrap().len();
    let answer = request(&gateway, &host, "/a.txt", &[("range", "bytes=1-2")]).await;
    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(answer.header(header::CONTENT_RANGE), Some("bytes 1-2/3"));
    assert_eq!(answer.header(header::CONTENT_LENGTH), Some("2"));
    assert_eq!(answer.header(header::ACCEPT_RANGES), Some("bytes"));
    let ranges = logs.ranges.lock().unwrap();
    assert_eq!(
        &ranges[before..],
        [None, Some((1, 2))],
        "the manifest, then the window"
    );
    let caps = logs.size_caps.lock().unwrap();
    assert_eq!(
        caps.last().copied().flatten(),
        Some(3),
        "the entry's size still caps the pull"
    );
}

/// A range past the end is refused with the entry's size and no pull.
#[tokio::test]
async fn an_unsatisfiable_range_answers_416() {
    let (gateway, logs, host) = public_gateway().await;
    let answer = request(&gateway, &host, "/a.txt", &[("range", "bytes=3-")]).await;
    assert_eq!(answer.status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(answer.header(header::CONTENT_RANGE), Some("bytes */3"));
    assert_eq!(
        *logs.caps.lock().unwrap(),
        [MAX_MANIFEST_MB],
        "only the manifest was pulled"
    );
}

/// A range the gateway cannot honour is ignored, as HTTP allows: a malformed
/// or multi-part range, a response that is rewritten (its length changes),
/// and a blob whose size no proof proved all get the whole body as 200.
#[tokio::test]
async fn a_range_is_ignored_where_it_cannot_be_served() {
    let (gateway, logs, host) = public_gateway().await;
    let before = logs.ranges.lock().unwrap().len();
    for bad in ["bytes=x", "bytes=0-0,2-2"] {
        let answer = request(&gateway, &host, "/a.txt", &[("range", bad)]).await;
        assert_eq!(
            (answer.status, answer.body.as_str()),
            (StatusCode::OK, "abc"),
            "{bad}"
        );
    }
    let page = request(&gateway, &host, "/", &[("range", "bytes=0-1")]).await;
    assert_eq!(
        page.status,
        StatusCode::OK,
        "a rewritten page has no fixed length"
    );
    assert_eq!(page.header(header::ACCEPT_RANGES), None);
    let other = alloy::hex::encode(OTHER);
    let raw = request(
        &gateway,
        "decdn.test",
        &format!("/{other}"),
        &[("range", "bytes=0-1")],
    )
    .await;
    assert_eq!(
        (raw.status, raw.body.as_str()),
        (StatusCode::OK, "other bytes")
    );
    assert_eq!(raw.header(header::ACCEPT_RANGES), None);
    let ranges = logs.ranges.lock().unwrap();
    assert!(
        ranges[before..].iter().all(Option::is_none),
        "{:?}",
        &ranges[before..]
    );
}

/// A sized entry advertises ranges on `GET` and `HEAD`; a response without a
/// fixed length does not, so a resuming client never sends a range the
/// gateway would answer whole.
#[tokio::test]
async fn accept_ranges_is_advertised_for_a_sized_entry_only() {
    let (gateway, _logs, host) = public_gateway().await;
    let whole = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!(whole.header(header::ACCEPT_RANGES), Some("bytes"));
    let head = send(&gateway, Method::HEAD, &host, "/a.txt", &[]).await;
    assert_eq!(head.header(header::ACCEPT_RANGES), Some("bytes"));
    let page = send(&gateway, Method::HEAD, &host, "/", &[]).await;
    assert_eq!(page.header(header::ACCEPT_RANGES), None);
}

/// [`public_gateway_sized`] whose earlier pulls verified [`OTHER`] and
/// [`BIG`] at their lengths.
async fn sized_gateway() -> (Arc<Gateway>, Logs, String) {
    let sizes = HashMap::from([(OTHER, 11), (BIG, u64::try_from(BIG_LEN).unwrap())]);
    public_gateway_sized(sizes).await
}

/// A raw blob whose size a pull verified answers with its length, on `HEAD`
/// with no pull and on `GET` with the pull capped at that size.
#[tokio::test]
async fn a_raw_blob_of_known_size_answers_with_its_length() {
    let (gateway, logs, _) = sized_gateway().await;
    let other = alloy::hex::encode(OTHER);
    let head = send(
        &gateway,
        Method::HEAD,
        "decdn.test",
        &format!("/{other}"),
        &[],
    )
    .await;
    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(head.header(header::CONTENT_LENGTH), Some("11"));
    assert_eq!(head.header(header::ACCEPT_RANGES), Some("bytes"));
    assert_eq!(
        head.header(header::ETAG),
        Some(format!("\"{other}\"").as_str())
    );
    assert!(logs.caps.lock().unwrap().is_empty(), "HEAD pulls nothing");

    let whole = request(&gateway, "decdn.test", &format!("/{other}"), &[]).await;
    assert_eq!(
        (whole.status, whole.body.as_str()),
        (StatusCode::OK, "other bytes")
    );
    assert_eq!(whole.header(header::CONTENT_LENGTH), Some("11"));
    assert_eq!(whole.header(header::ACCEPT_RANGES), Some("bytes"));
    assert_eq!(*logs.size_caps.lock().unwrap(), [Some(11)]);
    assert!(
        logs.proofs.lock().unwrap().is_empty(),
        "a verified size needs no proof"
    );
}

/// A raw blob whose proven size passes `--max-blob-mb` answers 413 before
/// any pull.
#[tokio::test]
async fn a_raw_blob_past_the_blob_cap_answers_413_with_no_pull() {
    let (gateway, logs, _) = public_gateway_flagged(
        HashMap::from([(OTHER, 3 << 20)]),
        HashMap::new(),
        flags(&["--max-blob-mb", "2"]),
    )
    .await;
    let path = format!("/{}", alloy::hex::encode(OTHER));
    let answer = request(&gateway, "decdn.test", &path, &[]).await;
    assert_eq!(answer.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(logs.caps.lock().unwrap().is_empty(), "no pull runs");
    // A `Range` inside the cap pulls only its span, so it is served.
    let window = request(&gateway, "decdn.test", &path, &[("range", "bytes=0-3")]).await;
    assert_ne!(window.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(*logs.ranges.lock().unwrap(), [Some((0, 4))]);
}

/// A bundle entry whose declared size passes `--max-blob-mb` answers 413 as a
/// raw blob does: the manifest pulls, the entry does not.
#[tokio::test]
async fn an_entry_past_the_blob_cap_answers_413_with_no_entry_pull() {
    let manifest = serde_json::json!({"version": 1, "entries": [
        {"path": "huge.bin", "hash": alloy::hex::encode(ENTRY), "size": 3 << 20},
    ]})
    .to_string()
    .into_bytes();
    let (gateway, caps) = recording_gateway(Reply::Bytes(manifest), &["--max-blob-mb", "2"]).await;
    let (status, body) = get(&gateway, &format!("/{}/huge.bin", alloy::hex::encode(BLOB))).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(caps.lock().unwrap().len(), 1, "only the manifest pulls");
}

/// A raw blob of verified size takes one `Range`, as a sized entry does: 206
/// over the window, and 416 with the size for a window past the end, with
/// no pull.
#[tokio::test]
async fn a_range_on_a_raw_blob_of_known_size_answers_206() {
    let (gateway, logs, _) = sized_gateway().await;
    let other = alloy::hex::encode(OTHER);
    let window = request(
        &gateway,
        "decdn.test",
        &format!("/{other}"),
        &[("range", "bytes=6-9")],
    )
    .await;
    assert_eq!(
        (window.status, window.body.as_str()),
        (StatusCode::PARTIAL_CONTENT, "byte")
    );
    assert_eq!(window.header(header::CONTENT_RANGE), Some("bytes 6-9/11"));
    assert_eq!(window.header(header::CONTENT_LENGTH), Some("4"));
    assert_eq!(*logs.ranges.lock().unwrap(), [Some((6, 4))]);

    let past = request(
        &gateway,
        "decdn.test",
        &format!("/{other}"),
        &[("range", "bytes=11-")],
    )
    .await;
    assert_eq!(past.status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(past.header(header::CONTENT_RANGE), Some("bytes */11"));
    assert_eq!(logs.ranges.lock().unwrap().len(), 1, "a 416 pulls nothing");
}

/// `HEAD` under a site's `/_decdn/` answers for a raw blob as the bare host
/// does, and redirects a bundle entry to its site, instead of reading the
/// path as one of the site's own entries.
#[tokio::test]
async fn a_site_answers_head_under_decdn() {
    let (gateway, logs, host) = sized_gateway().await;
    let other = alloy::hex::encode(OTHER);
    let raw = send(
        &gateway,
        Method::HEAD,
        &host,
        &format!("/_decdn/{other}"),
        &[],
    )
    .await;
    assert_eq!(raw.status, StatusCode::OK);
    assert_eq!(raw.header(header::CONTENT_LENGTH), Some("11"));
    let blob = alloy::hex::encode(BLOB);
    let bundle = send(
        &gateway,
        Method::HEAD,
        &host,
        &format!("/_decdn/{blob}/a.txt"),
        &[],
    )
    .await;
    assert!(bundle.status.is_redirection(), "{:?}", bundle.status);
    assert!(logs.caps.lock().unwrap().is_empty(), "HEAD pulls nothing");
}

/// `HEAD` of a directory under a site's `/_decdn/` redirects to the
/// directory on the bundle's site, at no pull.
#[tokio::test]
async fn a_site_answers_head_for_a_directory_under_decdn() {
    let (gateway, logs, host) = public_gateway().await;
    let blob = alloy::hex::encode(BLOB);
    let docs = send(
        &gateway,
        Method::HEAD,
        &host,
        &format!("/_decdn/{blob}/docs/"),
        &[],
    )
    .await;
    assert_eq!(docs.status, StatusCode::FOUND, "{}", docs.body);
    assert_eq!(
        docs.header(header::LOCATION),
        Some(format!("https://{host}/docs/").as_str())
    );
    assert!(logs.caps.lock().unwrap().is_empty(), "HEAD pulls nothing");
}

/// The empty root is empty, with no pull, on `GET` and `HEAD`.
#[tokio::test]
async fn the_empty_root_is_empty_with_no_pull() {
    let (gateway, logs, _) = public_gateway().await;
    let empty = alloy::hex::encode(decdn_protocol::EMPTY_ROOT);
    let answer = request(&gateway, "decdn.test", &format!("/{empty}"), &[]).await;
    assert_eq!((answer.status, answer.body.as_str()), (StatusCode::OK, ""));
    assert_eq!(answer.header(header::CONTENT_LENGTH), Some("0"));
    let head = send(
        &gateway,
        Method::HEAD,
        "decdn.test",
        &format!("/{empty}"),
        &[],
    )
    .await;
    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(head.header(header::CONTENT_LENGTH), Some("0"));
    assert!(logs.caps.lock().unwrap().is_empty(), "nothing was pulled");
    assert!(logs.proofs.lock().unwrap().is_empty(), "nothing was proved");
}

/// [`public_gateway_proving`] whose size proofs prove [`OTHER`] and [`BIG`]
/// at their lengths.
async fn proving_gateway() -> (Arc<Gateway>, Logs, String) {
    let proves = HashMap::from([(OTHER, 11), (BIG, u64::try_from(BIG_LEN).unwrap())]);
    public_gateway_proving(HashMap::new(), proves).await
}

/// The first request for a raw blob proves its size before it answers, and
/// answers with that length: one proof, then one pull capped at the size. A
/// repeat request finds the size and proves nothing.
#[tokio::test]
async fn a_cold_raw_blob_answers_with_its_proven_length() {
    let (gateway, logs, _) = proving_gateway().await;
    let other = alloy::hex::encode(OTHER);
    for _ in 0..2 {
        let whole = request(&gateway, "decdn.test", &format!("/{other}"), &[]).await;
        assert_eq!(
            (whole.status, whole.body.as_str()),
            (StatusCode::OK, "other bytes")
        );
        assert_eq!(whole.header(header::CONTENT_LENGTH), Some("11"));
        assert_eq!(whole.header(header::ACCEPT_RANGES), Some("bytes"));
    }
    assert_eq!(*logs.proofs.lock().unwrap(), [OTHER]);
    assert_eq!(*logs.size_caps.lock().unwrap(), [Some(11), Some(11)]);
}

/// A `Range` on a raw blob no pull verified is served once a proof proves
/// its size: 206 over the window, and 416 for a window past the proven end,
/// with no pull.
#[tokio::test]
async fn a_range_on_a_cold_raw_blob_answers_206() {
    let (gateway, logs, _) = proving_gateway().await;
    let other = alloy::hex::encode(OTHER);
    let window = request(
        &gateway,
        "decdn.test",
        &format!("/{other}"),
        &[("range", "bytes=6-9")],
    )
    .await;
    assert_eq!(
        (window.status, window.body.as_str()),
        (StatusCode::PARTIAL_CONTENT, "byte")
    );
    assert_eq!(window.header(header::CONTENT_RANGE), Some("bytes 6-9/11"));
    assert_eq!(*logs.ranges.lock().unwrap(), [Some((6, 4))]);

    let (gateway, logs, _) = proving_gateway().await;
    let past = request(
        &gateway,
        "decdn.test",
        &format!("/{other}"),
        &[("range", "bytes=11-")],
    )
    .await;
    assert_eq!(past.status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(past.header(header::CONTENT_RANGE), Some("bytes */11"));
    assert_eq!(*logs.proofs.lock().unwrap(), [OTHER]);
    assert!(logs.caps.lock().unwrap().is_empty(), "a 416 pulls nothing");
}

/// `HEAD` of a raw blob no pull verified learns its size from a proof alone,
/// and answers with that length and no pull. A site answers the same under
/// `/_decdn/`.
#[tokio::test]
async fn a_cold_head_learns_its_size_from_a_proof() {
    let (gateway, logs, host) = proving_gateway().await;
    let other = alloy::hex::encode(OTHER);
    let head = send(
        &gateway,
        Method::HEAD,
        "decdn.test",
        &format!("/{other}"),
        &[],
    )
    .await;
    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(head.header(header::CONTENT_LENGTH), Some("11"));
    assert_eq!(head.header(header::ACCEPT_RANGES), Some("bytes"));
    assert_eq!(*logs.proofs.lock().unwrap(), [OTHER]);
    assert!(logs.caps.lock().unwrap().is_empty(), "HEAD pulls nothing");

    let big = alloy::hex::encode(BIG);
    let site = send(
        &gateway,
        Method::HEAD,
        &host,
        &format!("/_decdn/{big}"),
        &[],
    )
    .await;
    assert_eq!(site.status, StatusCode::OK);
    assert_eq!(
        site.header(header::CONTENT_LENGTH),
        Some(BIG_LEN.to_string().as_str())
    );
}

/// A blob no node holds answers as its pull would, after a proof that found
/// no holder: no second attempt.
#[tokio::test]
async fn a_blob_no_node_holds_is_not_pulled_after_its_proof() {
    let (gateway, logs, _) = proving_gateway().await;
    let missing = alloy::hex::encode([0x99; 32]);
    let answer = request(&gateway, "decdn.test", &format!("/{missing}"), &[]).await;
    assert_eq!(answer.status, StatusCode::BAD_GATEWAY);
    assert_eq!(*logs.proofs.lock().unwrap(), [[0x99; 32]]);
    assert!(
        logs.caps.lock().unwrap().is_empty(),
        "no pull after the proof"
    );
}

/// A blob whose pull stops for funding stops at its proof, as its pull
/// would: 503, and nothing pulled.
#[tokio::test]
async fn a_proof_that_cannot_be_paid_answers_503() {
    let (gateway, logs, _) = proving_gateway().await;
    let unfunded = alloy::hex::encode(UNFUNDED);
    let answer = request(&gateway, "decdn.test", &format!("/{unfunded}"), &[]).await;
    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(*logs.proofs.lock().unwrap(), [UNFUNDED]);
    assert!(logs.caps.lock().unwrap().is_empty(), "nothing was pulled");
}

/// The metrics of the gateway under test.
fn metrics(gateway: &Gateway) -> String {
    gateway.metrics.render()
}

/// A loopback gateway counts its requests too: `/_status` reads the same
/// counters on every gateway. A content request with no capability that can
/// pay counts as payment required.
#[tokio::test]
async fn a_loopback_gateway_counts_its_requests() {
    let gateway = test_gateway().await;
    call(&gateway, Method::GET, "/nope", &[], "").await;
    call(&gateway, Method::DELETE, "/", &[], "").await;
    let (status, body) = get(&gateway, &format!("/{}", alloy::hex::encode(BLOB))).await;
    assert_eq!(status, StatusCode::PAYMENT_REQUIRED, "{body}");
    assert!(body.contains("PUT /_capability"), "{body}");
    let c = gateway.metrics.snapshot();
    let answered = c.requests_ok
        + c.requests_not_found
        + c.requests_refused
        + c.requests_payment_required
        + c.pull_failures_funding
        + c.pull_failures_other;
    assert_eq!(answered, 3, "{c:?}");
    assert_eq!(c.requests_payment_required, 1, "{c:?}");
}

/// Every answer counts under the sibling its outcome names, and the bytes a
/// response delivers count as they go out.
#[tokio::test]
async fn requests_are_counted_by_outcome() {
    let (gateway, _, host) = public_gateway().await;
    // The miss first: it reads the manifest, which the entry then finds
    // cached, so the entry's bytes are the only ones served.
    let missing = request(&gateway, &host, "/nope.txt", &[]).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    let ok = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!(ok.status, StatusCode::OK);
    let refused = request(&gateway, "evil.test", "/", &[]).await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    let text = metrics(&gateway);
    for line in [
        "decdn_gateway_requests_ok_total 1",
        "decdn_gateway_requests_not_found_total 1",
        "decdn_gateway_requests_payment_required_total 0",
        "decdn_gateway_requests_refused_total 1",
        "decdn_gateway_pulls_active 0",
    ] {
        assert!(text.contains(&format!("\n{line}\n")), "{line}: {text}");
    }
    let served = text
        .lines()
        .find_map(|l| l.strip_prefix("decdn_gateway_bytes_served_total "))
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap();
    assert_eq!(
        served, 3,
        "the entry's bytes; the manifest is not a response"
    );
}

/// A capability that cannot pay and an upstream fault count as pull failures.
#[tokio::test]
async fn pull_failures_are_counted_by_cause() {
    let (gateway, _logs, _) = public_gateway().await;
    // The upstream fault first: the funding stop retires the one capability.
    let unknown = alloy::hex::encode([0x11; 32]);
    let answer = request(&gateway, "decdn.test", &format!("/{unknown}"), &[]).await;
    assert_eq!(answer.status, StatusCode::BAD_GATEWAY);
    let unfunded = alloy::hex::encode(UNFUNDED);
    let answer = request(&gateway, "decdn.test", &format!("/{unfunded}"), &[]).await;
    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    let text = metrics(&gateway);
    assert!(
        text.contains("\ndecdn_gateway_pull_failures_funding_total 1\n"),
        "{text}"
    );
    assert!(
        text.contains("\ndecdn_gateway_pull_failures_other_total 1\n"),
        "{text}"
    );
}

/// The metrics listener answers `GET /metrics` with the exposition and
/// nothing else.
#[tokio::test]
async fn the_metrics_listener_serves_the_exposition() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let (gateway, _logs, _) = public_gateway().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_metrics(Arc::clone(&gateway), listener));
    let fetch_as = |host: String, path: &'static str| async move {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut out = String::new();
        stream.read_to_string(&mut out).await.unwrap();
        out
    };
    let fetch = |path: &'static str| fetch_as(addr.to_string(), path);
    let exposition = fetch("/metrics").await;
    assert!(exposition.starts_with("HTTP/1.1 200"), "{exposition}");
    assert!(
        exposition.contains("text/plain; version=0.0.4"),
        "{exposition}"
    );
    assert!(
        exposition.contains("decdn_gateway_bytes_served_total 0"),
        "{exposition}"
    );
    // The state answers here; no page does.
    for path in ["/", "/_dashboard/app.js"] {
        let page = fetch(path).await;
        assert!(page.starts_with("HTTP/1.1 404"), "{path}: {page}");
    }
    let status = fetch("/_status").await;
    assert!(status.starts_with("HTTP/1.1 200"), "{status}");
    let body = status.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    let status: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(status["mode"], "public");
    assert!(status["pointers"].is_array(), "{status}");
    let other = fetch("/nope").await;
    assert!(other.starts_with("HTTP/1.1 404"), "{other}");
    // A page whose name was rebound to the listener's address reads nothing.
    for path in ["/_status", "/metrics", "/"] {
        let rebound = fetch_as(format!("evil.example:{}", addr.port()), path).await;
        assert!(rebound.starts_with("HTTP/1.1 403"), "{path}: {rebound}");
    }
    let named = fetch_as(format!("localhost:{}", addr.port()), "/metrics").await;
    assert!(named.starts_with("HTTP/1.1 200"), "{named}");
    // A page the operator visits reaches this address with its own Host, so
    // only the missing CORS grant keeps it from reading the state.
    for path in ["/_status", "/metrics"] {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                format!(
                    "GET {path} HTTP/1.1\r\nHost: {addr}\r\nOrigin: https://evil.example\r\n\
                     Connection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut out = String::new();
        stream.read_to_string(&mut out).await.unwrap();
        assert!(out.starts_with("HTTP/1.1 200"), "{path}: {out}");
        assert!(
            !out.to_ascii_lowercase()
                .contains("access-control-allow-origin"),
            "{path} is readable cross-origin: {out}"
        );
    }
    // The operator's own requests are not what the gateway served.
    assert_eq!(gateway.metrics.snapshot(), metrics::Counters::default());
}

/// The headers a cross-origin page may read from a public gateway.
const READABLE: &str = "Content-Length, Content-Encoding, Content-Range, Accept-Ranges, \
                        Content-Type, Content-Disposition, ETag, Retry-After, Link";

/// Whether `answer` is readable by any origin.
fn readable_anywhere(answer: &Answer) -> bool {
    answer.header(header::ACCESS_CONTROL_ALLOW_ORIGIN) == Some("*")
        && answer.header(header::ACCESS_CONTROL_EXPOSE_HEADERS) == Some(READABLE)
}

/// A public gateway answers a content preflight on every content host: a
/// page on any origin may `GET` or `HEAD` with a `Range`.
#[tokio::test]
async fn a_public_preflight_admits_ranged_reads_from_any_origin() {
    let (gateway, logs, host) = public_gateway().await;
    let preflight = [
        ("origin", "https://app.example"),
        ("access-control-request-method", "GET"),
        ("access-control-request-headers", "range"),
    ];
    let other = format!("/{}", alloy::hex::encode(OTHER));
    for (on, path) in [
        ("decdn.test", other.as_str()),
        (host.as_str(), "/a.txt"),
        ("7.decdn.test", "/a.txt"),
    ] {
        let answer = send(&gateway, Method::OPTIONS, on, path, &preflight).await;
        assert_eq!(answer.status, StatusCode::NO_CONTENT, "{on}{path}");
        assert!(readable_anywhere(&answer), "{on}{path}");
        assert_eq!(
            answer.header(header::ACCESS_CONTROL_ALLOW_METHODS),
            Some("GET, HEAD")
        );
        assert_eq!(
            answer.header(header::ACCESS_CONTROL_ALLOW_HEADERS),
            Some("range")
        );
        assert_eq!(answer.header(header::ACCESS_CONTROL_MAX_AGE), Some("600"));
    }
    let unknown = send(&gateway, Method::OPTIONS, "evil.test", "/", &preflight).await;
    assert_eq!(unknown.status, StatusCode::FORBIDDEN);
    assert!(
        logs.caps.lock().unwrap().is_empty(),
        "a preflight pulls nothing"
    );
}

/// A browser publisher may `PUT /_pointer` with a JSON body from any origin,
/// on the bare host, where the gateway takes it. A site or namespace host
/// answers the `PUT` with 404, and so its preflight too.
#[tokio::test]
async fn a_public_pointer_preflight_admits_a_json_put_on_the_bare_host() {
    let (gateway, _logs, host) = public_gateway().await;
    let answer = send(&gateway, Method::OPTIONS, "decdn.test", "/_pointer", &[]).await;
    assert_eq!(answer.status, StatusCode::NO_CONTENT);
    assert!(readable_anywhere(&answer));
    assert_eq!(
        answer.header(header::ACCESS_CONTROL_ALLOW_METHODS),
        Some("PUT")
    );
    assert_eq!(
        answer.header(header::ACCESS_CONTROL_ALLOW_HEADERS),
        Some("content-type")
    );
    assert_eq!(answer.header(header::ACCESS_CONTROL_MAX_AGE), Some("600"));
    for off in [host.as_str(), "7.decdn.test"] {
        let answer = send(&gateway, Method::OPTIONS, off, "/_pointer", &[]).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{off}");
        assert!(readable_anywhere(&answer), "{off}");
    }
    let refused = send_body(&gateway, Method::PUT, "decdn.test", "/_pointer", "{}").await;
    assert!(refused.status.is_client_error(), "{}", refused.status);
    assert!(readable_anywhere(&refused), "a refused PUT is readable too");
}

/// Every answer of a public gateway is readable cross-origin: content on the
/// bare, site and namespace hosts, a window, `HEAD`, a redirect, and the
/// 416, 404 and 503 refusals.
#[tokio::test]
async fn every_public_answer_is_readable_by_any_origin() {
    let (gateway, _logs, host) = public_gateway().await;
    hold(&gateway, unsigned_pointer(7, BLOB, 1), Address::ZERO);
    let origin = [("origin", "https://app.example")];
    let unfunded = format!("/{}", alloy::hex::encode(UNFUNDED));
    let blob = format!("/{}", alloy::hex::encode(OTHER));
    let bundle = format!("/{}/", alloy::hex::encode(BLOB));
    let window = [("origin", "https://app.example"), ("range", "bytes=1-2")];
    let past = [("origin", "https://app.example"), ("range", "bytes=3-")];
    // The funding stop comes last: it retires the one capability.
    let answers = [
        request(&gateway, &host, "/a.txt", &origin).await,
        request(&gateway, &host, "/a.txt", &window).await,
        request(&gateway, &host, "/a.txt", &past).await,
        request(&gateway, &host, "/missing.txt", &origin).await,
        send(&gateway, Method::HEAD, &host, "/a.txt", &origin).await,
        send(&gateway, Method::HEAD, "decdn.test", &blob, &origin).await,
        request(&gateway, "decdn.test", &bundle, &origin).await,
        request(&gateway, "7.decdn.test", "/a.txt", &origin).await,
        request(&gateway, "8.decdn.test", "/a.txt", &origin).await,
        request(&gateway, "decdn.test", &unfunded, &origin).await,
    ];
    let statuses: Vec<_> = answers.iter().map(|a| a.status).collect();
    assert_eq!(
        statuses,
        [
            StatusCode::OK,
            StatusCode::PARTIAL_CONTENT,
            StatusCode::RANGE_NOT_SATISFIABLE,
            StatusCode::NOT_FOUND,
            StatusCode::OK,
            StatusCode::OK,
            StatusCode::FOUND,
            StatusCode::OK,
            StatusCode::NOT_FOUND,
            StatusCode::SERVICE_UNAVAILABLE,
        ]
    );
    for answer in &answers {
        assert!(readable_anywhere(answer), "{}", answer.status);
        assert_eq!(answer.header(header::VARY), None, "`*` needs no Vary");
    }
}

/// A loopback gateway keeps content same-origin: a request from a page on
/// another origin is refused, with no CORS header and no preflight.
#[tokio::test]
async fn a_loopback_gateway_answers_content_to_no_other_origin() {
    let (gateway, host) = site_gateway().await;
    let mut headers = SAME_ORIGIN.to_vec();
    headers.push(("origin", "https://app.example"));
    let site = request(&gateway, &host, "/css/site.css", &headers).await;
    assert_eq!(site.status, StatusCode::FORBIDDEN);
    assert_eq!(site.header(header::ACCESS_CONTROL_ALLOW_ORIGIN), None);
    let preflight = [
        ("origin", "https://app.example"),
        ("access-control-request-headers", "range"),
    ];
    let other = format!("/{}", alloy::hex::encode(OTHER));
    let bare = send(
        &gateway,
        Method::OPTIONS,
        "127.0.0.1:8484",
        &other,
        &preflight,
    )
    .await;
    assert_eq!(bare.status, StatusCode::FORBIDDEN);
    assert_eq!(bare.header(header::ACCESS_CONTROL_ALLOW_ORIGIN), None);
}

/// Each request a loopback gateway refuses from a browser, with its answer.
const BROWSER_REFUSALS: &[(&[(&str, &str)], &str)] = &[
    (
        &[
            ("sec-fetch-site", "cross-site"),
            ("sec-fetch-mode", "no-cors"),
        ],
        "a page on another site may not use the gateway\n",
    ),
    (
        &[
            ("sec-fetch-site", "same-site"),
            ("origin", "http://evil.example"),
        ],
        "a page on another origin may not use the gateway\n",
    ),
    (
        &[("user-agent", "Mozilla/5.0 (X11; Linux x86_64)")],
        "a browser that sends no Sec-Fetch-Site may not use the gateway\n",
    ),
    (
        &[("origin", "http://127.0.0.1:8484")],
        "a browser that sends no Sec-Fetch-Site may not use the gateway\n",
    ),
];

/// A page on another site or origin, and a browser that sends no
/// `Sec-Fetch-Site`, reach neither content on the bare host nor the API.
#[tokio::test]
async fn the_browser_gate_refuses_other_pages_on_content_and_the_api() {
    let (gateway, _) = site_gateway().await;
    let content = format!("/{}", alloy::hex::encode(OTHER));
    for path in [content.as_str(), "/_capability"] {
        for (headers, why) in BROWSER_REFUSALS {
            let answer = request(&gateway, "127.0.0.1:8484", path, headers).await;
            assert_eq!(
                (answer.status, answer.body.as_str()),
                (StatusCode::FORBIDDEN, *why),
                "{path} {headers:?}"
            );
        }
    }
}

/// A page on the gateway's own origin, the bare host or a site host, uses
/// the gateway, and so does a client outside a browser.
#[tokio::test]
async fn the_browser_gate_passes_own_origins_and_clients_outside_a_browser() {
    let (gateway, host) = site_gateway().await;
    let site_origin = format!("http://{host}");
    let browser = "Mozilla/5.0 (X11; Linux x86_64)";
    let content = format!("/{}", alloy::hex::encode(OTHER));
    for headers in [
        vec![
            ("sec-fetch-site", "same-origin"),
            ("sec-fetch-mode", "cors"),
            ("origin", "http://127.0.0.1:8484"),
            ("user-agent", browser),
        ],
        vec![
            ("sec-fetch-site", "same-site"),
            ("sec-fetch-mode", "cors"),
            ("origin", site_origin.as_str()),
            ("user-agent", browser),
        ],
        vec![],
        vec![("user-agent", "curl/8.0")],
    ] {
        let answer = request(&gateway, "127.0.0.1:8484", &content, &headers).await;
        assert_eq!(
            (answer.status, answer.body.as_str()),
            (StatusCode::OK, "other bytes"),
            "{headers:?}"
        );
        let answer = request(&gateway, "127.0.0.1:8484", "/_capability", &headers).await;
        assert_eq!(
            answer.status,
            StatusCode::OK,
            "{headers:?}: {}",
            answer.body
        );
    }
}

/// `GET /_capability` answers this exact body for one registered capability:
/// the SDKs read it, so its keys and spelling are the contract.
#[tokio::test]
async fn the_capability_list_keeps_its_shape() {
    let gateway = test_gateway().await;
    let signer = {
        let mut delegates = gateway.delegates();
        let signer = delegates.pending();
        let mut held = grant(signer, 0x01, 1_900_000_000);
        held.spending_cap = 5_000;
        delegates.register(held.clone(), TEST_POOL, None).unwrap();
        delegates.retire(signer, TEST_POOL, held.pool_id);
        signer
    };
    let answer = send(&gateway, Method::GET, "127.0.0.1:8484", "/_capability", &[]).await;
    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(
        answer.body,
        format!(
            "[{{\"cap\":5000,\"expiry\":1900000000,\"pool\":\"0x{}\",\"signer\":\"{signer}\",\
             \"spent\":true}}]",
            "01".repeat(32),
        )
    );
}

/// A pull in flight is listed with its hash and payer until the worker's pull
/// ends, which is when its slot frees. A dropped request leaves its pull
/// listed while the worker still runs it.
#[tokio::test]
async fn an_active_pull_is_listed_until_the_worker_ends_it() {
    let notice = Arc::new(tokio::sync::Notify::new());
    let replies = HashMap::from([(HELD, Reply::FailOn(Arc::clone(&notice)))]);
    let (gateway, _caps, _pointers) = public_gateway_replying(replies).await;
    assert!(gateway.active_pulls().is_empty());
    let held = alloy::hex::encode(HELD);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/{held}"))
        .header(header::HOST, "decdn.test")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let pending = tokio::spawn(Arc::clone(&gateway).handle(req));
    let listed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let active = gateway.active_pulls();
            if !active.is_empty() {
                return active;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the pull is listed while it runs");
    assert_eq!(listed.len(), 1);
    let pull = &listed[0];
    assert_eq!(pull.hash, held);
    assert_eq!(pull.namespace, None);
    assert_eq!(pull.range, None);
    assert_eq!(
        pull.payer.pool,
        alloy::primitives::B256::repeat_byte(0),
        "{:?}",
        pull.payer
    );
    assert_eq!(pull.delivered_bytes, 0);
    pending.abort();
    let _ = pending.await;
    assert_eq!(
        gateway.active_pulls().len(),
        1,
        "the worker still runs the dropped request's pull"
    );
    assert_eq!(gateway.metrics.snapshot().pulls_active, 1);
    notice.notify_one();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !gateway.active_pulls().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the pull leaves the list when the worker's pull ends");
    assert_eq!(gateway.metrics.snapshot().pulls_active, 0);
}

/// `GET path` on the loopback host, with no browser headers.
async fn loopback_get(gateway: &Arc<Gateway>, path: &str) -> Answer {
    send(gateway, Method::GET, "127.0.0.1:8484", path, &[]).await
}

/// `/_status` on a loopback gateway lists the capabilities `/_capability`
/// lists. A loopback gateway holds no pointers, so they are null.
#[tokio::test]
async fn status_on_loopback_lists_the_capabilities() {
    let gateway = test_gateway().await;
    hold_capability(&gateway, 0);
    let answer = loopback_get(&gateway, "/_status").await;
    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(
        answer.header(header::CONTENT_TYPE),
        Some("application/json")
    );
    let status: serde_json::Value = serde_json::from_str(&answer.body).unwrap();
    assert_eq!(status["mode"], "loopback");
    assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
    assert!(status.get("dashboard_built").is_none(), "{status}");
    assert_eq!(status["listen"], "127.0.0.1:8484");
    assert_eq!(status["chain_id"], 421_614);
    assert!(status["pointers"].is_null(), "{status}");
    assert!(status["pool_error"].is_null(), "{status}");
    assert!(status["pools"].is_object(), "{status}");
    let listed = loopback_get(&gateway, "/_capability").await;
    let listed: serde_json::Value = serde_json::from_str(&listed.body).unwrap();
    assert_eq!(status["capabilities"], listed);
    assert_eq!(listed.as_array().map(Vec::len), Some(1), "{listed}");
    // A page cannot read it cross-origin: no preflight passes.
    let preflight = send(
        &gateway,
        Method::OPTIONS,
        "127.0.0.1:8484",
        "/_status",
        &[(header::ORIGIN.as_str(), "https://evil.example")],
    )
    .await;
    assert_eq!(preflight.status, StatusCode::FORBIDDEN);
}

/// A loopback gateway serves no page: its `/` is the stub that names
/// `decdn dashboard`, and the dashboard's asset paths are no route.
#[tokio::test]
async fn a_loopback_gateway_serves_no_dashboard() {
    let gateway = test_gateway().await;
    let root = loopback_get(&gateway, "/").await;
    assert_eq!(root.status, StatusCode::OK);
    assert!(root.body.contains("decdn dashboard"), "{}", root.body);
    assert!(root.header(header::CONTENT_SECURITY_POLICY).is_none());
    for path in ["/_dashboard/app.js", "/_dashboard/app.css"] {
        let asset = loopback_get(&gateway, path).await;
        assert_ne!(asset.status, StatusCode::OK, "{path}");
    }
}

/// A public gateway answers its state on its bare host, readable by any
/// origin, and on no site host. `/` keeps its stub.
#[tokio::test]
async fn status_answers_on_a_public_bare_host_only() {
    let (gateway, _logs, host) = public_gateway().await;
    let answer = request(&gateway, "decdn.test", "/_status", &[]).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let status: serde_json::Value = serde_json::from_str(&answer.body).unwrap();
    assert_eq!(status["mode"], "public");
    let preflight = send(&gateway, Method::OPTIONS, "decdn.test", "/_status", &[]).await;
    assert_eq!(preflight.status, StatusCode::NO_CONTENT);
    let site = request(&gateway, &host, "/_status", &[]).await;
    assert_eq!(site.status, StatusCode::NOT_FOUND);
    let root = request(&gateway, "decdn.test", "/", &[]).await;
    assert_eq!(root.status, StatusCode::OK);
    assert!(
        !root.body.contains("decdn-dashboard-fallback"),
        "{}",
        root.body
    );
    assert!(root.header(header::CONTENT_SECURITY_POLICY).is_none());
}

/// A public gateway's status lists each pointer it holds.
#[tokio::test]
async fn status_lists_a_stored_pointer() {
    let (gateway, _logs, _) = public_gateway().await;
    let owner = Address::repeat_byte(0x77);
    hold(&gateway, unsigned_pointer(7, BLOB, 3), owner);
    let status = serde_json::to_value(gateway.status().await).unwrap();
    assert_eq!(status["mode"], "public");
    let pointers = status["pointers"].as_array().unwrap();
    assert_eq!(pointers.len(), 1);
    assert_eq!(pointers[0]["namespace"], 7);
    assert_eq!(pointers[0]["seq"], 3);
    assert_eq!(pointers[0]["root"], alloy::hex::encode(BLOB));
    assert_eq!(pointers[0]["signer"], owner.to_string());
}

/// A backend whose pools query answers one pool, or fails.
struct Pools {
    fail: bool,
}

impl worker::Backend for Pools {
    type Host = ();
    type Payer = ();

    async fn open(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn payer(&self, _credential: Credential) -> anyhow::Result<()> {
        Ok(())
    }

    async fn pull(
        &self,
        (): &(),
        (): &(),
        _blob: &BlobFetch,
        _sink: &mut super::super::fetch::Sink,
    ) -> anyhow::Result<()> {
        anyhow::bail!("these tests start no pull")
    }

    async fn pools(
        &self,
        (): &(),
        _delegates: &[worker::DelegateKey],
    ) -> anyhow::Result<worker::PoolSnapshot> {
        anyhow::ensure!(
            !self.fail,
            "the store at http://user:secret@host is unreadable"
        );
        let mut snapshot = worker::PoolSnapshot::default();
        snapshot.pools.skipped.push("0x44".to_owned());
        Ok(snapshot)
    }

    async fn close(&self, (): ()) {}
}

/// The status carries what the worker read from its store.
#[tokio::test]
async fn status_reports_the_pool_snapshot_from_the_worker() {
    let gateway = test_gateway_with(Pools { fail: false }, flags(&[])).await;
    let status = serde_json::to_value(gateway.status().await).unwrap();
    assert_eq!(status["pools"]["pools"]["skipped"][0], "0x44");
    assert!(status["pool_error"].is_null(), "{status}");
}

/// A store that cannot be read leaves `pools` null and says why, with the
/// error's secrets stripped.
#[tokio::test]
async fn status_reports_a_pool_snapshot_failure() {
    let gateway = test_gateway_with(Pools { fail: true }, flags(&[])).await;
    let status = serde_json::to_value(gateway.status().await).unwrap();
    assert!(status["pools"].is_null(), "{status}");
    let error = status["pool_error"].as_str().unwrap();
    assert!(error.contains("unreadable"), "{error}");
    assert!(!error.contains("secret"), "{error}");
}

/// A loopback gateway's metrics listener answers `/_status` and `/metrics`,
/// as the gateway's own host answers `/_status`.
#[tokio::test]
async fn a_loopback_metrics_listener_answers_status() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let gateway = test_gateway().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_metrics(Arc::clone(&gateway), listener));
    let fetch = |path: &'static str| async move {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut out = String::new();
        stream.read_to_string(&mut out).await.unwrap();
        out
    };
    let status = fetch("/_status").await;
    assert!(status.starts_with("HTTP/1.1 200"), "{status}");
    let metrics = fetch("/metrics").await;
    assert!(metrics.starts_with("HTTP/1.1 200"), "{metrics}");
}

/// A dashboard's polls are not what the gateway served: each poll of
/// `/_status` leaves the counters alone.
#[tokio::test]
async fn the_dashboard_does_not_count_itself() {
    let gateway = test_gateway().await;
    for _ in 0..3 {
        loopback_get(&gateway, "/_status").await;
    }
    assert_eq!(gateway.metrics.snapshot(), metrics::Counters::default());
}

/// One spelling per kind of value in `/_status`: an address is EIP-55, as
/// `GET /_capability` and the pool rows write it, and a content hash is bare
/// hex, as a gateway URL carries it.
#[tokio::test]
async fn status_spells_addresses_eip55_and_hashes_bare() {
    let mixed = Address::repeat_byte(0xab);
    assert_ne!(
        mixed.to_string(),
        format!("{mixed:#x}"),
        "a mixed-case address"
    );
    let pool = alloy::primitives::B256::repeat_byte(0x01);
    let payer = serde_json::to_value(PayerLabel {
        signer: mixed,
        pool,
    })
    .unwrap();
    assert_eq!(payer["signer"], mixed.to_string());
    assert_eq!(payer["pool"], format!("{pool:#x}"));
    let lanes = serde_json::to_value(worker::DelegateLanes::new(mixed, pool, Vec::new())).unwrap();
    assert_eq!(lanes["signer"], mixed.to_string());
    assert_eq!(lanes["pool_id"], format!("{pool:#x}"));
    let (gateway, _logs, _) = public_gateway().await;
    hold(&gateway, unsigned_pointer(7, BLOB, 3), mixed);
    let status = serde_json::to_value(gateway.status().await).unwrap();
    assert_eq!(status["pointers"][0]["signer"], mixed.to_string());
    assert_eq!(status["pointers"][0]["root"], alloy::hex::encode(BLOB));
    assert_eq!(
        status["payment_pool"],
        gateway
            .chain
            .first_pool()
            .unwrap()
            .deployment
            .payment_pool
            .to_string()
    );
}

/// A raw blob of known size whose pull stops for funding answers 503, as an
/// entry's does: no proof, and the pull reached.
#[tokio::test]
async fn a_sized_raw_blob_whose_pull_cannot_be_paid_answers_503() {
    let found = HashMap::from([(UNFUNDED, 5)]);
    let (gateway, logs, _) = public_gateway_sized(found).await;
    let unfunded = alloy::hex::encode(UNFUNDED);
    let answer = request(&gateway, "decdn.test", &format!("/{unfunded}"), &[]).await;
    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        logs.proofs.lock().unwrap().is_empty(),
        "a known size needs no proof"
    );
    assert_eq!(*logs.size_caps.lock().unwrap(), [Some(5)]);
}

/// A loopback gateway proves a raw blob's size too, on the capability that
/// pays for the request, and answers with its length.
#[tokio::test]
async fn a_loopback_raw_blob_answers_with_its_proven_length() {
    let replies = HashMap::from([(BLOB, Reply::Bytes(b"raw".to_vec()))]);
    let (gateway, caps, proofs) =
        recording_gateway_proving(replies, HashMap::from([(BLOB, 3)]), &[], 1).await;
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/{}", alloy::hex::encode(BLOB)))
        .header(header::HOST, "127.0.0.1:8484")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = Arc::clone(&gateway).handle(req).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()),
        Some("3")
    );
    assert_eq!(*proofs.lock().unwrap(), [BLOB]);
    assert_eq!(caps.lock().unwrap().len(), 1);
}

/// A capability that runs out at a size proof is retired, as at a pull, and
/// the request moves on: with no other capability, 402 and no pull.
#[tokio::test]
async fn a_capability_that_runs_out_at_a_proof_is_retired() {
    let replies = HashMap::from([(BLOB, Reply::Funding)]);
    let (gateway, caps, proofs) = recording_gateway_proving(replies, HashMap::new(), &[], 1).await;
    let (status, body) = get(&gateway, &format!("/{}", alloy::hex::encode(BLOB))).await;
    assert_eq!(status, StatusCode::PAYMENT_REQUIRED, "{body}");
    assert!(body.contains("no capability that can pay"), "{body}");
    assert_eq!(*proofs.lock().unwrap(), [BLOB]);
    assert!(caps.lock().unwrap().is_empty(), "nothing was pulled");
}

/// `GET /{hash}/` as its own task, so the test can run other requests while
/// this one's pull holds.
fn spawn_get(
    gateway: &Arc<Gateway>,
    hash: [u8; 32],
) -> tokio::task::JoinHandle<(StatusCode, String)> {
    let gateway = Arc::clone(gateway);
    tokio::spawn(async move { get(&gateway, &format!("/{}/", alloy::hex::encode(hash))).await })
}

/// Wait until the pulls in `caps` reach `count`.
async fn pulls_reach(caps: &Log<u64>, count: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while caps.lock().unwrap().len() < count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the pull starts");
}

/// Wait until `count` requests hold the fill lock of `key` or wait on it.
async fn fill_holders_reach(gateway: &Gateway, key: FlightKey, count: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let holders = gateway.manifest_fills.holders(&key);
            if holders >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the requests reach the fill lock");
}

/// Abort `task` and check it was still running: a request held by its pull.
async fn abort_held<T: std::fmt::Debug>(task: tokio::task::JoinHandle<T>) {
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

/// A stalled manifest pull of one hash delays no other hash: the fill lock is
/// per hash.
#[tokio::test]
async fn a_stalled_manifest_pull_does_not_block_another_hash() {
    let replies = HashMap::from([
        (HELD, Reply::Hold),
        (BLOB, Reply::Bytes(noindex_manifest())),
    ]);
    let (gateway, caps) = recording_gateway_slots(replies, &[], 2).await;
    let stalled = spawn_get(&gateway, HELD);
    // The held pull is in flight, with its hash's fill lock held.
    pulls_reach(&caps, 1).await;
    let blob = alloy::hex::encode(BLOB);
    let (status, body) = tokio::time::timeout(Duration::from_secs(10), async {
        get(&gateway, &format!("/{blob}/")).await
    })
    .await
    .expect("a request for another hash does not wait on the stalled fill");
    assert_eq!(status, StatusCode::OK, "{body}");
    abort_held(stalled).await;
}

/// Concurrent requests for one missing manifest pull once: the second waits
/// on the hash's fill lock. A dropped puller frees the lock, and the waiter
/// pulls itself.
#[tokio::test]
async fn concurrent_requests_for_one_manifest_pull_once() {
    let replies = HashMap::from([(HELD, Reply::Hold)]);
    let (gateway, caps) = recording_gateway_slots(replies, &[], 2).await;
    let first = spawn_get(&gateway, HELD);
    pulls_reach(&caps, 1).await;
    let second = spawn_get(&gateway, HELD);
    fill_holders_reach(&gateway, (HELD, None), 2).await;
    assert_eq!(
        caps.lock().unwrap().len(),
        1,
        "the second request waits on the fill lock instead of pulling"
    );
    abort_held(first).await;
    // The dropped request's lock frees: the waiter re-checks the cache,
    // misses, and runs its own pull.
    pulls_reach(&caps, 2).await;
    abort_held(second).await;
}

/// `count` distinct blobs for the fill-bound tests.
fn slot_blobs(count: usize) -> Vec<[u8; 32]> {
    (0xd0..).take(count).map(|n| [n; 32]).collect()
}

/// More stalled fills than the old fill slots.
const STALLED_FILLS: usize = 5;

/// A pull that has found no holder yet buffers nothing: stalled discoveries
/// leave the next manifest free to fill.
#[tokio::test]
async fn stalled_discoveries_buffer_nothing() {
    let held = slot_blobs(STALLED_FILLS);
    let mut replies: HashMap<_, _> = held.iter().map(|&h| (h, Reply::Hold)).collect();
    replies.insert(BLOB, Reply::Bytes(noindex_manifest()));
    let (gateway, caps) = recording_gateway_slots(replies, &[], STALLED_FILLS + 1).await;
    let stalled: Vec<_> = held.iter().map(|&h| spawn_get(&gateway, h)).collect();
    pulls_reach(&caps, STALLED_FILLS).await;
    assert_eq!(
        gateway.manifest_buffer.available_permits(),
        MANIFEST_BUFFER_BYTES
    );
    let blob = alloy::hex::encode(BLOB);
    let (status, body) = tokio::time::timeout(Duration::from_secs(10), async {
        get(&gateway, &format!("/{blob}/")).await
    })
    .await
    .expect("stalled discoveries block no other manifest");
    assert_eq!(status, StatusCode::OK, "{body}");
    for task in stalled {
        abort_held(task).await;
    }
}

/// Fills whose bytes trickle in hold only the bytes they buffered, so they
/// block no other manifest.
#[tokio::test]
async fn trickling_fills_block_no_other_manifest() {
    let held = slot_blobs(STALLED_FILLS);
    let mut replies: HashMap<_, _> = held
        .iter()
        .map(|&h| (h, Reply::BytesThenHold(b"{".to_vec())))
        .collect();
    replies.insert(BLOB, Reply::Bytes(noindex_manifest()));
    let (gateway, _) = recording_gateway_slots(replies, &[], STALLED_FILLS + 1).await;
    let filling: Vec<_> = held.iter().map(|&h| spawn_get(&gateway, h)).collect();
    tokio::time::timeout(Duration::from_secs(10), async {
        while gateway.manifest_buffer.available_permits() > MANIFEST_BUFFER_BYTES - STALLED_FILLS {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("each fill buffers its byte");
    let blob = alloy::hex::encode(BLOB);
    let (status, body) = tokio::time::timeout(Duration::from_secs(10), async {
        get(&gateway, &format!("/{blob}/")).await
    })
    .await
    .expect("trickling fills block no other manifest");
    assert_eq!(status, StatusCode::OK, "{body}");
    for task in filling {
        abort_held(task).await;
    }
}

/// A fill that would pass the buffer budget fails at once instead of
/// waiting, and the next fill runs once the budget frees.
#[tokio::test]
async fn a_spent_buffer_budget_fails_a_fill_fast() {
    let replies = HashMap::from([(BLOB, Reply::Bytes(noindex_manifest()))]);
    let (gateway, _) = recording_gateway_of(replies, &[]).await;
    let spent = u32::try_from(MANIFEST_BUFFER_BYTES - 1).unwrap();
    let taken = gateway.manifest_buffer.try_acquire_many(spent).unwrap();
    let blob = alloy::hex::encode(BLOB);
    let (status, body) = tokio::time::timeout(Duration::from_secs(10), async {
        get(&gateway, &format!("/{blob}/")).await
    })
    .await
    .expect("a fill past the budget does not wait");
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(body.contains("manifest buffer is full"), "{body}");
    drop(taken);
    let (status, body) = get(&gateway, &format!("/{blob}/")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        gateway.manifest_buffer.available_permits(),
        MANIFEST_BUFFER_BYTES,
        "the fill gave its budget back"
    );
}

/// The flight map prunes the locks of finished fills as it grows, so it does
/// not keep one entry per hash ever filled.
#[tokio::test]
async fn the_fill_map_prunes_finished_fills() {
    let blobs = slot_blobs(PRUNE_FLOOR + 4);
    let replies = blobs
        .iter()
        .map(|&h| (h, Reply::Bytes(noindex_manifest())))
        .collect();
    let (gateway, _) = recording_gateway_of(replies, &[]).await;
    for &h in &blobs {
        let (status, body) = get(&gateway, &format!("/{}/", alloy::hex::encode(h))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let len = gateway.manifest_fills.len();
    assert!(len <= PRUNE_FLOOR, "{len}");
}

/// A public gateway on [`Recording`] that answers each blob in `replies`,
/// the caps its pulls record, and the directory its pointers live in.
async fn public_gateway_replying(
    replies: HashMap<[u8; 32], Reply>,
) -> (Arc<Gateway>, Log<u64>, tempfile::TempDir) {
    let caps = Log::default();
    let backend = Recording {
        caps: Arc::clone(&caps),
        size_caps: Arc::default(),
        ranges: Arc::default(),
        proofs: Arc::default(),
        replies,
        sizes: std::sync::Mutex::default(),
        proves: HashMap::new(),
    };
    let pointers_dir = tempfile::tempdir().unwrap();
    let public = public_with(pointers_dir.path());
    let sites = SiteDomain::Public {
        origin: "https://decdn.test".parse().unwrap(),
    };
    let gateway = test_gateway_in(backend, flags(&[]), sites, Some(public), 2).await;
    hold_capability(&gateway, 0);
    (gateway, caps, pointers_dir)
}

/// Requests that wait on a fill which fails upstream answer with its failure
/// instead of each pulling again in turn.
#[tokio::test]
async fn waiters_share_a_failed_fill() {
    let notice = Arc::new(tokio::sync::Notify::new());
    let replies = HashMap::from([(HELD, Reply::FailOn(Arc::clone(&notice)))]);
    let (gateway, caps, _pointers) = public_gateway_replying(replies).await;
    let host = gateway.sites.site_host(&Site {
        hash: HELD,
        namespace: None,
    });
    let requests: Vec<_> = (0..3)
        .map(|_| {
            let (gateway, host) = (Arc::clone(&gateway), host.clone());
            tokio::spawn(async move { send(&gateway, Method::GET, &host, "/", &[]).await.status })
        })
        .collect();
    pulls_reach(&caps, 1).await;
    fill_holders_reach(&gateway, (HELD, None), 3).await;
    notice.notify_one();
    for request in requests {
        let status = tokio::time::timeout(Duration::from_secs(10), request)
            .await
            .expect("every waiter answers")
            .unwrap();
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }
    assert_eq!(caps.lock().unwrap().len(), 1, "the waiters pulled nothing");
}

/// A request that comes after a fill failed pulls again, even while the
/// fill's lock lives on.
#[tokio::test]
async fn a_failure_is_not_shared_with_later_requests() {
    let (gateway, caps, _pointers) = public_gateway_replying(HashMap::new()).await;
    let host = gateway.sites.site_host(&Site {
        hash: HELD,
        namespace: None,
    });
    // Holds the entry alive, as an overlapping request would.
    let _lock = gateway.manifest_flight((HELD, None));
    for pulls in 1..=2 {
        let status = send(&gateway, Method::GET, &host, "/", &[]).await.status;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(caps.lock().unwrap().len(), pulls, "each request pulls");
    }
}

/// A request routed through another namespace does not wait on a fill of the
/// same hash: the fill lock is per hash and namespace.
#[tokio::test]
async fn another_namespace_does_not_wait_on_a_fill() {
    let replies = HashMap::from([(HELD, Reply::Hold)]);
    let (gateway, caps, _pointers) = public_gateway_replying(replies).await;
    let spawn_in = |namespace| {
        let gateway = Arc::clone(&gateway);
        let host = gateway.sites.site_host(&Site {
            hash: HELD,
            namespace: Some(namespace),
        });
        tokio::spawn(async move { send(&gateway, Method::GET, &host, "/", &[]).await.status })
    };
    let first = spawn_in(7);
    pulls_reach(&caps, 1).await;
    let second = spawn_in(9);
    pulls_reach(&caps, 2).await;
    abort_held(first).await;
    abort_held(second).await;
}

/// The bytes the cache tests serve: text, so a response body reads as a
/// string, and longer than one chunk group.
fn cached_text() -> String {
    "cached bytes ".repeat(3_000)
}

/// A cache under `dir` holding `range` of `data` (`(0, len)` for all of it),
/// and the blob's hash.
async fn cache_holding(dir: &Path, data: &[u8], range: (u64, u64)) -> (CacheEngine, [u8; 32]) {
    let engine = CacheEngine::open(dir, Vec::new(), 0).await.unwrap();
    let root = admit(&engine, data, range).await;
    (engine, root)
}

/// Admit `range` of `data` (`(0, len)` for all of it) into `engine`, and
/// return the blob's hash.
async fn admit(engine: &CacheEngine, data: &[u8], range: (u64, u64)) -> [u8; 32] {
    let outboard =
        bao_tree::io::outboard::PreOrderMemOutboard::create(data, decdn_bao_range::IROH_BLOCK_SIZE);
    let root = *outboard.root.as_bytes();
    let total = u64::try_from(data.len()).unwrap();
    let aligned = decdn_bao_range::align_range(range.0, range.1, total).unwrap();
    let start = usize::try_from(aligned.fetch_start()).unwrap();
    let end = usize::try_from(aligned.fetch_end()).unwrap();
    let wire = decdn_bao_range::encode_verified_range(
        root,
        &aligned,
        &data[start..end],
        Bytes::from(outboard.data),
    )
    .unwrap();
    engine
        .admit_bao_stream(
            decdn_cache::Hash::from_bytes(root),
            aligned.chunk_ranges().clone(),
            total,
            wire.slice(8..),
            None,
        )
        .await
        .map_err(|(_, e)| e)
        .unwrap();
    root
}

/// A public gateway that can start no pull and holds no capability, serving
/// out of `cache`.
async fn cached_public_gateway(cache: CacheEngine, pointers_dir: &Path) -> Arc<Gateway> {
    let sites = SiteDomain::Public {
        origin: "https://decdn.test".parse().unwrap(),
    };
    let public = public_with(pointers_dir);
    test_gateway_cached(NoPulls, flags(&[]), sites, Some(public), 1, Some(cache)).await
}

/// A blob the cache holds whole is served from it with its length, and with
/// no pull.
#[tokio::test]
async fn a_cached_blob_is_served_with_no_pull() {
    let dir = tempfile::tempdir().unwrap();
    let text = cached_text();
    let len = u64::try_from(text.len()).unwrap();
    let (cache, hash) = cache_holding(&dir.path().join("cache"), text.as_bytes(), (0, len)).await;
    let gateway = cached_public_gateway(cache, dir.path()).await;

    let path = format!("/{}", alloy::hex::encode(hash));
    let answer = request(&gateway, "decdn.test", &path, &[]).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.body, text);
    assert_eq!(
        answer.header(header::CONTENT_LENGTH),
        Some(len.to_string().as_str())
    );
    let counters = gateway.metrics.snapshot();
    assert_eq!(counters.cache_hits, 1);
    assert_eq!(counters.bytes_served, len);
}

/// A `Range` the cache holds is served from it as a 206, with its
/// `Content-Range`.
#[tokio::test]
async fn a_cached_range_is_served_from_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let text = cached_text();
    let len = u64::try_from(text.len()).unwrap();
    let (cache, hash) = cache_holding(&dir.path().join("cache"), text.as_bytes(), (0, len)).await;
    let gateway = cached_public_gateway(cache, dir.path()).await;

    let path = format!("/{}", alloy::hex::encode(hash));
    let range = [(header::RANGE.as_str(), "bytes=100-199")];
    let answer = request(&gateway, "decdn.test", &path, &range).await;
    assert_eq!(
        answer.status,
        StatusCode::PARTIAL_CONTENT,
        "{}",
        answer.body
    );
    assert_eq!(answer.body, text[100..200]);
    assert_eq!(
        answer.header(header::CONTENT_RANGE),
        Some(format!("bytes 100-199/{len}").as_str())
    );
    assert_eq!(gateway.metrics.snapshot().cache_hits, 1);
}

/// A cache entry that does not hold the bytes a request asks for is no hit:
/// the request goes to a pull.
#[tokio::test]
async fn a_partial_entry_that_misses_the_request_is_no_hit() {
    let dir = tempfile::tempdir().unwrap();
    let text = cached_text();
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let (cache, hash) = cache_holding(&dir.path().join("cache"), text.as_bytes(), (0, group)).await;
    let gateway = cached_public_gateway(cache, dir.path()).await;
    hold_capability(&gateway, 0);

    let path = format!("/{}", alloy::hex::encode(hash));
    let answer = request(&gateway, "decdn.test", &path, &[]).await;
    assert_eq!(answer.status, StatusCode::BAD_GATEWAY, "{}", answer.body);
    assert_eq!(gateway.metrics.snapshot().cache_hits, 0);
}

/// A public gateway with no capability that can pay still serves what the
/// cache holds whole; a blob the cache lacks gets the 503.
#[tokio::test]
async fn a_public_gateway_with_no_capability_serves_a_cached_blob() {
    let dir = tempfile::tempdir().unwrap();
    let text = cached_text();
    let len = u64::try_from(text.len()).unwrap();
    let (cache, hash) = cache_holding(&dir.path().join("cache"), text.as_bytes(), (0, len)).await;
    let gateway = cached_public_gateway(cache, dir.path()).await;

    let cached = format!("/{}", alloy::hex::encode(hash));
    let answer = request(&gateway, "decdn.test", &cached, &[]).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.body, text);

    let missing = format!("/{}", alloy::hex::encode(OTHER));
    let refused = request(&gateway, "decdn.test", &missing, &[]).await;
    assert_eq!(
        refused.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        refused.body
    );
    assert_eq!(gateway.metrics.snapshot().cache_hits, 1);
}

/// With no capability that can pay, a site's entry and its `/` come out of
/// the cache, manifest included: the manifest is read from the cache too, so
/// nothing pulls.
#[tokio::test]
async fn a_public_gateway_with_no_capability_serves_a_cached_site() {
    let dir = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(&dir.path().join("cache"), Vec::new(), 0)
        .await
        .unwrap();
    let text = cached_text();
    let page = b"<p>cached page</p>".to_vec();
    let text_hash = admit(&engine, text.as_bytes(), (0, text.len() as u64)).await;
    let page_hash = admit(&engine, &page, (0, page.len() as u64)).await;
    let manifest = serde_json::json!({"version": 1, "entries": [
        {"path": "a.txt", "hash": alloy::hex::encode(text_hash), "size": text.len()},
        {"path": "index.html", "hash": alloy::hex::encode(page_hash), "size": page.len()},
    ]})
    .to_string()
    .into_bytes();
    let bundle = admit(&engine, &manifest, (0, manifest.len() as u64)).await;
    let gateway = cached_public_gateway(engine, dir.path()).await;
    let host = gateway.sites.site_host(&Site {
        hash: bundle,
        namespace: None,
    });

    let entry = send(&gateway, Method::GET, &host, "/a.txt", &[]).await;
    assert_eq!(entry.status, StatusCode::OK, "{}", entry.body);
    assert_eq!(entry.body, text);
    let root = send(&gateway, Method::GET, &host, "/", &[]).await;
    assert_eq!(root.status, StatusCode::OK, "{}", root.body);
    assert_eq!(root.body.as_bytes(), page.as_slice());
    assert_eq!(gateway.metrics.snapshot().cache_hits, 2);
}

/// A loopback gateway that holds no capability still serves a blob the cache
/// holds whole, at no cost; a blob it lacks gets the 402.
#[tokio::test]
async fn a_gateway_with_no_capability_serves_a_cached_blob() {
    let dir = tempfile::tempdir().unwrap();
    let text = cached_text();
    let len = u64::try_from(text.len()).unwrap();
    let (cache, hash) = cache_holding(&dir.path().join("cache"), text.as_bytes(), (0, len)).await;
    let sites = SiteDomain::Loopback {
        listen: "127.0.0.1:8484".parse().unwrap(),
    };
    let gateway = test_gateway_cached(NoPulls, flags(&[]), sites, None, 1, Some(cache)).await;

    let (status, body) = get(&gateway, &format!("/{}", alloy::hex::encode(hash))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, text);
    assert_eq!(gateway.metrics.snapshot().cache_hits, 1);

    let (status, body) = get(&gateway, &format!("/{}", alloy::hex::encode(OTHER))).await;
    assert_eq!(status, StatusCode::PAYMENT_REQUIRED, "{body}");
    assert!(body.contains("no capability that can pay"), "{body}");
}

/// A capability scoped to a namespace pays for the requests that
/// namespace's pointer serves, on its host, and for nothing else: not the
/// same bundle on its own site host, not other content on the namespace's
/// host, not a blob on the bare host.
#[tokio::test]
async fn a_scoped_capability_pays_only_on_its_namespace_host() {
    let (gateway, _logs, site_host) =
        public_gateway_unpaid(HashMap::new(), HashMap::new(), flags(&[])).await;
    hold(
        &gateway,
        unsigned_pointer(7, BLOB, 1),
        Address::repeat_byte(7),
    );
    {
        let mut delegates = gateway.delegates();
        let signer = delegates.pending();
        delegates
            .register(grant(signer, 1, u64::MAX), TEST_POOL, Some(vec![7]))
            .unwrap();
    }
    let served = request(&gateway, "7.decdn.test", "/a.txt", &[]).await;
    assert_eq!(served.status, StatusCode::OK, "{}", served.body);
    assert_eq!(served.body, "abc");
    let other = format!("/{}", alloy::hex::encode(OTHER));
    for (host, path) in [
        (site_host.as_str(), "/a.txt"),
        ("7.decdn.test", &*format!("/_decdn{other}")),
        ("decdn.test", other.as_str()),
    ] {
        let refused = request(&gateway, host, path, &[]).await;
        assert_eq!(
            refused.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{host}{path}: {}",
            refused.body
        );
    }
}

/// On a loopback gateway, the namespace a request's `decdn:` URI names
/// selects a capability scoped to it: `?ns=` on the bare host. A request that
/// names another namespace, or none, finds nothing that can pay.
#[tokio::test]
async fn a_loopback_scoped_capability_pays_for_the_namespace_its_uri_names() {
    let (gateway, _) = site_gateway().await;
    {
        let mut delegates = gateway.delegates();
        for (pool, pool_id, signer) in delegates.lanes() {
            delegates.retire(signer, pool, pool_id);
        }
        let signer = delegates.pending();
        delegates
            .register(grant(signer, 1, u64::MAX), TEST_POOL, Some(vec![7]))
            .unwrap();
    }
    let other = alloy::hex::encode(OTHER);
    let served = request(&gateway, "127.0.0.1:8484", &format!("/{other}?ns=7"), &[]).await;
    assert_eq!(
        (served.status, served.body.as_str()),
        (StatusCode::OK, "other bytes")
    );
    for path in [format!("/{other}?ns=8"), format!("/{other}")] {
        let refused = request(&gateway, "127.0.0.1:8484", &path, &[]).await;
        assert_eq!(refused.status, StatusCode::PAYMENT_REQUIRED, "{path}");
    }
}

/// A link on another site opens content on a loopback gateway: a top-level
/// navigation passes the browser gate from anywhere.
#[tokio::test]
async fn a_link_on_another_site_opens_loopback_content() {
    let (gateway, _) = site_gateway().await;
    let other = alloy::hex::encode(OTHER);
    let navigation = [
        ("sec-fetch-site", "cross-site"),
        ("sec-fetch-mode", "navigate"),
        ("sec-fetch-dest", "document"),
        ("user-agent", "Mozilla/5.0 (X11; Linux x86_64)"),
    ];
    let opened = request(
        &gateway,
        "127.0.0.1:8484",
        &format!("/{other}"),
        &navigation,
    )
    .await;
    assert_eq!(
        (opened.status, opened.body.as_str()),
        (StatusCode::OK, "other bytes")
    );
}

/// A capability signed by a pool owner, issued to the gateway's pending key.
async fn signed_grant(gateway: &Arc<Gateway>, owner: &PrivateKeySigner) -> CapabilityGrant {
    let key = pending_address(gateway).await;
    let capability = decdn_incentive::Capability {
        signer: key,
        spending_cap: 1_000,
        pool_id: alloy::primitives::B256::repeat_byte(1),
        expiry: now_secs() + 86_400,
    };
    let signed = capability.sign(owner, &TEST_POOL.voucher_domain()).unwrap();
    CapabilityGrant::from_signed_capability(&signed)
}

/// `PUT /_capability` with `grant` and the JSON `scope`: the status and body.
async fn put_scoped(
    gateway: &Arc<Gateway>,
    grant: &CapabilityGrant,
    scope: serde_json::Value,
) -> (StatusCode, String) {
    let body = serde_json::json!({ "capability": grant.to_token(), "scope": scope }).to_string();
    let (status, _, body) = exchange(gateway, Method::PUT, "/_capability", &[], &body).await;
    (status, body)
}

/// A scope must name a namespace and be signed by the capability's own pool
/// owner; both are checked before the chain is read. A scope that passes
/// them reaches the chain read, which this gateway cannot make.
#[tokio::test]
async fn a_scope_is_checked_before_the_chain_is_read() {
    let gateway = test_gateway().await;
    let owner = PrivateKeySigner::random();
    let grant = signed_grant(&gateway, &owner).await;
    let domain = scope::scope_domain(TEST_POOL.chain_id, TEST_POOL.payment_pool);

    let empty = serde_json::json!({ "namespaces": [], "signature": "0x00" });
    let (status, body) = put_scoped(&gateway, &grant, empty).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains("at least one namespace"), "{body}");

    let stranger = PrivateKeySigner::random();
    let foreign =
        SignedScope::sign(&stranger, grant.signer, grant.pool_id, vec![7], &domain).unwrap();
    let (status, body) = put_scoped(&gateway, &grant, serde_json::json!(foreign)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body.contains("not signed by the capability's pool owner"),
        "{body}"
    );

    let own = SignedScope::sign(&owner, grant.signer, grant.pool_id, vec![7], &domain).unwrap();
    let (status, body) = put_scoped(&gateway, &grant, serde_json::json!(own)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
}
