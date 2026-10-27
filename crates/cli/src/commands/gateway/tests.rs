use super::*;
use crate::commands::fetch::{MALFORMED_MANIFEST, MAX_MANIFEST_MB};
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
fn scope_parses_any_domain_and_host() {
    assert_eq!(Scope::parse("*").unwrap(), Scope::Any);
    assert_eq!(
        Scope::parse(" *.Decdn.org ").unwrap(),
        Scope::Domain("decdn.org".into())
    );
    assert_eq!(
        Scope::parse("up.decdn.org").unwrap(),
        Scope::Host("up.decdn.org".into())
    );
    for bad in [
        "",
        "*.",
        "**",
        "*.*.org",
        "a..b",
        "a b",
        "https://x.org",
        "x.org/",
        "-x.org!",
    ] {
        assert!(Scope::parse(bad).is_err(), "{bad}");
    }
}

#[test]
fn domain_scope_covers_the_apex_and_subdomains_only() {
    let scope = Scope::Domain("decdn.org".into());
    assert!(scope.covers("decdn.org"));
    assert!(scope.covers("up.decdn.org"));
    assert!(scope.covers("a.b.decdn.org"));
    assert!(!scope.covers("evildecdn.org"));
    assert!(!scope.covers("decdn.org.evil.com"));
    assert!(!scope.covers("org"));
}

#[test]
fn host_scope_covers_one_host() {
    let scope = Scope::Host("decdn.org".into());
    assert!(scope.covers("decdn.org"));
    assert!(!scope.covers("up.decdn.org"));
}

#[test]
fn more_specific_scopes_rank_higher() {
    assert!(Scope::Host("a.org".into()).rank() > Scope::Domain("a.org".into()).rank());
    assert!(Scope::Domain("a.org".into()).rank() > Scope::Any.rank());
}

#[test]
fn origin_host_reads_the_host_of_an_origin() {
    assert_eq!(
        origin_host("https://up.decdn.org").as_deref(),
        Some("up.decdn.org")
    );
    assert_eq!(
        origin_host("http://Example.COM:8080").as_deref(),
        Some("example.com")
    );
    assert_eq!(origin_host("null"), None);
    assert_eq!(origin_host("https://"), None);
    assert_eq!(origin_host("chrome-extension://abcdefgh"), None);
}

fn page(origin: &str) -> Caller {
    Caller::of(Some(origin), false).unwrap()
}

#[test]
fn a_page_registers_only_scopes_that_cover_itself() {
    let origin = page("https://decdn.org");
    for scope in ["decdn.org", "*.decdn.org", "*"] {
        assert!(
            may_register(&origin, &Scope::parse(scope).unwrap()).is_ok(),
            "{scope}"
        );
    }
    for scope in ["netflix.com", "*.netflix.com", "up.decdn.org"] {
        assert!(
            may_register(&origin, &Scope::parse(scope).unwrap()).is_err(),
            "{scope}"
        );
    }
    let sub = page("https://up.decdn.org");
    assert!(may_register(&sub, &Scope::parse("*.decdn.org").unwrap()).is_ok());
    assert!(may_register(&sub, &Scope::parse("decdn.org").unwrap()).is_err());
}

#[test]
fn a_page_may_not_register_a_public_suffix_wildcard() {
    for (origin, scope) in [
        ("https://attacker.io", "*.io"),
        ("https://attacker.github.io", "*.github.io"),
        ("https://shop.example.co.uk", "*.co.uk"),
    ] {
        assert!(
            may_register(&page(origin), &Scope::parse(scope).unwrap()).is_err(),
            "{scope}"
        );
    }
    // The site's own wildcard and `*` stay open to it.
    let origin = page("https://attacker.github.io");
    assert!(may_register(&origin, &Scope::parse("*.attacker.github.io").unwrap()).is_ok());
    assert!(may_register(&origin, &Scope::Any).is_ok());
    let origin = page("https://shop.example.co.uk");
    assert!(may_register(&origin, &Scope::parse("*.example.co.uk").unwrap()).is_ok());
    // The token holder registers any scope.
    assert!(may_register(&Caller::Token, &Scope::parse("*.io").unwrap()).is_ok());
}

#[test]
fn a_page_stays_a_page_with_the_token() {
    let caller = Caller::of(Some("https://decdn.org"), true).unwrap();
    assert_eq!(caller.origin(), Some("https://decdn.org"));
    assert!(may_register(&caller, &Scope::parse("netflix.com").unwrap()).is_err());
}

#[test]
fn the_token_holder_registers_any_scope_outside_a_page() {
    for origin in [None, Some("chrome-extension://abcdefgh")] {
        let caller = Caller::of(origin, true).unwrap();
        assert_eq!(caller, Caller::Token);
        assert!(may_register(&caller, &Scope::parse("netflix.com").unwrap()).is_ok());
    }
}

#[test]
fn callers_without_a_page_origin_need_the_token() {
    assert!(Caller::of(None, false).is_err());
    assert!(Caller::of(Some("chrome-extension://abcdefgh"), false).is_err());
    assert!(Caller::of(Some("null"), false).is_err());
    assert!(Caller::of(Some("null"), true).is_err());
}

fn registered(scope: &str, origin: Option<&str>, registered: Instant) -> Registered {
    Registered {
        grant: CapabilityGrant {
            pool_id: alloy::primitives::B256::ZERO,
            signer: Address::ZERO,
            spending_cap: 1,
            expiry: u64::MAX,
            owner_signature: vec![0; 65],
        },
        scope: Scope::parse(scope).unwrap(),
        origin: origin.map(str::to_owned),
        since: registered,
        spent: false,
    }
}

#[test]
fn the_token_holders_capability_wins_a_tie_with_a_pages() {
    let now = Instant::now();
    let page = registered("*", Some("https://evil.example"), now);
    let own = registered("*", None, now + Duration::from_secs(1));
    assert!(own.preference() > page.preference());
    // A more specific scope still wins over the token holder's `*`.
    let site = registered("decdn.org", Some("https://decdn.org"), now);
    assert!(site.preference() > own.preference());
}

#[test]
fn a_later_registration_never_shadows_an_earlier_one() {
    let now = Instant::now();
    let incumbent = registered("*.example.com", Some("https://example.com"), now);
    let newcomer = registered(
        "*.example.com",
        Some("https://x.example.com"),
        now + Duration::from_secs(1),
    );
    assert!(incumbent.preference() > newcomer.preference());
}

fn delegate(site: Option<&str>, capability: Option<Registered>) -> Delegate {
    Delegate {
        signer: Arc::new(PrivateKeySigner::random()),
        minted: Instant::now(),
        site: site.map(str::to_owned),
        capability,
    }
}

fn insert(delegates: &mut HashMap<Address, Delegate>, d: Delegate) {
    delegates.insert(d.signer.address(), d);
}

#[test]
fn a_site_is_the_registrable_domain() {
    assert_eq!(site("decdn.org"), "decdn.org");
    assert_eq!(site("a.b.decdn.org"), "decdn.org");
    assert_eq!(site("www.example.co.uk"), "example.co.uk");
    // A public suffix that hosts user sites keeps each user apart.
    assert_eq!(site("alice.github.io"), "alice.github.io");
    assert_eq!(site("x.alice.github.io"), "alice.github.io");
    assert_eq!(site("127.0.0.1"), "127.0.0.1");
    assert_eq!(site("localhost"), "localhost");
}

#[test]
fn the_pages_of_one_site_share_its_room() {
    let page = |origin| page(origin).site();
    assert_eq!(
        page("https://a.evil.example.com"),
        page("https://b.evil.example.com")
    );
    let mut delegates = HashMap::new();
    for _ in 0..MAX_DELEGATES_PER_SITE {
        insert(&mut delegates, delegate(Some("example.com"), None));
    }
    assert!(may_mint(&delegates, Some("example.com")).is_err());
    assert!(may_mint(&delegates, Some("decdn.org")).is_ok());
    assert!(may_mint(&delegates, None).is_ok());
}

#[test]
fn registering_does_not_free_a_sites_room() {
    let mut delegates = HashMap::new();
    for _ in 0..MAX_DELEGATES_PER_SITE {
        let capability = registered("*", Some("https://evil.example"), Instant::now());
        insert(
            &mut delegates,
            delegate(Some("evil.example"), Some(capability)),
        );
    }
    assert!(may_mint(&delegates, Some("evil.example")).is_err());
}

#[test]
fn pages_stop_at_the_global_bound_and_the_token_holder_does_not() {
    let mut delegates = HashMap::new();
    for n in 0..MAX_PAGE_DELEGATES {
        insert(
            &mut delegates,
            delegate(Some(&format!("site{n}.example")), None),
        );
    }
    assert!(may_mint(&delegates, Some("decdn.org")).is_err());
    assert!(may_mint(&delegates, None).is_ok());
}

#[test]
fn a_capability_must_be_signed_by_its_pools_on_chain_owner() {
    let owner = PrivateKeySigner::random().address();
    let other = PrivateKeySigner::random().address();
    let pool = alloy::primitives::B256::ZERO;
    assert!(owner_matches(owner, owner, pool).is_ok());
    assert!(owner_matches(owner, other, pool).is_err());
    assert!(owner_matches(Address::ZERO, owner, pool).is_err());
}

const TEST_TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

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
    let sites = SiteDomain::Loopback {
        listen: "127.0.0.1:8484".parse().unwrap(),
    };
    test_gateway_in(backend, common, sites, None).await
}

/// The registry address a test's public gateway verifies pointers against.
const REGISTRY: Address = Address::repeat_byte(3);

/// What a test's public gateway holds: `allowance`, no pointers yet, in
/// `pointers_dir`.
fn public_with(allowance: Allowance, pointers_dir: &Path) -> Public {
    Public {
        allowance,
        pointers: Pointers::load(pointers_dir, 421_614, REGISTRY).unwrap(),
        registry: REGISTRY,
        lookups: std::sync::Mutex::new(Budgets::default()),
        pointer_writes: Mutex::new(()),
    }
}

/// A gateway on `backend` whose sites live under `sites`: a loopback gateway
/// with [`TEST_TOKEN`] and no `public`, or a public one with no token.
async fn test_gateway_in(
    backend: impl worker::Backend,
    common: cli::ClientFetchArgs,
    sites: SiteDomain,
    public: Option<Public>,
) -> Arc<Gateway> {
    let token = matches!(sites, SiteDomain::Loopback { .. }).then(|| TEST_TOKEN.to_owned());
    assert_eq!(matches!(sites, SiteDomain::Public { .. }), public.is_some());
    let chain = ChainConfig {
        // Nothing here reaches the chain: every request below stops first.
        rpc_url: "http://127.0.0.1:1".to_owned(),
        chain_id: 421_614,
        payment_pool: Address::repeat_byte(1),
        slash_judge: Address::repeat_byte(2),
        capacity_bond: None,
    };
    Arc::new(Gateway {
        common,
        token,
        domain: voucher_domain(chain.chain_id, chain.payment_pool),
        chain,
        delegates: Mutex::new(HashMap::new()),
        pulls: PullWorker::start(backend, 1).await.unwrap(),
        manifests: std::sync::Mutex::new(ManifestCache::default()),
        not_manifests: std::sync::Mutex::new(NotManifests::default()),
        manifest_fill: Mutex::new(()),
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
        &[(TOKEN_HEADER, TEST_TOKEN), (HOST_HEADER, "decdn.org")],
        "",
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn minting_needs_a_page_origin_or_the_token_and_stops_per_site() {
    let gateway = test_gateway().await;
    assert_eq!(
        call(&gateway, Method::GET, "/_delegate", &[], "").await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &gateway,
            Method::GET,
            "/_delegate",
            &[(TOKEN_HEADER, TEST_TOKEN)],
            ""
        )
        .await,
        StatusCode::OK
    );
    // Every page is a fresh subdomain of one site, as with wildcard DNS.
    let mint = |n: usize| {
        let gateway = Arc::clone(&gateway);
        async move {
            let origin = format!("https://p{n}.evil.com");
            let page = [(header::ORIGIN.as_str(), origin.as_str())];
            call(&gateway, Method::GET, "/_delegate", &page, "").await
        }
    };
    for n in 0..MAX_DELEGATES_PER_SITE {
        assert_eq!(mint(n).await, StatusCode::OK);
    }
    assert_eq!(
        mint(MAX_DELEGATES_PER_SITE).await,
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[tokio::test]
async fn a_page_with_the_token_still_registers_only_its_own_scope() {
    let gateway = test_gateway().await;
    let body = r#"{"capability": "dcap1:x", "scope": "netflix.com"}"#;
    let status = call(
        &gateway,
        Method::PUT,
        "/_capability",
        &[
            (header::ORIGIN.as_str(), "https://decdn.org"),
            (TOKEN_HEADER, TEST_TOKEN),
        ],
        body,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
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
/// byte window.
struct Recording {
    caps: Log<u64>,
    size_caps: Log<Option<u64>>,
    ranges: Log<Option<(u64, u64)>>,
    replies: HashMap<[u8; 32], Reply>,
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

    async fn pool_payer(&self, (): &()) -> anyhow::Result<Option<()>> {
        Ok(Some(()))
    }

    async fn pull(
        &self,
        (): &(),
        (): &(),
        blob: &BlobFetch,
        sink: &mut super::super::fetch::Sink,
    ) -> anyhow::Result<()> {
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
            Some(Reply::Hold) => std::future::pending().await,
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

/// A gateway on [`Recording`] that answers [`BLOB`] with `reply`, whose token
/// holder pays for every host, and the caps its pulls record.
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
    let caps = Arc::default();
    let backend = Recording {
        caps: Arc::clone(&caps),
        size_caps: Arc::default(),
        ranges: Arc::default(),
        replies,
    };
    let gateway = test_gateway_with(backend, flags(args)).await;
    insert(
        &mut *gateway.delegates.lock().await,
        delegate(None, Some(registered("*", None, Instant::now()))),
    );
    (gateway, caps)
}

/// A paid `GET path`: the status and the body as text.
async fn get(gateway: &Arc<Gateway>, path: &str) -> (StatusCode, String) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(path)
        .header(header::HOST, "127.0.0.1:8484")
        .header(TOKEN_HEADER, TEST_TOKEN)
        .header(HOST_HEADER, "decdn.org")
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

/// `GET path` with `Host: host` and `headers`, and no token.
async fn request(
    gateway: &Arc<Gateway>,
    host: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> Answer {
    send(gateway, Method::GET, host, path, headers).await
}

/// `method path` with `Host: host` and `headers`, an empty body and no token.
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
const OTHER: [u8; 32] = [0xcd; 32];

/// A gateway serving [`BLOB`] as a site bundle with `index.html`,
/// `css/site.css` and `docs/index.html`, the blob [`OTHER`], and the host of
/// that site.
async fn site_gateway() -> (Arc<Gateway>, String) {
    let other = alloy::hex::encode(OTHER);
    let manifest = serde_json::json!({"version": 1, "entries": [
        {"path": "index.html", "hash": alloy::hex::encode(PAGE)},
        {"path": "css/site.css", "hash": alloy::hex::encode(STYLE)},
        {"path": "docs/index.html", "hash": alloy::hex::encode(DOCS)},
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
    let missing = request(&gateway, &host, "/nope.html", CLICK).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    assert!(
        missing
            .headers
            .contains_key(header::CONTENT_SECURITY_POLICY)
    );
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
    for headers in [&image[..], &frame[..]] {
        let answer = request(&gateway, &host, "/", headers).await;
        assert_eq!(answer.status, StatusCode::FORBIDDEN, "{headers:?}");
    }
    let link = [
        ("sec-fetch-site", "cross-site"),
        ("sec-fetch-mode", "navigate"),
        ("sec-fetch-dest", "document"),
    ];
    let deep = request(&gateway, &host, "/docs/", &link).await;
    assert_eq!(
        deep.status,
        StatusCode::OK,
        "a link opens a site at any path"
    );
    let script = request(&gateway, &host, "/", &[]).await;
    assert_eq!(script.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_site_answers_no_gateway_api_and_a_bad_site_host_is_refused() {
    let (gateway, host) = site_gateway().await;
    for path in ["/_delegate", "/_capability"] {
        let answer = request(&gateway, &host, path, TYPED).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{path}");
    }
    let answer = request(&gateway, "127.0.0.1:8484", "/_delegate", &[]).await;
    assert_eq!(
        answer.status,
        StatusCode::FORBIDDEN,
        "the bare origin answers it"
    );
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
async fn the_bare_origin_opens_a_bundle_as_its_site_without_the_token() {
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
        StatusCode::UNAUTHORIZED,
        "a blob has no site"
    );
}

#[tokio::test]
async fn the_bare_origin_still_serves_markup_as_text() {
    let (gateway, _) = site_gateway().await;
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/{}/index.html", alloy::hex::encode(BLOB)))
        .header(header::HOST, "127.0.0.1:8484")
        .header(TOKEN_HEADER, TEST_TOKEN)
        .header(HOST_HEADER, "decdn.org")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = Arc::clone(&gateway).handle(req).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/plain; charset=utf-8"
    );
    assert!(
        !response
            .headers()
            .contains_key(header::CONTENT_SECURITY_POLICY)
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        String::from_utf8_lossy(&body).contains("decdn:"),
        "not rewritten"
    );
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
fn a_loopback_gateway_drops_the_password_file_and_refuses_key_flags() {
    let common = fetch_flags(&flags(&["--keystore-password-file", "/pw"]), false).unwrap();
    assert!(common.keystore_password_file.is_none());
    for flag in [
        ["--keystore", "/ks"],
        ["--capability", "dcap1:x"],
        ["--capability-file", "/cap"],
    ] {
        assert!(fetch_flags(&flags(&flag), false).is_err(), "{flag:?}");
        assert!(fetch_flags(&flags(&flag), true).is_ok(), "{flag:?}");
    }
    // A public gateway pays from its keystore, so it keeps the password file.
    let common = fetch_flags(&flags(&["--keystore-password-file", "/pw"]), true).unwrap();
    assert_eq!(
        common.keystore_password_file.as_deref(),
        Some(std::path::Path::new("/pw"))
    );
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
fn escape_covers_markup_and_quotes() {
    assert_eq!(
        escape(r#"<a href="x">'&'</a>"#),
        "&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;"
    );
}

#[test]
fn the_token_is_created_owner_only_and_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    let token = load_or_create_token(&path).unwrap();
    assert_eq!(token.len(), 64);
    assert_eq!(load_or_create_token(&path).unwrap(), token);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    std::fs::write(&path, "not a token").unwrap();
    assert!(load_or_create_token(&path).is_err());
}

#[test]
fn prune_drops_stale_pending_and_expired_delegates() {
    let mut delegates = HashMap::new();
    let fresh = PrivateKeySigner::random();
    let stale = PrivateKeySigner::random();
    let stale_minted = Instant::now()
        .checked_sub(PENDING_TTL + Duration::from_secs(1))
        .unwrap();
    for (signer, minted) in [(&fresh, Instant::now()), (&stale, stale_minted)] {
        delegates.insert(
            signer.address(),
            Delegate {
                signer: Arc::new(signer.clone()),
                minted,
                site: None,
                capability: None,
            },
        );
    }
    Gateway::prune(&mut delegates);
    assert!(delegates.contains_key(&fresh.address()));
    assert!(!delegates.contains_key(&stale.address()));
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

/// The [`NOINDEX`] bundle's manifest.
fn noindex_manifest() -> Vec<u8> {
    serde_json::json!({"version": 1, "entries": [
        {"path": "a b.txt", "hash": alloy::hex::encode(ENTRY), "size": 3},
    ]})
    .to_string()
    .into_bytes()
}
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
    public_gateway_with(u64::MAX).await
}

/// [`public_gateway`] with `allowance` bytes per day, shared by every request.
async fn public_gateway_with(allowance: u64) -> (Arc<Gateway>, Logs, String) {
    public_gateway_for(Allowance::with_namespaces(allowance, [])).await
}

/// [`public_gateway`] with `allowance`.
async fn public_gateway_for(allowance: Allowance) -> (Arc<Gateway>, Logs, String) {
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
    ]);
    let logs = Logs {
        caps: Arc::default(),
        size_caps: Arc::default(),
        ranges: Arc::default(),
        pointers_dir: tempfile::tempdir().unwrap(),
    };
    let backend = Recording {
        caps: Arc::clone(&logs.caps),
        size_caps: Arc::clone(&logs.size_caps),
        ranges: Arc::clone(&logs.ranges),
        replies,
    };
    let sites = SiteDomain::Public {
        origin: "https://decdn.test".parse().unwrap(),
    };
    let public = public_with(allowance, logs.pointers_dir.path());
    let gateway = test_gateway_in(backend, flags(&[]), sites, Some(public)).await;
    let host = gateway.sites.site_host(&Site {
        hash: BLOB,
        namespace: None,
    });
    (gateway, logs, host)
}

/// `method path` with `Host: host` and `body`, and no token.
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
    let domain = pointer_domain(gateway.chain.chain_id, REGISTRY);
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

/// A namespace's own allowance pays for its own bundle and nothing else:
/// `/_decdn/` content from another bundle on its host spends the shared
/// allowance, so a stranger cannot drain the namespace's budget with
/// unrelated content, and the namespace cannot lend its budget out.
#[tokio::test]
async fn a_namespace_allowance_pays_for_its_own_bundle_only() {
    let (gateway, _logs, _) =
        public_gateway_for(Allowance::with_namespaces(u64::MAX, [(7, 1)])).await;
    hold(&gateway, unsigned_pointer(7, BLOB, 1), Address::ZERO);
    let own = request(&gateway, "7.decdn.test", "/a.txt", &[]).await;
    assert_eq!(own.status, StatusCode::PAYMENT_REQUIRED);
    let own_via_decdn = request(
        &gateway,
        "7.decdn.test",
        &format!("/_decdn/{}/a.txt", alloy::hex::encode(BLOB)),
        SAME_ORIGIN,
    )
    .await;
    assert_eq!(own_via_decdn.status, StatusCode::PAYMENT_REQUIRED);
    for headers in [&[][..], SAME_ORIGIN] {
        let other = request(
            &gateway,
            "7.decdn.test",
            &format!("/_decdn/{}", alloy::hex::encode(OTHER)),
            headers,
        )
        .await;
        assert_eq!(
            (other.status, other.body.as_str()),
            (StatusCode::OK, "other bytes"),
            "{headers:?}"
        );
    }
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
    let domain = pointer_domain(gateway.chain.chain_id, REGISTRY);
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
    let domain = pointer_domain(gateway.chain.chain_id, REGISTRY);
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
    let domain = pointer_domain(gateway.chain.chain_id, REGISTRY);
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

/// A namespace with an allowance of its own spends it on the requests its
/// pointer serves, and nothing else: past it, its host answers 402 naming
/// the entry's URI with the namespace, while the bundle's own site, on the
/// shared allowance, still serves.
#[tokio::test]
async fn a_namespace_with_its_own_allowance_spends_it_alone() {
    let manifest = u64::try_from(public_manifest().len()).unwrap();
    let (gateway, _logs, host) =
        public_gateway_for(Allowance::with_namespaces(u64::MAX, [(7, manifest + 6)])).await;
    hold(&gateway, unsigned_pointer(7, BLOB, 1), Address::ZERO);
    for _ in 0..2 {
        let ok = request(&gateway, "7.decdn.test", "/a.txt", &[]).await;
        assert_eq!((ok.status, ok.body.as_str()), (StatusCode::OK, "abc"));
    }
    let refused = request(&gateway, "7.decdn.test", "/a.txt", &[]).await;
    assert_eq!(refused.status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(
        refused.header(header::LINK),
        Some(
            format!(
                "<decdn:{}/a.txt?ns=7>; rel=\"alternate\"",
                alloy::hex::encode(BLOB)
            )
            .as_str()
        )
    );
    let shared = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!(
        (shared.status, shared.body.as_str()),
        (StatusCode::OK, "abc")
    );
}

/// A public site serves its entries to anyone: no token, no browser
/// headers, the operator's pool pays.
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

/// A public gateway has no delegates and no capabilities: its API paths
/// answer 404 on every origin, a preflight included.
#[tokio::test]
async fn the_public_gateway_api_is_closed() {
    let (gateway, logs, host) = public_gateway().await;
    for (method, path) in [
        (Method::GET, "/_delegate"),
        (Method::PUT, "/_capability"),
        (Method::GET, "/_capability"),
        (Method::OPTIONS, "/_delegate"),
        (Method::OPTIONS, "/_capability"),
    ] {
        for origin in ["decdn.test", host.as_str()] {
            let answer = send(&gateway, method.clone(), origin, path, &[]).await;
            assert_eq!(
                answer.status,
                StatusCode::NOT_FOUND,
                "{method} {path} on {origin}"
            );
        }
    }
    let landing = request(&gateway, "decdn.test", "/", &[]).await;
    assert_eq!(landing.status, StatusCode::OK);
    assert!(logs.caps.lock().unwrap().is_empty(), "nothing was pulled");
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
}

/// A public gateway answers `HEAD` from the bundle manifest: the entry's
/// length and hash, with no pull of the entry itself. A raw blob's `HEAD`
/// costs no pull either, and has no length to give.
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
    assert_eq!(raw.header(header::CONTENT_LENGTH), None);
    assert_eq!(
        logs.caps.lock().unwrap().len(),
        1,
        "a raw blob's HEAD pulls nothing"
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

/// A key whose allowance is spent answers 402 before any pull, naming the
/// canonical `decdn:` URI so a paying client continues on its own; the
/// response that crosses the limit still completes.
#[tokio::test]
async fn an_exhausted_allowance_answers_402_with_the_uri() {
    // Exactly the manifest and one entry: the first request spends it all.
    let allowance = u64::try_from(public_manifest().len()).unwrap() + 3;
    let (gateway, logs, host) = public_gateway_with(allowance).await;
    let first = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!((first.status, first.body.as_str()), (StatusCode::OK, "abc"));
    let pulls = logs.caps.lock().unwrap().len();

    let refused = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!(refused.status, StatusCode::PAYMENT_REQUIRED);
    let uri = format!("decdn:{}/a.txt", alloy::hex::encode(BLOB));
    assert_eq!(
        refused.header(header::LINK),
        Some(format!("<{uri}>; rel=\"alternate\"").as_str())
    );
    assert!(refused.header(header::RETRY_AFTER).is_some());
    assert_eq!(
        refused.header(header::CONTENT_TYPE),
        Some("application/json")
    );
    let body: serde_json::Value = serde_json::from_str(&refused.body).unwrap();
    assert_eq!(body["uri"], uri);
    assert!(body["retry_after_secs"].is_u64(), "{body}");
    assert_eq!(
        logs.caps.lock().unwrap().len(),
        pulls,
        "a refused request pulls nothing"
    );
}

/// Every request spends the one allowance, whatever namespace hint it
/// carries: the hint is the client's to set, so it buys no bucket of its own.
#[tokio::test]
async fn every_request_spends_the_one_allowance() {
    let (gateway, _, _) = public_gateway_with(10).await;
    let other = alloy::hex::encode(OTHER);
    let first = request(&gateway, "decdn.test", &format!("/{other}"), &[]).await;
    assert_eq!(first.status, StatusCode::OK);
    for (host, path) in [
        ("decdn.test", format!("/{other}")),
        ("decdn.test", format!("/{other}?ns=7")),
        (
            &*gateway.sites.site_host(&Site {
                hash: BLOB,
                namespace: Some(7),
            }),
            "/a.txt".to_owned(),
        ),
    ] {
        let spent = request(&gateway, host, &path, &[]).await;
        assert_eq!(spent.status, StatusCode::PAYMENT_REQUIRED, "{host}{path}");
    }
}

/// An entry whose size the manifest gives, and that does not fit what is
/// left, is refused before its pull: the manifest is read, the entry never.
#[tokio::test]
async fn an_entry_that_does_not_fit_is_refused_before_its_pull() {
    let (gateway, logs, host) = public_gateway_with(2).await;
    let refused = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!(refused.status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(
        *logs.caps.lock().unwrap(),
        [MAX_MANIFEST_MB],
        "only the manifest was pulled"
    );
    assert_eq!(
        refused.header(header::LINK),
        Some(
            format!(
                "<decdn:{}/a.txt>; rel=\"alternate\"",
                alloy::hex::encode(BLOB)
            )
            .as_str()
        )
    );
}

/// A stream whose size nobody knew is cut once the allowance runs out: the
/// frames already paid for go out, then the body ends with an error, never a
/// quiet short file. A stream whose last frame reaches the bound ends whole.
#[tokio::test]
async fn a_stream_is_cut_when_the_allowance_runs_out() {
    // A 600 KiB blob streams in three frames of 256 KiB; the second reaches
    // a 300 KiB allowance and the third never goes out.
    let (gateway, _, _) = public_gateway_with(300 * 1024).await;
    let big = alloy::hex::encode(BIG);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/{big}"))
        .header(header::HOST, "decdn.test")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = Arc::clone(&gateway).handle(req).await;
    assert_eq!(response.status(), StatusCode::OK, "the pull was admitted");
    let body = response.into_body().collect().await;
    assert!(
        body.is_err(),
        "the body ends with an error once the allowance is spent"
    );
    let next = request(&gateway, "decdn.test", &format!("/{big}"), &[]).await;
    assert_eq!(next.status, StatusCode::PAYMENT_REQUIRED);

    // Eleven bytes against ten: the one frame reaches the bound and the body
    // still ends whole.
    let (gateway, _, _) = public_gateway_with(10).await;
    let other = alloy::hex::encode(OTHER);
    let whole = request(&gateway, "decdn.test", &format!("/{other}"), &[]).await;
    assert_eq!(
        (whole.status, whole.body.as_str()),
        (StatusCode::OK, "other bytes")
    );
}

/// `HEAD` reads the manifest, a paid pull, so it needs the allowance too.
#[tokio::test]
async fn a_public_head_needs_the_allowance() {
    let (gateway, logs, host) = public_gateway_with(10).await;
    let other = alloy::hex::encode(OTHER);
    let spend = request(&gateway, "decdn.test", &format!("/{other}"), &[]).await;
    assert_eq!(spend.status, StatusCode::OK);
    let pulls = logs.caps.lock().unwrap().len();
    let head = send(&gateway, Method::HEAD, &host, "/a.txt", &[]).await;
    assert_eq!(head.status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(logs.caps.lock().unwrap().len(), pulls, "no manifest pull");
}

/// When the gateway's own pool cannot pay, the client did nothing wrong: 503
/// with the URI and a retry hint, never 402.
#[tokio::test]
async fn a_pool_that_cannot_pay_answers_503() {
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

/// A manifest pull the pool cannot pay for says nothing of the blob, so the
/// next request pulls again.
#[tokio::test]
async fn a_manifest_pull_stopped_for_funding_is_not_remembered() {
    let (gateway, logs, _) = public_gateway().await;
    let host = gateway.sites.site_host(&Site {
        hash: UNFUNDED,
        namespace: None,
    });
    for _ in 0..2 {
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

/// A manifest read that one namespace's allowance stopped says nothing of
/// the blob: the same bundle on the shared allowance still serves.
#[tokio::test]
async fn a_manifest_read_stopped_by_the_allowance_is_not_remembered() {
    let (gateway, _logs, host) =
        public_gateway_for(Allowance::with_namespaces(u64::MAX, [(7, 1)])).await;
    hold(&gateway, unsigned_pointer(7, BLOB, 1), Address::ZERO);
    let refused = request(&gateway, "7.decdn.test", "/a.txt", &[]).await;
    assert_eq!(refused.status, StatusCode::PAYMENT_REQUIRED);
    let served = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!(
        (served.status, served.body.as_str()),
        (StatusCode::OK, "abc")
    );
}

/// What the public allowance has left right now.
fn left(gateway: &Gateway) -> u64 {
    gateway
        .public
        .as_ref()
        .unwrap()
        .allowance
        .remaining(None, now_secs())
        .unwrap_or(0)
}

/// A blob read as a manifest that turns out not to be one was still pulled
/// and paid for: its bytes are charged. A manifest read that ends short (the
/// blob ran past the manifest cap) is also charged the read-ahead the pull
/// may have paid past the bytes that landed.
#[tokio::test]
async fn a_blob_that_is_no_manifest_is_still_charged() {
    let allowance = 64 << 20;
    let (gateway, _, _) = public_gateway_with(allowance).await;
    let site = gateway.sites.site_host(&Site {
        hash: OTHER,
        namespace: None,
    });
    let answer = request(&gateway, &site, "/x", &[]).await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
    let after_whole = allowance - "other bytes".len() as u64;
    assert_eq!(left(&gateway), after_whole, "a whole read costs its bytes");

    let site = gateway.sites.site_host(&Site {
        hash: PARTIAL,
        namespace: None,
    });
    let answer = request(&gateway, &site, "/x", &[]).await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
    assert_eq!(
        left(&gateway),
        after_whole - "partial".len() as u64 - DROP_PENALTY_BYTES,
        "a short read costs its bytes and the read-ahead bound"
    );
}

/// A response dropped mid-stream is charged the frames it took and the
/// read-ahead the pull paid for beyond them, so a client that connects and
/// drops spends no more than it is charged. A stream read to its end is
/// charged its bytes alone.
#[tokio::test]
async fn a_dropped_response_is_charged_its_read_ahead() {
    let allowance = 64 << 20;
    let (gateway, _, _) = public_gateway_with(allowance).await;
    let big = alloy::hex::encode(BIG);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/{big}"))
        .header(header::HOST, "decdn.test")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = Arc::clone(&gateway).handle(req).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let first = body.frame().await.unwrap().unwrap();
    let taken = first.data_ref().unwrap().len() as u64;
    assert!(taken > 0 && taken < BIG_LEN as u64);
    drop(body);
    assert_eq!(left(&gateway), allowance - taken - DROP_PENALTY_BYTES);

    let whole = request(&gateway, "decdn.test", &format!("/{big}"), &[]).await;
    assert_eq!(whole.status, StatusCode::OK);
    assert_eq!(whole.body.len(), BIG_LEN);
    assert_eq!(
        left(&gateway),
        allowance - taken - DROP_PENALTY_BYTES - BIG_LEN as u64
    );
}

/// A body with a known length is dropped by the HTTP server once that many
/// bytes are out, without a final poll: delivered whole, it costs its bytes
/// and no penalty.
#[tokio::test]
async fn a_body_delivered_to_its_known_length_pays_no_penalty() {
    let allowance = 64 << 20;
    let (gateway, _, host) = public_gateway_with(allowance).await;
    let req = Request::builder()
        .method(Method::GET)
        .uri("/a.txt")
        .header(header::HOST, &host)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = Arc::clone(&gateway).handle(req).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "3");
    let mut body = response.into_body();
    let first = body.frame().await.unwrap().unwrap();
    assert_eq!(first.data_ref().unwrap().len(), 3);
    // As hyper does: the declared length is out, the stream is dropped.
    drop(body);
    assert_eq!(
        left(&gateway),
        allowance - public_manifest().len() as u64 - 3
    );
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

/// A generated listing has a known length too: one that does not fit what is
/// left of the allowance is refused, not sent.
#[tokio::test]
async fn a_listing_that_does_not_fit_the_allowance_is_refused() {
    let allowance = u64::try_from(noindex_manifest().len()).unwrap() + 1;
    let (gateway, _, _) = public_gateway_with(allowance).await;
    let host = gateway.sites.site_host(&Site {
        hash: NOINDEX,
        namespace: None,
    });
    let listing = request(&gateway, &host, "/", &[]).await;
    assert_eq!(listing.status, StatusCode::PAYMENT_REQUIRED);
}

/// A manifest read stops at the allowance: a blob that is no manifest, read
/// with a few bytes left, is not drained to the manifest cap. The request is
/// refused, and the read-ahead the pull may have paid counts.
#[tokio::test]
async fn a_manifest_read_stops_at_the_allowance() {
    let allowance = 100;
    let (gateway, _, _) = public_gateway_with(allowance).await;
    let host = gateway.sites.site_host(&Site {
        hash: BIG,
        namespace: None,
    });
    let answer = request(&gateway, &host, "/x", &[]).await;
    assert_eq!(
        answer.status,
        StatusCode::PAYMENT_REQUIRED,
        "{}",
        answer.body
    );
    assert_eq!(left(&gateway), 0);
}

/// A request dropped while its pull waits for a first byte still pays for
/// the read-ahead that pull may have taken: the penalty is charged however
/// early the client leaves.
#[tokio::test]
async fn a_request_dropped_before_its_first_byte_is_charged() {
    let allowance = 64 << 20;
    let (gateway, _, _) = public_gateway_with(allowance).await;
    let held = alloy::hex::encode(HELD);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/{held}"))
        .header(header::HOST, "decdn.test")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let dropped =
        tokio::time::timeout(Duration::from_millis(200), Arc::clone(&gateway).handle(req)).await;
    assert!(
        dropped.is_err(),
        "the pull never answers, so the request is dropped"
    );
    assert_eq!(left(&gateway), allowance - DROP_PENALTY_BYTES);
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
/// and a blob whose size nothing knows all get the whole body as 200.
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

/// The allowance admits a range by the span the pull pays for: the window
/// widened to whole chunk groups. A window whose span fits is served where
/// the whole entry would be refused, and one whose span does not fit is
/// refused even when the window itself would.
#[tokio::test]
async fn a_range_is_admitted_by_its_aligned_span() {
    let manifest = u64::try_from(public_manifest().len()).unwrap();
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let (gateway, _, host) = public_gateway_with(manifest + group).await;
    let whole = request(&gateway, &host, "/big.bin", &[]).await;
    assert_eq!(whole.status, StatusCode::PAYMENT_REQUIRED);
    let part = request(&gateway, &host, "/big.bin", &[("range", "bytes=0-1")]).await;
    assert_eq!(part.status, StatusCode::PARTIAL_CONTENT);

    let (gateway, _, host) = public_gateway_with(manifest + 100).await;
    let part = request(&gateway, &host, "/big.bin", &[("range", "bytes=0-1")]).await;
    assert_eq!(
        part.status,
        StatusCode::PAYMENT_REQUIRED,
        "two bytes cost a chunk group"
    );
}

/// A range is charged the span the pull pays for, not the bytes the client
/// reads: two bytes inside one chunk group cost the group, and two bytes
/// across a group boundary cost two.
#[tokio::test]
async fn a_range_is_charged_by_its_aligned_span() {
    let allowance = 64 << 20;
    let manifest = u64::try_from(public_manifest().len()).unwrap();
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let (gateway, _, host) = public_gateway_with(allowance).await;
    let part = request(&gateway, &host, "/big.bin", &[("range", "bytes=0-1")]).await;
    assert_eq!(
        (part.status, part.body.len()),
        (StatusCode::PARTIAL_CONTENT, 2)
    );
    assert_eq!(left(&gateway), allowance - manifest - group);
    let straddling = format!("bytes={}-{}", group - 1, group);
    let part = request(&gateway, &host, "/big.bin", &[("range", &straddling)]).await;
    assert_eq!(
        (part.status, part.body.len()),
        (StatusCode::PARTIAL_CONTENT, 2)
    );
    assert_eq!(left(&gateway), allowance - manifest - 3 * group);
}

/// The metrics of the gateway under test.
fn metrics(gateway: &Gateway) -> String {
    gateway.metrics.render()
}

/// A loopback gateway counts its requests too: `/_status` reads the same
/// counters on every gateway.
#[tokio::test]
async fn a_loopback_gateway_counts_its_requests() {
    let gateway = test_gateway().await;
    call(&gateway, Method::GET, "/nope", &[], "").await;
    call(&gateway, Method::DELETE, "/", &[], "").await;
    let c = gateway.metrics.snapshot();
    let answered = c.requests_ok
        + c.requests_not_found
        + c.requests_refused
        + c.requests_allowance_exhausted
        + c.pull_failures_funding
        + c.pull_failures_other;
    assert_eq!(answered, 2, "{c:?}");
}

/// Every answer counts under the sibling its outcome names, and the bytes a
/// response delivers count as they go out.
#[tokio::test]
async fn requests_are_counted_by_outcome() {
    let allowance = u64::try_from(public_manifest().len()).unwrap() + 3;
    let (gateway, _, host) = public_gateway_with(allowance).await;
    // The miss first: it reads the manifest, which the entry then finds
    // cached, so the allowance is spent exactly by the entry.
    let missing = request(&gateway, &host, "/nope.txt", &[]).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    let ok = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!(ok.status, StatusCode::OK);
    let spent = request(&gateway, &host, "/a.txt", &[]).await;
    assert_eq!(spent.status, StatusCode::PAYMENT_REQUIRED);
    let refused = request(&gateway, "evil.test", "/", &[]).await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    let text = metrics(&gateway);
    for line in [
        "decdn_gateway_requests_ok_total 1",
        "decdn_gateway_requests_not_found_total 1",
        "decdn_gateway_requests_allowance_exhausted_total 1",
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

/// A pool that cannot pay and an upstream fault count as pull failures.
#[tokio::test]
async fn pull_failures_are_counted_by_cause() {
    let (gateway, _logs, _) = public_gateway().await;
    let unfunded = alloy::hex::encode(UNFUNDED);
    let answer = request(&gateway, "decdn.test", &format!("/{unfunded}"), &[]).await;
    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    let unknown = alloy::hex::encode([0x11; 32]);
    let answer = request(&gateway, "decdn.test", &format!("/{unknown}"), &[]).await;
    assert_eq!(answer.status, StatusCode::BAD_GATEWAY);
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
    // The state answers here, with no token; no page does.
    for path in ["/", "/_dashboard/app.js"] {
        let page = fetch(path).await;
        assert!(page.starts_with("HTTP/1.1 404"), "{path}: {page}");
    }
    let status = fetch("/_status").await;
    assert!(status.starts_with("HTTP/1.1 200"), "{status}");
    let body = status.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    let status: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(status["mode"], "public");
    assert!(status["allowance"].is_array(), "{status}");
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
    // only the missing CORS grant keeps it from reading the token-free state.
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
const READABLE: &str = "Content-Length, Content-Range, Accept-Ranges, Content-Type, \
                        Content-Disposition, ETag, Retry-After, Link";

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
/// 402, 416, 503 and 404 refusals.
#[tokio::test]
async fn every_public_answer_is_readable_by_any_origin() {
    let allowance = u64::try_from(public_manifest().len()).unwrap() + 3;
    let (gateway, _logs, host) = public_gateway_with(allowance).await;
    let origin = [("origin", "https://app.example")];
    let ok = request(&gateway, &host, "/a.txt", &origin).await;
    let spent = request(&gateway, &host, "/a.txt", &origin).await;
    let (gateway, _logs, host) = public_gateway().await;
    hold(&gateway, unsigned_pointer(7, BLOB, 1), Address::ZERO);
    let unfunded = format!("/{}", alloy::hex::encode(UNFUNDED));
    let blob = format!("/{}", alloy::hex::encode(OTHER));
    let bundle = format!("/{}/", alloy::hex::encode(BLOB));
    let window = [("origin", "https://app.example"), ("range", "bytes=1-2")];
    let past = [("origin", "https://app.example"), ("range", "bytes=3-")];
    let answers = [
        ok,
        spent,
        request(&gateway, &host, "/a.txt", &window).await,
        request(&gateway, &host, "/a.txt", &past).await,
        request(&gateway, "decdn.test", &unfunded, &origin).await,
        request(&gateway, &host, "/missing.txt", &origin).await,
        send(&gateway, Method::HEAD, &host, "/a.txt", &origin).await,
        send(&gateway, Method::HEAD, "decdn.test", &blob, &origin).await,
        request(&gateway, "decdn.test", &bundle, &origin).await,
        request(&gateway, "7.decdn.test", "/a.txt", &origin).await,
        request(&gateway, "8.decdn.test", "/a.txt", &origin).await,
    ];
    let statuses: Vec<_> = answers.iter().map(|a| a.status).collect();
    assert_eq!(
        statuses,
        [
            StatusCode::OK,
            StatusCode::PAYMENT_REQUIRED,
            StatusCode::PARTIAL_CONTENT,
            StatusCode::RANGE_NOT_SATISFIABLE,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::NOT_FOUND,
            StatusCode::OK,
            StatusCode::OK,
            StatusCode::FOUND,
            StatusCode::OK,
            StatusCode::NOT_FOUND,
        ]
    );
    for answer in &answers {
        assert!(readable_anywhere(answer), "{}", answer.status);
        assert_eq!(answer.header(header::VARY), None, "`*` needs no Vary");
    }
}

/// A loopback gateway keeps content same-origin: no CORS header on content
/// and no preflight outside its API, so no page on another origin can send
/// the token header to content.
#[tokio::test]
async fn a_loopback_gateway_answers_content_to_no_other_origin() {
    let (gateway, host) = site_gateway().await;
    let mut headers = SAME_ORIGIN.to_vec();
    headers.push(("origin", "https://app.example"));
    let site = request(&gateway, &host, "/css/site.css", &headers).await;
    assert_eq!(site.status, StatusCode::OK);
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

/// `GET /_capability` answers this exact body for one registered capability:
/// the TypeScript SDK reads it, so its keys and spelling are the contract.
#[tokio::test]
async fn the_capability_list_keeps_its_shape() {
    let gateway = test_gateway().await;
    let mut held = registered("decdn.org", Some("https://decdn.org"), Instant::now());
    held.grant.signer = Address::repeat_byte(0xab);
    held.grant.pool_id = alloy::primitives::B256::repeat_byte(0x01);
    held.grant.spending_cap = 5_000;
    held.grant.expiry = 1_900_000_000;
    held.spent = true;
    insert(
        &mut *gateway.delegates.lock().await,
        delegate(None, Some(held)),
    );
    let answer = send(
        &gateway,
        Method::GET,
        "127.0.0.1:8484",
        "/_capability",
        &[(TOKEN_HEADER, TEST_TOKEN)],
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(
        answer.body,
        format!(
            "[{{\"cap\":5000,\"expiry\":1900000000,\"origin\":\"https://decdn.org\",\"pool\":\
             \"0x{}\",\"scope\":\"decdn.org\",\"signer\":\"{}\",\"spent\":true}}]",
            "01".repeat(32),
            Address::repeat_byte(0xab)
        )
    );
}

/// A pull in flight is listed with its hash and payer, and leaves the list
/// when its request ends.
#[tokio::test]
async fn an_active_pull_is_listed_while_it_streams() {
    let (gateway, _, _) = public_gateway().await;
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
    assert!(matches!(pull.payer, PayerLabel::Pool), "{:?}", pull.payer);
    assert_eq!(pull.delivered_bytes, 0);
    pending.abort();
    let _ = pending.await;
    assert!(
        gateway.active_pulls().is_empty(),
        "the dropped request's pull leaves the list"
    );
    assert_eq!(gateway.metrics.snapshot().pulls_active, 0);
}

/// `GET path` on the loopback host, with the token when `token`.
async fn loopback_get(gateway: &Arc<Gateway>, path: &str, token: bool) -> Answer {
    let headers: &[(&str, &str)] = if token {
        &[(TOKEN_HEADER, TEST_TOKEN)]
    } else {
        &[]
    };
    send(gateway, Method::GET, "127.0.0.1:8484", path, headers).await
}

/// `/_status` on a loopback gateway takes the token, as `/_capability` does,
/// and lists the same capabilities. A loopback gateway has no allowance and
/// no pointers, so both are null.
#[tokio::test]
async fn status_needs_the_token_on_loopback() {
    let gateway = test_gateway().await;
    insert(
        &mut *gateway.delegates.lock().await,
        delegate(None, Some(registered("*", None, Instant::now()))),
    );
    let refused = loopback_get(&gateway, "/_status", false).await;
    assert_eq!(refused.status, StatusCode::UNAUTHORIZED);
    let answer = loopback_get(&gateway, "/_status", true).await;
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
    assert!(status["allowance"].is_null(), "{status}");
    assert!(status["pointers"].is_null(), "{status}");
    assert!(status["pool_error"].is_null(), "{status}");
    assert!(status["pools"].is_object(), "{status}");
    // The 401 before it is the dashboard's own request, and not counted.
    assert_eq!(status["counters"]["requests_refused"], 0, "{status}");
    let listed = loopback_get(&gateway, "/_capability", true).await;
    let listed: serde_json::Value = serde_json::from_str(&listed.body).unwrap();
    assert_eq!(status["capabilities"], listed);
    // A page cannot send the token header cross-origin: no preflight passes.
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
    let root = loopback_get(&gateway, "/", false).await;
    assert_eq!(root.status, StatusCode::OK);
    assert!(root.body.contains("decdn dashboard"), "{}", root.body);
    assert!(root.header(header::CONTENT_SECURITY_POLICY).is_none());
    for path in ["/_dashboard/app.js", "/_dashboard/app.css"] {
        let asset = loopback_get(&gateway, path, false).await;
        assert_ne!(asset.status, StatusCode::OK, "{path}");
    }
}

/// A public gateway serves no state on its public host: it lives on the
/// metrics listener. `/` keeps its stub.
#[tokio::test]
async fn status_is_404_on_a_public_bare_host() {
    let (gateway, _logs, host) = public_gateway().await;
    let answer = request(&gateway, "decdn.test", "/_status", &[]).await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
    let preflight = send(&gateway, Method::OPTIONS, "decdn.test", "/_status", &[]).await;
    assert_eq!(preflight.status, StatusCode::NOT_FOUND);
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

/// A public gateway's status lists each allowance bucket and each pointer
/// it holds.
#[tokio::test]
async fn status_lists_a_stored_pointer_and_its_bucket() {
    let (gateway, _logs, _) =
        public_gateway_for(Allowance::with_namespaces(1_000, [(7, 500)])).await;
    let owner = Address::repeat_byte(0x77);
    hold(&gateway, unsigned_pointer(7, BLOB, 3), owner);
    let status = serde_json::to_value(gateway.status().await).unwrap();
    assert_eq!(status["mode"], "public");
    let buckets = status["allowance"].as_array().unwrap();
    assert_eq!(buckets.len(), 2);
    assert!(buckets[0]["namespace"].is_null());
    assert_eq!(buckets[0]["per_day"], 1_000);
    assert_eq!(buckets[1]["namespace"], 7);
    assert_eq!(buckets[1]["per_day"], 500);
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
        _delegates: &[(PoolId, Address)],
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

/// A loopback gateway's metrics listener asks for the token before it lists
/// the capabilities, as the gateway's own host does: the token-free state is
/// for a public gateway's operator alone.
#[tokio::test]
async fn a_loopback_metrics_listener_needs_the_token_for_status() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let gateway = test_gateway().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_metrics(Arc::clone(&gateway), listener));
    let fetch = |path: &'static str, token: bool| async move {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let header = if token {
            format!("{TOKEN_HEADER}: {TEST_TOKEN}\r\n")
        } else {
            String::new()
        };
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n{header}Connection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut out = String::new();
        stream.read_to_string(&mut out).await.unwrap();
        out
    };
    let refused = fetch("/_status", false).await;
    assert!(refused.starts_with("HTTP/1.1 401"), "{refused}");
    let status = fetch("/_status", true).await;
    assert!(status.starts_with("HTTP/1.1 200"), "{status}");
    let metrics = fetch("/metrics", false).await;
    assert!(metrics.starts_with("HTTP/1.1 200"), "{metrics}");
}

/// A dashboard's polls are not what the gateway served: each poll of
/// `/_status`, refused or answered, leaves the counters alone.
#[tokio::test]
async fn the_dashboard_does_not_count_itself() {
    let gateway = test_gateway().await;
    for (path, token) in [("/_status", false), ("/_status", true), ("/_status", true)] {
        loopback_get(&gateway, path, token).await;
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
    let payer = serde_json::to_value(PayerLabel::Capability { signer: mixed }).unwrap();
    assert_eq!(payer["signer"], mixed.to_string());
    let pool = alloy::primitives::B256::repeat_byte(0x01);
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
        gateway.chain.payment_pool.to_string()
    );
}
