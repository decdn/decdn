use super::*;
use crate::commands::fetch::{MALFORMED_MANIFEST, MAX_MANIFEST_MB};

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
fn allowed_hosts_name_the_listen_port_under_every_loopback_name() {
    let hosts = allowed_hosts("127.0.0.1:8484".parse().unwrap());
    for host in ["127.0.0.1:8484", "localhost:8484", "[::1]:8484"] {
        assert!(hosts.iter().any(|h| h == host), "{host}");
    }
    assert!(!hosts.iter().any(|h| h == "evil.example:8484"));
    assert!(!hosts.iter().any(|h| h == "localhost:9999"));
}

#[test]
fn token_matches_only_the_exact_token() {
    let token = "ab".repeat(32);
    assert!(token_matches(&token, &token));
    assert!(!token_matches(&token, &"ac".repeat(32)));
    assert!(!token_matches(&token, &token[..63]));
    assert!(!token_matches(&token, ""));
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
    test_gateway_in(backend, common, sites, u64::MAX).await
}

/// A gateway on `backend` whose sites live under `sites`: a loopback gateway
/// with [`TEST_TOKEN`], or a public one with no token and `allowance` bytes
/// per key per day.
async fn test_gateway_in(
    backend: impl worker::Backend,
    common: cli::ClientFetchArgs,
    sites: SiteDomain,
    allowance: u64,
) -> Arc<Gateway> {
    let token = matches!(sites, SiteDomain::Loopback { .. }).then(|| TEST_TOKEN.to_owned());
    let public = matches!(sites, SiteDomain::Public { .. }).then(|| Public {
        allowance: Allowance::new(allowance),
    });
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
        manifest_fill: Mutex::new(()),
        hosts: allowed_hosts("127.0.0.1:8484".parse().unwrap()),
        sites,
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

/// A pull backend that answers each blob by its hash and records the
/// `--max-blob-mb` each pull runs with, and the size cap it carries.
struct Recording {
    caps: Log<u64>,
    size_caps: Log<Option<u64>>,
    replies: HashMap<[u8; 32], Reply>,
}

/// A shared, appended log of what the pulls carried.
type Log<T> = Arc<std::sync::Mutex<Vec<T>>>;

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
                tokio::io::AsyncWriteExt::write_all(writer, bytes).await?;
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
    let (status, _) = get(&gateway, &format!("/{blob}/a.bin")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(*caps.lock().unwrap(), [MAX_MANIFEST_MB, MAX_MANIFEST_MB]);

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
    let (gateway, _) = recording_gateway(reply, &["--max-blob-mb", "10"]).await;
    let (status, body) = get(&gateway, &format!("/{}/", alloy::hex::encode(BLOB))).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
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
    let (gateway, _) = recording_gateway(Reply::Bytes(b"not json".to_vec()), &[]).await;
    let blob = alloy::hex::encode(BLOB);
    let (status, body) = get(&gateway, &format!("/{blob}/")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body.contains(&format!("fetch `/{blob}` for the raw blob")),
        "{body}"
    );
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
        [MAX_MANIFEST_MB; 3],
        "each request pulls only the manifest"
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
        over_manifest_cap(PullError::Other(too_large()), 64, None),
        PullError::NotManifest(_)
    ));
    assert!(matches!(
        over_manifest_cap(PullError::Other(anyhow::anyhow!("no node")), 64, None),
        PullError::Other(_)
    ));
    assert!(matches!(
        over_manifest_cap(PullError::Funding(too_large()), 64, None),
        PullError::Funding(_)
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

/// [`public_gateway`] with `allowance` bytes per key per day.
async fn public_gateway_with(allowance: u64) -> (Arc<Gateway>, Logs, String) {
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
    };
    let backend = Recording {
        caps: Arc::clone(&logs.caps),
        size_caps: Arc::clone(&logs.size_caps),
        replies,
    };
    let sites = SiteDomain::Public {
        origin: "https://decdn.test".parse().unwrap(),
    };
    let gateway = test_gateway_in(backend, flags(&[]), sites, allowance).await;
    let host = gateway.sites.site_host(&Site {
        hash: BLOB,
        namespace: None,
    });
    (gateway, logs, host)
}

/// A public site serves its entries to anyone: no token, no browser
/// headers, the operator's pool pays.
#[tokio::test]
async fn a_public_site_serves_to_anyone() {
    let (gateway, _, host) = public_gateway().await;
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
/// answer 404 on every origin, and nothing answers cross-origin.
#[tokio::test]
async fn the_public_gateway_api_is_closed() {
    let (gateway, logs, host) = public_gateway().await;
    for (method, path) in [
        (Method::GET, "/_delegate"),
        (Method::PUT, "/_capability"),
        (Method::GET, "/_capability"),
        (Method::OPTIONS, "/_delegate"),
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
    let (gateway, _, host) = public_gateway().await;
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
    assert_eq!(head.header(header::ACCEPT_RANGES), None);
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
    let (gateway, _, _) = public_gateway().await;
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
    let (gateway, _, _) = public_gateway().await;
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
    let (gateway, _, _) = public_gateway().await;
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

/// What the public allowance has left right now.
fn left(gateway: &Gateway) -> u64 {
    gateway
        .public
        .as_ref()
        .unwrap()
        .allowance
        .remaining(now_secs())
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
    let (gateway, _, host) = public_gateway().await;
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
    let (gateway, _, _) = public_gateway().await;
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
