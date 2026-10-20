use super::*;
use crate::commands::fetch::MAX_MANIFEST_MB;

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
        token: TEST_TOKEN.to_owned(),
        domain: voucher_domain(chain.chain_id, chain.payment_pool),
        chain,
        delegates: Mutex::new(HashMap::new()),
        pulls: PullWorker::start(backend, 1).await.unwrap(),
        manifests: std::sync::Mutex::new(ManifestCache::default()),
        manifest_fill: Mutex::new(()),
        hosts: allowed_hosts("127.0.0.1:8484".parse().unwrap()),
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
    /// The pull writes these bytes, then ends at its cap, as a blob larger
    /// than the cap does once the cap's worth has landed.
    BytesThenTooLarge(Vec<u8>),
    /// The pull writes these bytes and succeeds.
    Bytes(Vec<u8>),
}

/// A pull backend that answers each blob by its hash and records the
/// `--max-blob-mb` each pull runs with.
struct Recording {
    caps: Arc<std::sync::Mutex<Vec<u64>>>,
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

    async fn pull(
        &self,
        (): &(),
        (): &(),
        blob: &BlobFetch,
        sink: &mut super::super::fetch::Sink,
    ) -> anyhow::Result<()> {
        self.caps.lock().unwrap().push(blob.common.max_blob_mb);
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

/// A manifest entry whose hash does not parse is a fixed property of the
/// content-addressed manifest, not an upstream fault: it answers 404 naming
/// the entry, and no pull for the entry starts.
#[tokio::test]
async fn a_malformed_entry_hash_is_not_found() {
    let manifest = br#"{"version": 1, "entries": [{"path": "a.bin", "hash": "zz", "size": 3}]}"#;
    let (gateway, caps) = recording_gateway(Reply::Bytes(manifest.to_vec()), &[]).await;
    let blob = alloy::hex::encode(BLOB);
    for _ in 0..2 {
        let (status, body) = get(&gateway, &format!("/{blob}/a.bin")).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(
            body.contains(r#"bundle entry "a.bin" has a malformed hash"#),
            "{body}"
        );
    }
    assert_eq!(
        *caps.lock().unwrap(),
        [MAX_MANIFEST_MB],
        "only the manifest is pulled, once; the repeat reads the cache"
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
fn a_keystore_password_file_is_dropped_and_key_flags_are_refused() {
    let common = keyless(&flags(&["--keystore-password-file", "/pw"])).unwrap();
    assert!(common.keystore_password_file.is_none());
    for flag in [
        ["--keystore", "/ks"],
        ["--capability", "dcap1:x"],
        ["--capability-file", "/cap"],
    ] {
        assert!(keyless(&flags(&flag)).is_err(), "{flag:?}");
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
