use super::*;

fn manifest() -> Arc<BundleManifest> {
    Arc::new(BundleManifest::new(Vec::new()))
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

fn pending(minter: Option<&str>) -> Delegate {
    Delegate {
        signer: Arc::new(PrivateKeySigner::random()),
        minted: Instant::now(),
        minter: minter.map(str::to_owned),
        capability: None,
    }
}

#[test]
fn each_minter_keeps_its_own_pending_room() {
    let mut delegates = HashMap::new();
    for _ in 0..MAX_PENDING_PER_MINTER {
        let d = pending(Some("https://evil.example"));
        delegates.insert(d.signer.address(), d);
    }
    assert!(may_mint(&delegates, Some("https://evil.example")).is_err());
    assert!(may_mint(&delegates, Some("https://decdn.org")).is_ok());
    assert!(may_mint(&delegates, None).is_ok());
}

#[test]
fn minting_stops_at_the_global_bound() {
    let mut delegates = HashMap::new();
    for n in 0..MAX_PENDING {
        let d = pending(Some(&format!("https://{n}.example")));
        delegates.insert(d.signer.address(), d);
    }
    assert!(may_mint(&delegates, Some("https://decdn.org")).is_err());
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
                minter: None,
                capability: None,
            },
        );
    }
    Gateway::prune(&mut delegates);
    assert!(delegates.contains_key(&fresh.address()));
    assert!(!delegates.contains_key(&stale.address()));
}
