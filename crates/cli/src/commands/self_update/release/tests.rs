use semver::Version;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

fn v(s: &str) -> Version {
    Version::parse(s).unwrap()
}

/// `SHA256SUMS` is GNU `sha256sum` output: 64 lowercase hex, then either two
/// spaces and the name (text mode) or one space, `*` and the name (binary
/// mode). CRLF line ends are tolerated. Both forms are what the installer's
/// `awk` accepts.
#[test]
fn manifest_parses_gnu_text_lines() {
    let text = format!(
        "{a}  decdn-0.2.0-x86_64-unknown-linux-gnu.tar.gz\r\n{b} *decdn-0.2.0-x86_64-pc-windows-msvc.zip\n",
        a = "a".repeat(64),
        b = "b".repeat(64)
    );
    let manifest = Manifest::parse(&text).unwrap();
    assert_eq!(
        manifest.digest_for("decdn-0.2.0-x86_64-unknown-linux-gnu.tar.gz"),
        Some("a".repeat(64).as_str())
    );
    assert_eq!(
        manifest.digest_for("decdn-0.2.0-x86_64-pc-windows-msvc.zip"),
        Some("b".repeat(64).as_str())
    );
    assert_eq!(manifest.digest_for("missing"), None);
}

/// A line that is not `<64 hex>  <name>` is an error, never skipped: a
/// manifest the client cannot read in full is a manifest it must not act on.
#[test]
fn manifest_rejects_a_malformed_line() {
    let text = format!("{}  ok.tar.gz\nnot a manifest line\n", "a".repeat(64));
    let err = Manifest::parse(&text).unwrap_err();
    assert!(err.to_string().contains("line 2"), "{err}");
    let one_space = format!("{} one-space.tar.gz\n", "a".repeat(64));
    assert!(Manifest::parse(&one_space).is_err());
    let short = format!("{}  short.tar.gz\n", "a".repeat(63));
    assert!(Manifest::parse(&short).is_err());
    let upper = format!("{}  upper.tar.gz\n", "A".repeat(64));
    assert!(Manifest::parse(&upper).is_err());
}

/// GitHub answers `releases/latest/download/<asset>` with a redirect to
/// `releases/download/<tag>/<asset>`; the tag is that path segment.
#[test]
fn tag_from_location_extracts_the_release_tag() {
    assert_eq!(
        tag_from_location("https://github.com/decdn/decdn/releases/download/v0.2.0/SHA256SUMS")
            .as_deref(),
        Some("v0.2.0")
    );
    assert_eq!(
        tag_from_location("https://github.com/decdn/decdn/releases/download/v0.2.0-rc1/SHA256SUMS")
            .as_deref(),
        Some("v0.2.0-rc1")
    );
    assert_eq!(
        tag_from_location("/decdn/decdn/releases/download/v1.0.0/SHA256SUMS").as_deref(),
        Some("v1.0.0")
    );
}

#[test]
fn tag_from_location_rejects_a_non_release_path() {
    assert_eq!(
        tag_from_location("https://github.com/decdn/decdn/releases"),
        None
    );
    assert_eq!(
        tag_from_location("https://github.com/login?return_to=x"),
        None
    );
    assert_eq!(
        tag_from_location("https://github.com/decdn/decdn/releases/download//SHA256SUMS"),
        None
    );
    assert_eq!(tag_from_location("not a url"), None);
}

#[test]
fn version_from_tag_strips_the_v() {
    assert_eq!(version_from_tag("v0.2.0").unwrap(), v("0.2.0"));
    assert_eq!(version_from_tag("v0.2.0-rc1").unwrap(), v("0.2.0-rc1"));
    assert!(version_from_tag("0.2.0").is_err());
    assert!(version_from_tag("vlatest").is_err());
}

/// Only a strictly newer stable release is an upgrade: a pre-release is
/// never offered, an equal version is up to date, an older one is refused.
#[test]
fn decide_orders_versions() {
    assert_eq!(decide(&v("0.1.0"), &v("0.2.0")), Upgrade::Newer);
    assert_eq!(decide(&v("0.2.0"), &v("0.2.0")), Upgrade::UpToDate);
    assert_eq!(decide(&v("0.2.0"), &v("0.1.9")), Upgrade::Downgrade);
    assert_eq!(decide(&v("0.1.0"), &v("0.2.0-rc1")), Upgrade::PreRelease);
    // A pre-release build upgrades to its own stable version.
    assert_eq!(decide(&v("0.2.0-rc1"), &v("0.2.0")), Upgrade::Newer);
}

#[test]
fn archive_name_follows_the_release_layout() {
    assert_eq!(
        Target::X86_64LinuxGnu.archive_name(&v("0.2.0")),
        "decdn-0.2.0-x86_64-unknown-linux-gnu.tar.gz"
    );
    assert_eq!(
        Target::Aarch64WindowsMsvc.archive_name(&v("0.2.0")),
        "decdn-0.2.0-aarch64-pc-windows-msvc.zip"
    );
}

/// The six targets are the release matrix; Windows ships zips, the rest tarballs.
#[test]
fn every_target_names_its_triple_and_archive_kind() {
    let expected = [
        (
            Target::X86_64LinuxGnu,
            "x86_64-unknown-linux-gnu",
            ArchiveKind::TarGz,
        ),
        (
            Target::Aarch64LinuxGnu,
            "aarch64-unknown-linux-gnu",
            ArchiveKind::TarGz,
        ),
        (
            Target::X86_64Darwin,
            "x86_64-apple-darwin",
            ArchiveKind::TarGz,
        ),
        (
            Target::Aarch64Darwin,
            "aarch64-apple-darwin",
            ArchiveKind::TarGz,
        ),
        (
            Target::X86_64WindowsMsvc,
            "x86_64-pc-windows-msvc",
            ArchiveKind::Zip,
        ),
        (
            Target::Aarch64WindowsMsvc,
            "aarch64-pc-windows-msvc",
            ArchiveKind::Zip,
        ),
    ];
    for (target, triple, kind) in expected {
        assert_eq!(target.triple(), triple);
        assert_eq!(target.kind(), kind);
    }
}

/// An override is used as given, minus a trailing slash; blank means default.
#[test]
fn base_from_trims_a_trailing_slash_and_defaults_when_blank() {
    assert_eq!(base_from(None), REPO_BASE);
    assert_eq!(base_from(Some("  ")), REPO_BASE);
    assert_eq!(
        base_from(Some("http://127.0.0.1:8000/")),
        "http://127.0.0.1:8000"
    );
    assert_eq!(
        base_from(Some("http://127.0.0.1:8000")),
        "http://127.0.0.1:8000"
    );
}

/// This test binary runs on a release target, so the build knows its triple.
#[cfg(all(target_arch = "x86_64", target_os = "linux", target_env = "gnu"))]
#[test]
fn current_target_is_the_linux_release_triple() {
    assert_eq!(Target::current(), Some(Target::X86_64LinuxGnu));
}

#[test]
fn running_version_is_the_crate_version() {
    assert_eq!(running_version().unwrap(), v(env!("CARGO_PKG_VERSION")));
}

#[tokio::test]
async fn resolve_latest_reads_the_tag_from_the_redirect() {
    decdn_common::tls::install_crypto_provider();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/releases/latest/download/SHA256SUMS"))
        .respond_with(ResponseTemplate::new(302).insert_header(
            "location",
            format!("{}/releases/download/v9.9.9/SHA256SUMS", server.uri()).as_str(),
        ))
        .mount(&server)
        .await;
    let clients = Clients::build(std::time::Duration::from_secs(5)).unwrap();
    assert_eq!(
        resolve_latest(&clients, &server.uri()).await.unwrap(),
        "v9.9.9"
    );
}

/// A 200 where a redirect is expected means the server is not serving the
/// release layout; nothing is trusted from it.
#[tokio::test]
async fn resolve_latest_refuses_a_direct_answer() {
    decdn_common::tls::install_crypto_provider();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/releases/latest/download/SHA256SUMS"))
        .respond_with(ResponseTemplate::new(200).set_body_string("abc  x.tar.gz\n"))
        .mount(&server)
        .await;
    let clients = Clients::build(std::time::Duration::from_secs(5)).unwrap();
    let err = resolve_latest(&clients, &server.uri()).await.unwrap_err();
    assert!(err.to_string().contains("200"), "{err}");
}

#[tokio::test]
async fn resolve_latest_reports_a_missing_release() {
    decdn_common::tls::install_crypto_provider();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let clients = Clients::build(std::time::Duration::from_secs(5)).unwrap();
    let err = resolve_latest(&clients, &server.uri()).await.unwrap_err();
    assert!(err.to_string().contains("404"), "{err}");
}

#[tokio::test]
async fn fetch_asset_returns_the_body_and_caps_its_size() {
    decdn_common::tls::install_crypto_provider();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/releases/download/v9.9.9/SHA256SUMS"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![7u8; 100]))
        .mount(&server)
        .await;
    let clients = Clients::build(std::time::Duration::from_secs(5)).unwrap();
    let body = fetch_asset(&clients, &server.uri(), "v9.9.9", "SHA256SUMS", 100)
        .await
        .unwrap();
    assert_eq!(body.len(), 100);
    let err = fetch_asset(&clients, &server.uri(), "v9.9.9", "SHA256SUMS", 99)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("declares 100 bytes"), "{err}");
    let err = fetch_asset(&clients, &server.uri(), "v9.9.9", "missing", 100)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("404"), "{err}");
}

/// GitHub answers an asset URL with a redirect to its object store; the
/// download must follow it, and the cap applies to the redirected body.
#[tokio::test]
async fn fetch_asset_follows_the_asset_store_redirect() {
    decdn_common::tls::install_crypto_provider();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/releases/download/v9.9.9/SHA256SUMS"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/objects/abc", server.uri()).as_str()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/objects/abc"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1u8; 50]))
        .mount(&server)
        .await;
    let clients = Clients::build(std::time::Duration::from_secs(5)).unwrap();
    let body = fetch_asset(&clients, &server.uri(), "v9.9.9", "SHA256SUMS", 50)
        .await
        .unwrap();
    assert_eq!(body.len(), 50);
    let err = fetch_asset(&clients, &server.uri(), "v9.9.9", "SHA256SUMS", 49)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("declares 50 bytes"), "{err}");
}

/// A server that sends no Content-Length (chunked) is capped while the body
/// streams, not trusted to stay small.
#[tokio::test]
async fn fetch_asset_caps_a_chunked_body_while_it_streams() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    decdn_common::tls::install_crypto_provider();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await;
        sock.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        for _ in 0..4 {
            sock.write_all(b"40\r\n").await.unwrap();
            sock.write_all(&[7u8; 64]).await.unwrap();
            sock.write_all(b"\r\n").await.unwrap();
        }
        sock.write_all(b"0\r\n\r\n").await.unwrap();
    });
    let clients = Clients::build(std::time::Duration::from_secs(5)).unwrap();
    let err = fetch_asset(&clients, &base, "v9.9.9", "SHA256SUMS", 100)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("body is larger than"), "{err}");
}
