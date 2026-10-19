use semver::Version;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

fn v(s: &str) -> Version {
    Version::parse(s).unwrap()
}

/// `SHA256SUMS` is GNU text format: 64 lowercase hex, two spaces, the bare
/// file name. A `*name` binary marker and CRLF line ends are tolerated, as
/// the installer's `awk` tolerates them.
#[test]
fn manifest_parses_gnu_text_lines() {
    let text = format!(
        "{a}  decdn-0.2.0-x86_64-unknown-linux-gnu.tar.gz\r\n{b}  *decdn-0.2.0-x86_64-pc-windows-msvc.zip\n",
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
    let linux = Target::new("x86_64-unknown-linux-gnu", ArchiveKind::TarGz);
    assert_eq!(
        linux.archive_name(&v("0.2.0")),
        "decdn-0.2.0-x86_64-unknown-linux-gnu.tar.gz"
    );
    let windows = Target::new("aarch64-pc-windows-msvc", ArchiveKind::Zip);
    assert_eq!(
        windows.archive_name(&v("0.2.0")),
        "decdn-0.2.0-aarch64-pc-windows-msvc.zip"
    );
}

/// This test binary runs on a release target, so the build knows its triple.
#[cfg(all(target_arch = "x86_64", target_os = "linux", target_env = "gnu"))]
#[test]
fn current_target_is_the_linux_release_triple() {
    let target = Target::current().unwrap();
    assert_eq!(target.triple(), "x86_64-unknown-linux-gnu");
    assert_eq!(target.kind(), ArchiveKind::TarGz);
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
    assert!(err.to_string().contains("larger than"), "{err}");
    let err = fetch_asset(&clients, &server.uri(), "v9.9.9", "missing", 100)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("404"), "{err}");
}
