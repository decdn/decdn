use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use minisign_verify::PublicKey;
use semver::Version;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::release::{Clients, Target};
use super::*;

const TEST_PUB: &str = include_str!("testdata/minisign-test.pub");
const SUMS: &[u8] = include_bytes!("testdata/install/SHA256SUMS");
const SUMS_SIG: &[u8] = include_bytes!("testdata/install/SHA256SUMS.minisig");
const ARCHIVE: &[u8] =
    include_bytes!("testdata/install/decdn-99.0.0-x86_64-unknown-linux-gnu.tar.gz");
const ARCHIVE_NAME: &str = "decdn-99.0.0-x86_64-unknown-linux-gnu.tar.gz";

/// A release page whose "latest" is `tag`, serving the install fixtures.
async fn serve(tag: &str, archive: &[u8]) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/releases/latest/download/SHA256SUMS"))
        .respond_with(ResponseTemplate::new(302).insert_header(
            "location",
            format!("{}/releases/download/{tag}/SHA256SUMS", server.uri()).as_str(),
        ))
        .mount(&server)
        .await;
    for (name, body) in [
        ("SHA256SUMS", SUMS),
        ("SHA256SUMS.minisig", SUMS_SIG),
        (ARCHIVE_NAME, archive),
    ] {
        Mock::given(method("GET"))
            .and(path(format!("/releases/download/{tag}/{name}")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;
    }
    server
}

/// Records what reached the swap: the staged file's parent and its bytes.
#[derive(Default)]
struct Swapped(Mutex<Option<(std::path::PathBuf, Vec<u8>, u32)>>);

impl Swapped {
    fn record(&self, staged: &tempfile::NamedTempFile) -> anyhow::Result<()> {
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            std::fs::metadata(staged.path())?.permissions().mode() & 0o777
        };
        #[cfg(not(unix))]
        let mode = 0;
        let parent = staged.path().parent().unwrap().to_path_buf();
        let bytes = std::fs::read(staged.path())?;
        *self.0.lock().unwrap() = Some((parent, bytes, mode));
        Ok(())
    }

    fn taken(&self) -> Option<(std::path::PathBuf, Vec<u8>, u32)> {
        self.0.lock().unwrap().clone()
    }
}

struct Fixture {
    keys: Vec<PublicKey>,
    running: Version,
    exe_dir: tempfile::TempDir,
    clients: Clients,
    base: String,
}

impl Fixture {
    fn new(server: &MockServer) -> Self {
        decdn_common::tls::install_crypto_provider();
        let exe_dir = tempfile::tempdir().unwrap();
        std::fs::write(exe_dir.path().join("decdn"), b"old binary").unwrap();
        Self {
            keys: vec![PublicKey::decode(TEST_PUB).unwrap()],
            running: Version::parse("0.1.0").unwrap(),
            exe_dir,
            clients: Clients::build(Duration::from_secs(5)).unwrap(),
            base: server.uri(),
        }
    }

    fn exe(&self) -> std::path::PathBuf {
        self.exe_dir.path().join("decdn")
    }

    fn request<'a>(&'a self, exe: &'a Path, target: Target, check: bool) -> Request<'a> {
        Request {
            keys: &self.keys,
            target,
            running: &self.running,
            exe,
            clients: &self.clients,
            base: &self.base,
            check,
        }
    }
}

async fn requested(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| r.url.path().to_string())
        .collect()
}

#[tokio::test]
async fn apply_hands_the_verified_binary_to_the_swap() {
    let server = serve("v99.0.0", ARCHIVE).await;
    let fx = Fixture::new(&server);
    let exe = fx.exe();
    let swapped = Swapped::default();
    let outcome = apply(&fx.request(&exe, Target::X86_64LinuxGnu, false), |s| {
        swapped.record(&s)
    })
    .await
    .unwrap();
    assert_eq!(
        outcome,
        Outcome::Updated {
            latest: Version::parse("99.0.0").unwrap()
        }
    );
    let (parent, bytes, mode) = swapped.taken().expect("swap ran");
    assert_eq!(parent, fx.exe_dir.path());
    assert_eq!(bytes, b"fixture binary\n");
    if cfg!(unix) {
        assert_eq!(mode, 0o755);
    }
}

/// `--check` verifies the manifest and stops: the archive is never requested
/// and the swap never runs.
#[tokio::test]
async fn apply_with_check_downloads_no_archive() {
    let server = serve("v99.0.0", ARCHIVE).await;
    let fx = Fixture::new(&server);
    let exe = fx.exe();
    let swapped = Swapped::default();
    let outcome = apply(&fx.request(&exe, Target::X86_64LinuxGnu, true), |s| {
        swapped.record(&s)
    })
    .await
    .unwrap();
    assert_eq!(
        outcome,
        Outcome::Available {
            latest: Version::parse("99.0.0").unwrap()
        }
    );
    assert!(swapped.taken().is_none());
    let paths = requested(&server).await;
    assert!(
        paths.iter().any(|p| p.ends_with("/SHA256SUMS.minisig")),
        "{paths:?}"
    );
    assert!(!paths.iter().any(|p| p.ends_with(".tar.gz")), "{paths:?}");
}

/// An archive that does not hash to the signed manifest's digest is refused
/// before anything is staged.
#[tokio::test]
async fn apply_refuses_an_archive_the_manifest_does_not_vouch_for() {
    let mut tampered = ARCHIVE.to_vec();
    if let Some(last) = tampered.last_mut() {
        *last ^= 0x01;
    }
    let server = serve("v99.0.0", &tampered).await;
    let fx = Fixture::new(&server);
    let exe = fx.exe();
    let swapped = Swapped::default();
    let err = apply(&fx.request(&exe, Target::X86_64LinuxGnu, false), |s| {
        swapped.record(&s)
    })
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("does not match SHA256SUMS"),
        "{err}"
    );
    assert!(swapped.taken().is_none());
    assert_eq!(std::fs::read(&exe).unwrap(), b"old binary");
}

/// A release that ships no archive for this target is refused before the
/// archive is requested.
#[tokio::test]
async fn apply_refuses_a_release_without_this_targets_archive() {
    let server = serve("v99.0.0", ARCHIVE).await;
    let fx = Fixture::new(&server);
    let exe = fx.exe();
    let swapped = Swapped::default();
    let err = apply(&fx.request(&exe, Target::X86_64Darwin, false), |s| {
        swapped.record(&s)
    })
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("ships no decdn-99.0.0-x86_64-apple-darwin.tar.gz"),
        "{err}"
    );
    assert!(swapped.taken().is_none());
    assert!(
        !requested(&server)
            .await
            .iter()
            .any(|p| p.ends_with(".tar.gz"))
    );
}

/// An up-to-date binary fetches nothing beyond the tag lookup.
#[tokio::test]
async fn apply_reports_up_to_date_after_one_request() {
    let server = serve("v0.1.0", ARCHIVE).await;
    let fx = Fixture::new(&server);
    let exe = fx.exe();
    let swapped = Swapped::default();
    let outcome = apply(&fx.request(&exe, Target::X86_64LinuxGnu, false), |s| {
        swapped.record(&s)
    })
    .await
    .unwrap();
    assert_eq!(outcome, Outcome::UpToDate);
    assert_eq!(requested(&server).await.len(), 1);
}

/// A manifest signed by a key outside `keys` is refused by name.
#[tokio::test]
async fn apply_refuses_a_manifest_no_given_key_signed() {
    let server = serve("v99.0.0", ARCHIVE).await;
    let mut fx = Fixture::new(&server);
    fx.keys = vec![PublicKey::decode(verify::TRUSTED_KEYS.first().copied().unwrap()).unwrap()];
    let exe = fx.exe();
    let swapped = Swapped::default();
    let err = apply(&fx.request(&exe, Target::X86_64LinuxGnu, false), |s| {
        swapped.record(&s)
    })
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("not signed by a key this decdn build trusts"),
        "{err}"
    );
    assert!(swapped.taken().is_none());
}
