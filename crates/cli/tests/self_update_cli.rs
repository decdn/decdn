//! CLI-level checks for `decdn self-update`: the refusals, and `--check`
//! against a release the build does not trust.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::path::{Path, PathBuf};

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SUMS: &[u8] = include_bytes!("../src/commands/self_update/testdata/SHA256SUMS");
const SUMS_SIG: &[u8] = include_bytes!("../src/commands/self_update/testdata/SHA256SUMS.minisig");

/// Copy the built `decdn` into `dir` and return its path.
fn install_copy(dir: &Path) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let exe = dir.join("decdn");
    std::fs::copy(env!("CARGO_BIN_EXE_decdn"), &exe).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    exe
}

/// A release page whose "latest" is `tag`, serving the fixture manifest and
/// its signature (signed by the test key, which no build trusts).
async fn serve_release(tag: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/releases/latest/download/SHA256SUMS"))
        .respond_with(ResponseTemplate::new(302).insert_header(
            "location",
            format!("{}/releases/download/{tag}/SHA256SUMS", server.uri()).as_str(),
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/releases/download/{tag}/SHA256SUMS")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(SUMS))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/releases/download/{tag}/SHA256SUMS.minisig")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(SUMS_SIG))
        .mount(&server)
        .await;
    server
}

async fn run(exe: &Path, home: &Path, base: &str, args: &[&str]) -> std::process::Output {
    tokio::process::Command::new(exe)
        .arg("self-update")
        .args(args)
        .env("HOME", home)
        .env("DECDN_RELEASES_BASE", base)
        .env_remove("CARGO_HOME")
        .output()
        .await
        .unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A binary under `~/.cargo/bin` belongs to cargo; nothing is fetched.
#[tokio::test(flavor = "multi_thread")]
async fn a_cargo_install_is_refused_with_the_cargo_command() {
    let home = tempfile::tempdir().unwrap();
    let exe = install_copy(&home.path().join(".cargo").join("bin"));
    let out = run(&exe, home.path(), "http://127.0.0.1:9", &[]).await;
    assert!(!out.status.success());
    let err = text(&out.stderr);
    assert!(err.contains("cargo install --locked decdn-cli"), "{err}");
}

/// A binary in a directory the user cannot write to refuses by path.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn an_unwritable_directory_is_refused_by_path() {
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir().unwrap();
    let dir = home.path().join("opt").join("bin");
    let exe = install_copy(&dir);
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::File::create(dir.join("probe")).is_ok() {
        // Running as root: the directory is writable regardless.
        return;
    }
    let out = run(&exe, home.path(), "http://127.0.0.1:9", &[]).await;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(!out.status.success());
    let err = text(&out.stderr);
    assert!(err.contains(exe.to_str().unwrap()), "{err}");
}

/// The fixtures are signed by the test key. The build embeds the maintainer
/// key only, so `--check` refuses the release by name and leaves the binary
/// alone. This is the proof that the embedded key set is what gates a swap.
#[tokio::test(flavor = "multi_thread")]
async fn check_refuses_a_manifest_signed_by_an_untrusted_key() {
    let home = tempfile::tempdir().unwrap();
    let exe = install_copy(&home.path().join(".local").join("bin"));
    let before = std::fs::read(&exe).unwrap();
    let server = serve_release("v99.0.0").await;
    let out = run(&exe, home.path(), &server.uri(), &["--check"]).await;
    assert!(!out.status.success());
    let err = text(&out.stderr);
    assert!(
        err.contains("not signed by a key this decdn build trusts"),
        "{err}"
    );
    assert_eq!(std::fs::read(&exe).unwrap(), before);
}

/// A "latest" that equals the running version is up to date; one older than
/// it is refused as a downgrade. Neither downloads a manifest.
#[tokio::test(flavor = "multi_thread")]
async fn check_reports_up_to_date_and_refuses_a_downgrade() {
    let home = tempfile::tempdir().unwrap();
    let exe = install_copy(&home.path().join(".local").join("bin"));

    let same = serve_release(concat!("v", env!("CARGO_PKG_VERSION"))).await;
    let out = run(&exe, home.path(), &same.uri(), &["--check"]).await;
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains("is up to date"),
        "{}",
        text(&out.stdout)
    );

    let older = serve_release("v0.0.1").await;
    let out = run(&exe, home.path(), &older.uri(), &["--check"]).await;
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains("older than this build"),
        "{}",
        text(&out.stdout)
    );
    assert_eq!(older.received_requests().await.unwrap().len(), 1);
}
