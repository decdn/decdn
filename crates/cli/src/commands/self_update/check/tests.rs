use std::time::Duration;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

const DAY: u64 = 86_400;

/// A missing, unreadable, stale, or future-dated stamp means the check is
/// due; a stamp younger than a day means it is not.
#[test]
fn is_due_when_the_stamp_is_missing_or_a_day_old() {
    let now = 1_000_000;
    assert!(is_due(None, now));
    assert!(is_due(Some("not a number\n"), now));
    assert!(is_due(Some(&format!("{}\n", now - DAY)), now));
    assert!(is_due(Some(&format!("{}\n", now + 5)), now));
    assert!(!is_due(Some(&format!("{}\n", now - DAY + 1)), now));
    assert!(!is_due(Some(&format!("{now}")), now));
}

#[test]
fn env_flag_reads_booleans_and_ignores_the_rest() {
    for on in ["1", "true", "TRUE", "yes", "on", " on "] {
        assert_eq!(env_flag(Some(on)), Some(true), "{on:?}");
    }
    for off in ["0", "false", "no", "OFF"] {
        assert_eq!(env_flag(Some(off)), Some(false), "{off:?}");
    }
    assert_eq!(env_flag(Some("maybe")), None);
    assert_eq!(env_flag(Some("")), None);
    assert_eq!(env_flag(None), None);
}

/// The env var wins; then `[self_update] check`; absent means on. A config
/// file that cannot be read or parsed turns the notice off: silence is the
/// safe side for a best-effort notice.
#[test]
fn check_enabled_applies_env_then_file_then_default() {
    assert!(check_enabled(None, Ok(None)));
    assert!(check_enabled(None, Ok(Some(true))));
    assert!(!check_enabled(None, Ok(Some(false))));
    assert!(!check_enabled(None, Err(())));
    assert!(!check_enabled(Some(false), Ok(Some(true))));
    assert!(check_enabled(Some(true), Ok(Some(false))));
    assert!(check_enabled(Some(true), Err(())));
}

/// A newer release yields the one notice line, and the stamp is written so
/// the next day's run is the next check.
#[tokio::test]
async fn notice_names_the_newer_release_and_stamps_the_check() {
    decdn_common::tls::install_crypto_provider();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/releases/latest/download/SHA256SUMS"))
        .respond_with(ResponseTemplate::new(302).insert_header(
            "location",
            format!("{}/releases/download/v99.0.0/SHA256SUMS", server.uri()).as_str(),
        ))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let stamp = dir.path().join("nested").join("self-update-check");
    let line = notice(&stamp, &server.uri(), Duration::from_secs(5), 1_000).await;
    assert_eq!(
        line.as_deref(),
        Some("decdn 99.0.0 is available; run `decdn self-update`")
    );
    assert_eq!(std::fs::read_to_string(&stamp).unwrap(), "1000\n");
}

/// The stamp is written before the lookup, so a server that never answers
/// costs one bounded wait per day, not one per command.
#[tokio::test]
async fn notice_stamps_before_a_lookup_that_times_out() {
    decdn_common::tls::install_crypto_provider();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(302).set_delay(Duration::from_secs(5)))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let stamp = dir.path().join("self-update-check");
    let started = std::time::Instant::now();
    let line = notice(&stamp, &server.uri(), Duration::from_millis(200), 7).await;
    assert!(line.is_none());
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(std::fs::read_to_string(&stamp).unwrap(), "7\n");
}

/// An up-to-date binary, a pre-release, or an older "latest" says nothing.
#[tokio::test]
async fn notice_is_silent_unless_the_release_is_newer() {
    decdn_common::tls::install_crypto_provider();
    for tag in [
        "v0.0.1",
        "v99.0.0-rc1",
        concat!("v", env!("CARGO_PKG_VERSION")),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                format!("{}/releases/download/{tag}/SHA256SUMS", server.uri()).as_str(),
            ))
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().unwrap();
        let stamp = dir.path().join("self-update-check");
        let line = notice(&stamp, &server.uri(), Duration::from_secs(5), 1).await;
        assert_eq!(line, None, "{tag}");
    }
}

/// When the stamp cannot be written, no lookup happens at all: without the
/// stamp every command would check, which the daily bound forbids.
#[tokio::test]
async fn notice_skips_the_lookup_when_the_stamp_cannot_be_written() {
    decdn_common::tls::install_crypto_provider();
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    // A file where the stamp's parent directory should be.
    std::fs::write(dir.path().join("blocker"), b"").unwrap();
    let stamp = dir.path().join("blocker").join("self-update-check");
    let line = notice(&stamp, &server.uri(), Duration::from_secs(5), 1).await;
    assert_eq!(line, None);
    assert!(server.received_requests().await.unwrap().is_empty());
}

/// The stamp's directory is `~/.decdn`; when the notice is what creates it,
/// it is private, as every other writer of that directory requires.
#[cfg(unix)]
#[tokio::test]
async fn notice_creates_the_data_dir_private() {
    use std::os::unix::fs::PermissionsExt;
    decdn_common::tls::install_crypto_provider();
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join(".decdn");
    let stamp = stamp_path(&data_dir);
    notice(&stamp, &server.uri(), Duration::from_secs(5), 1).await;
    let dir_mode = std::fs::metadata(&data_dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(dir_mode, 0o700, "dir mode {dir_mode:o}");
    let file_mode = std::fs::metadata(&stamp).unwrap().permissions().mode() & 0o777;
    assert_eq!(file_mode, 0o600, "stamp mode {file_mode:o}");
}

#[test]
fn stamp_lives_under_the_data_dir() {
    assert_eq!(
        stamp_path(std::path::Path::new("/home/u/.decdn")),
        std::path::PathBuf::from("/home/u/.decdn/self-update-check")
    );
}

/// A lookup that finished during the command yields its line at once.
#[tokio::test]
async fn settle_returns_a_line_the_lookup_already_produced() {
    let handle = tokio::spawn(async { Some("decdn 99.0.0 is available".to_string()) });
    let line = settle(handle, Duration::from_millis(250)).await;
    assert_eq!(line.as_deref(), Some("decdn 99.0.0 is available"));
}

/// A lookup still in flight when the command ends is given the grace period
/// and no more; the command's exit does not wait on GitHub.
#[tokio::test]
async fn settle_drops_a_lookup_that_outlives_the_grace() {
    let handle = tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Some("too late".to_string())
    });
    let started = std::time::Instant::now();
    let line = settle(handle, Duration::from_millis(100)).await;
    assert_eq!(line, None);
    assert!(started.elapsed() < Duration::from_secs(1));
}
