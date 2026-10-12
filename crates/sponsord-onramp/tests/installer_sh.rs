//! `assets/decdn.sh` run for real against `file://` releases: a second run
//! with the same pins keeps the installed binaries and downloads nothing, and
//! a missing marker or binary brings the download back.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::fmt::Write as _;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const TEMPLATE: &str = include_str!("../assets/decdn.sh");

/// Every target the installer can pick, so the test runs on any host.
const TARGETS: [&str; 4] = [
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
];

fn sha256(path: &Path) -> String {
    let run = |cmd: &str, args: &[&str]| Command::new(cmd).args(args).arg(path).output().ok();
    let out = run("sha256sum", &[])
        .filter(|o| o.status.success())
        .or_else(|| run("shasum", &["-a", "256"]))
        .expect("sha256sum or shasum");
    String::from_utf8(out.stdout).unwrap()[..64].to_owned()
}

/// Publishes `<root>/<repo>/releases/download/<tag>/` with one archive per
/// target holding a stub `bin`, and returns the SHA256SUMS digest.
fn publish(root: &Path, repo: &str, bin: &str, tag: &str, version: &str) -> String {
    let dir = root.join(repo).join("releases/download").join(tag);
    let stage = root.join(format!("stage-{bin}"));
    fs::create_dir_all(&dir).unwrap();
    fs::create_dir_all(&stage).unwrap();
    let stub = stage.join(bin);
    fs::write(&stub, format!("#!/bin/sh\necho {bin}\n")).unwrap();
    fs::set_permissions(&stub, fs::Permissions::from_mode(0o755)).unwrap();

    let mut sums = String::new();
    for target in TARGETS {
        let archive = format!("{bin}-{version}-{target}.tar.gz");
        let status = Command::new("tar")
            .arg("-czf")
            .arg(dir.join(&archive))
            .arg("-C")
            .arg(&stage)
            .arg(bin)
            .status()
            .unwrap();
        assert!(status.success(), "tar {archive}");
        writeln!(sums, "{}  {archive}", sha256(&dir.join(&archive))).unwrap();
    }
    let sums_path = dir.join("SHA256SUMS");
    fs::write(&sums_path, sums).unwrap();
    sha256(&sums_path)
}

struct Fixture {
    home: PathBuf,
    releases: PathBuf,
    script: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
        let _ = fs::remove_dir_all(&root);
        let home = root.join("home");
        let releases = root.join("releases");
        fs::create_dir_all(&home).unwrap();

        let decdn_sums = publish(&releases, "decdn", "decdn", "v0.1.0", "0.1.0");
        let cli_sums = publish(&releases, "sponsord", "decdn-sponsored", "v0.2.0", "0.2.0");
        let rendered = TEMPLATE
            .replace("{{ONRAMP_URL}}", "https://up.example.org")
            .replace(
                "{{RELEASES_BASE}}",
                &format!("file://{}", releases.display()),
            )
            .replace("{{DECDN_RELEASE}}", "v0.1.0")
            .replace("{{DECDN_VERSION}}", "0.1.0")
            .replace("{{DECDN_SUMS_SHA256}}", &decdn_sums)
            .replace("{{CLI_RELEASE}}", "v0.2.0")
            .replace("{{CLI_VERSION}}", "0.2.0")
            .replace("{{CLI_SUMS_SHA256}}", &cli_sums);
        let script = root.join("decdn.sh");
        fs::write(&script, rendered).unwrap();
        Self {
            home,
            releases,
            script,
        }
    }

    fn run(&self) -> String {
        let Output {
            status,
            stdout,
            stderr,
        } = Command::new("sh")
            .arg(&self.script)
            .env("HOME", &self.home)
            .output()
            .unwrap();
        let stdout = String::from_utf8(stdout).unwrap();
        assert!(
            status.success(),
            "decdn.sh failed\nstdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&stderr)
        );
        stdout
    }

    fn bin(&self, name: &str) -> PathBuf {
        self.home.join(".local/bin").join(name)
    }

    fn marker(&self, name: &str) -> PathBuf {
        self.home.join(".decdn/installer").join(name)
    }
}

#[test]
fn a_second_run_with_the_same_pins_downloads_nothing() {
    let fx = Fixture::new("same_pins");
    let first = fx.run();
    assert!(first.contains("Installing decdn v0.1.0"), "{first}");
    assert!(
        first.contains("Installing decdn-sponsored v0.2.0"),
        "{first}"
    );
    assert!(fx.bin("decdn").exists() && fx.bin("decdn-sponsored").exists());
    assert!(fx.marker("decdn").exists() && fx.marker("decdn-sponsored").exists());

    // With the releases gone, any download would fail the run.
    fs::remove_dir_all(&fx.releases).unwrap();
    let second = fx.run();
    assert!(!second.contains("Installing"), "{second}");
    assert!(
        second.contains("decdn v0.1.0 is already installed."),
        "{second}"
    );
    assert!(
        second.contains("decdn-sponsored v0.2.0 is already installed."),
        "{second}"
    );
    assert!(fx.home.join(".decdn/sponsor.toml").exists());
}

#[test]
fn a_missing_marker_or_binary_reinstalls_only_that_binary() {
    let fx = Fixture::new("reinstall");
    fx.run();

    fs::remove_file(fx.marker("decdn")).unwrap();
    let out = fx.run();
    assert!(out.contains("Installing decdn v0.1.0"), "{out}");
    assert!(
        out.contains("decdn-sponsored v0.2.0 is already installed."),
        "{out}"
    );

    fs::remove_file(fx.bin("decdn-sponsored")).unwrap();
    let out = fx.run();
    assert!(out.contains("decdn v0.1.0 is already installed."), "{out}");
    assert!(out.contains("Installing decdn-sponsored v0.2.0"), "{out}");
    assert!(fx.bin("decdn-sponsored").exists());
}

#[test]
fn a_changed_pin_reinstalls() {
    let fx = Fixture::new("changed_pin");
    fx.run();
    // A marker from another release, as after the onramp moves its pin.
    fs::write(fx.marker("decdn"), "00 x86_64-unknown-linux-gnu\n").unwrap();
    let out = fx.run();
    assert!(out.contains("Installing decdn v0.1.0"), "{out}");
}
