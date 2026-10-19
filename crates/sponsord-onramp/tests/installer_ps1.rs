//! `assets/decdn.ps1` as the onramp serves it, run in Windows PowerShell
//! against that onramp over HTTP (fake daemon and gate), a release served
//! over HTTP and a stub `decdn.exe` compiled for the test: `get` passes the
//! gate and runs `decdn` with the capability, and a failed download resumes
//! without the gate.
//!
//! The installer adds its bin dir to the user's PATH in the registry, so
//! these tests run only where `CI` is set, on a throwaway runner.

#![cfg(windows)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use axum::extract::Path as UrlPath;
use axum::http::StatusCode;
use sponsord_onramp::test_support::{FakeOptions, Fakes, app_state_with_options, test_config};
use tokio::io::{AsyncBufReadExt, BufReader};

const HASH: &str = "9194000d7b356650e6924a7746ec4afb0b705838b65913c0e46cba6b15af69e5";

/// The stub `decdn.exe`, the Rust twin of `test_support::STUB_DECDN`:
/// `key-gen` prints a fresh address, anything else records its arguments in
/// `$STUB_DIR/calls` and exits with `$STUB_EXIT`.
const STUB_SOURCE: &str = r#"
use std::io::Write;
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dir = std::path::PathBuf::from(std::env::var("STUB_DIR").unwrap());
    if args.first().map(String::as_str) == Some("key-gen") {
        let keys = dir.join("keys");
        let n: u64 = std::fs::read_to_string(&keys)
            .map(|s| s.trim().parse().unwrap())
            .unwrap_or(0) + 1;
        std::fs::write(&keys, n.to_string()).unwrap();
        let out = &args[args.iter().position(|a| a == "--output-dir").unwrap() + 1];
        std::fs::write(std::path::Path::new(out).join("keystore.json"), "{}").unwrap();
        println!("node id: stub");
        println!("eth address: 0x{n:040x}");
    } else {
        let mut calls = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("calls"))
            .unwrap();
        writeln!(calls, "{}", args.join(" ")).unwrap();
        let code = std::env::var("STUB_EXIT").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
        std::process::exit(code);
    }
}
"#;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    url
}

/// Windows PowerShell started from PowerShell 7 (the CI step's shell)
/// inherits its `PSModulePath`, which hides 5.1's own modules
/// (`Get-FileHash`, `Expand-Archive`); a fresh session starts without it.
const INHERITED_MODULE_PATH: &str = "PSModulePath";

fn powershell(command: &str) -> String {
    let out = std::process::Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", command])
        .env_remove(INHERITED_MODULE_PATH)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}

/// Publish `<root>/decdn/releases/download/v0.2.0/` with the stub in a zip
/// per Windows target, and return the `SHA256SUMS` digest.
fn publish(root: &Path) -> String {
    let stage = root.join("stage");
    fs::create_dir_all(&stage).unwrap();
    fs::write(stage.join("stub.rs"), STUB_SOURCE).unwrap();
    let status = std::process::Command::new("rustc")
        .args(["--edition", "2021", "-O", "-o"])
        .arg(stage.join("decdn.exe"))
        .arg(stage.join("stub.rs"))
        .status()
        .unwrap();
    assert!(status.success(), "rustc stub");

    let dir = root.join("decdn/releases/download/v0.2.0");
    fs::create_dir_all(&dir).unwrap();
    powershell(&format!(
        "$ErrorActionPreference = 'Stop'; Set-Location -LiteralPath '{dir}'; \
         foreach ($t in 'x86_64', 'aarch64') {{ \
           Compress-Archive -Force -LiteralPath '{exe}' -DestinationPath \"decdn-0.2.0-$t-pc-windows-msvc.zip\" }}; \
         $lines = Get-ChildItem *.zip | ForEach-Object {{ \
           (Get-FileHash -Algorithm SHA256 $_.FullName).Hash.ToLower() + '  ' + $_.Name }}; \
         [IO.File]::WriteAllText((Join-Path $PWD 'SHA256SUMS'), ($lines -join \"`n\") + \"`n\"); \
         (Get-FileHash -Algorithm SHA256 SHA256SUMS).Hash.ToLower()",
        dir = dir.display(),
        exe = stage.join("decdn.exe").display(),
    ))
}

struct Fixture {
    root: PathBuf,
    stub: PathBuf,
    script: PathBuf,
    onramp: String,
    fakes: Fakes,
}

impl Fixture {
    async fn new(name: &str) -> Self {
        let root = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join("installer_ps1")
            .join(name);
        let _ = fs::remove_dir_all(&root);
        let stub = root.join("stub");
        fs::create_dir_all(&stub).unwrap();
        let releases = root.join("releases");
        let sums = publish(&releases);

        let files = releases.clone();
        let releases_url = serve(axum::Router::new().route(
            "/{*path}",
            axum::routing::get(move |UrlPath(path): UrlPath<String>| {
                let file = files.join(path);
                async move { fs::read(file).map_err(|_| StatusCode::NOT_FOUND) }
            }),
        ))
        .await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let onramp = format!("http://{}", listener.local_addr().unwrap());
        let mut config = test_config();
        config.public_url.clone_from(&onramp);
        config.releases_base = releases_url;
        config.decdn_release.sums_sha256 = sums;
        let (state, fakes) = app_state_with_options(FakeOptions {
            config,
            ..FakeOptions::default()
        })
        .await;
        let app = sponsord_onramp::http::router(state);
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let body = reqwest::get(format!("{onramp}/decdn.ps1"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let script = root.join("decdn.ps1");
        fs::write(&script, body).unwrap();
        Self {
            root,
            stub,
            script,
            onramp,
            fakes,
        }
    }

    /// Run `get <args>` (PowerShell syntax, URI first) and pass the gate for
    /// whatever key the script prints a gate link for. Returns (success,
    /// stdout, stderr).
    async fn get(&self, args: &str, stub_exit: &str) -> (bool, String, String) {
        let home = self.root.join("home");
        let local = self.root.join("local");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&local).unwrap();
        let command = format!(
            "& ([scriptblock]::Create((Get-Content -Raw -LiteralPath '{}'))) get {args}",
            self.script.display()
        );
        let mut child = tokio::process::Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", &command])
            .env_remove(INHERITED_MODULE_PATH)
            .env("USERPROFILE", &home)
            .env("HOME", &home)
            .env("LOCALAPPDATA", &local)
            .env("STUB_DIR", &self.stub)
            .env("STUB_EXIT", stub_exit)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let onramp = self.onramp.clone();
        let reader = tokio::spawn(async move {
            let mut stdout = String::new();
            while let Some(line) = lines.next_line().await.unwrap() {
                if let Some((_, client)) = line.split_once("/fund?client=") {
                    let resp = reqwest::Client::new()
                        .post(format!("{onramp}/v1/fund"))
                        .json(&serde_json::json!({ "client": client.trim(), "proof": "ok" }))
                        .send()
                        .await
                        .unwrap();
                    assert!(resp.status().is_success(), "{}", resp.status());
                }
                stdout.push_str(&line);
                stdout.push('\n');
            }
            stdout
        });
        let out = tokio::time::timeout(Duration::from_secs(120), child.wait_with_output())
            .await
            .expect("decdn.ps1 finishes")
            .unwrap();
        let stdout = reader.await.unwrap();
        (
            out.status.success(),
            stdout,
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    fn calls(&self) -> Vec<String> {
        fs::read_to_string(self.stub.join("calls"))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

#[tokio::test]
async fn get_passes_the_gate_and_resumes_a_failed_download_without_it() {
    if std::env::var_os("CI").is_none() {
        return;
    }
    let fx = Fixture::new("get").await;
    let args = format!("'decdn:{HASH}/?ns=1' --jobs 8");

    let (ok, stdout, stderr) = fx.get(&args, "3").await;
    let both = format!("stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(!ok, "a failed decdn fails the run\n{both}");
    assert!(stdout.contains("Installing decdn v0.2.0"), "{both}");
    assert!(
        stdout.contains("Open this link to start the download:"),
        "{both}"
    );
    assert!(
        stderr.contains("Re-run the same command to resume"),
        "{both}"
    );

    let (ok, stdout, stderr) = fx.get(&args, "0").await;
    let both = format!("stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(ok, "{both}");
    assert!(
        stdout.contains("decdn v0.2.0 is already installed."),
        "{both}"
    );
    assert!(
        !stdout.contains("Open this link"),
        "resumed without the gate\n{both}"
    );

    let pool = alloy_primitives::Address::repeat_byte(0x22);
    let calls = fx.calls();
    assert_eq!(calls.len(), 2, "{calls:?}");
    for call in &calls {
        assert!(
            call.starts_with(&format!(
                "get decdn:{HASH}/?ns=1 --jobs 8 --capability dcap1:FAKE1 "
            )),
            "{call}"
        );
        assert!(
            call.contains(&format!(" --payment-pool-address {pool} ")),
            "{call}"
        );
        assert!(call.ends_with(" --chain-id 421614"), "{call}");
    }
    assert_eq!(fx.fakes.source.issued_count(), 1);
    assert_eq!(
        fs::read_to_string(fx.stub.join("keys")).unwrap().trim(),
        "1"
    );
}

#[tokio::test]
async fn a_blob_uri_needs_an_output_file() {
    if std::env::var_os("CI").is_none() {
        return;
    }
    let fx = Fixture::new("blob").await;

    let (ok, stdout, stderr) = fx.get(&format!("'decdn:{HASH}?ns=1'"), "0").await;
    let both = format!("stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(!ok, "a blob URI without -o is refused\n{both}");
    assert!(both.contains("a blob URI needs -o <file>"), "{both}");
    assert!(fx.calls().is_empty(), "{both}");

    let (ok, stdout, stderr) = fx
        .get(&format!("'decdn:{HASH}?ns=1' -o file.iso"), "0")
        .await;
    let both = format!("stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(ok, "{both}");
    let calls = fx.calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert!(
        calls[0].starts_with(&format!(
            "get decdn:{HASH}?ns=1 -o file.iso --capability dcap1:FAKE1 "
        )),
        "{}",
        calls[0]
    );
}
