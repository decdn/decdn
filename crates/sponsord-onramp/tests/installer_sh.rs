//! `assets/decdn.sh` as the onramp serves it, run for real against that
//! onramp over HTTP (fake daemon and gate), `file://` releases and a stub
//! `decdn`: the install skips binaries already at the pin, it says how to
//! put `~/.local/bin` on PATH when it is missing, and `get` passes
//! the gate once per download, resumes a failed one without it, replaces a
//! capability near expiry, and refuses a second run of the same download.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::fs;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use sponsord_onramp::test_support::{
    FakeOptions, Fakes, STUB_DECDN, app_state_with_options, publish_release, test_config,
};
use tokio::io::{AsyncBufReadExt, BufReader};

macro_rules! hash {
    () => {
        "9194000d7b356650e6924a7746ec4afb0b705838b65913c0e46cba6b15af69e5"
    };
}
const HASH: &str = hash!();
const BUNDLE: &str = concat!("decdn:", hash!(), "/");
const GET: [&str; 2] = ["get", BUNDLE];

/// The address the stub `decdn` prints for the `n`th key it makes.
fn stub_address(n: u64) -> String {
    format!("0x{n:040x}")
}

/// One run of the script.
struct Run {
    ok: bool,
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

struct Fixture {
    home: PathBuf,
    releases: PathBuf,
    stub: PathBuf,
    script: PathBuf,
    onramp: String,
    fakes: Fakes,
}

impl Fixture {
    async fn new(name: &str) -> Self {
        let root = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join("installer_sh")
            .join(name);
        let _ = fs::remove_dir_all(&root);
        let home = root.join("home");
        let releases = root.join("releases");
        let stub = root.join("stub");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&stub).unwrap();
        let sums = publish_release(&releases, "decdn", "decdn", "v0.1.0", "0.1.0", STUB_DECDN);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let onramp = format!("http://{}", listener.local_addr().unwrap());
        let mut config = test_config();
        config.public_url.clone_from(&onramp);
        config.releases_base = format!("file://{}", releases.display());
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

        let body = reqwest::get(format!("{onramp}/decdn.sh"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let script = root.join("decdn.sh");
        fs::write(&script, body).unwrap();
        Self {
            home,
            releases,
            stub,
            script,
            onramp,
            fakes,
        }
    }

    /// Run the script with `args` and extra `env`. Plays the person in the
    /// browser: whenever the script prints a gate link, the gate is passed
    /// for that key, as opening the link would.
    async fn run_with(&self, args: &[&str], env: &[(&str, &str)]) -> Run {
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg(&self.script)
            .args(args)
            .env("HOME", &self.home)
            .env("STUB_DIR", &self.stub)
            .env_remove("STUB_EXIT")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().unwrap();

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
        let out = tokio::time::timeout(Duration::from_secs(60), child.wait_with_output())
            .await
            .expect("decdn.sh finishes")
            .unwrap();
        Run {
            ok: out.status.success(),
            code: out.status.code(),
            stdout: reader.await.unwrap(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        }
    }

    /// Run the script, and fail the test unless it succeeds. Returns stdout.
    async fn run_ok(&self, args: &[&str], env: &[(&str, &str)]) -> String {
        let run = self.run_with(args, env).await;
        assert!(
            run.ok,
            "decdn.sh failed\nstdout:\n{}\nstderr:\n{}",
            run.stdout, run.stderr
        );
        run.stdout
    }

    fn bin(&self, name: &str) -> PathBuf {
        self.home.join(".local/bin").join(name)
    }

    fn marker(&self, name: &str) -> PathBuf {
        self.home.join(".decdn/installer").join(name)
    }

    fn sponsored(&self) -> PathBuf {
        self.home.join(".decdn/sponsored")
    }

    fn state(&self) -> PathBuf {
        self.sponsored().join("downloads").join(HASH)
    }

    fn calls(&self) -> Vec<String> {
        fs::read_to_string(self.stub.join("calls"))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn keys_made(&self) -> u32 {
        fs::read_to_string(self.stub.join("keys")).map_or(0, |s| s.trim().parse().unwrap())
    }

    fn gate_link(&self, n: u64) -> String {
        format!("{}/fund?client={}", self.onramp, stub_address(n))
    }
}

#[tokio::test]
async fn a_second_run_with_the_same_pin_downloads_nothing() {
    let fx = Fixture::new("same_pin").await;
    let first = fx.run_ok(&[], &[]).await;
    assert!(first.contains("Installing decdn v0.1.0"), "{first}");
    assert!(first.contains("decdn is installed at"), "{first}");
    assert!(fx.bin("decdn").exists() && fx.marker("decdn").exists());

    // With the releases gone, any download would fail the run.
    fs::remove_dir_all(&fx.releases).unwrap();
    let second = fx.run_ok(&[], &[]).await;
    assert!(!second.contains("Installing"), "{second}");
    assert!(
        second.contains("decdn v0.1.0 is already installed."),
        "{second}"
    );
}

#[tokio::test]
async fn a_missing_marker_or_binary_reinstalls() {
    let fx = Fixture::new("reinstall").await;
    fx.run_ok(&[], &[]).await;

    fs::remove_file(fx.marker("decdn")).unwrap();
    let out = fx.run_ok(&[], &[]).await;
    assert!(out.contains("Installing decdn v0.1.0"), "{out}");

    fs::remove_file(fx.bin("decdn")).unwrap();
    let out = fx.run_ok(&[], &[]).await;
    assert!(out.contains("Installing decdn v0.1.0"), "{out}");
    assert!(fx.bin("decdn").exists());
}

#[tokio::test]
async fn a_changed_pin_reinstalls() {
    let fx = Fixture::new("changed_pin").await;
    fx.run_ok(&[], &[]).await;
    // A marker from another release, as after the onramp moves its pin.
    fs::write(fx.marker("decdn"), "00 x86_64-unknown-linux-gnu\n").unwrap();
    let out = fx.run_ok(&[], &[]).await;
    assert!(out.contains("Installing decdn v0.1.0"), "{out}");
}

#[tokio::test]
async fn install_suggests_adding_the_bin_dir_to_path_only_when_missing() {
    let fx = Fixture::new("install").await;
    let bin_dir = fx.bin("");
    let bin_dir = bin_dir.to_str().unwrap().trim_end_matches('/');
    let system = std::env::var("PATH").unwrap();

    let out = fx
        .run_ok(&[], &[("PATH", &system), ("SHELL", "/bin/zsh")])
        .await;
    assert!(
        out.contains(&format!("decdn is installed at {bin_dir}/decdn.")),
        "{out}"
    );
    assert!(
        out.contains(&format!("{bin_dir} is not on your PATH")),
        "{out}"
    );
    assert!(
        out.contains(r#"echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc"#),
        "{out}"
    );
    assert!(fx.bin("decdn").exists());
    assert!(fx.calls().is_empty(), "{:?}", fx.calls());

    let out = fx.run_ok(&[], &[("SHELL", "/usr/bin/fish")]).await;
    assert!(out.contains("fish_add_path ~/.local/bin"), "{out}");

    let on_path = format!("{bin_dir}:{system}");
    let out = fx.run_ok(&[], &[("PATH", &on_path)]).await;
    assert!(!out.contains("not on your PATH"), "{out}");
}

#[tokio::test]
async fn get_passes_the_gate_and_runs_decdn_with_the_capability() {
    let fx = Fixture::new("get").await;
    // An entry URI with a namespace: the download's state is keyed on the hash
    // alone, and the URI reaches decdn as given.
    let uri = concat!("decdn:", hash!(), "/a%20b.txt?ns=1");
    let args = [GET[0], uri, "-o", "out dir"];
    let stdout = fx.run_ok(&args, &[]).await;

    assert!(
        stdout.contains("Open this link to start the download:"),
        "{stdout}"
    );
    assert!(stdout.contains(&fx.gate_link(1)), "{stdout}");
    let pool = alloy_primitives::Address::repeat_byte(0x22);
    let bond = alloy_primitives::Address::repeat_byte(0x33);
    let state = fx.state();
    let shared = fx.sponsored().join("decdn");
    assert_eq!(
        fx.calls(),
        [format!(
            "get {uri} -o out dir --capability dcap1:FAKE1 \
             --keystore {}/keystore.json --keystore-password-file {}/password \
             --data-dir {} --rpc-url https://rpc.example --payment-pool-address {pool} \
             --capacity-bond-address {bond} --chain-id 421614",
            state.display(),
            state.display(),
            shared.display(),
        )]
    );
    assert_eq!(fx.fakes.source.issued_count(), 1);
    assert!(!state.exists(), "state is deleted after a good download");
    assert!(!fx.sponsored().join("decdn.lock").exists());
    assert!(
        !fx.sponsored()
            .join(format!("downloads/{HASH}.lock"))
            .exists()
    );
}

#[tokio::test]
async fn every_argument_after_the_uri_reaches_decdn_as_given() {
    let fx = Fixture::new("pass_through").await;

    // An output flag in any form replaces `-o .`.
    let args = [
        GET[0],
        GET[1],
        "--jobs",
        "8",
        "--output=out",
        "--include",
        "*.txt",
    ];
    fx.run_ok(&args, &[]).await;
    // Without one, the bundle lands in the current directory.
    fx.run_ok(&[GET[0], GET[1], "--jobs", "8"], &[]).await;
    // A blob URI names its file with -o.
    let blob = format!("decdn:{HASH}?ns=1");
    fx.run_ok(&[GET[0], &blob, "-o", "file.iso"], &[]).await;

    let calls = fx.calls();
    assert_eq!(calls.len(), 3, "{calls:?}");
    let expected = [
        format!("get {BUNDLE} --jobs 8 --output=out --include *.txt --capability "),
        format!("get {BUNDLE} -o . --jobs 8 --capability "),
        format!("get {blob} -o file.iso --capability "),
    ];
    for (call, start) in calls.iter().zip(&expected) {
        assert!(call.starts_with(start), "{call}");
        assert!(call.ends_with(" --chain-id 421614"), "{call}");
    }
}

#[tokio::test]
async fn a_failed_download_resumes_with_the_same_key_and_capability() {
    let fx = Fixture::new("resume").await;

    let run = fx.run_with(&GET, &[("STUB_EXIT", "3")]).await;
    assert_eq!(run.code, Some(3));
    assert!(run.stdout.contains(&fx.gate_link(1)), "{}", run.stdout);
    assert!(
        run.stderr.contains("Re-run the same command to resume"),
        "{}",
        run.stderr
    );
    let state = fx.state();
    assert_eq!(
        fs::read_to_string(state.join("capability")).unwrap(),
        "dcap1:FAKE1"
    );
    assert!(
        fs::read_to_string(state.join("expiry"))
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
    // decdn refuses a data dir that group or others can read.
    let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&state), 0o700);
    assert_eq!(mode(&fx.sponsored().join("decdn")), 0o700);
    assert_eq!(mode(&state.join("password")), 0o600);

    // No gate this time: the saved key and capability are reused.
    let stdout = fx.run_ok(&GET, &[]).await;
    assert!(!stdout.contains("Open this link"), "{stdout}");
    assert_eq!(fx.keys_made(), 1);
    assert_eq!(fx.fakes.source.issued_count(), 1);
    let calls = fx.calls();
    assert_eq!(calls.len(), 2);
    assert!(
        calls
            .iter()
            .all(|c| c.contains("--capability dcap1:FAKE1 "))
    );
    assert!(!state.exists());
}

#[tokio::test]
async fn a_capability_near_expiry_is_replaced_with_a_new_key() {
    let fx = Fixture::new("expiry").await;

    let run = fx.run_with(&GET, &[("STUB_EXIT", "1")]).await;
    assert!(run.stdout.contains(&fx.gate_link(1)), "{}", run.stdout);
    fs::write(fx.state().join("expiry"), "1").unwrap();

    let stdout = fx.run_ok(&GET, &[]).await;
    assert!(stdout.contains(&fx.gate_link(2)), "{stdout}");
    assert_eq!(fx.keys_made(), 2);
    assert!(fx.calls()[1].contains("--capability dcap1:FAKE2 "));
}

#[tokio::test]
async fn a_download_already_running_is_not_started_twice() {
    let fx = Fixture::new("locked").await;
    let lock = fx.sponsored().join(format!("downloads/{HASH}.lock"));
    fs::create_dir_all(&lock).unwrap();
    fs::write(lock.join("pid"), std::process::id().to_string()).unwrap();

    let run = fx.run_with(&GET, &[]).await;
    assert!(!run.ok);
    assert!(run.stderr.contains("already running"), "{}", run.stderr);
    assert_eq!(fx.keys_made(), 0);
    assert!(lock.exists(), "another run's lock is left alone");

    // A lock left by a process that has exited is taken over.
    let mut dead = std::process::Command::new("true").spawn().unwrap();
    let dead_pid = dead.id();
    dead.wait().unwrap();
    fs::write(lock.join("pid"), dead_pid.to_string()).unwrap();
    fx.run_ok(&GET, &[]).await;
    assert!(!lock.exists());
}

#[tokio::test]
async fn a_busy_shared_data_dir_runs_the_download_in_its_own() {
    let fx = Fixture::new("busy_data_dir").await;
    let lock = fx.sponsored().join("decdn.lock");
    fs::create_dir_all(&lock).unwrap();
    fs::write(lock.join("pid"), std::process::id().to_string()).unwrap();

    let run = fx.run_with(&GET, &[("STUB_EXIT", "1")]).await;
    assert!(
        run.stderr.contains("without the shared peer cache"),
        "{}",
        run.stderr
    );
    assert!(fx.calls()[0].contains(&format!("--data-dir {} ", fx.state().display())));
}

#[tokio::test]
async fn bad_arguments_are_refused_before_installing() {
    let fx = Fixture::new("bad_args").await;
    let blob = format!("decdn:{HASH}");
    let blob_ns = format!("decdn:{HASH}?ns=1");
    let b3 = format!("b3:{HASH}");
    let upper_scheme = format!("DECDN:{HASH}/");
    let short = format!("decdn:{}/", &HASH[1..]);
    for args in [
        &["get", "--jobs", "8"][..],
        &["get", "decdn:nothex/"],
        &["get"],
        &["pull", BUNDLE],
        &["get", "-o", "out", BUNDLE],
        // A blob URI is written to a file, so it needs -o to name one.
        &["get", &blob],
        &["get", &blob_ns],
        &["get", &b3],
        &["get", HASH],
        &["get", &upper_scheme],
        &["get", &short],
    ] {
        let run = fx.run_with(args, &[]).await;
        assert!(!run.ok, "{args:?}");
    }
    assert!(!fx.bin("decdn").exists());
}
