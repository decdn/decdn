//! Embed the page `decdn dashboard` serves. When `dashboard/dist/` holds the Vite
//! build (`index.html`, `app.js`, `app.css`), copy it into `OUT_DIR`;
//! otherwise write a page there that says how to build it, so a build with
//! no Node toolchain still succeeds. `DECDN_DASHBOARD_BUILT` tells the crate
//! which one it embeds.
#![allow(
    clippy::print_stdout,
    reason = "cargo reads a build script's instructions from its stdout"
)]

use std::path::{Path, PathBuf};
use std::{env, fs, io};

/// The files the Vite build writes, and the crate embeds.
const FILES: [&str; 3] = ["index.html", "app.js", "app.css"];

/// The page a build without the dashboard serves. The marker comment lets
/// the release workflow refuse a binary that embeds it.
const FALLBACK: &str = "<!doctype html>
<!-- decdn-dashboard-fallback -->
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<meta name=\"viewport\" content=\"width=device-width\">
<title>decdn dashboard</title>
</head>
<body>
<h1>decdn dashboard</h1>
<p>This <code>decdn</code> binary was built without its dashboard page. The dashboard runs, and
what it reads is at <code>GET /_status</code>.</p>
<p>To build the dashboard, run from the repository root (Node 24 and pnpm 10):</p>
<pre>pnpm -C crates/cli/dashboard install --frozen-lockfile
pnpm -C crates/cli/dashboard build
cargo build -p decdn-cli</pre>
</body>
</html>
";

fn main() -> io::Result<()> {
    let out = PathBuf::from(env::var_os("OUT_DIR").ok_or_else(|| io::Error::other("no OUT_DIR"))?)
        .join("dashboard");
    fs::create_dir_all(&out)?;
    let dist = Path::new("dashboard/dist");
    let built = FILES.iter().all(|file| dist.join(file).is_file());
    println!("cargo::rerun-if-changed=build.rs");
    if built {
        for file in FILES {
            fs::copy(dist.join(file), out.join(file))?;
        }
        println!("cargo::rerun-if-changed=dashboard/dist");
    } else {
        fs::write(out.join("index.html"), FALLBACK)?;
        fs::write(out.join("app.js"), "")?;
        fs::write(out.join("app.css"), "")?;
        // A path that does not exist reruns the script, and rebuilds the
        // crate, on every build; the dashboard's own directory does exist.
        println!("cargo::rerun-if-changed=dashboard");
    }
    println!("cargo::rustc-env=DECDN_DASHBOARD_BUILT={}", u8::from(built));
    Ok(())
}
