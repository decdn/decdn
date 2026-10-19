//! `decdn self-update`: replace the running binary with the latest signed
//! release, and the once-a-day notice that a newer one exists.
//!
//! The order is deliberate: ownership first, offline, so a binary this
//! command may not replace costs no download; then the latest tag; then the
//! signed manifest, which must verify against an embedded minisign key
//! before anything else is trusted; then the archive, checked against the
//! manifest and unpacked; and only then the swap, staged beside the running
//! binary and renamed into place.

pub(crate) mod check;
pub(crate) mod install;
pub(crate) mod release;
pub(crate) mod verify;

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use decdn_common::cli::SelfUpdateArgs;

pub use check::{NudgePolicy, maybe_nudge};
use install::Ownership;
use release::{ArchiveKind, Clients, Manifest, Target, Upgrade};

/// Whole-request bound on each download; an archive is tens of megabytes.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);
/// `SHA256SUMS` lists eleven archives; anything near this is not it.
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
/// A minisign signature is four short lines.
const MAX_SIGNATURE_BYTES: usize = 4 * 1024;
/// Generous ceiling for a release archive and the binary inside it.
const MAX_ARCHIVE_BYTES: usize = 256 * 1024 * 1024;

/// Run `decdn self-update`.
pub async fn self_update(args: &SelfUpdateArgs) -> anyhow::Result<()> {
    let target = Target::current().context(
        "no release archive is built for this platform; build from source or run \
         `cargo install --locked decdn-cli`",
    )?;
    let data_dir =
        decdn_common::cli::default_data_dir().context("cannot determine the home directory")?;
    let home = data_dir.parent().unwrap_or(&data_dir).to_path_buf();
    let exe = install::running_exe()?;
    let cargo_home = std::env::var_os("CARGO_HOME").map(std::path::PathBuf::from);
    let cargo_bins = install::cargo_bin_dirs(&home, cargo_home.as_deref());
    match install::classify(&exe, &cargo_bins, install::probe_writable) {
        Ownership::Replaceable => {}
        Ownership::CargoBin(path) => bail!(
            "{} was installed by cargo; run `cargo install --locked decdn-cli` to update it",
            path.display()
        ),
        Ownership::NotWritable { path, reason } => bail!(
            "cannot replace {}: {reason}. Update it the way it was installed (`docker pull`, \
             the package manager, or the fleet tooling that placed it)",
            path.display()
        ),
    }

    let running = release::running_version()?;
    let base = release::releases_base();
    let clients = Clients::build(DOWNLOAD_TIMEOUT)?;
    let tag = release::resolve_latest(&clients, &base).await?;
    let latest = release::version_from_tag(&tag)?;
    match release::decide(&running, &latest) {
        Upgrade::Newer => {}
        Upgrade::UpToDate => {
            println!("decdn {running} is up to date");
            return Ok(());
        }
        Upgrade::Downgrade => {
            println!(
                "the latest release ({latest}) is older than this build ({running}); nothing to do"
            );
            return Ok(());
        }
        Upgrade::PreRelease => {
            println!("the latest release ({latest}) is a pre-release; nothing to do");
            return Ok(());
        }
    }

    let sums =
        release::fetch_asset(&clients, &base, &tag, "SHA256SUMS", MAX_MANIFEST_BYTES).await?;
    let minisig = release::fetch_asset(
        &clients,
        &base,
        &tag,
        "SHA256SUMS.minisig",
        MAX_SIGNATURE_BYTES,
    )
    .await?;
    let minisig = std::str::from_utf8(&minisig).context("SHA256SUMS.minisig is not UTF-8")?;
    verify::verify_manifest_with(&verify::trusted_keys()?, &sums, minisig)?;
    let manifest = Manifest::parse(std::str::from_utf8(&sums).context("SHA256SUMS is not UTF-8")?)?;
    let name = target.archive_name(&latest);
    if manifest.digest_for(&name).is_none() {
        bail!("release {tag} ships no {name}");
    }
    if args.check {
        println!(
            "decdn {latest} is available (signed manifest ok); run `decdn self-update` to \
             install it over {}",
            exe.display()
        );
        return Ok(());
    }

    eprintln!("Downloading {name}...");
    let archive = release::fetch_asset(&clients, &base, &tag, &name, MAX_ARCHIVE_BYTES).await?;
    verify::verify_archive(&manifest, &name, &archive)?;
    let bytes = install::extract_binary(&archive, target.kind(), MAX_ARCHIVE_BYTES as u64)?;
    let dir = exe
        .parent()
        .context("the running binary has no parent directory")?;
    let staged = install::stage(dir, &bytes)?;
    install::swap(staged.path())?;
    println!("updated {}: {running} -> {latest}", exe.display());

    let marker = install::installer_marker_path(&data_dir);
    let line = install::marker_line(&verify::sha256_hex(&sums), target.triple());
    match install::rewrite_marker_if_present(&marker, &line) {
        Ok(true) => eprintln!("installer marker updated: {}", marker.display()),
        Ok(false) => {}
        Err(e) => eprintln!(
            "warning: the binary is updated, but the installer marker {} was not: {e}",
            marker.display()
        ),
    }
    if matches!(target.kind(), ArchiveKind::Zip) {
        stale_exe_hint(dir);
    }
    Ok(())
}

/// On Windows the running `.exe` is moved aside, not deleted; say so once.
fn stale_exe_hint(dir: &Path) {
    eprintln!(
        "the previous decdn.exe stays in {} until it exits and can be deleted",
        dir.display()
    );
}
