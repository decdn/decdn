use std::io::Write;
use std::path::{Path, PathBuf};

use super::super::release::ArchiveKind;
use super::*;

#[allow(clippy::unnecessary_wraps)] // stands in for a probe that may fail
fn writable(_: &Path) -> std::io::Result<()> {
    Ok(())
}

fn read_only(_: &Path) -> std::io::Result<()> {
    Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
}

/// A binary under a cargo bin directory belongs to `cargo install`; the
/// user re-runs that instead.
#[test]
fn classify_refuses_a_cargo_install() {
    let cargo = PathBuf::from("/home/u/.cargo/bin");
    let exe = cargo.join("decdn");
    assert_eq!(
        classify(&exe, std::slice::from_ref(&cargo), writable),
        Ownership::CargoBin(exe)
    );
}

/// A directory the user cannot write to (the container image's
/// `/usr/local/bin`, an Ansible-managed fleet path) refuses with the path
/// and the OS reason; nothing is downloaded for it.
#[test]
fn classify_refuses_an_unwritable_directory() {
    let exe = PathBuf::from("/usr/local/bin/decdn");
    match classify(&exe, &[PathBuf::from("/home/u/.cargo/bin")], read_only) {
        Ownership::NotWritable { path, reason } => {
            assert_eq!(path, exe);
            assert!(reason.to_lowercase().contains("permission"), "{reason}");
        }
        other => panic!("expected NotWritable, got {other:?}"),
    }
}

#[test]
fn classify_accepts_a_writable_install() {
    let exe = PathBuf::from("/home/u/.local/bin/decdn");
    assert_eq!(
        classify(&exe, &[PathBuf::from("/home/u/.cargo/bin")], writable),
        Ownership::Replaceable
    );
}

/// `~/.cargo/bin` always counts; `$CARGO_HOME/bin` too when set.
#[test]
fn cargo_bin_dirs_cover_home_and_cargo_home() {
    let home = Path::new("/home/u");
    assert_eq!(
        cargo_bin_dirs(home, None),
        vec![PathBuf::from("/home/u/.cargo/bin")]
    );
    assert_eq!(
        cargo_bin_dirs(home, Some(Path::new("/opt/cargo"))),
        vec![
            PathBuf::from("/home/u/.cargo/bin"),
            PathBuf::from("/opt/cargo/bin")
        ]
    );
}

fn tar_gz(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut tar = tar::Builder::new(gz);
    for (name, data) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        tar.append_data(&mut header, name, *data).unwrap();
    }
    tar.into_inner().unwrap().finish().unwrap()
}

#[test]
fn extract_binary_reads_the_bare_decdn_entry() {
    let archive = tar_gz(&[("decdn", b"new binary")]);
    assert_eq!(
        extract_binary(&archive, ArchiveKind::TarGz, 1024).unwrap(),
        b"new binary"
    );
}

#[test]
fn extract_binary_accepts_a_dot_slash_prefix() {
    let archive = tar_gz(&[("./decdn", b"new binary")]);
    assert_eq!(
        extract_binary(&archive, ArchiveKind::TarGz, 1024).unwrap(),
        b"new binary"
    );
}

#[test]
fn extract_binary_rejects_an_archive_without_decdn() {
    let archive = tar_gz(&[("README", b"nope"), ("bin/decdn", b"nested")]);
    let err = extract_binary(&archive, ArchiveKind::TarGz, 1024).unwrap_err();
    assert!(err.to_string().contains("no `decdn` entry"), "{err}");
}

#[test]
fn extract_binary_caps_the_entry_size() {
    let archive = tar_gz(&[("decdn", &[0u8; 2048])]);
    let err = extract_binary(&archive, ArchiveKind::TarGz, 1024).unwrap_err();
    assert!(err.to_string().contains("larger than"), "{err}");
}

#[test]
fn extract_binary_rejects_garbage() {
    assert!(extract_binary(b"not an archive", ArchiveKind::TarGz, 1024).is_err());
}

/// The staged file is a sibling of the executable (same filesystem, so the
/// final rename is atomic), holds the bytes, and is executable.
#[test]
fn stage_writes_an_executable_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let staged = stage(dir.path(), b"new binary").unwrap();
    assert_eq!(staged.path().parent().unwrap(), dir.path());
    assert_eq!(std::fs::read(staged.path()).unwrap(), b"new binary");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(staged.path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o111, 0o111, "mode {mode:o}");
    }
}

/// The marker line is what the sponsored installer writes: the manifest
/// digest, a space, the target triple, a newline.
#[test]
fn marker_line_matches_the_installer_format() {
    let line = marker_line(&"c".repeat(64), "x86_64-unknown-linux-gnu");
    assert_eq!(
        line,
        format!("{} x86_64-unknown-linux-gnu\n", "c".repeat(64))
    );
}

/// Only an existing marker is rewritten: an install the sponsored installer
/// did not make gets no marker.
#[test]
fn rewrite_marker_only_when_present() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("installer").join("decdn");
    assert!(!rewrite_marker_if_present(&marker, "new\n").unwrap());
    assert!(!marker.exists());
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
    std::fs::File::create(&marker)
        .unwrap()
        .write_all(b"old x\n")
        .unwrap();
    assert!(rewrite_marker_if_present(&marker, "new\n").unwrap());
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "new\n");
}

#[test]
fn installer_marker_lives_under_the_data_dir() {
    assert_eq!(
        installer_marker_path(Path::new("/home/u/.decdn")),
        PathBuf::from("/home/u/.decdn/installer/decdn")
    );
}
