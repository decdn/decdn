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
        Ownership::CargoBin
    );
}

/// A directory the user cannot write to (the container image's
/// `/usr/local/bin`, an Ansible-managed fleet path) refuses with the path
/// and the OS reason; nothing is downloaded for it.
#[test]
fn classify_refuses_an_unwritable_directory() {
    let exe = PathBuf::from("/usr/local/bin/decdn");
    match classify(&exe, &[PathBuf::from("/home/u/.cargo/bin")], read_only) {
        Ownership::NotWritable(reason) => {
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

/// `~/.cargo/bin` always counts; `$CARGO_HOME/bin` and `$CARGO_INSTALL_ROOT/bin`
/// too when set. A directory that exists is canonicalised, so a symlinked
/// cargo home still matches the canonical path of the running binary.
#[test]
fn cargo_bin_dirs_cover_home_cargo_home_and_install_root() {
    let home = Path::new("/home/u");
    assert_eq!(
        cargo_bin_dirs(home, None, None),
        vec![PathBuf::from("/home/u/.cargo/bin")]
    );
    assert_eq!(
        cargo_bin_dirs(
            home,
            Some(Path::new("/opt/cargo")),
            Some(Path::new("/opt/tools"))
        ),
        vec![
            PathBuf::from("/home/u/.cargo/bin"),
            PathBuf::from("/opt/cargo/bin"),
            PathBuf::from("/opt/tools/bin"),
        ]
    );
}

#[cfg(unix)]
#[test]
fn cargo_bin_dirs_canonicalise_a_symlinked_cargo_home() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("disk").join("cargo");
    std::fs::create_dir_all(real.join("bin")).unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::os::unix::fs::symlink(&real, home.join(".cargo")).unwrap();
    let dirs = cargo_bin_dirs(&home, None, None);
    assert_eq!(dirs, vec![real.canonicalize().unwrap().join("bin")]);
    let exe = real.canonicalize().unwrap().join("bin").join("decdn");
    assert_eq!(classify(&exe, &dirs, writable), Ownership::CargoBin);
}

fn tar_gz(entries: &[(&str, &[u8])]) -> Vec<u8> {
    tar_gz_typed(
        &entries
            .iter()
            .map(|(n, d)| (*n, *d, tar::EntryType::Regular))
            .collect::<Vec<_>>(),
    )
}

fn tar_gz_typed(entries: &[(&str, &[u8], tar::EntryType)]) -> Vec<u8> {
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut tar = tar::Builder::new(gz);
    for (name, data, kind) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o755);
        header.set_entry_type(*kind);
        if *kind == tar::EntryType::Symlink {
            header.set_link_name("elsewhere").unwrap();
        }
        header.set_cksum();
        tar.append_data(&mut header, name, *data).unwrap();
    }
    tar.into_inner().unwrap().finish().unwrap()
}

fn zip_archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
    use std::io::Write;
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for (name, data) in entries {
        zip.start_file(*name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(data).unwrap();
    }
    zip.finish().unwrap().into_inner()
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
    assert!(extract_binary(b"not an archive", ArchiveKind::Zip, 1024).is_err());
}

/// A `decdn` entry that is not a regular file (a symlink, a directory) reads
/// as zero bytes; installing that would leave no binary at all.
#[test]
fn extract_binary_rejects_a_symlink_entry() {
    let archive = tar_gz_typed(&[("decdn", b"", tar::EntryType::Symlink)]);
    let err = extract_binary(&archive, ArchiveKind::TarGz, 1024).unwrap_err();
    assert!(err.to_string().contains("not a regular file"), "{err}");
}

#[test]
fn extract_binary_rejects_an_empty_binary() {
    let archive = tar_gz(&[("decdn", b"")]);
    let err = extract_binary(&archive, ArchiveKind::TarGz, 1024).unwrap_err();
    assert!(err.to_string().contains("is empty"), "{err}");
    let archive = zip_archive(&[("decdn.exe", b"")]);
    let err = extract_binary(&archive, ArchiveKind::Zip, 1024).unwrap_err();
    assert!(err.to_string().contains("is empty"), "{err}");
}

/// A Windows release archive is a zip holding `decdn.exe` at its root.
#[test]
fn extract_binary_reads_decdn_exe_from_a_zip() {
    let archive = zip_archive(&[("decdn.exe", b"new exe")]);
    assert_eq!(
        extract_binary(&archive, ArchiveKind::Zip, 1024).unwrap(),
        b"new exe"
    );
}

#[test]
fn extract_binary_rejects_a_nested_zip_entry() {
    let archive = zip_archive(&[("bin/decdn.exe", b"nested"), ("decdn", b"wrong name")]);
    let err = extract_binary(&archive, ArchiveKind::Zip, 1024).unwrap_err();
    assert!(err.to_string().contains("no `decdn.exe` entry"), "{err}");
}

#[test]
fn extract_binary_caps_a_zip_entry() {
    let archive = zip_archive(&[("decdn.exe", &[0u8; 2048])]);
    let err = extract_binary(&archive, ArchiveKind::Zip, 1024).unwrap_err();
    assert!(err.to_string().contains("larger than"), "{err}");
}

/// On Unix the swap is one rename of the staged sibling over the binary:
/// atomic, mode kept, and the staged name gone afterwards.
#[cfg(unix)]
#[test]
fn swap_renames_the_staged_file_over_the_binary() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let exe = dir.path().join("decdn");
    std::fs::write(&exe, b"old").unwrap();
    let staged = stage(dir.path(), b"new binary").unwrap();
    let staged_path = staged.path().to_path_buf();
    swap(staged, &exe).unwrap();
    assert_eq!(std::fs::read(&exe).unwrap(), b"new binary");
    assert!(!staged_path.exists());
    let mode = std::fs::metadata(&exe).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o755, "mode {mode:o}");
}

/// A swap that fails keeps the verified download and names it, so nothing
/// the user paid to fetch and verify is thrown away.
#[cfg(unix)]
#[test]
fn a_failed_swap_keeps_the_staged_file() {
    let dir = tempfile::tempdir().unwrap();
    let staged = stage(dir.path(), b"new binary").unwrap();
    let staged_path = staged.path().to_path_buf();
    // Renaming over a non-empty directory fails.
    let target = dir.path().join("occupied");
    std::fs::create_dir_all(target.join("child")).unwrap();
    let err = swap(staged, &target).unwrap_err();
    assert!(
        err.to_string().contains(staged_path.to_str().unwrap()),
        "{err}"
    );
    assert_eq!(std::fs::read(&staged_path).unwrap(), b"new binary");
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

/// The probe names the OS reason, not the temporary file it tried to make:
/// the user is told which directory refused, and why, by the caller.
#[cfg(unix)]
#[test]
fn probe_writable_reports_the_os_reason_without_the_probe_path() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    let result = probe_writable(dir.path());
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let Err(err) = result else {
        return; // root writes anywhere
    };
    let text = err.to_string();
    assert!(text.to_lowercase().contains("permission denied"), "{text}");
    assert!(!text.contains(".decdn-update-probe"), "{text}");
}
