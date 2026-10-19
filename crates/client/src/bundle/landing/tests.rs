use super::*;
use crate::bundle::STAGING_DIR;

/// A duplicate destination is materialized from the canonical file (no second
/// paid fetch), lands with identical bytes, creates any missing parent dir,
/// and leaves the source in place (it is a link/copy, not a move).
#[test]
fn link_or_copy_atomic_materializes_identical_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let landing = Landing::open(dir.path()).unwrap();
    let src = dir.path().join("src.bin");
    let dest = dir.path().join("nested/dest.bin");
    std::fs::write(&src, b"the-canonical-bytes").unwrap();

    landing.link_or_copy_atomic(&src, &dest).unwrap();

    assert_eq!(std::fs::read(&dest).unwrap(), b"the-canonical-bytes");
    assert!(src.exists());
}

/// Materialization atomically *replaces* an existing destination, upholding the
/// "a present final file is verified-good" invariant skip-existing relies on.
#[test]
fn link_or_copy_atomic_replaces_existing_dest() {
    let dir = tempfile::tempdir().unwrap();
    let landing = Landing::open(dir.path()).unwrap();
    let src = dir.path().join("src.bin");
    let dest = dir.path().join("dest.bin");
    std::fs::write(&src, b"new").unwrap();
    std::fs::write(&dest, b"stale-and-longer").unwrap();

    landing.link_or_copy_atomic(&src, &dest).unwrap();

    assert_eq!(std::fs::read(&dest).unwrap(), b"new");
}

/// A landed link leaves no `.decdn-link-*` staging name beside it.
#[test]
fn link_or_copy_atomic_leaves_no_staging_name_behind() {
    let dir = tempfile::tempdir().unwrap();
    let landing = Landing::open(dir.path()).unwrap();
    let src = dir.path().join("src.bin");
    std::fs::write(&src, b"bytes").unwrap();

    landing
        .link_or_copy_atomic(&src, &dir.path().join("sub/dest.bin"))
        .unwrap();

    let names: Vec<String> = std::fs::read_dir(dir.path().join("sub"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, ["dest.bin"]);
}

/// `materialize` (the paid-path writer) atomically replaces an existing
/// destination from the staging file, and leaves the staging file intact so a
/// retry for the next duplicate path can read it again.
#[test]
fn materialize_replaces_dest_and_keeps_staging() {
    let dir = tempfile::tempdir().unwrap();
    let landing = Landing::open(dir.path()).unwrap();
    let staging = dir.path().join("blob.partial");
    std::fs::write(&staging, b"new-content").unwrap();
    let dest = dir.path().join("out.bin");
    std::fs::write(&dest, b"stale-old").unwrap();

    let n = landing.materialize(&staging, &dest).unwrap();

    assert_eq!(n, b"new-content".len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), b"new-content");
    assert!(staging.exists(), "staging must survive for retries");
    assert_eq!(std::fs::read(&staging).unwrap(), b"new-content");
}

/// A non-donor blob's first write moves staging into place — no second copy
/// of the blob — and replaces a stale destination atomically.
#[test]
fn first_write_moves_a_non_donor_blob_into_place() {
    let dir = tempfile::tempdir().unwrap();
    let landing = Landing::open(dir.path()).unwrap();
    let staging = dir.path().join("blob");
    std::fs::write(&staging, b"new-content").unwrap();
    let dest = dir.path().join("sub").join("out.bin");
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    std::fs::write(&dest, b"stale-old").unwrap();

    let n = landing.first_write(&staging, &dest, false).unwrap();

    assert_eq!(n, b"new-content".len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), b"new-content");
    assert!(!staging.exists(), "the blob moved, it was not copied");
}

/// A donor blob's first write keeps its staging name — later recipients
/// still splice from it — and shares its storage with the destination.
#[cfg(unix)]
#[test]
fn first_write_links_a_donor_blob_and_keeps_staging() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let landing = Landing::open(dir.path()).unwrap();
    let staging = dir.path().join("blob");
    std::fs::write(&staging, b"donor-bytes").unwrap();
    let dest = dir.path().join("out.bin");

    let n = landing.first_write(&staging, &dest, true).unwrap();

    assert_eq!(n, b"donor-bytes".len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), b"donor-bytes");
    assert!(staging.exists(), "a donor's staging name survives");
    assert_eq!(
        std::fs::metadata(&staging).unwrap().ino(),
        std::fs::metadata(&dest).unwrap().ino(),
        "the first destination is a hard link, not a copy"
    );
}

/// An output directory that does not exist yet is created.
#[test]
fn open_creates_a_missing_output_directory() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("new/out");
    let landing = Landing::open(&out).unwrap();
    assert_eq!(landing.root(), out);
    assert!(out.is_dir());
}

/// A destination outside the root is refused before anything is written.
#[test]
fn a_destination_outside_the_root_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let landing = Landing::open(dir.path()).unwrap();
    let staging = dir.path().join("blob");
    std::fs::write(&staging, b"bytes").unwrap();

    let err = landing
        .materialize(&staging, &other.path().join("file"))
        .unwrap_err();

    assert!(
        format!("{err:#}").contains("not under the output directory"),
        "{err:#}"
    );
    assert!(!other.path().join("file").exists());
}

/// An output root whose `link` entry points outside it, with the entry
/// `link/file` that lexically sits under the root: `(out, outside, landing)`.
#[cfg(unix)]
fn escaping_root() -> (tempfile::TempDir, tempfile::TempDir, Landing) {
    let out = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), out.path().join("link")).unwrap();
    let landing = Landing::open(out.path()).unwrap();
    (out, outside, landing)
}

/// `err` refused the entry `link/file` for leaving the root, and `outside`
/// holds nothing: no file, no temp, no `.decdn-link-*` name.
#[cfg(unix)]
fn assert_refused(err: &anyhow::Error, outside: &Path) {
    let text = format!("{err:#}");
    assert!(text.contains("outside the output directory"), "{text}");
    assert!(text.contains("link/file"), "{text}");
    let litter: Vec<_> = std::fs::read_dir(outside).unwrap().collect();
    assert!(
        litter.is_empty(),
        "nothing lands outside the root: {litter:?}"
    );
}

/// A move through a symlinked ancestor that leaves the root fails the entry
/// and keeps the fetched blob in staging for a rerun.
#[cfg(unix)]
#[test]
fn move_into_place_refuses_a_symlinked_ancestor_that_leaves_the_root() {
    let (out, outside, landing) = escaping_root();
    let staging = out.path().join(STAGING_DIR).join("blob");
    std::fs::create_dir_all(staging.parent().unwrap()).unwrap();
    std::fs::write(&staging, b"bytes").unwrap();

    let err = landing
        .move_into_place(&staging, &out.path().join("link/file"))
        .unwrap_err();

    assert_refused(&err, outside.path());
    assert!(staging.exists(), "the fetched blob is kept for a rerun");
}

/// A copy through a symlinked ancestor that leaves the root fails the entry.
#[cfg(unix)]
#[test]
fn materialize_refuses_a_symlinked_ancestor_that_leaves_the_root() {
    let (out, outside, landing) = escaping_root();
    let staging = out.path().join("blob");
    std::fs::write(&staging, b"bytes").unwrap();

    let err = landing
        .materialize(&staging, &out.path().join("link/file"))
        .unwrap_err();

    assert_refused(&err, outside.path());
    assert!(staging.exists());
}

/// A link or copy through a symlinked ancestor that leaves the root fails the
/// entry.
#[cfg(unix)]
#[test]
fn link_or_copy_atomic_refuses_a_symlinked_ancestor_that_leaves_the_root() {
    let (out, outside, landing) = escaping_root();
    let src = out.path().join("src.bin");
    std::fs::write(&src, b"bytes").unwrap();

    let err = landing
        .link_or_copy_atomic(&src, &out.path().join("link/file"))
        .unwrap_err();

    assert_refused(&err, outside.path());
}

/// A relative symlink that stays inside the root is followed: the entry
/// lands where the link points.
#[cfg(unix)]
#[test]
fn a_relative_symlink_inside_the_root_still_lands() {
    let out = tempfile::tempdir().unwrap();
    std::fs::create_dir(out.path().join("real")).unwrap();
    std::os::unix::fs::symlink("real", out.path().join("alias")).unwrap();
    let landing = Landing::open(out.path()).unwrap();
    let staging = out.path().join("blob");
    std::fs::write(&staging, b"bytes").unwrap();

    let n = landing
        .first_write(&staging, &out.path().join("alias/file"), false)
        .unwrap();

    assert_eq!(n, 5);
    assert_eq!(
        std::fs::read(out.path().join("real/file")).unwrap(),
        b"bytes"
    );
}

/// A symlink with an absolute target is an escape, even one that points back
/// inside the root.
#[cfg(unix)]
#[test]
fn an_absolute_symlink_inside_the_root_is_an_escape() {
    let out = tempfile::tempdir().unwrap();
    std::fs::create_dir(out.path().join("real")).unwrap();
    std::os::unix::fs::symlink(out.path().join("real"), out.path().join("alias")).unwrap();
    let landing = Landing::open(out.path()).unwrap();
    let staging = out.path().join("blob");
    std::fs::write(&staging, b"bytes").unwrap();

    let err = landing
        .first_write(&staging, &out.path().join("alias/file"), false)
        .unwrap_err();

    assert!(
        format!("{err:#}").contains("outside the output directory"),
        "{err:#}"
    );
    assert!(!out.path().join("real/file").exists());
}

/// A staging directory that is a symlink elsewhere still lands its blob: the
/// move is refused, and the copy fallback reads the blob by its own path.
#[cfg(unix)]
#[test]
fn a_symlinked_staging_dir_lands_through_the_copy_fallback() {
    let out = tempfile::tempdir().unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(elsewhere.path(), out.path().join(STAGING_DIR)).unwrap();
    let landing = Landing::open(out.path()).unwrap();
    let staging = out.path().join(STAGING_DIR).join("blob");
    std::fs::write(&staging, b"bytes").unwrap();
    let dest = out.path().join("file");

    let n = landing.move_into_place(&staging, &dest).unwrap();

    assert_eq!(n, 5);
    assert_eq!(std::fs::read(&dest).unwrap(), b"bytes");
    assert!(staging.exists(), "copied, not moved");
}

/// The pre-fetch check refuses a parent chain that leads out of the root and
/// accepts one that does not exist yet.
#[cfg(unix)]
#[test]
fn check_refuses_an_escaping_parent_and_accepts_a_missing_one() {
    let (out, _outside, landing) = escaping_root();

    let err = landing.check(&out.path().join("link/file")).unwrap_err();
    assert!(
        format!("{err:#}").contains("outside the output directory"),
        "{err:#}"
    );

    landing.check(&out.path().join("new/deep/file")).unwrap();
    landing.check(&out.path().join("top")).unwrap();
    assert!(
        !out.path().join("new").exists(),
        "the check creates nothing"
    );
}
