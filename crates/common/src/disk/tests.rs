use super::*;

#[test]
fn read_disk_space_of_tempdir_is_nonzero() {
    let dir = tempfile::tempdir().unwrap();
    let s = read_disk_space(dir.path()).unwrap();
    assert!(s.total > 0);
}

#[test]
fn statvfs_target_walks_to_existing_ancestor() {
    let dir = tempfile::tempdir().unwrap();
    // A not-yet-created child resolves against its existing parent volume.
    let missing = dir.path().join("does/not/exist/yet");
    let s = statvfs_target(&missing).unwrap();
    assert!(s.total > 0);
}
