use super::*;

#[cfg(unix)]
fn chmod(path: &Path, mode: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

// Some CI runners (and this dev workstation) run under umask 002, so
// `tempfile::tempdir()` comes up with mode 0o775 — which the new validator
// rejects. Return a tempdir that's already 0o700 so tests exercising the
// happy path don't need to repeat the chmod.
fn secure_tempdir() -> anyhow::Result<tempfile::TempDir> {
    let dir = tempfile::tempdir()?;
    #[cfg(unix)]
    chmod(dir.path(), 0o700)?;
    Ok(dir)
}

/// Every `*.tmp.*` staging file left in `dir`.
fn tmp_files(dir: &Path) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|e| {
            let p = e.expect("dir entry").path();
            let name = p.file_name()?.to_str()?.to_string();
            name.contains(".tmp.").then_some(p)
        })
        .collect()
}

/// The core `keep()` contract, and the one that fails silently if the
/// `committed`-flag ordering ever regresses: `keep` would still return
/// `Ok(path)` and the caller would still print "preserved at …", pointing
/// at a file `Drop` had just deleted.
#[test]
fn keep_parks_the_key_and_it_survives_drop() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let staged = stage_node_key(dir.path())?;
    let public = staged.public();

    let parked = staged.keep()?;
    // `staged` is consumed and dropped by here — this is the assertion.
    assert!(parked.exists(), "the parked key must outlive the stage");
    assert_eq!(
        parked.file_name().and_then(|s| s.to_str()),
        Some(format!("{KEY_FILE_NAME}.pending.{public}").as_str()),
    );
    assert!(
        !key_path(dir.path()).exists(),
        "parking must NOT install the key as node.secret — the whole point \
         is that no daemon loads it"
    );
    assert!(tmp_files(dir.path()).is_empty(), "no staging litter");

    // The parked bytes must be the secret for the id in the filename, or
    // "move this over node.secret" bricks the node.
    let bytes = fs::read(&parked)?;
    let raw: [u8; KEY_LEN] = bytes.as_slice().try_into().expect("32 raw bytes");
    assert_eq!(SecretKey::from_bytes(&raw).public(), public);
    Ok(())
}

/// The inverse of `commit`'s policy: `keep` is only ever called when the key
/// may already be bound on-chain, so a failure must leave the material on
/// disk and name it, not reclaim it.
#[test]
fn keep_leaves_the_temp_in_place_when_the_rename_fails() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let staged = stage_node_key(dir.path())?;
    let tmp = staged.tmp.clone();

    // Make the destination un-renameable by putting a *directory* where the
    // parked file would go. `fs::rename` of a file onto a non-empty dir path
    // fails on every platform we target.
    let dest = dir
        .path()
        .join(format!("{KEY_FILE_NAME}.pending.{}", staged.public()));
    fs::create_dir(&dest)?;
    fs::write(dest.join("occupied"), b"x")?;

    let err = staged
        .keep()
        .expect_err("rename onto a non-empty dir fails");
    assert!(
        tmp.exists(),
        "a key that may already be bound on-chain must not be deleted when parking fails"
    );
    assert!(
        format!("{err:#}").contains(&tmp.display().to_string()),
        "the error must name the surviving path: {err:#}"
    );
    Ok(())
}

/// A stage that is neither committed nor kept must leave nothing behind —
/// the pre-existing `Drop` contract, stated directly now that there is a
/// third outcome it has to stay distinct from.
#[test]
fn an_abandoned_stage_leaves_nothing() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    drop(stage_node_key(dir.path())?);
    assert!(tmp_files(dir.path()).is_empty());
    assert!(!key_path(dir.path()).exists());
    Ok(())
}

/// `commit` keeps the opposite policy to `keep`, and the difference is
/// deliberate: a failed install means the key was never bound on-chain, so
/// reclaiming it is right. Pinned so the two policies cannot be "unified"
/// by mistake — `keep`'s whole point is that it does NOT do this.
#[cfg(unix)]
#[test]
fn a_failed_commit_still_reclaims_the_temp() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let mut staged = stage_node_key(dir.path())?;
    let tmp = staged.tmp.clone();
    // A pre-existing `node.secret` forces `install_staged` through
    // `move_aside` first, and a read-only data dir makes that archive
    // rename fail — the realistic shape (a permissions problem), rather
    // than deleting the temp, which would make the assertion vacuous.
    fs::write(key_path(dir.path()), [0u8; KEY_LEN])?;
    chmod(dir.path(), 0o500)?;

    let failed = staged.commit();

    // Restore write access BEFORE the drop, or `Drop`'s cleanup is the
    // thing that fails and the assertion below tests the chmod, not the
    // policy.
    chmod(dir.path(), 0o700)?;
    failed.expect_err("archiving into a read-only dir must fail");
    drop(staged);
    assert!(!tmp.exists(), "a never-bound key is reclaimed on drop");
    Ok(())
}

#[test]
fn generates_and_reloads() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let k1 = load_or_generate(dir.path())?;
    let k2 = load_or_generate(dir.path())?;
    assert_eq!(k1.to_bytes(), k2.to_bytes());
    Ok(())
}

#[test]
fn load_or_create_reports_whether_it_generated() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let first = load_or_create(dir.path())?;
    let second = load_or_create(dir.path())?;
    assert!(first.generated, "an empty data_dir generates a key");
    assert!(!second.generated, "an existing key is read, not replaced");
    assert_eq!(first.key.to_bytes(), second.key.to_bytes());
    Ok(())
}

// `load` reads the same key `load_or_generate` persisted, without a second
// generate path that could diverge.
#[test]
fn load_reads_the_persisted_key() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let generated = load_or_generate(dir.path())?;
    let loaded = load(dir.path())?;
    assert_eq!(generated.to_bytes(), loaded.to_bytes());
    Ok(())
}

// The whole point of `load` over `load_or_generate`: an absent key is an
// error, and the call writes nothing — `whoami` must never mint an identity.
#[test]
fn load_errors_on_absent_key_and_writes_nothing() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let err = load(dir.path()).expect_err("absent key must error, not generate");
    assert!(
        format!("{err:#}").contains(&key_path(dir.path()).display().to_string()),
        "error must name the missing key path: {err:#}"
    );
    assert!(
        !key_path(dir.path()).exists(),
        "load must not create node.secret"
    );
    Ok(())
}

// `load` keeps `load_or_generate`'s permission validation: a world-readable
// key is rejected rather than loaded.
#[cfg(unix)]
#[test]
fn load_rejects_insecure_key_file() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let path = key_path(dir.path());
    fs::write(&path, [0u8; KEY_LEN])?;
    chmod(&path, 0o644)?;
    let err = load(dir.path()).expect_err("world-readable key must be rejected");
    assert!(
        format!("{err:#}").contains("invalid node.secret"),
        "{err:#}"
    );
    Ok(())
}

#[test]
fn rejects_wrong_size() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let path = key_path(dir.path());
    fs::write(&path, b"too short")?;
    // `fs::write` respects umask, typically producing 0o644 — which the
    // new `validate_key_file` rejects before the size check runs. Tighten
    // to 0o600 so this test continues to exercise the size-rejection path.
    #[cfg(unix)]
    chmod(&path, 0o600)?;
    let err = load_or_generate(dir.path()).expect_err("should reject wrong-sized key file");
    assert!(
        err.to_string().contains("32 bytes")
            || err.chain().any(|c| c.to_string().contains("32 bytes")),
        "expected size error, got: {err:#}"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn accepts_0700_data_dir() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let k1 = load_or_generate(dir.path())?;
    let k2 = load_or_generate(dir.path())?;
    assert_eq!(k1.to_bytes(), k2.to_bytes());
    Ok(())
}

#[cfg(unix)]
#[test]
fn rejects_world_writable_data_dir() -> anyhow::Result<()> {
    assert_data_dir_mode_rejected(0o777)
}

// Locks in the strict-mode policy: `0o770` still allows any group member
// to replace `node.secret`, which is the same attack class as 0o777.
#[cfg(unix)]
#[test]
fn rejects_group_writable_data_dir() -> anyhow::Result<()> {
    assert_data_dir_mode_rejected(0o770)
}

// The next three lock the `FORBIDDEN_BITS = 0o077` mask against a
// regression that narrows it to writable-only (which would silently
// accept group/world-*readable* but not writable dirs). Each mode sets
// exactly one forbidden bit — no overlap.
#[cfg(unix)]
#[test]
fn rejects_group_readable_data_dir() -> anyhow::Result<()> {
    assert_data_dir_mode_rejected(0o740)
}

#[cfg(unix)]
#[test]
fn rejects_world_readable_data_dir() -> anyhow::Result<()> {
    assert_data_dir_mode_rejected(0o704)
}

#[cfg(unix)]
#[test]
fn rejects_world_exec_only_data_dir() -> anyhow::Result<()> {
    assert_data_dir_mode_rejected(0o701)
}

#[cfg(unix)]
fn assert_data_dir_mode_rejected(mode: u32) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    chmod(dir.path(), mode)?;
    let err =
        load_or_generate(dir.path()).expect_err(&format!("should reject data_dir mode {mode:#o}"));
    let msg = format!("{err:#}");
    assert!(msg.contains("invalid data_dir"), "unexpected: {msg}");
    assert!(msg.contains("insecure permissions"), "unexpected: {msg}");
    Ok(())
}

// Catches a refactor that replaced `is_dir()` with `!is_file()` — a
// regular file would pass the latter even though it's not a directory.
#[cfg(unix)]
#[test]
fn rejects_file_as_data_dir() -> anyhow::Result<()> {
    let parent = secure_tempdir()?;
    let file = parent.path().join("not_a_dir");
    fs::write(&file, b"")?;
    chmod(&file, 0o600)?;
    let err = load_or_generate(&file).expect_err("should reject file as data_dir");
    let msg = format!("{err:#}");
    assert!(msg.contains("is not a directory"), "unexpected: {msg}");
    Ok(())
}

// Symmetric counterpart to `rejects_group_readable_data_dir`: guards the
// key-file side of the `0o077` mask against a writable-only narrowing.
#[cfg(unix)]
#[test]
fn rejects_world_readable_key_file() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let path = key_path(dir.path());
    fs::write(&path, [0u8; KEY_LEN])?;
    chmod(&path, 0o604)?;
    let err = load_or_generate(dir.path()).expect_err("should reject world-readable key");
    let msg = format!("{err:#}");
    assert!(msg.contains("invalid node.secret"), "unexpected: {msg}");
    assert!(msg.contains("0o604"), "expected mode in message: {msg}");
    Ok(())
}

#[cfg(unix)]
#[test]
fn rejects_key_file_with_group_access() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let path = key_path(dir.path());
    fs::write(&path, [0u8; KEY_LEN])?;
    chmod(&path, 0o640)?;
    let err = load_or_generate(dir.path()).expect_err("should reject group-readable key");
    let msg = format!("{err:#}");
    assert!(msg.contains("invalid node.secret"), "unexpected: {msg}");
    assert!(msg.contains("0o640"), "expected mode in message: {msg}");
    Ok(())
}

// A symlink at the key path — even to a well-formed 32-byte file with
// mode 0o600 — must be rejected. Without this check, an attacker with
// write access to `data_dir` could replace `node.secret` with a symlink
// pointing at another user's key.
#[cfg(unix)]
#[test]
fn rejects_non_regular_key_file() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let target_dir = secure_tempdir()?;
    let target = target_dir.path().join("attacker_key");
    let sentinel = [0xAAu8; KEY_LEN];
    fs::write(&target, sentinel)?;
    chmod(&target, 0o600)?;
    std::os::unix::fs::symlink(&target, key_path(dir.path()))?;
    let err = load_or_generate(dir.path()).expect_err("should reject symlinked key");
    let msg = format!("{err:#}");
    assert!(msg.contains("not a regular file"), "unexpected: {msg}");
    // Confirm the symlink target wasn't overwritten by a key-gen fallback.
    assert_eq!(fs::read(&target)?, sentinel, "target was modified");
    Ok(())
}

// Documents the "check the path, not the target" tradeoff: a symlink at
// `data_dir` itself is rejected by the `is_dir()` check on
// `symlink_metadata`, even if the target is a valid 0o700 directory.
#[cfg(unix)]
#[test]
fn rejects_symlinked_data_dir() -> anyhow::Result<()> {
    let parent = secure_tempdir()?;
    let real = parent.path().join("real");
    fs::create_dir(&real)?;
    chmod(&real, 0o700)?;
    let link = parent.path().join("link");
    std::os::unix::fs::symlink(&real, &link)?;
    let err = load_or_generate(&link).expect_err("should reject symlinked data_dir");
    let msg = format!("{err:#}");
    assert!(msg.contains("is not a directory"), "unexpected: {msg}");
    Ok(())
}

// Guards against a regression where a dangling symlink at `node.secret`
// would be treated as "not existing" by a plain `path.exists()` check and
// then silently overwritten via `rename`, destroying whatever the
// attacker pointed at.
#[cfg(unix)]
#[test]
fn rejects_dangling_symlink_at_key_path() -> anyhow::Result<()> {
    let dir = secure_tempdir()?;
    let nowhere = dir.path().join("does_not_exist");
    std::os::unix::fs::symlink(&nowhere, key_path(dir.path()))?;
    let err = load_or_generate(dir.path()).expect_err("should reject dangling symlink");
    let msg = format!("{err:#}");
    assert!(msg.contains("not a regular file"), "unexpected: {msg}");
    // Confirm nothing was written to the symlink target.
    assert!(
        !nowhere.exists(),
        "load_or_generate must not follow the symlink"
    );
    Ok(())
}

// Guards the `DirBuilder::mode(0o700)` call against a future refactor
// that drops `DirBuilderExt`. Stat the created leaf explicitly rather
// than trusting the OS umask.
#[cfg(unix)]
#[test]
fn creates_dir_with_0700_on_first_run() -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let parent = secure_tempdir()?;
    let sub = parent.path().join("sub");
    let leaf = sub.join("data");
    load_or_generate(&leaf)?;
    // Both the leaf and any intermediate dir this call created must be
    // 0o700 — `DirBuilderExt::mode` applies to every dir the single
    // `create` call materializes. Checking both guards against a future
    // refactor that switched to `create_dir_all` (which ignores the mode
    // on intermediates).
    let leaf_mode = fs::metadata(&leaf)?.mode() & 0o777;
    let sub_mode = fs::metadata(&sub)?.mode() & 0o777;
    assert_eq!(leaf_mode, 0o700, "leaf: expected 0o700, got {leaf_mode:#o}");
    assert_eq!(sub_mode, 0o700, "sub: expected 0o700, got {sub_mode:#o}");
    Ok(())
}
