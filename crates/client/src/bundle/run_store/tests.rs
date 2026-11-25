use std::path::{Path, PathBuf};

use super::{RunStore, STAGING_DIR};

/// Where a puller into `out_root` keeps its store.
fn store_path(out_root: &Path) -> PathBuf {
    out_root.join(STAGING_DIR).join("store")
}

/// Write a finished export of a blob beside the store, as an entry's fetch
/// leaves it before the entry lands at its destination.
fn write_export(out_root: &Path) -> anyhow::Result<PathBuf> {
    let path = out_root
        .join(STAGING_DIR)
        .join(blake3::Hash::from_bytes([7; 32]).to_hex().as_str());
    std::fs::create_dir_all(out_root.join(STAGING_DIR))?;
    std::fs::write(&path, b"blob")?;
    Ok(path)
}

#[tokio::test]
async fn a_puller_that_never_opened_its_store_creates_none() {
    let out = tempfile::tempdir().expect("tmp");
    RunStore::default().close().await;
    assert!(!store_path(out.path()).exists());
}

#[tokio::test]
async fn a_pull_that_settled_every_entry_removes_the_staging_directory() -> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let run = RunStore::default();
    run.pull_started(out.path());
    run.open(out.path()).await?;
    write_export(out.path())?;
    run.pull_settled();
    run.close().await;
    assert!(!out.path().join(STAGING_DIR).exists());
    Ok(())
}

#[tokio::test]
async fn a_settled_pull_that_never_opened_its_store_removes_the_staging_directory()
-> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let run = RunStore::default();
    run.pull_started(out.path());
    write_export(out.path())?;
    run.pull_settled();
    run.close().await;
    assert!(!out.path().join(STAGING_DIR).exists());
    Ok(())
}

#[tokio::test]
async fn a_pull_that_left_work_keeps_the_store_and_its_exports() -> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let run = RunStore::default();
    run.pull_started(out.path());
    run.open(out.path()).await?;
    let export = write_export(out.path())?;
    run.close().await;
    assert!(store_path(out.path()).exists());
    assert!(export.exists());
    Ok(())
}

#[tokio::test]
async fn a_lost_export_removes_the_store_and_keeps_the_exports() -> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let run = RunStore::default();
    run.pull_started(out.path());
    run.open(out.path()).await?;
    let export = write_export(out.path())?;
    run.note_lost_export();
    run.close().await;
    assert!(!store_path(out.path()).exists());
    assert!(export.exists());
    Ok(())
}
