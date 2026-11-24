use std::path::{Path, PathBuf};

use super::{RunStore, STAGING_DIR};

/// Where a puller into `out_root` keeps its store.
fn store_path(out_root: &Path) -> PathBuf {
    out_root.join(STAGING_DIR).join("store")
}

#[tokio::test]
async fn a_puller_that_never_opened_its_store_creates_none() {
    let out = tempfile::tempdir().expect("tmp");
    RunStore::default().close().await;
    assert!(!store_path(out.path()).exists());
}

#[tokio::test]
async fn a_pull_that_settled_every_entry_removes_the_store() -> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let run = RunStore::default();
    run.pull_started();
    run.open(out.path()).await?;
    run.pull_settled();
    run.close().await;
    assert!(!store_path(out.path()).exists());
    Ok(())
}

#[tokio::test]
async fn a_pull_that_left_work_keeps_the_store() -> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let run = RunStore::default();
    run.pull_started();
    run.open(out.path()).await?;
    run.close().await;
    assert!(store_path(out.path()).exists());
    Ok(())
}

#[tokio::test]
async fn a_lost_export_removes_the_store() -> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let run = RunStore::default();
    run.pull_started();
    run.open(out.path()).await?;
    run.note_lost_export();
    run.close().await;
    assert!(!store_path(out.path()).exists());
    Ok(())
}
