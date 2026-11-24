use decdn_bao_range::{CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE};

use super::{RunStore, store_path};

/// A deterministic blob of `len` bytes and its bao root.
fn blob(len: u64) -> (Vec<u8>, [u8; 32]) {
    let data: Vec<u8> = (0..len)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let root = *bao_tree::io::outboard::PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE)
        .root
        .as_bytes();
    (data, root)
}

/// The root of the blob [`store_with_a_partial`] holds the head of.
fn root_of_partial() -> [u8; 32] {
    blob(4 * CHUNK_GROUP_BYTES).1
}

/// A run store under a fresh `out_root` holding the first chunk group of a
/// four-group blob, and that blob's root.
async fn store_with_a_partial(out_root: &std::path::Path) -> anyhow::Result<(RunStore, [u8; 32])> {
    let total = 4 * CHUNK_GROUP_BYTES;
    let (data, root) = blob(total);
    let run = RunStore::default();
    run.open(out_root)
        .await?
        .blob(root, total, None)
        .await?
        .admit_prefix(&data, CHUNK_GROUP_BYTES)
        .await?;
    Ok((run, root))
}

#[tokio::test]
async fn a_run_that_never_opened_its_store_creates_none() -> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let run = RunStore::default();
    assert!(run.existing(out.path()).await?.is_none());
    assert!(run.partials(out.path()).await.is_empty());
    run.close().await;
    assert!(!store_path(out.path()).exists());
    Ok(())
}

#[tokio::test]
async fn a_partial_blob_the_run_did_not_settle_keeps_the_store() -> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let (run, _) = store_with_a_partial(out.path()).await?;
    run.close().await;
    assert!(store_path(out.path()).exists());

    // The next run reads what the first one left.
    let next = RunStore::default();
    assert_eq!(
        next.partials(out.path()).await,
        vec![(root_of_partial(), CHUNK_GROUP_BYTES)]
    );
    next.close().await;
    Ok(())
}

#[tokio::test]
async fn a_store_whose_partials_all_settled_is_removed() -> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let (run, root) = store_with_a_partial(out.path()).await?;
    run.settle(root);
    run.close().await;
    assert!(!store_path(out.path()).exists());
    Ok(())
}

#[tokio::test]
async fn a_lost_export_removes_the_store() -> anyhow::Result<()> {
    let out = tempfile::tempdir()?;
    let (run, _) = store_with_a_partial(out.path()).await?;
    run.note_lost_export();
    run.close().await;
    assert!(!store_path(out.path()).exists());
    Ok(())
}

#[tokio::test]
async fn one_store_serves_one_out_root() -> anyhow::Result<()> {
    let (a, b) = (tempfile::tempdir()?, tempfile::tempdir()?);
    let run = RunStore::default();
    run.open(a.path()).await?;
    assert!(run.open(b.path()).await.is_err());
    run.close().await;
    Ok(())
}
