//! [`RunStore`]: the one iroh-blobs store a bundle run fetches every entry
//! into, at `<out_root>/.decdn-partial/store`.
//!
//! Every entry, and the manifest fetch, keeps its verified bytes in the same
//! store, so an interrupted run resumes each entry from it. The store opens
//! on first use and stays open for the run. At the run's end
//! ([`RunStore::close`]) it is removed once it holds nothing a later run
//! needs: no partial blob of an entry the run did not settle. It is also
//! removed when an entry met a lost export of its blob
//! ([`crate::ranged_store::StaleStoreEntry`]), which only a new store fetches
//! again.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use super::STAGING_DIR;
use crate::ranged_store::BlobDir;

/// The store's directory under the staging directory.
const STORE_DIR: &str = "store";

/// Where a run into `out_root` keeps its store.
pub(super) fn store_path(out_root: &Path) -> PathBuf {
    out_root.join(STAGING_DIR).join(STORE_DIR)
}

/// The store a bundle run fetches every entry into. See the module docs.
#[derive(Debug, Default)]
pub(super) struct RunStore {
    /// The open store, once a fetch or a read needed it.
    dir: tokio::sync::OnceCell<BlobDir>,
    /// Every blob the run landed or found already on disk: its partial
    /// bytes in the store are no longer needed.
    settled: Mutex<HashSet<[u8; 32]>>,
    /// An entry met a lost export: the store goes at the run's end.
    lost: AtomicBool,
}

impl RunStore {
    /// The run's store for `out_root`, opened (and created) on first use.
    ///
    /// # Errors
    ///
    /// The store cannot open, or it is open for another `out_root`: one
    /// puller pulls into one directory.
    pub(super) async fn open(&self, out_root: &Path) -> anyhow::Result<&BlobDir> {
        let path = store_path(out_root);
        let dir = self.dir.get_or_try_init(|| BlobDir::open(&path)).await?;
        anyhow::ensure!(
            dir.path() == path,
            "this pull keeps its store in {}, not {}",
            dir.path().display(),
            path.display()
        );
        Ok(dir)
    }

    /// The run's store for `out_root` if it is open or on disk, else `None`:
    /// a run that only reads the store creates none.
    ///
    /// # Errors
    ///
    /// As [`Self::open`], or the directory cannot be checked.
    pub(super) async fn existing(&self, out_root: &Path) -> anyhow::Result<Option<&BlobDir>> {
        if self.dir.get().is_none() && !tokio::fs::try_exists(store_path(out_root)).await? {
            return Ok(None);
        }
        self.open(out_root).await.map(Some)
    }

    /// Every blob the store for `out_root` holds only part of, with the
    /// verified bytes it holds of each. No store holds none, and a store that
    /// cannot be read logs at `debug` and reports none.
    pub(super) async fn partials(&self, out_root: &Path) -> Vec<([u8; 32], u64)> {
        let listed = async {
            let Some(dir) = self.existing(out_root).await? else {
                return anyhow::Ok(Vec::new());
            };
            let mut partials = Vec::new();
            for hash in dir.partial_hashes().await? {
                let held = dir
                    .blob(hash, u64::MAX, None)
                    .await?
                    .present_byte_ranges()
                    .iter()
                    .fold(0u64, |sum, &(_, len)| sum.saturating_add(len));
                partials.push((hash, held));
            }
            Ok(partials)
        };
        listed.await.unwrap_or_else(|e| {
            tracing::debug!("could not list the download store: {e:#}");
            Vec::new()
        })
    }

    /// Record that the run landed `hash`, or found it on disk.
    pub(super) fn settle(&self, hash: [u8; 32]) {
        self.settled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(hash);
    }

    /// Record that an entry met a lost export of its blob.
    pub(super) fn note_lost_export(&self) {
        self.lost.store(true, Ordering::SeqCst);
    }

    /// End the run's use of the store: remove it when it holds nothing a
    /// later run needs, else stop it so every write it took is committed.
    pub(super) async fn close(self) {
        let Some(dir) = self.dir.into_inner() else {
            return;
        };
        let lost = self.lost.load(Ordering::SeqCst);
        let settled = self
            .settled
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let needed = if lost {
            false
        } else {
            match dir.partial_hashes().await {
                Ok(partial) => partial.iter().any(|hash| !settled.contains(hash)),
                Err(e) => {
                    tracing::warn!(error = %e, "listing the download store; it stays");
                    true
                }
            }
        };
        let closed = if needed {
            dir.shutdown().await
        } else {
            dir.remove().await
        };
        if let Err(e) = closed {
            tracing::warn!(error = %e, "closing the download store");
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests;
