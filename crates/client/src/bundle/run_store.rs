//! [`RunStore`]: the one iroh-blobs store a bundle puller fetches every entry
//! into, at `<out_root>/.decdn-partial/store`.
//!
//! Every entry keeps its verified bytes in the same store, so an interrupted
//! pull resumes each entry from it. The store opens on first use and stays
//! open for the puller. At the end ([`RunStore::close`]) it is removed when
//! every pull the puller started settled every entry, and kept otherwise for
//! the next run. It is also removed when an entry met a lost export of its
//! blob ([`crate::ranged_store::StaleStoreEntry`]), which only a new store
//! fetches again. When every pull settled, each pull's whole staging
//! directory (`<out_root>/.decdn-partial/`) goes too: what it still holds
//! belongs to entries no pull selected, which a later pull fetches again.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use super::STAGING_DIR;
use crate::ranged_store::BlobDir;

/// The store a bundle puller fetches every entry into. See the module docs.
#[derive(Debug, Default)]
pub(super) struct RunStore {
    /// The open store, once a fetch or a read needed it.
    dir: tokio::sync::OnceCell<BlobDir>,
    /// Pulls started.
    started: AtomicUsize,
    /// Pulls that settled every entry: none failed, and the pull was not cut
    /// short.
    settled: AtomicUsize,
    /// An entry met a lost export: the store goes at the end.
    lost: AtomicBool,
    /// The staging directory of each pull started.
    staging: Mutex<Vec<PathBuf>>,
}

impl RunStore {
    /// The puller's store for `out_root`, opened (and created) on first use.
    ///
    /// # Errors
    ///
    /// The store cannot open.
    pub(super) async fn open(&self, out_root: &Path) -> anyhow::Result<&BlobDir> {
        let path = out_root.join(STAGING_DIR).join("store");
        self.dir.get_or_try_init(|| BlobDir::open(&path)).await
    }

    /// Record that a pull into `out_root` started.
    pub(super) fn pull_started(&self, out_root: &Path) {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.staging
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(out_root.join(STAGING_DIR));
    }

    /// Record that a pull settled every entry.
    pub(super) fn pull_settled(&self) {
        self.settled.fetch_add(1, Ordering::SeqCst);
    }

    /// Record that an entry met a lost export of its blob.
    pub(super) fn note_lost_export(&self) {
        self.lost.store(true, Ordering::SeqCst);
    }

    /// End the puller's use of the store: remove it when nothing in it is
    /// needed, else stop it so every write it took is committed. When every
    /// pull settled, remove each pull's staging directory too.
    pub(super) async fn close(self) {
        let done = self.started.into_inner() == self.settled.into_inner();
        if let Some(dir) = self.dir.into_inner() {
            let closed = if done || self.lost.into_inner() {
                dir.remove().await
            } else {
                dir.shutdown().await
            };
            if let Err(e) = closed {
                tracing::warn!(error = %e, "closing the download store");
            }
        }
        if !done {
            return;
        }
        let staging = self
            .staging
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        for dir in staging {
            match tokio::fs::remove_dir_all(&dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        dir = %dir.display(),
                        "removing the staging directory"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests;
