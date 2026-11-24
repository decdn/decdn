//! [`RunStore`]: the one iroh-blobs store a bundle puller fetches every entry
//! into, at `<out_root>/.decdn-partial/store`.
//!
//! Every entry keeps its verified bytes in the same store, so an interrupted
//! pull resumes each entry from it. The store opens on first use and stays
//! open for the puller. At the end ([`RunStore::close`]) it is removed when
//! every pull the puller started settled every entry, and kept otherwise for
//! the next run. It is also removed when an entry met a lost export of its
//! blob ([`crate::ranged_store::StaleStoreEntry`]), which only a new store
//! fetches again.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    /// Record that a pull started.
    pub(super) fn pull_started(&self) {
        self.started.fetch_add(1, Ordering::SeqCst);
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
    /// needed, else stop it so every write it took is committed.
    pub(super) async fn close(self) {
        let Some(dir) = self.dir.into_inner() else {
            return;
        };
        let done = self.started.into_inner() == self.settled.into_inner();
        let closed = if done || self.lost.into_inner() {
            dir.remove().await
        } else {
            dir.shutdown().await
        };
        if let Err(e) = closed {
            tracing::warn!(error = %e, "closing the download store");
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests;
