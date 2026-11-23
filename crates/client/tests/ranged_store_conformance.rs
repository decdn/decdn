//! Runs the shared decdn-bao-range conformance suite against
//! `decdn_client::ClientRangedStore`, over a file store and over an in-memory
//! one — proving the client backend meets the same cross-backend contract as
//! the node backend (#1621).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use decdn_bao_range::conformance::{ConformanceFactory, run_all};
use decdn_client::ClientRangedStore;
use tempfile::TempDir;

/// Each store in a file store of its own.
struct OnDisk {
    tmps: Mutex<Vec<TempDir>>,
}

impl ConformanceFactory for OnDisk {
    type Store = ClientRangedStore;
    fn make(
        &self,
        root: [u8; 32],
        total_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = ClientRangedStore> + Send + '_>> {
        Box::pin(async move {
            let tmp = TempDir::new().expect("tempdir");
            let store = ClientRangedStore::on_disk(tmp.path(), root, total_bytes)
                .await
                .expect("open");
            self.tmps.lock().expect("lock").push(tmp);
            store
        })
    }
}

/// Each store in memory.
struct InMemory;

impl ConformanceFactory for InMemory {
    type Store = ClientRangedStore;
    fn make(
        &self,
        root: [u8; 32],
        total_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = ClientRangedStore> + Send + '_>> {
        Box::pin(async move { ClientRangedStore::in_memory(root, total_bytes) })
    }
}

#[tokio::test]
async fn client_backend_conformance_on_disk() {
    run_all(OnDisk {
        tmps: Mutex::new(Vec::new()),
    })
    .await;
}

#[tokio::test]
async fn client_backend_conformance_in_memory() {
    run_all(InMemory).await;
}
