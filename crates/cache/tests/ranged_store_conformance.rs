//! Runs the shared `decdn-bao-range` conformance suite against
//! [`decdn_cache::NodeRangedStore`] — the node-side `RangedStore` backend
//! over the iroh-blobs cache. Exercises that suite against a concrete
//! backend (#1621).
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
use decdn_cache::NodeRangedStore;
use iroh_blobs::Hash;
use tempfile::TempDir;

mod util;

/// Produces a fresh, empty `CacheEngine` per blob and keeps every backing
/// `TempDir` alive for the duration of the suite (dropping it would delete
/// the store directory out from under the engine).
struct NodeFactory {
    tmps: Mutex<Vec<TempDir>>,
}

impl ConformanceFactory for NodeFactory {
    type Store = NodeRangedStore;

    fn make(
        &self,
        root: [u8; 32],
        total_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = NodeRangedStore> + Send + '_>> {
        Box::pin(async move {
            let (engine, tmp) = util::empty_engine().await.expect("empty_engine");
            self.tmps.lock().expect("lock").push(tmp);
            NodeRangedStore::new(engine, Hash::from(root), total_bytes)
        })
    }
}

#[tokio::test]
async fn node_backend_conformance() {
    let factory = NodeFactory {
        tmps: Mutex::new(Vec::new()),
    };
    run_all(factory).await;
}
