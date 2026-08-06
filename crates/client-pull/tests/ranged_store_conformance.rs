//! Runs the shared decdn-bao-range conformance suite against
//! `decdn_client_pull::ClientRangedStore` — proving the client backend meets the
//! same cross-backend contract as the node backend (#1621 P2).
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
use decdn_client_pull::ClientRangedStore;
use tempfile::TempDir;

struct ClientFactory {
    tmps: Mutex<Vec<TempDir>>,
}

impl ConformanceFactory for ClientFactory {
    type Store = ClientRangedStore;
    fn make(
        &self,
        root: [u8; 32],
        total_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = ClientRangedStore> + Send + '_>> {
        Box::pin(async move {
            let tmp = TempDir::new().expect("tempdir");
            let store =
                ClientRangedStore::create(tmp.path(), "blob", root, total_bytes).expect("create");
            self.tmps.lock().expect("lock").push(tmp);
            store
        })
    }
}

#[tokio::test]
async fn client_backend_conformance() {
    let factory = ClientFactory {
        tmps: Mutex::new(Vec::new()),
    };
    run_all(factory).await;
}
