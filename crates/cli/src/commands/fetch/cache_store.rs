//! The `decdn gateway` cache as a [`Streamer`](decdn_client::Streamer)'s fill
//! store (#2881): each stream fills a [`CacheStore`] over the gateway's
//! [`CacheEngine`], so the bytes one request pays for serve the next request
//! for the same blob.

use decdn_cache::{CacheEngine, CacheStore, Hash};
use decdn_client::StreamStores;

/// Opens a [`CacheStore`] over one [`CacheEngine`] for each stream.
///
/// Every call for one hash opens a view of the same cache entry, and two
/// streams may fill it at the same time ([`CacheStore`]). Each stream fetches
/// what is missing when it plans a range, so two concurrent streams of one
/// blob can both pay for a range neither had yet.
#[derive(Clone)]
pub(crate) struct CacheStores {
    engine: CacheEngine,
}

impl CacheStores {
    /// Stores over `engine`.
    pub(crate) const fn new(engine: CacheEngine) -> Self {
        Self { engine }
    }
}

impl StreamStores for CacheStores {
    type Store = CacheStore;

    async fn open(&self, hash: [u8; 32], total_bytes: u64) -> anyhow::Result<CacheStore> {
        Ok(CacheStore::new(
            self.engine.clone(),
            Hash::from_bytes(hash),
            total_bytes,
        ))
    }
}

#[cfg(test)]
mod tests;
