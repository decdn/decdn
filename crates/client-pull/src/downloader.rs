//! The `Downloader` consumption face (#1848 T4): fetch a set of content-addressed
//! blobs — a bundle, or a single blob — to files in a directory.
//!
//! A `Downloader` is an OUTPUT + SCHEDULING adapter over the one paid pull engine,
//! not a second engine: each entry opens a [`ClientRangedStore`] beside its file
//! and drives it through [`crate::driver::drive`], which pulls ONLY the missing
//! ranges, bao-verifies every byte on ingest, and promotes the `.partial` to the
//! final file on completion. So a resumed download re-pulls only what it lacks,
//! and a bundle layer above can pre-seed held ranges (its chunk-hint dedup) into
//! the store and the same `drive` fills only the complement.
//!
//! Bundles are the main scenario: a bundle is just a set of `(hash, total_bytes)`
//! entries, so `fetch_to_dir` takes a slice of them and writes one file per entry,
//! named by its content-address hex. A single blob is a bundle of one. The
//! `Streamer` (a fetch-like, consumption-paced single-blob face) is the other
//! consumer of the same engine.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::driver::{DriveConfig, drive};
use crate::pacer::Pacer;
use crate::source::{BlobSource, Funder};
use crate::{ClientRangedStore, PoolContext, PoolLedger, PullConfig};

/// Fetch content-addressed blobs to files in a directory, reusing the paid pull
/// engine ([`crate::driver::drive`] + [`ClientRangedStore`]).
///
/// The pull machinery is INJECTED and shared across every entry: one `source`
/// (e.g. a [`crate::source::PeerSource`] over a reused
/// [`crate::WarmConnection`]) serves every hash — [`BlobSource::open`] takes the
/// hash, so one provider connection fetches a whole bundle — and one
/// `ctx`/`ledger` pays for all of them out of a single pool. That is what keeps a
/// bundle download to one dial and one payment channel.
pub struct Downloader<'a, S, P, F> {
    /// The raw-bao byte source every entry is pulled from.
    source: &'a S,
    /// The pacing axis `drive` consults (a [`crate::pacer::BudgetPacer`] for a
    /// full-throughput download).
    pacer: &'a P,
    /// The top-up seam `drive` funds a mid-fetch cap exhaustion through.
    funder: &'a F,
    /// The buyer context, shared (interior-mutable) with the source and driver so
    /// a mid-fetch top-up's new deposit is visible to the next entry's opens.
    ctx: &'a Arc<Mutex<PoolContext>>,
    /// The channel's voucher ledger, shared across every entry on this pool.
    ledger: &'a Arc<PoolLedger>,
    /// The driver's funding/settle policy.
    drive_config: DriveConfig,
}

impl<S, P, F> std::fmt::Debug for Downloader<'_, S, P, F> {
    /// Prints only the non-generic policy; the injected `source`/`pacer`/`funder`
    /// are behaviour, not data, and `ctx` holds a signing key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Downloader")
            .field("drive_config", &self.drive_config)
            .finish_non_exhaustive()
    }
}

impl<'a, S, P, F> Downloader<'a, S, P, F> {
    /// Build a downloader over the injected pull machinery. The same `source`,
    /// `ctx`, and `ledger` serve every entry a later `fetch_to_dir` fetches.
    #[must_use]
    pub const fn new(
        source: &'a S,
        pacer: &'a P,
        funder: &'a F,
        ctx: &'a Arc<Mutex<PoolContext>>,
        ledger: &'a Arc<PoolLedger>,
        drive_config: DriveConfig,
    ) -> Self {
        Self {
            source,
            pacer,
            funder,
            ctx,
            ledger,
            drive_config,
        }
    }
}

impl<S, P, F> Downloader<'_, S, P, F>
where
    S: BlobSource,
    P: Pacer,
    F: Funder,
{
    /// Fetch every `(hash, total_bytes)` entry to a file named by its
    /// content-address hex inside `dir`, returning the written paths in entry
    /// order.
    ///
    /// Each entry opens a [`ClientRangedStore`] beside its file and drives only
    /// its missing ranges through [`crate::driver::drive`], which bao-verifies
    /// every byte and promotes `.partial` to the final file on completion. Writes
    /// land at their absolute offsets, so an out-of-order or multi-source fill
    /// assembles correctly; a resumed download re-pulls only what it lacks. The
    /// whole blob is fetched at full throughput — `config`'s read-ahead bound is a
    /// `Streamer` tunable and does not apply here.
    ///
    /// `total_bytes` is authoritative for keying the store, and the entry `hash`
    /// is the bao root every ingested byte is verified against: a wrong size or
    /// hash surfaces as a verification failure, never as silent corruption.
    ///
    /// # Errors
    ///
    /// A store open/create I/O error, or any fault [`crate::driver::drive`] raises
    /// for an entry (a refused or underfunded pull, a stalled peer, a
    /// verification failure). The first failing entry aborts the batch; entries
    /// already written stay on disk (and any `.partial` a failed entry left is the
    /// resume prefix a retry inherits).
    pub async fn fetch_to_dir(
        &self,
        entries: &[([u8; 32], u64)],
        dir: &Path,
        // The Downloader fetches at full throughput; no `PullConfig` tunable
        // changes that yet. Taken for API symmetry with the `Streamer` and so a
        // future per-download tunable has a home.
        config: &PullConfig,
    ) -> anyhow::Result<Vec<PathBuf>> {
        let _ = config;
        std::fs::create_dir_all(dir)
            .map_err(|e| anyhow::anyhow!("create download dir {}: {e}", dir.display()))?;
        let mut written = Vec::with_capacity(entries.len());
        for &(hash, total_bytes) in entries {
            let stem = blake3::Hash::from_bytes(hash).to_hex();
            let store = ClientRangedStore::open_or_create(dir, stem.as_str(), hash, total_bytes)
                .map_err(|e| anyhow::anyhow!("open ranged store for {stem}: {e}"))?;
            drive(
                &store,
                self.source,
                self.pacer,
                self.funder,
                self.ctx,
                self.ledger,
                hash,
                0,
                0,
                &self.drive_config,
                None,
                // A `BudgetPacer` never returns `Wait`, and a full-throughput
                // download has no downstream leg, so neither hook is needed.
                None,
                None,
                // One lane IS the pool for a solo download; no pool-wide view.
                None,
            )
            .await?;
            written.push(dir.join(stem.as_str()));
        }
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use alloy::primitives::{Address, B256, U256};
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::DepositOutcome;

    use super::Downloader;
    use crate::driver::DriveConfig;
    use crate::pacer::BudgetPacer;
    use crate::source::{FakeFunder, ScriptedSource};
    use crate::{Cumulative, PoolContext, PoolLedger, PullConfig};

    /// A buyer context with a huge deposit so funding never gates the fetch.
    fn healthy_ctx() -> PoolContext {
        PoolContext {
            pool_id: B256::ZERO,
            provider: Address::repeat_byte(0xAB),
            deposit: U256::from(u128::MAX),
            client_signer: Arc::new(PrivateKeySigner::random()),
            voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        }
    }

    fn drive_config() -> DriveConfig {
        DriveConfig {
            working_deposit: U256::from(u128::MAX),
            seller_reserve: U256::ZERO,
            max_settle_waits: 2,
            settle_backoff: Duration::ZERO,
        }
    }

    /// A deterministic payload spanning several chunk groups and one voucher
    /// interval boundary.
    fn payload(len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        let mut x: u32 = 0x1234_5678;
        for b in &mut out {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes()[0];
        }
        out
    }

    /// `fetch_to_dir` writes each blob to `dir/<hex>` and the file is
    /// BLAKE3-identical to the source blob (the whole-blob bao root IS its BLAKE3
    /// hash, so the promoted file's hash must equal the entry hash).
    #[tokio::test]
    async fn fetch_to_dir_writes_a_blake3_identical_file() -> anyhow::Result<()> {
        let blob = payload(1_500_000);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
        let root = source.root();
        let total = u64::try_from(blob.len())?;

        let pacer = BudgetPacer::new();
        let funder = FakeFunder::new(3, DepositOutcome::Added(U256::from(u128::MAX)));
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let cfg = PullConfig::default();

        let downloader = Downloader::new(&source, &pacer, &funder, &ctx, &ledger, drive_config());
        let dir = tempfile::tempdir()?;
        let paths = downloader
            .fetch_to_dir(&[(root, total)], dir.path(), &cfg)
            .await?;

        anyhow::ensure!(
            paths.len() == 1,
            "one entry → one file, got {}",
            paths.len()
        );
        let path = paths
            .first()
            .ok_or_else(|| anyhow::anyhow!("no path returned"))?;
        let got = std::fs::read(path)?;
        anyhow::ensure!(
            got == blob,
            "written file must be byte-identical to the blob"
        );
        anyhow::ensure!(
            blake3::hash(&got).as_bytes() == &root,
            "written file must be BLAKE3-identical to the entry hash"
        );
        // The file is named by its content address.
        anyhow::ensure!(
            path.file_name().and_then(|n| n.to_str())
                == Some(blake3::Hash::from_bytes(root).to_hex().as_str()),
            "file is named by its content-address hex"
        );
        Ok(())
    }
}
