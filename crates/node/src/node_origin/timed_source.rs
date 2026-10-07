//! `TimedSource` — a [`BlobSource`] wrapper that records each paid pull leg's
//! time to first byte into `decdn_node_pull_first_byte_seconds`.
//!
//! The clock of a leg starts before the wrapped source opens its pull, so the
//! window includes a dial when the node has no warm connection to the peer. The
//! clock stops on the first read that returns bao bytes. A leg that reads no
//! bytes records nothing.
//!
//! A drive can adopt a pull that a header handshake opened before the drive
//! started (see [`decdn_client::PrimedSource`]). The adopting `open` returns at
//! once, so a clock started there reads only the time since adoption. The
//! handshake site therefore opens its pull through [`timed_open`], which starts
//! the clock before the handshake. `PrimedSource<TimedSource<_>>` accepts only a
//! [`TimedReader`], and [`timed_open`] is the only way to build one, so every
//! handshake site times from before its own open.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use decdn_bao_range::AlignedRange;
use decdn_client::sink::StashedFault;
use decdn_client::source::SourceFuture;
use decdn_client::{BlobSource, UpstreamPullHeader, VoucherProgress};

use crate::metrics::Metrics;

/// Drive `open` to its header and wrap its reader in a [`TimedReader`] whose
/// clock starts before `open` is first polled.
///
/// An open future does no work until it is polled, so the clock starts before
/// the dial and the request.
///
/// # Errors
///
/// The error of `open`, unchanged.
pub(crate) async fn timed_open<R>(
    open: impl Future<Output = anyhow::Result<(UpstreamPullHeader, R)>>,
    metrics: Arc<Metrics>,
) -> anyhow::Result<(UpstreamPullHeader, TimedReader<R>)> {
    let opened_at = Instant::now();
    let (header, reader) = open.await?;
    Ok((header, TimedReader::new(reader, opened_at, metrics)))
}

/// A [`BlobSource`] that wraps each reader of `S` in a [`TimedReader`] whose
/// clock starts before `S` opens the pull.
pub(crate) struct TimedSource<S> {
    inner: S,
    metrics: Arc<Metrics>,
}

impl<S> TimedSource<S> {
    /// Wrap `inner`. The legs it opens record into `metrics`.
    pub(crate) const fn new(inner: S, metrics: Arc<Metrics>) -> Self {
        Self { inner, metrics }
    }
}

impl<S: BlobSource> BlobSource for TimedSource<S> {
    type Reader = TimedReader<S::Reader>;

    fn open(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
    ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
        Box::pin(timed_open(
            self.inner.open(hash, range),
            Arc::clone(&self.metrics),
        ))
    }

    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        self.inner.finish(reader.inner)
    }

    fn stop(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        self.inner.stop(reader.inner)
    }

    fn max_blob_size_bytes(&self) -> u64 {
        self.inner.max_blob_size_bytes()
    }
}

/// A pull-leg reader that records the time from the start of its open to its
/// first bao bytes, once. Built only by [`timed_open`].
pub(crate) struct TimedReader<R> {
    inner: R,
    /// The instant the open started, until the first bytes consume it.
    opened_at: Option<Instant>,
    metrics: Arc<Metrics>,
}

impl<R> TimedReader<R> {
    /// Wrap `inner`, a reader whose open started at `opened_at`.
    const fn new(inner: R, opened_at: Instant, metrics: Arc<Metrics>) -> Self {
        Self {
            inner,
            opened_at: Some(opened_at),
            metrics,
        }
    }

    /// Record the first byte, if this leg has not recorded yet.
    fn first_bytes(&mut self) {
        if let Some(opened_at) = self.opened_at.take() {
            self.metrics.node_pull_first_byte(opened_at.elapsed());
        }
    }
}

impl<R: iroh_io::AsyncStreamReader> iroh_io::AsyncStreamReader for TimedReader<R> {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        let bytes = self.inner.read_bytes(len).await?;
        if !bytes.is_empty() {
            self.first_bytes();
        }
        Ok(bytes)
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        let bytes = self.inner.read::<L>().await?;
        if L > 0 {
            self.first_bytes();
        }
        Ok(bytes)
    }
}

impl<R: StashedFault> StashedFault for TimedReader<R> {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        self.inner.take_fault()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
