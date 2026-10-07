use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::error::OriginPullError;
use crate::origin::{OriginFetch, OutboardFetch};
use crate::range_pull::align_range;

#[test]
fn window_spans_tile_the_range_in_bounded_steps() {
    let w = RANGE_PULL_WINDOW_BYTES;
    let blob = 3 * w + 20_000;
    let aligned = align_range(16 * 1024, 0, blob).unwrap();
    let spans: Vec<_> = WindowSpans::new(&aligned).collect();
    assert_eq!(spans.first().unwrap().0, aligned.fetch_start());
    assert_eq!(spans.last().unwrap().1, aligned.fetch_end());
    for pair in spans.windows(2) {
        assert_eq!(pair[0].1, pair[1].0, "spans must be contiguous");
    }
    assert!(spans.iter().all(|(s, e)| e > s && e - s <= w));
    assert_eq!(spans.len(), 4);
}

#[test]
fn window_spans_of_the_empty_blob_is_one_empty_span() {
    let aligned = align_range(0, 0, 0).unwrap();
    assert_eq!(WindowSpans::new(&aligned).collect::<Vec<_>>(), vec![(0, 0)]);
}

/// Serves `data` by range and an empty outboard, counting data reads.
#[derive(Debug)]
struct MemOrigin {
    data: Bytes,
    reads: AtomicUsize,
}

impl Origin for MemOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(OriginFetch::NotFound) })
    }

    fn fetch_outboard(
        &self,
        _hash: Hash,
        _outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(OutboardFetch::Found(Bytes::new())) })
    }

    fn fetch_range_data(
        &self,
        _hash: Hash,
        req: OriginRangeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let s = usize::try_from(req.fetch_start).unwrap();
        let e = usize::try_from(req.fetch_end).unwrap();
        let data = self.data.slice(s..e);
        Box::pin(async move { Ok(OriginRangeFetch::Ranged { data }) })
    }
}

/// The reader serves a read inside one window, a read that straddles two
/// windows, and a read clamped at the span end — fetching each window once —
/// and refuses a backward read as a local fault.
#[tokio::test]
async fn window_reader_reads_across_windows_and_fetches_each_once() {
    let w = usize::try_from(RANGE_PULL_WINDOW_BYTES).unwrap();
    let blob: Vec<u8> = (0..2 * w + 100).map(|i| (i % 251) as u8).collect();
    let origin = Arc::new(MemOrigin {
        data: Bytes::from(blob.clone()),
        reads: AtomicUsize::new(0),
    });
    let hash = Hash::new(&blob);
    let aligned = align_range(0, 0, u64::try_from(blob.len()).unwrap()).unwrap();
    let cursor = OriginRangeCursor::open(
        Arc::clone(&origin) as Arc<dyn Origin>,
        hash,
        &aligned,
        Bytes::new(),
        None,
        None,
    )
    .await
    .unwrap()
    .unwrap();
    let fault: FaultSlot = Arc::new(Mutex::new(None));
    let mut reader = OriginWindowReader::new(cursor, &aligned, Arc::clone(&fault));

    assert_eq!(reader.read_at(10, 20).await.unwrap(), &blob[10..30]);
    let w64 = RANGE_PULL_WINDOW_BYTES;
    assert_eq!(
        reader.read_at(w64 - 10, 20).await.unwrap(),
        &blob[w - 10..w + 10],
        "a read that straddles two windows is byte-exact",
    );
    assert_eq!(
        reader.read_at(2 * w64 + 90, 100).await.unwrap(),
        &blob[2 * w + 90..],
        "a read past the span end is clamped",
    );
    assert_eq!(
        origin.reads.load(Ordering::SeqCst),
        3,
        "one read per window"
    );

    assert!(reader.read_at(0, 1).await.is_err(), "a backward read fails");
    assert!(
        matches!(*fault.lock().unwrap(), Some(CacheError::Internal(_))),
        "a backward read is a local fault, not an origin fault",
    );
}
