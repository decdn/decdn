use std::time::Duration;

use decdn_bao_range::align_range;
use decdn_client::{PrimedSource, ScriptedSource};
use iroh_io::AsyncStreamReader as _;

use super::*;

const PULL_TTFB: &str = "decdn_node_pull_first_byte_seconds";

fn has_line(metrics: &Metrics, line: &str) -> bool {
    metrics.encode().unwrap().lines().any(|l| l == line)
}

fn count_is(metrics: &Metrics, n: u64) -> bool {
    has_line(metrics, &format!("{PULL_TTFB}_count {n}"))
}

/// Read `reader` to its end.
async fn drain(reader: &mut impl iroh_io::AsyncStreamReader) {
    while !reader.read_bytes(16 * 1024).await.unwrap().is_empty() {}
}

#[tokio::test]
async fn a_leg_records_once_on_its_first_bytes() {
    let metrics = Arc::new(Metrics::new());
    let inner = ScriptedSource::new(vec![7u8; 40 * 1024]).unwrap();
    let root = inner.root();
    let range = align_range(0, 0, 40 * 1024).unwrap();
    let source = TimedSource::new(inner, Arc::clone(&metrics));

    let (_, mut reader) = source.open(root, range.clone()).await.unwrap();
    assert!(count_is(&metrics, 0), "the open alone records nothing");
    drain(&mut reader).await;
    assert!(count_is(&metrics, 1), "many reads, one leg, one record");
    source.finish(reader).await.unwrap();

    // A second leg is a second request, so it records again.
    let (_, mut reader) = source.open(root, range).await.unwrap();
    drain(&mut reader).await;
    assert!(count_is(&metrics, 2));
}

#[tokio::test]
async fn a_leg_that_reads_nothing_records_nothing() {
    let metrics = Arc::new(Metrics::new());
    let inner = ScriptedSource::new(vec![7u8; 4 * 1024]).unwrap();
    let root = inner.root();
    let range = align_range(0, 0, 4 * 1024).unwrap();
    let source = TimedSource::new(inner, Arc::clone(&metrics));

    let (_, reader) = source.open(root, range).await.unwrap();
    drop(reader);
    assert!(count_is(&metrics, 0));
}

/// The fixed-size read the bao decoder also uses records the first bytes.
#[tokio::test]
async fn a_fixed_size_read_records_the_first_bytes() {
    let metrics = Arc::new(Metrics::new());
    let inner = ScriptedSource::new(vec![7u8; 4 * 1024]).unwrap();
    let root = inner.root();
    let range = align_range(0, 0, 4 * 1024).unwrap();
    let source = TimedSource::new(inner, Arc::clone(&metrics));

    let (_, mut reader) = source.open(root, range).await.unwrap();
    reader.read::<8>().await.unwrap();
    assert!(count_is(&metrics, 1));
}

/// An adopted handshake pull keeps the start of its handshake open: the
/// adopting `open` returns at once, and the window must not restart there.
#[tokio::test]
async fn an_adopted_pull_keeps_its_handshake_open_time() {
    let metrics = Arc::new(Metrics::new());
    let inner = ScriptedSource::new(vec![7u8; 4 * 1024]).unwrap();
    let root = inner.root();
    let range = align_range(0, 0, 4 * 1024).unwrap();

    // A handshake that takes 300 ms before its header arrives.
    let slow_handshake = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        inner.open(root, range.clone()).await
    };
    let (header, handshake) = timed_open(slow_handshake, Arc::clone(&metrics))
        .await
        .unwrap();

    let source = PrimedSource::new(TimedSource::new(inner, Arc::clone(&metrics)));
    source.prime(
        root,
        range.clone(),
        header,
        handshake,
        tokio::time::Instant::now(),
    );
    let (_, mut reader) = source.open(root, range).await.unwrap();
    drain(&mut reader).await;

    assert!(count_is(&metrics, 1));
    assert!(
        has_line(&metrics, &format!("{PULL_TTFB}_bucket{{le=\"0.25\"}} 0")),
        "the adopted leg timed from its adoption, not from its handshake"
    );
}
