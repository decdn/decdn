use super::{BlobSource, Funder};
use crate::sink::StashedFault;
use alloy::primitives::U256;
use decdn_bao_range::{AlignedRange, align_range};
use decdn_incentive::DepositOutcome;
use iroh_io::AsyncStreamReader;

fn blob(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// The scripted source yields wire that the store's decoder can verify, and
/// reports the blob's true `total_bytes`.
#[tokio::test]
async fn scripted_source_opens_a_verifiable_range() -> anyhow::Result<()> {
    let data = blob(200 * 1024 + 7);
    let source = super::ScriptedSource::new(data.clone())?;
    let range = align_range(0, 0, data.len() as u64)?;
    let (header, mut reader) = source.open(source.root(), range).await?;
    assert_eq!(header.total_bytes, data.len() as u64);
    let first = reader.read_bytes(64).await?;
    assert!(!first.is_empty(), "a non-empty blob must yield wire bytes");
    assert!(
        reader.take_fault().is_none(),
        "no fault was scripted, so none should be parked"
    );
    Ok(())
}

/// A scripted mid-range fault is parked on the reader for the decode loop to
/// surface, exactly as a real stalled peer would.
#[tokio::test]
async fn scripted_source_parks_a_mid_range_fault() -> anyhow::Result<()> {
    let data = blob(200 * 1024 + 7);
    let source = super::ScriptedSource::new(data.clone())?
        .with_fault_after(4096, || anyhow::anyhow!("scripted stall"));
    let range = align_range(0, 0, data.len() as u64)?;
    assert!(
        drain(&source, &range).await.is_err(),
        "the scripted fault must be parked once the truncated wire is drained"
    );
    Ok(())
}

/// Open `range` on `src` and read its wire to the end. Returns the wire
/// bytes read, or the fault the reader parked.
async fn drain(src: &super::ScriptedSource, range: &AlignedRange) -> anyhow::Result<u64> {
    let (_header, mut reader) = src.open(src.root(), range.clone()).await?;
    let mut read = 0u64;
    loop {
        let chunk = reader.read_bytes(64 * 1024).await?;
        if chunk.is_empty() {
            break;
        }
        read += chunk.len() as u64;
    }
    reader.take_fault().map_or(Ok(read), Err)
}

#[tokio::test]
async fn fault_once_after_fires_on_the_first_reader_only() -> anyhow::Result<()> {
    let src = super::ScriptedSource::new(vec![3u8; 64 * 1024])?
        .fault_once_after(4096, || anyhow::anyhow!("scripted reset"));
    let range = align_range(0, 64 * 1024, 64 * 1024)?;
    let first = drain(&src, &range).await;
    assert!(first.is_err(), "the first reader faults");
    let second = drain(&src, &range).await;
    assert!(second.is_ok(), "the second reader delivers");
    Ok(())
}

/// `PeerSource` implements `BlobSource` — checked at compile time rather than
/// exercised end-to-end, since a real run needs a live `Endpoint`/connection.
/// The driver tests exercise the trait's behavior against `ScriptedSource`;
/// the loopback suites (`open_progressive_pull` under the `stream_fetch*`
/// wrappers, the `byte_len` plumbing above) cover `PeerSource`'s own
/// building blocks. `'static` is just a concrete lifetime to instantiate the
/// generic type parameter with — no value is constructed.
#[test]
fn peer_source_is_a_blob_source() {
    fn assert_impl<T: BlobSource>() {}
    assert_impl::<super::PeerSource<'static>>();
}

/// The fake funder records amounts and echoes its scripted outcome.
#[tokio::test]
async fn fake_funder_records_and_returns() -> anyhow::Result<()> {
    let funder = super::FakeFunder::new(3, DepositOutcome::Added(U256::from(100u64)));
    assert_eq!(funder.max_topups(), 3);
    let out = funder.top_up(U256::from(40u64)).await?;
    assert_eq!(out, DepositOutcome::Added(U256::from(100u64)));
    assert_eq!(funder.calls(), vec![U256::from(40u64)]);
    Ok(())
}
