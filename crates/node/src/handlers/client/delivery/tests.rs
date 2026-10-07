use super::{BaoExportStream, ChunkFramer, Hash};
use bytes::Bytes;
use decdn_cache::CacheError;

/// Build an export stream from per-item payloads.
fn stream_of(items: Vec<Vec<u8>>) -> BaoExportStream {
    Box::pin(futures_util::stream::iter(
        items.into_iter().map(|b| Ok(Bytes::from(b))),
    ))
}

/// At any target, the framer must cut exactly what `slice::chunks(target)` cuts
/// over the concatenated export: same byte sequence, same boundaries, full frames
/// until the remainder. The client's cumulative wire-byte accounting is defined
/// over the frame sequence, so this equivalence is the safety argument for the
/// framer existing at all (#1132) — and it must hold for every target, because
/// the serve loop varies the target per call to track the credit window.
#[tokio::test]
async fn framing_matches_slice_chunks_at_every_target() -> anyhow::Result<()> {
    // Deliberately awkward item sizes: a 64-byte proof pair, a full chunk group,
    // and remainders that straddle frame boundaries at every target below.
    let items = vec![
        vec![1u8; 64],
        vec![2u8; 16 * 1024],
        vec![3u8; 64],
        vec![4u8; 1000],
        vec![5u8; 1],
    ];
    let flat: Vec<u8> = items.iter().flatten().copied().collect();

    // A target below, at, and above one export item, plus one that exceeds the
    // whole export (so the framer must still flush a single short remainder).
    for target in [1usize, 64, 1000, 1024, 16 * 1024, 64 * 1024, 1024 * 1024] {
        let mut framer = ChunkFramer::new(stream_of(items.clone()), Hash::new(b"framing-test"));
        let mut got: Vec<Bytes> = Vec::new();
        while let Some(frame) = framer.next_frame(target).await? {
            got.push(frame);
        }

        let want: Vec<&[u8]> = flat.chunks(target).collect();
        anyhow::ensure!(
            got.len() == want.len(),
            "target {target}: framed {} chunks, slice::chunks yields {}",
            got.len(),
            want.len()
        );
        for (i, (actual, expected)) in got.iter().zip(want.iter()).enumerate() {
            anyhow::ensure!(
                actual.as_ref() == *expected,
                "target {target}: frame {i} differs"
            );
        }
        // Restated as an invariant rather than inferred from the comparison: no
        // frame may be empty (#1088) or exceed what the caller asked for.
        for frame in &got {
            anyhow::ensure!(
                !frame.is_empty(),
                "target {target}: framer emitted an empty frame"
            );
            anyhow::ensure!(
                frame.len() <= target,
                "target {target}: framer emitted an oversized frame"
            );
        }
    }
    Ok(())
}

/// An empty export (the 0-byte blob, #1054) must yield no frames at all, so
/// the deliver phase makes no pass and the serve goes straight to `StreamEnd`.
#[tokio::test]
async fn an_empty_export_yields_no_frames() -> anyhow::Result<()> {
    let mut framer = ChunkFramer::new(stream_of(Vec::new()), Hash::new(b"empty-test"));
    anyhow::ensure!(
        framer.next_frame(1024).await?.is_none(),
        "an empty export must yield no frames"
    );
    Ok(())
}

/// A zero target must be refused by name. The serve loop reads `None` as "the
/// blob is fully delivered", so on an empty queue a zero target would send
/// `StreamEnd` over a truncation and let the client pay the closing voucher for
/// it; on a non-empty queue the cut refuses anyway, but blames a `queued`/`queue`
/// desync for what is a bad argument. `frame_target` never returns zero, so this
/// pins the floor restated where the failure would otherwise be silent or
/// misattributed.
#[tokio::test]
async fn a_zero_frame_target_is_refused_not_read_as_end_of_blob() -> anyhow::Result<()> {
    let mut framer = ChunkFramer::new(
        stream_of(vec![vec![9u8; 4096]]),
        Hash::new(b"zero-target-test"),
    );
    let err = match framer.next_frame_chunks(0).await {
        Ok(Some(_)) => anyhow::bail!("a zero target must not cut a frame"),
        Ok(None) => anyhow::bail!("a zero target must not read as end-of-blob"),
        Err(e) => e,
    };
    anyhow::ensure!(
        err.to_string().contains("zero-length frame"),
        "unexpected error: {err}"
    );
    // The bytes are still there: the refusal is about the target, not the export.
    let frame = framer.next_frame(4096).await?;
    anyhow::ensure!(frame.is_some(), "the export survives a refused target");
    Ok(())
}

/// A frame wider than one export item must arrive as several `Bytes`, not as one
/// coalesced buffer. That is the whole zero-copy claim, and nothing else in this
/// module observes it: every other assertion here compares byte sequences, which a
/// framer that concatenated would satisfy exactly as well.
#[tokio::test]
async fn a_frame_spanning_several_export_items_rides_uncopied() -> anyhow::Result<()> {
    // Four 64-byte proof-node-sized items; one 200-byte frame spans three of them
    // and splits the fourth.
    let items = vec![vec![1u8; 64], vec![2u8; 64], vec![3u8; 64], vec![4u8; 64]];
    let mut framer = ChunkFramer::new(stream_of(items), Hash::new(b"spanning-test"));

    let frame = framer
        .next_frame_chunks(200)
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected a frame"))?;
    let total = frame.total();
    anyhow::ensure!(total == 200, "expected a full 200-byte frame, got {total}");
    let chunks = frame.chunks();
    anyhow::ensure!(
        chunks.len() == 4,
        "a frame over four export items must stay four slices, got {}",
        chunks.len()
    );
    anyhow::ensure!(
        chunks.iter().map(Bytes::len).collect::<Vec<_>>() == vec![64, 64, 64, 8],
        "only the boundary item is split"
    );
    Ok(())
}

/// A mid-export fault — including the truncation refusal, which the streaming
/// export can only report after its last item — must propagate out of
/// `next_frame_chunks` rather than being swallowed into a short-but-clean
/// delivery. That is what makes the caller abort without `StreamEnd`, so the
/// client rejects the delivery and never pays the closing voucher.
///
/// The fault is also terminal: the queue is dropped and every later call errors
/// rather than answering `None`, which the serve loop would read as a clean end
/// of blob. The coherent twin pins the same rule in
/// `a_coherent_encode_fault_is_terminal`.
#[tokio::test]
async fn a_mid_export_fault_propagates() -> anyhow::Result<()> {
    let stream: BaoExportStream = Box::pin(futures_util::stream::iter(vec![
        // 1500, deliberately NOT a multiple of the 1024 target used below: one
        // full frame is cuttable, leaving 476 bytes queued when the fault
        // lands. With a multiple the queue would already be empty at that point
        // and the fault's `queue.clear()` would be unobservable.
        Ok(Bytes::from(vec![7u8; 1500])),
        Err(CacheError::Store(anyhow::anyhow!(
            "export_bao stream for deadbeef ended without Done; refusing truncated export"
        ))),
    ]));
    let mut framer = ChunkFramer::new(stream, Hash::new(b"fault-test"));

    // The first full frame is already cuttable from the buffered bytes.
    let first = framer.next_frame(1024).await?;
    anyhow::ensure!(first.is_some(), "expected a frame before the fault");

    // Draining toward the next frame reaches the error.
    let err = loop {
        match framer.next_frame(1024).await {
            Ok(Some(_)) => {}
            Ok(None) => anyhow::bail!("framer ended cleanly; the export fault was swallowed"),
            Err(e) => break e,
        }
    };
    anyhow::ensure!(
        format!("{err:#}").contains("refusing truncated export"),
        "fault lost its cause: {err:#}"
    );

    // The framer must now be POISONED. Without it, the 476 unverified bytes
    // still buffered when the export faulted would be cut into a frame and
    // put on the wire as if they were good — and the client billed for them.
    // `Ok(None)` would be just as wrong: it reads as a clean end of blob.
    let after = framer.next_frame(1024).await;
    let err = match after {
        Err(e) => e,
        Ok(o) => anyhow::bail!(
            "a faulted framer must refuse further frames, got {:?}",
            o.map(|b| b.len())
        ),
    };
    anyhow::ensure!(
        err.to_string().contains("already faulted"),
        "the second call must refuse by name, not re-report the export fault: {err}"
    );
    anyhow::ensure!(
        framer.queue.is_empty(),
        "a fault clears the queue and drops the queued bytes"
    );
    Ok(())
}
