use bytes::Bytes;

use super::{
    ClientPaymentFault, FrameError, FrameQueue, MissRefusal, PaidProgress, PeerFault,
    ServeRejectReason, StreamRequest, WriteError, chunk_frame_bufs, is_peer_attributable,
    log_miss_refusal, tag_paid_progress, tolerate_departed_peer, write_chunk_error,
    write_frame_error,
};
use crate::handlers::client::{CoverageShortfall, MissPath};
use crate::node_origin::PullMiss;

#[derive(Clone, Default)]
struct CapturedLog(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A coverage shortfall with every detail field set.
const SHORTFALL: CoverageShortfall = CoverageShortfall {
    pull_offset: 32_768,
    pull_len: 65_536,
    candidates: 2,
    first_uncovered_chunk: Some(48),
};

/// A miss refusal is `InternalError` exactly when a tier faulted, whatever
/// the path, and every path logs under its own stable name (#2282).
#[test]
fn a_miss_refusal_carries_the_fault_reason_and_a_stable_path_name() {
    let paths = [
        (MissPath::CoverageGate(SHORTFALL), "coverage_gate"),
        (MissPath::PullLegMiss(PullMiss::Clean), "pull_leg_miss"),
        (MissPath::PullLegTimeout, "pull_leg_timeout"),
        (MissPath::BufferedMiss, "buffered_miss"),
        (MissPath::NoPullThrough, "no_pull_through"),
        (MissPath::NoLane, "no_lane"),
    ];
    for (path, name) in paths {
        assert_eq!(path.as_str(), name);
        assert_eq!(
            MissRefusal::new(false, path).reason,
            ServeRejectReason::CacheMiss
        );
        assert_eq!(
            MissRefusal::new(true, path).reason,
            ServeRejectReason::InternalError
        );
    }
}

/// The one line `log_miss_refusal` writes for `refusal`, captured at INFO.
fn refusal_line(refusal: MissRefusal, suppressed: Option<u64>) -> String {
    let log = CapturedLog::default();
    let sink = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    let req = StreamRequest {
        hash: [0xAB; 32],
        namespace_id: [0; 32],
        pool_id: [0xCD; 32],
        byte_offset: 16_384,
        byte_len: 82_251,
        timestamp_us: 0,
    };
    tracing::subscriber::with_default(subscriber, || {
        log_miss_refusal(&req, refusal, suppressed);
    });
    let text = String::from_utf8(
        log.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 1, "one line per refusal: {text}");
    lines[0].to_owned()
}

/// The refusal line is what an operator greps Loki for: it must name the
/// hash, the requested range, the span's `reason` value and the path, at
/// info. A pull-leg miss names its cause, so a buy-ceiling decline reads
/// apart from an absent blob (#2282).
#[test]
fn a_miss_refusal_logs_one_info_line_with_hash_range_reason_path_and_cause() {
    let line = refusal_line(
        MissRefusal::new(false, MissPath::PullLegMiss(PullMiss::BelowMargin)),
        None,
    );
    assert!(line.contains(" INFO "), "{line}");
    assert!(line.contains("serve-miss: refusing the request"), "{line}");
    assert!(
        line.contains(&format!("hash={}", "ab".repeat(32))),
        "{line}"
    );
    assert!(line.contains("byte_offset=16384"), "{line}");
    assert!(line.contains("byte_len=82251"), "{line}");
    assert!(line.contains("reason=\"cache_miss\""), "{line}");
    assert!(line.contains("path=\"pull_leg_miss\""), "{line}");
    assert!(line.contains("cause=\"below_margin\""), "{line}");
    for absent in ["pull_offset=", "candidates=", "suppressed="] {
        assert!(!line.contains(absent), "unexpected `{absent}`: {line}");
    }
}

/// The coverage-gate line carries the shortfall that tells one partial
/// holder apart from a gap every holder shares (#2195, #2282).
#[test]
fn a_coverage_gate_refusal_logs_its_shortfall() {
    let line = refusal_line(
        MissRefusal::new(false, MissPath::CoverageGate(SHORTFALL)),
        None,
    );
    assert!(line.contains("path=\"coverage_gate\""), "{line}");
    assert!(line.contains("pull_offset=32768"), "{line}");
    assert!(line.contains("pull_len=65536"), "{line}");
    assert!(line.contains("candidates=2"), "{line}");
    assert!(line.contains("first_uncovered_chunk=48"), "{line}");
    assert!(!line.contains("cause="), "{line}");
}

/// A throttled no-lane line counts the lines it stands for.
#[test]
fn a_throttled_no_lane_refusal_logs_its_suppressed_count() {
    let line = refusal_line(MissRefusal::new(false, MissPath::NoLane), Some(7));
    assert!(line.contains("path=\"no_lane\""), "{line}");
    assert!(line.contains("suppressed=7"), "{line}");
}

/// The whole classification rests on a marker staying recoverable under the
/// context layers callers stack on the way up to the dispatch sink. This file
/// re-wraps errors with `anyhow!("...: {e}")` in many places; the day one of
/// those lands on a marked error the marker is gone, and the only thing that
/// would notice is this test.
#[test]
fn a_marker_survives_the_context_layers_stacked_above_it() {
    let e = anyhow::Error::new(PeerFault)
        .context("write failed: connection lost")
        .context("serve leg gave up");
    assert!(is_peer_attributable(&e), "marker lost under context");
    // The alternate form is what the sink logs, so the cause must still read.
    let rendered = format!("{e:#}");
    assert!(
        rendered.contains("connection lost"),
        "cause dropped from the log line: {rendered}"
    );
}

/// An unmarked error is a node fault. Nothing else may reach `debug!`.
#[test]
fn an_unmarked_error_is_a_node_fault() {
    assert!(!is_peer_attributable(&anyhow::anyhow!("store read failed")));
    assert!(is_peer_attributable(
        &anyhow::Error::new(ClientPaymentFault).context("voucher fails rate check")
    ));
}

/// A frame this node encoded too large never reached the wire, so it is this
/// node's bug — not the peer-disconnect shape the rest of `write_frame`'s
/// errors carry.
#[test]
fn an_oversized_frame_is_a_node_fault_but_a_write_io_error_is_the_peer() {
    let too_large = write_frame_error(FrameError::TooLarge(1 << 30));
    assert!(
        !is_peer_attributable(&too_large),
        "an oversized frame must reach error!: {too_large}"
    );

    let io = write_frame_error(FrameError::Io(std::io::Error::other("reset by peer")));
    assert!(is_peer_attributable(&io), "a write I/O error is the peer");
}

/// Writing to a stream this node already finished is a fault in this node's own
/// stream state machine; a peer that stopped the stream is not.
#[test]
fn a_write_after_close_is_a_node_fault_but_a_peer_stop_is_not() {
    let closed = write_chunk_error(WriteError::ClosedStream);
    assert!(
        !is_peer_attributable(&closed),
        "a write after our own finish must reach error!: {closed}"
    );

    let stopped = write_chunk_error(WriteError::Stopped(iroh::endpoint::VarInt::from_u32(0)));
    assert!(
        is_peer_attributable(&stopped),
        "a peer stop is the peer: {stopped}"
    );
}

/// The framed writes attribute a stream-state fault the way the vectored
/// `ChunkData` write does. The transport wraps the stream's `WriteError`
/// inside the `io::Error`, so a write after this node's own close stays a node
/// fault, while a peer stop or a lost connection is the peer.
#[test]
fn a_framed_write_after_close_is_a_node_fault_but_a_peer_stop_is_not() {
    let framed = |e: WriteError| write_frame_error(FrameError::Io(std::io::Error::from(e)));

    for own in [WriteError::ClosedStream, WriteError::ZeroRttRejected] {
        let err = framed(own);
        assert!(
            !is_peer_attributable(&err),
            "a framed write fault in this node's own stream state must reach error!: {err}"
        );
    }
    let stopped = framed(WriteError::Stopped(iroh::endpoint::VarInt::from_u32(0)));
    assert!(
        is_peer_attributable(&stopped),
        "a peer stop is the peer: {stopped}"
    );
    let lost = write_frame_error(FrameError::Io(std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        "connection lost",
    )));
    assert!(
        is_peer_attributable(&lost),
        "a transport error with no stream-state cause is the peer: {lost}"
    );
}

/// A refusal or stop whose frame the departed peer never reads keeps its
/// outcome; a write fault this node caused still fails the stream.
#[test]
fn a_departed_peer_does_not_fail_a_decided_end() {
    assert!(tolerate_departed_peer(Ok(()), "stop").is_ok());
    let gone = anyhow::Error::new(PeerFault).context("write failed");
    assert!(tolerate_departed_peer(Err(gone), "stop").is_ok());
    let own = write_frame_error(FrameError::Io(std::io::Error::from(
        WriteError::ClosedStream,
    )));
    let err = tolerate_departed_peer(Err(own), "stop").unwrap_err();
    assert!(
        !is_peer_attributable(&err),
        "a node fault still fails the stream"
    );
}

/// `PaidProgress` rides only an error from a stream that a voucher paid, keeps
/// the peer marker under it, and never turns a node fault peer-side.
#[test]
fn paid_progress_tags_only_a_paid_stream() {
    let unpaid = tag_paid_progress(anyhow::Error::new(PeerFault).context("gone"), 0);
    assert!(
        !unpaid.is::<PaidProgress>(),
        "an unpaid stream is not tagged"
    );

    let paid = tag_paid_progress(anyhow::Error::new(PeerFault).context("gone"), 50_176);
    assert!(paid.is::<PaidProgress>(), "a paid stream is tagged");
    assert_eq!(
        paid.downcast_ref::<PaidProgress>()
            .map(|p| p.wire_bytes.get()),
        Some(50_176),
        "the tag carries the credited wire bytes"
    );
    assert!(
        format!("{paid:#}").starts_with("after payment: "),
        "{paid:#}"
    );
    assert!(paid.is::<PeerFault>(), "the peer marker survives the tag");

    let node = tag_paid_progress(anyhow::anyhow!("store fault"), 1);
    assert!(
        !is_peer_attributable(&node),
        "a node fault after payment stays a node fault"
    );
}

/// The vectored buffers must lay down exactly the bytes the single-buffer
/// encoder would, whatever the payload is split into.
///
/// This is the claim the whole zero-copy path rests on: the serve loops build no
/// `ChunkData` frame at all, so nothing else proves the header they emit matches
/// `encode_chunk_frame` + `write_frame`. The chunk
/// counts span what a real frame looks like — one queued item, a handful, and
/// the ~130 a default 1 MiB frame spans over 64 B proof nodes and 16 KiB leaves.
#[tokio::test]
async fn chunk_frame_bufs_match_the_single_buffer_encoder() -> Result<(), Box<dyn std::error::Error>>
{
    for count in [1usize, 2, 4, 5, 130] {
        for chunk_len in [1usize, 64, 16 * 1024] {
            let chunks: Vec<Bytes> = (0..count)
                .map(|i| Bytes::from(vec![u8::try_from(i % 251).unwrap_or(0); chunk_len]))
                .collect();
            let total: usize = chunks.iter().map(Bytes::len).sum();

            // Build the frame the only way production does: push the items onto a
            // `FrameQueue`, then cut all of them into one `FrameChunks`.
            let mut fq = FrameQueue::new();
            for c in &chunks {
                fq.push(c.clone());
            }
            let frame = fq.cut(total).expect("a non-empty queue cuts a frame");

            let bufs = chunk_frame_bufs(&frame)?;
            let mut via_bufs = Vec::new();
            for b in &bufs {
                via_bufs.extend_from_slice(b);
            }

            let mut concat = Vec::with_capacity(total);
            for c in &chunks {
                concat.extend_from_slice(c);
            }
            let postcard = decdn_protocol::encode_chunk_frame(&concat)?;
            let mut via_encoder = Vec::new();
            decdn_protocol::write_frame(&mut via_encoder, &postcard).await?;

            assert_eq!(
                via_bufs, via_encoder,
                "diverged at {count} chunks of {chunk_len} bytes"
            );
            assert_eq!(
                bufs.len(),
                count + 1,
                "the payload must ride uncopied: one buf per chunk, plus the header"
            );
        }
    }
    Ok(())
}

/// `push` drops an empty item so the byte count never desyncs from the queue.
/// This is what makes `is_empty()` (count `== 0`) and an empty `queue` the same
/// statement, which is what lets `cut` treat `None` as the end of the blob rather
/// than a bookkeeping fault.
#[test]
fn push_drops_empties_so_the_count_never_desyncs() {
    let mut fq = FrameQueue::new();
    assert!(fq.is_empty());
    assert_eq!(fq.len(), 0);

    fq.push(Bytes::new());
    assert!(fq.is_empty(), "an empty item leaves the queue empty");
    assert_eq!(fq.len(), 0, "and leaves the count at zero");

    fq.push(Bytes::from_static(b"abcd"));
    fq.push(Bytes::new());
    assert_eq!(fq.len(), 4, "the empty push between real ones is a no-op");
    assert!(!fq.is_empty());
}

/// Cutting a frame moves whole items and splits only the one the frame ends in,
/// leaving `len()` equal to the bytes still queued.
///
/// The chunk COUNTS are the load-bearing assertions. `total` is computed from the
/// same lengths a sum over the result would re-add, so checking one against the
/// other proves nothing; what the zero-copy path actually rests on is that a frame
/// spanning two queued items arrives as two `Bytes` rather than one coalesced
/// buffer. A `cut` rewritten to concatenate would satisfy every other assertion in
/// this file.
#[test]
fn cut_cuts_at_the_target_and_keeps_the_remainder() {
    let mut fq = FrameQueue::new();
    fq.push(Bytes::from_static(b"aaaa"));
    fq.push(Bytes::from_static(b"bbbb"));
    assert_eq!(fq.len(), 8, "len tracks the pushed bytes");

    let frame = fq.cut(6).expect("6 of 8 bytes");
    assert_eq!(frame.total(), 6);
    assert_eq!(
        frame.chunks().len(),
        2,
        "a frame spanning two items must stay two uncopied slices"
    );
    assert_eq!(
        frame.chunks().concat(),
        b"aaaabb",
        "the cut is an in-order prefix of the queue"
    );
    assert_eq!(fq.len(), 2, "the split remainder stays queued");

    let rest = fq.cut(6).expect("the remainder");
    assert_eq!(rest.total(), 2, "a short final frame, not a padded one");
    assert_eq!(
        rest.chunks().len(),
        1,
        "the remainder is what is left of one item"
    );
    assert_eq!(rest.chunks().concat(), b"bb");
    assert_eq!(fq.len(), 0);

    assert!(fq.cut(6).is_none(), "an empty queue is the only `None`");
}

/// A frame that ends exactly on an item boundary takes whole items and splits
/// nothing — the case where an off-by-one in the `front_len <= remaining` branch
/// would show up as a spurious extra chunk or a dropped byte.
#[test]
fn cut_ends_on_an_item_boundary_without_splitting() {
    let mut fq = FrameQueue::new();
    for i in 0..4u8 {
        fq.push(Bytes::from(vec![i; 4]));
    }
    assert_eq!(fq.len(), 16);

    let frame = fq.cut(8).expect("two whole items");
    assert_eq!(frame.total(), 8);
    assert_eq!(frame.chunks().len(), 2, "two items moved whole, none split");
    assert_eq!(frame.chunks().concat(), [0, 0, 0, 0, 1, 1, 1, 1]);
    assert_eq!(fq.len(), 8, "the untouched items stay queued whole");
}

/// A target past everything queued yields one short frame of exactly what is
/// there, not a parked call and not a padded frame.
#[test]
fn cut_takes_everything_when_the_target_exceeds_the_queue() {
    let mut fq = FrameQueue::new();
    fq.push(Bytes::from_static(b"ab"));
    fq.push(Bytes::from_static(b"cde"));
    assert_eq!(fq.len(), 5);

    let frame = fq.cut(1024).expect("all 5 bytes");
    assert_eq!(frame.total(), 5);
    assert_eq!(frame.chunks().len(), 2, "both items ride uncopied");
    assert_eq!(frame.chunks().concat(), b"abcde");
    assert!(fq.is_empty());
}

/// `cut` returns `None` only on an empty queue. Because `push` never admits an
/// empty item, `len() == 0` and an empty queue are the same state, so the
/// under-counting desync a hand-maintained counter could reach — which would end a
/// truncated delivery with `StreamEnd` — is unrepresentable here by construction.
#[test]
fn cut_is_none_only_on_an_empty_queue() {
    let mut fq = FrameQueue::new();
    assert!(fq.cut(8).is_none(), "an empty queue cuts nothing");

    fq.push(Bytes::from_static(b"xy"));
    assert!(fq.cut(8).is_some(), "a non-empty queue always cuts");
    assert!(fq.is_empty());
    assert!(fq.cut(8).is_none(), "drained again, back to `None`");
}
