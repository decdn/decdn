//! Drive a live paid pull straight into a byte sink, verifying as it goes
//! (#1120).
//!
//! The buffered requester (`stream_fetch` and friends) accumulates the whole bao
//! wire form in a `BytesMut`, decodes it into a second `Vec`, and hands the
//! caller a `Bytes`. Peak memory is therefore ~2× the blob, and an interrupted
//! fetch leaves nothing usable behind. This module is the streaming alternative:
//! it feeds [`UpstreamPull`]'s chunks to `bao-tree`'s INCREMENTAL decoder
//! ([`ResponseDecoder`]) and writes each chunk group's plaintext to the sink the
//! moment it verifies.
//!
//! Two properties make that safe, both from ADR 038:
//!
//! - Every 16 KiB chunk group is verified against the content root as it
//!   decodes, not by a whole-blob re-hash at the end. A group that verifies is
//!   independently trustworthy, so flushing it is not "writing unverified bytes
//!   optimistically".
//! - A range fetched at any `byte_offset > 0` self-verifies against the root
//!   with no dependency on earlier bytes, which is what makes resume possible at
//!   all.
//!
//! What this module does NOT do is silently trust bytes already on disk from a
//! previous process. It could check them against a persisted outboard sidecar,
//! but deliberately keeps none — see [`resume_offset`] — so [`resume_is_genuine`]
//! settles it with a single whole-file hash once the tail has arrived.

use std::io::Write;

use bao_tree::io::fsm::{ResponseDecoder, ResponseDecoderNext};
use bao_tree::io::{BaoContentItem, DecodeError};
use bao_tree::{BaoTree, ChunkRanges};
use bytes::{Bytes, BytesMut};
use decdn_bao_range::{CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE, align_range};

use crate::{HashMismatch, LocalPullFault, UpstreamPull, VoucherProgress};

/// An [`iroh_io::AsyncStreamReader`] over a live [`UpstreamPull`], so the bao
/// decoder can pull wire bytes on demand instead of being handed a complete
/// buffer.
///
/// # Why the error is stashed rather than propagated
///
/// The reader trait speaks `io::Error`, but `next_chunk` produces the typed
/// errors the reputation layer classifies on (`PullStalled`, `PullTimeout`,
/// `UpstreamRefused`, `UpstreamVoucherRejected`). Collapsing those into an
/// `io::Error` would silently downgrade, say, a peer's mid-stream refusal into
/// an anonymous decode failure and stop it being scored. So the original
/// `anyhow::Error` is parked in `fault` and the trait returns a placeholder; the
/// driver below checks `fault` first and returns the real error, using the
/// decoder's complaint only when there is no parked fault.
struct PullReader {
    pull: UpstreamPull,
    /// Wire bytes received but not yet consumed by the decoder.
    buf: BytesMut,
    /// `StreamEnd` seen — no more chunks will arrive.
    ended: bool,
    /// The first `next_chunk` fault, preserved with its type. See the type docs.
    fault: Option<anyhow::Error>,
}

impl PullReader {
    fn new(pull: UpstreamPull) -> Self {
        Self {
            pull,
            buf: BytesMut::new(),
            ended: false,
            fault: None,
        }
    }

    /// Pull chunks until `buf` holds `n` bytes, the stream ends, or a fault is
    /// parked.
    async fn fill_to(&mut self, n: usize) {
        while !self.ended && self.fault.is_none() && self.buf.len() < n {
            match self.pull.next_chunk().await {
                Ok(Some(chunk)) => self.buf.extend_from_slice(&chunk),
                Ok(None) => self.ended = true,
                Err(e) => self.fault = Some(e),
            }
        }
    }

    /// Take up to `n` buffered bytes. A short read is the decoder's EOF signal,
    /// which it turns into `ParentNotFound`/`LeafNotFound` — the driver maps
    /// that to a truncation, not to corruption.
    fn take(&mut self, n: usize) -> Bytes {
        let take = self.buf.len().min(n);
        self.buf.split_to(take).freeze()
    }
}

impl iroh_io::AsyncStreamReader for PullReader {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        self.fill_to(len).await;
        Ok(self.take(len))
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        self.fill_to(L).await;
        let got = self.take(L);
        let mut out = [0u8; L];
        if got.len() < L {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "paid pull ended before the bao decoder's next fixed-size read",
            ));
        }
        out.copy_from_slice(&got);
        Ok(out)
    }
}

/// Stream a paid pull into `sink`, verifying every chunk group against `hash` as
/// it lands, and return the channel's acked voucher watermark.
///
/// `byte_offset` is where the CALLER's content starts. The server serves the
/// chunk-group-aligned superset of that (a bao proof anchors whole groups), so
/// the leading `byte_offset - fetch_start` bytes of the first group are decoded,
/// verified, and then dropped rather than written — the same trim the buffered
/// `decode_verified_range` performs, done per leaf here via each leaf's absolute
/// `offset`. The serve side must never trim (it would break the proof), so this
/// is the only place it can happen.
///
/// `sink` sees ONLY verified bytes, in order, starting exactly at `byte_offset`.
///
/// # Errors
///
/// [`HashMismatch`] for a group/root verification failure (a paid-but-corrupt
/// delivery — the caller classifies this as upstream corruption). Otherwise the
/// pull's own typed errors (stall, timeout, refusal, voucher rejection) or a
/// sink write failure, the last marked [`LocalPullFault`] so it is not scored
/// against the peer.
pub async fn pull_to_sink<W: Write>(
    pull: UpstreamPull,
    hash: [u8; 32],
    total_bytes: u64,
    byte_offset: u64,
    sink: &mut W,
    on_progress: Option<&crate::ProgressCallback>,
) -> anyhow::Result<VoucherProgress> {
    // The empty blob has no chunk groups and no proof, so there is nothing to
    // decode and nothing to write. Prove the empty stream against the empty root
    // explicitly rather than letting a zero-group decode accept ANY root — the
    // same trivial-empty-range bypass the buffered decoder guards (#1054).
    if total_bytes == 0 {
        if hash != *blake3::hash(&[]).as_bytes() {
            return Err(anyhow::Error::new(HashMismatch));
        }
        return pull.finish().await;
    }

    let reader = decode_to_sink(
        PullReader::new(pull),
        hash,
        total_bytes,
        byte_offset,
        sink,
        on_progress,
    )
    .await?;
    // `finish` enforces wire-byte completeness and drains to `StreamEnd`; the
    // decoder finishing only means the requested chunk ranges were satisfied.
    reader.pull.finish().await
}

/// A reader that may be holding back a richer error than the one the bao decoder
/// will report.
///
/// The decoder can only say "the bytes stopped arriving". When the reader knows
/// WHY — a stalled peer, a mid-stream refusal, a rejected voucher — that reason
/// is what the caller needs in order to score the peer, so it wins.
trait StashedFault {
    fn take_fault(&mut self) -> Option<anyhow::Error>;
}

impl StashedFault for PullReader {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        self.fault.take()
    }
}

/// A plain in-memory wire buffer has no out-of-band failure mode: whatever the
/// decoder says about it is the whole story.
///
/// Test-only, and gated to say so: it exists purely so the decode loop can be
/// driven from a fixed wire buffer. Note that a test using it exercises the
/// `None` branch — the one this trait was invented to avoid — so it must not be
/// the only coverage. See `fault_tests` for the branch that matters.
#[cfg(test)]
impl StashedFault for Bytes {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        None
    }
}

/// The decode-verify-trim-write loop, over any byte source.
///
/// Split from [`pull_to_sink`] so it can be driven from a fixed wire buffer in
/// tests — the verification, trimming, and error classification are the parts
/// worth testing, and none of them need a live connection or a payment channel.
/// Returns the reader so the caller can finish the underlying transfer.
async fn decode_to_sink<R, W>(
    reader: R,
    hash: [u8; 32],
    total_bytes: u64,
    byte_offset: u64,
    sink: &mut W,
    on_progress: Option<&crate::ProgressCallback>,
) -> anyhow::Result<R>
where
    R: iroh_io::AsyncStreamReader + StashedFault,
    W: Write,
{
    let aligned = align_range(byte_offset, 0, total_bytes)
        .map_err(|e| anyhow::anyhow!("range alignment: {e}").context(LocalPullFault))?;
    let tree = BaoTree::new(total_bytes, IROH_BLOCK_SIZE);
    let root = blake3::Hash::from_bytes(hash);
    let ranges: ChunkRanges = aligned.chunk_ranges().clone();

    let mut decoder = ResponseDecoder::new(root, ranges, tree, reader);
    loop {
        match decoder.next().await {
            ResponseDecoderNext::More((rest, Ok(BaoContentItem::Leaf(leaf)))) => {
                // Drop the part of this group that precedes the caller's offset.
                // Only the first decoded group can have any, but computing it
                // per-leaf from the absolute offset needs no running state and
                // cannot drift.
                let skip = usize::try_from(byte_offset.saturating_sub(leaf.offset))
                    .unwrap_or(usize::MAX)
                    .min(leaf.data.len());
                let payload = leaf.data.get(skip..).unwrap_or_default();
                if !payload.is_empty() {
                    sink.write_all(payload).map_err(|e| {
                        anyhow::Error::new(e)
                            .context("writing verified bytes to the output sink")
                            .context(LocalPullFault)
                    })?;
                    // Report against the WHOLE blob, not this attempt's tail, so a
                    // resumed fetch's bar continues from where the partial left off
                    // rather than snapping back to zero. `leaf.offset` is absolute,
                    // and the first leaf may start before `byte_offset` (the
                    // group-aligned superset), so clamp before adding what was
                    // actually written.
                    if let Some(cb) = on_progress {
                        let written = u64::try_from(payload.len()).unwrap_or(0);
                        let done = leaf
                            .offset
                            .max(byte_offset)
                            .saturating_add(written)
                            .min(total_bytes);
                        cb(done, total_bytes);
                    }
                }
                decoder = rest;
            }
            ResponseDecoderNext::More((rest, Ok(BaoContentItem::Parent(_)))) => {
                decoder = rest;
            }
            ResponseDecoderNext::More((rest, Err(decode_err))) => {
                let mut reader = rest.finish();
                // A stashed fault is the REAL cause — the decoder only complained
                // because the reader stopped feeding it. See [`StashedFault`].
                if let Some(fault) = reader.take_fault() {
                    return Err(fault);
                }
                return Err(classify_decode_error(decode_err));
            }
            ResponseDecoderNext::Done(mut reader) => {
                if let Some(fault) = reader.take_fault() {
                    return Err(fault);
                }
                return Ok(reader);
            }
        }
    }
}

/// Split a bao decode failure into "the peer lied" and "the stream ended early",
/// matching the taxonomy the buffered decoder and the node-side tee both use
/// (ADR 038). Only a hash mismatch is provably corruption; a truncated stream is
/// transport-class and must not tar the peer as a liar.
fn classify_decode_error(err: DecodeError) -> anyhow::Error {
    match err {
        DecodeError::ParentHashMismatch(_) | DecodeError::LeafHashMismatch(_) => {
            anyhow::Error::new(HashMismatch)
        }
        DecodeError::Io(io_err) => anyhow::Error::new(io_err).context("bao decode read failed"),
        not_found @ (DecodeError::ParentNotFound(_) | DecodeError::LeafNotFound(_)) => {
            anyhow::anyhow!("bao stream truncated mid-tree: {not_found}")
        }
    }
}

/// The offset a partial download of `have` bytes may resume from: `have` rounded
/// DOWN to a 16 KiB chunk-group boundary. `0` means start over.
///
/// A resumed request must start on a group boundary — the server anchors its
/// proof to whole groups, so a non-aligned offset just re-fetches the enclosing
/// group anyway. The caller truncates its partial file to this length before
/// resuming, discarding the sub-group tail.
///
/// Deliberately takes only `have`: the resume offset has to be chosen BEFORE the
/// request goes out, and the blob's true size is not known until the signed
/// response comes back. A `have` that overshoots the real blob (a stale file
/// under the same name) is rejected downstream by the response-validation floor
/// (`total_bytes >= byte_offset`), and the CLI treats that refusal as proof the
/// partial is not this blob's: it discards the prefix and refetches from zero.
///
/// # This does not validate the bytes — by choice, not by necessity
///
/// It is tempting to verify the on-disk prefix against the content hash here and
/// skip the whole-file check below. That is *possible in principle*, and it is
/// worth being precise about why we don't, because "it can't be done" would be
/// wrong: BLAKE3 is a Merkle tree, so given the interior nodes — the **outboard**
/// — a range verifies against the root from an `O(log n)` proof, with no earlier
/// bytes (ADR 038). This client already saw those hashes: the bao proof it
/// decoded carried, at every level, the sibling hashes covering the parts of the
/// tree it did not descend into. That is exactly how each group was checked
/// against the root on the way in.
///
/// What it does not do is *persist* them. It writes plaintext only, links no blob
/// store (#578), and keeps no outboard beside the `.partial` — so by the time a
/// later process picks the file up, the hashes are gone and only the content hash
/// remains to check against. Hence [`resume_is_genuine`], once, at the end.
///
/// Two things to know before trying to remove that pass. It would need a
/// *partial* outboard format: [`decdn_bao_range`] verifies a range against an
/// untrusted `{H}.obao4`, but its encoder requires a COMPLETE outboard, and an
/// interrupted client never received the interior nodes of the subtrees it never
/// fetched. And it would not remove the hashing — verifying a prefix still hashes
/// every chunk group in it, so the win is detecting a bad prefix *before* paying
/// for the tail, not avoiding an O(prefix) pass.
#[must_use]
pub const fn resume_offset(have: u64) -> u64 {
    have - (have % CHUNK_GROUP_BYTES)
}

/// Whether an assembled file that was RESUMED really is the blob `hash`.
///
/// Every byte this process fetched was verified per chunk group as it landed, so
/// a fetch that started at offset 0 needs no check — it cannot be wrong. A
/// resumed fetch is different: the prefix came off disk, written by an earlier
/// process, and could have been truncated mid-write, corrupted by the
/// filesystem, or simply be a different blob's bytes under the same filename.
/// Nothing verified it, because this client keeps no outboard sidecar to verify
/// it against (see [`resume_offset`] — that is a choice, not an impossibility).
///
/// So the check happens here, once, against the one thing that is authoritative:
/// the content hash. This is O(blob) local hashing — around a gigabyte per
/// second — against a download the user has already paid network egress for, so
/// it is cheap in the only currency that matters here, and it is what keeps the
/// content-addressing guarantee true end to end. A `false` result means the
/// prefix was bad: the partial file must be discarded, not promoted.
///
/// Takes a reader, not a buffer, and hashes through a fixed window: handing it
/// `&[u8]` would mean reading the whole blob back into memory to prove we had
/// avoided holding the whole blob in memory.
///
/// # Errors
///
/// Propagates read failures from `reader`.
pub fn resume_is_genuine<R: std::io::Read>(hash: [u8; 32], mut reader: R) -> std::io::Result<bool> {
    // Large enough to keep the hasher fed, small enough to be irrelevant next to
    // the process's other buffers.
    const WINDOW: usize = 64 * 1024;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; WINDOW];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        // A `Read` impl reporting more bytes than the buffer holds is broken.
        // `unwrap_or_default()` here would feed the hasher NOTHING and quietly
        // return the wrong verdict from the one check standing between a corrupt
        // `.partial` and a wrong output file — so refuse instead.
        let filled = buf.get(..n).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "reader claimed more bytes than it returned",
            )
        })?;
        hasher.update(filled);
    }
    Ok(*hasher.finalize().as_bytes() == hash)
}

#[cfg(test)]
mod tests {
    use super::{decode_to_sink, resume_is_genuine, resume_offset};
    use crate::HashMismatch;
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use decdn_bao_range::{IROH_BLOCK_SIZE, align_range, encode_verified_range};

    fn make_blob(len: usize) -> Vec<u8> {
        let mut v = vec![0u8; len];
        let mut x: u32 = 0x9e37_79b9;
        for b in &mut v {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes().first().copied().unwrap_or(0);
        }
        v
    }

    /// The header-less bao wire a server emits for `[byte_offset, end)` of
    /// `blob`, plus the content root — mirrors `wire_for` in the crate's own
    /// tests so both decoders are exercised against identical input.
    fn wire_for(blob: &[u8], byte_offset: u64) -> anyhow::Result<([u8; 32], Vec<u8>)> {
        let ob = PreOrderMemOutboard::create(blob, IROH_BLOCK_SIZE);
        let root = *ob.root.as_bytes();
        let blob_size = u64::try_from(blob.len())?;
        let aligned = align_range(byte_offset, 0, blob_size)?;
        let s = usize::try_from(aligned.fetch_start())?;
        let e = usize::try_from(aligned.fetch_end())?;
        let data = blob
            .get(s..e)
            .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
            .to_vec();
        let combined = encode_verified_range(root, &aligned, &data, ob.data.clone().into())?;
        let wire = combined
            .get(8..)
            .ok_or_else(|| anyhow::anyhow!("combined shorter than 8-byte header"))?
            .to_vec();
        Ok((root, wire))
    }

    /// Whole-blob streaming decode must write exactly the blob — the streaming
    /// path's equivalent of the buffered `decode_verified_range` round-trip.
    #[tokio::test]
    async fn streams_a_whole_blob_to_the_sink() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (root, wire) = wire_for(&blob, 0)?;
        let mut out: Vec<u8> = Vec::new();
        decode_to_sink(
            Bytes::from(wire),
            root,
            u64::try_from(blob.len())?,
            0,
            &mut out,
            None,
        )
        .await?;
        anyhow::ensure!(out == blob, "streamed output differs from the blob");
        Ok(())
    }

    /// A resumed fetch at a NON-aligned offset must still write exactly the
    /// caller's span. The server anchors its proof to whole chunk groups and never
    /// trims, so the decoder receives bytes before `byte_offset` — dropping them
    /// is this module's job, and getting it wrong would silently prepend bytes to
    /// the user's output.
    #[tokio::test]
    async fn trims_the_group_aligned_prefix_on_a_resumed_offset() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let blob_size = u64::try_from(blob.len())?;
        // Deliberately mid-group: 70000 is not a multiple of 16 KiB.
        for offset in [16 * 1024u64, 70_000, 64 * 1024] {
            let (root, wire) = wire_for(&blob, offset)?;
            let mut out: Vec<u8> = Vec::new();
            // Record progress while we are here: the clamp
            // (`leaf.offset.max(byte_offset)`) only matters at a resumed offset,
            // and without it the bar reports positions BELOW where the partial
            // already reached — i.e. it appears to go backwards on resume.
            let seen: std::sync::Arc<std::sync::Mutex<Vec<(u64, u64)>>> =
                std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let sink_seen = std::sync::Arc::clone(&seen);
            let cb: Box<crate::ProgressCallback> = Box::new(move |done, total| {
                if let Ok(mut g) = sink_seen.lock() {
                    g.push((done, total));
                }
            });
            decode_to_sink(
                Bytes::from(wire),
                root,
                blob_size,
                offset,
                &mut out,
                Some(cb.as_ref()),
            )
            .await?;

            let reports = seen
                .lock()
                .map_err(|_| anyhow::anyhow!("poisoned"))?
                .clone();
            anyhow::ensure!(
                !reports.is_empty(),
                "progress must be reported as groups land, not once at the end (offset {offset})"
            );
            anyhow::ensure!(
                reports.is_sorted_by_key(|(done, _)| *done),
                "progress must not go backwards at offset {offset}: {reports:?}"
            );
            anyhow::ensure!(
                reports.first().is_some_and(|(done, _)| *done >= offset),
                "a resumed fetch must report from the resume point, not from 0 (offset {offset})"
            );
            anyhow::ensure!(
                reports.last() == Some(&(blob_size, blob_size)),
                "the final report must reach 100% at offset {offset}: {:?}",
                reports.last()
            );
            let want = blob
                .get(usize::try_from(offset)?..)
                .ok_or_else(|| anyhow::anyhow!("offset out of bounds"))?;
            anyhow::ensure!(
                out == want,
                "resumed span at offset {offset} is wrong: got {} bytes, want {}",
                out.len(),
                want.len()
            );
        }
        Ok(())
    }

    /// A corrupted group must surface as the typed `HashMismatch` — that is what
    /// the caller downcasts on to score the upstream for a paid-but-corrupt
    /// delivery, and it must NOT be confused with a truncated stream.
    #[tokio::test]
    async fn a_corrupt_group_is_a_typed_hash_mismatch() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (root, mut wire) = wire_for(&blob, 0)?;
        // Flip a byte deep in the payload, past the proof preamble.
        let victim = wire
            .len()
            .checked_sub(1024)
            .ok_or_else(|| anyhow::anyhow!("wire too short"))?;
        if let Some(b) = wire.get_mut(victim) {
            *b ^= 0xff;
        }
        let mut out: Vec<u8> = Vec::new();
        let err = decode_to_sink(
            Bytes::from(wire),
            root,
            u64::try_from(blob.len())?,
            0,
            &mut out,
            None,
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("corrupt group must not decode"))?;
        anyhow::ensure!(
            err.downcast_ref::<HashMismatch>().is_some(),
            "corruption must surface as the typed HashMismatch, got: {err}"
        );
        Ok(())
    }

    /// A truncated stream is transport-class, NOT corruption: reporting it as
    /// `HashMismatch` would tar an honest peer whose connection merely dropped.
    #[tokio::test]
    async fn a_truncated_stream_is_not_reported_as_corruption() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (root, wire) = wire_for(&blob, 0)?;
        let half = wire.len() / 2;
        let cut = wire.get(..half).unwrap_or_default().to_vec();
        let mut out: Vec<u8> = Vec::new();
        let err = decode_to_sink(
            Bytes::from(cut),
            root,
            u64::try_from(blob.len())?,
            0,
            &mut out,
            None,
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a truncated stream must not decode cleanly"))?;
        anyhow::ensure!(
            err.downcast_ref::<HashMismatch>().is_none(),
            "a truncated stream must not be classified as corruption: {err}"
        );
        Ok(())
    }

    /// Bytes must reach the sink as groups verify, not all at the end — that is
    /// the whole point of the module. Proven by truncating the wire: everything
    /// decoded before the cut must already have been written.
    #[tokio::test]
    async fn verified_bytes_reach_the_sink_before_the_stream_ends() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let (root, wire) = wire_for(&blob, 0)?;
        let cut = wire.get(..wire.len() / 2).unwrap_or_default().to_vec();
        let mut out: Vec<u8> = Vec::new();
        let _ = decode_to_sink(
            Bytes::from(cut),
            root,
            u64::try_from(blob.len())?,
            0,
            &mut out,
            None,
        )
        .await;
        anyhow::ensure!(
            !out.is_empty(),
            "a failed transfer must still have flushed the groups that DID verify — \
             otherwise there is nothing to resume from"
        );
        anyhow::ensure!(
            blob.starts_with(&out),
            "flushed bytes must be a genuine prefix of the blob"
        );
        Ok(())
    }

    /// The resume offset must land on a chunk-group boundary and never exceed
    /// what is on disk; a partial longer than the blob is not this blob's partial.
    #[test]
    fn resume_offset_snaps_down_to_a_group_boundary() {
        use decdn_bao_range::CHUNK_GROUP_BYTES as GROUP;
        assert_eq!(resume_offset(0), 0, "nothing on disk");
        assert_eq!(resume_offset(GROUP - 1), 0, "short of the first group");
        assert_eq!(resume_offset(GROUP), GROUP, "exactly one group");
        assert_eq!(
            resume_offset(GROUP + 1),
            GROUP,
            "a sub-group tail is discarded"
        );
        assert_eq!(
            resume_offset(3 * GROUP + 5),
            3 * GROUP,
            "floors to the group below, never rounds up past what is on disk"
        );
    }

    /// The whole-file check is what makes resume safe, since the on-disk prefix
    /// is unverifiable in isolation. It must accept the true blob and reject a
    /// prefix that was tampered with.
    #[test]
    fn resume_is_genuine_accepts_the_blob_and_rejects_a_tampered_prefix() -> anyhow::Result<()> {
        let blob = make_blob(200 * 1024 + 777);
        let hash = *blake3::hash(&blob).as_bytes();
        anyhow::ensure!(
            resume_is_genuine(hash, &blob[..])?,
            "the true blob must verify"
        );

        let mut tampered = blob.clone();
        if let Some(b) = tampered.first_mut() {
            *b ^= 0x01;
        }
        anyhow::ensure!(
            !resume_is_genuine(hash, &tampered[..])?,
            "a tampered prefix must be rejected — this is the only thing standing between a \
             corrupt .partial and a wrong output file"
        );
        Ok(())
    }
}

/// The stashed-fault contract, tested without a network.
///
/// [`PullReader`] itself needs a live `UpstreamPull`, so these drive
/// [`decode_to_sink`] through a stand-in reader that behaves identically in the
/// one dimension that matters: it feeds wire bytes, then stops and parks a typed
/// error. That is the exact shape a stalled or refusing peer produces, and the
/// property under test — that the parked error beats the decoder's complaint — is
/// what keeps the reputation layer able to score a misbehaving peer.
#[cfg(test)]
mod fault_tests {
    use super::{StashedFault, decode_to_sink};
    use crate::PullStalled;
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use decdn_bao_range::{IROH_BLOCK_SIZE, align_range, encode_verified_range};
    use std::time::Duration;

    /// Feeds `prefix`, then reports EOF while holding a typed fault — exactly what
    /// `PullReader` does when `next_chunk` fails partway through a transfer.
    struct FaultingReader {
        prefix: Bytes,
        fault: Option<anyhow::Error>,
    }

    impl iroh_io::AsyncStreamReader for FaultingReader {
        async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
            let take = self.prefix.len().min(len);
            Ok(self.prefix.split_to(take))
        }

        async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
            if self.prefix.len() < L {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "faulting reader exhausted",
                ));
            }
            let got = self.prefix.split_to(L);
            let mut out = [0u8; L];
            out.copy_from_slice(&got);
            Ok(out)
        }
    }

    impl StashedFault for FaultingReader {
        fn take_fault(&mut self) -> Option<anyhow::Error> {
            self.fault.take()
        }
    }

    fn wire_for(blob: &[u8]) -> anyhow::Result<([u8; 32], Vec<u8>)> {
        let ob = PreOrderMemOutboard::create(blob, IROH_BLOCK_SIZE);
        let root = *ob.root.as_bytes();
        let aligned = align_range(0, 0, u64::try_from(blob.len())?)?;
        let combined = encode_verified_range(root, &aligned, blob, ob.data.clone().into())?;
        Ok((
            root,
            combined
                .get(8..)
                .ok_or_else(|| anyhow::anyhow!("combined shorter than the header"))?
                .to_vec(),
        ))
    }

    /// A parked upstream fault must be what the caller sees — NOT the decoder's
    /// "the bytes ran out".
    ///
    /// This is the entire reason `StashedFault` exists. Swap the two branches in
    /// `decode_to_sink`'s error arm and every other test in this file still
    /// passes, while in production every stall, refusal, and voucher rejection on
    /// the CLI fetch path silently degrades to an anonymous decode error and the
    /// peer stops being scored for it.
    #[tokio::test]
    async fn a_parked_upstream_fault_beats_the_decoder_complaint() -> anyhow::Result<()> {
        let blob = vec![7u8; 200 * 1024];
        let (root, wire) = wire_for(&blob)?;
        // Half a wire is a truncation as far as the decoder is concerned.
        let half = wire.get(..wire.len() / 2).unwrap_or_default().to_vec();
        let reader = FaultingReader {
            prefix: Bytes::from(half),
            fault: Some(anyhow::Error::new(PullStalled {
                after: Duration::from_secs(30),
            })),
        };

        let mut out: Vec<u8> = Vec::new();
        let err = decode_to_sink(reader, root, u64::try_from(blob.len())?, 0, &mut out, None)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("a truncated feed must not decode cleanly"))?;

        anyhow::ensure!(
            err.downcast_ref::<PullStalled>().is_some(),
            "the typed upstream fault must survive; got: {err}"
        );
        Ok(())
    }

    /// With no parked fault the decoder's own classification stands, so the
    /// precedence rule cannot be implemented as "always return the stash".
    #[tokio::test]
    async fn without_a_parked_fault_the_decoder_error_stands() -> anyhow::Result<()> {
        let blob = vec![9u8; 200 * 1024];
        let (root, wire) = wire_for(&blob)?;
        let half = wire.get(..wire.len() / 2).unwrap_or_default().to_vec();
        let reader = FaultingReader {
            prefix: Bytes::from(half),
            fault: None,
        };

        let mut out: Vec<u8> = Vec::new();
        let err = decode_to_sink(reader, root, u64::try_from(blob.len())?, 0, &mut out, None)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("a truncated feed must not decode cleanly"))?;

        anyhow::ensure!(
            err.downcast_ref::<PullStalled>().is_none(),
            "no fault was parked, so nothing should have been substituted: {err}"
        );
        anyhow::ensure!(
            err.downcast_ref::<crate::HashMismatch>().is_none(),
            "a truncation is not corruption: {err}"
        );
        Ok(())
    }
}
