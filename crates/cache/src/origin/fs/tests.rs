use super::*;

#[tokio::test]
async fn new_rejects_missing_path() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let missing = tmp.path().join("does-not-exist");
    let err = FilesystemOrigin::new(&missing)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("missing path should have been rejected"))?
        .to_string();
    anyhow::ensure!(
        err.contains("not accessible"),
        "error lacked context: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn new_rejects_non_directory() -> anyhow::Result<()> {
    let tmp = tempfile::NamedTempFile::new()?;
    let err = FilesystemOrigin::new(tmp.path())
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("file path should have been rejected"))?
        .to_string();
    anyhow::ensure!(
        err.contains("not a directory"),
        "error lacked context: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn path_for_uses_two_char_shard() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"marker");
    let hex = hash.to_hex();
    let path = origin.path_for(hash);
    let expected_shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    // Compare against the canonicalized tmp dir — on macOS the
    // tempdir lives under /var, which is itself a symlink to
    // /private/var, so origin.base differs from tmp.path().
    let canonical_tmp = tokio::fs::canonicalize(tmp.path()).await?;
    let expected = canonical_tmp.join(expected_shard).join(hex.as_str());
    anyhow::ensure!(path == expected, "got: {}", path.display());
    Ok(())
}

/// A symlink in the shard directory pointing outside the base must
/// be rejected — that's the whole point of the per-fetch
/// containment check (see issue #374).
#[cfg(unix)]
#[tokio::test]
async fn fetch_rejects_symlink_pointing_outside_base() -> anyhow::Result<()> {
    let outside = tempfile::tempdir()?;
    let secret = outside.path().join("secret");
    tokio::fs::write(&secret, b"top secret").await?;

    let inside = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(inside.path()).await?;
    let hash = Hash::new(b"marker");
    let hex = hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let shard_dir = inside.path().join(shard);
    tokio::fs::create_dir_all(&shard_dir).await?;
    let link = shard_dir.join(hex.as_str());
    tokio::fs::symlink(&secret, &link).await?;

    let err = origin
        .fetch(hash, 1024)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("symlink outside base should have been rejected"))?
        .to_string();
    anyhow::ensure!(err.contains("outside base"), "error lacked context: {err}");
    Ok(())
}

/// A symlink that still resolves to a regular file inside the base
/// is fine — we're guarding against escape, not symlinks per se.
#[cfg(unix)]
#[tokio::test]
async fn fetch_follows_symlink_inside_base() -> anyhow::Result<()> {
    let inside = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(inside.path()).await?;

    // Drop the real file in a sibling directory under base, then
    // place a symlink at the expected sharded location that points
    // at it. canonicalize() resolves to the real file, which is
    // still under base, so the fetch should succeed.
    let real_dir = inside.path().join("real");
    tokio::fs::create_dir_all(&real_dir).await?;
    let real_file = real_dir.join("blob");
    tokio::fs::write(&real_file, b"hello").await?;

    let hash = Hash::new(b"marker");
    let hex = hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let shard_dir = inside.path().join(shard);
    tokio::fs::create_dir_all(&shard_dir).await?;
    let link = shard_dir.join(hex.as_str());
    tokio::fs::symlink(&real_file, &link).await?;

    let fetched = origin.fetch(hash, 1024).await?;
    let bytes = fetched
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found, got NotFound"))?;
    anyhow::ensure!(bytes.as_ref() == b"hello", "got: {bytes:?}");
    Ok(())
}

/// A missing file should still surface as `NotFound`, even though
/// `canonicalize` is the first syscall and errors when the leaf
/// doesn't exist.
#[tokio::test]
async fn fetch_missing_file_is_not_found() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"marker");
    match origin.fetch(hash, 1024).await? {
        OriginFetch::NotFound => Ok(()),
        OriginFetch::Found { .. } => anyhow::bail!("expected NotFound"),
        OriginFetch::AlreadyAdmitted => {
            anyhow::bail!("FilesystemOrigin never admits directly; expected NotFound")
        }
    }
}

/// `fstat`-time cap rejection: a file whose `metadata().len()`
/// already exceeds `max_bytes` is rejected upfront in the prologue,
/// before any stream is constructed. This is the cheap path —
/// catches the legitimate "operator pre-seeded an oversized
/// blob" case without paying for `take(max_bytes + 1)` /
/// `ReaderStream` setup.
#[tokio::test]
async fn fetch_rejects_oversize_file_at_fstat_prologue() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"oversize-marker");
    let hex = hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let shard_dir = tmp.path().join(shard);
    tokio::fs::create_dir_all(&shard_dir).await?;
    let payload = vec![0xAAu8; 8 * 1024];
    tokio::fs::write(shard_dir.join(hex.as_str()), &payload).await?;

    // 8 KiB on disk, 1 KiB cap → prologue rejects (no stream
    // is built; this is the expected fast path).
    let err = origin
        .fetch(hash, 1024)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("oversize fstat must be rejected"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("exceeds max"),
        "fstat-time cap message lost actionable wording: {msg}"
    );
    Ok(())
}

/// Mid-stream cap (`cap_at_max_bytes`) is the defense for the
/// TOCTOU window: between `fstat` and the actual read, the file
/// could grow on the same inode (append, pwrite past EOF,
/// truncate-then-extend). The kernel bound is
/// `take(max_bytes + 1)`; this test exercises the wrapper that
/// catches the one-byte overrun.
///
/// We can't deterministically stage a TOCTOU race in a unit
/// test without OS-level coordination, so we drive
/// `cap_at_max_bytes` directly with a synthetic upstream that
/// produces `max_bytes + 1` bytes — same chunk-shape that
/// `ReaderStream::new(file.take(max_bytes + 1))` would emit on
/// a grew-during-read file.
#[tokio::test]
async fn cap_at_max_bytes_rejects_one_byte_overrun() -> anyhow::Result<()> {
    use futures_util::StreamExt;
    let chunks: Vec<std::io::Result<Bytes>> = vec![
        Ok(Bytes::from(vec![0xAAu8; 1024])),
        // 1025th byte — should trip the running-total cap.
        Ok(Bytes::from(vec![0xBBu8; 1])),
    ];
    let upstream = futures_util::stream::iter(chunks);
    let mut stream = Box::pin(cap_at_max_bytes(upstream, 1024));

    // First chunk: 1024 bytes, exactly at cap, must pass through.
    let first = stream
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("expected first chunk"))?;
    let first = first.map_err(|e| anyhow::anyhow!("first chunk errored: {e}"))?;
    anyhow::ensure!(
        first.len() == 1024,
        "first chunk truncated: {}",
        first.len()
    );

    // Second chunk: 1 byte, would push total to 1025 > 1024,
    // wrapper must error.
    let second = stream
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("expected second chunk"))?;
    let err = second
        .err()
        .ok_or_else(|| anyhow::anyhow!("second chunk should have errored"))?;
    // Cap breach is signalled via a typed `BlobTooLargeMarker`
    // packed into the `io::Error` inner so the engine can
    // surface `CacheError::BlobTooLarge` (not a generic
    // `OriginError`). Operator-visible Display still mentions
    // the cap; assert on the typed shape because that's what
    // the engine's downcast walks.
    let has_marker = err.get_ref().is_some_and(
        <dyn std::error::Error + Send + Sync>::is::<crate::origin::BlobTooLargeMarker>,
    );
    anyhow::ensure!(
        has_marker,
        "wrapper error lost typed BlobTooLargeMarker inner: {err}"
    );
    anyhow::ensure!(
        err.to_string().contains("max_blob_bytes=1024"),
        "wrapper Display lost cap-value wording: {err}"
    );
    Ok(())
}

/// The streaming path keeps the file descriptor alive across many
/// `poll_next` calls — `ReaderStream` owns the `Take<File>` and
/// drives one read per poll. A regression that borrowed `&mut
/// file` instead of moving ownership would close the fd between
/// chunks and either truncate the read or blow up; this test
/// catches that by asking for a payload large enough to require
/// multiple reads (default `ReaderStream` chunk = 4 KiB) and
/// asserting the full bytes come back.
#[tokio::test]
async fn fetch_streams_multi_chunk_payload() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    // 32 KiB → 8 chunks at the default 4 KiB ReaderStream size.
    let payload = (0..32u8)
        .flat_map(|i| std::iter::repeat_n(i, 1024))
        .collect::<Vec<_>>();
    let hash = Hash::new(&payload);
    let hex = hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let shard_dir = tmp.path().join(shard);
    tokio::fs::create_dir_all(&shard_dir).await?;
    tokio::fs::write(shard_dir.join(hex.as_str()), &payload).await?;

    let fetched = origin.fetch(hash, 1 << 20).await?;
    let bytes = fetched
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found"))?;
    anyhow::ensure!(
        bytes.len() == payload.len(),
        "streamed length mismatch: {} vs {}",
        bytes.len(),
        payload.len()
    );
    anyhow::ensure!(bytes.as_ref() == payload.as_slice(), "byte mismatch");
    Ok(())
}

/// Seed a sharded data object plus its sibling `{hex}.obao4` outboard
/// under `base`, returning the content hash. Mirrors `path_for` /
/// `obao4_path_for`.
async fn seed_blob_with_outboard(base: &Path, payload: &[u8]) -> anyhow::Result<Hash> {
    use bao_tree::io::outboard::PreOrderMemOutboard;
    let ob = PreOrderMemOutboard::create(payload, crate::range_pull::IROH_BLOCK_SIZE);
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    let hex = hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let shard_dir = base.join(shard);
    tokio::fs::create_dir_all(&shard_dir).await?;
    tokio::fs::write(shard_dir.join(hex.as_str()), payload).await?;
    tokio::fs::write(
        shard_dir.join(format!("{}{OBAO4_SUFFIX}", hex.as_str())),
        ob.data,
    )
    .await?;
    Ok(hash)
}

/// `enumerate` returns exactly the data-object hashes present under the
/// sharded tree, excluding `.obao4` siblings and stray non-hash files,
/// and without reading payloads (#1130 discovery, trust model).
#[tokio::test]
async fn enumerate_lists_data_hashes_only() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let canonical = tokio::fs::canonicalize(tmp.path()).await?;

    // Two real blobs, one WITH a sibling outboard (so the `.obao4`
    // exclusion is exercised) and one without.
    let h1 = seed_blob_with_outboard(&canonical, &vec![1u8; 40 * 1024]).await?;
    let payload2 = vec![2u8; 8 * 1024];
    let h2 = Hash::new(&payload2);
    let hex2 = h2.to_hex();
    let shard2 = hex2
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let shard2_dir = canonical.join(shard2);
    tokio::fs::create_dir_all(&shard2_dir).await?;
    tokio::fs::write(shard2_dir.join(hex2.as_str()), &payload2).await?;

    // A stray non-hash file inside a valid shard dir — must be ignored.
    tokio::fs::write(shard2_dir.join("not-a-hash.txt"), b"junk").await?;

    let mut got = origin.enumerate().await?;
    got.sort_by_key(|h| *h.as_bytes());
    let mut want = vec![h1, h2];
    want.sort_by_key(|h| *h.as_bytes());
    anyhow::ensure!(
        got == want,
        "enumerate mismatch: got {got:?}, want {want:?}"
    );
    Ok(())
}

/// An empty origin enumerates to nothing (not an error).
#[tokio::test]
async fn enumerate_empty_origin_is_empty() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    anyhow::ensure!(origin.enumerate().await?.is_empty(), "expected empty");
    Ok(())
}

/// A ranged data fetch returns exactly the aligned span.
#[tokio::test]
async fn fetch_range_data_returns_span() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let canonical = tokio::fs::canonicalize(tmp.path()).await?;
    let payload = (0..200u32)
        .flat_map(|i| std::iter::repeat_n((i & 0xff) as u8, 1024))
        .collect::<Vec<_>>();
    let hash = seed_blob_with_outboard(&canonical, &payload).await?;

    let req = OriginRangeRequest {
        fetch_start: 16 * 1024,
        fetch_end: 48 * 1024,
    };
    match origin.fetch_range_data(hash, req).await? {
        OriginRangeFetch::Ranged { data } => {
            anyhow::ensure!(
                data.as_ref() == payload.get(16 * 1024..48 * 1024).unwrap_or_default(),
                "span mismatch",
            );
        }
        other => anyhow::bail!("expected Ranged, got {other:?}"),
    }
    Ok(())
}

/// A missing data object, or one shorter than the requested span (a stale
/// origin copy), degrades to `Unsupported`, never an error.
#[tokio::test]
async fn fetch_range_data_missing_or_short_object_is_unsupported() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let canonical = tokio::fs::canonicalize(tmp.path()).await?;
    let absent = Hash::new(b"no-such-object");
    let req = OriginRangeRequest {
        fetch_start: 0,
        fetch_end: 16 * 1024,
    };
    anyhow::ensure!(
        matches!(
            origin.fetch_range_data(absent, req).await?,
            OriginRangeFetch::Unsupported
        ),
        "missing object must degrade",
    );

    let payload = vec![7u8; 32 * 1024];
    let hash = seed_blob_with_outboard(&canonical, &payload).await?;
    let past_end = OriginRangeRequest {
        fetch_start: 16 * 1024,
        fetch_end: 48 * 1024,
    };
    anyhow::ensure!(
        matches!(
            origin.fetch_range_data(hash, past_end).await?,
            OriginRangeFetch::Unsupported
        ),
        "a short read must degrade",
    );
    Ok(())
}

/// A blob with a published sibling `{hex}.obao4` returns the outboard
/// bytes verbatim via `fetch_outboard` alone (no data read). This is the
/// failing-first TDD test for #1130's stream-while-store seam — written
/// before `fetch_outboard` existed on the trait.
#[tokio::test]
async fn fetch_outboard_returns_sibling_obao4() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let canonical = tokio::fs::canonicalize(tmp.path()).await?;
    let payload = vec![9u8; 64 * 1024];
    let hash = seed_blob_with_outboard(&canonical, &payload).await?;

    let obao4_path = origin.obao4_path_for(hash);
    let expected = tokio::fs::read(&obao4_path).await?;

    match origin.fetch_outboard(hash, 1 << 20).await? {
        OutboardFetch::Found(bytes) => {
            anyhow::ensure!(
                bytes.as_ref() == expected.as_slice(),
                "outboard bytes mismatch"
            );
        }
        other => anyhow::bail!("expected Found, got {other:?}"),
    }
    Ok(())
}

/// A hash with no sibling `.obao4` returns `NotFound`, not an error.
#[tokio::test]
async fn fetch_outboard_missing_sibling_is_not_found() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"no-outboard-marker");
    anyhow::ensure!(
        matches!(
            origin.fetch_outboard(hash, 1 << 20).await?,
            OutboardFetch::NotFound
        ),
        "missing sibling must be NotFound",
    );
    Ok(())
}

/// An oversize sibling outboard (beyond `outboard_max_bytes`) degrades
/// rather than buffering — a hostile/foreign `{H}.obao4` can't force a huge
/// read.
#[tokio::test]
async fn fetch_outboard_oversize_is_unsupported() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let canonical = tokio::fs::canonicalize(tmp.path()).await?;
    let payload = vec![3u8; 64 * 1024];
    let hash = seed_blob_with_outboard(&canonical, &payload).await?;
    // Cap the outboard read at 1 byte — the real outboard is larger.
    anyhow::ensure!(
        matches!(
            origin.fetch_outboard(hash, 1).await?,
            OutboardFetch::Unsupported
        ),
        "oversize outboard must degrade",
    );
    Ok(())
}

/// OOM guard: a multi-megabyte `{H}.obao4` against a tiny cap degrades
/// to `Unsupported` rather than being returned. The length checks before
/// the read reject it; `read_capped_stops_one_byte_past_the_cap` covers
/// the read's own bound.
#[tokio::test]
async fn fetch_outboard_oversize_rejected_before_read() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let canonical = tokio::fs::canonicalize(tmp.path()).await?;
    // Seed a real data object so only the outboard size is the gate.
    let payload = vec![3u8; 64 * 1024];
    let hash = Hash::new(&payload);
    let hex = hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let shard_dir = canonical.join(shard);
    tokio::fs::create_dir_all(&shard_dir).await?;
    tokio::fs::write(shard_dir.join(hex.as_str()), &payload).await?;
    // 8 MiB foreign/hostile outboard.
    tokio::fs::write(
        shard_dir.join(format!("{}{OBAO4_SUFFIX}", hex.as_str())),
        vec![0x5Au8; 8 * 1024 * 1024],
    )
    .await?;

    // 4 KiB cap — the 8 MiB outboard is rejected on its metadata length.
    anyhow::ensure!(
        matches!(
            origin.fetch_outboard(hash, 4 * 1024).await?,
            OutboardFetch::Unsupported
        ),
        "multi-MiB outboard must degrade without buffering",
    );
    Ok(())
}

/// Create the shard directory that holds `path`.
async fn create_parent(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} has no parent", path.display()))?;
    tokio::fs::create_dir_all(parent).await?;
    Ok(())
}

/// Require a `Permanent` error whose message carries `needle`.
fn ensure_permanent(what: &str, err: Option<OriginPullError>, needle: &str) -> anyhow::Result<()> {
    let err = err.ok_or_else(|| anyhow::anyhow!("{what}: expected an error"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        matches!(err, OriginPullError::Permanent(_)),
        "{what}: expected Permanent, got {err:?}"
    );
    anyhow::ensure!(
        msg.contains(needle),
        "{what}: error lacked {needle:?}: {msg}"
    );
    Ok(())
}

/// A sibling outboard symlinked outside the base is refused before any
/// read, the same containment `fetch` applies to the data object.
#[cfg(unix)]
#[tokio::test]
async fn fetch_outboard_rejects_symlink_pointing_outside_base() -> anyhow::Result<()> {
    let outside = tempfile::tempdir()?;
    let secret = outside.path().join("secret");
    tokio::fs::write(&secret, b"top secret").await?;

    let inside = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(inside.path()).await?;
    let hash = Hash::new(b"marker");
    let link = origin.obao4_path_for(hash);
    create_parent(&link).await?;
    tokio::fs::symlink(&secret, &link).await?;

    let err = origin.fetch_outboard(hash, 1 << 20).await.err();
    ensure_permanent("fetch_outboard", err, "outside base")
}

/// A sibling outboard symlinked to a regular file inside the base is read
/// through the symlink.
#[cfg(unix)]
#[tokio::test]
async fn fetch_outboard_follows_symlink_inside_base() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let canonical = tokio::fs::canonicalize(tmp.path()).await?;
    let hash = seed_blob_with_outboard(&canonical, &vec![4u8; 64 * 1024]).await?;

    let link = origin.obao4_path_for(hash);
    let expected = tokio::fs::read(&link).await?;
    let real = canonical.join("real.obao4");
    tokio::fs::rename(&link, &real).await?;
    tokio::fs::symlink(&real, &link).await?;

    match origin.fetch_outboard(hash, 1 << 20).await? {
        OutboardFetch::Found(bytes) => {
            anyhow::ensure!(
                bytes.as_ref() == expected.as_slice(),
                "outboard bytes mismatch"
            );
        }
        other => anyhow::bail!("expected Found, got {other:?}"),
    }
    Ok(())
}

/// A sibling outboard symlinked to `/dev/zero` (a zero length to `stat`,
/// endless when read) is refused by the containment check, since `/dev`
/// is outside the base, before any open. A special file inside the base
/// meets the regular-file check instead;
/// `every_read_rejects_a_special_file_inside_base` covers that.
#[cfg(unix)]
#[tokio::test]
async fn fetch_outboard_rejects_symlink_to_dev_zero() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"dev-zero-marker");
    let link = origin.obao4_path_for(hash);
    create_parent(&link).await?;
    tokio::fs::symlink("/dev/zero", &link).await?;

    let res = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        origin.fetch_outboard(hash, 1 << 20),
    )
    .await
    .map_err(|_| anyhow::anyhow!("fetch_outboard hung on a /dev/zero symlink"))?;
    ensure_permanent("fetch_outboard", res.err(), "outside base")
}

/// A directory in place of the sibling outboard is not a regular file.
#[tokio::test]
async fn fetch_outboard_rejects_a_directory_named_like_the_outboard() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"directory-marker");
    tokio::fs::create_dir_all(origin.obao4_path_for(hash)).await?;

    let err = origin.fetch_outboard(hash, 1 << 20).await.err();
    ensure_permanent("fetch_outboard", err, "not a regular file")
}

/// A special file inside the base is refused by every read with "not a
/// regular file", before any open. A Unix socket stands in for a FIFO:
/// its open fails rather than blocks, so the error message, not the
/// timeout, shows the check ran first. The data object and the outboard
/// are symlinks to one socket at a short path, because the sharded path
/// exceeds the socket address limit on macOS.
#[cfg(unix)]
#[tokio::test]
async fn every_read_rejects_a_special_file_inside_base() -> anyhow::Result<()> {
    use std::time::Duration;
    use tokio::time::timeout;

    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let socket = origin.base().join("s");
    let _listener = std::os::unix::net::UnixListener::bind(&socket)?;
    let hash = Hash::new(b"socket-marker");
    let data = origin.path_for(hash);
    create_parent(&data).await?;
    tokio::fs::symlink(&socket, &data).await?;
    tokio::fs::symlink(&socket, origin.obao4_path_for(hash)).await?;

    let limit = Duration::from_secs(5);
    let needle = "not a regular file";
    let fetch = timeout(limit, origin.fetch(hash, 1 << 20)).await?;
    ensure_permanent("fetch", fetch.err(), needle)?;
    let req = OriginRangeRequest {
        fetch_start: 0,
        fetch_end: 1024,
    };
    let range = timeout(limit, origin.fetch_range_data(hash, req)).await?;
    ensure_permanent("fetch_range_data", range.err(), needle)?;
    let size = timeout(limit, origin.size(hash)).await?;
    ensure_permanent("size", size.err(), needle)?;
    let outboard = timeout(limit, origin.fetch_outboard(hash, 1 << 20)).await?;
    ensure_permanent("fetch_outboard", outboard.err(), needle)
}

/// The outboard buffer is sized from the stat, so the `Bytes` the engine
/// keeps in its outboard cache holds no spare capacity past its length.
#[tokio::test]
async fn fetch_outboard_buffer_has_no_spare_capacity() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let canonical = tokio::fs::canonicalize(tmp.path()).await?;
    let hash = seed_blob_with_outboard(&canonical, &vec![5u8; 64 * 1024]).await?;

    let OutboardFetch::Found(bytes) = origin.fetch_outboard(hash, 1 << 20).await? else {
        anyhow::bail!("expected Found");
    };
    let len = bytes.len();
    let buf = bytes
        .try_into_mut()
        .map_err(|_| anyhow::anyhow!("outboard Bytes is shared"))?;
    anyhow::ensure!(
        buf.capacity() == len,
        "a {len}-byte outboard holds a {}-byte buffer",
        buf.capacity()
    );
    Ok(())
}

/// A presize the allocator refuses is an error for that one read, never
/// a process abort.
#[tokio::test]
async fn read_capped_reservation_failure_is_an_error() -> anyhow::Result<()> {
    let empty = std::io::Cursor::new(Vec::<u8>::new());
    let err = read_capped(empty, u64::MAX, u64::MAX)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a u64::MAX presize must fail"))?;
    anyhow::ensure!(
        err.kind() == std::io::ErrorKind::OutOfMemory,
        "expected OutOfMemory, got {err:?}"
    );
    Ok(())
}

/// The read stops one byte past the cap even when the length hint says
/// the source is empty, as for a file that grew after its stat. A source
/// of exactly the cap is read whole.
#[tokio::test]
async fn read_capped_stops_one_byte_past_the_cap() -> anyhow::Result<()> {
    let max = 4096u64;
    let mut over = std::io::Cursor::new(vec![0x5Au8; 64 * 1024]);
    anyhow::ensure!(
        read_capped(&mut over, 0, max).await?.is_none(),
        "a source past the cap must read as over the cap"
    );
    anyhow::ensure!(
        over.position() == max + 1,
        "read {} bytes against a {max}-byte cap",
        over.position()
    );

    let at_cap = std::io::Cursor::new(vec![0x5Au8; 4096]);
    let read = read_capped(at_cap, 0, max).await?;
    anyhow::ensure!(
        read.as_ref().map(Vec::len) == Some(4096),
        "a source of exactly the cap must read whole, got {:?}",
        read.as_ref().map(Vec::len)
    );
    Ok(())
}

/// A length hint gone stale between the stat and the read leaves no
/// spare capacity either way: a file that grew past its hint grows the
/// buffer, and one that shrank under it leaves the presize unfilled.
#[tokio::test]
async fn read_capped_stale_hint_leaves_no_spare_capacity() -> anyhow::Result<()> {
    for (hint, len) in [(100u64, 1500usize), (4096, 1500)] {
        let source = std::io::Cursor::new(vec![0x5Au8; len]);
        let buf = read_capped(source, hint, 4096)
            .await?
            .ok_or_else(|| anyhow::anyhow!("a {len}-byte source is under the cap"))?;
        anyhow::ensure!(
            buf.len() == len && buf.capacity() == len,
            "hint {hint}: a {}-byte read holds a {}-byte buffer",
            buf.len(),
            buf.capacity()
        );
    }
    Ok(())
}

/// A leaf symlink whose target does not exist is a permanent fault, never
/// the authoritative absence `Ok(None)`: as an absence, `size` would memoise
/// the hash as absent and drop it from the rescan with no log (#2801).
#[cfg(unix)]
#[tokio::test]
async fn a_dangling_leaf_symlink_is_permanent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"dangling-leaf");
    let data = origin.path_for(hash);
    create_parent(&data).await?;
    tokio::fs::symlink(origin.base().join("missing-target"), &data).await?;

    let needle = "symlink whose target does not exist";
    ensure_permanent("size", origin.size(hash).await.err(), needle)?;
    ensure_permanent("fetch", origin.fetch(hash, 1 << 20).await.err(), needle)
}

/// A shard directory that is a dangling symlink is a permanent fault too:
/// every leaf under it fails to resolve for the same operator mistake.
#[cfg(unix)]
#[tokio::test]
async fn a_dangling_shard_directory_symlink_is_permanent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"dangling-shard");
    let shard = origin
        .path_for(hash)
        .parent()
        .ok_or_else(|| anyhow::anyhow!("a sharded path has a parent"))?
        .to_path_buf();
    tokio::fs::symlink(origin.base().join("missing-shard"), &shard).await?;

    let err = origin.size(hash).await.err();
    ensure_permanent("size", err, "symlink whose target does not exist")
}

/// A valid symlinked shard directory with no leaf under it is still a plain
/// absence: only a link that cannot be followed is a fault.
#[cfg(unix)]
#[tokio::test]
async fn a_missing_leaf_under_a_valid_shard_symlink_is_absent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"linked-shard");
    let real = origin.base().join("real-shard");
    tokio::fs::create_dir_all(&real).await?;
    let shard = origin
        .path_for(hash)
        .parent()
        .ok_or_else(|| anyhow::anyhow!("a sharded path has a parent"))?
        .to_path_buf();
    tokio::fs::symlink(&real, &shard).await?;

    anyhow::ensure!(
        origin.size(hash).await?.is_none(),
        "a missing leaf under a valid shard symlink must stay absent"
    );
    Ok(())
}

/// A data object the process cannot read is a permanent fault from `size`,
/// not a size that every serve then fails on at `open` (#2801).
#[cfg(unix)]
#[tokio::test]
async fn size_of_an_unreadable_file_is_permanent() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let payload = b"unreadable payload";
    let hash = Hash::new(payload);
    let data = origin.path_for(hash);
    create_parent(&data).await?;
    tokio::fs::write(&data, payload).await?;
    tokio::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o000)).await?;
    // Root opens a mode-000 file anyway, so the test proves nothing there.
    if tokio::fs::File::open(&data).await.is_ok() {
        return Ok(());
    }

    let err = origin.size(hash).await.err();
    ensure_permanent("size", err, "open failed")
}

/// `ESTALE` usually clears on the next path lookup, which `canonicalize`
/// does on every read, so it is retry-eligible (#2801).
#[test]
fn a_stale_file_handle_is_transient() {
    let err = std::io::Error::from(std::io::ErrorKind::StaleNetworkFileHandle);
    assert!(classify_io_error(err).is_transient());
    let denied = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
    assert!(!classify_io_error(denied).is_transient());
}

/// A `MakeWriter` that keeps every formatted log line in memory.
#[derive(Clone, Default)]
struct LogBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl LogBuffer {
    fn contents(&self) -> String {
        let bytes = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl std::io::Write for LogBuffer {
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

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// An outboard over its cap degrades to `Unsupported`, which the engine
/// never sees, so the warn is the operator's only record. It names the
/// path, the length and the cap (#2801).
#[test]
fn an_oversize_outboard_warns_with_its_path_and_cap() -> anyhow::Result<()> {
    let logs = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let tmp = tempfile::tempdir()?;
    let (outcome, obao4) = tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
            let origin = FilesystemOrigin::new(tmp.path()).await?;
            let hash = seed_blob_with_outboard(origin.base(), &vec![3u8; 64 * 1024]).await?;
            let outcome = origin.fetch_outboard(hash, 1).await?;
            anyhow::Ok((outcome, origin.obao4_path_for(hash)))
        })
    })?;

    anyhow::ensure!(
        matches!(outcome, OutboardFetch::Unsupported),
        "an oversize outboard must degrade"
    );
    let text = logs.contents();
    anyhow::ensure!(
        text.contains("WARN") && text.contains("larger than its cap"),
        "expected the oversize warn:\n{text}"
    );
    anyhow::ensure!(
        text.contains(&obao4.display().to_string()),
        "the warn must name the outboard path {}:\n{text}",
        obao4.display()
    );
    anyhow::ensure!(
        text.contains("cap=1"),
        "the warn must name the cap:\n{text}"
    );
    Ok(())
}
