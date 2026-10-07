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
    let mut stream = Box::pin(cap_at_max_bytes(
        upstream,
        1024,
        std::path::PathBuf::from("/tmp/test-fixture"),
    ));

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

/// OOM guard: a multi-megabyte `{H}.obao4` against a tiny cap must degrade
/// via the `metadata()` length pre-check, BEFORE `tokio::fs::read` buffers
/// the whole file into memory.
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
