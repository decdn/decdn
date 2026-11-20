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

/// The range request covering the first `len` bytes of a blob.
const fn first_bytes(len: u64) -> OriginRangeRequest {
    OriginRangeRequest {
        fetch_start: 0,
        fetch_end: len,
    }
}

/// A symlink in the shard directory pointing outside the base must
/// be rejected — that's the whole point of the per-read
/// containment check (see issue #374).
#[cfg(unix)]
#[tokio::test]
async fn range_read_rejects_symlink_pointing_outside_base() -> anyhow::Result<()> {
    let outside = tempfile::tempdir()?;
    let secret = outside.path().join("secret");
    tokio::fs::write(&secret, b"top secret").await?;

    let inside = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(inside.path()).await?;
    let hash = Hash::new(b"marker");
    let link = origin.path_for(hash);
    create_parent(&link).await?;
    tokio::fs::symlink(&secret, &link).await?;

    let err = origin.fetch_range_data(hash, first_bytes(4)).await.err();
    ensure_permanent("fetch_range_data", err, "outside base")
}

/// A symlink that still resolves to a regular file inside the base
/// is fine — we're guarding against escape, not symlinks per se.
#[cfg(unix)]
#[tokio::test]
async fn range_read_follows_symlink_inside_base() -> anyhow::Result<()> {
    let inside = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(inside.path()).await?;

    // Drop the real file in a sibling directory under base, then
    // place a symlink at the expected sharded location that points
    // at it. canonicalize() resolves to the real file, which is
    // still under base, so the read succeeds.
    let real_dir = inside.path().join("real");
    tokio::fs::create_dir_all(&real_dir).await?;
    let real_file = real_dir.join("blob");
    tokio::fs::write(&real_file, b"hello").await?;

    let hash = Hash::new(b"marker");
    let link = origin.path_for(hash);
    create_parent(&link).await?;
    tokio::fs::symlink(&real_file, &link).await?;

    match origin.fetch_range_data(hash, first_bytes(5)).await? {
        OriginRangeFetch::Ranged { data } => {
            anyhow::ensure!(data.as_ref() == b"hello", "got: {data:?}");
        }
        OriginRangeFetch::NotFound => anyhow::bail!("expected Ranged, got NotFound"),
    }
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
        OriginRangeFetch::NotFound => anyhow::bail!("expected Ranged, got NotFound"),
    }
    Ok(())
}

/// A missing data object is `NotFound`, never an error.
#[tokio::test]
async fn fetch_range_data_missing_object_is_not_found() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let absent = Hash::new(b"no-such-object");
    anyhow::ensure!(
        matches!(
            origin
                .fetch_range_data(absent, first_bytes(16 * 1024))
                .await?,
            OriginRangeFetch::NotFound
        ),
        "a missing object must be NotFound",
    );
    Ok(())
}

/// A data object shorter than the requested span (a stale or truncated
/// origin copy) is a permanent fault.
#[tokio::test]
async fn fetch_range_data_short_object_is_permanent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let canonical = tokio::fs::canonicalize(tmp.path()).await?;
    let hash = seed_blob_with_outboard(&canonical, &vec![7u8; 32 * 1024]).await?;
    let past_end = OriginRangeRequest {
        fetch_start: 16 * 1024,
        fetch_end: 48 * 1024,
    };
    let err = origin.fetch_range_data(hash, past_end).await.err();
    ensure_permanent("fetch_range_data", err, "shorter than the range")
}

/// A blob with a published sibling `{hex}.obao4` returns the outboard
/// bytes verbatim via `fetch_outboard` alone (no data read).
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
        OutboardFetch::NotFound => anyhow::bail!("expected Found, got NotFound"),
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

/// An outboard over `outboard_max_bytes` is a permanent fault that names
/// its path, its length and the cap — a hostile/foreign `{H}.obao4` can't
/// force a huge read.
#[tokio::test]
async fn fetch_outboard_oversize_is_permanent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let canonical = tokio::fs::canonicalize(tmp.path()).await?;
    let hash = seed_blob_with_outboard(&canonical, &vec![3u8; 64 * 1024]).await?;
    let obao4 = origin.obao4_path_for(hash);
    // Cap the outboard read at 1 byte — the real outboard is larger.
    let err = origin.fetch_outboard(hash, 1).await.err();
    ensure_permanent("fetch_outboard", err, "over its 1-byte cap")?;
    ensure_permanent(
        "fetch_outboard",
        origin.fetch_outboard(hash, 1).await.err(),
        &obao4.display().to_string(),
    )
}

/// OOM guard: a multi-megabyte `{H}.obao4` against a tiny cap is rejected
/// on its metadata length, before any read; `read_capped_stops_one_byte_past_the_cap`
/// covers the read's own bound.
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
    let err = origin.fetch_outboard(hash, 4 * 1024).await.err();
    ensure_permanent("fetch_outboard", err, "is 8388608 bytes")
}

/// A published `{hex}.sizeproof` is read verbatim; a missing one is `None`.
#[tokio::test]
async fn fetch_size_proof_reads_the_sibling() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"size-proof-marker");
    let path = origin.size_proof_path_for(hash);
    create_parent(&path).await?;
    let object = decdn_bao_range::size_proof_object(5, b"proof");
    tokio::fs::write(&path, &object).await?;

    let got = origin.fetch_size_proof(hash).await?;
    anyhow::ensure!(
        got.as_deref() == Some(object.as_slice()),
        "size proof object mismatch: {got:?}"
    );
    anyhow::ensure!(
        origin
            .fetch_size_proof(Hash::new(b"no-such-proof"))
            .await?
            .is_none(),
        "a missing size proof must be None"
    );
    Ok(())
}

/// A `{hex}.sizeproof` over its cap is a broken object: a permanent fault,
/// never an absence.
#[tokio::test]
async fn fetch_size_proof_oversize_is_permanent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"oversize-size-proof");
    let path = origin.size_proof_path_for(hash);
    create_parent(&path).await?;
    tokio::fs::write(&path, vec![0u8; MAX_SIZE_PROOF_OBJECT_BYTES + 1]).await?;

    let err = origin.fetch_size_proof(hash).await.err();
    ensure_permanent("fetch_size_proof", err, "over its")
}

/// Create the shard directory that holds `path`.
async fn create_parent(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} has no parent", path.display()))?;
    tokio::fs::create_dir_all(parent).await?;
    Ok(())
}

/// A base directory removed after construction is a permanent fault, not an
/// absence of every hash: a presence read of one hash between rescans
/// surfaces it.
#[tokio::test]
async fn a_vanished_base_is_permanent_not_absent() -> anyhow::Result<()> {
    let parent = tempfile::tempdir()?;
    let base = parent.path().join("origin");
    tokio::fs::create_dir_all(&base).await?;
    let origin = FilesystemOrigin::new(&base).await?;
    let hash = Hash::new(b"vanished-base");
    anyhow::ensure!(
        origin.fetch_size_proof(hash).await?.is_none(),
        "a missing hash under a present base is an absence"
    );

    tokio::fs::remove_dir_all(&base).await?;
    ensure_permanent(
        "fetch_size_proof",
        origin.fetch_size_proof(hash).await.err(),
        "no longer exists",
    )
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
/// read, the same containment every read applies to the data object.
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
        OutboardFetch::NotFound => anyhow::bail!("expected Found, got NotFound"),
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
/// timeout, shows the check ran first. The data object, the outboard and
/// the size proof are symlinks to one socket at a short path, because the
/// sharded path exceeds the socket address limit on macOS.
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
    tokio::fs::symlink(&socket, origin.size_proof_path_for(hash)).await?;

    let limit = Duration::from_secs(5);
    let needle = "not a regular file";
    let range = timeout(limit, origin.fetch_range_data(hash, first_bytes(1024))).await?;
    ensure_permanent("fetch_range_data", range.err(), needle)?;
    let proof = timeout(limit, origin.fetch_size_proof(hash)).await?;
    ensure_permanent("fetch_size_proof", proof.err(), needle)?;
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
/// an absence: as an absence, the presence walk would memoise the hash as
/// absent and drop it from the rescan with no log (#2801).
#[cfg(unix)]
#[tokio::test]
async fn a_dangling_leaf_symlink_is_permanent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"dangling-leaf");
    let needle = "symlink whose target does not exist";

    let data = origin.path_for(hash);
    create_parent(&data).await?;
    tokio::fs::symlink(origin.base().join("missing-target"), &data).await?;
    let range = origin.fetch_range_data(hash, first_bytes(1024)).await;
    ensure_permanent("fetch_range_data", range.err(), needle)?;

    let proof = origin.size_proof_path_for(hash);
    tokio::fs::symlink(origin.base().join("missing-proof-target"), &proof).await?;
    ensure_permanent(
        "fetch_size_proof",
        origin.fetch_size_proof(hash).await.err(),
        needle,
    )
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

    let err = origin.fetch_size_proof(hash).await.err();
    ensure_permanent(
        "fetch_size_proof",
        err,
        "symlink whose target does not exist",
    )
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
        origin.fetch_size_proof(hash).await?.is_none(),
        "a missing leaf under a valid shard symlink must stay absent"
    );
    Ok(())
}

/// A data object the process cannot read is a permanent fault (#2801).
#[cfg(unix)]
#[tokio::test]
async fn range_read_of_an_unreadable_file_is_permanent() -> anyhow::Result<()> {
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

    let err = origin.fetch_range_data(hash, first_bytes(4)).await.err();
    ensure_permanent("fetch_range_data", err, "open failed")
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

/// A dangling `.obao4` symlink is a permanent fault from `fetch_outboard`,
/// the same as for the data leaf (#2801).
#[cfg(unix)]
#[tokio::test]
async fn a_dangling_outboard_symlink_is_permanent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = FilesystemOrigin::new(tmp.path()).await?;
    let hash = Hash::new(b"dangling-outboard");
    let link = origin.obao4_path_for(hash);
    create_parent(&link).await?;
    tokio::fs::symlink(origin.base().join("missing-outboard-target"), &link).await?;

    let err = origin.fetch_outboard(hash, 1 << 20).await.err();
    ensure_permanent("fetch_outboard", err, "symlink whose target does not exist")
}
