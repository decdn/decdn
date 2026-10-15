use super::*;

/// Minimal `ManifestEntry` for the `--select` tests: path + size, no chunks.
fn sel_entry(path: &str, size: u64) -> ManifestEntry {
    ManifestEntry::new(path, "b3:00", Some(size))
}

/// A plain manifest entry: path + hash, no size or chunks.
fn entry(path: &str, hash: &str) -> ManifestEntry {
    ManifestEntry::new(path, hash, None)
}

/// Write `manifest` to `out`'s manifest cache, as a pull that fetched it
/// leaves it, and return its hash as bare hex.
fn cache_manifest(out: &Path, manifest: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(manifest).unwrap();
    let hash = *blake3::hash(&bytes).as_bytes();
    let path = BundleManifest::cache_path(out, hash);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, &bytes).unwrap();
    blake3::Hash::from_bytes(hash).to_hex().to_string()
}

/// Parse `decdn get <uri> <extra…>` into its args and URI.
fn get_args(uri: &str, extra: &[&str]) -> (GetArgs, DecdnUri) {
    use clap::Parser as _;
    let argv = ["decdn", "get", uri]
        .into_iter()
        .chain(extra.iter().copied());
    let decdn_common::cli::Command::Get(get) = decdn_common::cli::Cli::parse_from(argv).command
    else {
        panic!("not a get");
    };
    let parsed = get.uri.parse().unwrap();
    (*get, parsed)
}

/// A report of `fetched` fetched, `linked` linked and `skipped` skipped
/// entries, with each of `failures` failed.
fn report_of(fetched: u64, linked: u64, skipped: u64, failures: &[(&str, &str)]) -> BundleReport {
    let mut rep = BundleReport::default();
    rep.fetched = fetched;
    rep.linked = linked;
    rep.skipped = skipped;
    rep.failed = u64::try_from(failures.len()).unwrap();
    rep.failures = failures
        .iter()
        .map(|(path, error)| {
            let mut failure = decdn_client::bundle::EntryFailure::default();
            failure.path = (*path).to_string();
            failure.error = (*error).to_string();
            failure
        })
        .collect();
    rep
}

fn strs(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

fn sample() -> Vec<ManifestEntry> {
    vec![
        entry("models/a.bin", "b3:1"),
        entry("models/b.txt", "b3:2"),
        entry("docs/readme.md", "b3:3"),
        entry("docs/deep/notes.txt", "b3:4"),
    ]
}

#[test]
fn entry_retries_flag_is_gone() {
    use clap::Parser as _;
    let uri = format!("decdn:{}/", "ab".repeat(32));
    let base = ["decdn", "get", uri.as_str(), "-o", "out"];
    assert!(decdn_common::cli::Cli::try_parse_from(base).is_ok());
    let parsed = decdn_common::cli::Cli::try_parse_from(
        base.iter().copied().chain(["--entry-retries", "2"]),
    );
    assert!(parsed.is_err());
}

#[test]
fn parse_selection_drops_commented_and_deleted_lines() {
    let entries = vec![
        sel_entry("a.bin", 1),
        sel_entry("b.bin", 2),
        sel_entry("c.bin", 3),
    ];
    // "a.bin" kept, "b.bin" commented out, "c.bin" deleted entirely.
    let edited = "# header\na.bin\t1 B\n#b.bin\t2 B\n";
    let kept = parse_selection(edited, entries).expect("parse");
    let paths: Vec<&str> = kept.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, vec!["a.bin"]);
}

#[test]
fn parse_selection_preserves_manifest_order_regardless_of_edit_order() {
    let entries = vec![sel_entry("a.bin", 1), sel_entry("b.bin", 2)];
    // User reordered the lines; output still follows manifest order.
    let edited = "b.bin\nb.bin is not a path\n".replace("b.bin is not a path", "a.bin");
    let kept = parse_selection(&edited, entries).expect("parse");
    let paths: Vec<&str> = kept.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, vec!["a.bin", "b.bin"]);
}

#[test]
fn parse_selection_empty_when_everything_removed() {
    let entries = vec![sel_entry("a.bin", 1)];
    let kept = parse_selection("# all gone\n", entries).expect("parse");
    assert!(kept.is_empty());
}

#[test]
fn parse_selection_rejects_unknown_path() {
    let entries = vec![sel_entry("a.bin", 1)];
    let err = parse_selection("a.bin\ntypo.bin\n", entries).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("typo.bin"), "{msg}");
}

#[test]
fn parse_selection_ignores_the_size_annotation_after_the_tab() {
    let entries = vec![sel_entry("weights.bin", 1500)];
    // The size column is arbitrary human text; only the path is read.
    let kept = parse_selection("weights.bin\t1.5 KB\n", entries).expect("parse");
    assert_eq!(kept.len(), 1);
}

#[test]
fn render_then_parse_unedited_keeps_every_entry() {
    let entries = vec![sel_entry("a.bin", 1), sel_entry("dir/b.bin", 2000)];
    let buffer = render_selection(&entries);
    let kept = parse_selection(&buffer, entries).expect("parse");
    let paths: Vec<&str> = kept.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, vec!["a.bin", "dir/b.bin"]);
}

#[test]
fn editor_command_prefers_visual_then_editor_then_vi() {
    assert_eq!(editor_command(Some("nano"), Some("vim")), vec!["nano"]);
    assert_eq!(editor_command(None, Some("vim")), vec!["vim"]);
    assert_eq!(editor_command(None, None), vec!["vi"]);
}

#[test]
fn editor_command_treats_blank_as_unset() {
    // A blank/whitespace-only $VISUAL falls through to $EDITOR.
    assert_eq!(editor_command(Some("   "), Some("vim")), vec!["vim"]);
    assert_eq!(editor_command(Some(""), None), vec!["vi"]);
}

#[test]
fn editor_command_splits_arguments() {
    assert_eq!(
        editor_command(Some("code --wait"), None),
        vec!["code", "--wait"]
    );
}

#[test]
fn check_selectable_rejects_hash_leading_and_tabbed_paths() {
    assert!(check_selectable(&[sel_entry("#weird.bin", 1)]).is_err());
    assert!(check_selectable(&[sel_entry("has\ttab.bin", 1)]).is_err());
    // Leading whitespace before a '#' would read as a comment and be
    // silently dropped by parse_selection — must be rejected here.
    assert!(check_selectable(&[sel_entry(" #weird.bin", 1)]).is_err());
    // Any leading/trailing whitespace cannot round-trip (parse trims it).
    assert!(check_selectable(&[sel_entry(" leading.bin", 1)]).is_err());
    assert!(check_selectable(&[sel_entry("trailing.bin ", 1)]).is_err());
    assert!(check_selectable(&[sel_entry("fine/name.bin", 1)]).is_ok());
}

#[test]
fn report_errors_when_any_failed() {
    let rep = report_of(1, 0, 1, &[("x", "boom")]);
    let err = report(&rep, 0, Path::new("/out"), true).unwrap_err();
    assert!(format!("{err:#}").contains("1 entr"), "{err:#}");
}

#[test]
fn report_ok_when_none_failed() {
    let rep = report_of(1, 0, 1, &[]);
    assert!(report(&rep, 0, Path::new("/out"), false).is_ok());
}

/// A warning line, such as a blob whose size differs from its manifest, is
/// printed and never fails the pull.
#[test]
fn a_report_warning_does_not_fail_the_pull() {
    let mut rep = report_of(2, 0, 0, &[]);
    rep.warnings = vec!["big.bin: manifest says 4 bytes, the blob is 12 bytes".to_string()];
    assert!(
        report(&rep, 0, Path::new("/out"), false).is_ok(),
        "a size warning is not a failure"
    );
}

/// The summary counts every entry: a whole-file reuse reads `reused`, and
/// the entries the filter or `--select` dropped read `excluded` (#2190).
#[test]
fn the_summary_counts_reused_and_excluded_entries() {
    let mut rep = report_of(1, 1, 1, &[("x", "boom")]);
    rep.reused = 1;
    rep.reused_bytes = 7;
    rep.downloaded = 4;
    rep.paid_bytes = 5;
    rep.reconstructed = 20;
    rep.resumed_bytes = 6;
    let rep = pull_report(&rep, 3, Path::new("/out"));
    assert_eq!(
        counts_line(&rep),
        "pulled into /out (1 fetched, 1 linked, 1 skipped, 1 reused, 3 excluded, 1 failed)"
    );
    assert_eq!(rep.reused_bytes, 7);

    let json = serde_json::to_value(&rep).expect("serialize");
    assert_eq!(json["reused"], 1);
    assert_eq!(json["reused_bytes"], 7);
    assert_eq!(json["excluded"], 3);
    assert_eq!(json["resumed_bytes"], 6);
    assert_eq!(json["downloaded"], 4);
    assert_eq!(json["paid_bytes"], 5);
    assert!(json.get("deduped").is_none(), "{json}");
}

#[test]
fn human_bytes_scales_units() {
    assert_eq!(human_bytes(0), "0 B");
    assert_eq!(human_bytes(512), "512 B");
    assert_eq!(human_bytes(1_500), "1.5 KB");
    assert_eq!(human_bytes(27_600_000_000), "27.6 GB");
}

#[test]
fn transfer_line_shows_arrow_only_when_they_differ() {
    // Equal → a single figure, no "→ reconstructed" clause.
    assert_eq!(
        transfer_line(27_600_000_000, 27_700_000_000, 27_600_000_000),
        "downloaded 27.6 GB (paid 27.7 GB)"
    );
    // Differ (dedup saved bytes) → both figures with the arrow.
    assert_eq!(
        transfer_line(13_800_000_000, 13_900_000_000, 27_600_000_000),
        "downloaded 13.8 GB (paid 13.9 GB) → reconstructed 27.6 GB"
    );
}

/// The paid figure shows whenever the run paid or downloaded anything, so a
/// run that paid for bytes twice prints a figure above `downloaded` (#2377).
/// A run that neither downloaded nor paid leaves it out.
#[test]
fn transfer_line_shows_paid_unless_nothing_moved() {
    assert_eq!(
        transfer_line(45_700_000_000, 52_100_000_000, 76_900_000_000),
        "downloaded 45.7 GB (paid 52.1 GB) → reconstructed 76.9 GB"
    );
    // Every file reused from an on-disk donor: nothing paid.
    assert_eq!(
        transfer_line(0, 0, 4_000),
        "downloaded 0 B → reconstructed 4.0 KB"
    );
    assert_eq!(transfer_line(0, 0, 0), "downloaded 0 B");
    // A run whose every fetch failed after paying still shows what it paid.
    assert_eq!(transfer_line(0, 2_000, 0), "downloaded 0 B (paid 2.0 KB)");
}

fn filtered_paths(include: &[&str], exclude: &[&str], entries: Vec<ManifestEntry>) -> Vec<String> {
    let filter = EntryFilter::compile(None, &strs(include), &strs(exclude)).unwrap();
    filter
        .apply_reporting(entries)
        .kept
        .into_iter()
        .map(|e| e.path)
        .collect()
}

/// An entry URI keeps exactly the entry whose path it names, byte for byte:
/// glob metacharacters in a real filename match only themselves.
#[test]
fn entry_filter_keeps_exactly_the_named_entry() {
    let entries = vec![
        entry("x[1].txt", "b3:1"),
        entry("x1.txt", "b3:2"),
        entry("a*.bin", "b3:3"),
        entry("abc.bin", "b3:4"),
        entry("dir/{a,b}.txt", "b3:5"),
        entry("dir/a.txt", "b3:6"),
    ];
    for path in ["x[1].txt", "a*.bin", "dir/{a,b}.txt", "x1.txt"] {
        let filter = EntryFilter::compile(Some(path.to_owned()), &[], &[]).unwrap();
        let (kept, excluded) = filter.apply_and_warn(entries.clone()).unwrap();
        assert_eq!(
            kept.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(),
            vec![path]
        );
        assert_eq!(excluded, 5);
    }
}

/// An entry URI that names no manifest entry is an error, not an empty run.
#[test]
fn entry_filter_rejects_a_missing_entry() {
    let filter = EntryFilter::compile(Some("models/c.bin".to_owned()), &[], &[]).unwrap();
    let err = filter.apply_and_warn(sample()).unwrap_err();
    assert!(
        err.to_string().contains("no entry \"models/c.bin\""),
        "{err}"
    );
}

/// Each pattern that matches no entry is reported with its flag, and one
/// whose `**/` form would match gets that form as a hint: every pattern
/// matches from the bundle root (#2190).
#[test]
fn entry_filter_reports_each_pattern_that_matched_nothing() {
    let mut entries = sample();
    entries.push(entry("gpt/metal/model.bin", "b3:5"));
    let filter = EntryFilter::compile(
        None,
        &strs(&["models/*", "no/such/*"]),
        &strs(&["metal/*", "**/*.md"]),
    )
    .unwrap();
    let pass = filter.apply_reporting(entries);
    assert_eq!(
        pass.kept
            .iter()
            .map(|e| e.path.as_str())
            .collect::<Vec<_>>(),
        vec!["models/a.bin", "models/b.txt"]
    );
    assert_eq!(pass.excluded, 3);
    assert_eq!(
        pass.unmatched,
        vec![
            Unmatched {
                flag: "--include",
                pattern: "no/such/*".into(),
                suggestion: None,
            },
            Unmatched {
                flag: "--exclude",
                pattern: "metal/*".into(),
                suggestion: Some("**/metal/*".into()),
            },
        ]
    );
    assert_eq!(
        pass.unmatched[0].warning(),
        "--include 'no/such/*' matched no entries"
    );
    assert_eq!(
        pass.unmatched[1].warning(),
        "--exclude 'metal/*' matched no entries (a pattern matches the whole path from the \
         bundle root; did you mean '**/metal/*'?)"
    );
}

/// A pattern with no `/` is anchored too: `*.txt` matches only a root-level
/// file, so a miss suggests `**/*.txt`. A pattern that already starts with
/// `**` gets no hint.
#[test]
fn entry_filter_hints_an_anchored_pattern_without_a_slash() {
    let filter = EntryFilter::compile(None, &[], &strs(&["*.txt", "**/*.png"])).unwrap();
    let pass = filter.apply_reporting(sample());
    assert_eq!(pass.excluded, 0);
    assert_eq!(
        pass.unmatched
            .iter()
            .map(|u| (u.pattern.as_str(), u.suggestion.as_deref()))
            .collect::<Vec<_>>(),
        vec![("*.txt", Some("**/*.txt")), ("**/*.png", None)]
    );
}

/// Patterns that each match an entry report nothing, even an `--exclude`
/// that matches only entries the include gate already dropped.
#[test]
fn entry_filter_reports_nothing_when_every_pattern_matches() {
    let filter = EntryFilter::compile(None, &strs(&["models/*"]), &strs(&["docs/**"])).unwrap();
    let pass = filter.apply_reporting(sample());
    assert!(pass.unmatched.is_empty(), "{:?}", pass.unmatched);
    assert_eq!(pass.excluded, 2);

    let pass = EntryFilter::compile(None, &[], &[])
        .unwrap()
        .apply_reporting(sample());
    assert!(pass.unmatched.is_empty());
    assert_eq!(pass.excluded, 0);
}

/// `--select` adds each entry it drops to the filter's excluded count.
#[test]
fn a_selection_adds_its_dropped_entries_to_the_excluded_count() {
    let kept = Kept {
        manifest: BundleManifest::new(sample()),
        excluded: 2,
    };
    let kept = apply_selection(kept, |entries| {
        parse_selection("models/a.bin\ndocs/readme.md\n", entries)
    })
    .unwrap();
    assert_eq!(kept.manifest.entries.len(), 2);
    assert_eq!(kept.excluded, 4);
}

/// The run-end scan counts the staging files of blobs the run did not
/// use, once per blob, totals the disk space they take, and ignores the
/// run's own blobs, the manifest blob, and names that are not a blob's
/// staging file (#2190).
#[test]
fn leftover_partials_counts_only_unselected_blobs() {
    let tmp = tempfile::tempdir().expect("tmp");
    assert_eq!(
        leftover_partials(tmp.path(), &HashSet::new()),
        Leftovers::default(),
        "no staging dir, nothing left over"
    );

    let dir = tmp.path().join(STAGING_DIR);
    std::fs::create_dir_all(&dir).expect("staging dir");
    let hex = |b: u8| blake3::Hash::from_bytes([b; 32]).to_hex().to_string();
    let write = |name: String, len: usize| {
        std::fs::write(dir.join(name), vec![0u8; len]).expect("write");
    };
    // The run's selected entry and the manifest blob: kept for this run.
    write(format!("{}.partial", hex(1)), 500);
    write(format!("{}.partial.ranges", hex(1)), 5);
    write(format!("{}.partial", hex(2)), 300);
    // An orphan partial and its record, and a finalized orphan blob.
    write(format!("{}.partial", hex(3)), 1000);
    write(format!("{}.partial.ranges", hex(3)), 10);
    write(hex(4), 200);
    // A record's temporary file and a stray name.
    write(".tmpAbC123".into(), 50);
    write("notes.txt".into(), 50);

    let keep: HashSet<[u8; 32]> = [[1; 32], [2; 32]].into_iter().collect();
    let on_disk =
        |name: String| allocated_bytes(&std::fs::metadata(dir.join(name)).expect("metadata"));
    let expected = on_disk(format!("{}.partial", hex(3)))
        + on_disk(format!("{}.partial.ranges", hex(3)))
        + on_disk(hex(4));
    let leftovers = leftover_partials(tmp.path(), &keep);
    assert_eq!(
        leftovers,
        Leftovers {
            blobs: 2,
            bytes: expected,
        }
    );
    assert_eq!(
        leftover_warning(
            Path::new("/out/.decdn-partial"),
            &Leftovers {
                blobs: 2,
                bytes: 1210,
            }
        )
        .as_deref(),
        Some(
            "2 partial downloads (1.2 KB on disk) that this run did not use remain in \
             /out/.decdn-partial; delete them to reclaim the space"
        )
    );
    assert_eq!(
        leftover_warning(
            Path::new("/out/.decdn-partial"),
            &Leftovers {
                blobs: 1,
                bytes: 1_500_000_000,
            }
        )
        .as_deref(),
        Some(
            "1 partial download (1.5 GB on disk) that this run did not use remains in \
             /out/.decdn-partial; delete it to reclaim the space"
        )
    );
    assert_eq!(
        leftover_warning(Path::new("/out"), &Leftovers::default()),
        None
    );
}

/// A range-dedup `.partial` sized to the whole blob before its bytes land
/// counts only the space it takes, not its length (#2190).
#[cfg(unix)]
#[test]
fn leftover_partials_count_a_sparse_partial_by_its_allocated_space() {
    let tmp = tempfile::tempdir().expect("tmp");
    let dir = tmp.path().join(STAGING_DIR);
    std::fs::create_dir_all(&dir).expect("staging dir");
    let hex = blake3::Hash::from_bytes([9; 32]).to_hex().to_string();
    let file = std::fs::File::create(dir.join(format!("{hex}.partial"))).expect("create");
    file.set_len(64 * 1024 * 1024).expect("size the partial");
    let leftovers = leftover_partials(tmp.path(), &HashSet::new());
    assert_eq!(leftovers.blobs, 1);
    assert!(
        leftovers.bytes < 1024 * 1024,
        "a hole-only partial takes almost no space: {}",
        leftovers.bytes
    );
}

/// A malformed glob is a hard error naming the flag it came from.
#[test]
fn entry_filter_rejects_bad_glob() {
    let err = EntryFilter::compile(None, &strs(&["["]), &[]).unwrap_err();
    assert!(
        format!("{err:#}").contains("--include"),
        "error should name the offending flag: {err:#}"
    );
}

/// A linked duplicate is counted separately and never fails the pull.
#[test]
fn report_counts_linked_without_failing() {
    let rep = report_of(1, 1, 1, &[]);
    assert!(report(&rep, 0, Path::new("/out"), false).is_ok());
}

/// `--dry-run` must still enforce the flag-combination rule: a dangling
/// `--provider-address` (no `--node-id`) is rejected even for `--dry-run`,
/// which does not run at parse time — so `bundle_pull` calls `validate()`
/// BEFORE the dry-run short-circuit. Asserted end-to-end: the command
/// returns the guard error (before any network/chain I/O, since
/// `validate()` fails first).
#[tokio::test]
async fn dry_run_still_rejects_a_dangling_provider_address() {
    let addr = "0x0000000000000000000000000000000000000001";
    let uri = format!("decdn:{}/", "ab".repeat(32));
    let (args, uri) = get_args(
        &uri,
        &["-o", "out", "--dry-run", "--provider-address", addr],
    );
    let err = super::bundle_pull(&args, &Destination::of(&args, &uri), &uri, None)
        .await
        .expect_err("a dangling --provider-address must be rejected even for --dry-run");
    assert!(
        format!("{err:#}").contains("--provider-address requires --node-id"),
        "expected the validate() guard error, got: {err:#}"
    );
}

/// `--dry-run` reads the bundle's cached manifest, even under `--overwrite`,
/// so the entry selection runs against its entries (#2327): an entry URI
/// naming a cached entry plans it, and one naming no entry fails.
#[tokio::test]
async fn dry_run_reads_a_cached_manifest() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path().to_str().expect("utf-8 tmp path");
    let hash = cache_manifest(
        tmp.path(),
        &serde_json::json!({
            "version": 1,
            "entries": [
                {"path": "openai-gpt-oss-20b/config.json", "hash": "b3:aa", "size": 10},
                {"path": "openai-gpt-oss-20b/metal/model.bin", "hash": "b3:bb", "size": 20},
            ],
        }),
    );
    for overwrite in [false, true] {
        let mut extra = vec!["-o", out, "--dry-run"];
        if overwrite {
            extra.push("--overwrite");
        }
        let (args, uri) = get_args(
            &format!("decdn:{hash}/openai-gpt-oss-20b/config.json"),
            &extra,
        );
        super::bundle_pull(&args, &Destination::of(&args, &uri), &uri, None)
            .await
            .unwrap_or_else(|e| panic!("overwrite={overwrite}: {e:#}"));

        let (args, uri) = get_args(&format!("decdn:{hash}/openai-gpt-oss-20b/no.json"), &extra);
        let err = super::bundle_pull(&args, &Destination::of(&args, &uri), &uri, None)
            .await
            .expect_err("the cached manifest has no such entry");
        assert!(format!("{err:#}").contains("no entry"), "{err:#}");
    }
}

/// A dry run with no cached manifest has no entries to list, and still
/// finishes with no endpoint, chain, or keystore.
#[tokio::test]
async fn dry_run_has_no_manifest_for_an_uncached_hash() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path().to_str().expect("utf-8 tmp path");
    let (args, uri) = get_args(
        &format!("decdn:{}/", "ab".repeat(32)),
        &["-o", out, "--dry-run"],
    );
    super::bundle_pull(&args, &Destination::of(&args, &uri), &uri, None)
        .await
        .unwrap();
}

/// The full dry run reads the cached manifest: a cached blob that hashes to
/// the bundle but is not a v1 manifest fails the dry run, naming the cache
/// file and the drop-the-trailing-`/` hint, as it would fail a real pull.
#[tokio::test]
async fn dry_run_fails_on_a_cached_manifest_that_does_not_parse() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path().to_str().expect("utf-8 tmp path");
    let hash = cache_manifest(
        tmp.path(),
        &serde_json::json!({"version": 2, "entries": []}),
    );
    let (args, uri) = get_args(&format!("decdn:{hash}/"), &["-o", out, "--dry-run"]);
    let err = super::bundle_pull(&args, &Destination::of(&args, &uri), &uri, None)
        .await
        .expect_err("a cached non-v1 manifest fails the dry run");
    let msg = format!("{err:#}");
    assert!(msg.contains("unsupported bundle version 2"), "{msg}");
    assert!(msg.contains(".decdn-bundles"), "{msg}");
    assert!(msg.contains("drop the trailing `/`"), "{msg}");
}

/// Only a blob that is not a manifest gets the drop-the-trailing-`/` hint.
#[test]
fn only_a_non_manifest_gets_the_trailing_slash_hint() {
    let not_manifest = BundleManifest::parse(b"plain bytes").unwrap_err();
    let hinted = format!("{:#}", not_a_manifest_hint(not_manifest));
    assert!(hinted.contains("drop the trailing `/`"), "{hinted}");

    let other = format!("{:#}", not_a_manifest_hint(anyhow!("no holder answered")));
    assert_eq!(other, "no holder answered");
}

/// A cached manifest file that exists but cannot be read fails the dry run,
/// naming the file and the OS error (#2361).
#[tokio::test]
async fn dry_run_fails_naming_an_unreadable_cached_manifest() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path().to_str().expect("utf-8 tmp path");
    let raw = [0xab_u8; 32];
    let hex = blake3::Hash::from_bytes(raw).to_hex();
    // A directory at the cache path is unreadable as a file, even as root.
    std::fs::create_dir_all(BundleManifest::cache_path(tmp.path(), raw)).expect("mkdir");
    let (args, uri) = get_args(&format!("decdn:{hex}/"), &["-o", out, "--dry-run"]);
    let err = super::bundle_pull(&args, &Destination::of(&args, &uri), &uri, None)
        .await
        .expect_err("an unreadable cached manifest fails the dry run");
    let msg = format!("{err:#}");
    assert!(msg.contains(".decdn-bundles"), "{msg}");
    assert!(msg.contains(hex.as_str()), "{msg}");
    assert!(msg.contains("os error"), "{msg}");
}

/// A cached manifest file whose bytes fail the hash check fails the dry run,
/// naming the file, so its filters are never reported as unchecked for want of
/// a cache (#2361).
#[tokio::test]
async fn dry_run_fails_naming_a_cached_manifest_that_fails_its_hash_check() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path().to_str().expect("utf-8 tmp path");
    let hash = cache_manifest(
        tmp.path(),
        &serde_json::json!({"version": 1, "entries": []}),
    );
    let raw = fetch::parse_hash(&hash).expect("hash");
    std::fs::write(BundleManifest::cache_path(tmp.path(), raw), b"tampered").expect("tamper");
    let (args, uri) = get_args(&format!("decdn:{hash}/"), &["-o", out, "--dry-run"]);
    let err = super::bundle_pull(&args, &Destination::of(&args, &uri), &uri, None)
        .await
        .expect_err("a tampered cached manifest fails the dry run");
    let msg = format!("{err:#}");
    assert!(msg.contains("does not hash to the bundle"), "{msg}");
    assert!(msg.contains(".decdn-bundles"), "{msg}");
}

/// A plain entry's line keeps its one rate, taken from what it downloaded.
#[test]
fn entry_done_line_rates_a_plain_entry_on_its_download() {
    let line = entry_done_line(
        [1; 32],
        4 << 20,
        EntryBytes {
            paid: 4 << 20,
            spliced: 0,
            resumed: 0,
        },
        std::time::Duration::from_secs(2),
    );
    assert!(line.ends_with(": 4.00 MiB in 2.0s (2.00 MiB/s)"), "{line}");
}

/// A range-dedup entry's rate counts only its downloaded bytes, and its
/// spliced bytes are reported apart (#2189).
#[test]
fn entry_done_line_splits_a_dedup_entrys_spliced_bytes() {
    let line = entry_done_line(
        [1; 32],
        12 << 20,
        EntryBytes {
            paid: 2 << 20,
            spliced: 10 << 20,
            resumed: 0,
        },
        std::time::Duration::from_secs(2),
    );
    assert!(
        line.ends_with(
            ": 12.00 MiB in 2.0s (2.00 MiB downloaded at 1.00 MiB/s, 10.00 MiB spliced \
             from disk)"
        ),
        "{line}"
    );
}

/// An entry spliced whole from disk downloads nothing and has no rate.
#[test]
fn entry_done_line_renders_a_fully_spliced_entry() {
    let line = entry_done_line(
        [1; 32],
        12 << 20,
        EntryBytes {
            paid: 0,
            spliced: 12 << 20,
            resumed: 0,
        },
        std::time::Duration::from_secs(2),
    );
    assert!(
        line.ends_with(": 12.00 MiB in 2.0s (0 B downloaded at --, 12.00 MiB spliced from disk)"),
        "{line}"
    );
}

/// A resumed entry's rate counts only what this run downloaded, and its
/// resumed and spliced bytes are reported apart (#2236).
#[test]
fn entry_done_line_splits_a_resumed_entrys_bytes() {
    let line = entry_done_line(
        [1; 32],
        12 << 20,
        EntryBytes {
            paid: 2 << 20,
            spliced: 6 << 20,
            resumed: 4 << 20,
        },
        std::time::Duration::from_secs(2),
    );
    assert!(
        line.ends_with(
            ": 12.00 MiB in 2.0s (2.00 MiB downloaded at 1.00 MiB/s, 4.00 MiB resumed, \
             6.00 MiB spliced from disk)"
        ),
        "{line}"
    );

    let line = entry_done_line(
        [1; 32],
        6 << 20,
        EntryBytes {
            paid: 2 << 20,
            spliced: 0,
            resumed: 4 << 20,
        },
        std::time::Duration::from_secs(2),
    );
    assert!(
        line.ends_with(": 6.00 MiB in 2.0s (2.00 MiB downloaded at 1.00 MiB/s, 4.00 MiB resumed)"),
        "{line}"
    );
}

/// An entry that took no measurable time renders the placeholder rate.
#[test]
fn entry_done_line_guards_a_zero_elapsed() {
    let line = entry_done_line(
        [1; 32],
        1 << 20,
        EntryBytes {
            paid: 1 << 20,
            spliced: 0,
            resumed: 0,
        },
        std::time::Duration::ZERO,
    );
    assert!(line.ends_with(": 1.00 MiB in 0.0s (--)"), "{line}");
}

/// No flags => every entry passes, in manifest order.
#[test]
fn entry_filter_passthrough_when_no_flags() {
    assert_eq!(
        filtered_paths(&[], &[], sample()),
        vec![
            "models/a.bin",
            "models/b.txt",
            "docs/readme.md",
            "docs/deep/notes.txt"
        ]
    );
}

/// `--include` is a whitelist gate: absent it opens, present an entry must
/// match at least one pattern. `*` does not cross `/`, so `models/*` keeps
/// only the direct children of `models/`.
#[test]
fn entry_filter_include_is_a_whitelist_gate() {
    assert_eq!(
        filtered_paths(&["models/*"], &[], sample()),
        vec!["models/a.bin", "models/b.txt"]
    );
}

/// Multiple `--include` patterns are OR-ed.
#[test]
fn entry_filter_includes_are_ored() {
    assert_eq!(
        filtered_paths(&["models/*.bin", "docs/readme.md"], &[], sample()),
        vec!["models/a.bin", "docs/readme.md"]
    );
}

/// `--exclude` drops matches; multiple patterns are OR-ed. A leading `**/`
/// is what makes a suffix glob match at any depth — a bare `*.txt` matches
/// only a root-level file, since `*` never crosses `/`.
#[test]
fn entry_filter_excludes_are_ored() {
    assert_eq!(
        filtered_paths(&[], &["**/*.txt", "**/*.md"], sample()),
        vec!["models/a.bin"]
    );
}

/// A bare `*.txt` does NOT cross `/`, so it leaves nested `.txt` entries in
/// place — the same gitignore separator rule `origin import --exclude` uses.
#[test]
fn entry_filter_star_does_not_cross_slash() {
    assert_eq!(
        filtered_paths(&[], &["*.txt"], sample()),
        vec![
            "models/a.bin",
            "models/b.txt",
            "docs/readme.md",
            "docs/deep/notes.txt"
        ]
    );
}

/// `--exclude` wins over `--include`: an entry matching both is dropped.
#[test]
fn entry_filter_exclude_beats_include() {
    assert_eq!(
        filtered_paths(&["models/*"], &["**/*.txt"], sample()),
        vec!["models/a.bin"]
    );
}

/// `**` recurses across `/` where a one-level `*` does not.
#[test]
fn entry_filter_double_star_recurses() {
    assert_eq!(
        filtered_paths(&["docs/**"], &[], sample()),
        vec!["docs/readme.md", "docs/deep/notes.txt"]
    );
    assert_eq!(
        filtered_paths(&["docs/*"], &[], sample()),
        vec!["docs/readme.md"]
    );
}

/// A filter that matches nothing yields an empty set (the caller then
/// reports "no entries match"), never an error.
#[test]
fn entry_filter_can_empty_the_set() {
    assert!(filtered_paths(&["no/such/*"], &[], sample()).is_empty());
}

#[test]
fn a_flattened_entry_filter_keeps_the_file_name_only() {
    let mut filter = EntryFilter::compile(Some("models/v1/c.bin".to_owned()), &[], &[]).unwrap();
    filter.flatten = true;
    let entries = vec![sel_entry("models/v1/c.bin", 3), sel_entry("d.bin", 4)];
    let (kept, excluded) = filter.apply_and_warn(entries).unwrap();
    let paths: Vec<&str> = kept.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, ["c.bin"]);
    assert_eq!(excluded, 1);
}
