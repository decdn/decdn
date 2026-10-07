use super::*;

#[test]
fn file_label_single_path_is_the_path() {
    assert_eq!(file_label(&["models/a.bin".to_string()]), "models/a.bin");
}

#[test]
fn file_label_multi_path_appends_more_count() {
    let paths = [
        "models/a.bin".to_string(),
        "b.bin".to_string(),
        "c.bin".to_string(),
    ];
    assert_eq!(file_label(&paths), "models/a.bin (+2 more)");
}

#[test]
fn file_label_empty_is_placeholder() {
    assert_eq!(file_label(&[]), "(entry)");
}

/// A hidden total bar of fixed content length `len`, with its own rate meter.
fn hidden_total(len: u64) -> TotalBar {
    TotalBar::wrap(
        indicatif::ProgressBar::with_draw_target(
            Some(len),
            indicatif::ProgressDrawTarget::hidden(),
        ),
        None,
    )
}

#[test]
fn disabled_file_bar_has_no_callback() {
    let fb = FileBar::disabled();
    assert!(fb.callback().is_none());
}

/// A hidden [`PullProgress`] whose total bar denominates the run's download. The
/// total bar is added to the container so a per-file `insert_before` has an
/// anchor, exactly as production builds it.
fn hidden_pp(download_total: u64) -> (PullProgress, TotalBar) {
    let mp = indicatif::MultiProgress::with_draw_target(indicatif::ProgressDrawTarget::hidden());
    let total = TotalBar::wrap(mp.add(indicatif::ProgressBar::new(download_total)), None);
    let pp = PullProgress {
        inner: Some(Inner {
            mp,
            total: Some(total.clone()),
            tab: None,
        }),
    };
    (pp, total)
}

#[test]
fn file_bar_download_folds_into_the_total_capped_at_download_bytes() {
    // A blob that downloads 1000 and reconstructs 500 from disk. Its delivery
    // callback advances the total's DOWNLOAD meter by the delivered bytes only,
    // capped at the download size — a splice never inflates the download total.
    let (pp, total) = hidden_pp(1000);
    let fb = pp.file_bar("m", 1000, 500);
    let cb = fb.callback().expect("enabled");
    cb(400, 9999);
    assert_eq!(total.position(), 400);
    cb(1000, 9999);
    assert_eq!(total.position(), 1000);
    // Delivery past the download size (should not happen, but be safe) does not
    // push the total past the download denominator.
    cb(1200, 9999);
    assert_eq!(total.position(), 1000);
}

#[test]
fn unsized_file_bar_shows_stream_length_but_adds_nothing_to_the_total() {
    // An entry the manifest declares no size for has download_total 0. Its bar
    // borrows the stream's `expected` for display so the row still moves, but it
    // contributes nothing to the total bar's download meter.
    let (pp, total) = hidden_pp(1000);
    let fb = pp.file_bar("m", 0, 0);
    let cb = fb.callback().expect("enabled");
    cb(500, 2000);
    let bar = &fb.phase.as_ref().expect("enabled").bar;
    assert_eq!(bar.length(), Some(2000), "borrows the stream length");
    assert_eq!(bar.position(), 500);
    assert_eq!(
        total.position(),
        0,
        "an unsized entry adds nothing to the total"
    );
    cb(1500, 2000);
    assert_eq!(bar.position(), 1500);
    assert_eq!(total.position(), 0);
}

#[test]
fn file_bar_message_shows_download_counts_and_reconstruct_size() {
    let (pp, _total) = hidden_pp(1000);
    let fb = pp.file_bar("m", 1000, 500);
    let bar = &fb.phase.as_ref().expect("enabled").bar;
    fb.callback().expect("enabled")(400, 0);
    let msg = bar.message();
    // "<downloaded>/<download-total> (<reconstruct>)".
    assert!(msg.contains('/'), "{msg}");
    assert!(msg.contains('('), "{msg}");
}

#[test]
fn file_bar_reconstructing_spans_the_whole_blob_and_tracks_hashed_bytes() {
    let (pp, _total) = hidden_pp(1000);
    let fb = pp.file_bar("m", 1000, 500);
    let reporter = fb.reconstruct_reporter().expect("enabled");
    let bar = &fb.phase.as_ref().expect("enabled").bar;
    // The bar now spans the whole blob (download + reconstruct) and fills with
    // bytes hashed, so a multi-GB verify is never a frozen row.
    assert_eq!(bar.length(), Some(1500));
    reporter(750);
    assert_eq!(bar.position(), 750);
    assert!(
        bar.message().contains("reconstructing"),
        "{}",
        bar.message()
    );
}

#[test]
fn file_bar_pending_marks_the_download_done_and_waiting() {
    let (pp, _total) = hidden_pp(1000);
    let fb = pp.file_bar("m", 1000, 500);
    fb.set_pending();
    let bar = &fb.phase.as_ref().expect("enabled").bar;
    assert_eq!(bar.position(), 1000, "download is complete while pending");
    assert!(
        bar.message().contains("pending siblings"),
        "{}",
        bar.message()
    );
}

#[test]
fn total_bar_inc_writes_rate_and_eta_message() {
    let total = hidden_total(1000);
    assert_eq!(total.bar.message(), "", "nothing until a byte moves");
    total.inc(100);
    assert_eq!(total.position(), 100);
    let msg = total.bar.message();
    assert!(msg.starts_with('('), "{msg}");
    assert!(msg.contains("ETA"), "{msg}");
}

#[test]
fn dropping_the_renderer_clears_the_tab_while_a_callback_lives() {
    let (tab, log) = TabProgress::recorded(true);
    let (mut pp, _total) = hidden_pp(1000);
    if let Some(i) = pp.inner.as_mut() {
        i.tab = Some(Arc::clone(&tab));
    }
    // A per-file callback outlives the renderer, still holding the tab.
    let late = Arc::clone(&tab);
    drop(pp);
    assert_eq!(log.lock().unwrap().last().unwrap(), "\x1b]9;4;0;\x1b\\");
    late.update(500, 1000);
    assert_eq!(
        log.lock().unwrap().last().unwrap(),
        "\x1b]9;4;0;\x1b\\",
        "an update after the clear writes nothing"
    );
}

#[test]
fn total_bar_inc_drives_the_tab_percent() {
    let (tab, log) = TabProgress::recorded(true);
    let total = TotalBar::wrap(
        indicatif::ProgressBar::with_draw_target(
            Some(1000),
            indicatif::ProgressDrawTarget::hidden(),
        ),
        Some(tab),
    );
    total.inc(250);
    assert_eq!(log.lock().unwrap().last().unwrap(), "\x1b]9;4;1;25\x1b\\");
}
