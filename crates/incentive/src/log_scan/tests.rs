use super::*;

/// Walk `[from, to]` the way the poller does with a fixed span.
fn windows(from: u64, to: u64, span: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut start = from;
    while start <= to {
        let end = window_end(start, to, span);
        out.push((start, end));
        start = end + 1;
    }
    out
}

#[test]
fn window_end_splits_the_range() {
    // Exact multiple: two full windows.
    assert_eq!(windows(0, 19, 10), vec![(0, 9), (10, 19)]);
    // Remainder: a short final window.
    assert_eq!(windows(0, 25, 10), vec![(0, 9), (10, 19), (20, 25)]);
    // Single block and single-window-fits cases.
    assert_eq!(windows(1_000, 1_000, 10), vec![(1_000, 1_000)]);
    assert_eq!(windows(1_000, 1_005, 10), vec![(1_000, 1_005)]);
    // Non-zero start offset: windows stay aligned to `from`, not to 0.
    assert_eq!(windows(100, 119, 10), vec![(100, 109), (110, 119)]);
    // A zero span is a one-block window, never an empty scan.
    assert_eq!(window_end(7, 100, 0), 7);
    // A span near u64::MAX saturates instead of overflowing.
    assert_eq!(window_end(5, 9, u64::MAX), 9);
}

#[test]
fn halved_span_halves_the_window_length() {
    assert_eq!(halved_span(1, 20), Some(10), "20 blocks -> 10");
    assert_eq!(halved_span(0, 20), Some(10), "21 blocks -> 10");
    assert_eq!(halved_span(5, 7), Some(1), "3 blocks -> 1");
    assert_eq!(halved_span(5, 6), Some(1), "2 blocks -> 1");
    assert_eq!(halved_span(9, 9), None, "a one-block window cannot shrink");
    assert_eq!(
        halved_span(0, u64::MAX),
        Some(1 << 63),
        "the full u64 range does not overflow"
    );
}

fn span(ceiling: u64) -> WindowSpan {
    WindowSpan::new(NonZeroU64::new(ceiling).unwrap_or(NonZeroU64::MIN))
}

/// Record `n` accepted windows; the last regrow, if any.
fn accept(span: &mut WindowSpan, n: u32) -> Option<u64> {
    (0..n).filter_map(|_| span.record_success()).last()
}

#[test]
fn window_span_shrinks_to_half_the_rejected_window() {
    let mut span = span(10_000);
    // A 30-block tail window is rejected: 15, not 5 000.
    assert_eq!(
        span.shrink(100, 129),
        Some(Shrunk {
            span: 15,
            rejected_probe: false
        })
    );
    assert_eq!(span.current(), 15);
    // A one-block window cannot shrink and leaves the span alone.
    assert_eq!(span.shrink(7, 7), None);
    assert_eq!(span.current(), 15);
}

#[test]
fn window_span_never_shrinks_above_the_current_span() {
    let mut span = span(100);
    assert!(span.shrink(0, 19).is_some(), "20 blocks -> 10");
    // A caller passing a window wider than the span cannot grow it.
    assert_eq!(span.shrink(0, 99).map(|s| s.span), Some(10));
    assert_eq!(span.current(), 10);
}

#[test]
fn window_span_regrows_after_a_streak_up_to_the_ceiling() {
    let mut span = span(100);
    assert!(span.shrink(0, 99).is_some(), "100 blocks -> 50");
    assert_eq!(accept(&mut span, SPAN_REGROW_AFTER_WINDOWS - 1), None);
    assert_eq!(accept(&mut span, 1), Some(100));
    // At the ceiling it never grows further.
    assert_eq!(accept(&mut span, SPAN_REGROW_AFTER_WINDOWS * 2), None);
    assert_eq!(span.current(), 100);
}

#[test]
fn window_span_climbs_back_to_the_ceiling_on_short_windows() {
    // A transient rejection of a 30-block live-tail window drops the span to
    // 15; short accepted windows keep counting, so it climbs all the way back.
    let mut span = span(10_000);
    assert!(span.shrink(0, 29).is_some());
    let mut last = None;
    for _ in 0..20 {
        last = accept(&mut span, SPAN_REGROW_AFTER_WINDOWS).or(last);
    }
    assert_eq!(last, Some(10_000));
    assert_eq!(span.current(), 10_000);
}

#[test]
fn window_span_flags_a_rejected_regrow_probe() {
    let mut span = span(200);
    let first = span.shrink(0, 199);
    assert_eq!(
        first.map(|s| (s.span, s.rejected_probe)),
        Some((100, false))
    );
    assert_eq!(accept(&mut span, SPAN_REGROW_AFTER_WINDOWS), Some(200));
    // The doubled span is rejected: back to 100, a rejected probe.
    let probe = span.shrink(0, 199);
    assert_eq!(probe.map(|s| (s.span, s.rejected_probe)), Some((100, true)));
    // A further shrink below the probe's start is news again.
    let deeper = span.shrink(0, 99);
    assert_eq!(
        deeper.map(|s| (s.span, s.rejected_probe)),
        Some((50, false))
    );
}

#[test]
fn window_span_rejection_resets_the_streak() {
    let mut span = span(200);
    assert!(span.shrink(0, 199).is_some());
    assert_eq!(accept(&mut span, SPAN_REGROW_AFTER_WINDOWS - 1), None);
    assert!(span.shrink(0, 99).is_some(), "100 -> 50, streak reset");
    assert_eq!(accept(&mut span, SPAN_REGROW_AFTER_WINDOWS - 1), None);
    assert_eq!(accept(&mut span, 1), Some(100));
}

#[test]
fn windows_cover_a_large_gap_without_overlap_or_gap() {
    let (from, to, span) = (1_000u64, 55_321u64, 10_000u64);
    let windows = windows(from, to, span);
    let mut expected_next = from;
    for (start, end) in &windows {
        assert_eq!(*start, expected_next, "windows must be contiguous");
        assert!(end >= start, "window end >= start");
        // Written as `< span` to avoid clippy's int_plus_one.
        assert!(
            end - start < span,
            "window span {} exceeds cap {span}",
            end - start + 1
        );
        expected_next = end.saturating_add(1);
    }
    assert_eq!(
        windows.last().map(|w| w.1),
        Some(to),
        "the final window must reach `to`"
    );
}

mod scan {
    use alloy::primitives::Address;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::types::Log;

    use super::*;

    fn ceiling(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
    }

    #[tokio::test]
    async fn scan_logs_halves_a_rejected_window_and_covers_the_range_once() {
        let asserter = Asserter::new();
        // The 10-block ceiling is over the provider's cap: one rejection,
        // then every 5-block window is accepted.
        asserter.push_failure_msg("ranges over 5 blocks are not supported");
        for _ in 0..6 {
            asserter.push_success(&Vec::<Log>::new());
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let filter = Filter::new().address(Address::repeat_byte(0x11));
        let mut ends = Vec::new();
        let logs = scan_logs(&provider, &filter, 0, 29, ceiling(10), |event| {
            if let ScanEvent::Window { end, to } = event {
                ends.push((end, to));
            }
        })
        .await
        .expect("scan");
        assert!(logs.is_empty());
        assert_eq!(
            ends,
            vec![(4, 29), (9, 29), (14, 29), (19, 29), (24, 29), (29, 29)],
            "contiguous 5-block windows after the shrink, the last clamped to `to`"
        );
    }

    #[tokio::test]
    async fn scan_logs_fails_on_a_rejected_one_block_window() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("ranges over 0 blocks are not supported");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let filter = Filter::new().address(Address::repeat_byte(0x11));
        let err = scan_logs(&provider, &filter, 7, 7, ceiling(10), |_| {})
            .await
            .expect_err("a one-block rejection cannot shrink");
        assert!(
            format!("{err:#}").contains("one-block"),
            "names the unshrinkable window: {err:#}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn scan_logs_retries_a_transient_failure_then_gives_up_naming_the_window() {
        let asserter = Asserter::new();
        // One failure more than the retries: every attempt on [0, 9] fails.
        for _ in 0..=SCAN_WINDOW_RETRIES {
            asserter.push_failure_msg("internal error");
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let filter = Filter::new().address(Address::repeat_byte(0x11));
        let mut retries = Vec::new();
        let err = scan_logs(&provider, &filter, 0, 99, ceiling(10), |event| {
            if let ScanEvent::Retry { attempt, .. } = event {
                retries.push(attempt);
            }
        })
        .await
        .expect_err("not a range rejection");
        let text = format!("{err:#}");
        assert!(text.contains("eth_getLogs [0, 9]"), "{text}");
        assert!(text.contains("blocks below 0 were scanned"), "{text}");
        assert_eq!(retries, vec![1, 2, 3], "every retry is reported");
    }

    #[tokio::test(start_paused = true)]
    async fn scan_logs_recovers_from_a_transient_failure() {
        let asserter = Asserter::new();
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_failure_msg("request timeout");
        asserter.push_success(&Vec::<Log>::new());
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let filter = Filter::new().address(Address::repeat_byte(0x11));
        let mut ends = Vec::new();
        scan_logs(&provider, &filter, 0, 19, ceiling(10), |event| {
            if let ScanEvent::Window { end, .. } = event {
                ends.push(end);
            }
        })
        .await
        .expect("the second window succeeds on retry");
        assert_eq!(ends, vec![9, 19]);
    }

    #[tokio::test]
    async fn scan_logs_of_an_empty_range_makes_no_request() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let filter = Filter::new().address(Address::repeat_byte(0x11));
        let logs = scan_logs(&provider, &filter, 10, 9, ceiling(10), |_| {
            panic!("no window");
        })
        .await
        .expect("empty scan");
        assert!(logs.is_empty());
    }
}
