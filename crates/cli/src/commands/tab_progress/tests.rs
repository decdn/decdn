use super::*;

#[test]
fn writes_each_new_percent_once_and_never_backwards() {
    let (tab, log) = TabProgress::recorded(true);
    tab.update(10, 1000); // 1%
    tab.update(19, 1000); // still 1%
    tab.update(500, 1000); // 50%
    tab.update(400, 1000); // behind: a racing thread's stale sample
    tab.update(2000, 1000); // past the end caps at 100%
    assert_eq!(
        *log.lock().unwrap(),
        [
            "\x1b]9;4;1;0\x1b\\",
            "\x1b]9;4;1;1\x1b\\",
            "\x1b]9;4;1;50\x1b\\",
            "\x1b]9;4;1;100\x1b\\",
        ]
    );
}

#[test]
fn a_zero_length_reads_as_done() {
    let (tab, log) = TabProgress::recorded(true);
    tab.update(0, 0);
    assert_eq!(log.lock().unwrap().last().unwrap(), "\x1b]9;4;1;100\x1b\\");
}

#[test]
fn clear_removes_once_and_stops_updates() {
    let (tab, log) = TabProgress::recorded(true);
    tab.clear();
    tab.update(500, 1000);
    drop(tab);
    assert_eq!(
        *log.lock().unwrap(),
        ["\x1b]9;4;1;0\x1b\\", "\x1b]9;4;0;\x1b\\"]
    );
}

#[test]
fn dropping_removes_the_indicator() {
    // An early return drops the indicator without a `clear`; the tab must not
    // keep showing a stale percent.
    let (tab, log) = TabProgress::recorded(false);
    drop(tab);
    assert_eq!(
        *log.lock().unwrap(),
        ["\x1b]9;4;3;\x1b\\", "\x1b]9;4;0;\x1b\\"]
    );
}
