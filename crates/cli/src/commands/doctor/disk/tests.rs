use super::*;
use crate::commands::doctor::Severity;

const GIB: u64 = 1024 * 1024 * 1024;

#[test]
fn budget_larger_than_volume_is_fine_now() {
    // A large budget is the intended way to use the disk-aware ceiling: the
    // clamp sizes the cache to free disk. 100 GiB budget on a 50 GiB volume
    // with 40 GiB free and 8 GiB headroom leaves 32 GiB usable => Pass.
    let f = evaluate_disk(100 * GIB, 8 * GIB, 50 * GIB, 40 * GIB, 0);
    assert_eq!(f.severity, Severity::Pass);
    assert_eq!(f.id, "disk.budget_vs_free");
}

#[test]
fn headroom_exceeds_whole_volume_warns_not_fails() {
    // 60 GiB headroom on a 50 GiB volume can never be satisfied, but the node
    // still runs => Warn, never Fail (doctor must not hard-fail on disk).
    let f = evaluate_disk(100 * GIB, 60 * GIB, 50 * GIB, 40 * GIB, 0);
    assert_eq!(f.severity, Severity::Warn);
    assert!(f.remediation.is_some());
}

#[test]
fn disk_findings_never_fail_even_when_headroom_dwarfs_the_volume() {
    // The whole point of the Warn-only model: no combination of budget,
    // headroom, and free disk yields a Fail, so doctor's exit code never
    // flips on the runner's disk size.
    for (budget, headroom, total, avail) in [
        (1 * GIB, 8 * GIB, 4 * GIB, 3 * GIB), // headroom > small volume
        (100 * GIB, 100 * GIB, 1 * GIB, 0),   // headroom == whole volume, no free
        (1 * GIB, 0, 1 * GIB, 0),             // no free disk at all
    ] {
        let f = evaluate_disk(budget, headroom, total, avail, 0);
        assert_ne!(f.severity, Severity::Fail, "{}", f.title);
    }
}

#[test]
fn headroom_above_free_disk_only_warns() {
    // 8 GiB headroom, only 3 GiB free, on a 50 GiB volume: no room right now
    // but the volume is large enough in principle => Warn, never Fail. This
    // is the case that must not hard-fail doctor on a small-disk host.
    let f = evaluate_disk(100 * GIB, 8 * GIB, 50 * GIB, 3 * GIB, 0);
    assert_eq!(f.severity, Severity::Warn);
}

#[test]
fn healthy_headroom_passes() {
    // 40 GiB free, 8 GiB headroom on a 50 GiB volume => 32 GiB usable, well
    // above a tenth of the volume => Pass.
    let f = evaluate_disk(10 * GIB, 8 * GIB, 50 * GIB, 40 * GIB, 0);
    assert_eq!(f.severity, Severity::Pass);
}

#[test]
fn little_room_above_headroom_warns() {
    // 9 GiB free, 8 GiB headroom on a 50 GiB volume => 1 GiB usable, under a
    // tenth of the 50 GiB volume => Warn (some room, but almost none).
    let f = evaluate_disk(100 * GIB, 8 * GIB, 50 * GIB, 9 * GIB, 0);
    assert_eq!(f.severity, Severity::Warn);
}
