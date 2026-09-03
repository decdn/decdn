//! Disk & cache-budget group. The eviction driver sizes the cache to real free
//! disk: each tick it clamps the effective ceiling to
//! `min(cache_size_mb, footprint + max(0, free - disk_headroom_mb))`, keeping
//! `disk_headroom_mb` of the `cache_dir` volume free (ADR 040 §Free-disk-aware
//! ceiling, #1930). So `cache_size_mb` is an upper bound and a large value is
//! expected — free disk, not the budget, normally binds. This group is the
//! pre-boot advisory: it checks the volume and `disk_headroom_mb` leave room for
//! a useful cache before the node starts and the reactive clamp takes over.

use std::path::Path;

use decdn_common::config::ResolvedConfig;
use decdn_common::disk::statvfs_target;

use super::{Finding, Report, Severity};

const BYTES_PER_MB: u64 = 1024 * 1024;

// Precision loss above 2^52 bytes (~4 PiB) is immaterial: this only feeds a
// human-readable GiB figure in report text, never a comparison or decision.
#[allow(clippy::cast_precision_loss)]
fn gib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0 * 1024.0)
}

/// Pure headroom-vs-disk evaluation for the disk-aware ceiling (#1930).
/// `current_footprint` is the live `decdn_cache_bytes` when known, else 0.
/// `reachable = fs_avail + current_footprint` is the space the cache could
/// occupy; the driver keeps `headroom_bytes` of it free, so the cache can grow
/// into at most `reachable - headroom_bytes` before the `cache_size_mb` upper
/// bound. A large `budget_bytes` is expected and never a failure on its own —
/// the disk clamp handles it. The real misconfiguration is a `disk_headroom_mb`
/// that leaves the cache little or no room on the `cache_dir` volume.
pub(crate) fn evaluate_disk(
    budget_bytes: u64,
    headroom_bytes: u64,
    fs_total: u64,
    fs_avail: u64,
    current_footprint: u64,
) -> Finding {
    let reachable = fs_avail.saturating_add(current_footprint);
    let usable = reachable.saturating_sub(headroom_bytes);
    let effective = usable.min(budget_bytes);

    let base = |severity, title: String, detail: String, remediation: Option<String>| Finding {
        group: "Disk & cache",
        id: "disk.budget_vs_free",
        severity,
        title,
        detail: Some(detail),
        remediation,
    };
    let data = format!(
        "cache_size_mib={} disk_headroom_gib={:.1} effective_ceiling_gib={:.1} free_gib={:.1} vol_total_gib={:.1}",
        budget_bytes / BYTES_PER_MB,
        gib(headroom_bytes),
        gib(effective),
        gib(fs_avail),
        gib(fs_total),
    );

    // Headroom at or above the reachable space collapses the effective ceiling
    // to (at most) the current footprint: the cache can never grow, and the
    // driver evicts constantly. That is a misconfiguration, not disk pressure.
    if headroom_bytes >= reachable {
        return base(
            Severity::Fail,
            "disk headroom leaves no room for the cache".into(),
            data,
            Some(format!(
                "cache.disk_headroom_mb ({} MiB) is at least the space on the cache_dir volume \
                 (~{:.0} MiB reachable); lower it or move cache.cache_dir to a larger volume",
                headroom_bytes / BYTES_PER_MB,
                gib(reachable) * 1024.0
            )),
        );
    }
    // Some room, but little: the cache above the headroom is under a tenth of the
    // volume, so it will hold almost nothing.
    if usable < fs_total / 10 {
        return base(
            Severity::Warn,
            "little room for the cache above the disk headroom".into(),
            data,
            Some("lower cache.disk_headroom_mb or move cache.cache_dir to a larger volume".into()),
        );
    }
    base(
        Severity::Pass,
        "cache is sized by free disk with headroom reserved".into(),
        data,
        None,
    )
}

/// Push all disk-group findings.
pub(crate) fn check_disk(report: &mut Report, cfg: &ResolvedConfig, live_footprint: Option<u64>) {
    let cache_dir = &cfg.cache.cache_dir;
    let budget = cfg.cache.cache_size_mb.saturating_mul(BYTES_PER_MB);
    let headroom = cfg.cache.disk_headroom_mb.saturating_mul(BYTES_PER_MB);

    // headroom vs free disk (needs statvfs on an existing dir; fall back to the
    // nearest existing ancestor when cache_dir does not exist yet).
    match statvfs_target(cache_dir) {
        Ok(space) => {
            report.push(evaluate_disk(
                budget,
                headroom,
                space.total,
                space.avail,
                live_footprint.unwrap_or(0),
            ));
            // A single max-size blob larger than free disk can fill the volume.
            let max_blob = cfg.cache.max_blob_size_mb.saturating_mul(BYTES_PER_MB);
            if max_blob > space.avail {
                report.push(Finding {
                    group: "Disk & cache",
                    id: "disk.max_blob_headroom",
                    severity: Severity::Warn,
                    title: "one max-size blob can exceed free disk".into(),
                    detail: Some(format!(
                        "max_blob_size_mib={} free_gib={:.1}",
                        cfg.cache.max_blob_size_mb,
                        gib(space.avail)
                    )),
                    remediation: Some("lower cache.max_blob_size_mb or free disk space".into()),
                });
            }
        }
        Err(e) => report.push(Finding {
            group: "Disk & cache",
            id: "disk.budget_vs_free",
            severity: Severity::Warn,
            title: "cannot read free disk space".into(),
            detail: Some(format!("cache_dir={} err={e}", cache_dir.display())),
            remediation: Some(
                "ensure cache.cache_dir (or its parent) exists and is accessible".into(),
            ),
        }),
    }

    // gc disabled => the budget ceiling is unenforceable.
    if cfg.cache.gc_interval_sec == 0 {
        report.push(Finding {
            group: "Disk & cache",
            id: "disk.gc_enabled",
            severity: Severity::Warn,
            title: "cache GC is disabled; budget ceiling is unenforceable".into(),
            detail: Some("cache.gc_interval_sec=0".into()),
            remediation: Some("set cache.gc_interval_sec > 0 so eviction can reclaim disk".into()),
        });
    }

    check_writable(report, "disk.cache_dir", "cache_dir", cache_dir);
    check_writable(report, "disk.data_dir", "data_dir", &cfg.identity.data_dir);
}

/// Probe writability by creating and removing a temp file in `dir` (or its
/// parent when `dir` does not exist yet). Read-only w.r.t. real node state.
fn check_writable(report: &mut Report, id: &'static str, label: &'static str, dir: &Path) {
    let target = if dir.exists() {
        dir
    } else {
        dir.parent().unwrap_or(dir)
    };
    match tempfile::Builder::new()
        .prefix(".decdn-doctor-")
        .tempfile_in(target)
    {
        Ok(_) => report.push(Finding {
            group: "Disk & cache",
            id,
            severity: Severity::Pass,
            title: format!("{label} is writable"),
            detail: Some(format!("{label}={}", dir.display())),
            remediation: None,
        }),
        Err(e) => report.push(Finding {
            group: "Disk & cache",
            id,
            severity: Severity::Fail,
            title: format!("{label} is not writable"),
            detail: Some(format!("{label}={} err={e}", dir.display())),
            remediation: Some(format!(
                "create {} and grant the node user write access",
                dir.display()
            )),
        }),
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::identity_op
)] // tests
mod tests {
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
    fn headroom_at_or_above_reachable_fails() {
        // 8 GiB headroom but only 3 GiB reachable => the cache can never grow.
        let f = evaluate_disk(100 * GIB, 8 * GIB, 50 * GIB, 3 * GIB, 0);
        assert_eq!(f.severity, Severity::Fail);
        assert!(f.remediation.is_some());
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
}
