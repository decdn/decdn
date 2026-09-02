//! Disk & cache-budget group. The eviction driver bounds the *logical* cache
//! footprint to `eviction_high_water_pct`% of `cache_size_mb` — a config
//! number, never real free disk. This group compares that ceiling against the
//! actual volume at `cache_dir`, so an over-budget cache on a small volume is
//! caught before it fills the disk.

use std::path::Path;

use decdn_common::config::ResolvedConfig;

use super::{Finding, Report, Severity};

const BYTES_PER_MB: u64 = 1024 * 1024;

/// Total and unprivileged-available bytes at a path.
pub struct DiskSpace {
    /// Total filesystem capacity, in bytes.
    pub total: u64,
    /// Bytes available to an unprivileged user, in bytes.
    pub avail: u64,
}

/// Read filesystem capacity at `path` via `statvfs`. Bytes = fragment size ×
/// block counts (`f_frsize × f_blocks`, `f_frsize × f_bavail`).
pub fn read_disk_space(path: &Path) -> anyhow::Result<DiskSpace> {
    let stat = nix::sys::statvfs::statvfs(path)
        .map_err(|e| anyhow::anyhow!("statvfs({}) failed: {e}", path.display()))?;
    let frsize = stat.fragment_size();
    // fsblkcnt_t is u32 on macOS (real widening) and u64 on 64-bit Linux glibc
    // (identity); the value always fits u64 either way.
    #[allow(clippy::useless_conversion)]
    let blocks = u64::from(stat.blocks());
    #[allow(clippy::useless_conversion)]
    let blocks_available = u64::from(stat.blocks_available());
    Ok(DiskSpace {
        total: frsize.saturating_mul(blocks),
        avail: frsize.saturating_mul(blocks_available),
    })
}

// Precision loss above 2^52 bytes (~4 PiB) is immaterial: this only feeds a
// human-readable GiB figure in report text, never a comparison or decision.
#[allow(clippy::cast_precision_loss)]
fn gib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0 * 1024.0)
}

/// Pure budget-vs-disk evaluation. `current_footprint` is the live
/// `decdn_cache_bytes` when known, else 0. `reachable = fs_avail +
/// current_footprint` is the space the cache could grow into.
pub fn evaluate_disk(
    budget_bytes: u64,
    high_water_pct: u64,
    fs_total: u64,
    fs_avail: u64,
    current_footprint: u64,
) -> Finding {
    let high_water = budget_bytes.saturating_mul(high_water_pct) / 100;
    let reachable = fs_avail.saturating_add(current_footprint);

    let base = |severity, title: String, detail: String, remediation: Option<String>| Finding {
        group: "Disk & cache",
        id: "disk.budget_vs_free",
        severity,
        title,
        detail: Some(detail),
        remediation,
    };
    let data = format!(
        "cache_size_mib={} high_water_gib={:.1} free_gib={:.1} vol_total_gib={:.1}",
        budget_bytes / BYTES_PER_MB,
        gib(high_water),
        gib(fs_avail),
        gib(fs_total),
    );

    if budget_bytes > fs_total {
        return base(
            Severity::Fail,
            "cache budget exceeds the whole volume".into(),
            data,
            Some(format!(
                "set cache.cache_size_mb below the volume size (~{:.0} MiB) or move cache.cache_dir",
                gib(fs_total) * 1024.0
            )),
        );
    }
    if high_water > reachable {
        // Suggest a budget whose high-water fits in reachable space, with a
        // safety margin: reserve max(10% of volume, 1 GiB).
        let margin = (fs_total / 10).max(1024 * 1024 * 1024);
        let usable = reachable.saturating_sub(margin);
        let suggested_mb = (usable.saturating_mul(100) / high_water_pct.max(1)) / BYTES_PER_MB;
        return base(
            Severity::Fail,
            "cache budget will fill the disk before eviction fires".into(),
            data,
            Some(format!(
                "eviction only fires at {high_water_pct}% of the config budget, not real disk; \
                 set cache.cache_size_mb <= {suggested_mb} or move cache.cache_dir to a larger volume"
            )),
        );
    }
    let thin = fs_avail < fs_total / 10;
    if thin {
        return base(
            Severity::Warn,
            "free disk is low relative to the volume".into(),
            data,
            Some("monitor free space or move cache.cache_dir to a larger volume".into()),
        );
    }
    base(
        Severity::Pass,
        "cache budget fits free disk".into(),
        data,
        None,
    )
}

/// Push all disk-group findings.
pub fn check_disk(report: &mut Report, cfg: &ResolvedConfig, live_footprint: Option<u64>) {
    let cache_dir = &cfg.cache.cache_dir;
    let budget = cfg.cache.cache_size_mb.saturating_mul(BYTES_PER_MB);

    // budget vs free disk (needs statvfs on an existing dir; fall back to the
    // nearest existing ancestor when cache_dir does not exist yet).
    match statvfs_target(cache_dir) {
        Ok(space) => {
            report.push(evaluate_disk(
                budget,
                cfg.cache.eviction_high_water_pct,
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

/// statvfs the path, or the nearest existing ancestor if it does not exist yet.
fn statvfs_target(path: &Path) -> anyhow::Result<DiskSpace> {
    let mut cur = path;
    loop {
        if cur.exists() {
            return read_disk_space(cur);
        }
        match cur.parent() {
            Some(p) => cur = p,
            None => return read_disk_space(path), // let statvfs surface the error
        }
    }
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
    fn budget_larger_than_volume_fails() {
        // 100 GiB budget, 50 GiB volume.
        let f = evaluate_disk(100 * GIB, 90, 50 * GIB, 40 * GIB, 0);
        assert_eq!(f.severity, Severity::Fail);
        assert_eq!(f.id, "disk.budget_vs_free");
    }

    #[test]
    fn high_water_above_reachable_fails() {
        // 10 GiB budget (hw=9 GiB), only 3 GiB avail + 0 used => reachable 3 GiB < 9 GiB.
        let f = evaluate_disk(10 * GIB, 90, 50 * GIB, 3 * GIB, 0);
        assert_eq!(f.severity, Severity::Fail);
        assert!(f.remediation.is_some());
    }

    #[test]
    fn healthy_headroom_passes() {
        // 10 GiB budget (hw=9 GiB), 40 GiB avail => reachable 40 GiB >= 9 GiB, not thin.
        let f = evaluate_disk(10 * GIB, 90, 50 * GIB, 40 * GIB, 0);
        assert_eq!(f.severity, Severity::Pass);
    }

    #[test]
    fn thin_but_sufficient_warns() {
        // hw=0.9 GiB, avail=1 GiB on a 50 GiB volume => reachable >= hw but avail < 10% of total.
        let f = evaluate_disk(1 * GIB, 90, 50 * GIB, 1 * GIB, 0);
        assert_eq!(f.severity, Severity::Warn);
    }

    #[test]
    fn read_disk_space_of_tempdir_is_nonzero() {
        let dir = tempfile::tempdir().unwrap();
        let s = read_disk_space(dir.path()).unwrap();
        assert!(s.total > 0);
    }
}
