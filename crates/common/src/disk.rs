//! Filesystem free-space probing: `statvfs` on Unix, `GetDiskFreeSpaceExW` on
//! Windows, both through `fs4`.
//!
//! Two consumers share this: `decdn node doctor` compares the configured cache
//! budget against the real volume advisorily, and the node's eviction driver
//! clamps its effective ceiling to real free disk each tick (#1930). Sharing one
//! implementation keeps the advisory check and the runtime clamp measuring the
//! same numbers.

use std::path::Path;

/// Total and unprivileged-available bytes at a path.
#[derive(Debug, Clone, Copy)]
pub struct DiskSpace {
    /// Total filesystem capacity, in bytes.
    pub total: u64,
    /// Bytes available to an unprivileged user, in bytes.
    pub avail: u64,
}

/// Read filesystem capacity at `path`. On Unix, bytes = fragment size × block
/// counts (`f_frsize × f_blocks`, `f_frsize × f_bavail`); on Windows, the
/// caller-visible totals `GetDiskFreeSpaceExW` reports.
pub fn read_disk_space(path: &Path) -> anyhow::Result<DiskSpace> {
    let total = fs4::total_space(path)
        .map_err(|e| anyhow::anyhow!("disk total_space({}) failed: {e}", path.display()))?;
    let avail = fs4::available_space(path)
        .map_err(|e| anyhow::anyhow!("disk available_space({}) failed: {e}", path.display()))?;
    Ok(DiskSpace { total, avail })
}

/// Probe the path, or the nearest existing ancestor if it does not exist
/// yet. The cache directory can be probed before it is created (pre-boot doctor)
/// or after (the running eviction driver); walking to the nearest existing
/// ancestor gives the right volume in both cases.
pub fn statvfs_target(path: &Path) -> anyhow::Result<DiskSpace> {
    let mut cur = path;
    loop {
        if cur.exists() {
            return read_disk_space(cur);
        }
        match cur.parent() {
            Some(p) => cur = p,
            None => return read_disk_space(path), // let the probe surface the error
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests
mod tests {
    use super::*;

    #[test]
    fn read_disk_space_of_tempdir_is_nonzero() {
        let dir = tempfile::tempdir().unwrap();
        let s = read_disk_space(dir.path()).unwrap();
        assert!(s.total > 0);
    }

    #[test]
    fn statvfs_target_walks_to_existing_ancestor() {
        let dir = tempfile::tempdir().unwrap();
        // A not-yet-created child resolves against its existing parent volume.
        let missing = dir.path().join("does/not/exist/yet");
        let s = statvfs_target(&missing).unwrap();
        assert!(s.total > 0);
    }
}
