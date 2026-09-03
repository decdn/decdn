//! Filesystem free-space probing via `statvfs`.
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

/// `statvfs` the path, or the nearest existing ancestor if it does not exist
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
            None => return read_disk_space(path), // let statvfs surface the error
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
