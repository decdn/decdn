//! Parse a human byte size (`4MiB`, `1048576`, `2M`) for CLI `value_parser`s.
//! Binary units only: `K`/`KiB`/`KB` = 1024, `M`/`MiB`/`MB` = 1024², `G`/`GiB`/`GB` = 1024³.

/// Parse a byte size: a bare integer (bytes) or an integer followed by a
/// binary unit suffix (`K`/`KiB`/`KB`, `M`/`MiB`/`MB`, `G`/`GiB`/`GB`, all
/// powers of 1024). Returns the size in bytes. The error is a `String` so it
/// renders directly in clap's `value_parser` diagnostics.
pub fn parse_byte_size(s: &str) -> Result<u64, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty byte size".to_string());
    }
    // Split leading digits from an optional unit suffix (whitespace allowed between).
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    if num.is_empty() {
        return Err(format!("byte size {s:?} must start with digits"));
    }
    let value: u64 = num
        .parse()
        .map_err(|_| format!("byte size {s:?} has an invalid number"))?;
    let mult: u64 = match unit.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "K" | "KB" | "KIB" => 1024,
        "M" | "MB" | "MIB" => 1024 * 1024,
        "G" | "GB" | "GIB" => 1024 * 1024 * 1024,
        other => return Err(format!("byte size {s:?} has an unknown unit {other:?}")),
    };
    value
        .checked_mul(mult)
        .ok_or_else(|| format!("byte size {s:?} overflows u64"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
