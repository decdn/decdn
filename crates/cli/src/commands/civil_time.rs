//! Proleptic-Gregorian date arithmetic for the CLI's UTC labels.
//!
//! The CLI deliberately links no calendar crate. These are Howard Hinnant's
//! `days_from_civil` / `civil_from_days` (public domain), pure integer math,
//! enough for a `YYYY-MM-DD HH:MM UTC` label and for parsing a `--since` date.

/// Seconds in a day.
const DAY_SECS: i64 = 86_400;

/// Days from 1970-01-01 to the given proleptic-Gregorian date. Pure integer
/// math, no casts.
pub(crate) const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if month > 2 { month - 3 } else { month + 9 }; // [0, 11]
    let doy = (153 * mp + 2) / 5 + day - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// The proleptic-Gregorian `(year, month, day)` of the day `days` after
/// 1970-01-01 (negative for before). The inverse of [`days_from_civil`].
pub(crate) const fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if month <= 2 { y + 1 } else { y }, month, day)
}

/// `YYYY-MM-DD` of a Unix time, UTC.
pub(crate) fn format_date(unix: u64) -> String {
    let days = i64::try_from(unix / 86_400).unwrap_or(i64::MAX);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

/// `YYYY-MM-DD HH:MM UTC` of a Unix time.
pub(crate) fn format_utc(unix: u64) -> String {
    let secs_of_day = unix % 86_400;
    format!(
        "{} {:02}:{:02} UTC",
        format_date(unix),
        secs_of_day / 3_600,
        (secs_of_day % 3_600) / 60
    )
}

/// Parse a `--since` value: a UTC date `YYYY-MM-DD` (midnight) or Unix
/// seconds.
///
/// # Errors
///
/// Fails on anything else, naming both accepted forms.
pub(crate) fn parse_since(value: &str) -> anyhow::Result<u64> {
    let value = value.trim();
    if let Ok(unix) = value.parse::<u64>() {
        return Ok(unix);
    }
    let parts: Vec<&str> = value.split('-').collect();
    let date = match parts.as_slice() {
        [y, m, d] if y.len() == 4 && m.len() == 2 && d.len() == 2 => {
            match (y.parse::<i64>(), m.parse::<i64>(), d.parse::<i64>()) {
                (Ok(y), Ok(m), Ok(d)) if (1..=12).contains(&m) && (1..=31).contains(&d) => {
                    Some((y, m, d))
                }
                _ => None,
            }
        }
        _ => None,
    };
    let Some((year, month, day)) = date else {
        anyhow::bail!("--since takes a UTC date `YYYY-MM-DD` or Unix seconds, not `{value}`");
    };
    let days = days_from_civil(year, month, day);
    // A date that does not exist (2026-02-30) normalises forward; refuse it
    // instead, so a typo does not silently shift the period.
    anyhow::ensure!(
        civil_from_days(days) == (year, month, day),
        "--since: `{value}` is not a calendar date"
    );
    u64::try_from(days * DAY_SECS)
        .map_err(|_| anyhow::anyhow!("--since: `{value}` is before 1970-01-01"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
