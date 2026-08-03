#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MicroUsdc(pub u64);

impl MicroUsdc {
    pub fn saturating_add(self, other: MicroUsdc) -> MicroUsdc {
        MicroUsdc(self.0.saturating_add(other.0))
    }
}

/// Months since year 0, as `year*12 + (month-1)`. Uses civil-date math (UTC).
pub fn month_bucket(unix_secs: u64) -> u32 {
    // days since epoch
    let days = (unix_secs / 86_400) as i64;
    // Howard Hinnant's civil_from_days
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year as u32) * 12 + (m as u32 - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn month_bucket_rolls_on_month_boundary() {
        // 2026-01-31T23:59:59Z and 2026-02-01T00:00:00Z differ by one bucket.
        let jan = month_bucket(1_769_903_999);
        let feb = month_bucket(1_769_904_000);
        assert_eq!(feb, jan + 1);
    }
    #[test]
    fn micro_add_saturates() {
        assert_eq!(MicroUsdc(u64::MAX).saturating_add(MicroUsdc(1)).0, u64::MAX);
    }
}
