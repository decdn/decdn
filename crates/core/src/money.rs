#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MicroUsdc(pub u64);

impl MicroUsdc {
    pub fn saturating_add(self, other: MicroUsdc) -> MicroUsdc {
        MicroUsdc(self.0.saturating_add(other.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn micro_add_saturates() {
        assert_eq!(MicroUsdc(u64::MAX).saturating_add(MicroUsdc(1)).0, u64::MAX);
    }
}
