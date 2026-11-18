use super::RateCeiling;
use crate::RateAboveCeiling;

/// The quote a refusal names, and the ceiling it exceeded.
fn refusal(err: &anyhow::Error) -> Option<(u64, u64)> {
    err.downcast_ref::<RateAboveCeiling>()
        .map(|r| (r.quoted_rate_per_mb(), r.ceiling_rate_per_mb()))
}

/// With no first quote and no absolute ceiling, the first stream sets the
/// ceiling at its own quote, and a later stream above it is refused.
#[test]
fn the_first_stream_sets_the_ceiling_when_no_answer_quoted() -> anyhow::Result<()> {
    let ceiling = RateCeiling::new(0);
    assert_eq!(ceiling.bound(), None);
    ceiling.admit(10)?;
    assert_eq!(ceiling.first_quote(), Some(10));
    ceiling.admit(10)?;
    ceiling.admit(4)?;
    let err = ceiling
        .admit(11)
        .err()
        .ok_or_else(|| anyhow::anyhow!("a quote above the first is refused"))?;
    assert_eq!(refusal(&err), Some((11, 10)));
    Ok(())
}

/// A discovery answer's quote is the ceiling, and a lower stream quote does
/// not lower it.
#[test]
fn the_discovery_quote_binds_every_later_stream() -> anyhow::Result<()> {
    let ceiling = RateCeiling::quoted(0, 7);
    ceiling.admit(3)?;
    assert_eq!(ceiling.bound(), Some(7), "a lower quote keeps the ceiling");
    let err = ceiling
        .admit(8)
        .err()
        .ok_or_else(|| anyhow::anyhow!("8 is above the first quote"))?;
    assert_eq!(refusal(&err), Some((8, 7)));
    Ok(())
}

/// A quote of zero is a ceiling of zero, not "no ceiling".
#[test]
fn a_free_first_quote_refuses_any_paid_stream() {
    let ceiling = RateCeiling::quoted(0, 0);
    assert_eq!(ceiling.bound(), Some(0));
    assert!(ceiling.admit(0).is_ok());
    assert!(ceiling.admit(1).is_err());
}

/// The absolute ceiling and the first quote bind together: the lower wins.
#[test]
fn the_lower_of_the_first_quote_and_the_absolute_ceiling_binds() -> anyhow::Result<()> {
    assert_eq!(
        RateCeiling::quoted(25, 7).bound(),
        Some(7),
        "the quote binds"
    );
    assert_eq!(RateCeiling::quoted(5, 7).bound(), Some(5), "the cap binds");
    let ceiling = RateCeiling::new(5);
    let err = ceiling
        .admit(6)
        .err()
        .ok_or_else(|| anyhow::anyhow!("6 is above the cap"))?;
    assert_eq!(refusal(&err), Some((6, 5)));
    assert_eq!(ceiling.first_quote(), None, "a refused quote sets nothing");
    Ok(())
}

/// Clones share the first quote: a lane built later for the same node in the
/// same fetch holds the quote an earlier lane saw.
#[test]
fn clones_share_the_first_quote() {
    let ceiling = RateCeiling::new(0);
    let later = ceiling.clone();
    ceiling.note_quote(9);
    later.note_quote(1);
    assert_eq!(later.first_quote(), Some(9), "the first quote stands");
}
