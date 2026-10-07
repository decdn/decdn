use super::*;

#[test]
fn headless_without_yes_refuses() {
    assert_eq!(decide_confirmation(false, false), Confirmation::NeedFlag);
    assert_eq!(decide_confirmation(false, true), Confirmation::Prompt);
    // `--yes` bypasses the prompt on a terminal AND headless — the flag is
    // what makes a scripted rotation legal.
    assert_eq!(decide_confirmation(true, false), Confirmation::Bypassed);
    assert_eq!(decide_confirmation(true, true), Confirmation::Bypassed);
}
