use super::exit_code_for;
use std::process::ExitCode;

#[test]
fn gave_up_exits_75_and_anything_else_fails_with_1() {
    let gave_up = anyhow::Error::new(decdn_client::GaveUp {
        idle: std::time::Duration::from_mins(10),
    });
    assert_eq!(exit_code_for(&gave_up), ExitCode::from(75));
    let wrapped = gave_up.context("fetch");
    assert_eq!(exit_code_for(&wrapped), ExitCode::from(75));
    let interrupted = anyhow::Error::new(decdn_cli::commands::interrupt::Interrupted);
    assert_eq!(exit_code_for(&interrupted), ExitCode::from(130));
    assert_eq!(
        exit_code_for(&anyhow::anyhow!("pool empty")),
        ExitCode::FAILURE
    );
}
