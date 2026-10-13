use super::*;

#[tokio::test]
async fn wait_resolves_again_after_it_fired() {
    let (fire, mut interrupt) = Interrupt::manual();
    fire.send(()).unwrap();
    interrupt.wait().await;
    // A second wait (a later `select!`) must not poll the spent oneshot.
    interrupt.wait().await;
}

#[tokio::test]
async fn wait_pends_while_nothing_fired() {
    let (_fire, mut interrupt) = Interrupt::manual();
    let waited = tokio::time::timeout(std::time::Duration::from_millis(20), interrupt.wait()).await;
    assert!(waited.is_err());
}

fn open() -> OpenUnconfirmed {
    OpenUnconfirmed {
        tx: Some(alloy::primitives::B256::repeat_byte(0xef)),
        nonce: 5,
    }
}

#[test]
fn a_command_with_no_dropped_open_ends_as_it_did() {
    assert_eq!(name_unconfirmed_open(Ok(7), &[]).unwrap(), 7);
    let err = name_unconfirmed_open::<()>(Err(Interrupted.into()), &[]).unwrap_err();
    assert_eq!(
        interrupted_message(&err),
        "interrupted; run the same command again to resume"
    );
}

/// A command that succeeded beside a dropped open still exits non-zero, naming
/// the tx: the deposit may escrow with no local record.
#[test]
fn a_dropped_open_turns_success_into_an_error_naming_the_tx() {
    let err = name_unconfirmed_open(Ok(()), &[open()]).unwrap_err();
    assert_eq!(err.downcast_ref::<OpenUnconfirmed>(), Some(&open()));
    assert!(format!("{err:#}").contains("nonce 5"), "{err:#}");
}

/// An interrupt during an open still exits 130, and `main` names the tx
/// instead of telling the user to run the command again.
#[test]
fn an_interrupted_open_keeps_the_interrupt_and_names_the_tx() {
    let err = name_unconfirmed_open::<()>(Err(Interrupted.into()), &[open()]).unwrap_err();
    assert!(err.downcast_ref::<Interrupted>().is_some());
    assert_eq!(err.downcast_ref::<OpenUnconfirmed>(), Some(&open()));
    let message = interrupted_message(&err);
    assert!(
        message.starts_with("interrupted: openPool tx 0xefef"),
        "{message}"
    );
    assert!(!message.contains("run the same command again"), "{message}");
}
