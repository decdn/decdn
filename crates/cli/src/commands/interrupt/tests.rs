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

fn open_tx(byte: u8, nonce: u64) -> OpenUnconfirmed {
    OpenUnconfirmed {
        tx: Some(alloy::primitives::B256::repeat_byte(byte)),
        nonce,
    }
}

fn open(byte: u8, nonce: u64) -> EscrowUnconfirmed {
    open_tx(byte, nonce).into()
}

fn top_up_tx(byte: u8, nonce: u64) -> TopUpUnconfirmed {
    TopUpUnconfirmed {
        tx: Some(alloy::primitives::B256::repeat_byte(byte)),
        nonce,
    }
}

fn top_up(byte: u8, nonce: u64) -> EscrowUnconfirmed {
    top_up_tx(byte, nonce).into()
}

fn gave_up() -> anyhow::Error {
    GaveUp {
        idle: std::time::Duration::from_secs(600),
    }
    .into()
}

#[test]
fn a_command_with_no_dropped_open_ends_as_it_did() {
    assert_eq!(name_unconfirmed_escrows(Ok(7), &[]).unwrap(), 7);
    let err = name_unconfirmed_escrows::<()>(Err(Interrupted.into()), &[]).unwrap_err();
    assert_eq!(
        stop_message(&err).unwrap(),
        "interrupted; run the same command again to resume"
    );
    assert_eq!(stop_exit(&err), Some(INTERRUPTED_EXIT));
    let err = name_unconfirmed_escrows::<()>(Err(gave_up()), &[]).unwrap_err();
    assert_eq!(
        stop_message(&err).unwrap(),
        "no progress for 10m; rerun to resume"
    );
    assert_eq!(stop_exit(&err), Some(GAVE_UP_EXIT));
}

/// A command that succeeded beside a dropped open still exits non-zero, naming
/// the tx: the deposit may escrow with no local record.
#[test]
fn a_dropped_open_turns_success_into_an_error_naming_the_tx() {
    let err = name_unconfirmed_escrows(Ok(()), &[open(0xef, 5)]).unwrap_err();
    assert_eq!(
        err.downcast_ref::<PendingEscrows>(),
        Some(&PendingEscrows(vec![open(0xef, 5)]))
    );
    assert!(format!("{err:#}").contains("nonce 5"), "{err:#}");
    assert_eq!(stop_message(&err), None);
    assert_eq!(stop_exit(&err), None);
}

/// An interrupt during an open still exits 130, and `main` names the tx
/// instead of telling the user to run the command again.
#[test]
fn an_interrupted_open_keeps_the_interrupt_and_names_the_tx() {
    let err =
        name_unconfirmed_escrows::<()>(Err(Interrupted.into()), &[open(0xef, 5)]).unwrap_err();
    assert!(err.downcast_ref::<Interrupted>().is_some());
    assert_eq!(stop_exit(&err), Some(INTERRUPTED_EXIT));
    let message = stop_message(&err).unwrap();
    assert!(
        message.starts_with("interrupted: openPool tx 0xefef"),
        "{message}"
    );
    assert!(!message.contains("run the same command again"), "{message}");
}

/// A give-up during an open names the tx in place of "rerun to resume", and
/// exits 1: `EX_TEMPFAIL` would tell a wrapper the re-run is safe.
#[test]
fn a_give_up_during_an_open_names_the_tx_and_is_not_a_tempfail() {
    let err = name_unconfirmed_escrows::<()>(Err(gave_up()), &[open(0xef, 5)]).unwrap_err();
    let message = stop_message(&err).unwrap();
    assert!(
        message.starts_with("no progress for 600s: openPool tx 0xefef"),
        "{message}"
    );
    assert!(!message.contains("rerun"), "{message}");
    assert_eq!(stop_exit(&err), None);
}

/// Every open left pending is named, one per line.
#[test]
fn every_pending_open_is_named() {
    let err =
        name_unconfirmed_escrows::<()>(Err(Interrupted.into()), &[open(0xef, 5), open(0xab, 6)])
            .unwrap_err();
    let message = stop_message(&err).unwrap();
    assert!(
        message.contains("0xefef") && message.contains("0xabab"),
        "{message}"
    );
    assert_eq!(message.lines().count(), 2, "{message}");
}

/// An error that already carries an open, from a receipt read that failed,
/// does not name it twice.
#[test]
fn an_open_the_error_carries_is_not_named_twice() {
    let carried = anyhow::anyhow!("receipt read failed").context(open_tx(0xef, 5));
    let err = name_unconfirmed_escrows::<()>(Err(carried), &[open(0xef, 5)]).unwrap_err();
    assert!(err.downcast_ref::<PendingEscrows>().is_none(), "{err:#}");
    assert_eq!(format!("{err:#}").matches("0xefef").count(), 1, "{err:#}");
    let carried = anyhow::anyhow!("receipt read failed").context(open_tx(0xef, 5));
    let err =
        name_unconfirmed_escrows::<()>(Err(carried), &[open(0xef, 5), open(0xab, 6)]).unwrap_err();
    assert_eq!(
        err.downcast_ref::<PendingEscrows>(),
        Some(&PendingEscrows(vec![open(0xab, 6)]))
    );
}

/// A give-up while a recovery step's `topUp` waits for its receipt names the
/// tx in place of "rerun to resume", and is not `EX_TEMPFAIL`: a rerun before
/// it confirms escrows a second top-up (#2491).
#[test]
fn a_give_up_during_a_top_up_names_the_tx_and_is_not_a_tempfail() {
    let err = name_unconfirmed_escrows::<()>(Err(gave_up()), &[top_up(0xcd, 7)]).unwrap_err();
    let message = stop_message(&err).unwrap();
    assert!(
        message.starts_with("no progress for 600s: topUp tx 0xcdcd"),
        "{message}"
    );
    assert!(message.contains("nonce 7"), "{message}");
    assert!(!message.contains("rerun"), "{message}");
    assert_eq!(stop_exit(&err), None);
}

/// A command that succeeded while a dropped `topUp` may still mine (a late
/// refill cut off when the run drains its lane builds) exits non-zero, naming
/// the tx.
#[test]
fn a_dropped_top_up_turns_success_into_an_error_naming_the_tx() {
    let err = name_unconfirmed_escrows(Ok(()), &[top_up(0xcd, 7)]).unwrap_err();
    assert_eq!(
        err.downcast_ref::<PendingEscrows>(),
        Some(&PendingEscrows(vec![top_up(0xcd, 7)]))
    );
    assert!(format!("{err:#}").contains("topUp tx 0xcdcd"), "{err:#}");
    assert_eq!(stop_message(&err), None);
    assert_eq!(stop_exit(&err), None);
}

/// An interrupt names every pending `openPool` and `topUp`, one per line, and
/// still exits 130.
#[test]
fn an_interrupt_names_pending_opens_and_top_ups() {
    let err =
        name_unconfirmed_escrows::<()>(Err(Interrupted.into()), &[open(0xef, 5), top_up(0xcd, 7)])
            .unwrap_err();
    assert_eq!(stop_exit(&err), Some(INTERRUPTED_EXIT));
    let message = stop_message(&err).unwrap();
    assert!(
        message.starts_with("interrupted: openPool tx 0xefef"),
        "{message}"
    );
    assert!(message.contains("topUp tx 0xcdcd"), "{message}");
    assert_eq!(message.lines().count(), 2, "{message}");
}

/// An error that already carries a `topUp`, from a lapsed receipt wait or a
/// refusal that names the held one, does not name it twice, and is still not
/// `EX_TEMPFAIL` under a give-up.
#[test]
fn a_top_up_the_error_carries_is_not_named_twice() {
    let carried = anyhow::anyhow!("receipt wait lapsed").context(top_up_tx(0xcd, 7));
    let err = name_unconfirmed_escrows::<()>(Err(carried), &[top_up(0xcd, 7)]).unwrap_err();
    assert!(err.downcast_ref::<PendingEscrows>().is_none(), "{err:#}");
    assert_eq!(format!("{err:#}").matches("0xcdcd").count(), 1, "{err:#}");

    let carried = gave_up().context(top_up_tx(0xcd, 7));
    assert_eq!(stop_exit(&carried), None);
    let message = stop_message(&carried).unwrap();
    assert!(message.contains("topUp tx 0xcdcd"), "{message}");
}
