//! Graceful Ctrl-C for `decdn get`, `decdn pool open` and `decdn pool top-up`.
//!
//! The first Ctrl-C resolves `Interrupt::wait`: the command stops its
//! transfers, writes what it must keep (the bundle skip-cache, the progress
//! indicators' cleanup), and returns [`Interrupted`], which exits with status
//! 130. A second Ctrl-C, or one that arrives after the command stops watching,
//! exits at once with the same status.
//!
//! Stopping mid-transfer loses nothing already paid for: every fetch resumes
//! from its on-disk `.partial` on the next run. Stopping an `openPool` or a
//! `topUp` before its receipt is read does not cancel the transaction, so the
//! command names it (`name_unconfirmed_escrows`, and the immediate exit)
//! instead of telling the user to run it again: a re-run before it confirms
//! escrows a second time.

use decdn_client::GaveUp;
use decdn_client::buyer_pool::{
    EscrowUnconfirmed, OpenUnconfirmed, TopUpUnconfirmed, UnconfirmedEscrows,
};

/// The error a command returns when Ctrl-C stopped it. `main` prints
/// [`stop_message`] and exits with status 130 (128 + SIGINT), the shell
/// convention for an interrupted command.
#[derive(Debug)]
pub struct Interrupted;

impl std::fmt::Display for Interrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("interrupted")
    }
}

impl std::error::Error for Interrupted {}

/// The `openPool` and `topUp` transactions a command ended with and did not
/// see confirm. Each may still mine and escrow USDC. Displays one per line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingEscrows(Vec<EscrowUnconfirmed>);

impl std::fmt::Display for PendingEscrows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, escrow) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("\n")?;
            }
            write!(f, "{escrow}")?;
        }
        Ok(())
    }
}

impl std::error::Error for PendingEscrows {}

/// End a command that may have dropped an `openPool` or a `topUp` before its
/// receipt was read. `escrows` are the transactions it did not see confirm
/// ([`UnconfirmedEscrows::pending`]).
///
/// A transaction the error already carries is not named twice. With none
/// left, `result` passes through. Otherwise they are attached to the result as
/// [`PendingEscrows`], so the command names each tx. An `Ok` becomes an error:
/// the USDC may escrow with no local record, and only a non-zero exit makes
/// sure someone reconciles it. An error keeps its own type underneath, so an
/// [`Interrupted`] still exits 130.
pub(crate) fn name_unconfirmed_escrows<T>(
    result: anyhow::Result<T>,
    escrows: &[EscrowUnconfirmed],
) -> anyhow::Result<T> {
    let carried = result.as_ref().err().and_then(carried);
    let left: Vec<EscrowUnconfirmed> = escrows
        .iter()
        .copied()
        .filter(|escrow| Some(*escrow) != carried)
        .collect();
    if left.is_empty() {
        return result;
    }
    let pending = PendingEscrows(left);
    match result {
        Ok(_) => Err(anyhow::Error::new(pending)),
        Err(err) => Err(err.context(pending)),
    }
}

/// The unconfirmed `openPool` or `topUp` that `err` itself carries, from a
/// submit lost in transport, a receipt read that failed or lapsed, or a
/// refusal that names an earlier one.
fn carried(err: &anyhow::Error) -> Option<EscrowUnconfirmed> {
    err.downcast_ref::<OpenUnconfirmed>()
        .copied()
        .map(EscrowUnconfirmed::from)
        .or_else(|| {
            err.downcast_ref::<TopUpUnconfirmed>()
                .copied()
                .map(EscrowUnconfirmed::from)
        })
}

/// The `openPool` and `topUp` transactions `err` names as possibly escrowing,
/// as text.
fn in_flight(err: &anyhow::Error) -> Option<String> {
    err.downcast_ref::<PendingEscrows>()
        .map(ToString::to_string)
        .or_else(|| carried(err).map(|escrow| escrow.to_string()))
}

/// The line `main` prints for a command that stopped rather than failed: a
/// Ctrl-C, or a give-up after the no-progress limit. `None` for any other
/// error. An `openPool` or `topUp` still in flight replaces the advice to run
/// the command again, because a re-run before it confirms escrows again.
#[must_use]
pub fn stop_message(err: &anyhow::Error) -> Option<String> {
    let in_flight = in_flight(err);
    if err.downcast_ref::<Interrupted>().is_some() {
        return Some(match in_flight {
            Some(escrows) => format!("interrupted: {escrows}"),
            None => "interrupted; run the same command again to resume".to_owned(),
        });
    }
    let gave_up = err.downcast_ref::<GaveUp>()?;
    Some(match in_flight {
        Some(escrows) => format!("no progress for {}s: {escrows}", gave_up.idle.as_secs()),
        None => gave_up.to_string(),
    })
}

/// The exit status for a command that stopped rather than failed:
/// [`INTERRUPTED_EXIT`] for a Ctrl-C, and [`GAVE_UP_EXIT`] for a give-up.
/// `None` for any other error, and for a give-up with an `openPool` or `topUp`
/// in flight: `EX_TEMPFAIL` tells a wrapper that a re-run is safe, and that one
/// is not.
#[must_use]
pub fn stop_exit(err: &anyhow::Error) -> Option<u8> {
    if err.downcast_ref::<Interrupted>().is_some() {
        return Some(INTERRUPTED_EXIT);
    }
    (err.downcast_ref::<GaveUp>().is_some() && in_flight(err).is_none()).then_some(GAVE_UP_EXIT)
}

/// The exit status for an interrupted command: 128 + SIGINT.
pub const INTERRUPTED_EXIT: u8 = 130;

/// The exit status for a fetch that gave up after its no-progress limit:
/// `EX_TEMPFAIL`, since a rerun resumes it.
pub const GAVE_UP_EXIT: u8 = 75;

/// A handle that resolves on the first Ctrl-C. Dropping it hands Ctrl-C back
/// to an immediate exit. The immediate exit first names every `openPool` and
/// `topUp` in the command's [`UnconfirmedEscrows`]: it skips the command's own
/// return, which is where the first Ctrl-C names them.
pub(crate) struct Interrupt {
    /// Fires on the first Ctrl-C while this handle lives.
    rx: tokio::sync::oneshot::Receiver<()>,
    /// Whether `rx` has fired; a oneshot is not polled again after it resolves.
    fired: bool,
}

impl Interrupt {
    /// Start watching Ctrl-C for the rest of the process, for a command whose
    /// `openPool` and `topUp` transactions `escrows` holds.
    pub(crate) fn watch(escrows: UnconfirmedEscrows) -> Self {
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut tx = Some(tx);
            // An `Err` means no handler could be installed, so SIGINT keeps its
            // default action and there is nothing to watch.
            while tokio::signal::ctrl_c().await.is_ok() {
                if let Some(tx) = tx.take()
                    && tx.send(()).is_ok()
                {
                    continue;
                }
                // Nothing graceful left to do: remove the tab indicator that
                // `exit` would skip, name what may still escrow, then quit.
                super::tab_progress::remove_now();
                for escrow in escrows.pending() {
                    eprintln!("interrupted: {escrow}");
                }
                std::process::exit(i32::from(INTERRUPTED_EXIT));
            }
        });
        Self { rx, fired: false }
    }

    /// A handle fired by the returned sender in place of a Ctrl-C.
    #[cfg(test)]
    pub(crate) fn manual() -> (tokio::sync::oneshot::Sender<()>, Self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (tx, Self { rx, fired: false })
    }

    /// Resolve once the first Ctrl-C has arrived. Pends forever when no handler
    /// could be installed.
    pub(crate) async fn wait(&mut self) {
        if self.fired {
            return;
        }
        match (&mut self.rx).await {
            Ok(()) => self.fired = true,
            Err(_) => std::future::pending::<()>().await,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
