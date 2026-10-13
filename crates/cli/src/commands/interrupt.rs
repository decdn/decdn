//! Graceful Ctrl-C for `decdn fetch`, `decdn bundle pull` and `decdn pool open`.
//!
//! The first Ctrl-C resolves `Interrupt::wait`: the command stops its
//! transfers, writes what it must keep (the bundle skip-cache, the progress
//! indicators' cleanup), and returns [`Interrupted`], which exits with status
//! 130. A second Ctrl-C, or one that arrives after the command stops watching,
//! exits at once with the same status.
//!
//! Stopping mid-transfer loses nothing already paid for: every fetch resumes
//! from its on-disk `.partial` on the next run. Stopping an `openPool` before
//! its receipt is read does not cancel the transaction, so the command names
//! it (`name_unconfirmed_open`, and the immediate exit) instead of telling the
//! user to run it again: a re-run before it confirms escrows a second deposit.

use decdn_client::GaveUp;
use decdn_client::buyer_pool::{OpenUnconfirmed, UnconfirmedOpens};

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

/// The `openPool` transactions a command ended with and did not see confirm.
/// Each may still mine and escrow its deposit. Displays one open per line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingOpens(Vec<OpenUnconfirmed>);

impl std::fmt::Display for PendingOpens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, open) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("\n")?;
            }
            write!(f, "{open}")?;
        }
        Ok(())
    }
}

impl std::error::Error for PendingOpens {}

/// End a command that may have dropped an `openPool` before its receipt was
/// read. `opens` are the opens it did not see confirm
/// ([`UnconfirmedOpens::pending`]).
///
/// An open the error already carries is not named twice. With none left,
/// `result` passes through. Otherwise they are attached to the result as
/// [`PendingOpens`], so the command names each tx. An `Ok` becomes an error:
/// the deposit may escrow with no local record, and only a non-zero exit makes
/// sure someone reconciles it. An error keeps its own type underneath, so an
/// [`Interrupted`] still exits 130.
pub(crate) fn name_unconfirmed_open<T>(
    result: anyhow::Result<T>,
    opens: &[OpenUnconfirmed],
) -> anyhow::Result<T> {
    let carried = result
        .as_ref()
        .err()
        .and_then(|err| err.downcast_ref::<OpenUnconfirmed>().copied());
    let left: Vec<OpenUnconfirmed> = opens
        .iter()
        .copied()
        .filter(|open| Some(*open) != carried)
        .collect();
    if left.is_empty() {
        return result;
    }
    let pending = PendingOpens(left);
    match result {
        Ok(_) => Err(anyhow::Error::new(pending)),
        Err(err) => Err(err.context(pending)),
    }
}

/// The `openPool` transactions `err` names as possibly escrowing, as text.
fn in_flight(err: &anyhow::Error) -> Option<String> {
    err.downcast_ref::<PendingOpens>()
        .map(ToString::to_string)
        .or_else(|| {
            err.downcast_ref::<OpenUnconfirmed>()
                .map(ToString::to_string)
        })
}

/// The line `main` prints for a command that stopped rather than failed: a
/// Ctrl-C, or a give-up after the no-progress limit. `None` for any other
/// error. An `openPool` still in flight replaces the advice to run the command
/// again, because a re-run before it confirms escrows a second deposit.
#[must_use]
pub fn stop_message(err: &anyhow::Error) -> Option<String> {
    let in_flight = in_flight(err);
    if err.downcast_ref::<Interrupted>().is_some() {
        return Some(match in_flight {
            Some(opens) => format!("interrupted: {opens}"),
            None => "interrupted; run the same command again to resume".to_owned(),
        });
    }
    let gave_up = err.downcast_ref::<GaveUp>()?;
    Some(match in_flight {
        Some(opens) => format!("no progress for {}s: {opens}", gave_up.idle.as_secs()),
        None => gave_up.to_string(),
    })
}

/// The exit status for a command that stopped rather than failed:
/// [`INTERRUPTED_EXIT`] for a Ctrl-C, and [`GAVE_UP_EXIT`] for a give-up.
/// `None` for any other error, and for a give-up with an `openPool` in flight:
/// `EX_TEMPFAIL` tells a wrapper that a re-run is safe, and that one is not.
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
/// to an immediate exit. The immediate exit first names every `openPool` in
/// the command's [`UnconfirmedOpens`]: it skips the command's own return,
/// which is where the first Ctrl-C names them.
pub(crate) struct Interrupt {
    /// Fires on the first Ctrl-C while this handle lives.
    rx: tokio::sync::oneshot::Receiver<()>,
    /// Whether `rx` has fired; a oneshot is not polled again after it resolves.
    fired: bool,
}

impl Interrupt {
    /// Start watching Ctrl-C for the rest of the process, for a command whose
    /// `openPool` transactions `opens` holds.
    pub(crate) fn watch(opens: UnconfirmedOpens) -> Self {
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
                for open in opens.pending() {
                    eprintln!("interrupted: {open}");
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
