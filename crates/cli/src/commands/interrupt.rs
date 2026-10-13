//! Graceful Ctrl-C for `decdn fetch`, `decdn bundle pull` and `decdn pool open`.
//!
//! The first Ctrl-C resolves `Interrupt::wait`: the command stops its
//! transfers, writes what it must keep (the bundle skip-cache, the progress
//! indicators' cleanup), and returns [`Interrupted`], which exits with status
//! 130. A second Ctrl-C, or one that arrives after the command stops watching,
//! exits at once with the same status.
//!
//! Stopping mid-transfer loses nothing already paid for: every fetch resumes
//! from its on-disk `.partial` on the next run. Stopping during an `openPool`
//! receipt wait does not cancel the transaction, so the command names it
//! (`name_unconfirmed_open`) instead of telling the user to run it again.

use decdn_client::buyer_pool::OpenUnconfirmed;

/// The error a command returns when Ctrl-C stopped it. `main` prints a resume
/// hint and exits with status 130 (128 + SIGINT), the shell convention for an
/// interrupted command.
#[derive(Debug)]
pub struct Interrupted;

impl std::fmt::Display for Interrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("interrupted")
    }
}

impl std::error::Error for Interrupted {}

/// The line `main` prints for an interrupted command: the `openPool` that
/// `err` names as still in flight, or else the resume hint.
#[must_use]
pub fn interrupted_message(err: &anyhow::Error) -> String {
    match err.downcast_ref::<OpenUnconfirmed>() {
        Some(open) => format!("interrupted: {open}"),
        None => "interrupted; run the same command again to resume".to_owned(),
    }
}

/// End a command that may have dropped an `openPool` receipt wait. `opens`
/// are the opens whose wait did not return
/// ([`decdn_client::buyer_pool::UnconfirmedOpens::pending`]).
///
/// With none, `result` passes through. Otherwise the first one is attached
/// to the result, so the command names its tx. An `Ok` becomes an error: the
/// deposit may escrow with no local record, and only a non-zero exit makes
/// sure someone reconciles it. An error keeps its own type underneath, so an
/// [`Interrupted`] still exits 130.
pub(crate) fn name_unconfirmed_open<T>(
    result: anyhow::Result<T>,
    opens: &[OpenUnconfirmed],
) -> anyhow::Result<T> {
    let Some(open) = opens.first() else {
        return result;
    };
    match result {
        Ok(_) => Err(anyhow::Error::new(*open)),
        Err(err) => Err(err.context(*open)),
    }
}

/// The exit status for an interrupted command: 128 + SIGINT.
pub const INTERRUPTED_EXIT: u8 = 130;

/// The exit status for a fetch that gave up after its no-progress limit:
/// `EX_TEMPFAIL`, since a rerun resumes it.
pub const GAVE_UP_EXIT: u8 = 75;

/// A handle that resolves on the first Ctrl-C. Dropping it hands Ctrl-C back
/// to an immediate exit.
pub(crate) struct Interrupt {
    /// Fires on the first Ctrl-C while this handle lives.
    rx: tokio::sync::oneshot::Receiver<()>,
    /// Whether `rx` has fired; a oneshot is not polled again after it resolves.
    fired: bool,
}

impl Interrupt {
    /// Start watching Ctrl-C for the rest of the process.
    pub(crate) fn watch() -> Self {
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
                // `exit` would skip, then quit.
                super::tab_progress::remove_now();
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
