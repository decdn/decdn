//! Expiry-reclaim background sweep: periodically scans the persisted
//! channel table for rows past their configured TTL and attempts to reclaim
//! their remaining on-chain deposit via `Treasury::reclaim_expired`,
//! dropping the row on success.
//!
//! Eligibility is `opened_unix + ttl_secs <= now` — a *local* bookkeeping
//! TTL, distinct from (and expected to be shorter than) the on-chain
//! `Channel.expiresAt` the contract itself enforces. If the sweep gets to a
//! row before the chain considers it expired, `Treasury::reclaim_expired`
//! returns `Ok(false)` and the row is simply left for a later tick.

use std::time::Duration;

use crate::state::AppState;

/// Run the sweep on `interval` forever. Never panics the loop — a failed
/// sweep is logged and the loop just waits for the next tick.
pub async fn run(state: AppState, interval: Duration, ttl_secs: u64) {
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        if let Err(e) = sweep_once(&state, ttl_secs).await {
            tracing::warn!("reclaim sweep error: {e}");
        }
    }
}

/// One pass: reclaim every channel whose `opened_unix + ttl_secs <= now`,
/// removing it from the store on a successful on-chain reclaim.
async fn sweep_once(state: &AppState, ttl_secs: u64) -> anyhow::Result<()> {
    let now = now_unix();
    for (id, rec) in state.store.iter_channels()? {
        if rec.opened_unix + ttl_secs <= now {
            match state.treasury.reclaim_expired(id).await {
                Ok(true) => state.store.remove_channel(id, rec.client, rec.node_id)?,
                Ok(false) => {} // not yet expired on-chain; leave it for a later tick
                Err(e) => tracing::warn!("reclaim {id} failed: {e}"),
            }
        }
    }
    Ok(())
}

/// Seconds since the Unix epoch, saturating to `0` rather than panicking if
/// the clock is somehow set before `UNIX_EPOCH`.
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    #[test]
    fn selects_only_channels_past_expiry() {
        let now = 1_000_000u64;
        let recs = [
            (/*id*/ 1u8, /*opened*/ now - 10, /*ttl*/ 5), // expired
            (2u8, now - 1, 100),                          // fresh
        ];
        let due: Vec<u8> = recs
            .iter()
            .filter(|(_, opened, ttl)| opened + ttl <= now)
            .map(|(id, _, _)| *id)
            .collect();
        assert_eq!(due, vec![1u8]);
    }
}
