use super::*;
use crate::{PullStalled, UpstreamVoucherRejected};
use decdn_protocol::client::VoucherRejectReason;
use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;

/// 50 concurrent issuers on one ledger sign strictly increasing cumulatives
/// with no gaps: each successful send commits, so the committed watermark ends
/// at the exact sum of the 100-byte deltas and never more.
#[tokio::test]
async fn concurrent_issue_is_monotonic_and_exact() -> anyhow::Result<()> {
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let mut handles = Vec::new();
    for _ in 0..50u32 {
        let l = Arc::clone(&ledger);
        handles.push(tokio::spawn(async move {
            l.issue(100, 10, EpochAction::Keep, |_signed, _chain| async {
                tokio::task::yield_now().await;
                Ok(())
            })
            .await
        }));
    }

    let mut byte_totals = Vec::new();
    for h in handles {
        byte_totals.push(h.await??.bytes);
    }
    byte_totals.sort_unstable();
    let expected: Vec<U256> = (1..=50u64).map(|k| U256::from(k * 100)).collect();
    assert_eq!(
        byte_totals, expected,
        "every voucher's cumulative is distinct"
    );

    // Every voucher committed ⇒ committed carries all 50, and never more bytes
    // than the 100-per-voucher deltas we actually issued.
    let committed = ledger.committed();
    assert_eq!(committed.bytes, U256::from(5000u64));
    assert_eq!(committed.amount, U256::from(50u64));
    assert_eq!(ledger.settlement(), committed);
    Ok(())
}

#[tokio::test]
async fn failed_send_does_not_commit() -> anyhow::Result<()> {
    let ledger = PoolLedger::new(Cumulative::default());
    let result = ledger
        .issue(100, 10, EpochAction::Keep, |_signed, _chain| async {
            anyhow::bail!("send lost")
        })
        .await;
    assert!(result.is_err(), "a failed send must surface the error");
    // Committed unmoved: a send that never confirmed never advances committed.
    assert_eq!(ledger.committed(), Cumulative::default());
    // But it settles HIGH — the send is ambiguous, so the voucher stays armed.
    assert_eq!(ledger.settlement().bytes, U256::from(100u64));
    Ok(())
}

/// A terminal `StreamEnd` read after the failed send proves the upstream holds
/// the armed voucher: confirming it commits exactly what a successful send
/// would have, once — a second confirm finds nothing armed — and the next
/// voucher builds on it.
#[tokio::test]
async fn a_terminal_stream_end_confirms_the_armed_voucher() -> anyhow::Result<()> {
    let ledger = PoolLedger::new(Cumulative::default());
    let failed = ledger
        .issue(100, 10, EpochAction::Keep, |_signed, _chain| async {
            anyhow::bail!("send stopped")
        })
        .await;
    assert!(failed.is_err());
    let armed = Cumulative {
        bytes: U256::from(100u64),
        amount: U256::from(1u64),
    };
    assert_eq!(
        ledger
            .confirm_armed(armed.amount.saturating_add(U256::from(1u64)))
            .await,
        None,
        "a StreamEnd confirms only the voucher its own stream sent"
    );
    assert_eq!(ledger.committed(), Cumulative::default());
    assert_eq!(ledger.confirm_armed(armed.amount).await, Some(armed));
    assert_eq!(ledger.committed(), armed);
    assert_eq!(ledger.settlement(), armed);
    assert_eq!(ledger.confirm_armed(armed.amount).await, None);
    let next = ledger
        .issue(100, 10, EpochAction::Keep, |_signed, _chain| async {
            Ok(())
        })
        .await?;
    assert_eq!(next.bytes, U256::from(200u64));
    assert!(next.amount > armed.amount);
    assert_eq!(ledger.committed(), next);
    Ok(())
}

/// Confirming takes out of `accrued` only what the armed voucher folded. The
/// issuance lock is free between the failed send and the confirm, so a
/// sibling stream can release a reveal on the freshly rolled chain in that
/// window. The voucher never signed that reveal, so it stays owed — and a
/// later rejection of the confirmed voucher still rewinds both halves.
#[tokio::test]
async fn confirming_an_armed_voucher_keeps_reveals_released_after_it() -> anyhow::Result<()> {
    let ledger = metered_ledger(Cumulative::default());
    ledger
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    let anchor = ledger.committed();
    ledger
        .meter(ledger.chain_root(), |_r| async { Ok(()) })
        .await?;
    let chunk = ledger.committed().minus(anchor);
    assert!(!chunk.amount.is_zero(), "a reveal accrues one chunk");

    // The closing voucher folds that reveal (so it rolls), and its write fails.
    let mut sent = None;
    let failed = ledger
        .issue(100, 10, EpochAction::Keep, |next, _c| {
            sent = Some(next);
            async { anyhow::bail!("send stopped") }
        })
        .await;
    assert!(failed.is_err());
    let armed = sent.ok_or_else(|| anyhow::anyhow!("issue never reached the send"))?;

    // A sibling releases a reveal on the rolled chain before the confirm.
    ledger
        .meter(ledger.chain_root(), |_r| async { Ok(()) })
        .await?;
    let owed_before_confirm = ledger.committed();

    assert_eq!(ledger.confirm_armed(armed.amount).await, Some(armed));
    assert_eq!(
        ledger.committed(),
        armed.plus(chunk),
        "the sibling's reveal survives the confirm"
    );
    assert_eq!(ledger.settlement(), ledger.committed());

    assert!(ledger.resolve_reject(voucher_proof(armed)));
    assert_eq!(
        ledger.committed(),
        owed_before_confirm,
        "rejecting the confirmed voucher gives back its fold on top of the later reveal"
    );
    Ok(())
}

/// A `StreamEnd` read on one stream never confirms a voucher a sibling armed
/// on top of its own. Both sends failed, so the lane settles high on the
/// sibling's voucher, and only the sibling's own `StreamEnd` commits it.
#[tokio::test]
async fn confirming_skips_a_sibling_voucher_armed_on_top() -> anyhow::Result<()> {
    let ledger = PoolLedger::new(Cumulative::default());
    let mut ours = None;
    let mut theirs = None;
    let first = ledger
        .issue(100, 10, EpochAction::Keep, |next, _c| {
            ours = Some(next);
            async { anyhow::bail!("send stopped") }
        })
        .await;
    let second = ledger
        .issue(100, 10, EpochAction::Keep, |next, _c| {
            theirs = Some(next);
            async { anyhow::bail!("send stopped") }
        })
        .await;
    assert!(first.is_err() && second.is_err());
    let ours = ours.ok_or_else(|| anyhow::anyhow!("first issue never sent"))?;
    let theirs = theirs.ok_or_else(|| anyhow::anyhow!("second issue never sent"))?;
    assert!(theirs.amount > ours.amount, "the sibling built on ours");

    assert_eq!(ledger.confirm_armed(ours.amount).await, None);
    assert_eq!(ledger.committed(), Cumulative::default());
    assert_eq!(ledger.settlement(), theirs, "the lane still settles high");

    assert_eq!(ledger.confirm_armed(theirs.amount).await, Some(theirs));
    assert_eq!(ledger.committed(), theirs);
    Ok(())
}

/// A send that stalls past the lane's deadline fails the issue with an error
/// — releasing the issuance lock so the pull leg fails over — rather than
/// blocking every concurrent pull on the lane forever. The stalled voucher is
/// treated as any ambiguous send: it stays armed and `settlement` settles
/// high, while `committed` does not advance.
#[tokio::test]
async fn a_send_past_the_deadline_errors_instead_of_wedging() -> anyhow::Result<()> {
    let ledger =
        PoolLedger::new(Cumulative::default()).with_send_deadline(Duration::from_millis(20));
    let result = ledger
        .issue(100, 10, EpochAction::Keep, |_next, _chain| {
            // Upstream stopped reading: this send never completes.
            std::future::pending::<anyhow::Result<()>>()
        })
        .await;
    assert!(
        result.is_err(),
        "a send past the deadline must surface an error, not hang"
    );
    assert_eq!(
        ledger.committed(),
        Cumulative::default(),
        "a timed-out send never advances the committed watermark"
    );
    assert_eq!(
        ledger.settlement().bytes,
        U256::from(100u64),
        "an ambiguous timed-out send settles high on the armed voucher"
    );

    // The lock is free again: a subsequent issue on the same ledger proceeds.
    ledger
        .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
        .await?;
    Ok(())
}

/// The window this exists to close (#1122): a pull dropped inside the send
/// leaves the upstream possibly holding a voucher we have no committed record
/// of. Settle low and the deposit is stranded; settle high and it is honoured.
#[tokio::test]
async fn a_pull_dropped_inside_the_send_settles_at_the_voucher_it_sent() {
    let ledger = PoolLedger::new(Cumulative::default());
    let dropped = tokio::time::timeout(
        Duration::from_millis(20),
        ledger.issue(100, 10, EpochAction::Keep, |_next, _chain| {
            std::future::pending::<anyhow::Result<()>>()
        }),
    )
    .await;
    assert!(dropped.is_err(), "the send must still be in flight");

    assert_eq!(
        ledger.committed(),
        Cumulative::default(),
        "an unconfirmed voucher must never advance the committed watermark"
    );
    let settled = ledger.settlement();
    assert_eq!(
        settled.bytes,
        U256::from(100u64),
        "settle at the sent voucher"
    );
    assert_eq!(settled.amount, U256::from(1u64));
}

/// A voucher the upstream explicitly REJECTED was never taken, so settling at
/// it would inflate our cumulative. `resolve_reject` rewinds committed WITHOUT
/// keeping it, so — with nothing else in flight — settlement falls back.
#[tokio::test]
async fn a_rejected_voucher_is_not_settled_optimistically() -> anyhow::Result<()> {
    let ledger = PoolLedger::new(Cumulative::default());
    // Issue + successful send: committed advances to the voucher.
    let sent = ledger
        .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
        .await?;
    assert_eq!(ledger.committed().bytes, U256::from(100u64));
    // The upstream rejects it (arrived as a mid-stream VoucherRejected).
    assert!(
        ledger.resolve_reject(voucher_proof(sent)),
        "the committed voucher is rewound"
    );
    assert_eq!(
        ledger.settlement(),
        Cumulative::default(),
        "an explicitly rejected voucher must not advance what we persist"
    );
    Ok(())
}

/// A ledger for the metered tests below. Identical to any other — a chain
/// draws its own secret, so metering needs no identity and no key material.
fn metered_ledger(seed: Cumulative) -> PoolLedger {
    PoolLedger::new(seed)
}

/// Name the voucher a successful `issue` just put on the wire, the way the
/// issuing stream does.
fn voucher_proof(sent: Cumulative) -> StreamProof {
    StreamProof::Voucher {
        amount: sent.amount,
        generation: 0,
    }
}

/// Name the reveal a successful `meter` just put on the wire, the way the
/// releasing stream does.
fn reveal_proof(metered: Metered) -> anyhow::Result<StreamProof> {
    match metered {
        Metered::Released(released) => Ok(StreamProof::Reveal {
            chain_root: released.chain_root,
            index: released.index,
        }),
        Metered::Exhausted => anyhow::bail!("the epoch was exhausted; nothing was released"),
        Metered::Moved => {
            anyhow::bail!("the lane moved to another chain; nothing was released")
        }
    }
}

/// A rejected REVEAL must not touch the signed anchor.
///
/// The wire rejection names no proof, and the old one-step `committed → prev`
/// rewind assumed it was always a voucher. Under the chain it often is not: a
/// refused reveal would un-commit a voucher the node ACCEPTED, dropping the
/// payer's anchor permanently below the node's — after which every voucher it
/// signs regresses and every re-anchor states a cumulative the node passed
/// long ago.
#[tokio::test]
async fn a_rejected_reveal_rewinds_the_chunk_not_the_anchor() -> anyhow::Result<()> {
    let ledger = metered_ledger(Cumulative::default());
    // A voucher the node ACCEPTS, then two reveals on top of it.
    ledger
        .issue(1_000, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    let anchor = ledger.committed();
    ledger
        .meter(ledger.chain_root(), |_r| async { Ok(()) })
        .await?;
    let second = ledger
        .meter(ledger.chain_root(), |_r| async { Ok(()) })
        .await?;
    let two_reveals = ledger.committed();
    assert!(two_reveals.amount > anchor.amount);

    assert!(
        ledger.resolve_reject(reveal_proof(second)?),
        "a released reveal is rewindable"
    );
    assert_eq!(
        ledger.committed().amount,
        two_reveals.amount - U256::from(10u64),
        "exactly one chunk comes off"
    );

    // And the anchor the next voucher builds on is untouched: re-anchoring
    // still states what the node accepted, not something behind it.
    let stated = std::sync::Mutex::new(None);
    ledger
        .reanchor(|cum, _chain| {
            *stated
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cum);
            async { Ok(()) }
        })
        .await?;
    assert_eq!(
        stated
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        Some(anchor),
        "the signed anchor must survive a rejected reveal"
    );
    Ok(())
}

/// The index rewinds with the money, because the index IS the money: a claim
/// is `anchor + index × chunk_price`. A payer that gave back the chunk but let
/// the next reveal go one deeper would be charged for the depth it skipped,
/// and the two sides would diverge by exactly one chunk from then on.
#[tokio::test]
async fn a_rejected_reveal_gives_back_its_index_too() -> anyhow::Result<()> {
    let ledger = metered_ledger(Cumulative::default());
    ledger
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    let mut last = None;
    for _ in 0..3u8 {
        last = Some(
            ledger
                .meter(ledger.chain_root(), |_r| async { Ok(()) })
                .await?,
        );
    }
    let third = last.ok_or_else(|| anyhow::anyhow!("no reveal was released"))?;
    assert!(ledger.resolve_reject(reveal_proof(third)?));

    let next = std::sync::Mutex::new(None);
    ledger
        .meter(ledger.chain_root(), |r| {
            *next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(r.index);
            async { Ok(()) }
        })
        .await?;
    assert_eq!(
        next.into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        Some(3),
        "the refused depth is released again, not skipped"
    );
    Ok(())
}

/// A stream anchored to a chain a sibling has since rolled away from gets
/// `Moved` from `meter`: the node would place its reveal against the old
/// root and fold nothing. Nothing goes on the wire and nothing accrues, so
/// the stream re-anchors and meters again.
#[tokio::test]
async fn a_reveal_under_a_moved_root_is_not_released() -> anyhow::Result<()> {
    let ledger = metered_ledger(Cumulative::default());
    ledger
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    let anchored = ledger.chain_root();
    ledger.meter(anchored, |_r| async { Ok(()) }).await?;
    // A sibling rolls the lane.
    ledger
        .issue(0, 10, EpochAction::Roll, |_n, _c| async { Ok(()) })
        .await?;
    anyhow::ensure!(
        ledger.chain_root() != anchored,
        "the roll opens a new chain"
    );
    let before = ledger.settlement();

    let mut sent = false;
    let metered = ledger
        .meter(anchored, |_r| {
            sent = true;
            async { Ok(()) }
        })
        .await?;
    assert_eq!(metered, Metered::Moved);
    assert!(!sent, "no reveal goes on the wire");
    assert_eq!(ledger.settlement(), before, "nothing accrues");

    let live = ledger
        .meter(ledger.chain_root(), |_r| async { Ok(()) })
        .await?;
    assert_matches!(
        live,
        Metered::Released(_),
        "the re-anchored stream meters the live chain: {live:?}"
    );
    Ok(())
}

/// A rejection is rewound against the proof it was FOR, not against whatever
/// the lane did last.
///
/// One `PoolLedger` is shared by every stream on a lane (`stream_fetch_shared`,
/// the node's `BuyerLedgers`), and a rejection is read on the stream that
/// earned it — so the two can interleave: stream A releases a reveal, stream B
/// rolls and its voucher is ACCEPTED, and only then does A read the rejection
/// for its reveal. Keyed to the lane's last proof, that rejection would find a
/// voucher in the slot and un-commit B's accepted anchor, leaving the payer
/// permanently below the node's watermark — every later voucher regresses, and
/// the accrual B folded is re-added on top of an anchor that never advanced.
#[tokio::test]
async fn a_stale_rejection_cannot_uncommit_a_siblings_accepted_voucher() -> anyhow::Result<()> {
    let ledger = metered_ledger(Cumulative::default());
    ledger
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    // Stream A releases a reveal.
    let a_reveal = ledger
        .meter(ledger.chain_root(), |_r| async { Ok(()) })
        .await?;
    // Stream B rolls; the node accepts the rollover (continued delivery IS
    // acceptance), so the lane's anchor is now B's.
    ledger
        .issue(0, 10, EpochAction::Roll, |_n, _c| async { Ok(()) })
        .await?;
    let accepted = ledger.committed();
    let live = ledger.chain_root();

    // Only now does A read the rejection for its reveal.
    assert!(
        !ledger.resolve_reject(reveal_proof(a_reveal)?),
        "a proof the lane has moved past rewinds nothing"
    );
    assert_eq!(
        ledger.committed(),
        accepted,
        "B's accepted anchor must survive A's stale rejection"
    );
    assert_eq!(
        ledger.chain_root(),
        live,
        "and the chain B opened must stay live"
    );
    Ok(())
}

/// A rejected ROLLOVER puts the chain back.
///
/// The chain is swapped in before the send — the voucher has to carry the new
/// root to be signed at all — so a refusal would otherwise leave the payer
/// metering a chain the node never adopted: its reveals name a root the lane
/// does not track and fold nothing, and its next re-anchor states a root the
/// node will not accept over a frontier it has already proved.
#[tokio::test]
async fn a_rejected_rollover_puts_the_chain_back() -> anyhow::Result<()> {
    let ledger = metered_ledger(Cumulative::default());
    ledger
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    let live = ledger.chain_root();
    ledger
        .meter(ledger.chain_root(), |_r| async { Ok(()) })
        .await?;

    let rolled = ledger
        .issue(0, 10, EpochAction::Roll, |_n, _c| async { Ok(()) })
        .await?;
    assert_ne!(ledger.chain_root(), live, "the roll drew a fresh chain");

    assert!(ledger.resolve_reject(voucher_proof(rolled)));
    assert_eq!(
        ledger.chain_root(),
        live,
        "a refused rollover leaves the lane on the chain the node still meters"
    );
    Ok(())
}

/// A rejected voucher that displaced NO chain must not resurrect an earlier
/// one. The displaced slot records "nothing" as distinctly as it records a
/// chain, or a refused residual would hand the lane back a retired root.
#[tokio::test]
async fn a_rejected_non_rolling_voucher_leaves_the_chain_alone() -> anyhow::Result<()> {
    let ledger = metered_ledger(Cumulative::default());
    ledger
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    ledger
        .issue(0, 10, EpochAction::Roll, |_n, _c| async { Ok(()) })
        .await?;
    let live = ledger.chain_root();
    // A residual voucher: `Keep`, with nothing accrued, so it displaces nothing.
    let residual = ledger
        .issue(100, 10, EpochAction::Keep, |_n, _c| async { Ok(()) })
        .await?;
    assert!(ledger.resolve_reject(voucher_proof(residual)));
    assert_eq!(
        ledger.chain_root(),
        live,
        "a voucher that displaced no chain restores no chain"
    );
    Ok(())
}

/// The bundle decodes to the lane's FULL claim, not just its signed anchor.
///
/// A node that rejects mid-chain reports an anchor plus the frontier its chain
/// has proved on top. A signer that re-seeded from the anchor alone would open
/// a fresh root beside a frontier the node is still metering, and every reveal
/// after that would fold nothing — the resume would never converge. The fold
/// is the node's documented side of the bargain (ADR 005 §Watermark bundle).
#[test]
fn a_bundle_decodes_with_its_proved_frontier_folded_in() {
    let bundle = WatermarkBundle {
        chain_root: [0x9Au8; 32],
        verified_index: 3,
        tip: [0x9Bu8; 32],
        chunk_price: 10,
        amount: 50,
        bytes_delivered: 5_000,
        last_signature: vec![0xCDu8; 65],
    };
    let cum = Cumulative::from(&bundle);
    assert_eq!(
        cum.amount,
        U256::from(80u64),
        "50 anchored + 3 chunks proved at 10 each"
    );
    assert_eq!(
        cum.bytes,
        U256::from(5_000u64) + U256::from(3u64) * U256::from(CHUNK_BYTES),
        "the byte axis folds the same three chunks"
    );
}

/// Re-seeding retires the live chain, because the cumulative it installs has
/// already folded that chain's frontier. Keeping it would leave the next
/// voucher re-committing a root the new anchor absorbed, and the node would
/// count those chunks twice — the same double-count the fold-must-roll rule
/// prevents on the issue path.
#[tokio::test]
async fn reseeding_retires_the_chain_whose_frontier_it_folded() -> anyhow::Result<()> {
    let ledger = metered_ledger(Cumulative::default());
    ledger
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    let opened = ledger
        .chain_root()
        .ok_or_else(|| anyhow::anyhow!("Open draws a chain"))?;

    let bundle = WatermarkBundle {
        chain_root: [0x9Au8; 32],
        verified_index: 2,
        tip: [0x9Bu8; 32],
        chunk_price: 10,
        amount: 500,
        bytes_delivered: 5_000,
        last_signature: vec![0xCDu8; 65],
    };
    assert!(ledger.reseed(Cumulative::from(&bundle)));
    assert_eq!(
        ledger.chain_root(),
        None,
        "the folded chain must not survive the reseed"
    );

    // And the chain drawn next is a genuinely new one: the fold moved the
    // anchor, and the anchor is what seeds the root.
    ledger
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    assert_ne!(ledger.chain_root(), Some(opened));
    Ok(())
}

/// A new process's stale-root state (#2257): it opens a fresh root at the
/// signed anchor and releases `reveals` under it before the node's
/// `UnderFold` lands, and the rejection rewinds only the last reveal. The
/// node's bundle states the anchor plus `frontier` proved chunks. Returns the
/// ledger, the refused root, the bundle, and the ledger's settlement after
/// the rewind.
async fn refused_root_state(
    reveals: u8,
    frontier: u8,
) -> anyhow::Result<(PoolLedger, B256, Cumulative, Cumulative)> {
    let anchor = Cumulative {
        bytes: U256::from(3 * CHUNK_BYTES),
        amount: U256::from(300u64),
    };
    let ledger = metered_ledger(anchor);
    ledger
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    let refused = ledger
        .chain_root()
        .ok_or_else(|| anyhow::anyhow!("Open draws a chain"))?;
    let mut last = None;
    for _ in 0..reveals {
        last = Some(ledger.meter(Some(refused), |_r| async { Ok(()) }).await?);
    }
    let chunk = ledger
        .committed()
        .minus(anchor)
        .amount
        .checked_div(U256::from(reveals))
        .ok_or_else(|| anyhow::anyhow!("at least one reveal"))?;
    let last = last.ok_or_else(|| anyhow::anyhow!("at least one reveal"))?;
    assert!(ledger.resolve_reject(reveal_proof(last)?));
    let bundle = Cumulative {
        bytes: anchor.bytes + U256::from(u64::from(frontier) * CHUNK_BYTES),
        amount: anchor.amount + chunk * U256::from(frontier),
    };
    let settled = ledger.settlement();
    Ok((ledger, refused, bundle, settled))
}

fn under_fold() -> anyhow::Error {
    anyhow::Error::new(UpstreamVoucherRejected {
        reason: VoucherRejectReason::UnderFold,
        bundle: None,
        proof_generation: None,
    })
}

/// The stale-root state heals in one step (#2257). The ledger already covers
/// the bundle, so a reseed refuses it, and a retry that re-anchors under the
/// refused root draws the same `UnderFold`. The heal folds the ledger and
/// retires the root instead, and the next voucher is an Open over a fresh
/// root that folds at least the node's claim — the voucher a new process
/// would send. A sibling stream's echo of the same rejection then leaves
/// the fresh chain alone.
#[tokio::test]
async fn an_under_fold_the_ledger_covers_retires_the_refused_root() -> anyhow::Result<()> {
    let (ledger, refused, bundle, settled) = refused_root_state(4, 2).await?;
    assert!(
        settled.amount > bundle.amount,
        "the ledger is past the bundle"
    );
    assert!(!ledger.reseed(bundle), "a covered bundle does not reseed");

    let err = under_fold();
    assert_eq!(
        crate::heal_watermark_desync(&err, bundle, &ledger).await,
        Some(crate::Healed::Stale)
    );
    assert_eq!(ledger.chain_root(), None, "the refused root is retired");
    assert_eq!(ledger.settlement(), settled, "the fold moves no money");

    let mut commit = None;
    let opened = ledger
        .issue(0, 10, EpochAction::Open, |_n, c| {
            commit = Some(c);
            async { Ok(()) }
        })
        .await?;
    let commit = commit.ok_or_else(|| anyhow::anyhow!("issue never reached the send"))?;
    assert!(opened.amount >= bundle.amount && opened.bytes >= bundle.bytes);
    assert_eq!(opened, settled, "the Open signs the whole fold");
    assert_ne!(commit.chain_root, refused, "the Open names a fresh root");
    assert_ne!(commit.chain_root, B256::ZERO, "the Open is not sealed");

    let fresh = ledger.chain_root();
    assert_eq!(
        crate::heal_watermark_desync(&err, bundle, &ledger).await,
        Some(crate::Healed::Stale)
    );
    assert_eq!(
        ledger.chain_root(),
        fresh,
        "a sibling's echo of a healed bundle keeps the fresh chain"
    );
    Ok(())
}

/// The same heal when the reveals that survive the rewind exactly match the
/// node's frontier: the ledger then sits at the bundle, as it does after a
/// sibling's reseed. No heal has taken this bundle, so the root still
/// retires. A reseed that took it first would leave the chain alone.
#[tokio::test]
async fn an_under_fold_at_the_ledger_retires_the_refused_root_once() -> anyhow::Result<()> {
    let (ledger, _refused, bundle, settled) = refused_root_state(3, 2).await?;
    assert_eq!(settled, bundle, "the ledger sits exactly at the bundle");
    assert_eq!(
        crate::heal_watermark_desync(&under_fold(), bundle, &ledger).await,
        Some(crate::Healed::Stale)
    );
    assert_eq!(ledger.chain_root(), None);

    let reseeded = metered_ledger(Cumulative::default());
    assert!(reseeded.reseed(bundle));
    reseeded
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    let fresh = reseeded.chain_root();
    assert_eq!(
        crate::heal_watermark_desync(&under_fold(), bundle, &reseeded).await,
        Some(crate::Healed::Stale)
    );
    assert_eq!(reseeded.chain_root(), fresh, "the reseed already healed it");
    Ok(())
}

/// Issue #1481: a wallet-less client cannot reconstruct its watermark from
/// chain, so a gated rejection carries the node's true watermark back in a
/// (nonce-free) `WatermarkBundle`. `Cumulative::from` must decode it losslessly.
#[test]
fn cumulative_from_bundle_is_lossless() {
    let bundle = WatermarkBundle {
        chain_root: [0u8; 32],
        verified_index: 0,
        tip: [0u8; 32],
        chunk_price: 0,
        amount: u64::MAX,
        bytes_delivered: 1_048_576u64,
        last_signature: vec![0xABu8; 65],
    };
    let cum = Cumulative::from(&bundle);
    assert_eq!(cum.amount, U256::from(u64::MAX));
    assert_eq!(cum.bytes, U256::from(1_048_576u64));
}

/// The self-heal itself: an `AmountRegression` rejection with an authenticated
/// bundle is not a dead end. `reseed` overwrites committed to the node's true
/// state and clears the armed/rewind state, so the next `issue` builds on the
/// bundle rather than colliding with what the node already holds.
#[tokio::test]
async fn an_amount_regression_rejection_with_a_bundle_self_heals() -> anyhow::Result<()> {
    // The caller's local ledger thinks it is at amount 10 (a wallet-less
    // delegate that never persisted the true watermark), but the node's true
    // watermark — echoed on the gated reject — is amount 50.
    let ledger = PoolLedger::new(Cumulative {
        bytes: U256::from(1000u64),
        amount: U256::from(10u64),
    });
    let bundle = WatermarkBundle {
        chain_root: [0u8; 32],
        verified_index: 0,
        tip: [0u8; 32],
        chunk_price: 0,
        amount: 50u64,
        bytes_delivered: 5000u64,
        last_signature: vec![0xCDu8; 65],
    };

    // A voucher armed then ambiguously failed (settle high) before the reject.
    let _ = ledger
        .issue(100, 10, EpochAction::Keep, |_next, _chain| async {
            anyhow::bail!("ambiguous")
        })
        .await;
    assert!(ledger.settlement().amount > U256::from(10u64));

    // Self-heal: re-seed to the node's authenticated watermark.
    assert!(
        ledger.reseed(Cumulative::from(&bundle)),
        "a bundle ahead of committed must be applied"
    );
    assert_eq!(
        ledger.settlement().amount,
        U256::from(50u64),
        "reseed cleared the armed voucher and reset to the bundle watermark"
    );

    let issued = ledger
        .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
        .await?;
    assert_eq!(issued.bytes, U256::from(5100u64)); // bundle.bytes_delivered + 100
    assert_eq!(issued.amount, U256::from(51u64)); // bundle.amount + ceil(100*10/MiB)
    Ok(())
}

/// The monotonicity guard (#1497 review): a bundle that does NOT advance past
/// the committed watermark must be refused, leaving the ledger untouched. The
/// node attaches a bundle to EVERY watermark-gated rejection once any voucher
/// has been accepted — including a genuinely exhausted lane, whose bundle just
/// echoes the watermark the client already holds.
#[tokio::test]
async fn reseed_refuses_a_bundle_that_does_not_advance_the_watermark() -> anyhow::Result<()> {
    let committed = Cumulative {
        bytes: U256::from(5000u64),
        amount: U256::from(50u64),
    };
    let ledger = PoolLedger::new(committed);
    ledger
        .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
        .await?;
    let after_issue = ledger.committed();

    // The exhausted-lane echo: same amount we already hold.
    let echo = Cumulative {
        bytes: after_issue.bytes,
        amount: after_issue.amount,
    };
    assert!(
        !ledger.reseed(echo),
        "a bundle at the committed watermark proves nothing and must be refused"
    );
    // And the strictly-behind case must not rewind us either.
    let behind = Cumulative {
        bytes: U256::from(2000u64),
        amount: U256::from(20u64),
    };
    assert!(
        !ledger.reseed(behind),
        "a bundle behind committed must be refused"
    );
    assert_eq!(
        ledger.committed(),
        after_issue,
        "committed must never regress"
    );
    Ok(())
}

/// An `Underpaid` rejection says the ledger ran AHEAD of the node. `rebase`
/// moves the committed watermark DOWN to the node's, retires the live chain,
/// bumps the generation, and the next voucher builds on the node's anchor.
#[tokio::test]
async fn rebase_moves_the_watermark_down_to_the_nodes() -> anyhow::Result<()> {
    let ledger = metered_ledger(Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(90u64),
    });
    ledger
        .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
        .await?;
    assert!(ledger.chain_root().is_some());
    let before = ledger.settlement();

    let node = Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(60u64),
    };
    assert_eq!(
        ledger.rebase(node, Some(0)).await,
        Rebase::Rebased { from: before }
    );
    assert_eq!(ledger.generation(), 1);
    assert_eq!(ledger.committed(), node);
    assert_eq!(
        ledger.settlement(),
        node,
        "nothing stays armed above the node"
    );
    assert_eq!(ledger.chain_root(), None, "the old chain must not survive");

    let (next, generation) = ledger
        .issue_stamped(1_000, 10, EpochAction::Keep, |_n, _c| async { Ok(()) })
        .await?;
    assert_eq!(
        generation, 1,
        "vouchers after the rebase carry the new generation"
    );
    assert_eq!(next.bytes, U256::from(6_000u64));
    assert_eq!(next.amount, node.amount + min_payment_for(1_000, 10));
    Ok(())
}

/// `rebase` refuses a bundle at or above the committed watermark (that is
/// `reseed`'s case), treats an `Underpaid` for a voucher signed before the
/// latest rebase as stale, and rebases again on a fresh divergence.
#[tokio::test]
async fn rebase_skips_stale_rejections_and_heals_fresh_ones() -> anyhow::Result<()> {
    let seed = Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(90u64),
    };
    let ledger = PoolLedger::new(seed);
    assert_eq!(ledger.rebase(seed, None).await, Rebase::Refused, "an echo");
    let ahead = Cumulative {
        bytes: U256::from(10_000u64),
        amount: U256::from(100u64),
    };
    assert_eq!(
        ledger.rebase(ahead, None).await,
        Rebase::Refused,
        "reseed's case"
    );
    assert_eq!(ledger.generation(), 0);
    assert_eq!(ledger.committed(), seed);

    let node = Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(60u64),
    };
    assert_matches!(ledger.rebase(node, Some(0)).await, Rebase::Rebased { .. });
    let healed = ledger
        .issue(1_000, 10, EpochAction::Keep, |_n, _c| async { Ok(()) })
        .await?;

    // A sibling's voucher signed under generation 0 is stale.
    let stale = Cumulative {
        bytes: U256::from(4_000u64),
        amount: U256::from(50u64),
    };
    assert_eq!(ledger.rebase(stale, Some(0)).await, Rebase::Stale);
    assert_eq!(
        ledger.committed(),
        healed,
        "a stale rejection moves nothing"
    );

    // A voucher signed under generation 1 that still underpays is a fresh
    // divergence, and heals again.
    assert_matches!(ledger.rebase(stale, Some(1)).await, Rebase::Rebased { .. });
    assert_eq!(ledger.committed(), stale);
    assert_eq!(ledger.generation(), 2);
    Ok(())
}

/// A node watermark equal on `amount` but behind on `bytes` is still behind:
/// every span signed from ours looks short to the node. It rebases, and the
/// next voucher signs above that amount, so no equal-amount divergence
/// follows.
#[tokio::test]
async fn rebase_heals_a_watermark_behind_only_on_bytes() -> anyhow::Result<()> {
    let ledger = PoolLedger::new(Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(90u64),
    });
    let node = Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(90u64),
    };
    assert_matches!(ledger.rebase(node, None).await, Rebase::Rebased { .. });
    assert_eq!(ledger.committed(), node);
    let next = ledger
        .issue(1_000, 10, EpochAction::Keep, |_n, _c| async { Ok(()) })
        .await?;
    assert!(next.amount > node.amount);
    Ok(())
}

/// A rebase waits for a voucher already mid-send, so nothing signed from the
/// old anchor can commit over the healed one.
#[tokio::test]
async fn rebase_waits_for_an_in_flight_voucher() -> anyhow::Result<()> {
    let ledger = std::sync::Arc::new(PoolLedger::new(Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(90u64),
    }));
    let (release, parked) = tokio::sync::oneshot::channel::<()>();
    let (entered_tx, entered) = tokio::sync::oneshot::channel::<()>();
    let sender = {
        let ledger = std::sync::Arc::clone(&ledger);
        tokio::spawn(async move {
            ledger
                .issue(1_000, 10, EpochAction::Keep, |_n, _c| async move {
                    let _ = entered_tx.send(());
                    let _ = parked.await;
                    Ok(())
                })
                .await
        })
    };
    entered.await?;

    let node = Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(60u64),
    };
    let rebase = {
        let ledger = std::sync::Arc::clone(&ledger);
        tokio::spawn(async move { ledger.rebase(node, None).await })
    };
    tokio::task::yield_now().await;
    assert!(!rebase.is_finished(), "the rebase must wait for the send");

    let _ = release.send(());
    let stale = sender.await??;
    assert_matches!(rebase.await?, Rebase::Rebased { .. });
    assert!(stale.amount > node.amount);
    assert_eq!(
        ledger.committed(),
        node,
        "the healed anchor holds; the in-flight voucher did not commit over it"
    );
    Ok(())
}

/// The watermark a rebase moved down to is handed out exactly once, for the
/// persist that records it with an overwrite.
#[tokio::test]
async fn the_unsaved_rebase_is_taken_once() {
    let ledger = PoolLedger::new(Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(90u64),
    });
    assert_eq!(ledger.take_unsaved_rebase(), None);
    let node = Cumulative {
        bytes: U256::from(5_000u64),
        amount: U256::from(60u64),
    };
    assert_matches!(ledger.rebase(node, None).await, Rebase::Rebased { .. });
    assert_eq!(ledger.take_unsaved_rebase(), Some(node));
    assert_eq!(ledger.take_unsaved_rebase(), None);
}

/// The per-voucher price `next_voucher` charges for `bytes` at `rate`.
fn min_payment_for(bytes: u64, rate: u64) -> U256 {
    U256::from(bytes)
        .saturating_mul(U256::from(rate))
        .div_ceil(U256::from(MB_BYTES))
}

/// `SpendingCapExhausted` with NO bundle (a genuinely exhausted capability, nothing to
/// resume from) must not be treated as self-healable — a caller checking
/// `bundle.is_none()` sees the "give up / top up" signal. This pins the
/// type-shape contract the resume path depends on.
#[test]
fn cap_exceeded_without_a_bundle_is_not_self_healable() -> anyhow::Result<()> {
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: VoucherRejectReason::SpendingCapExhausted,
        bundle: None,
        proof_generation: None,
    });
    let upstream = err
        .downcast_ref::<UpstreamVoucherRejected>()
        .ok_or_else(|| anyhow::anyhow!("expected UpstreamVoucherRejected, got: {err:?}"))?;
    assert_eq!(upstream.reason, VoucherRejectReason::SpendingCapExhausted);
    assert!(
        upstream.bundle.is_none(),
        "no bundle means no self-heal path — the caller must surface a top-up need"
    );
    Ok(())
}

/// An AMBIGUOUS failure — a stall timeout, a transport reset — is not a
/// rejection: the upstream may already hold the voucher, so it must leave the
/// voucher ARMED and settle HIGH, exactly as a drop does.
#[tokio::test]
async fn an_ambiguous_failure_settles_high() {
    for make_err in [
        || {
            anyhow::Error::new(PullStalled {
                after: Duration::from_secs(1),
            })
        },
        || anyhow::anyhow!("connection reset by peer"),
    ] {
        let ledger = PoolLedger::new(Cumulative::default());
        let result = ledger
            .issue(
                100,
                10,
                EpochAction::Keep,
                move |_next, _chain| async move { Err(make_err()) },
            )
            .await;
        assert!(result.is_err(), "the ambiguous send must surface its error");
        assert_eq!(ledger.committed(), Cumulative::default());
        let settled = ledger.settlement();
        assert_eq!(
            settled.bytes,
            U256::from(100u64),
            "settle at the sent voucher"
        );
    }
}

/// After a voucher is armed (a failed send), the NEXT issue must build on it,
/// not re-sign the same cumulative — which the upstream may already hold.
#[tokio::test]
async fn a_later_issue_builds_on_an_armed_voucher() -> anyhow::Result<()> {
    let ledger = PoolLedger::new(Cumulative::default());
    let stalled = ledger
        .issue(100, 10, EpochAction::Keep, |_next, _chain| async {
            Err(anyhow::Error::new(PullStalled {
                after: Duration::from_secs(1),
            }))
        })
        .await;
    assert!(stalled.is_err());
    // Second issue must build on the armed voucher (bytes 200, not 100).
    let sent = ledger
        .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
        .await?;
    assert_eq!(
        sent.bytes,
        U256::from(200u64),
        "the next voucher must build on the armed voucher, not collide with it"
    );
    Ok(())
}

/// A sequence of successful issues stays ordered and exact — the committed
/// watermark is the running cumulative, never ahead of what was delivered.
#[tokio::test]
async fn a_sequence_of_issues_stays_ordered_and_exact() -> anyhow::Result<()> {
    let ledger = PoolLedger::new(Cumulative::default());
    for expected in 1..=100u64 {
        let sent = ledger
            .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
            .await?;
        assert_eq!(sent.bytes, U256::from(expected * 100));
    }
    let committed = ledger.committed();
    assert_eq!(committed.bytes, U256::from(10_000u64));
    assert_eq!(ledger.settlement(), committed);
    Ok(())
}

#[test]
fn bytes_accumulate_by_delta() {
    let cur = Cumulative {
        bytes: U256::from(1000u64),
        amount: U256::ZERO,
    };
    assert_eq!(next_voucher(&cur, 500, 10).bytes, U256::from(1500u64));
}

#[test]
fn amount_rounds_up_per_voucher() {
    // 1 byte at rate 10/MiB rounds up to 1 (not 0).
    let cur = Cumulative::default();
    assert_eq!(next_voucher(&cur, 1, 10).amount, U256::from(1u64));
    // The largest sub-MiB delta still rounds up to a full MiB's cost.
    assert_eq!(
        next_voucher(&cur, MB_BYTES - 1, 10).amount,
        U256::from(10u64)
    );
    // A full MiB at rate 10 costs exactly 10.
    assert_eq!(next_voucher(&cur, MB_BYTES, 10).amount, U256::from(10u64));
}

#[test]
fn zero_delta_bumps_nothing() {
    let cur = Cumulative {
        bytes: U256::from(7u64),
        amount: U256::from(3u64),
    };
    let next = next_voucher(&cur, 0, 99);
    assert_eq!(next.bytes, U256::from(7u64));
    assert_eq!(next.amount, U256::from(3u64));
}

/// A fully released epoch (255 reveals out) reports no next index rather
/// than computing `255 + 1` in a `u8`; the depth before it still yields the
/// last releasable index.
#[test]
fn spent_epoch_has_no_next_index_and_does_not_overflow() {
    let mut epoch = ChainEpoch::open(U256::from(1u64));
    epoch.released = MAX_CHAIN_LENGTH - 1;
    assert_eq!(epoch.next_index(), Some(MAX_CHAIN_LENGTH));
    epoch.released = MAX_CHAIN_LENGTH;
    assert_eq!(epoch.next_index(), None);
}
