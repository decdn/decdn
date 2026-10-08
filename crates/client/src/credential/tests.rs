use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::{Capability, SignedCapability};

use super::{
    CapabilityCause, Credential, CredentialSlot, CredentialView, FundingEvent,
    NODE_EXPIRY_MARGIN_SECS, QuoteMax,
};

const MB: u64 = decdn_protocol::MB_BYTES;

fn domain() -> alloy::sol_types::Eip712Domain {
    decdn_incentive::bind_node_id_domain(1, Address::ZERO)
}

fn capability(signer: &PrivateKeySigner, spending_cap: u64, expiry: u64) -> SignedCapability {
    Capability {
        signer: signer.address(),
        spending_cap,
        pool_id: B256::repeat_byte(0x11),
        expiry,
    }
    .sign(signer, &domain())
    .expect("sign")
}

fn credential(spending_cap: u64, expiry: u64) -> Credential {
    let signer = PrivateKeySigner::random();
    let capability = capability(&signer, spending_cap, expiry);
    Credential {
        signer: Arc::new(signer),
        capability,
    }
}

/// A quote of `rate` micro-USDC per MB with a one-MB voucher interval.
fn quoted(rate: u64) -> QuoteMax {
    let quotes = QuoteMax::default();
    quotes.observe(rate, MB);
    quotes
}

const NOW: u64 = 1_000_000;
const FAR: u64 = NOW + 10 * NODE_EXPIRY_MARGIN_SECS;

/// The signal fires when the headroom falls below the remaining work plus
/// ONE credit window, and not before.
#[test]
fn running_low_fires_below_the_work_plus_one_credit_window() {
    let quotes = quoted(10);
    // 4 MB left at 10 per MB needs 40, and one window is 10.
    let at = |signed: u64| {
        CredentialView::of(
            &credential(100, FAR),
            U256::from(signed),
            4 * MB,
            &quotes,
            NOW,
        )
    };

    let covered = at(50);
    assert_eq!(covered.headroom, U256::from(50));
    assert_eq!(covered.projected_need, U256::from(40));
    assert_eq!(covered.window, U256::from(10));
    assert_eq!(covered.event(), None, "50 covers 40 plus one window of 10");
    assert_eq!(covered.cause(), None);

    let low = at(51);
    assert_eq!(
        low.event(),
        Some(FundingEvent::RunningLow {
            headroom: U256::from(49),
            projected_need: U256::from(40),
            expires_at: FAR,
        })
    );
    assert_eq!(low.cause(), Some(CapabilityCause::CapSpent));
}

/// The signal fires inside the node's expiry margin, however much headroom
/// is left, and the cause is the expiry.
#[test]
fn running_low_fires_inside_the_node_expiry_margin() {
    let quotes = quoted(10);
    let expiring = CredentialView::of(
        &credential(u64::MAX, NOW + NODE_EXPIRY_MARGIN_SECS),
        U256::ZERO,
        MB,
        &quotes,
        NOW,
    );
    assert!(expiring.event().is_some());
    assert_eq!(expiring.cause(), Some(CapabilityCause::Expired));

    let outside = CredentialView::of(
        &credential(u64::MAX, NOW + NODE_EXPIRY_MARGIN_SECS + 1),
        U256::ZERO,
        MB,
        &quotes,
        NOW,
    );
    assert_eq!(outside.event(), None);
}

/// The projection prices the work at the HIGHEST rate any lane was quoted.
#[test]
fn the_projection_uses_the_highest_quote() {
    let quotes = quoted(10);
    quotes.observe(30, MB);
    quotes.observe(20, MB / 2);
    let view = CredentialView::of(&credential(1_000, FAR), U256::ZERO, 2 * MB, &quotes, NOW);
    assert_eq!(view.projected_need, U256::from(60));
    assert_eq!(view.window, U256::from(30));
}

/// A swap bumps the generation, wakes a waiter, and makes a context on the
/// old key stale.
#[tokio::test]
async fn a_swap_wakes_waiters_and_makes_the_old_key_stale() {
    let old = credential(100, FAR);
    let slot = CredentialSlot::new(Arc::clone(&old.signer), old.capability.clone());
    let ctx = std::sync::Mutex::new(crate::PoolContext::new(
        B256::repeat_byte(0x11),
        U256::from(1u64),
        Arc::clone(&old.signer),
        domain(),
    ));
    assert!(!slot.is_stale(&ctx));
    assert_eq!(slot.generation(), 0);

    let waiter = {
        let slot = slot.clone();
        tokio::spawn(async move { slot.swapped_since(0).await })
    };
    let new = credential(200, FAR);
    slot.swap(Arc::clone(&new.signer), new.capability.clone());
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the swap wakes the waiter")
        .expect("join");
    assert_eq!(slot.generation(), 1);
    assert!(slot.is_stale(&ctx), "a lane on the old key is stale");
    assert_eq!(slot.current().signer.address(), new.signer.address());
}

/// The funding signal publishes only on a change.
#[test]
fn the_funding_signal_publishes_on_change() {
    let cred = credential(100, FAR);
    let slot = CredentialSlot::new(cred.signer, cred.capability);
    let mut events = slot.funding_events();
    let low = Some(FundingEvent::RunningLow {
        headroom: U256::from(1),
        projected_need: U256::from(2),
        expires_at: FAR,
    });
    slot.report(None);
    assert!(!events.has_changed().expect("open"));
    slot.report(low);
    assert!(events.has_changed().expect("open"));
    assert_eq!(*events.borrow_and_update(), low);
    slot.report(low);
    assert!(!events.has_changed().expect("open"));
}
