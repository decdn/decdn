use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256, U256};

use super::{ChainPoolOwners, PoolOwnerSource, PoolOwners};
use crate::pool_view::PoolProjection;

/// What the scripted chain answers for one pool.
#[derive(Clone, Copy)]
enum Answer {
    Owner(Address),
    Absent,
    Fail,
}

/// A [`PoolOwnerSource`] that answers from a script and counts reads per pool.
#[derive(Default)]
struct Scripted {
    answers: Mutex<HashMap<B256, Answer>>,
    reads: Mutex<HashMap<B256, usize>>,
}

impl Scripted {
    fn answer(&self, pool_id: B256, answer: Answer) {
        self.answers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(pool_id, answer);
    }

    fn reads(&self, pool_id: B256) -> usize {
        self.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pool_id)
            .copied()
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl PoolOwnerSource for Scripted {
    async fn owner(&self, pool_id: B256) -> anyhow::Result<Option<Address>> {
        *self
            .reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(pool_id)
            .or_insert(0) += 1;
        let answer = self
            .answers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pool_id)
            .copied()
            .unwrap_or(Answer::Absent);
        match answer {
            Answer::Owner(owner) => Ok(Some(owner)),
            Answer::Absent => Ok(None),
            Answer::Fail => Err(anyhow::anyhow!("scripted getPool failure")),
        }
    }
}

fn pool(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

fn owners_with(projection: &PoolProjection, source: &Arc<Scripted>) -> PoolOwners {
    PoolOwners::new(
        projection.clone(),
        Some(Arc::clone(source) as Arc<dyn PoolOwnerSource>),
    )
}

/// A pool the projection knows costs no chain read.
#[tokio::test]
async fn a_projection_hit_reads_nothing() {
    let projection = PoolProjection::new();
    let owner = Address::repeat_byte(0x0F);
    projection.record_opened(pool(1), owner, U256::from(1u64));
    let source = Arc::new(Scripted::default());

    let owners = owners_with(&projection, &source).resolve([pool(1)]).await;

    assert_eq!(owners.get(&pool(1)), Some(&owner));
    assert_eq!(source.reads(pool(1)), 0);
}

/// A pool the projection lacks, as every lane carried across a restart is, is
/// read once and then answered from the cache. Repeats in one call share the
/// read.
#[tokio::test]
async fn a_projection_miss_is_read_once_then_cached() {
    let projection = PoolProjection::new();
    let owner = Address::repeat_byte(0x0F);
    let source = Arc::new(Scripted::default());
    source.answer(pool(1), Answer::Owner(owner));
    let resolver = owners_with(&projection, &source);

    let first = resolver.resolve([pool(1), pool(1)]).await;
    let second = resolver.clone().resolve([pool(1)]).await;

    assert_eq!(first.get(&pool(1)), Some(&owner));
    assert_eq!(second.get(&pool(1)), Some(&owner));
    assert_eq!(source.reads(pool(1)), 1, "a pool's owner never changes");
}

/// A failed read leaves the owner unknown for that call only: it is not
/// cached, so the next call reads it again.
#[tokio::test]
async fn a_failed_read_is_unknown_and_retried() {
    let projection = PoolProjection::new();
    let owner = Address::repeat_byte(0x0F);
    let source = Arc::new(Scripted::default());
    source.answer(pool(1), Answer::Fail);
    let resolver = owners_with(&projection, &source);

    assert!(resolver.resolve([pool(1)]).await.is_empty());

    source.answer(pool(1), Answer::Owner(owner));
    assert_eq!(
        resolver.resolve([pool(1)]).await.get(&pool(1)),
        Some(&owner)
    );
    assert_eq!(source.reads(pool(1)), 2);
}

/// A pool the deployment does not have is unknown and is not cached: the id
/// can still open later.
#[tokio::test]
async fn an_absent_pool_is_unknown_and_not_cached() {
    let projection = PoolProjection::new();
    let source = Arc::new(Scripted::default());
    let resolver = owners_with(&projection, &source);

    assert!(resolver.resolve([pool(1)]).await.is_empty());
    assert!(resolver.resolve([pool(1)]).await.is_empty());
    assert_eq!(source.reads(pool(1)), 2);
}

/// One failing pool does not hide the owners of the pools beside it.
#[tokio::test]
async fn one_failure_does_not_hide_other_owners() {
    let projection = PoolProjection::new();
    let owner = Address::repeat_byte(0x0F);
    let source = Arc::new(Scripted::default());
    source.answer(pool(1), Answer::Fail);
    source.answer(pool(2), Answer::Owner(owner));

    let owners = owners_with(&projection, &source)
        .resolve([pool(1), pool(2)])
        .await;

    assert_eq!(owners.get(&pool(1)), None);
    assert_eq!(owners.get(&pool(2)), Some(&owner));
}

/// The resolver reads the live, shared projection: an owner the settlement
/// watcher folds after wiring is visible, and a reclaimed pool the watcher
/// forgets falls through to the chain read.
#[tokio::test]
async fn the_projection_is_read_live() {
    let projection = PoolProjection::new();
    let owner = Address::repeat_byte(0x0F);
    let resolver = PoolOwners::new(projection.clone(), None);

    assert!(resolver.resolve([pool(1)]).await.is_empty());

    projection.record_opened(pool(1), owner, U256::from(1u64));
    assert_eq!(
        resolver.resolve([pool(1)]).await.get(&pool(1)),
        Some(&owner)
    );

    projection.forget(pool(1));
    assert!(
        resolver.resolve([pool(1)]).await.is_empty(),
        "no chain source: a forgotten pool is unknown"
    );
}

/// With no chain source, a pool the projection lacks stays unknown.
#[tokio::test]
async fn no_source_leaves_misses_unknown() {
    let resolver = PoolOwners::new(PoolProjection::new(), None);
    assert!(resolver.resolve([pool(1)]).await.is_empty());
}

/// The chain read is bounded: a stalled provider reports an error, which the
/// resolver turns into an unknown owner, rather than wedging the admin call.
#[tokio::test(start_paused = true)]
async fn a_stalled_provider_bounds_the_owner_read() {
    use crate::chain_events::test_support::{bounded, hanging_provider};
    use decdn_incentive::payment_pool::PaymentPool;

    let source = ChainPoolOwners::new(PaymentPool::new(
        Address::repeat_byte(0x11),
        hanging_provider(),
    ));
    let err = bounded("admin getPool", source.owner(pool(1)))
        .await
        .err()
        .map(|e| format!("{e:#}"))
        .unwrap_or_default();
    assert!(err.contains("admin getPool timed out after"), "{err}");

    let resolver = PoolOwners::new(
        PoolProjection::new(),
        Some(Arc::new(source) as Arc<dyn PoolOwnerSource>),
    );
    assert!(
        bounded("admin lanes resolve", resolver.resolve([pool(1)]))
            .await
            .is_empty()
    );
}
