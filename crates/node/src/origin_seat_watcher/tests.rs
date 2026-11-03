use alloy::primitives::{B256, Bytes, Log as PrimLog, LogData};

use super::*;

struct Stub(Vec<(U256, Address)>);

impl SeatReads for Stub {
    async fn snapshot_block(&self) -> Result<u64> {
        Ok(7)
    }

    async fn seats_at(&self, _at: u64) -> Result<Vec<(U256, Address)>> {
        Ok(self.0.clone())
    }
}

fn sink(seated: &Arc<SeatedOriginSet>, reseed: Vec<(U256, Address)>) -> SeatSink<Stub> {
    SeatSink {
        reads: Stub(reseed),
        seated: Arc::clone(seated),
        metrics: Arc::new(Metrics::new()),
        interval: RESEED_INTERVAL,
        last_reseed: Some(Instant::now()),
    }
}

fn event_log(topic0: B256, namespace: u64, operator: Address) -> Log {
    let topic_list = vec![
        topic0,
        B256::from(U256::from(namespace)),
        operator.into_word(),
        Address::ZERO.into_word(),
    ];
    Log {
        inner: PrimLog {
            address: Address::ZERO,
            data: LogData::new_unchecked(topic_list, Bytes::new()),
        },
        ..Log::default()
    }
}

#[tokio::test]
async fn seat_events_move_operators_in_and_out() {
    let seated = Arc::new(SeatedOriginSet::new());
    let mut s = sink(&seated, Vec::new());
    let op = Address::repeat_byte(1);
    s.apply(event_log(
        OriginAssignment::OriginAdded::SIGNATURE_HASH,
        3,
        op,
    ))
    .await
    .unwrap();
    assert!(seated.is_seated(&op));
    s.apply(event_log(
        OriginAssignment::InactiveOriginPruned::SIGNATURE_HASH,
        3,
        op,
    ))
    .await
    .unwrap();
    assert!(!seated.is_seated(&op));
}

#[tokio::test]
async fn a_due_reseed_replaces_the_set() {
    let seated = Arc::new(SeatedOriginSet::from_seats([(
        U256::from(1),
        Address::repeat_byte(1),
    )]));
    let mut s = sink(&seated, vec![(U256::from(2), Address::repeat_byte(2))]);
    s.last_reseed = None;
    s.on_tick_complete().await.unwrap();
    assert!(!seated.is_seated(&Address::repeat_byte(1)));
    assert!(seated.is_seated(&Address::repeat_byte(2)));
}

#[tokio::test]
async fn a_reseed_waits_for_its_interval() {
    let seated = Arc::new(SeatedOriginSet::from_seats([(
        U256::from(1),
        Address::repeat_byte(1),
    )]));
    let mut s = sink(&seated, Vec::new());
    s.on_tick_complete().await.unwrap();
    assert!(seated.is_seated(&Address::repeat_byte(1)));
}
