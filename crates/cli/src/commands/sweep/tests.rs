use std::time::Duration;

use alloy::primitives::{Bloom, Log as PrimitiveLog, LogData};
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;
use alloy::rpc::types::Log;
use alloy::sol_types::SolValue;
use decdn_incentive::Deployment;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

const OPERATOR: Address = Address::repeat_byte(0x0E);
const POOL: Address = Address::repeat_byte(0xB0);
const RECEIVER: Address = Address::repeat_byte(0xC5);
const TRANSMITTER: Address = Address::repeat_byte(0xD7);
const SWEEP_TX: B256 = B256::repeat_byte(0x55);

const OTHER: Address = Address::repeat_byte(0x0F);
const THIRD: Address = Address::repeat_byte(0x10);

const fn share(operator: Address, micro_usdc: u64) -> PendingShare {
    PendingShare {
        operator,
        micro_usdc,
    }
}

/// A batch total under `minSweep`, or an empty one, sends nothing; a total at
/// or above it is swept.
#[test]
fn plan_sweep_sends_at_or_above_min_sweep_only() {
    assert_eq!(
        plan_sweep(&[share(OPERATOR, 999)], 1_000, 32).unwrap(),
        SweepPlan::Below {
            total: 999,
            min_sweep: 1_000
        }
    );
    assert_eq!(
        plan_sweep(&[share(OPERATOR, 1_000)], 1_000, 32).unwrap(),
        SweepPlan::Send {
            operators: vec![OPERATOR],
            total: 1_000
        }
    );
    assert_eq!(
        plan_sweep(&[share(OPERATOR, 0)], 0, 32).unwrap(),
        SweepPlan::Below {
            total: 0,
            min_sweep: 0
        },
        "the pool never sends an empty batch"
    );
}

/// Small shares reach `minSweep` together; an operator with nothing pending
/// and a repeated one are left out, as the pool leaves them out.
#[test]
fn plan_sweep_batches_shares_and_leaves_out_empty_and_repeated_operators() {
    let shares = [
        share(OPERATOR, 400),
        share(OTHER, 0),
        share(THIRD, 700),
        share(OPERATOR, 400),
    ];
    assert_eq!(
        plan_sweep(&shares, 1_000, 32).unwrap(),
        SweepPlan::Send {
            operators: vec![OPERATOR, THIRD],
            total: 1_100
        }
    );
    assert_eq!(
        plan_sweep(&[share(OTHER, 0), share(THIRD, 700)], 0, 32).unwrap(),
        SweepPlan::Send {
            operators: vec![THIRD],
            total: 700
        },
        "the sweeping operator's own share may be empty"
    );
}

/// A batch over `MAX_SWEEP_BATCH()` is refused before anything is sent.
#[test]
fn plan_sweep_refuses_a_batch_over_the_maximum() {
    let shares = [share(OPERATOR, 1), share(OTHER, 1), share(THIRD, 1)];
    let err = plan_sweep(&shares, 0, 2).unwrap_err();
    assert!(err.to_string().contains("at most 2"), "{err}");
    assert!(
        plan_sweep(
            &[share(OPERATOR, 1), share(OTHER, 0), share(THIRD, 1)],
            0,
            2
        )
        .is_ok(),
        "an empty share does not count toward the maximum"
    );
}

fn log(address: Address, data: LogData) -> Log {
    Log {
        inner: PrimitiveLog { address, data },
        block_hash: None,
        block_number: Some(7),
        block_timestamp: None,
        transaction_hash: Some(SWEEP_TX),
        transaction_index: Some(0),
        log_index: Some(0),
        removed: false,
    }
}

fn receipt(logs: Vec<Log>) -> TransactionReceipt {
    TransactionReceipt {
        inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
            receipt: alloy::consensus::Receipt {
                status: alloy::consensus::Eip658Value::Eip658(true),
                cumulative_gas_used: 0,
                logs,
            },
            logs_bloom: Bloom::ZERO,
        }),
        transaction_hash: SWEEP_TX,
        transaction_index: Some(0),
        block_hash: None,
        block_number: Some(7),
        gas_used: 0,
        effective_gas_price: 0,
        blob_gas_used: None,
        blob_gas_price: None,
        from: OPERATOR,
        to: Some(POOL),
        contract_address: None,
    }
}

fn swept(operator: Address, amount: u64, bytes: u64) -> LogData {
    PaymentPool::Swept {
        operator,
        servedBytes: U256::from(bytes),
        amount: U256::from(amount),
    }
    .encode_log_data()
}

fn sweep_sent(amount: u64, max_fee: u64) -> LogData {
    PaymentPool::SweepSent {
        amount: U256::from(amount),
        maxFee: U256::from(max_fee),
    }
    .encode_log_data()
}

/// Each swept operator's share comes from the pool's own `Swept` events, in
/// batch order, and the total and fee from its `SweepSent`.
#[test]
fn swept_shares_and_total_are_read_from_the_pools_events() {
    let r = receipt(vec![
        log(Address::repeat_byte(0x01), swept(OPERATOR, 9, 9)),
        log(Address::repeat_byte(0x01), sweep_sent(9, 9)),
        log(POOL, swept(OPERATOR, 1_500_000, 4_096)),
        log(POOL, swept(OTHER, 500_000, 1_024)),
        log(POOL, sweep_sent(2_000_000, 7)),
    ]);
    let events = swept_from_receipt(&r, POOL);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].operator, OPERATOR);
    assert_eq!(events[0].amount, U256::from(1_500_000u64));
    assert_eq!(events[0].servedBytes, U256::from(4_096u64));
    assert_eq!(events[1].operator, OTHER);
    let sent = sweep_sent_from_receipt(&r, POOL).expect("the pool's SweepSent");
    assert_eq!(sent.amount, U256::from(2_000_000u64));
    assert_eq!(sent.maxFee, U256::from(7u64));
    assert!(swept_from_receipt(&receipt(vec![]), POOL).is_empty());
    assert!(sweep_sent_from_receipt(&receipt(vec![]), POOL).is_none());
}

/// The hook data is `abi.encode(SweepShare[])`: an offset word, the length,
/// then one `(operator, servedBytes, amount)` entry per operator. This pins
/// the decode to the layout `CctpSweepReceiver` decodes.
#[test]
fn sweep_shares_decode_the_hook_data_layout() {
    let mut hook = Vec::new();
    hook.extend_from_slice(U256::from(0x20u64).to_be_bytes::<32>().as_slice());
    hook.extend_from_slice(U256::from(2u64).to_be_bytes::<32>().as_slice());
    for (operator, bytes, amount) in [(OPERATOR, 4_096u64, 1_500_000u64), (OTHER, 1_024, 500_000)] {
        hook.extend_from_slice(operator.into_word().as_slice());
        hook.extend_from_slice(U256::from(bytes).to_be_bytes::<32>().as_slice());
        hook.extend_from_slice(U256::from(amount).to_be_bytes::<32>().as_slice());
    }
    let mut message = vec![0u8; HOOK_DATA];
    message.extend_from_slice(&hook);
    let shares = sweep_shares(&message).expect("the hook data decodes");
    assert_eq!(shares.len(), 2);
    assert_eq!(shares[0].operator, OPERATOR);
    assert_eq!(shares[0].servedBytes, U256::from(4_096u64));
    assert_eq!(shares[0].amount, U256::from(1_500_000u64));
    assert_eq!(shares[1].operator, OTHER);
    assert_eq!(
        hook,
        vec![
            PaymentPool::SweepShare {
                operator: OPERATOR,
                servedBytes: U256::from(4_096u64),
                amount: U256::from(1_500_000u64),
            },
            PaymentPool::SweepShare {
                operator: OTHER,
                servedBytes: U256::from(1_024u64),
                amount: U256::from(500_000u64),
            },
        ]
        .abi_encode(),
        "the encoder agrees with the hand-built layout"
    );
    assert!(sweep_shares(&[0u8; 10]).is_none(), "a short message");
}

/// A batch transaction is received once, whatever operators it carries.
#[test]
fn a_repeated_sweep_transaction_is_kept_once() {
    let other_tx = SweepTx {
        tx: B256::repeat_byte(0x66),
        ..sweep_tx()
    };
    assert_eq!(
        distinct(vec![sweep_tx(), other_tx.clone(), sweep_tx()]),
        vec![sweep_tx(), other_tx]
    );
}

/// A CCTP V2 message in Circle's `MessageV2` layout: version, source domain,
/// destination domain, nonce, sender, recipient, destination caller, two
/// finality thresholds, then a burn body whose hook data is a two-operator
/// batch: `OPERATOR` with 1.5 USDC and `OTHER` with 0.5 USDC.
fn cctp_message(source: u32, nonce: B256, caller: Address) -> Bytes {
    let mut m = Vec::new();
    m.extend_from_slice(&1u32.to_be_bytes());
    m.extend_from_slice(&source.to_be_bytes());
    m.extend_from_slice(&0u32.to_be_bytes());
    m.extend_from_slice(nonce.as_slice());
    m.extend_from_slice(B256::ZERO.as_slice());
    m.extend_from_slice(B256::ZERO.as_slice());
    m.extend_from_slice(caller.into_word().as_slice());
    m.extend_from_slice(&2_000u32.to_be_bytes());
    m.extend_from_slice(&2_000u32.to_be_bytes());
    m.extend_from_slice(&[0u8; 228]);
    m.extend_from_slice(
        &vec![
            PaymentPool::SweepShare {
                operator: OPERATOR,
                servedBytes: U256::from(4_096u64),
                amount: U256::from(1_500_000u64),
            },
            PaymentPool::SweepShare {
                operator: OTHER,
                servedBytes: U256::from(1_024u64),
                amount: U256::from(500_000u64),
            },
        ]
        .abi_encode(),
    );
    Bytes::from(m)
}

#[test]
fn the_message_header_names_domain_nonce_and_caller() {
    let message = cctp_message(3, B256::repeat_byte(0x9A), RECEIVER);
    assert_eq!(source_domain(&message), Some(3));
    assert_eq!(message_nonce(&message), Some(B256::repeat_byte(0x9A)));
    assert_eq!(destination_caller(&message), Some(RECEIVER));
    assert_eq!(
        source_domain(&[0u8; 3]),
        None,
        "a short message has no header"
    );
}

/// A two-operator batch sweep receipt: the pool's `Swept` events and
/// `SweepSent`, and the messenger's `MessageSent`, whose header names source
/// domain 3.
fn sweep_receipt() -> TransactionReceipt {
    let sent = MessageTransmitterV2::MessageSent {
        message: cctp_message(3, B256::ZERO, RECEIVER),
    };
    receipt(vec![
        log(POOL, swept(OPERATOR, 1_500_000, 4_096)),
        log(POOL, swept(OTHER, 500_000, 1_024)),
        log(POOL, sweep_sent(2_000_000, 0)),
        log(Address::repeat_byte(0xEE), sent.encode_log_data()),
    ])
}

/// An Iris server that answers pending once, then the attested message.
async fn iris(message: &Bytes) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/messages/3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"message": "0x", "attestation": "PENDING", "status": "pending_confirmations"}]
        })))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/messages/3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"message": message.to_string(), "attestation": "0xaabb", "status": "complete"}]
        })))
        .with_priority(2)
        .mount(&server)
        .await;
    server
}

fn sweep_tx() -> SweepTx {
    SweepTx {
        pool: PaymentPoolChain {
            rpc_url: "http://payment.invalid".to_owned(),
            deployment: Deployment {
                chain_id: 84_532,
                payment_pool: POOL,
            },
        },
        tx: SWEEP_TX,
    }
}

/// Receive one sweep against a mocked governance chain whose `usedNonces`
/// answers `used`.
async fn receive(used: bool, iris_message: Bytes, dry_run: bool) -> Vec<ReceiveLine> {
    let server = iris(&iris_message).await;
    let asserter = Asserter::new();
    asserter.push_success(&Bytes::from(U256::from(u8::from(used)).abi_encode()));
    let governance = ProviderBuilder::new().connect_mocked_client(asserter);
    let client = iris::client().unwrap();
    let wait = IrisWait {
        first: Duration::ZERO,
        max: Duration::ZERO,
        attempts: 3,
    };
    let uri = server.uri();
    let on = Receiving {
        governance: &governance,
        operator: OPERATOR,
        receiver: RECEIVER,
        iris: &uri,
        client: &client,
        wait: &wait,
        dry_run,
    };
    receive_one(&sweep_tx(), &sweep_receipt(), TRANSMITTER, &on)
        .await
        .unwrap()
}

/// A message the transmitter already marked used is skipped: another operator
/// of the batch, anyone else, or an earlier run received it. The line still
/// reports the batch size and this operator's entry from the hook data.
#[tokio::test]
async fn an_already_received_sweep_is_skipped() {
    let message = cctp_message(3, B256::repeat_byte(0x01), RECEIVER);
    let lines = receive(true, message, false).await;
    assert_eq!(
        lines,
        vec![ReceiveLine {
            pool: "eip155:84532:0xb0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0".to_owned(),
            sweep_tx: format!("{SWEEP_TX:#x}"),
            outcome: "already_received".to_owned(),
            batch_operators: Some(2),
            share_micro_usdc: Some(1_500_000),
            receive_tx: None,
        }]
    );
}

/// A pending attestation is waited out; an unused nonce is then ready to
/// receive (a dry run stops before the send).
#[tokio::test]
async fn a_pending_then_complete_sweep_is_ready_to_receive() {
    let message = cctp_message(3, B256::repeat_byte(0x02), RECEIVER);
    let lines = receive(false, message, true).await;
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].outcome, "dry_run");
}

/// A message whose destination caller is another contract is not ours to
/// receive, and no chain read is made for it.
#[tokio::test]
async fn a_message_for_another_receiver_is_left_alone() {
    let message = cctp_message(3, B256::repeat_byte(0x03), Address::repeat_byte(0x99));
    let server = iris(&message).await;
    let governance = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let client = iris::client().unwrap();
    let wait = IrisWait {
        first: Duration::ZERO,
        max: Duration::ZERO,
        attempts: 3,
    };
    let uri = server.uri();
    let on = Receiving {
        governance: &governance,
        operator: OPERATOR,
        receiver: RECEIVER,
        iris: &uri,
        client: &client,
        wait: &wait,
        dry_run: false,
    };
    let lines = receive_one(&sweep_tx(), &sweep_receipt(), TRANSMITTER, &on)
        .await
        .unwrap();
    assert_eq!(lines[0].outcome, "not_for_receiver");
}

/// A receipt with no CCTP message is not a sweep.
#[tokio::test]
async fn a_receipt_without_a_cctp_message_is_an_error() {
    let governance = ProviderBuilder::new().connect_mocked_client(Asserter::new());
    let client = iris::client().unwrap();
    let wait = IrisWait::default();
    let on = Receiving {
        governance: &governance,
        operator: OPERATOR,
        receiver: RECEIVER,
        iris: "http://iris.invalid",
        client: &client,
        wait: &wait,
        dry_run: true,
    };
    let err = receive_one(&sweep_tx(), &receipt(vec![]), TRANSMITTER, &on)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("MessageSent"), "{err}");
}
