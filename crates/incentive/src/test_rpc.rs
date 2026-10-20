//! A scripted RPC transport for tests that need a receipt wait to park.
//!
//! alloy's mock `Asserter` fails a call past its queue, so it cannot model a
//! transaction that is broadcast but never mines. [`ScriptedRpc`] answers the
//! pending-nonce read, the send, a queue of `eth_call`s and, when scripted, the
//! head block, and leaves every other call pending forever. Under a paused
//! tokio clock, a bounded receipt wait on it lapses as soon as the runtime
//! idles, because tokio advances the paused clock to the next timer; an
//! unbounded one never returns.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use alloy::consensus::{Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom};
use alloy::primitives::{Address, B256, Bloom, Bytes, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::{Block, Log, Transaction, TransactionReceipt};
use alloy::transports::{TransportError, TransportErrorKind, TransportFut};
use alloy_json_rpc::{RequestPacket, Response, ResponsePacket, ResponsePayload};

/// The hash [`ScriptedRpc`] returns for a send.
pub const SCRIPTED_TX: B256 = B256::repeat_byte(0xef);

/// The pending nonce [`ScriptedRpc`] returns.
pub const SCRIPTED_NONCE: u64 = 5;

/// A successful receipt for [`SCRIPTED_TX`], sent to `to`, that mined with
/// `logs`: one to hand [`ScriptedRpc::mined`].
#[must_use]
pub const fn mined_receipt(to: Address, logs: Vec<Log>) -> TransactionReceipt {
    TransactionReceipt {
        inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
            receipt: Receipt {
                status: Eip658Value::Eip658(true),
                cumulative_gas_used: 0,
                logs,
            },
            logs_bloom: Bloom::ZERO,
        }),
        transaction_hash: SCRIPTED_TX,
        transaction_index: Some(0),
        block_hash: Some(B256::repeat_byte(0x01)),
        block_number: Some(1),
        gas_used: 0,
        effective_gas_price: 0,
        blob_gas_used: None,
        blob_gas_price: None,
        from: Address::repeat_byte(0x22),
        to: Some(to),
        contract_address: None,
    }
}

/// Answers `eth_getTransactionCount` with [`SCRIPTED_NONCE`], and each
/// `eth_call` with the next answer in its queue. `eth_sendTransaction` gets
/// [`SCRIPTED_TX`] unless the send parks, and records the nonce it was sent
/// at. `eth_getTransactionReceipt` gets the scripted receipt when there is
/// one. `eth_getBlockByNumber`, whatever block it names, gets a block at the
/// timestamp [`ScriptedRpc::at_head`] set, with every other header field at its
/// default. Every other call, an `eth_call` past the end of the queue, and an
/// `eth_getBlockByNumber` with no head set, never completes, so a send on it
/// parks in its submit or its receipt wait. A [`ScriptedRpc::dropped`]
/// transport also answers the block number, and a receipt read before
/// [`ScriptedRpc::mine`] with "not mined".
///
/// A clone shares the receipt, the `eth_call` queue and the recorded nonces.
#[derive(Clone, Debug)]
pub struct ScriptedRpc {
    send: bool,
    receipt: Arc<Mutex<Option<TransactionReceipt>>>,
    /// Answer a receipt read with "not mined" while no receipt is scripted,
    /// instead of never completing.
    unmined_answers: bool,
    /// The block number `eth_blockNumber` answers, if it is answered.
    block: Option<u64>,
    /// The Unix timestamp of the head block `eth_getBlockByNumber` answers, if
    /// it is answered.
    head_timestamp: Option<u64>,
    calls: Arc<Mutex<VecDeque<Bytes>>>,
    sent_nonces: Arc<Mutex<Vec<u64>>>,
    after_calls: AfterCalls,
}

/// What runs once a [`ScriptedRpc`] hands out its last scripted `eth_call`
/// answer ([`ScriptedRpc::after_calls`]).
#[derive(Clone, Default)]
struct AfterCalls(Arc<Mutex<Option<Hook>>>);

/// A one-shot callback of a [`ScriptedRpc`].
type Hook = Box<dyn FnOnce() + Send>;

impl std::fmt::Debug for AfterCalls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AfterCalls")
    }
}

impl ScriptedRpc {
    /// A transport that returns [`SCRIPTED_TX`] for a send and never answers
    /// its receipt read: a broadcast transaction that never mines.
    #[must_use]
    pub fn parked_receipt() -> Self {
        Self {
            send: true,
            receipt: Arc::default(),
            unmined_answers: false,
            block: None,
            head_timestamp: None,
            calls: Arc::default(),
            sent_nonces: Arc::default(),
            after_calls: AfterCalls::default(),
        }
    }

    /// A transport whose sends are dropped from the mempool until
    /// [`Self::mine`]: a send returns [`SCRIPTED_TX`], a receipt read answers
    /// "not mined", the block number is `block`, and both the confirmed and the
    /// pending nonce are [`SCRIPTED_NONCE`], so no transaction holds it. A
    /// receipt wait still parks, because the block watcher alloy runs under it
    /// never gets an answer.
    #[must_use]
    pub fn dropped(block: u64) -> Self {
        Self {
            unmined_answers: true,
            block: Some(block),
            ..Self::parked_receipt()
        }
    }

    /// Answer every receipt read from now on with `receipt`: a transaction
    /// that mined late.
    pub fn mine(&self, receipt: TransactionReceipt) {
        *self.receipt.lock().unwrap_or_else(PoisonError::into_inner) = Some(receipt);
    }

    /// The nonce of each `eth_sendTransaction` this transport answered, in
    /// order. A send that names no nonce is not recorded.
    #[must_use]
    pub fn sent_nonces(&self) -> Vec<u64> {
        self.sent_nonces
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A transport whose send never completes: a submit still in flight.
    #[must_use]
    pub fn parked_send() -> Self {
        Self {
            send: false,
            ..Self::parked_receipt()
        }
    }

    /// A transport that returns [`SCRIPTED_TX`] for a send and `receipt` for
    /// its receipt read: a broadcast transaction that mined.
    #[must_use]
    pub fn mined(receipt: TransactionReceipt) -> Self {
        Self {
            receipt: Arc::new(Mutex::new(Some(receipt))),
            ..Self::parked_receipt()
        }
    }

    /// Answer the `eth_call`s this transport receives with `calls`, in order.
    #[must_use]
    pub fn answering_calls(self, calls: impl IntoIterator<Item = Bytes>) -> Self {
        Self {
            calls: Arc::new(Mutex::new(calls.into_iter().collect())),
            ..self
        }
    }

    /// Answer `eth_getBlockByNumber` with a head block at Unix `timestamp`: a
    /// chain-time read a caller makes before it sends.
    #[must_use]
    pub fn at_head(self, timestamp: u64) -> Self {
        Self {
            head_timestamp: Some(timestamp),
            ..self
        }
    }

    /// Run `hook` once, as this transport hands out its last scripted
    /// `eth_call` answer: a state change between that read and the caller's
    /// next step, which an always-ready transport gives no other task the
    /// chance to make.
    #[must_use]
    pub fn after_calls(self, hook: impl FnOnce() + Send + 'static) -> Self {
        Self {
            after_calls: AfterCalls(Arc::new(Mutex::new(Some(Box::new(hook))))),
            ..self
        }
    }

    /// Whether every scripted `eth_call` answer was read.
    #[must_use]
    pub fn calls_drained(&self) -> bool {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }

    /// A filler-free provider over this transport.
    #[must_use]
    pub fn provider(self) -> impl Provider + Clone {
        ProviderBuilder::default()
            .layer(crate::tx::SendLegLayer)
            .connect_client(RpcClient::new(self, false))
    }

    /// Record the nonce the `eth_sendTransaction` request `params` names.
    fn record_nonce(&self, params: Option<&serde_json::value::RawValue>) {
        let nonce = params
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw.get()).ok())
            .and_then(|value| {
                let hex = value.get(0)?.get("nonce")?.as_str()?.to_owned();
                u64::from_str_radix(hex.trim_start_matches("0x"), 16).ok()
            });
        if let Some(nonce) = nonce {
            self.sent_nonces
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(nonce);
        }
    }

    /// The next scripted `eth_call` answer, if any is left. Handing out the
    /// last one runs the [`Self::after_calls`] hook.
    fn next_call(&self) -> Option<Bytes> {
        let (answer, drained) = {
            let mut calls = self.calls.lock().unwrap_or_else(PoisonError::into_inner);
            (calls.pop_front(), calls.is_empty())
        };
        if answer.is_some() && drained {
            let hook = self
                .after_calls
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(hook) = hook {
                hook();
            }
        }
        answer
    }
}

/// A head block at Unix `timestamp`, in the shape `eth_getBlockByNumber`
/// returns without transaction bodies.
fn head_at(timestamp: u64) -> Block<Transaction> {
    let mut head = Block::<Transaction>::default();
    head.header.inner.timestamp = timestamp;
    head
}

impl tower::Service<RequestPacket> for ScriptedRpc {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let RequestPacket::Single(req) = req else {
            return Box::pin(std::future::pending());
        };
        let receipt = self
            .receipt
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let result = match (req.method(), receipt, self.block) {
            ("eth_getTransactionCount", ..) => {
                serde_json::value::to_raw_value(&U256::from(SCRIPTED_NONCE))
            }
            ("eth_call", ..) => match self.next_call() {
                Some(answer) => serde_json::value::to_raw_value(&answer),
                None => return Box::pin(std::future::pending()),
            },
            ("eth_sendTransaction", ..) if self.send => {
                self.record_nonce(req.params());
                serde_json::value::to_raw_value(&SCRIPTED_TX)
            }
            ("eth_getTransactionReceipt", Some(receipt), _) => {
                serde_json::value::to_raw_value(&receipt)
            }
            ("eth_getTransactionReceipt", None, _) if self.unmined_answers => {
                serde_json::value::to_raw_value(&serde_json::Value::Null)
            }
            ("eth_blockNumber", _, Some(block)) => {
                serde_json::value::to_raw_value(&U256::from(block))
            }
            ("eth_getBlockByNumber", ..) => match self.head_timestamp {
                Some(timestamp) => serde_json::value::to_raw_value(&head_at(timestamp)),
                None => return Box::pin(std::future::pending()),
            },
            _ => return Box::pin(std::future::pending()),
        };
        let id = req.id().clone();
        Box::pin(async move {
            Ok(ResponsePacket::Single(Response {
                id,
                payload: ResponsePayload::Success(result.map_err(TransportErrorKind::custom)?),
            }))
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
