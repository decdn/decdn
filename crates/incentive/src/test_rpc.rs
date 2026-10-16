//! A scripted RPC transport for tests that need a receipt wait to park.
//!
//! alloy's mock `Asserter` fails a call past its queue, so it cannot model a
//! transaction that is broadcast but never mines. [`ScriptedRpc`] answers the
//! pending-nonce read, the send, and a queue of `eth_call`s, and leaves every
//! other call pending forever. Under a paused tokio clock, a bounded receipt
//! wait on it lapses as soon as the runtime idles, because tokio advances the
//! paused clock to the next timer; an unbounded one never returns.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use alloy::primitives::{B256, Bytes, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::TransactionReceipt;
use alloy::transports::{TransportError, TransportErrorKind, TransportFut};
use alloy_json_rpc::{RequestPacket, Response, ResponsePacket, ResponsePayload};

/// The hash [`ScriptedRpc`] returns for a send.
pub const SCRIPTED_TX: B256 = B256::repeat_byte(0xef);

/// The pending nonce [`ScriptedRpc`] returns.
pub const SCRIPTED_NONCE: u64 = 5;

/// Answers `eth_getTransactionCount` with [`SCRIPTED_NONCE`], and each
/// `eth_call` with the next answer in its queue. `eth_sendTransaction` gets
/// [`SCRIPTED_TX`] unless the send parks, and `eth_getTransactionReceipt` gets
/// the scripted receipt when there is one. Every other call, and an `eth_call`
/// past the end of the queue, never completes, so a send on it parks in its
/// submit or its receipt wait.
#[derive(Clone, Debug)]
pub struct ScriptedRpc {
    send: bool,
    receipt: Option<TransactionReceipt>,
    calls: Arc<Mutex<VecDeque<Bytes>>>,
}

impl ScriptedRpc {
    /// A transport that returns [`SCRIPTED_TX`] for a send and never answers
    /// its receipt read: a broadcast transaction that never mines.
    #[must_use]
    pub fn parked_receipt() -> Self {
        Self {
            send: true,
            receipt: None,
            calls: Arc::default(),
        }
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
            receipt: Some(receipt),
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
        ProviderBuilder::default().connect_client(RpcClient::new(self, false))
    }

    /// The next scripted `eth_call` answer, if any is left.
    fn next_call(&self) -> Option<Bytes> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }
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
        let result = match (req.method(), &self.receipt) {
            ("eth_getTransactionCount", _) => {
                serde_json::value::to_raw_value(&U256::from(SCRIPTED_NONCE))
            }
            ("eth_call", _) => match self.next_call() {
                Some(answer) => serde_json::value::to_raw_value(&answer),
                None => return Box::pin(std::future::pending()),
            },
            ("eth_sendTransaction", _) if self.send => {
                serde_json::value::to_raw_value(&SCRIPTED_TX)
            }
            ("eth_getTransactionReceipt", Some(receipt)) => {
                serde_json::value::to_raw_value(receipt)
            }
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
