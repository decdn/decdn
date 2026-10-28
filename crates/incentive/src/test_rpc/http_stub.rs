//! An HTTP JSON-RPC stub that answers a gas estimate and records the gas
//! limit of every transaction sent to it.
//!
//! The provider builders take a URL, not a transport, so a test that checks
//! what a builder puts on the wire needs a real HTTP endpoint. [`GasStub`]
//! answers `eth_estimateGas` with a fixed estimate and accepts
//! `eth_sendRawTransaction`, decoding the gas limit out of the signed
//! envelope. A request that leaves the nonce, chain id and fee fields unset
//! also gets answers for those reads.

use std::sync::{Arc, Mutex, PoisonError};

use alloy::consensus::{Transaction, TxEnvelope};
use alloy::eips::eip2718::Decodable2718;
use alloy::primitives::hex;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use super::SCRIPTED_TX;

/// A local HTTP JSON-RPC endpoint. It answers `eth_estimateGas` with the
/// estimate it was spawned with, `eth_chainId` with 1,
/// `eth_getTransactionCount` with 0, and the fee reads with 1 wei. It answers
/// `eth_sendRawTransaction` with [`SCRIPTED_TX`] and records the transaction's
/// gas limit. Every other method gets a "method not found" error. The server
/// stops when the stub drops.
#[derive(Debug)]
pub struct GasStub {
    url: String,
    sent: Arc<Mutex<Vec<u64>>>,
    server: tokio::task::JoinHandle<()>,
}

impl GasStub {
    /// Bind a loopback port and serve on it, answering every gas estimate
    /// with `estimate`.
    ///
    /// # Errors
    ///
    /// Fails if no loopback port can be bound.
    pub async fn spawn(estimate: u64) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let server = tokio::spawn(accept(listener, estimate, Arc::clone(&sent)));
        Ok(Self { url, sent, server })
    }

    /// The endpoint's URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The gas limit of each transaction sent so far, in arrival order.
    #[must_use]
    pub fn sent_gas_limits(&self) -> Vec<u64> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Drop for GasStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn accept(listener: TcpListener, estimate: u64, sent: Arc<Mutex<Vec<u64>>>) {
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(serve(stream, estimate, Arc::clone(&sent)));
    }
}

/// Serve HTTP/1.1 requests on one keep-alive connection until it closes.
async fn serve(stream: TcpStream, estimate: u64, sent: Arc<Mutex<Vec<u64>>>) {
    let mut reader = BufReader::new(stream);
    loop {
        let mut content_length = 0;
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
        let mut body = vec![0; content_length];
        if reader.read_exact(&mut body).await.is_err() {
            return;
        }
        let reply = match serde_json::from_slice::<Value>(&body) {
            Ok(Value::Array(calls)) => Value::Array(
                calls
                    .iter()
                    .map(|call| answer(call, estimate, &sent))
                    .collect(),
            ),
            Ok(call) => answer(&call, estimate, &sent),
            Err(_) => json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": { "code": -32700, "message": "parse error" },
            }),
        };
        let reply = reply.to_string();
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            reply.len()
        );
        let stream = reader.get_mut();
        if stream.write_all(head.as_bytes()).await.is_err()
            || stream.write_all(reply.as_bytes()).await.is_err()
        {
            return;
        }
    }
}

fn answer(call: &Value, estimate: u64, sent: &Mutex<Vec<u64>>) -> Value {
    let id = call.get("id").cloned().unwrap_or(Value::Null);
    let result = match call.get("method").and_then(Value::as_str) {
        Some("eth_estimateGas") => json!(format!("{estimate:#x}")),
        Some("eth_chainId" | "eth_gasPrice" | "eth_maxPriorityFeePerGas") => json!("0x1"),
        Some("eth_getTransactionCount") => json!("0x0"),
        Some("eth_sendRawTransaction") => {
            let limit = call
                .get("params")
                .and_then(|params| params.get(0))
                .and_then(Value::as_str)
                .and_then(|raw| hex::decode(raw).ok())
                .and_then(|bytes| TxEnvelope::decode_2718(&mut bytes.as_slice()).ok())
                .map(|tx| tx.gas_limit());
            let Some(limit) = limit else {
                return json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32602, "message": "undecodable transaction" },
                });
            };
            sent.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(limit);
            json!(SCRIPTED_TX)
        }
        _ => {
            return json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32601, "message": "method not found" },
            });
        }
    };
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}
