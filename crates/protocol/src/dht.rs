//! Wire message types for the `cdn/dht/v1` Kademlia content DHT (ADR 022).
//!
//! `cdn/dht/v1` is the primary content discovery mechanism: nodes publish
//! `(hash → NodeId)` records to the K closest peers in keyspace, and clients
//! iteratively look up providers in O(log N) hops. This module defines the
//! wire surface only — routing-table, record-store, scheduler, and rate-limit
//! logic live in `decdn-node`.
//!
//! # Authentication model
//!
//! [`StoreRequest`] has no per-record signature. The receiver instead checks
//! that `holder` equals the authenticated `NodeId` of the inbound QUIC
//! connection (`NodeId`s are 32-byte Ed25519 public keys; iroh's QUIC handshake
//! binds the connection to that key). This depends on records being pushed
//! directly by the holder to the K-closest nodes and never relayed
//! peer-to-peer — if a future scheme introduces peer relay of records, a
//! per-record signature MUST be reintroduced together with an in-scope sender
//! timestamp in the signed body (ADR 022 §STORE Flow).
//!
//! # `BatchStore` wire types
//!
//! ADR 022 specifies optional `BatchStoreRequest` / `BatchStoreAck` variants
//! as a bandwidth optimization. We define the wire types here at their
//! ADR-canonical [`DhtMessage`] discriminants 4 and 5 so the production
//! ordering of `FindNode` / `FindNodeResponse` at 6 and 7 matches the ADR
//! and a future `BatchStore` handler doesn't have to do a wire-breaking
//! discriminant shuffle. The handler-side admission, two-stage rate-limit
//! accounting, and per-receiver fallback negotiation land in a follow-up
//! issue — until then the runtime closes inbound `BatchStore` streams
//! with `APP_ERR_UNSUPPORTED_MESSAGE` (`0x01`), which is the
//! stream-close-without-ack fallback signal ADR 022 §Schema Evolution
//! requires to trigger per-hash `Store` from the publisher.
//!
//! # Bounded-Vec deserialization
//!
//! Network responses with `Vec` fields are bounded at decode time via
//! `#[serde(deserialize_with = ...)]` hooks that enforce the wire-level
//! caps below. The framing layer caps the whole message at 16 MiB
//! ([`crate::framing::MAX_MESSAGE_SIZE`]), but the per-field caps stop
//! peers from forcing the responder to allocate up to a megabyte of
//! 32-byte `NodeId`s (or up to `~16M / 8B` booleans) inside one frame.
//! Pattern mirrors `ProbeResponseBody::rate_per_mb`.

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};

use crate::identity::{ContentHash, NodeId};

/// Wire-level maximum number of providers a [`FindValueResponse`] may carry
/// (ADR 022 §Content Records and TTL). Bounds the response size at the
/// `~2.3 KB` ceiling documented in ADR 022 §DHT Bandwidth Analysis.
///
/// Enforced at the wire boundary by `deserialize_providers`; oversize values
/// fail decode rather than being silently truncated.
pub const MAX_PROVIDERS_PER_HASH: usize = 50;

/// Wire-level maximum number of `NodeId`s in a `closer_nodes` field, equal to
/// the Kademlia bucket size K=20 (ADR 022 §Routing Table). Applies to both
/// [`FindValueResponse::closer_nodes`] and [`FindNodeResponse::closer_nodes`].
/// Enforced by the [`CloserNodes`] type — at construction
/// ([`CloserNodes::try_new`]) and at the wire boundary (its [`Deserialize`]).
pub const MAX_CLOSER_NODES: usize = 20;

/// Wire-level maximum number of hashes a [`BatchStoreRequest`] may carry
/// (ADR 022 §STORE Flow — "Batch size is bounded at 256 hashes"). Applies
/// symmetrically to [`BatchStoreAck::results`] (one bool per request hash,
/// same cap so a malicious responder can't reply with an oversize ack).
/// Enforced at the wire boundary; ADR 022 spells out that oversize batches
/// close the stream with `MALFORMED_MESSAGE` (`0x03`) — the decode-time
/// reject here surfaces as that error code via the framing layer.
pub const MAX_BATCH_STORE_HASHES: usize = 256;

/// A bounded list of the K closest [`NodeId`]s a responder returns in a
/// `closer_nodes` field (ADR 022 §Routing Table). Wraps `Vec<NodeId>` and
/// enforces the [`MAX_CLOSER_NODES`] invariant **at construction** — both
/// [`CloserNodes::try_new`] and the bounded [`Deserialize`] impl reject
/// oversize lists, so an over-cap `closer_nodes` cannot exist in memory or on
/// the wire. `#[repr(transparent)]` + `#[serde(transparent)]` keep the encoding
/// byte-identical to a bare `Vec<NodeId>`.
#[repr(transparent)]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct CloserNodes(Vec<NodeId>);

/// Error returned by [`CloserNodes::try_new`] when the list exceeds
/// [`MAX_CLOSER_NODES`].
#[derive(Debug, thiserror::Error)]
#[error("closer_nodes length {len} exceeds MAX_CLOSER_NODES ({MAX_CLOSER_NODES})")]
pub struct CloserNodesError {
    /// The rejected list length.
    pub len: usize,
}

impl CloserNodes {
    /// Wrap `nodes`, rejecting lists longer than [`MAX_CLOSER_NODES`].
    ///
    /// # Errors
    /// Returns [`CloserNodesError`] when `nodes.len() > MAX_CLOSER_NODES`.
    pub fn try_new(nodes: Vec<NodeId>) -> Result<Self, CloserNodesError> {
        if nodes.len() > MAX_CLOSER_NODES {
            return Err(CloserNodesError { len: nodes.len() });
        }
        Ok(Self(nodes))
    }

    /// Borrow the closer nodes as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[NodeId] {
        &self.0
    }

    /// Iterate over the closer nodes.
    pub fn iter(&self) -> std::slice::Iter<'_, NodeId> {
        self.0.iter()
    }

    /// Consume into the inner `Vec<NodeId>`.
    #[must_use]
    pub fn into_inner(self) -> Vec<NodeId> {
        self.0
    }

    /// Number of closer nodes (always `<= MAX_CLOSER_NODES`).
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// True if no closer nodes are present.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'de> Deserialize<'de> for CloserNodes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Reuses the shared bounded-vec hook so the decode-time cap and error
        // message match the other `closer_nodes`-shaped fields exactly.
        Ok(Self(deserialize_bounded_vec(
            deserializer,
            MAX_CLOSER_NODES,
            "closer_nodes",
        )?))
    }
}

impl<'a> IntoIterator for &'a CloserNodes {
    type Item = &'a NodeId;
    type IntoIter = std::slice::Iter<'a, NodeId>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Top-level protocol enum for `cdn/dht/v1`. Variant order matches ADR 022
/// §Message Types and is frozen per ADR 013.
///
/// ⚠️ **VARIANT ORDER FROZEN — ADR 013 §Protocol Enums**
/// Postcard encodes each variant as its declaration-order index. Reordering,
/// inserting, or removing a variant is a wire-breaking change requiring an
/// ALPN version bump (`cdn/dht/v2`). Discriminants are pinned by the
/// `dht_message_*_discriminant_is_*` tests in this module — if you change this
/// enum, those tests will fail and tell you why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DhtMessage {
    /// discriminant 0 — asserted by `dht_message_find_value_discriminant_is_zero`
    FindValue(FindValueRequest),
    /// discriminant 1 — asserted by `dht_message_find_value_response_discriminant_is_one`
    FindValueResponse(FindValueResponse),
    /// discriminant 2 — asserted by `dht_message_store_discriminant_is_two`
    Store(StoreRequest),
    /// discriminant 3 — asserted by `dht_message_store_ack_discriminant_is_three`
    StoreAck(StoreAck),
    /// discriminant 4 — asserted by `dht_message_batch_store_discriminant_is_four`.
    /// Wire type only; handler implementation deferred to a follow-up issue
    /// (see module docs).
    BatchStore(BatchStoreRequest),
    /// discriminant 5 — asserted by `dht_message_batch_store_ack_discriminant_is_five`.
    /// Wire type only; see [`Self::BatchStore`].
    BatchStoreAck(BatchStoreAck),
    /// discriminant 6 — asserted by `dht_message_find_node_discriminant_is_six`
    FindNode(FindNodeRequest),
    /// discriminant 7 — asserted by `dht_message_find_node_response_discriminant_is_seven`
    FindNodeResponse(FindNodeResponse),
}

impl crate::framing::TopLevelEnum for DhtMessage {
    /// `FindValue` (0) … `FindNodeResponse` (7). Pinned by
    /// `dht_message_variant_count_matches_discriminants`.
    const VARIANT_COUNT: u32 = 8;
}

/// Query for providers of a specific content hash (ADR 022 §Message Types).
///
/// The responder returns any known providers for `hash` and the K closest
/// `NodeId`s it knows toward the hash in keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindValueRequest {
    /// BLAKE3 hash of the content being sought (iroh `Hash`, 32 bytes).
    pub hash: ContentHash,
    /// The caller's self-declared `NodeId` (32-byte Ed25519 public key).
    ///
    /// **The responder MUST NOT use this wire value to update its routing
    /// table.** It is attacker-controlled and trusting it reintroduces the
    /// routing-table-poisoning bug class (an authenticated peer seeding
    /// arbitrary, possibly unreachable ids it does not own). The responder
    /// refreshes only the *authenticated* QUIC peer id (`conn.remote_id()`);
    /// this field is carried for protocol symmetry and possible future use
    /// (which would require a per-request signature first).
    pub requester: NodeId,
}

/// Response to a [`FindValueRequest`] (ADR 022 §Message Types).
///
/// `providers` is the responder's known holders for `hash` (may be empty);
/// `closer_nodes` is up to K of the responder's nearest `NodeId`s toward `hash`
/// in XOR keyspace, used by the requester to continue iterative lookup.
///
/// Responders MUST NOT emit more than [`MAX_PROVIDERS_PER_HASH`] providers or
/// more than [`MAX_CLOSER_NODES`] closer nodes — the wire-cost ceiling
/// (~2.3 KB) in ADR 022 §DHT Bandwidth Analysis depends on these bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindValueResponse {
    /// Echoes the requested hash.
    pub hash: ContentHash,
    /// Nodes known to hold this hash. May be empty. Capped at
    /// [`MAX_PROVIDERS_PER_HASH`]; oversize values fail decode.
    #[serde(deserialize_with = "deserialize_providers")]
    pub providers: Vec<NodeId>,
    /// K closest nodes to `hash` in the responder's routing table. The
    /// [`CloserNodes`] type enforces the [`MAX_CLOSER_NODES`] cap at
    /// construction and decode; oversize values fail decode.
    pub closer_nodes: CloserNodes,
}

/// Publish a content record: "I hold this hash" (ADR 022 §Message Types,
/// §STORE Flow). Sent by the holder to each of the K+3 nodes closest to
/// `hash` in keyspace.
///
/// There is no per-record signature: the receiver MUST instead check that
/// `holder` equals the authenticated `NodeId` of the inbound QUIC connection.
/// Records are never relayed peer-to-peer; if that changes, a per-record
/// signature MUST be reintroduced (see module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreRequest {
    /// BLAKE3 hash of the content being advertised.
    pub hash: ContentHash,
    /// The `NodeId` claiming to hold this hash. MUST equal the authenticated
    /// QUIC `NodeId` of the connection; the receiver rejects mismatches.
    pub holder: NodeId,
}

/// Acknowledgement for a [`StoreRequest`] (ADR 022 §Message Types).
///
/// `accepted == false` means the record was rejected — most commonly the
/// holder is over per-publisher quota or is not in the receiver's cached
/// active-staker set (ADR 022 §Content Records and TTL, §STORE Flow).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreAck {
    /// Echoes the published hash.
    pub hash: ContentHash,
    /// True iff the record was admitted to the receiver's record store.
    pub accepted: bool,
}

/// Kademlia node lookup — used during routing-table bootstrap and bucket
/// refresh (ADR 022 §Bootstrap, §Routing Table). Distinct from
/// [`FindValueRequest`] in that the target is a `NodeId`, not a content hash,
/// and no provider lookup is performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindNodeRequest {
    /// `NodeId` being sought.
    pub target: NodeId,
    /// The caller's self-declared `NodeId`.
    ///
    /// **The responder MUST NOT use this wire value to update its routing
    /// table** — it is attacker-controlled, and trusting it reintroduces the
    /// routing-table-poisoning bug class. The responder refreshes only the
    /// *authenticated* QUIC peer id (`conn.remote_id()`); this field is carried
    /// for protocol symmetry and possible future use (which would require a
    /// per-request signature first).
    pub requester: NodeId,
}

/// Response to a [`FindNodeRequest`] (ADR 022 §Message Types).
///
/// Responders MUST NOT emit more than [`MAX_CLOSER_NODES`] entries; oversize
/// values fail decode at the wire boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindNodeResponse {
    /// Echoes the requested target.
    pub target: NodeId,
    /// K closest `NodeId`s to `target` from the responder's routing table.
    /// [`CloserNodes`] enforces the [`MAX_CLOSER_NODES`] cap.
    pub closer_nodes: CloserNodes,
}

/// Batched publication of multiple content records from one holder to one
/// receiver (ADR 022 §STORE Flow). A bandwidth optimization with a per-hash
/// [`StoreRequest`] fallback — the wire types are defined here at their
/// ADR-canonical discriminants so a future handler doesn't shuffle wire
/// positions, but admission, two-stage rate-limit accounting, and
/// per-receiver fallback negotiation live in the handler that will land in
/// a follow-up issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchStoreRequest {
    /// Hashes being published in this batch. Capped at
    /// [`MAX_BATCH_STORE_HASHES`] (ADR 022 §STORE Flow); oversize values fail
    /// decode (the handler maps that to `MALFORMED_MESSAGE` 0x03 per
    /// ADR 013).
    #[serde(deserialize_with = "deserialize_batch_hashes")]
    pub hashes: Vec<ContentHash>,
    /// Single holder for the batch. Per ADR 022 §STORE Flow the receiver
    /// checks this once against the authenticated QUIC `NodeId`; the
    /// per-hash check is skipped because the field is batch-level.
    pub holder: NodeId,
}

/// Per-hash acknowledgement for a [`BatchStoreRequest`] (ADR 022 §STORE
/// Flow). `results[i]` corresponds to `BatchStoreRequest.hashes[i]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchStoreAck {
    /// One `bool` per request hash, in request order. Capped at
    /// [`MAX_BATCH_STORE_HASHES`] (matches the request cap so a malicious
    /// responder can't reply with an oversize ack).
    #[serde(deserialize_with = "deserialize_batch_results")]
    pub results: Vec<bool>,
}

// --- Bounded-Vec deserialization hooks (see module docs).
//
// Pattern follows `ProbeResponseBody::rate_per_mb`'s field-level
// `deserialize_with` so the postcard positional layout stays
// derive-driven — no mirror struct, no `Deserialize` impl drift risk.

fn deserialize_providers<'de, D>(d: D) -> Result<Vec<NodeId>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(d, MAX_PROVIDERS_PER_HASH, "providers")
}

fn deserialize_batch_hashes<'de, D>(d: D) -> Result<Vec<ContentHash>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(d, MAX_BATCH_STORE_HASHES, "hashes")
}

fn deserialize_batch_results<'de, D>(d: D) -> Result<Vec<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let v = Vec::<bool>::deserialize(d)?;
    if v.len() > MAX_BATCH_STORE_HASHES {
        return Err(de::Error::custom(format!(
            "BatchStoreAck.results length {} exceeds MAX_BATCH_STORE_HASHES ({})",
            v.len(),
            MAX_BATCH_STORE_HASHES,
        )));
    }
    Ok(v)
}

fn deserialize_bounded_vec<'de, D, T>(
    d: D,
    max: usize,
    field: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    // `T` is `#[serde(transparent)]` over `[u8; 32]` (or a bare `[u8; 32]`),
    // so the decoded length and per-element bytes are identical to the raw
    // wire form; the cap is the only thing this hook adds.
    //
    // Allocation safety (#845): the cap is enforced *after* `Vec::deserialize`,
    // so a malicious peer's length prefix is untrusted at the point of decode.
    // This is sound because the up-front allocation is bounded independently of
    // that prefix — serde's `Vec` impl uses `size_hint::cautious`, which caps
    // `with_capacity` to a small byte budget, and postcard streams elements
    // (a giant length prefix with a truncated body errors at EOF long before
    // the cap check). See `bounded_vec_giant_length_prefix_truncated_body_*`.
    let v = Vec::<T>::deserialize(d)?;
    if v.len() > max {
        return Err(de::Error::custom(format!(
            "{field} length {} exceeds wire cap ({max})",
            v.len(),
        )));
    }
    Ok(v)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    /// A `NodeId` with every byte set to `b`.
    fn nid(b: u8) -> NodeId {
        NodeId::from_bytes([b; 32])
    }

    /// A `ContentHash` with every byte set to `b`.
    fn ch(b: u8) -> ContentHash {
        ContentHash::from_bytes([b; 32])
    }

    /// Wrap nodes in `CloserNodes`, asserting they fit the cap (test inputs do).
    fn closer(nodes: Vec<NodeId>) -> CloserNodes {
        CloserNodes::try_new(nodes).expect("test closer_nodes within MAX_CLOSER_NODES")
    }

    fn sample_find_value_request() -> FindValueRequest {
        FindValueRequest {
            hash: ch(0x11),
            requester: nid(0x22),
        }
    }

    fn sample_find_value_response() -> FindValueResponse {
        FindValueResponse {
            hash: ch(0x11),
            providers: vec![nid(0x33), nid(0x44)],
            closer_nodes: closer(vec![nid(0x55)]),
        }
    }

    fn sample_store_request() -> StoreRequest {
        StoreRequest {
            hash: ch(0x66),
            holder: nid(0x77),
        }
    }

    fn sample_store_ack() -> StoreAck {
        StoreAck {
            hash: ch(0x66),
            accepted: true,
        }
    }

    fn sample_find_node_request() -> FindNodeRequest {
        FindNodeRequest {
            target: nid(0x88),
            requester: nid(0x99),
        }
    }

    fn sample_find_node_response() -> FindNodeResponse {
        FindNodeResponse {
            target: nid(0x88),
            closer_nodes: closer(vec![nid(0xAA), nid(0xBB)]),
        }
    }

    fn sample_batch_store_request() -> BatchStoreRequest {
        BatchStoreRequest {
            hashes: vec![ch(0xCC), ch(0xDD)],
            holder: nid(0xEE),
        }
    }

    fn sample_batch_store_ack() -> BatchStoreAck {
        BatchStoreAck {
            results: vec![true, false],
        }
    }

    #[test]
    fn find_value_request_roundtrip() -> Result<(), postcard::Error> {
        let msg = sample_find_value_request();
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: FindValueRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn find_value_response_roundtrip() -> Result<(), postcard::Error> {
        let msg = sample_find_value_response();
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: FindValueResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn find_value_response_empty_providers_roundtrip() -> Result<(), postcard::Error> {
        let msg = FindValueResponse {
            hash: ch(0x11),
            providers: vec![],
            closer_nodes: closer(vec![nid(0x55)]),
        };
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: FindValueResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn store_request_roundtrip() -> Result<(), postcard::Error> {
        let msg = sample_store_request();
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: StoreRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn store_ack_roundtrip() -> Result<(), postcard::Error> {
        let msg = sample_store_ack();
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: StoreAck = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn store_ack_rejected_roundtrip() -> Result<(), postcard::Error> {
        let msg = StoreAck {
            hash: ch(0x66),
            accepted: false,
        };
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: StoreAck = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn find_node_request_roundtrip() -> Result<(), postcard::Error> {
        let msg = sample_find_node_request();
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: FindNodeRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn find_node_response_roundtrip() -> Result<(), postcard::Error> {
        let msg = sample_find_node_response();
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: FindNodeResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn dht_message_find_value_discriminant_is_zero() -> Result<(), postcard::Error> {
        let msg = DhtMessage::FindValue(sample_find_value_request());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(0u8));
        let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn dht_message_find_value_response_discriminant_is_one() -> Result<(), postcard::Error> {
        let msg = DhtMessage::FindValueResponse(sample_find_value_response());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(1u8));
        let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn dht_message_store_discriminant_is_two() -> Result<(), postcard::Error> {
        let msg = DhtMessage::Store(sample_store_request());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(2u8));
        let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn dht_message_store_ack_discriminant_is_three() -> Result<(), postcard::Error> {
        let msg = DhtMessage::StoreAck(sample_store_ack());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(3u8));
        let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn dht_message_batch_store_discriminant_is_four() -> Result<(), postcard::Error> {
        let msg = DhtMessage::BatchStore(sample_batch_store_request());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(4u8));
        let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn dht_message_batch_store_ack_discriminant_is_five() -> Result<(), postcard::Error> {
        let msg = DhtMessage::BatchStoreAck(sample_batch_store_ack());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(5u8));
        let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn dht_message_find_node_discriminant_is_six() -> Result<(), postcard::Error> {
        let msg = DhtMessage::FindNode(sample_find_node_request());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(6u8));
        let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn dht_message_find_node_response_discriminant_is_seven() -> Result<(), postcard::Error> {
        let msg = DhtMessage::FindNodeResponse(sample_find_node_response());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(7u8));
        let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    // Pins `TopLevelEnum::VARIANT_COUNT` to the highest discriminant so a future
    // variant addition must update the count the ADR 013 unknown/known
    // classifier relies on.
    #[test]
    fn dht_message_variant_count_matches_discriminants() -> Result<(), postcard::Error> {
        use crate::framing::TopLevelEnum;
        assert_eq!(DhtMessage::VARIANT_COUNT, 8);
        // The last declared variant (`FindNodeResponse`) must encode to
        // discriminant VARIANT_COUNT - 1. Compare against postcard's own varint
        // encoding of that index (not `bytes.first()`) so the pin survives a
        // future multi-byte discriminant (> 127 variants).
        let last = DhtMessage::FindNodeResponse(sample_find_node_response());
        let bytes = postcard::to_allocvec(&last)?;
        let expected_disc = postcard::to_allocvec(&(DhtMessage::VARIANT_COUNT - 1))?;
        assert!(bytes.starts_with(&expected_disc));
        Ok(())
    }

    #[test]
    fn dht_message_unknown_discriminant_is_flagged_unsupported() {
        // Discriminant 8 is the first index past the known set → UNSUPPORTED.
        assert!(crate::is_unknown_variant::<DhtMessage>(&[8u8, 0, 0]));
        // A known in-range discriminant (7) with a bad payload stays MALFORMED.
        assert!(!crate::is_unknown_variant::<DhtMessage>(&[7u8, 0xFF]));
    }

    #[test]
    fn batch_store_request_roundtrip() -> Result<(), postcard::Error> {
        let msg = sample_batch_store_request();
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: BatchStoreRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn batch_store_ack_roundtrip() -> Result<(), postcard::Error> {
        let msg = sample_batch_store_ack();
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: BatchStoreAck = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn dht_message_rejects_unknown_discriminant() {
        // Discriminant 99 has no matching variant — silent-drop is a handler
        // concern; at the type level postcard surfaces a decode error.
        let bytes = [99u8, 0, 0, 0, 0];
        let r: Result<DhtMessage, _> = postcard::from_bytes(&bytes);
        assert!(r.is_err());
    }

    // ADR 022 §DHT Bandwidth Analysis pins the max FindValueResponse at
    // ~2.3 KB on the wire (50 providers × 32 B + 20 closer_nodes × 32 B +
    // framing). We measure the **fully wrapped** `DhtMessage` payload (one
    // discriminant byte for the outer enum + the inner struct) PLUS the
    // length-prefix varint added by `crate::framing::write_frame` so the
    // bound matches the operator-observable wire shape — measuring only
    // the inner struct would let drift creep in via discriminant or
    // framing-layer changes.
    #[test]
    fn find_value_response_max_framed_size_under_ceiling() -> Result<(), postcard::Error> {
        let inner = FindValueResponse {
            hash: ch(0xFF),
            providers: vec![nid(0xCD); MAX_PROVIDERS_PER_HASH],
            closer_nodes: closer(vec![nid(0xCD); MAX_CLOSER_NODES]),
        };
        let msg = DhtMessage::FindValueResponse(inner.clone());
        let payload = postcard::to_allocvec(&msg)?;
        // Account for the framing varint length prefix the writer prepends.
        // postcard `to_allocvec(&u32)` produces the same varint layout, so
        // its length is the right proxy for the prefix's byte count.
        let payload_len_u32: u32 = u32::try_from(payload.len())
            .expect("payload length must fit in u32 for framing varint");
        let length_prefix_len = postcard::to_allocvec(&payload_len_u32)?.len();
        let framed_len = payload.len() + length_prefix_len;
        // 1 (enum discriminant) + 32 (hash) + 1 (len varint, 50 fits in 1B)
        // + 50×32 (providers) + 1 (len varint, 20 fits in 1B) + 20×32 (closer)
        // = 1 + 32 + 1 + 1600 + 1 + 640 = 2275 B inner; + ~2B varint length
        // prefix = ~2277 B on the wire. Round up to 2.3 KB ceiling.
        assert!(
            framed_len <= 2_300,
            "framed max DhtMessage::FindValueResponse = {framed_len} B exceeds 2.3 KB ADR 022 ceiling"
        );
        // Lower-bound guard: a future regression that drops `providers` or
        // `closer_nodes` from the wire shape would shrink the encoded
        // size well below the ADR's modeled payload (~2.27 KB) without
        // failing the upper-bound assert. Pin the lower bound at 2 KB so
        // any accidental field removal trips the test loudly.
        assert!(
            framed_len > 2_000,
            "framed max DhtMessage::FindValueResponse shrank to {framed_len} B \
             — has the wire shape lost providers/closer_nodes?"
        );
        // Sanity: round-trips through the outer enum.
        let decoded: DhtMessage = postcard::from_bytes(&payload)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn find_value_response_decode_rejects_oversize_providers() -> Result<(), postcard::Error> {
        // Serialize a response whose `providers` field exceeds the cap.
        // Decoding must fail with the `deserialize_with` hook's error
        // rather than allocate the oversize Vec.
        let msg = FindValueResponse {
            hash: ch(0xFF),
            providers: vec![nid(0xCD); MAX_PROVIDERS_PER_HASH + 1],
            closer_nodes: CloserNodes::default(),
        };
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: Result<FindValueResponse, _> = postcard::from_bytes(&bytes);
        assert!(
            decoded.is_err(),
            "providers length {} > MAX_PROVIDERS_PER_HASH must reject at decode",
            MAX_PROVIDERS_PER_HASH + 1
        );
        Ok(())
    }

    #[test]
    fn bounded_vec_giant_length_prefix_truncated_body_does_not_overallocate() {
        // #845: pin the allocation-safety assumption of `deserialize_bounded_vec`.
        // An adversarial peer can send a `providers` length prefix claiming
        // billions of elements with no element bytes behind it. Decoding must
        // return an error (EOF) promptly without pre-allocating a multi-GB Vec
        // from the untrusted length — serde's `cautious` capacity bounds the
        // up-front allocation and postcard streams elements, so the body runs
        // out before the cap check is ever reached. If this regressed to an
        // unbounded `with_capacity(len)`, this test would OOM rather than fail.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[0xFFu8; 32]); // `hash` field
        // postcard LEB128 varint for u32::MAX (= 4_294_967_295) providers.
        bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        // No provider bytes follow: the body is truncated immediately.
        let decoded: Result<FindValueResponse, _> = postcard::from_bytes(&bytes);
        assert!(
            decoded.is_err(),
            "giant length prefix + truncated body must error at decode, not allocate"
        );
    }

    #[test]
    fn find_value_response_decode_rejects_oversize_closer_nodes() -> Result<(), postcard::Error> {
        // `CloserNodes` cannot *hold* an over-cap list, so build the oversize
        // wire bytes directly from the field components. postcard encodes a
        // struct as the concatenation of its fields, so a `(hash, providers,
        // closer_nodes)` tuple produces bytes identical to an over-cap
        // `FindValueResponse` — exactly what an adversarial peer could send.
        let bytes = postcard::to_allocvec(&(
            [0xFFu8; 32],
            Vec::<[u8; 32]>::new(),
            vec![[0xCDu8; 32]; MAX_CLOSER_NODES + 1],
        ))?;
        let decoded: Result<FindValueResponse, _> = postcard::from_bytes(&bytes);
        assert!(decoded.is_err());
        Ok(())
    }

    #[test]
    fn find_node_response_decode_rejects_oversize_closer_nodes() -> Result<(), postcard::Error> {
        // See the sibling test: hand-encode the over-cap wire form via a
        // `(target, closer_nodes)` tuple since `CloserNodes` rejects it.
        let bytes = postcard::to_allocvec(&([0u8; 32], vec![[0xCDu8; 32]; MAX_CLOSER_NODES + 1]))?;
        let decoded: Result<FindNodeResponse, _> = postcard::from_bytes(&bytes);
        assert!(decoded.is_err());
        Ok(())
    }

    #[test]
    fn batch_store_request_decode_rejects_oversize_hashes() -> Result<(), postcard::Error> {
        let msg = BatchStoreRequest {
            hashes: vec![ch(0xCD); MAX_BATCH_STORE_HASHES + 1],
            holder: nid(0),
        };
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: Result<BatchStoreRequest, _> = postcard::from_bytes(&bytes);
        assert!(
            decoded.is_err(),
            "ADR 022 §STORE Flow: BatchStore > 256 hashes must reject at decode"
        );
        Ok(())
    }

    #[test]
    fn batch_store_ack_decode_rejects_oversize_results() -> Result<(), postcard::Error> {
        let msg = BatchStoreAck {
            results: vec![true; MAX_BATCH_STORE_HASHES + 1],
        };
        let bytes = postcard::to_allocvec(&msg)?;
        let decoded: Result<BatchStoreAck, _> = postcard::from_bytes(&bytes);
        assert!(decoded.is_err());
        Ok(())
    }

    // Caps are inclusive: exactly-MAX must round-trip cleanly to confirm
    // the boundary check is on `>`, not `>=`.
    #[test]
    fn caps_accept_exactly_max() -> Result<(), postcard::Error> {
        let resp = FindValueResponse {
            hash: ch(0),
            providers: vec![nid(0); MAX_PROVIDERS_PER_HASH],
            closer_nodes: closer(vec![nid(0); MAX_CLOSER_NODES]),
        };
        let bytes = postcard::to_allocvec(&resp)?;
        let decoded: FindValueResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded.providers.len(), MAX_PROVIDERS_PER_HASH);
        assert_eq!(decoded.closer_nodes.len(), MAX_CLOSER_NODES);
        let batch = BatchStoreRequest {
            hashes: vec![ch(0); MAX_BATCH_STORE_HASHES],
            holder: nid(0),
        };
        let bytes = postcard::to_allocvec(&batch)?;
        let decoded: BatchStoreRequest = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded.hashes.len(), MAX_BATCH_STORE_HASHES);
        Ok(())
    }

    // `CloserNodes::try_new` enforces the same cap as the decode path, but at
    // construction. Pins the `>` boundary directly (decode tests cover the wire
    // side; this covers the in-memory side the type's doc advertises).
    #[test]
    fn closer_nodes_try_new_enforces_cap() {
        // Exactly MAX is accepted.
        let at_cap = CloserNodes::try_new(vec![nid(0); MAX_CLOSER_NODES])
            .expect("exactly MAX_CLOSER_NODES must construct");
        assert_eq!(at_cap.len(), MAX_CLOSER_NODES);

        // One over MAX is rejected, and the error reports the offending length.
        let err = CloserNodes::try_new(vec![nid(0); MAX_CLOSER_NODES + 1])
            .expect_err("over-cap must be rejected at construction");
        assert_eq!(err.len, MAX_CLOSER_NODES + 1);

        // Empty is fine.
        assert!(
            CloserNodes::try_new(vec![])
                .expect("empty is valid")
                .is_empty()
        );
    }

    #[test]
    fn dht_message_trailing_bytes_tolerated() -> Result<(), postcard::Error> {
        // ADR 013 Tier-1: extension bytes after the message are silently
        // tolerated by `take_from_bytes`. Confirms the DHT enum behaves the
        // same as ProbeMessage under unknown-extension data.
        let msg = DhtMessage::FindValue(sample_find_value_request());
        let mut bytes = postcard::to_allocvec(&msg)?;
        bytes.extend_from_slice(&[0xAAu8, 0xBB, 0xCC]);
        let (decoded, tail) = postcard::take_from_bytes::<DhtMessage>(&bytes)?;
        assert_eq!(decoded, msg);
        assert_eq!(tail, &[0xAAu8, 0xBB, 0xCC]);
        Ok(())
    }
}
