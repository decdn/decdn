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
//! timestamp in the signed body (ADR 022 §STORE Flow, ADR 015 §Replay Safety
//! Analysis).
//!
//! # No `BatchStore` in v1
//!
//! ADR 022 specifies optional `BatchStoreRequest` / `BatchStoreAck` variants
//! as a bandwidth optimization. They are deferred to a follow-up issue. When
//! added they MUST be appended to [`DhtMessage`] (variant order is frozen per
//! ADR 013 — see the discriminant pinning tests).

use serde::{Deserialize, Serialize};

/// Wire-level maximum number of providers a [`FindValueResponse`] may carry
/// (ADR 022 §Content Records and TTL). Bounds the response size at the
/// `~2.3 KB` ceiling documented in ADR 022 §DHT Bandwidth Analysis.
///
/// Receivers MAY enforce this on decode; responders MUST NOT exceed it.
pub const MAX_PROVIDERS_PER_HASH: usize = 50;

/// Wire-level maximum number of `NodeId`s in a `closer_nodes` field, equal to
/// the Kademlia bucket size K=20 (ADR 022 §Routing Table). Applies to both
/// [`FindValueResponse::closer_nodes`] and [`FindNodeResponse::closer_nodes`].
pub const MAX_CLOSER_NODES: usize = 20;

/// Top-level protocol enum for `cdn/dht/v1`. Variant order is frozen per
/// ADR 013 — new variants MUST be appended at the end.
///
/// ⚠️ **VARIANT ORDER FROZEN — ADR 013 §Protocol Enums**
/// Postcard encodes each variant as its declaration-order index. Reordering,
/// inserting, or removing a variant is a wire-breaking change requiring an
/// ALPN version bump (`cdn/dht/v2`). Discriminants are pinned by the
/// `dht_message_*_discriminant_is_*` tests in this module — if you change this
/// enum, those tests will fail and tell you why.
///
/// The reserved-for-future-extension variants from ADR 022 (`BatchStore`,
/// `BatchStoreAck`) are intentionally omitted here. When they are added they
/// MUST be appended after `FindNodeResponse`.
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
    /// discriminant 4 — asserted by `dht_message_find_node_discriminant_is_four`
    FindNode(FindNodeRequest),
    /// discriminant 5 — asserted by `dht_message_find_node_response_discriminant_is_five`
    FindNodeResponse(FindNodeResponse),
}

/// Query for providers of a specific content hash (ADR 022 §Message Types).
///
/// The responder returns any known providers for `hash` and the K closest
/// `NodeId`s it knows toward the hash in keyspace. `requester` is used by the
/// responder to update its own routing table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindValueRequest {
    /// BLAKE3 hash of the content being sought (iroh `Hash`, 32 bytes).
    pub hash: [u8; 32],
    /// The caller's `NodeId` (32-byte Ed25519 public key) so the responder can
    /// update its routing table with the live peer.
    pub requester: [u8; 32],
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
    pub hash: [u8; 32],
    /// Nodes known to hold this hash. May be empty. Capped at
    /// [`MAX_PROVIDERS_PER_HASH`].
    pub providers: Vec<[u8; 32]>,
    /// K closest nodes to `hash` in the responder's routing table. Capped at
    /// [`MAX_CLOSER_NODES`].
    pub closer_nodes: Vec<[u8; 32]>,
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
    pub hash: [u8; 32],
    /// The `NodeId` claiming to hold this hash. MUST equal the authenticated
    /// QUIC `NodeId` of the connection; the receiver rejects mismatches.
    pub holder: [u8; 32],
}

/// Acknowledgement for a [`StoreRequest`] (ADR 022 §Message Types).
///
/// `accepted == false` means the record was rejected — most commonly the
/// holder is over per-publisher quota or is not in the receiver's cached
/// active-staker set (ADR 022 §Content Records and TTL, §STORE Flow).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreAck {
    /// Echoes the published hash.
    pub hash: [u8; 32],
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
    pub target: [u8; 32],
    /// Caller's `NodeId` for the responder's routing-table update.
    pub requester: [u8; 32],
}

/// Response to a [`FindNodeRequest`] (ADR 022 §Message Types).
///
/// Responders MUST NOT emit more than [`MAX_CLOSER_NODES`] entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindNodeResponse {
    /// Echoes the requested target.
    pub target: [u8; 32],
    /// K closest `NodeId`s to `target` from the responder's routing table.
    pub closer_nodes: Vec<[u8; 32]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_find_value_request() -> FindValueRequest {
        FindValueRequest {
            hash: [0x11u8; 32],
            requester: [0x22u8; 32],
        }
    }

    fn sample_find_value_response() -> FindValueResponse {
        FindValueResponse {
            hash: [0x11u8; 32],
            providers: vec![[0x33u8; 32], [0x44u8; 32]],
            closer_nodes: vec![[0x55u8; 32]],
        }
    }

    fn sample_store_request() -> StoreRequest {
        StoreRequest {
            hash: [0x66u8; 32],
            holder: [0x77u8; 32],
        }
    }

    fn sample_store_ack() -> StoreAck {
        StoreAck {
            hash: [0x66u8; 32],
            accepted: true,
        }
    }

    fn sample_find_node_request() -> FindNodeRequest {
        FindNodeRequest {
            target: [0x88u8; 32],
            requester: [0x99u8; 32],
        }
    }

    fn sample_find_node_response() -> FindNodeResponse {
        FindNodeResponse {
            target: [0x88u8; 32],
            closer_nodes: vec![[0xAAu8; 32], [0xBBu8; 32]],
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
            hash: [0x11u8; 32],
            providers: vec![],
            closer_nodes: vec![[0x55u8; 32]],
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
            hash: [0x66u8; 32],
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
    fn dht_message_find_node_discriminant_is_four() -> Result<(), postcard::Error> {
        let msg = DhtMessage::FindNode(sample_find_node_request());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(4u8));
        let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
    }

    #[test]
    fn dht_message_find_node_response_discriminant_is_five() -> Result<(), postcard::Error> {
        let msg = DhtMessage::FindNodeResponse(sample_find_node_response());
        let bytes = postcard::to_allocvec(&msg)?;
        assert_eq!(bytes.first().copied(), Some(5u8));
        let decoded: DhtMessage = postcard::from_bytes(&bytes)?;
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
    // ~2.3 KB (50 providers × 32 B + 20 closer_nodes × 32 B + framing).
    // The cap is enforced at the responder (handler) layer; this test pins
    // the upper bound of the encoded size so a future change that adds
    // unaccounted bytes can't silently drift past the bandwidth target.
    #[test]
    fn find_value_response_max_size_under_ceiling() -> Result<(), postcard::Error> {
        let msg = FindValueResponse {
            hash: [0xFFu8; 32],
            providers: vec![[0xCDu8; 32]; MAX_PROVIDERS_PER_HASH],
            closer_nodes: vec![[0xCDu8; 32]; MAX_CLOSER_NODES],
        };
        let bytes = postcard::to_allocvec(&msg)?;
        // 32 (hash) + len-varint + 50×32 (providers) + len-varint + 20×32 (closer)
        // = 32 + 1 + 1600 + 1 + 640 = 2274 B. Round up to 2.3 KB ceiling.
        assert!(
            bytes.len() <= 2_300,
            "encoded max FindValueResponse = {} B exceeds 2.3 KB ADR 022 ceiling",
            bytes.len()
        );
        let decoded: FindValueResponse = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded, msg);
        Ok(())
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
