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
//! `BatchStoreRequest` / `BatchStoreAck` are part of `cdn/dht/v1` at
//! [`DhtMessage`] discriminants 4 and 5, ahead of `FindNode` /
//! `FindNodeResponse` at 6 and 7 to match ADR 022's variant order. They
//! carry the bandwidth optimization in which one holder publishes many
//! hashes to one receiver in a single RPC (ADR 022 §STORE Flow Batched
//! STORE). The receiver-side admission and two-stage rate-limit
//! accounting live in `decdn-node`; every DHT node implements them, so
//! there is no per-hash fallback or support negotiation.
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

use crate::coverage::Coverage;
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
    /// Batched publish; handled by `decdn-node` (see module docs).
    BatchStore(BatchStoreRequest),
    /// discriminant 5 — asserted by `dht_message_batch_store_ack_discriminant_is_five`.
    /// Per-hash ack for [`Self::BatchStore`].
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

/// A known holder of a content hash, plus which [`Coverage`] blocks it
/// reports being able to serve (range-keyed discovery). Carried in
/// [`FindValueResponse::providers`].
///
/// `coverage` is unsigned and responder-controlled, like the rest of a DHT
/// record — see the module docs' authentication-model note. A lying
/// responder can only ever cost the requester a wasted probe against a
/// provider that turns out not to hold what it claimed; it cannot forge a
/// record for a `node` it doesn't control (the STORE-time `holder ==
/// authenticated NodeId` check still applies).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provider {
    /// The holder's `NodeId`.
    pub node: NodeId,
    /// Discovery blocks this holder reports covering.
    pub coverage: Coverage,
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
    /// Nodes known to hold this hash, with per-holder coverage. May be
    /// empty. Capped at [`MAX_PROVIDERS_PER_HASH`]; oversize values fail
    /// decode.
    #[serde(deserialize_with = "deserialize_providers")]
    pub providers: Vec<Provider>,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreRequest {
    /// BLAKE3 hash of the content being advertised.
    pub hash: ContentHash,
    /// The `NodeId` claiming to hold this hash. MUST equal the authenticated
    /// QUIC `NodeId` of the connection; the receiver rejects mismatches.
    pub holder: NodeId,
    /// Discovery blocks the holder reports covering for `hash`.
    pub coverage: Coverage,
}

/// Optional [`FindValueResponse`] extension fields, carried as trailing bytes
/// after the message via the two-phase pattern (ADR 013 §Tier 1; see
/// [`encode_find_value_response`] / [`parse_find_value_response_ext`]).
///
/// Empty today. It names the append site for the first field a responder needs —
/// a coverage hint, a record age, a freshness bound — so that field lands as a
/// pure append rather than a `cdn/dht/v2` bump. Postcard fills no defaults for
/// absent trailing fields, which is why the field must go in a separately encoded
/// extension rather than onto [`FindValueResponse`] itself.
///
/// Anything added here is UNSIGNED and responder-controlled. DHT records carry
/// no per-record signature (see the module docs), so an extension field is a
/// hint the requester may act on only where a lying responder gains nothing.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FindValueResponseExt {}

/// Optional [`StoreRequest`] extension fields, carried as trailing bytes after
/// the message via the two-phase pattern (ADR 013 §Tier 1; see
/// [`encode_store_request`] / [`parse_store_request_ext`]).
///
/// Empty today, for the same reason as [`FindValueResponseExt`]: the publisher
/// side is where a record would gain a TTL hint or a partial-holding range, and
/// that append must stay Tier-1 after launch.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StoreRequestExt {}

/// Encode a [`DhtMessage::FindValueResponse`] with its optional trailing
/// [`FindValueResponseExt`] (ADR 013 §Tier 1, two-phase).
///
/// Typed per variant rather than taking pre-serialized bytes, so an extension
/// cannot be attached to a message it does not belong to.
///
/// # Errors
///
/// Propagates a [`postcard::Error`] if serialization fails.
pub fn encode_find_value_response(
    resp: &FindValueResponse,
    ext: Option<&FindValueResponseExt>,
) -> Result<Vec<u8>, postcard::Error> {
    let mut buf = postcard::to_allocvec(&DhtMessage::FindValueResponse(resp.clone()))?;
    if let Some(ext) = ext {
        buf.extend_from_slice(&postcard::to_allocvec(ext)?);
    }
    Ok(buf)
}

/// Encode a [`DhtMessage::Store`] with its optional trailing [`StoreRequestExt`]
/// (ADR 013 §Tier 1, two-phase). Typed per variant, as above.
///
/// # Errors
///
/// Propagates a [`postcard::Error`] if serialization fails.
pub fn encode_store_request(
    req: &StoreRequest,
    ext: Option<&StoreRequestExt>,
) -> Result<Vec<u8>, postcard::Error> {
    let mut buf = postcard::to_allocvec(&DhtMessage::Store(req.clone()))?;
    if let Some(ext) = ext {
        buf.extend_from_slice(&postcard::to_allocvec(ext)?);
    }
    Ok(buf)
}

/// Parse the trailing [`FindValueResponseExt`] bytes returned as the remainder
/// by [`crate::decode_message`] after a `DhtMessage::FindValueResponse`.
///
/// An empty remainder ⇒ [`FindValueResponseExt::default`]. Trailing bytes beyond
/// the known fields are tolerated for forward compatibility (ADR 013 §Tier 1).
///
/// # Errors
///
/// Returns a [`postcard::Error`] if a non-empty remainder is not a valid
/// `FindValueResponseExt` prefix. While [`FindValueResponseExt`] holds no fields it consumes
/// no bytes and cannot fail; the signature carries the error so that adding the
/// first field is a pure append here too.
pub fn parse_find_value_response_ext(
    remainder: &[u8],
) -> Result<FindValueResponseExt, postcard::Error> {
    if remainder.is_empty() {
        Ok(FindValueResponseExt::default())
    } else {
        Ok(postcard::take_from_bytes::<FindValueResponseExt>(remainder)?.0)
    }
}

/// Parse the trailing [`StoreRequestExt`] bytes returned as the remainder by
/// [`crate::decode_message`] after a `DhtMessage::Store`.
///
/// An empty remainder ⇒ [`StoreRequestExt::default`]. Trailing bytes beyond the
/// known fields are tolerated for forward compatibility (ADR 013 §Tier 1).
///
/// # Errors
///
/// Returns a [`postcard::Error`] if a non-empty remainder is not a valid
/// `StoreRequestExt` prefix. While [`StoreRequestExt`] holds no fields it consumes
/// no bytes and cannot fail; the signature carries the error so that adding the
/// first field is a pure append here too.
pub fn parse_store_request_ext(remainder: &[u8]) -> Result<StoreRequestExt, postcard::Error> {
    if remainder.is_empty() {
        Ok(StoreRequestExt::default())
    } else {
        Ok(postcard::take_from_bytes::<StoreRequestExt>(remainder)?.0)
    }
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
/// receiver (ADR 022 §STORE Flow). A bandwidth optimization over `n`
/// separate [`StoreRequest`]s: the re-publish scheduler groups due hashes
/// by receiver and sends each its set in one of these. Admission and
/// two-stage rate-limit accounting live in the `decdn-node` handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchStoreRequest {
    /// `(hash, coverage)` pairs being published in this batch. Capped at
    /// [`MAX_BATCH_STORE_HASHES`] (ADR 022 §STORE Flow); oversize values fail
    /// decode (the handler maps that to `MALFORMED_MESSAGE` 0x03 per
    /// ADR 013).
    #[serde(deserialize_with = "deserialize_batch_entries")]
    pub entries: Vec<(ContentHash, Coverage)>,
    /// Single holder for the batch. Per ADR 022 §STORE Flow the receiver
    /// checks this once against the authenticated QUIC `NodeId`; the
    /// per-hash check is skipped because the field is batch-level.
    pub holder: NodeId,
}

/// Per-hash acknowledgement for a [`BatchStoreRequest`] (ADR 022 §STORE
/// Flow). `results[i]` corresponds to `BatchStoreRequest.entries[i]`.
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

fn deserialize_providers<'de, D>(d: D) -> Result<Vec<Provider>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(d, MAX_PROVIDERS_PER_HASH, "providers")
}

fn deserialize_batch_entries<'de, D>(d: D) -> Result<Vec<(ContentHash, Coverage)>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(d, MAX_BATCH_STORE_HASHES, "entries")
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
    // `T` is either `#[serde(transparent)]` over `[u8; 32]` (a bare `NodeId`
    // or `ContentHash`) or a small fixed-shape struct built from such types
    // (`Provider`, `(ContentHash, Coverage)`), so the decoded length and
    // per-element bytes are identical to the raw wire form; the cap is the
    // only thing this hook adds.
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
mod tests;
