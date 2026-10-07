//! Off-chain construction of the `CapacityBond.registerNode` signing inputs
//! (ADR 019 § Step 2.3).
//!
//! Two byte-exact encodings the on-chain `CapacityBond` depends on:
//!
//! - [`ownership_message_digest`] reproduces
//!   `CapacityBond._verifyEd25519OwnershipSignature`'s message hash
//!   `keccak256(abi.encodePacked(nodeId, msg.sender, block.chainid, registrationNonce))`.
//!   The operator signs this 32-byte digest with the iroh node key; the
//!   contract's `Ed25519Verifier` treats the digest as the ed25519 message.
//!   A drift here makes `registerNode` revert `InvalidEd25519Signature`.
//! - [`pack_multiaddrs`] builds the on-chain `multiaddrs` `bytes` field:
//!   a sequence of `(uint16 length, bytes data)` entries (ADR 019 §
//!   Multiaddr encoding). The length prefix is big-endian, matching the
//!   `uint16` reading of the ADR. Nodes learn addresses via `cdn/dht/v1`
//!   discovery (ADR 022) rather than from this field, so the ADR — not a consumer —
//!   is the authority for the format. [`unpack_multiaddrs`] is its inverse,
//!   read back only by `decdn node rotate-key --key eth` when it re-registers
//!   an operator's existing addresses from a new Ethereum address.
//!
//! The EIP-712 `BindNodeId` half of registration lives in [`crate::bind_sig`]
//! and is reused as-is — `binding_signing_hash` produces the digest the
//! operator signs with the Ethereum key.

use alloy::primitives::{Address, B256, U256, keccak256};

/// On-chain ownership-proof digest signed by the iroh node key during
/// `registerNode` (ADR 019 § Step 2.3).
///
/// Byte-matches `CapacityBond._verifyEd25519OwnershipSignature`:
/// `keccak256(abi.encodePacked(nodeId, operator, chainId, nonce))` over the
/// 92-byte preimage `nodeId(32) ‖ operator(20) ‖ chainId(uint256, 32B BE) ‖
/// nonce(uint64, 8B BE)`. `nonce` is `registrationNonce[nodeId]` read from
/// the contract (0 for a never-registered nodeId).
#[must_use]
pub fn ownership_message_digest(
    node_id: B256,
    operator: Address,
    chain_id: u64,
    registration_nonce: u64,
) -> B256 {
    let mut preimage = Vec::with_capacity(92);
    preimage.extend_from_slice(node_id.as_slice()); // bytes32 → 32
    preimage.extend_from_slice(operator.as_slice()); // address → 20
    preimage.extend_from_slice(&U256::from(chain_id).to_be_bytes::<32>()); // uint256 → 32 BE
    preimage.extend_from_slice(&registration_nonce.to_be_bytes()); // uint64 → 8 BE
    keccak256(preimage)
}

/// Pack QUIC multiaddr strings into the on-chain `multiaddrs` `bytes` field:
/// a sequence of `(uint16 length, bytes data)` entries with big-endian length
/// prefixes (ADR 019 § Multiaddr encoding). An empty slice yields empty bytes,
/// which the contract accepts (a node may rely on the iroh relay / `cdn/dht/v1`
/// discovery for reachability until it promotes direct addresses via
/// `updateMultiaddrs`).
///
/// # Errors
///
/// Returns an error if any single multiaddr exceeds `u16::MAX` bytes (it could
/// not be length-prefixed). The contract separately enforces a total-size
/// ceiling (`maxMultiaddrSize`, default 1,024 bytes); that bound is reported by
/// the on-chain revert rather than re-checked here.
pub fn pack_multiaddrs(addrs: &[String]) -> anyhow::Result<Vec<u8>> {
    // Each entry is a 2-byte length prefix + its bytes; pre-size to avoid
    // reallocating as entries are appended.
    let mut out = Vec::with_capacity(addrs.iter().map(|a| a.len() + 2).sum());
    for addr in addrs {
        let bytes = addr.as_bytes();
        let len = u16::try_from(bytes.len()).map_err(|_| {
            anyhow::anyhow!(
                "multiaddr is {} bytes, exceeds the uint16 length prefix max of {}: {addr}",
                bytes.len(),
                u16::MAX,
            )
        })?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(bytes);
    }
    Ok(out)
}

/// Inverse of [`pack_multiaddrs`]: decode the on-chain `multiaddrs` `bytes`
/// field back into the strings that produced it.
///
/// Needed by `decdn node rotate-key --key eth`, which re-registers an operator's
/// existing multiaddrs from a new Ethereum address. Without this the migration
/// would either silently drop them — leaving the node reachable only through
/// relay / `cdn/dht/v1` discovery — or force the operator to retype a set the
/// chain already holds.
///
/// # Errors
///
/// Returns an error if the buffer is truncated (a length prefix with no body,
/// or a trailing odd byte) or if an entry is not UTF-8. Both mean the field was
/// not produced by [`pack_multiaddrs`], and guessing at a repair would put an
/// address the operator never registered back on-chain.
pub fn unpack_multiaddrs(packed: &[u8]) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    let mut rest = packed;
    while !rest.is_empty() {
        let (prefix, body) = rest.split_at_checked(2).ok_or_else(|| {
            anyhow::anyhow!(
                "truncated multiaddr length prefix ({} byte(s) left)",
                rest.len()
            )
        })?;
        // `split_at_checked` guarantees exactly 2 bytes, so the array conversion
        // cannot fail; `get`/`try_into` keeps it out of the indexing lint.
        let len =
            usize::from(u16::from_be_bytes(prefix.try_into().map_err(|_| {
                anyhow::anyhow!("multiaddr length prefix is not 2 bytes")
            })?));
        let (entry, tail) = body.split_at_checked(len).ok_or_else(|| {
            anyhow::anyhow!(
                "multiaddr entry claims {len} bytes but only {} remain",
                body.len()
            )
        })?;
        out.push(
            std::str::from_utf8(entry)
                .map_err(|e| anyhow::anyhow!("multiaddr entry is not UTF-8: {e}"))?
                .to_string(),
        );
        rest = tail;
    }
    Ok(out)
}

/// Decode the on-chain `multiaddrs` field into dialable UDP socket addresses,
/// for use as iroh direct-address hints (ADR 001 § Node Discovery). Leniency is
/// per entry: a non-UTF8 entry, or one that is not a
/// `/ip4|ip6/<addr>/udp/<port>/quic-v1` QUIC multiaddr, is skipped on its own
/// while the valid entries around it survive. This walks the `pack_multiaddrs`
/// framing directly rather than through [`unpack_multiaddrs`], whose all-or-
/// nothing contract would drop every entry on the first bad one. Only a length
/// prefix that overruns the buffer stops the walk — past that point the next
/// entry's position is unknowable — so the entries decoded before it still
/// count. A self-attested address is only ever one dial path among several, so
/// a malformed or partial record can only fail to add a direct path, never
/// remove a peer from the candidate set, leaving iroh discovery and the relay
/// fallback intact.
#[must_use]
pub fn decode_dial_addrs(packed: &[u8]) -> Vec<std::net::SocketAddr> {
    let mut out = Vec::new();
    let mut rest = packed;
    while let Some((prefix, body)) = rest.split_at_checked(2) {
        // `split_at_checked(2)` guarantees exactly two bytes, so the array
        // conversion cannot fail; keeping it fallible stays out of the indexing
        // lint without a panic path.
        let Ok(len_bytes) = <[u8; 2]>::try_from(prefix) else {
            break;
        };
        let len = usize::from(u16::from_be_bytes(len_bytes));
        let Some((entry, tail)) = body.split_at_checked(len) else {
            // Length prefix overruns the buffer: the next entry's offset is
            // unknowable, so stop — but keep whatever decoded before here.
            break;
        };
        if let Ok(s) = std::str::from_utf8(entry)
            && let Some(sock) = parse_quic_multiaddr(s)
        {
            out.push(sock);
        }
        rest = tail;
    }
    out
}

/// Parse one `/ip4/<addr>/udp/<port>/quic-v1` (or `/ip6/…`) QUIC multiaddr into
/// a [`std::net::SocketAddr`]. Returns `None` for any string that is not that
/// shape; the caller reads `None` as "no direct hint from this entry", never an
/// error. Trailing segments (e.g. `/p2p/<id>`) are ignored — the socket address
/// is fully determined by the ip and udp segments.
fn parse_quic_multiaddr(s: &str) -> Option<std::net::SocketAddr> {
    // Split on '/', dropping the empty element the leading slash produces:
    // ["ip4", "<addr>", "udp", "<port>", "quic-v1", ..].
    let mut segs = s.strip_prefix('/')?.split('/');
    let proto = segs.next()?;
    if proto != "ip4" && proto != "ip6" {
        return None;
    }
    let ip: std::net::IpAddr = segs.next()?.parse().ok()?;
    // Reject a family mismatch (`/ip4/::1/…`): the declared proto must match the
    // parsed address family, or the record is malformed and dropped.
    if proto == "ip4" && !ip.is_ipv4() || proto == "ip6" && !ip.is_ipv6() {
        return None;
    }
    if segs.next()? != "udp" {
        return None;
    }
    let port: u16 = segs.next()?.parse().ok()?;
    if segs.next()? != "quic-v1" {
        return None;
    }
    Some(std::net::SocketAddr::new(ip, port))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
