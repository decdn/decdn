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
/// the contract: a record whose framing does not decode, or an entry that is not
/// a `/ip4|ip6/<addr>/udp/<port>/quic-v1` QUIC multiaddr, is skipped rather than
/// raised. A self-attested address is only ever one dial path among several, so
/// a malformed or partial record must never remove a peer from the candidate
/// set — it can only fail to add a direct path, leaving iroh discovery and the
/// relay fallback intact.
#[must_use]
pub fn decode_dial_addrs(packed: &[u8]) -> Vec<std::net::SocketAddr> {
    unpack_multiaddrs(packed)
        .unwrap_or_default()
        .iter()
        .filter_map(|s| parse_quic_multiaddr(s))
        .collect()
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
mod tests {
    use super::*;
    use alloy::primitives::{address, b256};

    // Committed `registerNode` ownership vector from
    // `contracts/test/CapacityBondRegionE2E.t.sol` (generated by
    // `contracts/test/ed25519-vectors`). The published ed25519 signature
    // verifies against the nodeId only if our digest byte-matches the
    // contract's — a true differential check against the authoritative
    // on-chain encoding, not a self-consistent round-trip.
    const REG_NODE_ID: B256 =
        b256!("d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737");
    const REG_OPERATOR: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
    const REG_CHAIN_ID: u64 = 31_337;
    const REG_NONCE: u64 = 0;
    const REG_ED25519_SIG: [u8; 64] = [
        0x2d, 0x1b, 0x18, 0x05, 0xfe, 0x88, 0x07, 0x82, 0xcc, 0x1b, 0x60, 0xbc, 0x49, 0xf7, 0xd1,
        0xe2, 0x74, 0x45, 0x87, 0x0c, 0x01, 0xe1, 0xde, 0x19, 0x59, 0x80, 0x97, 0x0a, 0x65, 0xf4,
        0xe0, 0xc7, 0xfa, 0x9e, 0xa3, 0x03, 0x47, 0xbb, 0x87, 0x6c, 0x4b, 0x1c, 0xe6, 0x41, 0x74,
        0xfd, 0x8d, 0x6a, 0x2b, 0x90, 0x52, 0x0d, 0x7e, 0xd4, 0xde, 0xa1, 0x2d, 0x80, 0x5d, 0x40,
        0x1e, 0xa5, 0xe4, 0x05,
    ];

    #[test]
    fn ownership_digest_matches_published_vector() {
        let digest = ownership_message_digest(REG_NODE_ID, REG_OPERATOR, REG_CHAIN_ID, REG_NONCE);

        // The vector's signature is a strict-valid ed25519 signature over the
        // contract's ownership digest, with the public key equal to the nodeId.
        // If our digest is wrong, verification fails.
        let pubkey = iroh::PublicKey::from_bytes(REG_NODE_ID.as_slice().try_into().unwrap())
            .expect("nodeId is a valid ed25519 public key");
        let sig = iroh::Signature::from_bytes(&REG_ED25519_SIG);
        pubkey
            .verify(digest.as_slice(), &sig)
            .expect("published ed25519 signature must verify against our digest");
    }

    #[test]
    fn ownership_digest_is_nonce_sensitive() {
        let d0 = ownership_message_digest(REG_NODE_ID, REG_OPERATOR, REG_CHAIN_ID, 0);
        let d1 = ownership_message_digest(REG_NODE_ID, REG_OPERATOR, REG_CHAIN_ID, 1);
        assert_ne!(d0, d1, "registrationNonce must feed the digest");
    }

    #[test]
    fn pack_multiaddrs_length_prefixes_big_endian() {
        let packed = pack_multiaddrs(&["/ip4/203.0.113.10/udp/4433/quic-v1".to_string()]).unwrap();
        let body = "/ip4/203.0.113.10/udp/4433/quic-v1".as_bytes();
        let len = u16::try_from(body.len()).unwrap();
        assert_eq!(&packed[0..2], &len.to_be_bytes(), "BE uint16 length prefix");
        assert_eq!(&packed[2..], body, "body follows the prefix verbatim");
    }

    #[test]
    fn pack_multiaddrs_concatenates_entries() {
        let packed = pack_multiaddrs(&["aa".to_string(), "bbbb".to_string()]).unwrap();
        // (len=2, "aa") then (len=4, "bbbb").
        assert_eq!(
            packed,
            vec![0x00, 0x02, b'a', b'a', 0x00, 0x04, b'b', b'b', b'b', b'b']
        );
    }

    #[test]
    fn pack_multiaddrs_empty_is_empty() {
        assert!(pack_multiaddrs(&[]).unwrap().is_empty());
    }

    #[test]
    fn unpack_multiaddrs_round_trips() {
        let addrs = vec![
            "/ip4/203.0.113.10/udp/4433/quic-v1".to_string(),
            "/ip6/2001:db8::1/udp/4433/quic-v1".to_string(),
        ];
        let packed = pack_multiaddrs(&addrs).unwrap();
        assert_eq!(unpack_multiaddrs(&packed).unwrap(), addrs);
    }

    #[test]
    fn unpack_multiaddrs_empty_is_empty() {
        assert!(unpack_multiaddrs(&[]).unwrap().is_empty());
    }

    /// A truncated field must error rather than yield a short list: the eth
    /// rotation path re-registers whatever this returns, so a silent drop would
    /// put a node back on-chain missing an address it had.
    #[test]
    fn unpack_multiaddrs_rejects_truncation() {
        // Length prefix with no body at all.
        assert!(unpack_multiaddrs(&[0x00]).is_err());
        // Prefix claims 4 bytes, only 2 follow.
        assert!(unpack_multiaddrs(&[0x00, 0x04, b'a', b'b']).is_err());
    }

    #[test]
    fn unpack_multiaddrs_rejects_non_utf8() {
        assert!(unpack_multiaddrs(&[0x00, 0x01, 0xFF]).is_err());
    }

    #[test]
    fn pack_multiaddrs_rejects_oversized_entry() {
        let huge = "x".repeat(usize::from(u16::MAX) + 1);
        let err = pack_multiaddrs(&[huge]).unwrap_err();
        assert!(err.to_string().contains("exceeds the uint16"), "{err}");
    }

    #[test]
    fn decode_dial_addrs_parses_ip4_and_ip6() {
        let packed = pack_multiaddrs(&[
            "/ip4/203.0.113.10/udp/4433/quic-v1".to_string(),
            "/ip6/2001:db8::1/udp/4434/quic-v1".to_string(),
        ])
        .unwrap();
        let addrs = decode_dial_addrs(&packed);
        assert_eq!(
            addrs,
            vec![
                "203.0.113.10:4433".parse().unwrap(),
                "[2001:db8::1]:4434".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn decode_dial_addrs_ignores_trailing_segments() {
        let packed =
            pack_multiaddrs(&["/ip4/203.0.113.10/udp/4433/quic-v1/p2p/abc".to_string()]).unwrap();
        assert_eq!(
            decode_dial_addrs(&packed),
            vec!["203.0.113.10:4433".parse().unwrap()]
        );
    }

    #[test]
    fn decode_dial_addrs_skips_malformed_entries_keeps_valid() {
        // A TCP multiaddr, a family mismatch, and a garbage string are each
        // dropped; the one well-formed QUIC entry survives.
        let packed = pack_multiaddrs(&[
            "/ip4/203.0.113.10/tcp/4433".to_string(),
            "/ip4/::1/udp/4433/quic-v1".to_string(),
            "not-a-multiaddr".to_string(),
            "/ip4/198.51.100.7/udp/5000/quic-v1".to_string(),
        ])
        .unwrap();
        assert_eq!(
            decode_dial_addrs(&packed),
            vec!["198.51.100.7:5000".parse().unwrap()]
        );
    }

    #[test]
    fn decode_dial_addrs_empty_and_malformed_framing_yield_empty() {
        // Empty field → no hints.
        assert!(decode_dial_addrs(&[]).is_empty());
        // Truncated framing (unpack would error) → lenient empty, never a panic
        // or error: a torn record must not remove a peer from the dial set.
        assert!(decode_dial_addrs(&[0x00]).is_empty());
    }
}
