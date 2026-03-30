# ADR 006: End-to-End Encryption and Key Distribution

**Date:** 2026-03-29
**Status:** Draft

## Context

CDN nodes deliver content but should not be able to read it. QUIC provides transport encryption, but nodes see plaintext at rest and during forwarding. For applications like subscription-gated streaming (e.g., a Spotify-clone scenario), content must be encrypted end-to-end: only authorized, actively-subscribed clients can decrypt delivered blobs.

The encryption scheme must satisfy these constraints:

- Content-addressing (BLAKE3) must remain global — the same track produces the same hash for all clients and across all time periods
- CDN nodes, protocol, payment channels, and caching must remain unchanged
- Access must be revocable when a subscription expires, without re-encrypting content
- A compromised client must not gain access to the full catalog — blast radius must be proportional to what was individually requested
- There is no single master key whose compromise unlocks all content

## Decision

**Envelope encryption with epoch-rotated key distribution.**

The scheme has two independent layers: a permanent content layer and an ephemeral key delivery layer.

### Content Layer (at ingest, once per blob)

The origin generates a random symmetric key per blob, encrypts the content, and stores both the ciphertext and the key:

```
K_blob   = random 256-bit key
ciphertext = XChaCha20-Poly1305(K_blob, plaintext)
hash     = BLAKE3(ciphertext)
```

`K_blob` is stored in the app server's key store (never on CDN nodes). The ciphertext is pushed to the CDN network as an ordinary content-addressed blob. CDN nodes only ever see ciphertext.

XChaCha20-Poly1305 is chosen over AES-256-GCM because its 24-byte nonce eliminates nonce-reuse risk with random generation, and it requires no hardware AES support.

### Key Delivery Layer (session + per play request)

An app server (outside the CDN protocol) gates access and delivers `K_blob` to authorized clients. The app server runs an iroh `Endpoint` with its own `NodeId` and accepts connections on ALPN `cdn/keys/v1`. It is **not** a CDN node — it does not stake, serve blobs, or register in the `StakingRegistry`. Clients discover the app server's `NodeId` out-of-band (hardcoded in client config for PoC, similar to watchtower peer discovery in ADR 007).

Two mechanisms combine to deliver keys:

**Epoch keys** rotate on a fixed interval (default: 5 minutes). The app server derives each epoch key deterministically:

```
epoch_id  = floor(now_unix / 300)
epoch_key = BLAKE3_KDF(server_secret, epoch_id)
```

**Per-request sealed envelope:** On each play request, the app server verifies the client's subscription is active, then wraps `K_blob` with the current epoch key and seals the result to the client's public key:

```
wrapped   = XChaCha20-Poly1305(epoch_key, K_blob)
envelope  = crypto_box_seal(client_pubkey, {wrapped, epoch_id, blob_hash})
```

The client receives the sealed envelope. To decrypt the blob, the client needs both the envelope and the current epoch key.

**Epoch key delivery over `cdn/keys/v1`:** Epoch keys are pushed to clients over a long-lived QUIC stream on the `cdn/keys/v1` ALPN. The stream requires a valid session token. When the subscription expires or is canceled, the server closes the stream and the client receives no further epoch keys. This mirrors the `cdn/watchtower/v1` pattern (ADR 007) where connection liveness carries semantic meaning.

```mermaid
sequenceDiagram
    participant A as App Server
    participant C as Client

    C->>A: connect via cdn/keys/v1
    C->>A: KeySessionOpen {session_token}
    A->>C: KeySessionAccepted {epoch_id: 42, epoch_key}

    Note over A,C: 5 minutes pass...

    A->>C: EpochKeyUpdate {epoch_id: 43, epoch_key}
    C->>A: EpochKeyAck {epoch_id: 43}
    Note over C: epoch 42 key discarded

    Note over A: subscription canceled

    A->>C: close stream
    Note over C: no epoch 44 key — cannot decrypt new content
```

### `cdn/keys/v1` Protocol Messages

The protocol uses two QUIC stream patterns on a single connection, serialized with postcard (consistent with all other protocols in ADR 005):

**Long-lived stream (one per client) — epoch key push:**

| Message | Direction | Fields | Purpose |
| --- | --- | --- | --- |
| `KeySessionOpen` | client → server | `session_token` | Authenticate and start key session |
| `KeySessionAccepted` | server → client | `epoch_id`, `epoch_key` | Confirm session, deliver current epoch key |
| `EpochKeyUpdate` | server → client | `epoch_id`, `epoch_key` | Push rotated epoch key (every 5 minutes) |
| `EpochKeyAck` | client → server | `epoch_id` | Acknowledge receipt of epoch key |

**Short-lived streams (request/response) — sealed envelope delivery:**

| Message | Direction | Fields | Purpose |
| --- | --- | --- | --- |
| `EnvelopeRequest` | client → server | `blob_hash` | Request decryption envelope for a specific blob |
| `EnvelopeResponse` | server → client | `sealed_envelope` | Sealed envelope containing wrapped `K_blob` |

The server rejects `EnvelopeRequest` streams if no active `KeySession` stream exists for the client — this enforces that only clients with an active subscription receive envelopes.

**Authentication:** `KeySessionOpen` carries a `session_token` (opaque to the CDN protocol — issued by the application's auth system, e.g., JWT). The client's iroh `NodeId` provides transport-layer identity (the QUIC handshake proves the client holds the ed25519 private key). The server derives the client's public key for `crypto_box_seal` from the NodeId (ed25519 → X25519 conversion) — no separate `client_pubkey` field is needed. Both authentication layers are required: `NodeId` alone does not prove subscription status; `session_token` alone does not prove transport-layer identity.

**Concurrent stream limiting:** The long-lived epoch key stream doubles as a presence signal. The app server tracks open `KeySession` streams per account. If `active_sessions >= max_concurrent`, the server rejects new `KeySessionOpen` requests with an error code.

### Client Decryption Flow

```
1. Request sealed envelope from app server via cdn/keys/v1 (EnvelopeRequest)
2. Unseal with client private key -> {wrapped, epoch_id, blob_hash}
3. Decrypt wrapped with current epoch_key -> K_blob
4. Fetch ciphertext from CDN via existing protocol (StreamRequest{blob_hash})
5. Verify BLAKE3(ciphertext) == blob_hash
6. Decrypt XChaCha20-Poly1305(K_blob, ciphertext) -> plaintext
7. Discard K_blob from memory after use
```

### Full System Flow

```mermaid
sequenceDiagram
    participant O as Origin
    participant A as App Server
    participant N as CDN Node
    participant C as Client

    O->>O: K_blob = random key, encrypt blob
    O->>O: hash = BLAKE3(ciphertext)
    O->>A: store K_blob
    O->>N: push ciphertext (content-addressed blob)

    C->>A: EnvelopeRequest {hash} via cdn/keys/v1
    A->>A: verify active KeySession exists
    A->>A: wrapped = XChaCha20-Poly1305(epoch_key, K_blob)
    A->>C: EnvelopeResponse {sealed_envelope}

    C->>N: StreamRequest {hash}
    N->>C: ciphertext (paid per MB via cdn/client/v1)

    C->>C: unseal envelope with private key
    C->>C: decrypt wrapped with epoch_key via XChaCha20-Poly1305 to get K_blob
    C->>C: verify BLAKE3(ciphertext) == hash
    C->>C: decrypt XChaCha20-Poly1305(K_blob, ciphertext) and play
```

### Offline Playback (lease-based access)

The online scheme (epoch keys over a persistent connection) assumes connectivity. Offline playback requires a second access mode that trades revocation speed for availability.

**Offline lease issuance:** When a client requests offline access for specific tracks, the app server issues a lease — a bundle of `K_blob` values sealed to a device-bound key:

```
Client requests offline access for tracks [hash_1, hash_2, ..., hash_n]

App server:
  1. Verify subscription is active
  2. Verify device count is within limit (max 3-5 per account)
  3. Verify track count is within limit (max 500 per device)
  4. Build lease:

     lease = {
         k_blobs: {hash_1: K_1, hash_2: K_2, ...},
         issued_at: 1743300000,
         expires_at: 1745892000,       // 30 days
         device_id: "device_xyz",
         account_id: "alice"
     }

  5. Seal to device key:

     sealed_lease = encrypt(device_key, lease)
     // device_key lives in platform keystore:
     //   iOS: Secure Enclave via Keychain
     //   Android: Hardware-backed Keystore
     //   Desktop: OS credential store (less secure)

  6. Return sealed_lease to client
```

**Offline playback flow:**

```mermaid
flowchart TD
    A[Unseal lease with device_key] --> B{expires_at > now?}
    B -->|No| DENY[Deny playback]
    B -->|Yes| C{device_id matches?}
    C -->|No| DENY
    C -->|Yes| D[Look up K_blob for track hash]
    D --> E[Read ciphertext from local cache]
    E --> F["Decrypt XChaCha20-Poly1305(K_blob, ciphertext)"]
    F --> G[Play plaintext]
```

**Content download:** Before going offline, the client downloads tracks through the normal CDN protocol (`cdn/client/v1`, paid per MB). The ciphertext is stored in local cache. This is identical to online streaming — the CDN protocol does not distinguish between streaming and download-for-offline.

**Check-in and revocation:** When connectivity returns, the client contacts the app server to renew or revoke the lease:

```mermaid
flowchart TD
    A[Client reconnects] --> B[App server checks subscription]
    B --> C{Status?}
    C -->|Active| D["Renew lease<br/>(extend expires_at, update track list)"]
    C -->|Canceled| E["Revoke lease<br/>Client deletes sealed_lease + cached ciphertext"]
    C -->|Suspended| F["Revoke lease<br/>Notify client"]
```

If the client never checks in, the lease expires at `expires_at` and offline playback stops. The client retains cached ciphertext but cannot decrypt it without a valid lease.

**Relationship to the online scheme:** The two modes are complementary and coexist:

| | Online (epoch keys) | Offline (lease) |
| --- | --- | --- |
| Key source | Epoch key via `cdn/keys/v1` + sealed envelope | Sealed lease from device keystore |
| K_blob lifetime in client | Transient — in memory, discarded after play | Persistent — on disk, sealed to device key |
| Revocation speed | ~5 minutes (epoch boundary) | Up to 30 days (lease TTL) |
| Server dependency | Continuous | None until lease expires |
| Content source | CDN nodes (streamed) | Local cache (pre-downloaded) |
| Blast radius if compromised | K_blobs for tracks played in current epoch | K_blobs for all tracks in lease (up to 500) |

The offline lease intentionally weakens two properties of the online scheme: K_blob values persist on disk rather than transiently in memory, and revocation is delayed from 5 minutes to the lease TTL. This is an accepted tradeoff — it is the same tradeoff every major streaming service makes, and there is no cryptographic solution that provides both offline playback and instant revocation.

**Device key compromise (jailbroken devices):** If a device's keystore is compromised, the attacker can unseal the lease and extract all K_blob values in it. The blast radius is bounded by `max_tracks` (up to 500 tracks per device). Mitigations are operational, not cryptographic:

- Device attestation (iOS App Attest, Android Play Integrity) — refuse to issue leases to compromised devices
- Device limit per account (3-5) — bounds the total exposure per subscriber
- Per-account audio watermarking — pirated tracks trace back to the source account
- Behavioral detection — flag accounts that download max tracks, never stream online, and churn subscriptions

## Considered Alternatives

### Client-enforced expiry (timestamp in envelope, no epoch keys)

The app server seals `{K_blob, expires_at}` directly to the client's public key. The client checks the timestamp before decrypting.

Rejected because a hacked client can ignore the timestamp. Expiry becomes advisory, not enforced. Acceptable for a PoC but not for production subscription gating.

### Decryption proxy (server-side decryption)

A proxy fetches ciphertext from the CDN, decrypts with K_blob, and streams plaintext to the client over TLS. The client never sees any key.

Rejected because it breaks end-to-end encryption — the proxy sees plaintext. It also introduces a centralized bottleneck that undermines the decentralized CDN architecture.

### Proxy re-encryption (PRE)

The origin encrypts under its own key. A re-encryption proxy transforms ciphertext for each authorized client without learning the plaintext.

Rejected for the PoC due to complexity (BLS12-381 pairing-based crypto), performance overhead, and a significant new dependency (`recrypt`). May be revisited post-PoC if delegated access without origin involvement becomes a requirement.

### Per-client encrypted blobs (ECIES per recipient)

Each blob is re-encrypted per client, producing different ciphertexts and different BLAKE3 hashes.

Rejected because it destroys global content-addressing. The same track would have a different hash per client, breaking CDN caching, deduplication, and gossip announcements.

## PoC Scope

| Aspect | PoC | Production |
| --- | --- | --- |
| E2E encryption | Not implemented. Content is delivered as plaintext blobs. | Full implementation as described |
| Epoch key rotation | N/A | 5-minute rotation via BLAKE3_KDF |
| Key delivery infrastructure | N/A | `cdn/keys/v1` over iroh QUIC |
| Sealed envelopes | N/A | XChaCha20-Poly1305 + crypto_box_seal |
| Offline leases | N/A | 30-day TTL, device-bound keys |
| Device attestation | N/A | iOS Secure Enclave, Android Keystore |
| Audio watermarking | N/A | Per-account |
| App server key store | N/A | KMS/HSM-protected |

For PoC, no action items from this ADR are required. Content-addressed blobs are stored and served as plaintext. The CDN protocol, payment channels, and caching are unchanged regardless of whether encryption is applied at the application layer. This ADR documents the post-PoC design so that the protocol and contract interfaces remain forward-compatible.

## Consequences

**Positive:**

- Global content-addressing is preserved: one ciphertext, one hash, one cached copy for all clients. CDN nodes, protocol, payments, and gossip are completely unchanged.
- CDN nodes never see plaintext or any key material. Compromising a node yields only ciphertext.
- Compromising a client yields only `K_blob` values for tracks that client has already played — not the catalog. Each blob has an independent random key; there is no master key.
- Subscription revocation takes effect within one epoch (5 minutes): the client's epoch key stream is closed and previously sealed envelopes cannot be unwrapped without the next epoch key.
- The epoch key QUIC stream doubles as a presence signal for concurrent stream limiting.
- Key delivery uses the same transport stack as content delivery (iroh QUIC), eliminating the need for a second transport (WebSocket/SSE) and its associated infrastructure (WebSocket-capable load balancer, HTTP upgrade handling).
- The key delivery layer uses only primitives already in the stack: BLAKE3 for KDF, XChaCha20-Poly1305 for symmetric encryption, X25519 (convertible from iroh Ed25519 keys) for `crypto_box_seal`.

**Negative:**

- Adds an app server component outside the CDN protocol. It runs an iroh `Endpoint` but is otherwise a new service to build, deploy, and operate.
- The app server's key store (holding all `K_blob` values) is a high-value target. It must be protected with a KMS or HSM in production. Compromise of the key store exposes all content.
- A hacked client can still extract `K_blob` for tracks it plays in real-time. This is inherent to any scheme where the client produces plaintext output — equivalent to the "analog hole" in DRM systems.
- Epoch key rotation creates a hard dependency on the `cdn/keys/v1` connection. If the QUIC connection drops, the client cannot decrypt new tracks until it reconnects and receives the current epoch key. The client should cache the most recent epoch key in memory (not disk) to survive brief disconnects within the same epoch.
- The app server must run an iroh `Endpoint`, adding iroh as a dependency. This is acceptable given the project is entirely Rust + iroh (ADR 000), but means the app server cannot be a lightweight web service in a different language.
- Browser clients cannot use iroh QUIC directly (browsers do not support raw QUIC). If browser-based playback is ever needed, a WebSocket/WebTransport bridge would be required. This is explicitly a non-goal for PoC (architecture.md: "Mobile or web clients" under Non-Goals).
- `server_secret` (the epoch key derivation root) is a critical secret. Rotation of `server_secret` invalidates all outstanding epoch keys and sealed envelopes, forcing all clients to re-request. Rotation should be infrequent and coordinated.
- Offline leases trade revocation speed for availability: a canceled subscription may retain offline playback for up to the lease TTL (default 30 days). This is an accepted industry-standard tradeoff.
- Offline leases persist K_blob values on disk (sealed to device key), increasing the blast radius of a device compromise from one epoch's tracks to the full lease (up to 500 tracks). Device attestation and watermarking are operational mitigations, not cryptographic guarantees.
