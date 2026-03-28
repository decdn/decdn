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

### Key Delivery Layer (per play request)

An app server (outside the CDN protocol) gates access and delivers `K_blob` to authorized clients using two mechanisms combined:

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

**Epoch key delivery:** Epoch keys are pushed to clients over an authenticated persistent connection (WebSocket or SSE). The connection requires a valid session token. When the subscription expires or is canceled, the connection is closed and the client receives no further epoch keys.

```
App Server                              Client
  |                                       |
  |<-- connect (session token) -----------|
  |                                       |
  |-- epoch_key (epoch 42) ------------->|
  |       ... 5 minutes ...              |
  |-- epoch_key (epoch 43) ------------->|  (epoch 42 key discarded)
  |       ... subscription canceled ...  |
  |-- close connection ----------------->|  (no epoch 44 key)
```

### Client Decryption Flow

```
1. Receive sealed envelope from app server
2. Unseal with client private key -> {wrapped, epoch_id, blob_hash}
3. Decrypt wrapped with current epoch_key -> K_blob
4. Fetch ciphertext from CDN via existing protocol (StreamRequest{blob_hash})
5. Verify BLAKE3(ciphertext) == blob_hash
6. Decrypt XChaCha20-Poly1305(K_blob, ciphertext) -> plaintext
7. Discard K_blob from memory after use
```

### Full System Flow

```
Origin                    App Server              CDN Node              Client
  |                          |                       |                    |
  | encrypt blob             |                       |                    |
  | push ciphertext -------->|                       |                    |
  | store K_blob ----------->|                       |                    |
  |                          |                       |                    |
  |                          |<---- auth + play req -+--------------------|
  |                          | check subscription    |                    |
  |                          | wrap K_blob in epoch  |                    |
  |                          |-- sealed envelope ----|------------------>|
  |                          |                       |                    |
  |                          |                       |<-- StreamRequest --|
  |                          |                       |-- ciphertext ----->|
  |                          |                       |   (paid per MB)    |
  |                          |                       |                    |
  |                          |                       |         unseal key |
  |                          |                       |         decrypt    |
  |                          |                       |         play       |
```

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

## Consequences

**Positive:**

- Global content-addressing is preserved: one ciphertext, one hash, one cached copy for all clients. CDN nodes, protocol, payments, and gossip are completely unchanged.
- CDN nodes never see plaintext or any key material. Compromising a node yields only ciphertext.
- Compromising a client yields only `K_blob` values for tracks that client has already played — not the catalog. Each blob has an independent random key; there is no master key.
- Subscription revocation takes effect within one epoch (5 minutes): the client's epoch key stream is closed and previously sealed envelopes cannot be unwrapped without the next epoch key.
- The epoch key WebSocket doubles as a presence signal for concurrent stream limiting.
- The key delivery layer uses only primitives already in the stack: BLAKE3 for KDF, XChaCha20-Poly1305 for symmetric encryption, X25519 (convertible from iroh Ed25519 keys) for `crypto_box_seal`.

**Negative:**

- Adds an app server component outside the CDN protocol. This is a new service to build, deploy, and operate.
- The app server's key store (holding all `K_blob` values) is a high-value target. It must be protected with a KMS or HSM in production. Compromise of the key store exposes all content.
- A hacked client can still extract `K_blob` for tracks it plays in real-time. This is inherent to any scheme where the client produces plaintext output — equivalent to the "analog hole" in DRM systems.
- Epoch key rotation creates a hard dependency on the persistent connection. If the WebSocket drops, the client cannot decrypt new tracks until it reconnects and receives the current epoch key. The client should cache the most recent epoch key in memory (not disk) to survive brief disconnects within the same epoch.
- `server_secret` (the epoch key derivation root) is a critical secret. Rotation of `server_secret` invalidates all outstanding epoch keys and sealed envelopes, forcing all clients to re-request. Rotation should be infrequent and coordinated.
