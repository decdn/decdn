//! Cache engine for deCDN.
//!
//! Wraps `iroh-blobs` with origin pull-through logic: on a cache miss,
//! the engine discovers providers via probe fan-out and pulls the blob
//! via the `cdn/client/v1` protocol.
