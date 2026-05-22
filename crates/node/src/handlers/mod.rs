//! ALPN protocol handlers. Each handler impls `iroh::protocol::ProtocolHandler`
//! and is registered on the iroh `Router` by `crate::runtime`.

pub mod dht;
pub mod limited;
pub mod probe;
