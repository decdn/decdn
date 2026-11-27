//! Who is asking: the requester's IP address (from the socket, or from a
//! header set by a trusted reverse proxy), and per-client rate limits.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Mutex, Once};

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::HeaderName;
use axum::http::request::Parts;

use crate::state::AppState;

/// Where the requester's address comes from.
#[derive(Clone, Debug, Default)]
pub enum ClientIpSource {
    /// The TCP peer: right when nothing sits in front of the onramp.
    #[default]
    Peer,
    /// A header the trusted reverse proxy in front of the onramp sets, e.g.
    /// `CF-Connecting-IP` or `X-Forwarded-For`. Of a list, the right-most
    /// address is used: the one that proxy appended. Anything to its left
    /// came from the client and is ignored. A request without the header
    /// falls back to the TCP peer. Only safe when every request comes
    /// through exactly that one proxy.
    Header(HeaderName),
}

/// The requester's IP address, when known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientIp(pub Option<IpAddr>);

impl FromRequestParts<AppState> for ClientIp {
    type Rejection = std::convert::Infallible;

    fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let peer = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip());
        let ip = match &state.client_ip {
            ClientIpSource::Peer => {
                if peer.is_none() {
                    warn_no_peer_address();
                }
                peer
            }
            ClientIpSource::Header(name) => parts
                .headers
                .get_all(name)
                .iter()
                .next_back()
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.rsplit(',').next())
                .and_then(|v| v.trim().parse().ok())
                .or(peer),
        };
        std::future::ready(Ok(Self(ip)))
    }
}

/// Warn, once per process, that a request arrived with no socket address and
/// no client-address header configured: every such request goes unlimited.
/// The router needs serving with
/// `into_make_service_with_connect_info::<SocketAddr>()`.
fn warn_no_peer_address() {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        tracing::warn!(
            "a request has no client address, so rate limits skip it: serve the router with \
             into_make_service_with_connect_info::<SocketAddr>(), or set \
             ONRAMP_CLIENT_IP_HEADER behind a proxy"
        );
    });
}

/// A fixed-window limit of `per_minute` requests per client. A client is an
/// IPv4 address or an IPv6 /64 (an IPv4-mapped IPv6 address counts as its IPv4
/// address), since one host holds a whole /64. A limit of 0 disables it; a
/// request with no known address is never limited.
///
/// At most 100,000 clients are counted one by one. Once that many hold
/// a window in the current minute, every further client counts in one of
/// 1,024 overflow windows, picked by a keyed hash of the client. Neither memory
/// nor the limit gives way to a flood of addresses, and the flood shares a
/// window with only a slice of the other untracked clients.
#[derive(Debug)]
pub struct RateLimiter {
    per_minute: u32,
    /// Keys the overflow hash, random per process, so a sender cannot pick
    /// addresses that land in one victim's window.
    overflow_key: RandomState,
    windows: Mutex<Windows>,
}

#[derive(Debug)]
struct Windows {
    /// Per client: the minute counted, and the requests in it.
    counts: HashMap<IpAddr, (u64, u32)>,
    /// The windows the clients the map has no room for share, by hash.
    overflow: Box<[(u64, u32); OVERFLOW_WINDOWS]>,
    /// The minute of the last sweep, so a busy minute sweeps once, not on
    /// every request.
    swept: u64,
}

/// Distinct clients tracked before stale windows are swept.
const SWEEP_AT: usize = 10_000;

/// Distinct clients tracked at most; any more share the overflow windows.
const MAX_TRACKED: usize = 100_000;

/// Overflow windows: a power of two, so a hash masks to an index; 1,024
/// windows take 16 KiB.
const OVERFLOW_WINDOWS: usize = 1024;

/// The client `ip` is counted as: the IPv4 address, or the IPv6 /64.
fn client_key(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from_bits(v6.to_bits() & !u128::from(u64::MAX))),
        IpAddr::V4(v4) => IpAddr::V4(v4),
    }
}

/// Count one request in `window` for `minute`; `true` while it is within
/// `limit`.
const fn count(window: &mut (u64, u32), minute: u64, limit: u32) -> bool {
    let (m, count) = window;
    if *m != minute {
        *m = minute;
        *count = 0;
    }
    *count = count.saturating_add(1);
    *count <= limit
}

impl RateLimiter {
    /// A limiter allowing `per_minute` requests per client; 0 allows all.
    #[must_use]
    pub fn new(per_minute: u32) -> Self {
        Self {
            per_minute,
            overflow_key: RandomState::new(),
            windows: Mutex::new(Windows {
                counts: HashMap::new(),
                overflow: Box::new([(0, 0); OVERFLOW_WINDOWS]),
                swept: 0,
            }),
        }
    }

    /// The overflow window client `key` counts in once the map is full.
    fn overflow_slot(&self, key: IpAddr) -> usize {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "masked to below OVERFLOW_WINDOWS"
        )]
        let slot = (self.overflow_key.hash_one(key) & (OVERFLOW_WINDOWS as u64 - 1)) as usize;
        slot
    }

    /// Count a request from `ip` at `now`; `false` once its client is over
    /// the limit for the current minute.
    pub fn allow(&self, ip: Option<IpAddr>, now: u64) -> bool {
        let (Some(ip), true) = (ip, self.per_minute > 0) else {
            return true;
        };
        let key = client_key(ip);
        let minute = now / 60;
        let mut windows = self
            .windows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if windows.counts.len() >= SWEEP_AT && windows.swept != minute {
            windows.counts.retain(|_, (m, _)| *m == minute);
            windows.swept = minute;
        }
        let Windows {
            counts, overflow, ..
        } = &mut *windows;
        if counts.len() < MAX_TRACKED || counts.contains_key(&key) {
            return count(
                counts.entry(key).or_insert((minute, 0)),
                minute,
                self.per_minute,
            );
        }
        let slot = self.overflow_slot(key);
        overflow
            .get_mut(slot)
            .is_none_or(|window| count(window, minute, self.per_minute))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
