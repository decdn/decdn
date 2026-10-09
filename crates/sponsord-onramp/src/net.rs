//! Who is asking: the requester's IP address (from the socket, or from a
//! header set by a trusted reverse proxy), and per-IP rate limits.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;

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

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let peer = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip());
        let ip = match &state.client_ip {
            ClientIpSource::Peer => peer,
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
        Ok(Self(ip))
    }
}

/// A fixed-window limit of `per_minute` requests per IP address. A limit of
/// 0 disables it; a request with no known address is never limited.
#[derive(Debug)]
pub struct RateLimiter {
    per_minute: u32,
    windows: Mutex<Windows>,
}

#[derive(Debug, Default)]
struct Windows {
    /// Per address: the minute counted, and the requests in it.
    counts: HashMap<IpAddr, (u64, u32)>,
    /// The minute of the last sweep, so a busy minute sweeps once, not on
    /// every request.
    swept: u64,
}

/// Distinct addresses tracked before stale windows are swept.
const SWEEP_AT: usize = 10_000;

impl RateLimiter {
    /// A limiter allowing `per_minute` requests per address; 0 allows all.
    #[must_use]
    pub fn new(per_minute: u32) -> Self {
        Self {
            per_minute,
            windows: Mutex::default(),
        }
    }

    /// Count a request from `ip` at `now`; `false` once `ip` is over its limit
    /// for the current minute.
    pub fn allow(&self, ip: Option<IpAddr>, now: u64) -> bool {
        let (Some(ip), true) = (ip, self.per_minute > 0) else {
            return true;
        };
        let minute = now / 60;
        let mut windows = self
            .windows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if windows.counts.len() >= SWEEP_AT && windows.swept != minute {
            windows.counts.retain(|_, (m, _)| *m == minute);
            windows.swept = minute;
        }
        let (m, count) = windows.counts.entry(ip).or_insert((minute, 0));
        if *m != minute {
            *m = minute;
            *count = 0;
        }
        *count = count.saturating_add(1);
        *count <= self.per_minute
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn limits_per_ip_per_minute() {
        let limiter = RateLimiter::new(2);
        let a = Some("203.0.113.1".parse().unwrap());
        let b = Some("203.0.113.2".parse().unwrap());
        assert!(limiter.allow(a, 60));
        assert!(limiter.allow(a, 61));
        assert!(!limiter.allow(a, 62));
        assert!(limiter.allow(b, 62), "another address has its own budget");
        assert!(limiter.allow(a, 120), "a new minute resets the count");
    }

    #[test]
    fn a_full_map_is_swept_once_per_minute() {
        let limiter = RateLimiter::new(5);
        let ip = |n: u32| Some(IpAddr::from(n.to_be_bytes()));
        let sweep_at = u32::try_from(SWEEP_AT).unwrap();
        for n in 0..sweep_at {
            assert!(limiter.allow(ip(n), 60));
        }
        // A new minute: the first request sweeps the stale windows...
        assert!(limiter.allow(ip(u32::MAX), 120));
        assert_eq!(limiter.windows.lock().unwrap().counts.len(), 1);
        // ...and refilling the map within that minute sweeps nothing more.
        for n in 0..sweep_at {
            assert!(limiter.allow(ip(n), 121));
        }
        assert_eq!(limiter.windows.lock().unwrap().swept, 2);
        assert_eq!(limiter.windows.lock().unwrap().counts.len(), SWEEP_AT + 1);
    }

    #[test]
    fn zero_or_unknown_address_is_never_limited() {
        let off = RateLimiter::new(0);
        let a = Some("203.0.113.1".parse().unwrap());
        assert!((0..100).all(|_| off.allow(a, 60)));
        let on = RateLimiter::new(1);
        assert!((0..100).all(|_| on.allow(None, 60)));
    }
}
