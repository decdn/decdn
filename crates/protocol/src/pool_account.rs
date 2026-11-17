//! The CAIP-10 account id that names a `PaymentPool` contract on the wire (ADR
//! 005 § Payment pool routing).
//!
//! A [`PoolAccount`] is a [`ChainId`], a colon and an account address, such as
//! `eip155:42161:0xab16a96d359ec26a11e2c2b3d8f8b8942d5bfcdb`. The address
//! matches `[-.%a-zA-Z0-9]{1,128}`, so an id holds at most
//! [`MAX_POOL_ACCOUNT_LEN`] bytes. Every constructor and the decoder check
//! these rules, so a value of this type is always well formed.
//!
//! An `eip155` address of the form `0x` and 40 hex digits is stored in lower
//! case. Two spellings of one EVM contract are therefore one pool account.
//!
//! On the wire the id is a postcard string: a varint length and the ASCII
//! bytes.

use std::fmt;
use std::str::FromStr;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};

use crate::chain_id::{ChainId, ChainIdError, MAX_CHAIN_ID_LEN};

/// Longest account address of a [`PoolAccount`] in bytes (CAIP-10).
const MAX_ADDRESS_LEN: usize = 128;

/// Largest encoded length of a [`PoolAccount`] in bytes: a
/// [`MAX_CHAIN_ID_LEN`]-byte chain id, the colon and a 128-byte address.
pub const MAX_POOL_ACCOUNT_LEN: usize = MAX_CHAIN_ID_LEN + 1 + MAX_ADDRESS_LEN;

/// Most payment pools a receiver keeps from a `payment_pools` list (ADR 005 §
/// Accepted payment pools). The decoder drops every entry past this one.
pub const MAX_PAYMENT_POOLS: usize = 8;

/// Length of an EVM address in bytes.
const EVM_ADDRESS_LEN: usize = 20;

/// Why a string is not a CAIP-10 pool account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PoolAccountError {
    /// The string is longer than [`MAX_POOL_ACCOUNT_LEN`] bytes.
    #[error(
        "pool account is {len} bytes; a CAIP-10 account id holds at most {MAX_POOL_ACCOUNT_LEN}"
    )]
    TooLong {
        /// The refused length in bytes.
        len: usize,
    },
    /// The chain id part breaks the CAIP-2 rules.
    #[error("pool account chain id: {0}")]
    Chain(ChainIdError),
    /// The string has no colon between the chain id and the address.
    #[error("pool account has no `:` between chain id and address")]
    MissingAddress,
    /// The address is not 1 to 128 characters of `[-.%a-zA-Z0-9]`.
    #[error("pool account address must be 1-128 characters of [-.%a-zA-Z0-9]")]
    BadAddress,
}

/// A CAIP-10 account id that names one `PaymentPool` contract, such as
/// `eip155:42161:0xab16a96d359ec26a11e2c2b3d8f8b8942d5bfcdb` (ADR 005 §
/// Payment pool routing).
///
/// The value is always well formed: [`PoolAccount::new`], [`FromStr`] and the
/// serde decoder refuse a string that breaks the CAIP-10 rules. It is `Copy`
/// and stores its bytes inline. Equality and hashing follow the string;
/// ordering follows the chain id, then the address.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PoolAccount {
    /// The chain that runs the contract.
    chain_id: ChainId,
    /// Length of the address in `address`, at most `MAX_ADDRESS_LEN`.
    len: u8,
    /// The ASCII address, zero past `len`.
    address: [u8; MAX_ADDRESS_LEN],
}

impl PoolAccount {
    /// Parse and check a CAIP-10 pool account.
    ///
    /// # Errors
    ///
    /// A [`PoolAccountError`] that names the first rule the string breaks.
    pub fn new(id: &str) -> Result<Self, PoolAccountError> {
        if id.len() > MAX_POOL_ACCOUNT_LEN {
            return Err(PoolAccountError::TooLong { len: id.len() });
        }
        // The chain id holds exactly one colon, so the address follows the
        // second one.
        let (namespace, rest) = id
            .split_once(':')
            .ok_or(PoolAccountError::Chain(ChainIdError::MissingColon))?;
        let Some((reference, address)) = rest.split_once(':') else {
            // Report a chain id fault first, so `EIP155:1` names the namespace.
            ChainId::new(id).map_err(PoolAccountError::Chain)?;
            return Err(PoolAccountError::MissingAddress);
        };
        let chain_len = namespace.len() + 1 + reference.len();
        let chain = id.get(..chain_len).unwrap_or_default();
        let chain_id = ChainId::new(chain).map_err(PoolAccountError::Chain)?;
        if address.is_empty()
            || address.len() > MAX_ADDRESS_LEN
            || !address
                .bytes()
                .all(|b| b == b'-' || b == b'.' || b == b'%' || b.is_ascii_alphanumeric())
        {
            return Err(PoolAccountError::BadAddress);
        }
        let mut account = Self::from_checked(chain_id, address);
        if account.chain_id.eip155_id().is_some() && is_evm_hex(address) {
            account.address.make_ascii_lowercase();
        }
        Ok(account)
    }

    /// The account of an EVM contract: the `eip155` chain id and the address
    /// as `0x` and 40 lower-case hex digits.
    #[must_use]
    pub fn eip155(chain_id: u64, address: [u8; EVM_ADDRESS_LEN]) -> Self {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut text = String::with_capacity(2 + 2 * EVM_ADDRESS_LEN);
        text.push_str("0x");
        for byte in address {
            for nibble in [byte >> 4, byte & 0x0f] {
                text.push(char::from(
                    HEX.get(usize::from(nibble)).copied().unwrap_or(b'0'),
                ));
            }
        }
        Self::from_checked(ChainId::eip155(chain_id), &text)
    }

    /// The chain that runs the contract.
    #[must_use]
    pub const fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    /// The account address, such as `0xab16…fcdb`.
    #[must_use]
    pub fn address(&self) -> &str {
        let bytes = self
            .address
            .get(..usize::from(self.len))
            .unwrap_or_default();
        // Every constructor stores checked ASCII, so this never falls back.
        std::str::from_utf8(bytes).unwrap_or_default()
    }

    /// The 20-byte contract address, when this is an `eip155` account whose
    /// address is `0x` and 40 hex digits.
    #[must_use]
    pub fn eip155_address(&self) -> Option<[u8; EVM_ADDRESS_LEN]> {
        self.chain_id.eip155_id()?;
        let address = self.address();
        if !is_evm_hex(address) {
            return None;
        }
        let digits = address.get(2..)?.as_bytes();
        let mut out = [0u8; EVM_ADDRESS_LEN];
        let (pairs, _) = digits.as_chunks::<2>();
        for (slot, [hi, lo]) in out.iter_mut().zip(pairs) {
            *slot = (hex_value(*hi)? << 4) | hex_value(*lo)?;
        }
        Some(out)
    }

    /// Store an address that already meets the CAIP-10 rules.
    fn from_checked(chain_id: ChainId, address: &str) -> Self {
        let mut bytes = [0u8; MAX_ADDRESS_LEN];
        let len = address.len().min(MAX_ADDRESS_LEN);
        if let (Some(dst), Some(src)) = (bytes.get_mut(..len), address.as_bytes().get(..len)) {
            dst.copy_from_slice(src);
        }
        // `len` is at most MAX_ADDRESS_LEN (128), so the conversion holds.
        let len = u8::try_from(len).unwrap_or(u8::MAX);
        Self {
            chain_id,
            len,
            address: bytes,
        }
    }
}

/// Is `address` `0x` and 40 hex digits in either case?
fn is_evm_hex(address: &str) -> bool {
    address.len() == 2 + 2 * EVM_ADDRESS_LEN
        && address.starts_with("0x")
        && address.bytes().skip(2).all(|b| b.is_ascii_hexdigit())
}

/// The value of one ASCII hex digit.
fn hex_value(digit: u8) -> Option<u8> {
    char::from(digit)
        .to_digit(16)
        .and_then(|v| u8::try_from(v).ok())
}

impl PartialOrd for PoolAccount {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PoolAccount {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.chain_id
            .cmp(&other.chain_id)
            .then_with(|| self.address().cmp(other.address()))
    }
}

impl fmt::Debug for PoolAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PoolAccount")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for PoolAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.chain_id, self.address())
    }
}

impl FromStr for PoolAccount {
    type Err = PoolAccountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl Serialize for PoolAccount {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for PoolAccount {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PoolAccountVisitor;

        impl Visitor<'_> for PoolAccountVisitor {
            type Value = PoolAccount;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a CAIP-10 account id such as `eip155:42161:0xab…`")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<PoolAccount, E> {
                PoolAccount::new(v).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(PoolAccountVisitor)
    }
}

/// Decode a `payment_pools` list and keep its first [`MAX_PAYMENT_POOLS`]
/// entries.
pub(crate) fn deserialize_payment_pools<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<PoolAccount>>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut pools = Option::<Vec<PoolAccount>>::deserialize(deserializer)?;
    if let Some(pools) = pools.as_mut() {
        pools.truncate(MAX_PAYMENT_POOLS);
    }
    Ok(pools)
}

#[cfg(test)]
mod tests;
