use alloy::primitives::{Address, B256, address};
use alloy::signers::local::PrivateKeySigner;

use super::{LaneSalt, derive_lane_signer};

fn owner(byte: u8) -> anyhow::Result<PrivateKeySigner> {
    Ok(PrivateKeySigner::from_bytes(&B256::repeat_byte(byte))?)
}

fn lane_address(owner: &PrivateKeySigner, salt: [u8; 32]) -> anyhow::Result<Address> {
    Ok(derive_lane_signer(owner, &LaneSalt::from_bytes(salt))?.address())
}

/// A data dir re-derives its lane key on every run, so the same owner and salt
/// must give the same key every time.
#[test]
fn derivation_is_deterministic() -> anyhow::Result<()> {
    let owner = owner(0x11)?;
    anyhow::ensure!(lane_address(&owner, [0x22; 32])? == lane_address(&owner, [0x22; 32])?);
    Ok(())
}

/// Two data dirs on one wallet hold different salts, and must sign on different
/// lanes (#2468).
#[test]
fn another_salt_gives_another_signer() -> anyhow::Result<()> {
    let owner = owner(0x11)?;
    anyhow::ensure!(lane_address(&owner, [0x22; 32])? != lane_address(&owner, [0x23; 32])?);
    Ok(())
}

#[test]
fn another_owner_gives_another_signer() -> anyhow::Result<()> {
    anyhow::ensure!(
        lane_address(&owner(0x11)?, [0x22; 32])? != lane_address(&owner(0x12)?, [0x22; 32])?
    );
    Ok(())
}

/// The lane key is not the owner key, so a data dir does not sign on the lanes
/// the owner key itself signs on.
#[test]
fn the_lane_signer_is_not_the_owner() -> anyhow::Result<()> {
    let owner = owner(0x11)?;
    anyhow::ensure!(lane_address(&owner, [0x22; 32])? != owner.address());
    Ok(())
}

/// A fresh salt is random: two draws differ.
#[test]
fn random_salts_differ() {
    assert_ne!(LaneSalt::random(), LaneSalt::random());
}

/// Pins the derivation, cross-checked with `cast keccak` and `cast wallet
/// address` over the same bytes. A change to the domain, the input order or
/// the hash derives another key for every data dir and leaves each recorded
/// lane under a signer that no longer signs, so it must fail here first.
#[test]
fn derivation_known_answer() -> anyhow::Result<()> {
    let got = lane_address(&owner(0x11)?, [0x22; 32])?;
    let want = address!("0x7ABCCAe8f18F024315F289Ad33291De88f84a4C2");
    anyhow::ensure!(got == want, "derived {got}, want {want}");
    Ok(())
}
