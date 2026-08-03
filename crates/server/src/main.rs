use decdn_incentive::voucher_domain;

mod money;
mod store;
mod cap;
mod topup_auth;
mod captcha;

fn main() {
    // Spike: prove the decdn path dep resolves and a symbol is reachable.
    let _ = voucher_domain as fn(u64, alloy::primitives::Address) -> _;
    println!("sponsord skeleton ok");
}
