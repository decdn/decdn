use super::*;

#[tokio::test]
async fn wait_resolves_again_after_it_fired() {
    let (fire, mut interrupt) = Interrupt::manual();
    fire.send(()).unwrap();
    interrupt.wait().await;
    // A second wait (a later `select!`) must not poll the spent oneshot.
    interrupt.wait().await;
}

#[tokio::test]
async fn wait_pends_while_nothing_fired() {
    let (_fire, mut interrupt) = Interrupt::manual();
    let waited = tokio::time::timeout(std::time::Duration::from_millis(20), interrupt.wait()).await;
    assert!(waited.is_err());
}
