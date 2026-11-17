use std::time::Duration;

use super::{DIRECT_PATH_GRACE, PathRtt, path_rtt_with};

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// A direct path that reports within the grace sets the RTT.
#[tokio::test(start_paused = true)]
async fn a_direct_path_within_the_grace_sets_the_rtt() {
    let rtt = path_rtt_with(
        DIRECT_PATH_GRACE,
        async {
            tokio::time::sleep(ms(100)).await;
            Some(ms(40))
        },
        || Some(ms(300)),
    )
    .await;
    assert_eq!(
        rtt,
        Some(PathRtt {
            rtt: ms(40),
            direct: true
        })
    );
}

/// With no direct path within the grace, the selected path's estimate is
/// kept, and the wait stops at the grace.
#[tokio::test(start_paused = true)]
async fn no_direct_path_keeps_the_selected_path_rtt_after_the_grace() {
    let started = tokio::time::Instant::now();
    let rtt = path_rtt_with(DIRECT_PATH_GRACE, std::future::pending(), || Some(ms(300))).await;
    assert_eq!(
        rtt,
        Some(PathRtt {
            rtt: ms(300),
            direct: false
        })
    );
    assert_eq!(started.elapsed(), DIRECT_PATH_GRACE);
}

/// A connection with no open path has no RTT.
#[tokio::test(start_paused = true)]
async fn no_path_has_no_rtt() {
    let rtt = path_rtt_with(ms(10), async { None }, || None).await;
    assert_eq!(rtt, None);
}
