use std::cell::RefCell;
use std::path::{Path, PathBuf};

/// Test-only override value. Distinguishes "no override active"
/// (caller should use `dirs::home_dir()`) from "override set to
/// no-home" (caller should treat as if the home directory is
/// unavailable). A `clippy::option_option` waiver in disguise.
#[derive(Clone)]
pub(crate) struct HomeOverride(Option<PathBuf>);

impl HomeOverride {
    pub(crate) fn into_inner(self) -> Option<PathBuf> {
        self.0
    }
}

thread_local! {
    static HOME_OVERRIDE: RefCell<Option<HomeOverride>> = const { RefCell::new(None) };
}

pub(crate) fn current_home_override() -> Option<HomeOverride> {
    HOME_OVERRIDE.with(|c| c.borrow().clone())
}

/// Run `f` with `home` standing in for `dirs::home_dir()` on the
/// current thread. Restores the previous override on return, even
/// if `f` panics.
pub(crate) fn with_home_override<R>(home: Option<&Path>, f: impl FnOnce() -> R) -> R {
    struct Guard(Option<HomeOverride>);
    impl Drop for Guard {
        fn drop(&mut self) {
            let prev = self.0.take();
            HOME_OVERRIDE.with(|c| *c.borrow_mut() = prev);
        }
    }
    let next = HomeOverride(home.map(Path::to_path_buf));
    let prev = HOME_OVERRIDE.with(|c| c.replace(Some(next)));
    let _g = Guard(prev);
    f()
}
