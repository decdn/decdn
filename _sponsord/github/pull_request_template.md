<!-- markdownlint-disable-file MD041 -- a PR body starts at the section level -->

## What

<!-- What changes and why. Link the issue it resolves. -->

Closes #

<!-- Repeat the keyword for each issue: "Closes #1, Closes #2". A single keyword closes only the first issue in a list. -->

## Reviewer notes

<!-- What to read first, what stays unchanged on purpose, and any risk. -->

## Verification

<!-- The commands you ran and their result. -->

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo nextest run --workspace`
- [ ] Pool, keeper or daemon code changed: `cargo nextest run --features sponsord-core/anvil-e2e,sponsord/anvil-e2e -p sponsord-core -p sponsord` (needs `anvil` and `forge`)
- [ ] User-visible change: entry in the changed crate's `crates/<dir>/CHANGELOG.md`
- [ ] Installer archive names, `~/.decdn/sponsor.toml` or an `sponsord-api` type touched: every deployed installer and CLI still works (all three are contracts, see docs/architecture.md); OpenAPI documents regenerated if a type changed
- [ ] PR title uses a type the `pr-title` check allows (feat, fix, docs, chore, refactor, perf, test, build, ci, revert), and the subject does not start with an uppercase letter. The check gates merge and does not re-run on a title edit: push, or re-run the job.
