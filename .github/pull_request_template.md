<!-- markdownlint-disable-file MD041 -- a PR body starts at the section level -->

## What

<!-- What changes and why. Link the issue it resolves. -->

Closes #

<!-- Repeat the keyword for each issue: "Closes #1, Closes #2". A single keyword closes only the first issue in a list. -->

## Reviewer notes

<!-- What to read first, what stays unchanged on purpose, and any risk. -->

## Verification

<!-- The commands you ran and their result. -->

- [ ] `cargo fmt -- --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo nextest run`
- [ ] `contracts/` changed: `(cd contracts && forge fmt --check && FOUNDRY_PROFILE=ci forge build --sizes --deny warnings && forge test)`, and `.gas-snapshot` regenerated with `(cd contracts && FOUNDRY_PROFILE=ci forge snapshot --snap .gas-snapshot)`
- [ ] User-visible change: `CHANGELOG.md` entry added
- [ ] Design change: ADR in `adr/` added or updated
- [ ] PR title uses a type the `pr-title` check allows (feat, fix, docs, chore, refactor, perf, test, build, ci, revert), and the subject does not start with an uppercase letter. The check gates merge and does not re-run on a title edit: push, or re-run the job.
