<!-- markdownlint-disable-file MD041 -- a PR body starts at the section level -->

## What

<!-- What changes and why. Link the issue it resolves. -->

Closes #

<!-- Repeat the keyword for each issue: "Closes #1, Closes #2". A single keyword closes only the first issue in a list. -->

## Breaking

<!-- Delete this section if nothing breaks. Pre-mainnet, a breaking change is fine: name what breaks
     (wire / postcard, config, contract ABI, storage layout, sponsord API or installer arguments) and what
     an operator or a running download sees. The CHANGELOG entry goes under a "(BREAKING)" heading. -->

## Reviewer notes

<!-- What to read first, what stays unchanged on purpose, and any risk. -->

## Verification

<!-- The commands you ran and their result. -->

Always:

- [ ] `pre-commit run --all-files`
- [ ] `CARGO_BUILD_WARNINGS=deny cargo clippy --workspace --all-targets --all-features` (CI lints with `--all-features`, so the `anvil-e2e` targets count)
- [ ] `cargo nextest run`
- [ ] PR title uses a type the `pr-title` check allows (feat, fix, docs, chore, refactor, perf, test, build, ci, revert) and the subject does not start with an uppercase letter. The check does not re-run on a title edit: push, or re-run the job.

Only if touched:

- [ ] `contracts/`: `(cd contracts && forge fmt --check && FOUNDRY_PROFILE=ci forge build --sizes --deny warnings && forge test)`, and `.gas-snapshot` regenerated with `(cd contracts && FOUNDRY_PROFILE=ci forge snapshot --snap .gas-snapshot)`
- [ ] Public API of `decdn-protocol` or `decdn-client`: the `.snap` regenerated ([CONTRIBUTING § Public API snapshot](https://github.com/decdn/decdn/blob/main/CONTRIBUTING.md#public-api-snapshot))
- [ ] A type in `sponsord-api`: `UPDATE_OPENAPI=1 cargo test -p sponsord-api --test openapi`
- [ ] `adr/`: `make -C adr check-refs`, `adr/architecture.md` summary updated, terms and parameters consistent across ADRs
- [ ] A crate dependency edge: the table in `.github/scripts/check_crate_edges.py` and the CLAUDE.md § Crate Structure paragraph updated together
- [ ] A metric name: the row in `adr/appendix-observability.md` and the dashboards in `decdn/devops`
- [ ] User-visible change: `CHANGELOG.md` entry added by hand, one per PR
