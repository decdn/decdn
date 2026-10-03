# Releasing

1. Make sure `Cargo.lock` pins the deCDN commit to build against
   (`cargo update -p decdn-incentive` moves it). Releases build `--locked`.
2. Bump `version` in the root `Cargo.toml` (`[workspace.package]`), then
   regenerate the OpenAPI documents, which carry the version:
   `UPDATE_OPENAPI=1 cargo test -p sponsord-api --test openapi`. Move the
   `## [Unreleased]` entries in `CHANGELOG.md` under the new version.
3. Push a `vMAJOR.MINOR.PATCH[-pre]` tag. `.github/workflows/release.yml`
   builds `decdn-sponsored` for Linux, macOS and Windows (x86_64 and aarch64
   each) and `sponsord` and `sponsord-onramp` for Linux, and publishes them
   with a `SHA256SUMS` manifest as a GitHub Release.
4. The release notes print the `ONRAMP_CLI_RELEASE` and
   `ONRAMP_CLI_SUMS_SHA256` values that pin it. Set them on an onramp to make
   its installers serve it. `decdn` releases are pinned the same way
   (`ONRAMP_DECDN_RELEASE`, and `ONRAMP_DECDN_SUMS_SHA256` = the SHA-256 of
   that release's `SHA256SUMS`).
5. If the release changes what the onramp and the CLI exchange, set
   `ONRAMP_MIN_CLI_VERSION` on the onramp once the new installers are live,
   so older CLIs ask their users to re-run the installer.
