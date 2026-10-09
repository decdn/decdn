# Changelog

<!-- Maintained by hand, one entry per PR that changes this crate, under
     [Unreleased]. `cargo release` only opens the version heading above those
     entries; git-cliff generates the GitHub release notes. Conventions:
     RELEASING.md § Changelogs. -->

All notable changes to `sponsord-api` are documented in this file. It shares
one version with every sponsord crate, released together from `vX.Y.Z`
tags; a release with no entry here changed nothing in this crate.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the crate follows [Semantic Versioning](https://semver.org/) from its first
release.

## [Unreleased]

### Added

- **New crate: the wire contracts of both servers.** Request and response
  bodies (`IssuedCapability`, `daemon::*`, `onramp::*`), route paths,
  `ErrorCode` (with its HTTP status) and `ErrorBody`, `MicroUsdc`, and
  `time::Clock`. Typed clients behind `client` (`DaemonClient`,
  `OnrampClient`), `IntoResponse` behind `axum`, a redacted `Secret` behind
  `secret`, and OpenAPI documents behind `openapi`, committed under
  `docs/openapi/`. `DaemonClient` keeps its token in a `Secret`, and
  `OnrampClient::poll_capability`'s timeout bounds the whole poll.
