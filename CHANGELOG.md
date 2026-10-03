# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- deCDN crates are git dependencies on `github.com/decdn/decdn`, so a fresh
  clone builds on its own; `Cargo.lock` pins the deCDN commit and `decdn.ref`
  is gone.
- Licensed under MIT OR Apache-2.0.
