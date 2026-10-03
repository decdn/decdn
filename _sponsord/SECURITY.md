# Security

## Reporting a vulnerability

Email `security@decdn.org`. Please do not open a public issue for security
reports.

`sponsord` holds the key that owns a funded `PaymentPool`, so reports about
any of the following are especially welcome:

- a way to get a capability from `sponsord` without its bearer token, or from
  `sponsord-onramp` without passing its gate;
- a way to get a capability with terms above the daemon's maximum;
- anything that leaks the treasury key, its password, or the API token;
- installer behaviour that runs or installs something other than the pinned,
  checksum-verified release binaries.

## Supported versions

Only the latest release receives fixes.
