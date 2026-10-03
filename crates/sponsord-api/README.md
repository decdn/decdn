# sponsord-api

The wire contracts of [`sponsord`](https://crates.io/crates/sponsord), deCDN's
sponsorship daemon, and of its public onramp: request and response bodies,
error codes, and route paths, defined once for both servers and every client.

- `client` (feature): `DaemonClient`, for a gate of your own in front of the
  daemon, and `OnrampClient`, for clients of an onramp.
- `openapi` (feature): the OpenAPI 3.1 documents for both servers.

See the [integrator guide](https://github.com/decdn/sponsord/blob/main/docs/integrator.md).

Licensed under either of MIT or Apache-2.0, at your option.
