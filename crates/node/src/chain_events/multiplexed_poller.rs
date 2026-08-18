//! Multiplexed `eth_getLogs` polling: one poll tick, one `get_logs` call per
//! backfill window, demuxed by `(address, topic0)` into per-route sinks.
