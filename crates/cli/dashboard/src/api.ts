// The body of `GET /_status`, field for field as `decdn dashboard`
// serializes it: the `decdn get` runs on the host, the gateway's own
// `/_status` body, and the node's admin reads. src/fixtures/ holds both
// bodies as the Rust tests pin them, and contract.test.ts checks these types
// against them.

// The gateway's `/_status` body, which the dashboard passes through.

export interface Counters {
  bytes_served: number;
  requests_ok: number;
  requests_not_found: number;
  requests_refused: number;
  requests_allowance_exhausted: number;
  pull_failures_funding: number;
  pull_failures_other: number;
  pulls_active: number;
  pointer_updates: number;
}

export type Payer = { kind: "pool" } | { kind: "capability"; signer: string };

export interface ActivePull {
  hash: string;
  namespace: number | null;
  /** `[offset, length]` of a ranged pull. */
  range: [number, number] | null;
  payer: Payer;
  since_secs: number;
  delivered_bytes: number;
}

export interface Capability {
  cap: number;
  expiry: number;
  origin: string | null;
  pool: string;
  scope: string;
  signer: string;
  spent: boolean;
}

export interface Bucket {
  namespace: number | null;
  per_day: number | null;
  used: number;
  remaining: number | null;
  resets_in_secs: number;
}

export interface Pointer {
  namespace: number;
  root: string;
  seq: number;
  signer: string;
}

export interface Lane {
  voucher_signer: string;
  provider: string;
  last_amount_micro_usdc: number;
  last_bytes_delivered: number;
}

export interface Pool {
  pool_id: string;
  chain_id: number;
  payment_pool: string;
  owner: string;
  token: string;
  deposit_micro_usdc: number;
  spent_micro_usdc: number;
  lanes: Lane[];
}

export interface DelegateLanes {
  signer: string;
  pool_id: string;
  lanes: Lane[];
}

export interface PoolSnapshot {
  pools: { pools: Pool[]; skipped: string[] };
  delegate_lanes: DelegateLanes[];
}

export interface GatewayStatus {
  mode: "loopback" | "public";
  version: string;
  started_unix: number;
  uptime_secs: number;
  listen: string;
  chain_id: number;
  payment_pool: string;
  counters: Counters;
  active_pulls: ActivePull[];
  capabilities: Capability[];
  allowance: Bucket[] | null;
  pointers: Pointer[] | null;
  pools: PoolSnapshot | null;
  pool_error: string | null;
}

// The `decdn get` runs this host recorded.

export type PullKind = "blob" | "bundle" | "entry";

export interface Paid {
  amount_micro_usdc: number;
  /** The wire bytes the amount pays for. */
  bytes: number;
}

export interface NodePaid extends Paid {
  /** The node's operator address. */
  provider: string;
}

/** An ended bundle run's counts, as its summary prints them. */
export interface BundleCounts {
  fetched: number;
  linked: number;
  skipped: number;
  excluded: number;
  failed: number;
  not_run: number;
  downloaded: number;
  resumed_bytes: number;
  reused_bytes: number;
}

/** What every run's record carries, whatever its state. */
export interface PullFields {
  uri: string;
  kind: PullKind;
  /** A file, a directory, or `-` for stdout. */
  output: string;
  pid: number;
  started_unix_ms: number;
  /** The run's heartbeat. */
  updated_unix_ms: number;
  received_bytes: number;
  /** Null until the run knows what it fetches. */
  expected_bytes: number | null;
  files_started: number;
  files_landed: number;
  paid: Paid;
  nodes: NodePaid[];
  /** Set once a bundle run ended. */
  bundle: BundleCounts | null;
  /** Says `running`, but its heartbeat stopped: it exited without a summary. */
  stale: boolean;
}

/** A run whose record says it is live. */
export type RunningPull = PullFields & { state: "running" };

/** A run that ended: when, and for a failure or a give-up, why. */
export type EndedPull = PullFields &
  (
    | { state: "done"; finished_unix_ms: number }
    | { state: "interrupted"; finished_unix_ms: number }
    | { state: "failed"; finished_unix_ms: number; error: string }
    | { state: "gave_up"; finished_unix_ms: number; error: string }
  );

export type PullRecord = RunningPull | EndedPull;

export type PullState = PullRecord["state"];

export interface Pulls {
  /** The directory the records live in. */
  dir: string;
  /** Null before the first read. */
  updated_unix_ms: number | null;
  /** Newest first. */
  active: RunningPull[];
  /** Newest first. */
  recent: EndedPull[];
  /** Files in `dir` that did not read as a record. */
  unreadable: number;
  /** Why `dir` cannot be listed; the lists are then empty. */
  error: string | null;
}

/** The gateway at `--gateway`, as the dashboard last asked it. */
export type GatewaySection = {
  url: string;
  updated_unix_ms: number | null;
} & (
  | { state: "waiting" }
  | { state: "absent" }
  | { state: "unauthorized" }
  | { state: "error"; message: string }
  | { state: "up"; status: GatewayStatus }
);

// The node's admin reads, mirroring decdn-common's `admin` DTOs.

export type BindingStatus = "bound" | "mismatch" | "unbound" | "unknown";

export interface HealthResponse {
  node_id: string;
  uptime_s: number;
  in_flight_streams: number;
  binding: BindingStatus;
  bound_node_id: string | null;
  registry_active: boolean;
}

export interface BucketStat {
  index: number;
  fill: number;
}

export interface RoutingHealth {
  total_peers: number;
  non_empty_buckets: number;
  buckets: BucketStat[];
  bucket_capacity: number;
  refresh_interval_s: number;
  last_refresh_us: number | null;
}

export interface RecordStoreHealth {
  records: number;
  capacity: number;
}

export interface RepublishHealth {
  scheduled_records: number;
}

export interface StatusResponse {
  node_id: string;
  routing: RoutingHealth;
  known_stakers: number;
  record_store: RecordStoreHealth;
  republish: RepublishHealth;
  chain_denied_origins: number;
  operator_address: string | null;
}

export interface LaneSnapshot {
  pool_id: string;
  /** The pool owner; null when the node could not name it. */
  counterparty: string | null;
  voucher_signer: string;
  last_nonce: number;
  /** The node's whole accrued claim on the lane, redeemed or not. */
  outstanding_micro_usdc: number;
  unredeemed_micro_usdc: number;
  deposit_micro_usdc: number;
  /** Null when no voucher arrived since the node started. */
  seconds_since_last_voucher: number | null;
  settlement_eligible: boolean;
}

export interface LanesResponse {
  lanes: LaneSnapshot[];
  redeem_threshold_micro_usdc: number;
}

export interface EarningsPeriod {
  from_unix: number;
  net_micro_usdc: number;
  gross_micro_usdc: number;
  bytes_settled: number;
  settlements: number;
}

export interface LastSettlement {
  at_unix: number;
  tx_hash: string;
  block_number: number;
  net_micro_usdc: number;
}

export interface PendingRedeem {
  /** An upper bound on the next sweep. */
  micro_usdc: number;
  lanes: number;
  /** Owed on lanes no sweep redeems. */
  stranded_micro_usdc: number;
  stranded_lanes: number;
  last_sweep_at_unix: number | null;
  next_sweep_at_unix: number | null;
}

export interface EarningsResponse {
  operator_address: string | null;
  /** The earliest settlement the ledger holds; null while it holds none. */
  ledger_since_unix: number | null;
  last_24h: EarningsPeriod;
  last_7d: EarningsPeriod;
  since_ledger_start: EarningsPeriod;
  since: EarningsPeriod | null;
  last_settlement: LastSettlement | null;
  pending: PendingRedeem | null;
}

export interface BuyerLaneSnapshot {
  voucher_signer: string;
  provider: string;
  last_amount_micro_usdc: number;
  last_bytes_delivered: number;
}

export interface BuyerPoolSnapshot {
  pool_id: string;
  chain_id: number;
  payment_pool: string;
  owner: string;
  token: string;
  deposit_micro_usdc: number;
  spent_micro_usdc: number;
  lanes: BuyerLaneSnapshot[];
}

export interface BuyerPoolsResponse {
  pools: BuyerPoolSnapshot[];
  skipped: string[];
}

export interface SlashRecordDto {
  /** A `uint256`, in decimal. */
  slash_id: string;
  /** `0` rate manipulation, `1` blacklist. */
  offense_type: number;
  /** TOKEN base units, in decimal. */
  amount: string;
  evidence_hash: string;
  block_number: number | null;
  /** Nominal: a pause only moves the real deadline later. */
  appeal_window_close: number | null;
}

export interface SlashesResponse {
  slashes: SlashRecordDto[];
}

/** Per-second rates since the scrape before. */
export interface NodeRates {
  hits: number;
  misses: number;
  bytes: number;
}

/** The node's `/metrics`, as `decdn node top` reads it. */
export interface NodeMetrics {
  uptime_seconds: number;
  active_connections: number;
  dispatch_in_flight: number;
  cache_hits: number;
  cache_misses: number;
  cache_bytes_returned: number;
  /** Micro-USDC in accepted vouchers not yet redeemed. */
  unredeemed_usdc: number;
  rpc_healthy: boolean;
  /** Null on the first scrape. */
  rates: NodeRates | null;
  /** Hits over hits and misses since start; null before either. */
  hit_rate: number | null;
}

/**
 * One of the node's reads, as the dashboard last made it. `unconfigured`:
 * the node runs without what the read reports.
 */
export type Part<T> = { updated_unix_ms: number | null } & (
  | { state: "waiting" }
  | { state: "ok"; value: T }
  | { state: "unconfigured"; message: string }
  | { state: "error"; message: string }
);

/**
 * Whether a node answers at the admin URL: what its health read says.
 * `error`: the health read failed other than by a refused connection, and
 * the other reads may still have answered.
 */
export type NodeState =
  | { state: "waiting" }
  | { state: "absent" }
  | { state: "up" }
  | { state: "error"; message: string };

export type NodeSection = {
  admin_url: string;
  metrics_url: string;
  health: Part<HealthResponse>;
  status: Part<StatusResponse>;
  metrics: Part<NodeMetrics>;
  lanes: Part<LanesResponse>;
  earnings: Part<EarningsResponse>;
  pools: Part<BuyerPoolsResponse>;
  slashes: Part<SlashesResponse>;
} & NodeState;

/** The dashboard's `/_status` body. */
export interface Status {
  version: string;
  /** When the dashboard answered: the clock every `updated_unix_ms` is
   * measured against. */
  now_unix_ms: number;
  /** Whether the binary embeds the page's build. */
  dashboard_built: boolean;
  started_unix: number;
  pulls: Pulls;
  gateway: GatewaySection;
  node: NodeSection;
}

/** The header `/_status` reads the dashboard's token from. */
export const TOKEN_HEADER = "x-decdn-token";

export type Fetched =
  | { kind: "ok"; status: Status }
  | { kind: "unauthorized" }
  | { kind: "error"; message: string };

/** How long a read waits: the dashboard answers from memory, at once. */
export const STATUS_TIMEOUT_MS = 5_000;

/**
 * Read `/_status`, with `token` when there is one. A dashboard that does not
 * answer within `timeoutMs` is an error, so the page never shows a stalled
 * read as live.
 */
export async function fetchStatus(
  token: string | null,
  fetchImpl: typeof fetch = fetch,
  timeoutMs: number = STATUS_TIMEOUT_MS,
): Promise<Fetched> {
  const headers: Record<string, string> = {};
  if (token) headers[TOKEN_HEADER] = token;
  const signal = AbortSignal.timeout(timeoutMs);
  try {
    const response = await fetchImpl("/_status", {
      headers,
      cache: "no-store",
      signal,
    });
    if (response.status === 401) return { kind: "unauthorized" };
    if (!response.ok) {
      return {
        kind: "error",
        message: `${response.status} ${(await response.text()).trim()}`,
      };
    }
    const body: unknown = await response.json();
    if (
      typeof body !== "object" ||
      body === null ||
      !("pulls" in body) ||
      !("node" in body)
    ) {
      return {
        kind: "error",
        message: "the dashboard's answer is not its status",
      };
    }
    return { kind: "ok", status: body as Status };
  } catch (e) {
    if (signal.aborted) {
      return {
        kind: "error",
        message: `the dashboard did not answer within ${timeoutMs / 1000} s`,
      };
    }
    return {
      kind: "error",
      message: e instanceof Error ? e.message : String(e),
    };
  }
}
