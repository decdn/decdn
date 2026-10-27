// The body of `GET /_status`, field for field as the gateway serializes it.
// src/fixtures/ holds that body as the Rust tests pin it, and
// contract.test.ts checks these types against it.

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

export interface Status {
  mode: "loopback" | "public";
  version: string;
  dashboard_built: boolean;
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

/** The header a loopback gateway reads its token from. */
export const TOKEN_HEADER = "x-decdn-token";

export type Fetched =
  | { kind: "ok"; status: Status }
  | { kind: "unauthorized" }
  | { kind: "error"; message: string };

/** How long a read waits for the gateway: past its own 5 s store read. */
export const STATUS_TIMEOUT_MS = 8_000;

/**
 * Read `/_status`, with `token` when there is one. A gateway that does not
 * answer within `timeoutMs` is an error, so the page never shows a stalled
 * gateway as live.
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
    if (typeof body !== "object" || body === null || !("counters" in body)) {
      return { kind: "error", message: "the gateway's answer is not a status" };
    }
    return { kind: "ok", status: body as Status };
  } catch (e) {
    if (signal.aborted) {
      return {
        kind: "error",
        message: `the gateway did not answer within ${timeoutMs / 1000} s`,
      };
    }
    return {
      kind: "error",
      message: e instanceof Error ? e.message : String(e),
    };
  }
}
