// Pure reads of a `Status` into what the panels show.
import type {
  EarningsPeriod,
  EarningsResponse,
  GatewaySection,
  GatewayStatus,
  Lane,
  NodeSection,
  Part,
  Pool,
  PullKind,
  Pulls,
  PullState,
} from "./api";

export type Severity = "ok" | "warning" | "critical";

/** How close a share used is to its limit: 80% warns, 95% is critical. */
export function severity(used: number | null): Severity {
  if (used === null || used < 0.8) return "ok";
  return used < 0.95 ? "warning" : "critical";
}

function share(part: number, whole: number): number {
  return whole <= 0 ? 1 : Math.min(1, part / whole);
}

const same = (a: string, b: string) => a.toLowerCase() === b.toLowerCase();

export interface PoolRow {
  id: string;
  owner: string;
  deposit: number;
  spent: number;
  /** An upper bound on what every signer on the pool may still spend: the
   * spend is a lower bound, missing vouchers that no node redeemed yet. */
  headroom: number;
  used: number;
  severity: Severity;
  lanes: Lane[];
}

/** The gateway store's pools, with an upper bound on each one's headroom. */
export function pools(status: GatewayStatus): PoolRow[] {
  return poolRows(status.pools?.pools.pools ?? []);
}

/** `list`, with an upper bound on the headroom each pool has left. */
export function poolRows(list: Pool[]): PoolRow[] {
  return list.map((p) => {
    const used = share(p.spent_micro_usdc, p.deposit_micro_usdc);
    return {
      id: p.pool_id,
      owner: p.owner,
      deposit: p.deposit_micro_usdc,
      spent: p.spent_micro_usdc,
      headroom: Math.max(0, p.deposit_micro_usdc - p.spent_micro_usdc),
      used,
      severity: severity(used),
      lanes: p.lanes,
    };
  });
}

export interface CapabilityRow {
  signer: string;
  pool: string;
  scope: string;
  origin: string | null;
  cap: number;
  expiry: number;
  exhausted: boolean;
  /** What its key signed for, summed over its lanes; null when unknown. */
  spentMicro: number | null;
  bytes: number | null;
  used: number | null;
  severity: Severity;
}

/** Each capability, with what its delegate key's lanes spent of its cap. */
export function capabilities(status: GatewayStatus): CapabilityRow[] {
  const delegates = status.pools?.delegate_lanes;
  return status.capabilities.map((c) => {
    const lanes = delegates
      ?.filter((d) => same(d.signer, c.signer) && same(d.pool_id, c.pool))
      .flatMap((d) => d.lanes);
    const spentMicro =
      lanes?.reduce((sum, l) => sum + l.last_amount_micro_usdc, 0) ?? null;
    const bytes =
      lanes?.reduce((sum, l) => sum + l.last_bytes_delivered, 0) ?? null;
    const used = spentMicro === null ? null : share(spentMicro, c.cap);
    return {
      signer: c.signer,
      pool: c.pool,
      scope: c.scope,
      origin: c.origin,
      cap: c.cap,
      expiry: c.expiry,
      exhausted: c.spent,
      spentMicro,
      bytes,
      used,
      severity: c.spent ? "critical" : severity(used),
    };
  });
}

export interface BucketRow {
  label: string;
  perDay: number | null;
  usedBytes: number;
  remaining: number | null;
  /** The share of the day's bytes delivered; null when unbounded. */
  used: number | null;
  severity: Severity;
  resetsIn: number;
}

/** A public gateway's allowance buckets; none on loopback. */
export function buckets(status: GatewayStatus): BucketRow[] {
  return (status.allowance ?? []).map((b) => {
    const used = b.per_day === null ? null : share(b.used, b.per_day);
    return {
      label: b.namespace === null ? "Shared" : `Namespace ${b.namespace}`,
      perDay: b.per_day,
      usedBytes: b.used,
      remaining: b.remaining,
      used,
      severity: severity(used),
      resetsIn: b.resets_in_secs,
    };
  });
}

export interface LivePullRow {
  key: string;
  uri: string;
  kind: PullKind;
  output: string;
  received: number;
  expected: number | null;
  /** The share received; null until the run knows its size. */
  done: number | null;
  /** A bundle's blobs landed of those started; null for one blob. */
  files: string | null;
  paidMicro: number;
  nodes: number;
  /** Since the run started, as of the last read of the records. */
  ageSecs: number;
  /** The run exited without its last write. */
  stale: boolean;
}

const pullKey = (p: { pid: number; started_unix_ms: number; uri: string }) =>
  `${p.pid}-${p.started_unix_ms}-${p.uri}`;

/** The `decdn get` runs whose record says `running`, newest first. */
export function livePulls(pulls: Pulls): LivePullRow[] {
  const now = pulls.updated_unix_ms;
  return pulls.active.map((p) => ({
    key: pullKey(p),
    uri: p.uri,
    kind: p.kind,
    output: p.output,
    received: p.received_bytes,
    expected: p.expected_bytes,
    done:
      p.expected_bytes === null
        ? null
        : share(p.received_bytes, p.expected_bytes),
    files:
      p.kind === "bundle"
        ? `${p.files_landed} of ${p.files_started} files`
        : null,
    paidMicro: p.paid.amount_micro_usdc,
    nodes: p.nodes.length,
    ageSecs: now === null ? 0 : (now - p.started_unix_ms) / 1000,
    stale: p.stale,
  }));
}

export type Tone = "ok" | "muted" | "critical";

const OUTCOME: Record<PullState, { outcome: string; tone: Tone }> = {
  running: { outcome: "Running", tone: "muted" },
  done: { outcome: "✓ Done", tone: "ok" },
  failed: { outcome: "⛔ Failed", tone: "critical" },
  interrupted: { outcome: "Interrupted", tone: "muted" },
  gave_up: { outcome: "⚠ Gave up", tone: "critical" },
};

export interface RecentPullRow {
  key: string;
  outcome: string;
  tone: Tone;
  uri: string;
  output: string;
  received: number;
  durationSecs: number;
  paidMicro: number;
  nodes: number;
  error: string | null;
}

/** The `decdn get` runs that ended, newest first. */
export function recentPulls(pulls: Pulls): RecentPullRow[] {
  return pulls.recent.map((p) => ({
    key: pullKey(p),
    ...OUTCOME[p.state],
    uri: p.uri,
    output: p.output,
    received: p.received_bytes,
    durationSecs:
      ((p.finished_unix_ms ?? p.updated_unix_ms) - p.started_unix_ms) / 1000,
    paidMicro: p.paid.amount_micro_usdc,
    nodes: p.nodes.length,
    error: p.error,
  }));
}

/** What the gateway section says in place of its panels; null when up. */
export function gatewayLine(g: GatewaySection): string | null {
  switch (g.state) {
    case "waiting":
      return `Asking the gateway at ${g.url}…`;
    case "absent":
      return `No gateway at ${g.url}.`;
    case "unauthorized":
      return `The gateway at ${g.url} asks for a token: pass --gateway-token-file.`;
    case "error":
      return `Could not read the gateway at ${g.url}: ${g.message}`;
    case "up":
      return null;
  }
}

/** What the node section says in place of its panels; null when up. */
export function nodeLine(n: NodeSection): string | null {
  switch (n.state) {
    case "waiting":
      return `Asking the node at ${n.admin_url}…`;
    case "absent":
      return `No node at ${n.admin_url}.`;
    case "error":
      return `${n.admin_url} does not answer as a node${
        n.health.state === "error" ? `: ${n.health.message}` : "."
      }`;
    case "up":
      return null;
  }
}

export interface PartNote {
  text: string;
  /** A failed read, not an absent feature. */
  alert: boolean;
}

/** A read's value, or null while it has none. */
export function partValue<T>(part: Part<T>): T | null {
  return part.state === "ok" ? part.value : null;
}

/** What a node panel says in place of a read it has no value for. */
export function partNote(part: Part<unknown>): PartNote | null {
  switch (part.state) {
    case "ok":
      return null;
    case "waiting":
      return { text: "Waiting for the first read.", alert: false };
    case "unconfigured":
      return { text: part.message, alert: false };
    case "error":
      return { text: part.message, alert: true };
  }
}

export interface EarningsRow {
  label: string;
  period: EarningsPeriod;
}

/** The settled periods, the one a request asked for last. */
export function earningsRows(e: EarningsResponse): EarningsRow[] {
  const rows = [
    { label: "Last 24 h", period: e.last_24h },
    { label: "Last 7 days", period: e.last_7d },
    { label: "Since the ledger started", period: e.since_ledger_start },
  ];
  if (e.since) {
    rows.push({
      label: `Since ${new Date(e.since.from_unix * 1000).toISOString().slice(0, 10)}`,
      period: e.since,
    });
  }
  return rows;
}

/** A slash's offense type by name (ADR 014), or its number. */
export function offense(type: number): string {
  if (type === 0) return "Rate manipulation";
  if (type === 1) return "Blacklist";
  return `Offense ${type}`;
}
