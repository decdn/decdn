// Pure reads of a `Status` into what the panels show.
import type { Lane, Status } from "./api";

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

/** The store's pools, with an upper bound on the headroom each has left. */
export function pools(status: Status): PoolRow[] {
  return (status.pools?.pools.pools ?? []).map((p) => {
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
export function capabilities(status: Status): CapabilityRow[] {
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
export function buckets(status: Status): BucketRow[] {
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
