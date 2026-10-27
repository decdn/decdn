import type { GatewayStatus } from "../api";
import { buckets, capabilities, pools } from "../view";
import { ActivePulls } from "./ActivePulls";
import { Allowance } from "./Allowance";
import { Capabilities } from "./Capabilities";
import { Pointers } from "./Pointers";
import { Pools } from "./Pools";
import { StatTiles } from "./StatTiles";

/** A gateway's panels, from a body `gatewayShapeProblem` passed. */
export function GatewayPanels(props: { status: GatewayStatus }) {
  const s = props.status;
  const publicMode = s.mode === "public";
  return (
    <>
      <StatTiles counters={s.counters} publicMode={publicMode} />
      <ActivePulls pulls={s.active_pulls} />
      {publicMode ? null : <Capabilities rows={capabilities(s)} />}
      <Pools
        rows={pools(s)}
        delegates={s.pools?.delegate_lanes ?? []}
        skipped={s.pools?.pools.skipped ?? []}
        error={s.pool_error}
      />
      {publicMode && s.allowance ? <Allowance rows={buckets(s)} /> : null}
      {publicMode && s.pointers ? <Pointers pointers={s.pointers} /> : null}
    </>
  );
}
