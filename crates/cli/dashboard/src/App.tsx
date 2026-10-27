import { useCallback, useEffect, useState } from "react";
import {
  fetchStatus,
  type Fetched,
  type GatewaySection,
  type GatewayStatus,
  type NodeSection,
  type Pulls,
  type Status,
} from "./api";
import { ActivePulls } from "./components/ActivePulls";
import { Allowance } from "./components/Allowance";
import { Capabilities } from "./components/Capabilities";
import { Header } from "./components/Header";
import { LivePulls } from "./components/LivePulls";
import { NodeEarnings } from "./components/NodeEarnings";
import { NodeHealth } from "./components/NodeHealth";
import { NodeLanes } from "./components/NodeLanes";
import { NodePools } from "./components/NodePools";
import { NodeSlashes } from "./components/NodeSlashes";
import { Empty, Group } from "./components/Panel";
import { Pointers } from "./components/Pointers";
import { Pools } from "./components/Pools";
import { RecentPulls } from "./components/RecentPulls";
import { StatTiles } from "./components/StatTiles";
import { TokenForm } from "./components/TokenForm";
import { formatDuration, shortHex } from "./format";
import {
  adoptToken,
  clearToken,
  saveToken,
  session,
  type Adopted,
} from "./token";
import {
  buckets,
  capabilities,
  gatewayLine,
  livePulls,
  nodeLine,
  partValue,
  pools,
  recentPulls,
} from "./view";

const POLL_MS = 2_000;

function initialToken(): Adopted {
  return adoptToken(window.location, window.history, session(window));
}

/** Polls `/_status` every two seconds while the page is visible. */
function usePolledStatus(token: string | null) {
  const [last, setLast] = useState<Fetched | null>(null);
  const [status, setStatus] = useState<Status | null>(null);
  const [visible, setVisible] = useState(() => !document.hidden);

  useEffect(() => {
    const onChange = () => setVisible(!document.hidden);
    document.addEventListener("visibilitychange", onChange);
    return () => document.removeEventListener("visibilitychange", onChange);
  }, []);

  useEffect(() => {
    if (!visible) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const tick = async () => {
      const fetched = await fetchStatus(token);
      if (stopped) return;
      setLast(fetched);
      if (fetched.kind === "ok") setStatus(fetched.status);
      if (fetched.kind !== "unauthorized") timer = setTimeout(tick, POLL_MS);
    };
    void tick();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [token, visible]);

  return { last, status, live: visible && last?.kind === "ok" };
}

export function App() {
  const [adopted, setAdopted] = useState<Adopted>(initialToken);
  const token = adopted.token;
  const { last, status, live } = usePolledStatus(token);

  const acceptToken = useCallback((given: string) => {
    setAdopted({ token: given, kept: saveToken(session(window), given) });
  }, []);
  const forget = useCallback(() => {
    clearToken(session(window));
    setAdopted({ token: null, kept: true });
  }, []);

  const unauthorized = last?.kind === "unauthorized";
  const shown = unauthorized ? null : status;
  return (
    <main className="mx-auto flex max-w-6xl flex-col gap-4 px-4 py-6">
      <Header
        status={shown}
        live={live}
        onForget={token ? forget : undefined}
      />
      {unauthorized ? (
        <TokenForm rejected={token !== null} onToken={acceptToken} />
      ) : null}
      {token && !adopted.kept ? (
        <p role="status" className="text-sm text-ink-2">
          ⚠ This browser blocks storage for this page, so it keeps the token
          until you reload.
        </p>
      ) : null}
      {last?.kind === "error" ? (
        <p role="alert" className="text-sm text-critical-ink">
          ⛔ Could not read the dashboard's status: {last.message}
          {status ? " · showing the last answer" : ""}
        </p>
      ) : null}
      {last === null ? <p className="text-sm text-ink-3">Connecting…</p> : null}
      {shown ? <Dashboard status={shown} /> : null}
    </main>
  );
}

function Dashboard(props: { status: Status }) {
  return (
    <>
      <PullsGroup pulls={props.status.pulls} />
      <GatewayGroup gateway={props.status.gateway} />
      <NodeGroup node={props.status.node} />
    </>
  );
}

function PullsGroup(props: { pulls: Pulls }) {
  const p = props.pulls;
  return (
    <Group
      title="Pulls"
      note={<span className="font-mono text-xs">{p.dir}</span>}
    >
      {p.updated_unix_ms === null ? <Empty>Reading the records…</Empty> : null}
      {p.unreadable > 0 ? (
        <p role="alert" className="text-sm text-critical-ink">
          ⚠ {p.unreadable} file(s) in the directory did not read as a record.
        </p>
      ) : null}
      <LivePulls rows={livePulls(p)} />
      <RecentPulls rows={recentPulls(p)} />
    </Group>
  );
}

function GatewayGroup(props: { gateway: GatewaySection }) {
  const g = props.gateway;
  if (g.state !== "up") {
    return (
      <Group title="Gateway">
        <p
          role={g.state === "error" ? "alert" : undefined}
          className={`text-sm ${g.state === "error" ? "text-critical-ink" : "text-ink-3"}`}
        >
          {gatewayLine(g)}
        </p>
      </Group>
    );
  }
  const s = g.status;
  return (
    <Group
      title="Gateway"
      note={
        <>
          {s.mode} · v{s.version} · {s.listen} · chain {s.chain_id} · pool
          contract{" "}
          <span className="font-mono text-xs" title={s.payment_pool}>
            {shortHex(s.payment_pool)}
          </span>{" "}
          · up {formatDuration(s.uptime_secs)}
        </>
      }
    >
      <GatewayPanels status={s} />
    </Group>
  );
}

function GatewayPanels(props: { status: GatewayStatus }) {
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

function NodeGroup(props: { node: NodeSection }) {
  const n = props.node;
  const line = nodeLine(n);
  if (line !== null) {
    return (
      <Group title="Node">
        <p
          role={n.state === "error" ? "alert" : undefined}
          className={`text-sm ${n.state === "error" ? "text-critical-ink" : "text-ink-3"}`}
        >
          {line}
        </p>
      </Group>
    );
  }
  const id = partValue(n.health)?.node_id;
  const operator = partValue(n.status)?.operator_address;
  return (
    <Group
      title="Node"
      note={
        <>
          {n.admin_url}
          {id ? (
            <>
              {" "}
              · node{" "}
              <span className="font-mono text-xs" title={id}>
                {shortHex(id)}
              </span>
            </>
          ) : null}
          {operator ? (
            <>
              {" "}
              · operator{" "}
              <span className="font-mono text-xs" title={operator}>
                {shortHex(operator)}
              </span>
            </>
          ) : null}
        </>
      }
    >
      <NodeHealth node={n} />
      <NodeEarnings part={n.earnings} />
      <NodeLanes part={n.lanes} />
      <NodePools part={n.pools} />
      <NodeSlashes part={n.slashes} />
    </Group>
  );
}
