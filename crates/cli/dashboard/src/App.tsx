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
import { GatewayPanels } from "./components/GatewayPanels";
import { Header } from "./components/Header";
import { LivePulls } from "./components/LivePulls";
import { NodeEarnings } from "./components/NodeEarnings";
import { NodeHealth } from "./components/NodeHealth";
import { NodeLanes } from "./components/NodeLanes";
import { NodePools } from "./components/NodePools";
import { NodeSlashes } from "./components/NodeSlashes";
import { Empty, Group } from "./components/Panel";
import { RecentPulls } from "./components/RecentPulls";
import { SectionBoundary } from "./components/SectionBoundary";
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
  gatewayLine,
  gatewayShapeProblem,
  livePulls,
  nodeView,
  partValue,
  POLL_MS as SOURCE_POLL_MS,
  recentPulls,
  staleSecs,
  stalestSecs,
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
  const s = props.status;
  const now = s.now_unix_ms;
  return (
    <>
      <SectionBoundary title="Pulls" resetKey={now}>
        <PullsGroup pulls={s.pulls} now={now} />
      </SectionBoundary>
      <SectionBoundary title="Gateway" resetKey={now}>
        <GatewayGroup gateway={s.gateway} now={now} />
      </SectionBoundary>
      <SectionBoundary title="Node" resetKey={now}>
        <NodeGroup node={s.node} now={now} />
      </SectionBoundary>
    </>
  );
}

function PullsGroup(props: { pulls: Pulls; now: number }) {
  const p = props.pulls;
  return (
    <Group
      title="Pulls"
      note={<span className="font-mono text-xs">{p.dir}</span>}
      stale={staleSecs(p.updated_unix_ms, props.now, SOURCE_POLL_MS.pulls)}
    >
      {p.updated_unix_ms === null ? <Empty>Reading the records…</Empty> : null}
      {p.error ? (
        <p role="alert" className="text-sm text-critical-ink">
          ⛔ The records cannot be read: {p.error}
        </p>
      ) : null}
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

function GatewayGroup(props: { gateway: GatewaySection; now: number }) {
  const g = props.gateway;
  const stale = staleSecs(g.updated_unix_ms, props.now, SOURCE_POLL_MS.gateway);
  const line = (text: string | null, alert: boolean) => (
    <Group title="Gateway" stale={stale}>
      <p
        role={alert ? "alert" : undefined}
        className={`text-sm ${alert ? "text-critical-ink" : "text-ink-3"}`}
      >
        {text}
      </p>
    </Group>
  );
  if (g.state !== "up") return line(gatewayLine(g), g.state === "error");
  const problem = gatewayShapeProblem(g.status);
  if (problem !== null) {
    return line(
      `The gateway at ${g.url} answered a status this page cannot read: ${problem}.`,
      true,
    );
  }
  const s: GatewayStatus = g.status;
  return (
    <Group
      title="Gateway"
      stale={stale}
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

function NodeGroup(props: { node: NodeSection; now: number }) {
  const n = props.node;
  const view = nodeView(n);
  const line =
    view.line === null ? null : (
      <p
        role={view.alert ? "alert" : undefined}
        className={`text-sm ${view.alert ? "text-critical-ink" : "text-ink-3"}`}
      >
        {view.alert ? "⛔ " : ""}
        {view.line}
      </p>
    );
  if (!view.panels) {
    return <Group title="Node">{line}</Group>;
  }
  const slow = (part: { updated_unix_ms: number | null }) =>
    staleSecs(part.updated_unix_ms, props.now, SOURCE_POLL_MS.nodeSlow);
  const id = partValue(n.health)?.node_id ?? partValue(n.status)?.node_id;
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
      {line}
      <NodeHealth
        node={n}
        stale={stalestSecs(
          [n.health, n.status, n.metrics],
          props.now,
          SOURCE_POLL_MS.nodeFast,
        )}
      />
      <NodeEarnings part={n.earnings} stale={slow(n.earnings)} />
      <NodeLanes part={n.lanes} stale={slow(n.lanes)} />
      <NodePools part={n.pools} stale={slow(n.pools)} />
      <NodeSlashes part={n.slashes} stale={slow(n.slashes)} />
    </Group>
  );
}
