import { useCallback, useEffect, useState } from "react";
import { fetchStatus, type Fetched, type Status } from "./api";
import { ActivePulls } from "./components/ActivePulls";
import { Allowance } from "./components/Allowance";
import { Capabilities } from "./components/Capabilities";
import { Header } from "./components/Header";
import { Pointers } from "./components/Pointers";
import { Pools } from "./components/Pools";
import { StatTiles } from "./components/StatTiles";
import { TokenForm } from "./components/TokenForm";
import { adoptToken, clearToken, saveToken } from "./token";
import { buckets, capabilities, pools } from "./view";

const POLL_MS = 2_000;

function initialToken(): string | null {
  try {
    return adoptToken(window.location, window.history, window.sessionStorage);
  } catch {
    return null;
  }
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
  const [token, setToken] = useState<string | null>(initialToken);
  const { last, status, live } = usePolledStatus(token);

  const acceptToken = useCallback((given: string) => {
    saveToken(window.sessionStorage, given);
    setToken(given);
  }, []);
  const forget = useCallback(() => {
    clearToken(window.sessionStorage);
    setToken(null);
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
      {last?.kind === "error" ? (
        <p role="alert" className="text-sm text-critical-ink">
          ⛔ The gateway did not answer: {last.message}
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
