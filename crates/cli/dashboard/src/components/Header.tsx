import type { Status } from "../api";
import { formatDuration, shortHex } from "../format";

export function Header(props: {
  status: Status | null;
  live: boolean;
  onForget?: () => void;
}) {
  const s = props.status;
  return (
    <header className="flex flex-wrap items-end justify-between gap-3">
      <div>
        <h1 className="text-xl font-semibold">
          decdn gateway
          {s ? (
            <span className="ml-2 rounded border border-line px-1.5 py-0.5 align-middle text-xs font-medium text-ink-2">
              {s.mode}
            </span>
          ) : null}
        </h1>
        {s ? (
          <p className="mt-1 text-sm text-ink-2">
            v{s.version} · {s.listen} · chain {s.chain_id} · pool contract{" "}
            <span className="font-mono text-xs" title={s.payment_pool}>
              {shortHex(s.payment_pool)}
            </span>{" "}
            · up {formatDuration(s.uptime_secs)}
          </p>
        ) : null}
      </div>
      <div className="flex items-center gap-3 text-sm text-ink-2">
        <span>
          <span
            aria-hidden="true"
            className={props.live ? "text-good" : "text-ink-3"}
          >
            ●
          </span>{" "}
          {props.live ? "Live, every 2 s" : "Not connected"}
        </span>
        {props.onForget ? (
          <button
            type="button"
            onClick={props.onForget}
            className="rounded border border-line px-2 py-1 hover:bg-page"
          >
            Forget token
          </button>
        ) : null}
      </div>
    </header>
  );
}
