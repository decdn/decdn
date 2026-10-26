const BYTE_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

/** Bytes in binary units: `1023 B`, `1.5 KiB`, `20.0 GiB`. */
export function formatBytes(bytes: number): string {
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < BYTE_UNITS.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return unit === 0 ? `${bytes} B` : `${value.toFixed(1)} ${BYTE_UNITS[unit]}`;
}

/** Micro-USDC as USDC: at least two decimals, and every non-zero micro digit. */
export function formatUsdc(micro: number): string {
  const whole = Math.floor(micro / 1_000_000);
  const fraction = String(micro % 1_000_000)
    .padStart(6, "0")
    .replace(/0+$/, "")
    .padEnd(2, "0");
  return `${whole.toLocaleString("en-US")}.${fraction}`;
}

/** Seconds as their two largest units: `45s`, `3m 12s`, `2h 05m`, `3d 4h`. */
export function formatDuration(secs: number): string {
  const s = Math.max(0, Math.floor(secs));
  if (s < 60) return `${s}s`;
  if (s < 3_600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86_400) {
    return `${Math.floor(s / 3_600)}h ${String(Math.floor((s % 3_600) / 60)).padStart(2, "0")}m`;
  }
  return `${Math.floor(s / 86_400)}d ${Math.floor((s % 86_400) / 3_600)}h`;
}

const COMPACT = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

/** A count for a tile: grouped below ten thousand, compact above. */
export function formatCount(n: number): string {
  return n < 10_000 ? n.toLocaleString("en-US") : COMPACT.format(n);
}

/** A long hex string as its head and tail: `0x1234…abcd`. */
export function shortHex(hex: string): string {
  return hex.length <= 14 ? hex : `${hex.slice(0, 6)}…${hex.slice(-4)}`;
}
