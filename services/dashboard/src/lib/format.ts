const compact = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});
const full = new Intl.NumberFormat("en-US");

/** 1234 -> "1.2K", 3366365 -> "3.4M" */
export function fmtCompact(n: number): string {
  return compact.format(n);
}

/** 1234567 -> "1,234,567" */
export function fmtFull(n: number): string {
  return full.format(n);
}

/** 0.1234 -> "12.3%" */
export function fmtPct(fraction: number, digits = 1): string {
  return `${(fraction * 100).toFixed(digits)}%`;
}

/** Short clock time for "updated 14:03:22". */
export function fmtClock(ms: number): string {
  return new Date(ms).toLocaleTimeString("en-US", { hour12: false });
}

/** Axis tick for a time bucket, granularity-aware. */
export function fmtBucket(iso: string, byDay: boolean): string {
  const d = new Date(iso);
  return byDay
    ? d.toLocaleDateString("en-US", { month: "short", day: "numeric" })
    : d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false });
}
