import type { ReactNode } from "react";

interface KpiProps {
  label: string;
  value: ReactNode;
  /** Small caption under the value (e.g. "of views"). */
  sub?: string;
  /** Accent bar color (defaults to primary blue). */
  accent?: string;
  loading?: boolean;
}

/**
 * The signature element: an instrument-style readout. The figure is the hero —
 * large tabular-mono numerals — with a tiny uppercase label, sitting above a
 * thin accent baseline like a gauge.
 */
export function Kpi({ label, value, sub, accent = "var(--series-1)", loading }: KpiProps) {
  return (
    <div className="relative flex flex-col justify-between overflow-hidden rounded-lg border border-border bg-surface px-4 pb-4 pt-3">
      <span className="text-[10px] font-medium uppercase tracking-wider text-faint">
        {label}
      </span>
      <div className="mt-3">
        <span className="tnum text-2xl font-semibold leading-none text-text sm:text-[1.75rem]">
          {loading ? <span className="text-faint">—</span> : value}
        </span>
        {sub && <span className="ml-1.5 text-xs text-dim">{sub}</span>}
      </div>
      <span
        className="absolute inset-x-0 bottom-0 h-0.5"
        style={{ background: accent, opacity: loading ? 0.25 : 0.9 }}
      />
    </div>
  );
}
