"use client";

import type { ReactNode } from "react";
import { useEventStream, type StreamStatus } from "@/lib/useEventStream";
import { STREAM, type MetricsView, type ActiveSessionsView } from "@/lib/streams";
import { fmtFull } from "@/lib/format";

/** Dot color per connection state — emerald reserved for a healthy live stream. */
const DOT: Record<StreamStatus, string> = {
  live: "var(--accent)",
  connecting: "var(--text-faint)",
  error: "var(--danger)",
};

function Gauge({
  label,
  value,
  sub,
  status,
  accent = "var(--series-1)",
}: {
  label: string;
  value: ReactNode;
  sub: string;
  status: StreamStatus;
  accent?: string;
}) {
  const has = value !== null && value !== undefined;
  return (
    <div className="relative flex flex-col overflow-hidden rounded-lg border border-border bg-surface px-4 pb-4 pt-3">
      <div className="flex items-center justify-between">
        <span className="text-[10px] font-medium uppercase tracking-wider text-faint">{label}</span>
        <span
          className={`h-1.5 w-1.5 rounded-full ${status === "live" ? "live-dot" : ""}`}
          style={{ background: DOT[status] }}
        />
      </div>
      <div className="mt-3 flex items-baseline gap-1.5">
        <span className="tnum text-[1.75rem] font-semibold leading-none text-text">
          {has ? value : <span className="text-faint">—</span>}
        </span>
        <span className="text-xs text-dim">{sub}</span>
      </div>
      <span
        className="absolute inset-x-0 bottom-0 h-0.5"
        style={{ background: accent, opacity: has ? 0.9 : 0.25 }}
      />
    </div>
  );
}

/**
 * The live "overview" band: at-a-glance gauges above the detailed cards. Active sessions is its
 * own ~3s SSE stream; events/sec and conversions/min are derived from the per-minute metrics
 * rollup (window is exactly one minute, so events/60 is the mean rate and purchases is the
 * per-minute conversion count).
 */
export function LiveStatStrip() {
  const metrics = useEventStream<MetricsView>(STREAM.metrics);
  const sessions = useEventStream<ActiveSessionsView>(STREAM.activeSessions);

  const m = metrics.last;
  const s = sessions.last;
  const activeWindowMin = s ? Math.round(s.windowSeconds / 60) : null;

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
      <Gauge
        label="Events / sec"
        value={m ? fmtFull(Math.round(m.events / 60)) : null}
        sub="1-min avg"
        status={metrics.status}
      />
      <Gauge
        label="Active sessions"
        value={s ? fmtFull(s.activeSessions) : null}
        sub={activeWindowMin ? `last ${activeWindowMin} min` : "live"}
        status={sessions.status}
        accent="var(--series-4)"
      />
      <Gauge
        label="Conversions / min"
        value={m ? fmtFull(m.purchases) : null}
        sub="purchases"
        status={metrics.status}
        accent="var(--accent)"
      />
    </div>
  );
}
