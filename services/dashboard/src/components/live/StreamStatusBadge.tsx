"use client";

import type { StreamStatus } from "@/lib/useEventStream";
import { fmtClock } from "@/lib/format";

const TONE: Record<StreamStatus, { color: string; label: string; pulse: boolean }> = {
  live: { color: "var(--accent)", label: "Live", pulse: true },
  connecting: { color: "var(--text-faint)", label: "Connecting", pulse: false },
  error: { color: "var(--danger)", label: "Reconnecting", pulse: false },
};

/**
 * Connection LED for a live card header. Emerald is reserved for healthy status
 * (never a data hue), so a glancing look at the console reads stream health first.
 */
export function StreamStatusBadge({
  status,
  lastEventAt,
}: {
  status: StreamStatus;
  lastEventAt?: number | null;
}) {
  const tone = TONE[status];
  return (
    <div className="flex items-center gap-1.5">
      <span
        className={`h-1.5 w-1.5 rounded-full ${tone.pulse ? "live-dot" : ""}`}
        style={{ background: tone.color }}
      />
      <span className="text-[10px] font-medium uppercase tracking-wider text-faint">
        {status === "live" && lastEventAt ? fmtClock(lastEventAt) : tone.label}
      </span>
    </div>
  );
}
