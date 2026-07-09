"use client";

import { Card, CardStatus } from "@/components/ui/Card";
import { StreamStatusBadge } from "@/components/live/StreamStatusBadge";
import { useEventStream } from "@/lib/useEventStream";
import { STREAM, type AlertView } from "@/lib/streams";
import { CHART } from "@/lib/palette";
import { fmtClock, fmtFull } from "@/lib/format";

const FEED_LIMIT = 20;

const DIRECTION: Record<AlertView["direction"], { color: string; label: string }> = {
  spike: { color: CHART.amber, label: "Volume spike" },
  drop: { color: CHART.danger, label: "Volume drop" },
};

function AlertRow({ alert }: { alert: AlertView }) {
  const tone = DIRECTION[alert.direction] ?? DIRECTION.spike;
  return (
    <li className="flex items-start gap-3 border-l-2 py-2 pl-3" style={{ borderColor: tone.color }}>
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline justify-between gap-2">
          <span className="text-xs font-medium" style={{ color: tone.color }}>
            {tone.label}
          </span>
          <span className="tnum shrink-0 text-[11px] text-faint">
            {fmtClock(new Date(alert.alertTime).getTime())}
          </span>
        </div>
        <p className="mt-0.5 text-xs text-dim">
          <span className="tnum text-text">{fmtFull(alert.observed)}</span> events vs
          baseline <span className="tnum">{fmtFull(Math.round(alert.baselineMean))}</span>
          {" · "}
          <span className="tnum">z={alert.zScore.toFixed(2)}</span>
        </p>
      </div>
    </li>
  );
}

/**
 * Anomaly feed off the EWMA volume detector. The SSE consumer starts at the latest
 * offset, so alerts fired before this page loaded are not replayed — an empty feed
 * means "nothing since you opened this", which is the normal steady state.
 */
export function AlertsFeedCard({ className }: { className?: string }) {
  const { history, status, lastEventAt } = useEventStream<AlertView>(STREAM.alerts, {
    historyLimit: FEED_LIMIT,
  });
  const newestFirst = [...history].reverse();

  return (
    <Card
      title="Anomaly alerts"
      subtitle="EWMA volume detector · |z| ≥ 3 over a 1-minute bucket"
      aside={<StreamStatusBadge status={status} lastEventAt={lastEventAt} />}
      className={`min-h-[18rem] ${className ?? ""}`}
    >
      {newestFirst.length === 0 ? (
        <CardStatus>No anomalies since this page loaded</CardStatus>
      ) : (
        <ol className="flex max-h-64 flex-col gap-1 overflow-y-auto">
          {newestFirst.map((alert) => (
            <AlertRow key={`${alert.alertTime}-${alert.observed}`} alert={alert} />
          ))}
        </ol>
      )}
    </Card>
  );
}
