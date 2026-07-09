"use client";

import { Area, AreaChart, ResponsiveContainer, Tooltip, YAxis } from "recharts";
import { Card, CardStatus } from "@/components/ui/Card";
import { StreamStatusBadge } from "@/components/live/StreamStatusBadge";
import { TooltipBox } from "@/components/charts/tooltip";
import { useEventStream } from "@/lib/useEventStream";
import { STREAM, type MetricsView } from "@/lib/streams";
import { CHART } from "@/lib/palette";
import { fmtClock, fmtCompact, fmtFull } from "@/lib/format";

/** Half an hour of one-minute windows. */
const HISTORY_MINUTES = 30;

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] font-medium uppercase tracking-wider text-faint">
        {label}
      </span>
      <span className="tnum text-sm font-medium text-text">{value}</span>
    </div>
  );
}

/**
 * The per-minute rollup ticker. Each tumbling window is emitted once its grace
 * period closes, so the first record can take up to a minute to arrive — the empty
 * state says so rather than implying the stream is broken.
 */
export function LiveMetricsCard({ className }: { className?: string }) {
  const { last, history, status, lastEventAt } = useEventStream<MetricsView>(
    STREAM.metrics,
    { historyLimit: HISTORY_MINUTES },
  );

  return (
    <Card
      title="Events per minute"
      subtitle="Tumbling 1-minute rollups from Kafka Streams"
      aside={<StreamStatusBadge status={status} lastEventAt={lastEventAt} />}
      className={`min-h-[18rem] ${className ?? ""}`}
    >
      {!last ? (
        <CardStatus>Waiting for the next 1-minute window…</CardStatus>
      ) : (
        <div className="flex flex-1 flex-col justify-between gap-4">
          <div className="flex items-end justify-between gap-4">
            <div>
              <div className="tnum text-4xl font-semibold leading-none text-text">
                {fmtFull(last.events)}
              </div>
              <div className="mt-1.5 text-xs text-dim">
                window closed {fmtClock(new Date(last.windowEnd).getTime())}
              </div>
            </div>
            <div className="grid shrink-0 grid-cols-2 gap-x-6 gap-y-2 sm:grid-cols-4">
              <Stat label="Page views" value={fmtCompact(last.pageViews)} />
              <Stat label="Add to cart" value={fmtCompact(last.addToCart)} />
              <Stat label="Purchases" value={fmtCompact(last.purchases)} />
              <Stat label="Revenue" value={`$${fmtCompact(last.revenue)}`} />
            </div>
          </div>

          {/* A single window plots as a lone floating dot, which reads as a glitch.
              Hold the space until there's an actual trend to draw. */}
          {history.length < 2 ? (
            <div className="flex h-[120px] items-center justify-center text-xs text-faint">
              Collecting minutes…
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={120}>
              <AreaChart data={history} margin={{ top: 4, right: 0, bottom: 0, left: 0 }}>
                <defs>
                  <linearGradient id="live-metrics-fill" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={CHART.primary} stopOpacity={0.35} />
                    <stop offset="100%" stopColor={CHART.primary} stopOpacity={0.02} />
                  </linearGradient>
                </defs>
                {/* Domain from the data, not zero-based: this is a deviation trace, and
                    a fixed 0 floor would flatten normal minute-to-minute variation. */}
                <YAxis hide domain={["dataMin", "dataMax"]} />
                <Tooltip
                  cursor={{ stroke: CHART.axis, strokeWidth: 1 }}
                  content={({ active, payload }) => {
                    if (!active || !payload?.length) return null;
                    const row = payload[0].payload as MetricsView;
                    return (
                      <TooltipBox
                        title={fmtClock(new Date(row.windowEnd).getTime())}
                        rows={[
                          {
                            label: "Events",
                            value: fmtFull(row.events),
                            color: CHART.primary,
                          },
                          { label: "Purchases", value: fmtFull(row.purchases) },
                        ]}
                      />
                    );
                  }}
                />
                <Area
                  type="monotone"
                  dataKey="events"
                  stroke={CHART.primary}
                  strokeWidth={2}
                  fill="url(#live-metrics-fill)"
                  dot={false}
                  activeDot={{ r: 4, strokeWidth: 0 }}
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          )}
          <p className="text-[11px] text-faint">
            Last {history.length} of {HISTORY_MINUTES} minutes
          </p>
        </div>
      )}
    </Card>
  );
}
