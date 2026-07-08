"use client";

import { useCallback, useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Card, CardStatus } from "@/components/ui/Card";
import { TooltipBox } from "@/components/charts/tooltip";
import { useRange } from "@/components/RangeContext";
import { usePolling } from "@/lib/usePolling";
import { fetchTimeseries, type TimeseriesPoint } from "@/lib/olap";
import { CHART, SERIES } from "@/lib/palette";
import { fmtBucket, fmtCompact, fmtFull } from "@/lib/format";

const ALL = "__all__";

function prettyType(t: string): string {
  const s = t.replace(/_/g, " ");
  return s.charAt(0).toUpperCase() + s.slice(1);
}

export function TimeseriesCard() {
  const { range, preset } = useRange();
  const byDay = preset.byDay;
  const [selected, setSelected] = useState<string>(ALL);

  // Fetch every event type once; pivot locally for both "All" and a single type.
  const fetcher = useCallback(
    (signal: AbortSignal) => fetchTimeseries(range, preset.interval, undefined, signal),
    [range, preset.interval],
  );
  const { data, error, loading } = usePolling<TimeseriesPoint[]>(fetcher, [
    range,
    preset.interval,
  ]);

  const types = useMemo(() => {
    const set = new Set<string>();
    (data ?? []).forEach((p) => set.add(p.eventType));
    return [...set].sort();
  }, [data]);

  const series = useMemo(() => {
    const byBucket = new Map<string, number>();
    for (const p of data ?? []) {
      if (selected !== ALL && p.eventType !== selected) continue;
      byBucket.set(p.bucket, (byBucket.get(p.bucket) ?? 0) + p.events);
    }
    return [...byBucket.entries()]
      .map(([bucket, value]) => ({ bucket, value }))
      .sort((a, b) => a.bucket.localeCompare(b.bucket));
  }, [data, selected]);

  const color =
    selected === ALL
      ? CHART.primary
      : SERIES[types.indexOf(selected) % SERIES.length] ?? CHART.primary;

  const selector = (
    <select
      value={selected}
      onChange={(e) => setSelected(e.target.value)}
      className="rounded border border-border bg-surface-2 px-2 py-1 text-xs text-dim outline-none focus:border-border-strong"
      aria-label="Event type"
    >
      <option value={ALL}>All events</option>
      {types.map((t) => (
        <option key={t} value={t}>
          {prettyType(t)}
        </option>
      ))}
    </select>
  );

  return (
    <Card
      title="Event volume"
      subtitle={`Events per ${preset.bucketLabel}`}
      aside={data ? selector : undefined}
      className="min-h-[18rem]"
    >
      {loading ? (
        <CardStatus>Loading…</CardStatus>
      ) : error ? (
        <CardStatus tone="danger">Failed to load — {error}</CardStatus>
      ) : series.length === 0 ? (
        <CardStatus>No data in range</CardStatus>
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <AreaChart data={series} margin={{ top: 8, right: 12, bottom: 4, left: 4 }}>
            <defs>
              <linearGradient id="ts-fill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={color} stopOpacity={0.32} />
                <stop offset="100%" stopColor={color} stopOpacity={0.02} />
              </linearGradient>
            </defs>
            <CartesianGrid stroke={CHART.grid} vertical={false} />
            <XAxis
              dataKey="bucket"
              tickFormatter={(v: string) => fmtBucket(v, byDay)}
              tickLine={false}
              axisLine={{ stroke: CHART.axis }}
              tick={{ fill: "var(--color-faint)", fontSize: 11 }}
              minTickGap={28}
            />
            <YAxis
              tickFormatter={(v: number) => fmtCompact(v)}
              tickLine={false}
              axisLine={false}
              width={40}
              tick={{ fill: "var(--color-faint)", fontSize: 11 }}
            />
            <Tooltip
              cursor={{ stroke: CHART.axis, strokeWidth: 1 }}
              content={({ active, payload, label }) => {
                if (!active || !payload?.length) return null;
                return (
                  <TooltipBox
                    title={fmtBucket(String(label), byDay)}
                    rows={[
                      {
                        label: selected === ALL ? "All events" : prettyType(selected),
                        value: fmtFull(payload[0].value as number),
                        color,
                      },
                    ]}
                  />
                );
              }}
            />
            <Area
              type="monotone"
              dataKey="value"
              stroke={color}
              strokeWidth={2}
              fill="url(#ts-fill)"
              dot={false}
              activeDot={{ r: 4, strokeWidth: 0 }}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      )}
    </Card>
  );
}
