"use client";

import { useCallback } from "react";
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
import { fetchUniqueVisitors, type UniqueVisitorsPoint } from "@/lib/olap";
import { CHART } from "@/lib/palette";
import { fmtBucket, fmtCompact, fmtFull } from "@/lib/format";

export function UniqueVisitorsCard() {
  const { range, preset } = useRange();
  const byDay = preset.byDay;
  const fetcher = useCallback(
    (signal: AbortSignal) => fetchUniqueVisitors(range, preset.interval, signal),
    [range, preset.interval],
  );
  const { data, error, loading } = usePolling<UniqueVisitorsPoint[]>(fetcher, [
    range,
    preset.interval,
  ]);
  const rows = data ?? [];

  return (
    <Card
      title="Unique visitors"
      subtitle={`Distinct anonymous IDs per ${preset.bucketLabel}`}
      className="min-h-[18rem]"
    >
      {loading ? (
        <CardStatus>Loading…</CardStatus>
      ) : error ? (
        <CardStatus tone="danger">Failed to load — {error}</CardStatus>
      ) : rows.length === 0 ? (
        <CardStatus>No data in range</CardStatus>
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <AreaChart data={rows} margin={{ top: 8, right: 12, bottom: 4, left: 4 }}>
            <defs>
              <linearGradient id="uv-fill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={CHART.primary} stopOpacity={0.35} />
                <stop offset="100%" stopColor={CHART.primary} stopOpacity={0.02} />
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
                        label: "Unique visitors",
                        value: fmtFull(payload[0].value as number),
                        color: CHART.primary,
                      },
                    ]}
                  />
                );
              }}
            />
            <Area
              type="monotone"
              dataKey="uniqueVisitors"
              stroke={CHART.primary}
              strokeWidth={2}
              fill="url(#uv-fill)"
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
