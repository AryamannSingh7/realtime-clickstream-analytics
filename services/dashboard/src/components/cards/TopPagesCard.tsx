"use client";

import { useCallback } from "react";
import {
  Bar,
  BarChart,
  LabelList,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Card, CardStatus } from "@/components/ui/Card";
import { TooltipBox } from "@/components/charts/tooltip";
import { useRange } from "@/components/RangeContext";
import { usePolling } from "@/lib/usePolling";
import { fetchTopPages, type TopPage } from "@/lib/olap";
import { CHART } from "@/lib/palette";
import { fmtCompact, fmtFull } from "@/lib/format";

export function TopPagesCard() {
  const { range } = useRange();
  const fetcher = useCallback(
    (signal: AbortSignal) => fetchTopPages(range, 8, signal),
    [range],
  );
  const { data, error, loading } = usePolling<TopPage[]>(fetcher, [range]);
  const rows = data ?? [];

  return (
    <Card title="Top pages" subtitle="Most-viewed paths" className="min-h-[20rem]">
      {loading ? (
        <CardStatus>Loading…</CardStatus>
      ) : error ? (
        <CardStatus tone="danger">Failed to load — {error}</CardStatus>
      ) : rows.length === 0 ? (
        <CardStatus>No data in range</CardStatus>
      ) : (
        <ResponsiveContainer width="100%" height={240}>
          <BarChart
            layout="vertical"
            data={rows}
            margin={{ top: 4, right: 52, bottom: 4, left: 8 }}
            barCategoryGap="24%"
          >
            <XAxis type="number" hide domain={[0, "dataMax"]} />
            <YAxis
              type="category"
              dataKey="path"
              width={140}
              tickLine={false}
              axisLine={false}
              tick={{ fill: "var(--color-dim)", fontSize: 11 }}
            />
            <Tooltip
              cursor={{ fill: "rgba(255,255,255,0.04)" }}
              content={({ active, payload }) => {
                if (!active || !payload?.length) return null;
                const r = payload[0].payload as TopPage;
                return (
                  <TooltipBox
                    title={r.path}
                    rows={[{ label: "Views", value: fmtFull(r.views), color: CHART.primary }]}
                  />
                );
              }}
            />
            <Bar
              dataKey="views"
              fill={CHART.primary}
              radius={[0, 4, 4, 0]}
              isAnimationActive={false}
            >
              <LabelList
                dataKey="views"
                position="right"
                offset={8}
                fill="var(--color-dim)"
                fontSize={12}
                formatter={(v) => fmtCompact(Number(v))}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </Card>
  );
}
