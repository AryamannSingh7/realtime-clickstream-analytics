"use client";

import { useCallback } from "react";
import {
  Bar,
  BarChart,
  Cell,
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
import { fetchFunnel, type FunnelResponse, type FunnelStep } from "@/lib/olap";
import { FUNNEL_RAMP } from "@/lib/palette";
import { fmtCompact, fmtFull, fmtPct } from "@/lib/format";

const STAGE_LABEL: Record<FunnelStep["stage"], string> = {
  view: "Product view",
  cart: "Add to cart",
  checkout: "Checkout",
  purchase: "Purchase",
};

interface Row extends FunnelStep {
  label: string;
}

export function FunnelCard() {
  const { range } = useRange();
  const fetcher = useCallback(
    (signal: AbortSignal) => fetchFunnel(range, undefined, signal),
    [range],
  );
  const { data, error, loading } = usePolling<FunnelResponse>(fetcher, [range]);

  const rows: Row[] =
    data?.steps.map((s) => ({ ...s, label: STAGE_LABEL[s.stage] })) ?? [];

  return (
    <Card
      title="Conversion funnel"
      subtitle="Distinct visitors reaching each stage"
      className="min-h-[20rem]"
    >
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
            margin={{ top: 4, right: 88, bottom: 4, left: 8 }}
            barCategoryGap="28%"
          >
            <XAxis type="number" hide domain={[0, "dataMax"]} />
            <YAxis
              type="category"
              dataKey="label"
              width={104}
              tickLine={false}
              axisLine={false}
              tick={{ fill: "var(--color-dim)", fontSize: 12 }}
            />
            <Tooltip
              cursor={{ fill: "rgba(255,255,255,0.04)" }}
              content={({ active, payload }) => {
                if (!active || !payload?.length) return null;
                const r = payload[0].payload as Row;
                return (
                  <TooltipBox
                    title={r.label}
                    rows={[
                      { label: "Visitors", value: fmtFull(r.visitors) },
                      { label: "From top", value: fmtPct(r.conversionFromTop) },
                      { label: "From previous", value: fmtPct(r.conversionFromPrev) },
                    ]}
                  />
                );
              }}
            />
            <Bar dataKey="visitors" radius={[0, 4, 4, 0]} isAnimationActive={false}>
              {rows.map((_, i) => (
                <Cell key={i} fill={FUNNEL_RAMP[i] ?? FUNNEL_RAMP[FUNNEL_RAMP.length - 1]} />
              ))}
              {/* Direct labels at the bar end: compact count + from-top %. */}
              <LabelList
                dataKey="visitors"
                content={(props) => {
                  const { x, y, width, height, index } = props as {
                    x: number;
                    y: number;
                    width: number;
                    height: number;
                    index: number;
                  };
                  const r = rows[index];
                  if (r == null) return null;
                  return (
                    <text
                      x={x + width + 8}
                      y={y + height / 2}
                      dominantBaseline="central"
                      fontSize={12}
                    >
                      <tspan fill="var(--color-text)" fontWeight={600}>
                        {fmtCompact(r.visitors)}
                      </tspan>
                      <tspan fill="var(--color-faint)" dx={6}>
                        {fmtPct(r.conversionFromTop, 0)}
                      </tspan>
                    </text>
                  );
                }}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </Card>
  );
}
