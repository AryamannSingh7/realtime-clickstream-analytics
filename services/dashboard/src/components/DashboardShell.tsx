"use client";

import { RangeProvider, useRange } from "@/components/RangeContext";
import { KpiRow } from "@/components/cards/KpiRow";
import { FunnelCard } from "@/components/cards/FunnelCard";
import { TopPagesCard } from "@/components/cards/TopPagesCard";
import { UniqueVisitorsCard } from "@/components/cards/UniqueVisitorsCard";
import { TimeseriesCard } from "@/components/cards/TimeseriesCard";
import { LiveMetricsCard } from "@/components/cards/LiveMetricsCard";
import { LiveFunnelCard } from "@/components/cards/LiveFunnelCard";
import { LiveTopPagesCard } from "@/components/cards/LiveTopPagesCard";
import { AlertsFeedCard } from "@/components/cards/AlertsFeedCard";
import { RANGE_PRESETS } from "@/lib/range";

function RangeSelector() {
  const { key, setKey } = useRange();
  return (
    <div
      className="inline-flex rounded-md border border-border bg-surface p-0.5"
      role="group"
      aria-label="Time range"
    >
      {RANGE_PRESETS.map((p) => {
        const active = p.key === key;
        return (
          <button
            key={p.key}
            onClick={() => setKey(p.key)}
            aria-pressed={active}
            className={`rounded px-3 py-1 text-xs font-medium transition-colors ${
              active
                ? "bg-surface-2 text-text"
                : "text-dim hover:text-text"
            }`}
          >
            {p.label}
          </button>
        );
      })}
    </div>
  );
}

/** Push-based widgets fed by SSE. Deliberately outside RangeProvider — these
 *  always show "now" and ignore the historical range selector. */
function LiveSection() {
  return (
    <section className="flex flex-col gap-5">
      <div>
        <h2 className="text-lg font-semibold tracking-tight">Live</h2>
        <p className="mt-0.5 text-sm text-dim">
          Streaming rollups pushed from Kafka Streams over SSE
        </p>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <LiveMetricsCard className="lg:col-span-2" />
        <LiveFunnelCard />
        <LiveTopPagesCard />
        <AlertsFeedCard className="lg:col-span-2" />
      </div>
    </section>
  );
}

function Overview() {
  return (
    <section className="flex flex-col gap-5">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold tracking-tight">Overview</h2>
          <p className="mt-0.5 text-sm text-dim">Historical rollups from ClickHouse</p>
        </div>
        <RangeSelector />
      </div>

      <KpiRow />

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <FunnelCard />
        <TopPagesCard />
        <UniqueVisitorsCard />
        <TimeseriesCard />
      </div>
    </section>
  );
}

export function DashboardShell() {
  return (
    <div className="flex flex-col gap-10">
      <LiveSection />
      <RangeProvider>
        <Overview />
      </RangeProvider>
    </div>
  );
}
