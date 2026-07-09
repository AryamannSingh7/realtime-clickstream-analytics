"use client";

import { Card, CardStatus } from "@/components/ui/Card";
import { StreamStatusBadge } from "@/components/live/StreamStatusBadge";
import { useEventStream } from "@/lib/useEventStream";
import { STREAM, type TopNView } from "@/lib/streams";
import { CHART } from "@/lib/palette";
import { fmtClock, fmtFull } from "@/lib/format";

/**
 * Windowed top-N pages, recomputed per 1-minute window by the Processor-API
 * topology. Rows are ranked views-desc; the bar is scaled to the leader so the
 * shape of the distribution reads at a glance.
 */
export function LiveTopPagesCard({ className }: { className?: string }) {
  const { last, status, lastEventAt } = useEventStream<TopNView>(STREAM.topn);
  const leader = last?.pages[0]?.views ?? 0;

  return (
    <Card
      title="Top pages"
      subtitle={
        last
          ? `Window closing ${fmtClock(new Date(last.windowEnd).getTime())}`
          : "Ranked by views in the current window"
      }
      aside={<StreamStatusBadge status={status} lastEventAt={lastEventAt} />}
      className={`min-h-[18rem] ${className ?? ""}`}
    >
      {!last ? (
        <CardStatus>Waiting for the next 1-minute window…</CardStatus>
      ) : last.pages.length === 0 ? (
        <CardStatus>No page views in this window</CardStatus>
      ) : (
        <ol className="flex flex-col gap-2.5">
          {last.pages.map((page, i) => (
            <li key={page.path} className="flex flex-col gap-1">
              <div className="flex items-baseline justify-between gap-3 text-xs">
                <span className="flex min-w-0 items-baseline gap-2">
                  <span className="tnum w-4 shrink-0 text-right text-faint">
                    {i + 1}
                  </span>
                  <span className="truncate font-mono text-text">{page.path}</span>
                </span>
                <span className="tnum shrink-0 font-medium text-dim">
                  {fmtFull(page.views)}
                </span>
              </div>
              <div className="ml-6 h-1 overflow-hidden rounded-sm bg-surface-2">
                <div
                  className="h-full rounded-sm transition-[width] duration-500 ease-out"
                  style={{
                    width: leader > 0 ? `${(page.views / leader) * 100}%` : "0%",
                    background: CHART.primary,
                  }}
                />
              </div>
            </li>
          ))}
        </ol>
      )}
    </Card>
  );
}
