"use client";

import { Card, CardStatus } from "@/components/ui/Card";
import { StreamStatusBadge } from "@/components/live/StreamStatusBadge";
import { useEventStream } from "@/lib/useEventStream";
import { STREAM, type FunnelView } from "@/lib/streams";
import { FUNNEL_RAMP } from "@/lib/palette";
import { fmtFull, fmtPct } from "@/lib/format";

interface Stage {
  label: string;
  value: number;
  /** Conversion from the previous stage; null for the entry stage. */
  rate: number | null;
}

function stagesOf(f: FunnelView): Stage[] {
  return [
    { label: "Views", value: f.views, rate: null },
    { label: "Add to cart", value: f.addToCart, rate: f.viewToCartRate },
    { label: "Checkout", value: f.checkoutStart, rate: f.cartToCheckoutRate },
    { label: "Purchase", value: f.purchases, rate: f.checkoutToPurchaseRate },
  ];
}

/**
 * The live conversion funnel — cumulative distinct visitors reaching each stage,
 * pushed by the stream-processor's wall-clock punctuator every ~3s. Bars are plain
 * CSS (not Recharts) so a 3-second cadence animates smoothly and cheaply.
 */
export function LiveFunnelCard() {
  const { last, status, lastEventAt } = useEventStream<FunnelView>(STREAM.funnel);

  return (
    <Card
      title="Live funnel"
      subtitle="Cumulative distinct visitors per stage"
      aside={<StreamStatusBadge status={status} lastEventAt={lastEventAt} />}
      className="min-h-[18rem]"
    >
      {!last ? (
        <CardStatus>Waiting for the first snapshot…</CardStatus>
      ) : (
        <div className="flex flex-1 flex-col justify-between gap-4">
          <div className="flex flex-col gap-3">
            {stagesOf(last).map((stage, i) => {
              const width = last.views > 0 ? (stage.value / last.views) * 100 : 0;
              return (
                <div key={stage.label} className="flex flex-col gap-1">
                  <div className="flex items-baseline justify-between text-xs">
                    <span className="text-dim">{stage.label}</span>
                    <span className="flex items-baseline gap-2">
                      <span className="tnum font-medium text-text">
                        {fmtFull(stage.value)}
                      </span>
                      {stage.rate !== null && (
                        <span className="tnum w-12 text-right text-faint">
                          {fmtPct(stage.rate)}
                        </span>
                      )}
                    </span>
                  </div>
                  <div className="h-2 overflow-hidden rounded-sm bg-surface-2">
                    <div
                      className="h-full rounded-sm transition-[width] duration-500 ease-out"
                      style={{ width: `${width}%`, background: FUNNEL_RAMP[i] }}
                    />
                  </div>
                </div>
              );
            })}
          </div>

          <div className="flex items-baseline justify-between border-t border-border pt-3">
            <span className="text-[10px] font-medium uppercase tracking-wider text-faint">
              Overall conversion
            </span>
            <span className="tnum text-lg font-semibold text-accent">
              {fmtPct(last.overallConversionRate)}
            </span>
          </div>
        </div>
      )}
    </Card>
  );
}
