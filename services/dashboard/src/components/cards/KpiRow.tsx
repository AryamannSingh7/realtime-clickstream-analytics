"use client";

import { useCallback } from "react";
import { Kpi } from "@/components/ui/Kpi";
import { useRange } from "@/components/RangeContext";
import { usePolling } from "@/lib/usePolling";
import { fetchFunnel, type FunnelResponse, type FunnelStage } from "@/lib/olap";
import { CHART } from "@/lib/palette";
import { fmtFull, fmtPct } from "@/lib/format";

function stage(data: FunnelResponse | null, s: FunnelStage) {
  return data?.steps.find((x) => x.stage === s);
}

export function KpiRow() {
  const { range } = useRange();
  const fetcher = useCallback(
    (signal: AbortSignal) => fetchFunnel(range, undefined, signal),
    [range],
  );
  const { data, loading } = usePolling<FunnelResponse>(fetcher, [range]);

  const views = stage(data, "view");
  const cart = stage(data, "cart");
  const purchase = stage(data, "purchase");

  return (
    <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
      <Kpi
        label="Product views"
        value={views ? fmtFull(views.visitors) : "—"}
        sub="visitors"
        loading={loading}
      />
      <Kpi
        label="Add to cart"
        value={cart ? fmtFull(cart.visitors) : "—"}
        sub={cart ? fmtPct(cart.conversionFromTop) : undefined}
        loading={loading}
      />
      <Kpi
        label="Purchases"
        value={purchase ? fmtFull(purchase.visitors) : "—"}
        sub="converted"
        accent={CHART.good}
        loading={loading}
      />
      <Kpi
        label="Conversion rate"
        value={purchase ? fmtPct(purchase.conversionFromTop) : "—"}
        sub="view → purchase"
        accent={CHART.good}
        loading={loading}
      />
    </div>
  );
}
