// Wire types for the live SSE streams, mirroring the analytics-api `StreamDtos`
// records (com.clickstream.api.stream). Instants serialize as ISO-8601 strings.
//
// Each stream key doubles as the `/api/stream/{key}` path segment AND the SSE event
// name the server sends (`event: funnel`), so a client must listen for that named
// event rather than the default `message`.

export const STREAM = {
  metrics: "metrics",
  topn: "topn",
  funnel: "funnel",
  alerts: "alerts",
} as const;

export type StreamKey = (typeof STREAM)[keyof typeof STREAM];

/** Per-minute rollup — `analytics.metrics.1m`. Arrives once a minute. */
export interface MetricsView {
  windowStart: string;
  windowEnd: string;
  events: number;
  pageViews: number;
  addToCart: number;
  checkoutStart: number;
  purchases: number;
  revenue: number;
}

export interface PageView {
  path: string;
  views: number;
}

/** Windowed top-N pages — `analytics.topn.pages.1m`. Arrives once a minute. */
export interface TopNView {
  windowStart: string;
  windowEnd: string;
  pages: PageView[];
}

/** Live conversion-funnel snapshot — `analytics.funnel.live`. Arrives every ~3s. */
export interface FunnelView {
  snapshotTime: string;
  views: number;
  addToCart: number;
  checkoutStart: number;
  purchases: number;
  viewToCartRate: number;
  cartToCheckoutRate: number;
  checkoutToPurchaseRate: number;
  overallConversionRate: number;
}

/** Volume anomaly alert — `analytics.alerts`. Arrives only when the detector fires. */
export interface AlertView {
  alertTime: string;
  observed: number;
  baselineMean: number;
  baselineStddev: number;
  zScore: number;
  thresholdK: number;
  direction: "spike" | "drop";
  baselineSamples: number;
}
