// Typed client for the analytics-api OLAP endpoints. All calls go same-origin to
// /api/olap/* and are proxied server-side to the analytics-api (see next.config).
// Response shapes mirror the Spring `OlapDtos` records.

export type FunnelStage = "view" | "cart" | "checkout" | "purchase";

export interface FunnelStep {
  stage: FunnelStage;
  visitors: number;
  conversionFromTop: number; // 0..1 vs. the first stage
  conversionFromPrev: number; // 0..1 vs. the previous stage
}

export interface FunnelResponse {
  from: string;
  to: string;
  windowSeconds: number;
  steps: FunnelStep[];
}

export interface TopPage {
  path: string;
  views: number;
}

export interface UniqueVisitorsPoint {
  bucket: string; // ISO instant, start of bucket
  uniqueVisitors: number;
}

export interface TimeseriesPoint {
  bucket: string;
  eventType: string;
  events: number;
}

/** Bucket granularity accepted by the time-series / unique-users endpoints. */
export type Interval = "minute" | "5m" | "hour" | "day";

export interface TimeRange {
  from?: string; // ISO instant; omitted => API default (last 24h)
  to?: string;
}

function qs(params: Record<string, string | number | undefined>): string {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== "") sp.set(k, String(v));
  }
  const s = sp.toString();
  return s ? `?${s}` : "";
}

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(path, { signal, cache: "no-store" });
  if (!res.ok) {
    let detail = "";
    try {
      detail = await res.text();
    } catch {
      /* ignore */
    }
    throw new Error(
      `${res.status} ${res.statusText}${detail ? ` — ${detail.slice(0, 200)}` : ""}`,
    );
  }
  return res.json() as Promise<T>;
}

export function fetchFunnel(
  range: TimeRange,
  windowSeconds?: number,
  signal?: AbortSignal,
): Promise<FunnelResponse> {
  return getJson(
    `/api/olap/funnel${qs({ ...range, windowSeconds })}`,
    signal,
  );
}

export function fetchTopPages(
  range: TimeRange,
  limit = 8,
  signal?: AbortSignal,
): Promise<TopPage[]> {
  return getJson(`/api/olap/top-pages${qs({ ...range, limit })}`, signal);
}

export function fetchUniqueVisitors(
  range: TimeRange,
  interval: Interval,
  signal?: AbortSignal,
): Promise<UniqueVisitorsPoint[]> {
  return getJson(
    `/api/olap/unique-users${qs({ ...range, interval })}`,
    signal,
  );
}

export function fetchTimeseries(
  range: TimeRange,
  interval: Interval,
  eventType: string | undefined,
  signal?: AbortSignal,
): Promise<TimeseriesPoint[]> {
  return getJson(
    `/api/olap/timeseries${qs({ ...range, interval, eventType })}`,
    signal,
  );
}
