"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export interface PollingState<T> {
  data: T | null;
  error: string | null;
  loading: boolean; // true only on the first load (no data yet)
  refreshing: boolean; // true on background refetches
  lastUpdated: number | null;
  refresh: () => void;
}

/**
 * Fetch `fetcher` on mount and whenever `deps` change, then re-fetch every
 * `intervalMs`. Keeps the previous data visible during background refreshes so
 * the chart doesn't flash empty. Aborts in-flight requests on unmount/change.
 */
export function usePolling<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  deps: readonly unknown[],
  intervalMs = 60_000,
): PollingState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);

  // Keep the latest fetcher without making it a re-fetch trigger itself.
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const hasData = data !== null;
  const [tick, setTick] = useState(0);
  const refresh = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;

    setRefreshing(true);
    fetcherRef
      .current(controller.signal)
      .then((result) => {
        if (cancelled) return;
        setData(result);
        setError(null);
        setLastUpdated(Date.now());
      })
      .catch((err: unknown) => {
        if (cancelled || controller.signal.aborted) return;
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (cancelled) return;
        setLoading(false);
        setRefreshing(false);
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tick]);

  // Interval scheduler — bumps `tick` so the effect above re-runs.
  useEffect(() => {
    if (intervalMs <= 0) return;
    const id = setInterval(() => setTick((t) => t + 1), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);

  return {
    data,
    error,
    loading: loading && !hasData,
    refreshing,
    lastUpdated,
    refresh,
  };
}
