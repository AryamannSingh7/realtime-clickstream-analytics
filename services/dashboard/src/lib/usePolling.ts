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

  const hasData = data !== null;

  // Keep the latest fetcher without making it a re-fetch trigger itself.
  // Assigned in an effect, not during render, so the ref stays render-pure.
  const fetcherRef = useRef(fetcher);
  useEffect(() => {
    fetcherRef.current = fetcher;
  });

  // Tracks the in-flight request so a newer fetch (or unmount) cancels it.
  const controllerRef = useRef<AbortController | null>(null);

  // Extracted so mount/deps, the interval, and `refresh` all share one path.
  // Called from effects and event-like callbacks rather than run synchronously
  // in an effect body, so the `refreshing` flip doesn't cascade renders.
  const load = useCallback(() => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;

    // Flip the in-flight flag on a microtask, not synchronously, so kicking off a
    // fetch from an effect doesn't cascade an extra render before the effect settles.
    queueMicrotask(() => {
      if (!controller.signal.aborted) setRefreshing(true);
    });
    fetcherRef
      .current(controller.signal)
      .then((result) => {
        if (controller.signal.aborted) return;
        setData(result);
        setError(null);
        setLastUpdated(Date.now());
      })
      .catch((err: unknown) => {
        if (controller.signal.aborted) return;
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (controller.signal.aborted) return;
        setLoading(false);
        setRefreshing(false);
      });
  }, []);

  // Fetch on mount and whenever a dependency changes.
  useEffect(() => {
    load();
    return () => controllerRef.current?.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps]);

  // Interval scheduler — re-runs the shared fetch path.
  useEffect(() => {
    if (intervalMs <= 0) return;
    const id = setInterval(load, intervalMs);
    return () => clearInterval(id);
  }, [intervalMs, load]);

  const refresh = useCallback(() => load(), [load]);

  return {
    data,
    error,
    loading: loading && !hasData,
    refreshing,
    lastUpdated,
    refresh,
  };
}
