"use client";

import { useEffect, useRef, useState } from "react";
import type { StreamKey } from "@/lib/streams";

/** Connection state of the underlying EventSource. */
export type StreamStatus = "connecting" | "live" | "error";

export interface EventStreamState<T> {
  /** Most recent payload, or null before the first event arrives. */
  last: T | null;
  /** Bounded chronological history (oldest first), capped at `historyLimit`. */
  history: T[];
  status: StreamStatus;
  /** Epoch ms of the last received event, or null. */
  lastEventAt: number | null;
}

interface Options {
  /** How many past payloads to retain. 1 keeps only `last`. */
  historyLimit?: number;
  /** Set false to leave the stream closed (e.g. widget not visible). */
  enabled?: boolean;
}

/**
 * Subscribe to one of the live `/api/stream/{key}` SSE endpoints.
 *
 * The server emits *named* events (`event: funnel`), so we listen for `key` rather
 * than the default `message`. EventSource reconnects on its own after a drop; we
 * surface that as `error` while it retries and flip back to `live` on the next event.
 */
export function useEventStream<T>(
  key: StreamKey,
  { historyLimit = 1, enabled = true }: Options = {},
): EventStreamState<T> {
  const [last, setLast] = useState<T | null>(null);
  const [history, setHistory] = useState<T[]>([]);
  const [status, setStatus] = useState<StreamStatus>("connecting");
  const [lastEventAt, setLastEventAt] = useState<number | null>(null);

  // Read inside the event handler without making the limit a reconnect trigger.
  // Assigned in an effect, not during render, so the ref stays render-pure.
  const limitRef = useRef(historyLimit);
  useEffect(() => {
    limitRef.current = historyLimit;
  }, [historyLimit]);

  useEffect(() => {
    if (!enabled) return;

    const source = new EventSource(`/api/stream/${key}`);

    source.addEventListener("open", () => setStatus("live"));

    source.addEventListener(key, (event) => {
      let payload: T;
      try {
        payload = JSON.parse((event as MessageEvent<string>).data) as T;
      } catch {
        return; // Ignore a malformed frame rather than tearing down the stream.
      }
      setStatus("live");
      setLastEventAt(Date.now());
      setLast(payload);
      setHistory((prev) => {
        const next = [...prev, payload];
        return next.length > limitRef.current
          ? next.slice(next.length - limitRef.current)
          : next;
      });
    });

    // EventSource retries automatically; report the gap but keep the last payload
    // on screen so the widget shows stale data rather than flashing empty.
    source.addEventListener("error", () => setStatus("error"));

    return () => source.close();
  }, [key, enabled]);

  return { last, history, status, lastEventAt };
}
