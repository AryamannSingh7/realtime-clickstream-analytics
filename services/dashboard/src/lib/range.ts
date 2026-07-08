import type { Interval, TimeRange } from "./olap";

// Time-range presets driving the OLAP `from`/`to` params. Each preset also picks
// a sensible bucket granularity so a 24h view isn't 1440 one-minute points.
export type RangeKey = "24h" | "7d" | "30d";

export interface RangePreset {
  key: RangeKey;
  label: string;
  hours: number;
  interval: Interval;
  /** Human label for the bucket size, e.g. "5 min", "hour", "day". */
  bucketLabel: string;
  /** Whether buckets are day-grained (drives axis date formatting). */
  byDay: boolean;
}

export const RANGE_PRESETS: RangePreset[] = [
  // 24h uses 5-min buckets so a freshly-started pipeline shows a line within
  // minutes (hourly would be a single point for the first hour).
  { key: "24h", label: "24h", hours: 24, interval: "5m", bucketLabel: "5 min", byDay: false },
  { key: "7d", label: "7d", hours: 24 * 7, interval: "hour", bucketLabel: "hour", byDay: false },
  { key: "30d", label: "30d", hours: 24 * 30, interval: "day", bucketLabel: "day", byDay: true },
];

export function presetFor(key: RangeKey): RangePreset {
  return RANGE_PRESETS.find((p) => p.key === key) ?? RANGE_PRESETS[0];
}

/** Resolve a preset to concrete ISO `from`/`to` bounds anchored to `now`. */
export function toTimeRange(key: RangeKey, now: number = Date.now()): TimeRange {
  const preset = presetFor(key);
  return {
    from: new Date(now - preset.hours * 3_600_000).toISOString(),
    to: new Date(now).toISOString(),
  };
}
