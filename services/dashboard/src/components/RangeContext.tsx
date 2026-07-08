"use client";

import { createContext, useContext, useMemo, useState, type ReactNode } from "react";
import type { TimeRange } from "@/lib/olap";
import { presetFor, toTimeRange, type RangeKey, type RangePreset } from "@/lib/range";

interface RangeCtx {
  key: RangeKey;
  preset: RangePreset;
  range: TimeRange;
  setKey: (key: RangeKey) => void;
}

const Ctx = createContext<RangeCtx | null>(null);

export function RangeProvider({ children }: { children: ReactNode }) {
  const [key, setKey] = useState<RangeKey>("24h");

  const value = useMemo<RangeCtx>(
    () => ({ key, preset: presetFor(key), range: toTimeRange(key), setKey }),
    [key],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useRange(): RangeCtx {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useRange must be used within a RangeProvider");
  return ctx;
}
