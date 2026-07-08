"use client";

import type { ReactNode } from "react";

/** Dark, hairline tooltip surface shared by every chart. */
export function TooltipBox({
  title,
  rows,
}: {
  title: string;
  rows: { label: string; value: ReactNode; color?: string }[];
}) {
  return (
    <div className="rounded-md border border-border-strong bg-[#0e1013] px-3 py-2 shadow-lg">
      <div className="mb-1 text-[11px] font-medium text-dim">{title}</div>
      <div className="flex flex-col gap-0.5">
        {rows.map((r, i) => (
          <div key={i} className="flex items-center gap-2 text-xs">
            {r.color && (
              <span
                className="h-2 w-2 shrink-0 rounded-[2px]"
                style={{ background: r.color }}
              />
            )}
            <span className="text-faint">{r.label}</span>
            <span className="tnum ml-auto font-medium text-text">{r.value}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
