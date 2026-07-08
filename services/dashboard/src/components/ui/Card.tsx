import type { ReactNode } from "react";

interface CardProps {
  title: string;
  subtitle?: string;
  /** Right-aligned slot in the header (e.g. a selector or status). */
  aside?: ReactNode;
  children: ReactNode;
  className?: string;
}

/** A panel in the console grid: hairline border, dark surface, titled header. */
export function Card({ title, subtitle, aside, children, className }: CardProps) {
  return (
    <section
      className={`flex flex-col rounded-lg border border-border bg-surface ${className ?? ""}`}
    >
      <header className="flex items-start justify-between gap-3 border-b border-border px-4 py-3">
        <div className="min-w-0">
          <h3 className="text-sm font-medium tracking-tight text-text">{title}</h3>
          {subtitle && <p className="mt-0.5 truncate text-xs text-dim">{subtitle}</p>}
        </div>
        {aside && <div className="shrink-0">{aside}</div>}
      </header>
      <div className="flex flex-1 flex-col p-4">{children}</div>
    </section>
  );
}

/** Centered status line used inside a card body for loading / error / empty. */
export function CardStatus({
  tone = "dim",
  children,
}: {
  tone?: "dim" | "danger";
  children: ReactNode;
}) {
  return (
    <div
      className={`flex flex-1 items-center justify-center py-8 text-xs ${
        tone === "danger" ? "text-danger" : "text-faint"
      }`}
    >
      {children}
    </div>
  );
}
