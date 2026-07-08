import { DashboardShell } from "@/components/DashboardShell";

export default function Home() {
  return (
    <div className="flex flex-1 flex-col">
      <header className="sticky top-0 z-10 border-b border-border bg-bg/80 backdrop-blur">
        <div className="mx-auto flex w-full max-w-7xl items-center justify-between px-6 py-3">
          <div className="flex items-center gap-3">
            <span className="live-dot h-2.5 w-2.5 rounded-full bg-accent" />
            <h1 className="text-sm font-semibold tracking-tight">Clickstream Analytics</h1>
            <span className="rounded bg-surface-2 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-dim">
              Live
            </span>
          </div>
          <p className="hidden font-mono text-[11px] text-faint sm:block">
            Kafka → Kafka Streams → ClickHouse → SSE
          </p>
        </div>
      </header>

      <main className="mx-auto w-full max-w-7xl flex-1 px-6 py-8">
        <DashboardShell />
      </main>

      <footer className="border-t border-border">
        <div className="mx-auto w-full max-w-7xl px-6 py-3 text-xs text-faint">
          Real-Time Clickstream Analytics Pipeline
        </div>
      </footer>
    </div>
  );
}
