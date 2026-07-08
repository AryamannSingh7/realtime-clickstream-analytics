// M6.1 scaffold — a static dark "console" shell. The live SSE widgets (metrics
// ticker, live funnel, top-N, alerts) and OLAP charts land in M6.2 / M6.3; this
// page just proves the app boots, is themed, and lays out the widget grid.

const PLANNED_WIDGETS = [
  { title: "Live metrics", note: "events/s · page views · revenue", slice: "M6.3" },
  { title: "Conversion funnel", note: "view → cart → checkout → purchase", slice: "M6.2" },
  { title: "Top pages", note: "windowed top-N", slice: "M6.2" },
  { title: "Unique visitors", note: "uniqCombined over time", slice: "M6.2" },
  { title: "Event timeseries", note: "counts by type", slice: "M6.2" },
  { title: "Anomaly alerts", note: "EWMA volume spikes / drops", slice: "M6.3" },
];

export default function Home() {
  return (
    <div className="flex flex-1 flex-col">
      <header className="border-b border-border">
        <div className="mx-auto flex w-full max-w-7xl items-center justify-between px-6 py-4">
          <div className="flex items-center gap-3">
            <span className="live-dot h-2.5 w-2.5 rounded-full bg-accent" />
            <h1 className="text-sm font-semibold tracking-tight">
              Clickstream Analytics
            </h1>
            <span className="rounded bg-surface-2 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-dim">
              Live
            </span>
          </div>
          <p className="hidden text-xs text-faint sm:block">
            Kafka → Kafka Streams → ClickHouse → SSE
          </p>
        </div>
      </header>

      <main className="mx-auto w-full max-w-7xl flex-1 px-6 py-8">
        <div className="mb-6">
          <h2 className="text-lg font-semibold tracking-tight">Overview</h2>
          <p className="mt-1 text-sm text-dim">
            Dashboard scaffold is online. Widgets connect to{" "}
            <code className="tnum text-accent-2">/api/olap/*</code> and{" "}
            <code className="tnum text-accent-2">/api/stream/*</code> next.
          </p>
        </div>

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {PLANNED_WIDGETS.map((w) => (
            <div
              key={w.title}
              className="flex min-h-32 flex-col justify-between rounded-lg border border-border bg-surface p-4"
            >
              <div>
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-medium">{w.title}</h3>
                  <span className="rounded bg-surface-2 px-1.5 py-0.5 text-[10px] font-medium text-faint">
                    {w.slice}
                  </span>
                </div>
                <p className="mt-1 text-xs text-dim">{w.note}</p>
              </div>
              <div className="mt-4 h-12 rounded bg-surface-2/60" />
            </div>
          ))}
        </div>
      </main>

      <footer className="border-t border-border">
        <div className="mx-auto w-full max-w-7xl px-6 py-3 text-xs text-faint">
          Real-Time Clickstream Analytics Pipeline
        </div>
      </footer>
    </div>
  );
}
