# dashboard

Next.js (App Router, TypeScript, Tailwind v4) frontend for the Real-Time
Clickstream Analytics pipeline — a dark real-time "ops console" showing live
streaming rollups and ClickHouse OLAP views.

## Data sources

The app talks to the `analytics-api` serving layer. Same-origin requests are
proxied server-side (see `next.config.ts`), so the browser needs no backend URL
and there is no CORS:

- `/api/olap/*` → ClickHouse OLAP (funnel, top-pages, unique-users, timeseries)
- `/api/stream/*` → SSE live streams (metrics, top-N, funnel, alerts)

The proxy target is configurable via `ANALYTICS_API_URL`
(default `http://localhost:8091`; `http://analytics-api:8091` under compose).

## Develop

```bash
npm install
ANALYTICS_API_URL=http://localhost:8091 npm run dev   # http://localhost:3000
```

Requires a running analytics-api (see the repo root `docker compose`) for live
data. Charts use [Recharts](https://recharts.org).

## Build

```bash
npm run build      # standalone output (output: "standalone")
```

Containerized via the `Dockerfile`; wired into the root `docker-compose.yml` as
the `dashboard` service (served at `:3000`).
