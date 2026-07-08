import type { NextConfig } from "next";

// The dashboard talks to the analytics-api serving layer (SSE live streams +
// ClickHouse OLAP). Same-origin `/api/olap/*` and `/api/stream/*` calls are
// proxied by runtime route handlers (src/app/api/**/route.ts) rather than
// next.config rewrites — rewrites bake their destination at build time, which
// would freeze the ANALYTICS_API_URL env into the standalone image. The route
// handlers read it live per request, so one image works on host and in compose.
const nextConfig: NextConfig = {
  // Emit a self-contained server bundle for a small production Docker image.
  output: "standalone",
  // This project is a Next app nested in a Maven monorepo; pin the trace root
  // to the app dir so file tracing doesn't wander up into the Java modules.
  outputFileTracingRoot: __dirname,
};

export default nextConfig;
