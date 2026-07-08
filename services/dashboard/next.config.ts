import type { NextConfig } from "next";

// The dashboard talks to the analytics-api serving layer (SSE live streams +
// ClickHouse OLAP). We proxy same-origin `/api/*` calls through Next so the
// browser never needs the backend URL (no CORS) and the target is configurable
// per environment: http://localhost:8091 for host dev, http://analytics-api:8091
// inside docker-compose.
const ANALYTICS_API_URL =
  process.env.ANALYTICS_API_URL ?? "http://localhost:8091";

const nextConfig: NextConfig = {
  // Emit a self-contained server bundle for a small production Docker image.
  output: "standalone",
  // This project is a Next app nested in a Maven monorepo; pin the trace root
  // to the app dir so file tracing doesn't wander up into the Java modules.
  outputFileTracingRoot: __dirname,
  async rewrites() {
    return [
      {
        source: "/api/olap/:path*",
        destination: `${ANALYTICS_API_URL}/api/olap/:path*`,
      },
      {
        source: "/api/stream/:path*",
        destination: `${ANALYTICS_API_URL}/api/stream/:path*`,
      },
    ];
  },
};

export default nextConfig;
