import type { NextRequest } from "next/server";

// Runtime proxy for the analytics-api SSE streams. Like the OLAP handler, this is a
// route handler rather than a next.config rewrite for two reasons: rewrites bake their
// destination at BUILD time (freezing localhost:8091 into the standalone image), and
// they can buffer the response — which would defeat an event stream entirely.
//
// The upstream body is piped straight through, unread, so events reach the browser as
// the analytics-api emits them. Aborting on `req.signal` propagates a browser
// disconnect upstream, letting the Spring SseEmitter prune its dead subscriber.
export const dynamic = "force-dynamic";

function apiBase(): string {
  return process.env.ANALYTICS_API_URL ?? "http://localhost:8091";
}

export async function GET(
  req: NextRequest,
  ctx: { params: Promise<{ path: string[] }> },
) {
  const { path } = await ctx.params;
  const target = `${apiBase()}/api/stream/${path.join("/")}${req.nextUrl.search}`;

  let upstream: Response;
  try {
    upstream = await fetch(target, {
      headers: { accept: "text/event-stream" },
      signal: req.signal,
      cache: "no-store",
    });
  } catch (err) {
    return Response.json(
      { error: "upstream unreachable", detail: String(err) },
      { status: 502 },
    );
  }

  if (!upstream.ok || !upstream.body) {
    return Response.json(
      { error: "upstream error", status: upstream.status },
      { status: 502 },
    );
  }

  return new Response(upstream.body, {
    status: 200,
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      // `no-transform` stops intermediaries (and Next's own compression) from
      // buffering the stream; `X-Accel-Buffering` does the same for nginx-alikes.
      "cache-control": "no-cache, no-store, no-transform",
      connection: "keep-alive",
      "x-accel-buffering": "no",
    },
  });
}
