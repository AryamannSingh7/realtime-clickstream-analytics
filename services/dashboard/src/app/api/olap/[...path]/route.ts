import type { NextRequest } from "next/server";

// Runtime proxy for the analytics-api OLAP endpoints. A route handler (not a
// next.config rewrite) is used deliberately: rewrites are baked at BUILD time,
// so an env-var destination would freeze to the build-time value inside the
// standalone image. This handler reads ANALYTICS_API_URL live on each request,
// so the same image works on the host (localhost) and in compose (analytics-api).
export const dynamic = "force-dynamic";

function apiBase(): string {
  return process.env.ANALYTICS_API_URL ?? "http://localhost:8091";
}

export async function GET(
  req: NextRequest,
  ctx: { params: Promise<{ path: string[] }> },
) {
  const { path } = await ctx.params;
  const target = `${apiBase()}/api/olap/${path.join("/")}${req.nextUrl.search}`;

  try {
    const upstream = await fetch(target, { cache: "no-store" });
    const body = await upstream.text();
    return new Response(body, {
      status: upstream.status,
      headers: {
        "content-type":
          upstream.headers.get("content-type") ?? "application/json",
        "cache-control": "no-store",
      },
    });
  } catch (err) {
    return Response.json(
      { error: "upstream unreachable", detail: String(err) },
      { status: 502 },
    );
  }
}
