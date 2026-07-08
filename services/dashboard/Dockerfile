# syntax=docker/dockerfile:1
# Next.js dashboard (M6) — multi-stage build producing a slim standalone image.
# Build context is this directory (services/dashboard); the app is self-contained
# and has no dependency on the Maven modules or shared schemas.

# ---- deps: install node_modules from the lockfile ----
FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

# ---- build: compile the Next standalone output ----
FROM node:22-alpine AS build
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

# ---- runtime: minimal server, non-root ----
FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    PORT=3000 \
    HOSTNAME=0.0.0.0
RUN addgroup -g 1001 -S nodejs && adduser -S nextjs -u 1001

# Standalone server + static assets (see next.config.ts output: "standalone").
COPY --from=build /app/public ./public
COPY --from=build --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=build --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs
EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=5s --retries=12 \
  CMD wget -qO- http://localhost:3000/ >/dev/null 2>&1 || exit 1
CMD ["node", "server.js"]
