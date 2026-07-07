# Multi-stage build for the analytics-api Spring Boot service (SSE + ClickHouse OLAP).
# Build context MUST be the repo root (needs the parent pom + services/common):
#   docker build -f services/analytics-api/Dockerfile .
# docker-compose already sets context: . for this service.

# ---- build stage ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace

# Copy POMs first so dependency resolution is cached independently of source changes.
COPY pom.xml ./
COPY services/common/pom.xml services/common/
COPY services/analytics-api/pom.xml services/analytics-api/
RUN mvn -q -pl services/analytics-api -am dependency:go-offline -DskipTests || true

# Build the runnable fat jar (reactor: parent -> common -> analytics-api).
# schemas/ is required: the common module's avro-maven-plugin generates sources from ../../schemas.
# -U forces a re-check of remote repos: the cached go-offline layer above runs with `|| true`,
# so a transient download blip there can leave `.lastUpdated` "will not be reattempted" markers
# that would otherwise fail this step even though the artifacts are available.
COPY schemas ./schemas
COPY services ./services
RUN mvn -q -U -pl services/analytics-api -am clean package -DskipTests

# ---- runtime stage ----
FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app

# curl for the container healthcheck (actuator).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /workspace/services/analytics-api/target/analytics-api.jar app.jar
EXPOSE 8091
ENTRYPOINT ["java", "-jar", "app.jar"]
