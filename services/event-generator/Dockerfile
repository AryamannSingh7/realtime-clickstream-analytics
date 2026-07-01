# Multi-stage build for the event-generator Spring Boot service.
# Build context MUST be the repo root (needs the parent pom + services/common):
#   docker build -f services/event-generator/Dockerfile .
# docker-compose already sets context: . for this service.

# ---- build stage ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace

# Copy POMs first so dependency resolution is cached independently of source changes.
COPY pom.xml ./
COPY services/common/pom.xml services/common/
COPY services/event-generator/pom.xml services/event-generator/
RUN mvn -q -pl services/event-generator -am dependency:go-offline -DskipTests || true

# Build the runnable fat jar (reactor: parent -> common -> event-generator).
# schemas/ is required: the common module's avro-maven-plugin generates sources from ../../schemas.
COPY schemas ./schemas
COPY services ./services
RUN mvn -q -pl services/event-generator -am clean package -DskipTests

# ---- runtime stage ----
FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app

# curl for the container healthcheck (actuator).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /workspace/services/event-generator/target/event-generator.jar app.jar
EXPOSE 8089
ENTRYPOINT ["java", "-jar", "app.jar"]
