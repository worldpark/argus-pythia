# syntax=docker/dockerfile:1.6
# pythia multi-stage Dockerfile
# - builder: JDK 21 + gradle wrapper to produce bootJar
# - runtime: JRE 21 only (smaller image, smaller attack surface)

FROM eclipse-temurin:21-jdk AS builder
WORKDIR /app

# Copy gradle wrapper and build definition first to leverage layer cache
COPY gradle gradle
COPY gradlew gradlew
COPY gradlew.bat gradlew.bat
COPY settings.gradle build.gradle ./

# Copy sources
COPY src src

# Build bootJar (skip tests; tests are executed in CI/local, not in container build)
RUN chmod +x gradlew && ./gradlew clean bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
ENV TZ=Asia/Seoul

# Copy the produced fat jar from the builder stage
COPY --from=builder /app/build/libs/*-SNAPSHOT.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
