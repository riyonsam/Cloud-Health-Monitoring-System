# ACP CW3 — Intelligent Cloud Service Health Monitor

**Stack:** Java 21 · Spring Boot 3.4 · AWS SDK v2 · Kafka · RabbitMQ · Redis · PostgreSQL

## Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Java | 21 | Runtime |
| Spring Boot | 3.4.2 | Web framework + scheduling |
| AWS SDK v2 | 2.23.0 | S3 + DynamoDB clients |
| Apache Kafka | 3.6.1 | Event streaming |
| RabbitMQ AMQP | 5.20.0 | Alerting |
| Jedis | 5.1.0 | Redis client |
| PostgreSQL | 15 | Audit history |
| LocalStack | 3.8.1 | Local AWS simulation |
| Docker | Buildx | Multi-platform image |


---

## Overview

A production-grade observability platform that continuously monitors all six ACP stack services, detects failures, attempts automated recovery, and persists telemetry simultaneously across five storage backends.

The system addresses a fundamental gap in CW1/CW2: when any dependent service becomes unavailable, the application returns a generic HTTP 500 with no diagnostic context, no failure history, and no automated recovery. This monitor solves that.

**Inspired by:** AWS CloudWatch Synthetics, Datadog, PagerDuty — adapted to the ACP stack.

---

## Architecture

```
@Scheduled (30s) ──→ HealthMonitorService ──→ [S3, DynamoDB, Postgres, Redis, RabbitMQ, Kafka]
REST API (10 endpoints) ──→ HealthMonitorService
                                    │
                    ┌───────────────┼───────────────┐
                    ↓               ↓               ↓
                  Redis          Kafka           Postgres
              (status cache)  (event log)    (audit history)
                    ↓               ↓               ↓
                RabbitMQ           S3           DynamoDB
              (DOWN alerts)    (reports)       (analytics)
```

Each backend serves a distinct purpose:
- **Redis** — sub-millisecond live status + atomic consecutive failure counters
- **Kafka** — durable replayable event log (readable from offset 0)
- **PostgreSQL** — queryable audit history with recovery outcome tracking
- **RabbitMQ** — alerts on DOWN/DEGRADED events only
- **S3** — timestamped JSON health reports on demand
- **DynamoDB** — hourly aggregated uptime statistics

---

## Features

| Feature | Description |
|---|---|
| Health monitoring | All 6 services checked every 30 seconds |
| Auto-recovery | Fires automatically after 2 consecutive failures |
| Consecutive failure tracking | Redis INCR counter, resets on UP, expires after 1 hour |
| Full incident history | DOWN → recovery attempts → restoration all stored in Postgres |
| Multi-tier persistence | 5 storage backends written simultaneously per cycle |
| Web dashboard | 7-tab SPA with 30-second auto-refresh |
| S3 report export | Full JSON health report with service breakdown |
| DynamoDB analytics | Hourly aggregated uptime statistics |
| Kafka event stream | Manual partition assignment, reads from offset 0 |
| Simulate failure | Inject controlled DOWN state without stopping containers |

---

## REST Endpoints

All endpoints are prefixed with `/api/v1/health`.

| Method | Path | Function | Backend |
|---|---|---|---|
| POST | `/check` | Trigger immediate health check | All backends |
| GET | `/status` | Live status + consecutive failure count | Redis |
| GET | `/history` | Queryable audit log with service filter | PostgreSQL |
| GET | `/alerts` | Consume pending DOWN/DEGRADED alerts | RabbitMQ |
| GET | `/stream` | Kafka event stream with partition offsets | Kafka |
| GET | `/stats` | Uptime %, avg response time, total checks | PostgreSQL |
| GET | `/report` | List saved JSON health reports | S3 |
| POST | `/report` | Generate and save a health report | S3 |
| GET | `/analytics` | Hourly aggregated uptime statistics | DynamoDB |
| POST | `/recover/{service}` | Manual recovery trigger | Varies |
| POST | `/simulate/down/{service}` | Inject controlled failure for demo | Redis TTL |
| POST | `/simulate/up/{service}` | Clear simulated failure | Redis |

---

## Project Structure

```
src/main/java/uk/ac/ed/inf/cw3/
├── AcpCw3Application.java
├── configuration/
│   ├── SystemEnvironment.java          # Reads all 8 env vars
│   ├── InfrastructureConfiguration.java # All Spring beans
│   └── AppConfiguration.java           # @PostConstruct dependency wiring
├── model/
│   ├── ServiceStatus.java              # UP / DOWN / DEGRADED / UNKNOWN
│   └── HealthEvent.java                # Health check result with recovery fields
├── service/
│   ├── HealthMonitorService.java       # Scheduler + auto-recovery
│   ├── HealthEventProcessor.java       # Postgres storage + RabbitMQ alerts
│   ├── RecoveryService.java            # 6 service-specific recovery strategies
│   ├── StatsService.java               # Uptime stats from Postgres
│   ├── ReportService.java              # S3 report generation
│   ├── KafkaConsumerService.java       # Manual partition assign consumer
│   └── DynamoDbAnalyticsService.java   # Hourly DynamoDB aggregation
└── controller/
    └── HealthController.java           # All 10 REST endpoints

src/main/resources/
├── application.yml
└── static/
    └── dashboard.html                  # 7-tab web dashboard
```

---

## Environment Variables

| Variable | Example | Description |
|---|---|---|
| `ACP_POSTGRES` | `jdbc:postgresql://localhost:5432/postgres` | PostgreSQL connection string |
| `ACP_S3` | `http://localhost:4566` | S3 endpoint (LocalStack) |
| `ACP_DYNAMODB` | `http://localhost:4566` | DynamoDB endpoint (LocalStack) |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `RABBITMQ_HOST` | `localhost` | RabbitMQ hostname |
| `RABBITMQ_PORT` | `5672` | RabbitMQ port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |

---

## Running Locally

### Prerequisites
- Java 21
- Docker Desktop
- IntelliJ IDEA (or any IDE)

### Step 1 — Start infrastructure

```bash
docker-compose up -d
```

Wait ~15 seconds for all containers to initialise. Verify with:

```bash
docker ps
```

You should see: localstack, postgres, redis, rabbitmq, zookeeper, kafka.

### Step 2 — Stop native PostgreSQL (macOS only)

```bash
launchctl unload ~/Library/LaunchAgents/homebrew.mxcl.postgresql@14.plist
```

Skip if no local PostgreSQL is installed.

### Step 3 — Run the application

Open the project in IntelliJ and run `AcpCw3Application.java`, or via terminal:

```bash
./mvnw spring-boot:run
```

Application starts on **port 8080**.

### Step 4 — Open the dashboard

```
http://localhost:8080/dashboard.html
```

### Step 5 — Trigger a health check

```bash
curl -X POST http://localhost:8080/api/v1/health/check
```

---

## Running via Docker Image

```bash
docker run -d --publish 8080:8080 --name cw3 \
  -e REDIS_HOST=host.docker.internal \
  -e REDIS_PORT=6379 \
  -e RABBITMQ_HOST=host.docker.internal \
  -e RABBITMQ_PORT=5672 \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9093 \
  -e ACP_POSTGRES=jdbc:postgresql://host.docker.internal:5432/postgres \
  -e ACP_S3=http://host.docker.internal:4566 \
  -e ACP_DYNAMODB=http://host.docker.internal:4566 \
  acp-image-cw3
```

---

## Building the Docker Image

```bash
docker buildx build --platform=linux/amd64,linux/arm64 -t acp-image-cw3 --load .
docker image save acp-image-cw3 -o acp_submission_image.tar
```

---

## Demo Commands

```bash
# Simulate a service failure
curl -X POST http://localhost:8080/api/v1/health/simulate/down/S3

# Check status
curl http://localhost:8080/api/v1/health/status

# View alerts from RabbitMQ
curl http://localhost:8080/api/v1/health/alerts

# Manually trigger recovery
curl -X POST http://localhost:8080/api/v1/health/recover/S3

# Clear simulation
curl -X POST http://localhost:8080/api/v1/health/simulate/up/S3

# Generate S3 health report
curl -X POST http://localhost:8080/api/v1/health/report

# View Kafka event stream
curl "http://localhost:8080/api/v1/health/stream?limit=10"

# Trigger DynamoDB analytics snapshot
curl -X POST http://localhost:8080/api/v1/health/analytics/snapshot
```

---

## Engineering Decisions

**Circular dependency resolution**  
`HealthMonitorService` and `RecoveryService` share infrastructure clients, creating a circular Spring dependency. Resolved using `@PostConstruct` setter injection in `AppConfiguration` — the production pattern for event-driven Spring architectures. `@Lazy` was rejected as it defers errors to runtime.

**Kafka consumer strategy**  
The stream endpoint uses manual partition assignment with `seekToBeginning()` rather than `subscribe()`. The `subscribe()` API requires a group rebalance before messages are readable, causing empty results on the first poll. Manual assignment skips the rebalance entirely. A unique group ID per request ensures reads always start from offset 0.

**Consecutive failure tracking**  
A Redis `INCR` counter per service increments atomically on each DOWN/DEGRADED result, resets on UP, and expires after one hour. Stateless across application restarts — no stale counter state after a crash.

**Report health classification**  
S3 reports classify services as healthy/unhealthy based on current live status from Redis, not historical uptime percentage. A service with 95% uptime that is currently DOWN would correctly appear as unhealthy.


