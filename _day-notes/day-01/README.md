# Urbanix — Day 1: Distributed Infrastructure Foundation

Welcome to the **Day 1 Module Documentation** for Project Urbanix.

## Milestone Summary
- **Module:** Weekend 1 — Core Foundation & Ingestion (Day 1 of 12)
- **Core Technologies:** Docker, Docker Compose, Apache Zookeeper, Apache Kafka, Apache Spark Standalone (Master + Worker)
- **Status:** **VERIFIED & FORMALLY CLOSED** (All Acceptance Gates Passed)
- **Primary Deliverables:** Multi-container distributed cluster orchestration, health checks, networking, and volume persistence.

## Directory Contents
- [DAY_01_INFRASTRUCTURE_REPORT.md](DAY_01_INFRASTRUCTURE_REPORT.md) — Exhaustive technical report covering distributed cluster topology, Docker Compose definitions, Zookeeper quorum mechanics, Kafka topic provisioning, Spark Standalone worker registration, and cluster verification logs.
- [../../docker-compose.yml](../../docker-compose.yml) — Production Docker Compose orchestration manifest defining Zookeeper (`2181`), Kafka Broker (`9092`), Spark Master (`7077`, UI `8080`), and Spark Worker (`8081`).
- [../../infra/](../../infra/) — Infrastructure configuration files, environment variables, and persistence mount directories.

## Quick Execution Commands
```powershell
# Navigate to repository root
cd C:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix

# 1. Start all infrastructure containers
docker-compose up -d

# 2. Verify container health status
docker-compose ps

# 3. Inspect Kafka broker readiness
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```
