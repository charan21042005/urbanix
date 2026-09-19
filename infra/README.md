# 🏗️ Local Infrastructure
![Status](https://img.shields.io/badge/Status-Planned%20(Day%201)-lightgrey)

## Purpose
Defines the local, standalone distributed cluster required to run Urbanix using Docker Compose.

## Cluster Components
- **Zookeeper:** Manages Kafka cluster state.
- **Kafka Broker:** Single-node message broker for development.
- **Spark Master:** Cluster manager for PySpark/Scala jobs.
- **Spark Worker:** Executes distributed streaming and graph tasks.

## Future Port Bindings
- Kafka: `localhost:9092`
- Spark UI: `localhost:8080`
- Spark Master: `spark://localhost:7077`

**Current Status:** Planned for Day 1. Docker Compose and Kafka topics have not been created yet.
