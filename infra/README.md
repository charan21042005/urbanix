# 🏗️ Local Infrastructure
![Status](https://img.shields.io/badge/Status-Planned%20(Day%201)-lightgrey)

## Purpose
Defines the local, standalone distributed cluster required to run Urbanix using Docker Compose.

## Cluster Components
- **Zookeeper:** Manages Kafka cluster state.
- **Kafka Broker:** Single-node message broker for development.
- **Spark Master:** Cluster manager for PySpark/Scala jobs.
- **Spark Worker:** Executes distributed streaming and graph tasks.

## Port Bindings
- Kafka: `localhost:9092`
- Spark UI: `localhost:8081` (Project's intended default endpoint is `localhost:8080`. Actual endpoint on this machine is `localhost:8081` because host port 8080 is already occupied. Internal Spark Master UI port remains `8080`.)
- Spark Master: `spark://localhost:7077`

**Current Status:** Day 1 infrastructure created. Docker Compose and Kafka topics are active.
