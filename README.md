# Urbanix — Smart City IoT Traffic & Environmental Risk Forecasting Platform

## Overview
Urbanix is a distributed, event-driven smart-city analytics platform. It simulates and ingests sensor telemetry (traffic, air-quality, and weather), processes it in real-time, forecasts near-future conditions, and calculates congestion-aware routes. 

This project is built incrementally over a 12-day development roadmap to explore distributed systems, stream processing, machine learning, and graph algorithms.

## High-Level Architecture
The platform is composed of six core modules:
1. **Sensor Simulation Layer**: Python producers simulating telemetry data.
2. **Kafka Streaming Core**: Event ingestion and message brokering.
3. **Spark Structured Streaming + SQL**: Real-time aggregation and feature engineering.
4. **Spark MLlib Forecasting**: Machine learning models predicting traffic and pollution.
5. **Scala Spark GraphX Rerouting**: Congestion-aware graph shortest-path calculations.
6. **Geospatial Dashboard**: Visualization and API layer.

## Monorepo Organization
- `services/producers/`: Sensor data generation and Kafka ingestion.
- `services/streaming/`: Stream processing, aggregation, and ML forecasting.
- `services/graphx-reroute/`: Congestion-aware graph processing.
- `dashboard/`: User interface and visualizations.
- `infra/`: Docker Compose for local cluster infrastructure.
- `data/`: Sample and historical datasets.
- `docs/`: Architecture and design documentation.
- `.github/workflows/`: CI/CD automation pipelines.

## Technologies Used
* **Data Ingestion**: Python 3.10+, Kafka Producers
* **Streaming & Messaging**: Apache Kafka, Zookeeper
* **Stream Processing**: PySpark Structured Streaming, Spark SQL
* **Machine Learning**: Apache Spark MLlib
* **Graph Processing**: Scala, Apache Spark GraphX
* **Infrastructure**: Docker, Docker Compose
* **Visualization**: FastAPI/Flask, Folium/Plotly

## Current Status
**Day 1 Status:** 
- [x] Repository initialized.
- [ ] Local infrastructure setup (Kafka & Spark cluster).
- [ ] Kafka topics created.

*Note: This project is under active development. Modules listed above are planned and will be implemented incrementally according to the 12-day roadmap.*
