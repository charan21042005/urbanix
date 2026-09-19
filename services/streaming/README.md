# 🌊 Streaming Core & ML Forecasting
![Status](https://img.shields.io/badge/Status-Planned%20(Days%203--6)-blue)

## Purpose
This PySpark module consumes raw Kafka streams, applies time-based aggregations, engineers features, and executes machine learning models to forecast future urban conditions.

## Architecture & Concepts
- **Kafka to Spark:** Ingests `traffic-topic`, `aqi-topic`, and `weather-topic` using Spark Structured Streaming.
- **Event Time & Watermarking:** Handles out-of-order data using event-time windows and watermarks to drop stale records.
- **Spark SQL:** Flattens JSON payloads and enforces schemas.
- **Feature Engineering:** Creates tumbling/sliding window aggregations.
- **MLlib Integration:** Uses Linear Regression and Decision Tree Regressor models trained on historical data to predict future congestion and PM2.5 levels.

**Current Status:** Planned for Days 3–6. No Spark or ML code implemented yet.
