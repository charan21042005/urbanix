# 💾 Datasets & Storage
![Status](https://img.shields.io/badge/Status-Setup-brightgreen)

## Purpose
Central repository for sample, historical, and generated data used across the Urbanix platform.

## Data Types
- **Traffic Data:** Speed, density, vehicle counts.
- **AQI Data:** PM2.5, PM10, CO2 levels.
- **Weather Data:** Temperature, precipitation, visibility.

## Storage Formats
The primary output format for Spark Structured Streaming will be **Parquet**, enabling efficient columnar reads for the dashboard API. 

*Data Provenance:* All datasets will be clearly sourced (public/historical) or synthetically generated.

**Current Status:** Directory established. No datasets loaded yet.
