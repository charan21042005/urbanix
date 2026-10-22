# 🏙️ URBANIX — Geospatial Dashboard Backend, Live Heatmap & Engineering Verification
## Day 10 of 12 | INT315 Cluster Computing Capstone

Welcome to the **Urbanix Geospatial Dashboard**! This module serves as the primary visual interface and presentation tier for the Urbanix Smart City IoT Traffic & Environmental Risk Forecasting Platform.

It bridges our big-data distributed processing backend (Spark Structured Streaming, MLlib, and Scala GraphX) with modern web standards, rendering live traffic predictions, route recommendations, and geospatial heatmaps directly in the browser.

---

## 1. What the Dashboard Does

The Urbanix Dashboard provides a real-time, data-driven window into the city's vehicular transportation network:
1. **Reads Live Parquet Data**: Scans both the MLlib traffic prediction feature store (`data/feature_table/traffic/`) and the Scala GraphX route recommendation snapshot (`data/recommended_routes/`) afresh on every request.
2. **Serves RESTful APIs**: Exposes clean JSON endpoints for traffic forecasts (`/api/predictions`) and routing paths with ETAs (`/api/route`).
3. **Renders Dynamic Heatmaps**: Generates an interactive Folium Leaflet geospatial visualization (`/api/map`) mapping predicted zone speeds to color-coded Level of Service (LOS) markers.
4. **Auto-Refreshes the Client UI**: The web frontend (`/`) automatically polls backend endpoints every 15 seconds (`REFRESH_INTERVAL_MS = 15000`), updating KPI metric ribbons and the embedded map without requiring manual page reloads.

---

## 2. System Architecture & Component Interactions

The dashboard is the consumption layer of a four-tier distributed computing architecture:

```
[IoT Sensor Producers / Replay Engine]
          │ (Kafka Topics: traffic-topic, aqi-topic, weather-topic)
          ▼
[Spark Structured Streaming (Days 1–3)]
          │ (Windowed 5-min micro-batches, watermarking, zone aggregations)
          ▼
[MLlib Traffic Forecasting Engine (Days 4–6)]
          │ (Lag-feature regression, distributed model inference)
          ▼
[Production Parquet Feature Store: data/feature_table/traffic/]
          │
          ├──► [Scala Spark GraphX Engine (Days 7–9)]
          │        │ (Pregel shortest-path rerouting with speed weights)
          │        ▼
          │    [Production Routes Snapshot: data/recommended_routes/]
          │        │
          └────────┼───────────────┐
                   ▼               ▼
          [FastAPI Dashboard Backend (Day 10: dashboard/main.py)]
                   │ (Fresh Parquet scan, zero in-memory caching)
                   ├───────────────┬────────────────┐
                   ▼               ▼                ▼
           /api/predictions   /api/route        /api/map
             (JSON API)       (JSON API)     (Folium Leaflet)
                   │               │                │
                   └───────────────┼────────────────┘
                                   ▼
          [Browser Frontend: dashboard/static/index.html]
                   │ (15-second asynchronous polling)
                   ▼
          [Interactive Geospatial Traffic Operations Console]
```

### Component Breakdown
| Layer | Technology | Primary Role in Urbanix |
| :--- | :--- | :--- |
| **Storage / Contract** | Apache Parquet (Snappy) | High-performance, columnar immutable persistence for time-series feature tables and route snapshots. |
| **Backend API** | FastAPI + Uvicorn (ASGI) | Lightweight, asynchronous Python web server delivering low-latency JSON and HTML responses. |
| **Geospatial Engine**| Folium + Leaflet.js | Python-to-JavaScript geospatial library that compiles OpenStreetMap tiles and zone overlays into interactive HTML. |
| **Frontend UI** | HTML5, CSS3, Vanilla JS | Dark glassmorphism dashboard featuring iframe map embedding, real-time metrics, and polling timers. |

---

## 3. Directory Structure

```
dashboard/
├── main.py                      # FastAPI application: Parquet readers, REST endpoints, Folium map builder
├── requirements.txt             # Pinned dashboard dependencies
├── README.md                    # Comprehensive engineering and viva guide (this document)
├── static/
│   └── index.html               # Frontend dashboard: responsive UI, KPI ribbons, iframe map, auto-polling
├── tests/
│   ├── test_dashboard.py        # Pytest suite: 12 unit and API contract verification tests
│   └── smoke_test.py            # Live HTTP smoke test script targeting running Uvicorn server
└── venv/                        # Dedicated Python 3.10 virtual environment (gitignored)
```

---

## 4. Environment Requirements & Installation

### Prerequisites
- **Operating System**: Windows 11 (or Linux/macOS)
- **Python**: Python 3.10.x (verified on Python 3.10.11)
- **Shell**: Windows PowerShell

### Setup Instructions

1. **Open Windows PowerShell** and navigate to the repository root:
   ```powershell
   Set-Location "C:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix"
   ```

2. **Create the dedicated virtual environment** inside `dashboard\venv`:
   ```powershell
   py -3.10 -m venv dashboard\venv
   ```

3. **Install pinned dependencies**:
   ```powershell
   .\dashboard\venv\Scripts\python.exe -m pip install --upgrade pip
   .\dashboard\venv\Scripts\python.exe -m pip install -r dashboard\requirements.txt
   ```

4. **Verify installed packages**:
   ```powershell
   .\dashboard\venv\Scripts\python.exe -m pip list
   ```

---

## 5. Starting and Stopping the Dashboard

### Launching the Server
Run Uvicorn using the virtual environment's Python interpreter:

```powershell
# From the repository root:
.\dashboard\venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8000 --app-dir dashboard
```

Alternatively, if launched directly from inside `dashboard/`:
```powershell
Set-Location "C:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix\dashboard"
..\dashboard\venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8000
```
*(The backend automatically resolves paths relative to the repository root regardless of launch location).*

### Accessing the Web Interface
Once started, open your web browser to:
- **Interactive Dashboard**: [http://127.0.0.1:8000/](http://127.0.0.1:8000/)
- **Interactive Swagger API Docs**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **Alternative ReDoc API Docs**: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

### Stopping the Server
Press `Ctrl + C` in the terminal where Uvicorn is running. The ASGI server will gracefully terminate all active worker connections.

---

## 6. REST API Endpoints Specification

### 1. `GET /api/predictions`
Returns the latest traffic velocity predictions for all active zones.

- **Request**: `GET http://127.0.0.1:8000/api/predictions`
- **Response Format**: `application/json`
- **Status Codes**: `200 OK`, `500 Internal Server Error`, `503 Service Unavailable`
- **Sample JSON Output**:
  ```json
  [
    {
      "zone_id": "zone_1",
      "zone_name": "zone_1",
      "window_start": "2012-03-01 08:05:00",
      "window_end": "2012-03-01 08:10:00",
      "predicted_speed_kmph": 77.67,
      "status": "Optimal",
      "status_color": "#10b981",
      "traffic_record_count": 5,
      "latitude": 34.0275,
      "longitude": -118.277,
      "predicted_pm25": null,
      "model_provenance": "LinearRegression_MLlib"
    },
    {
      "zone_id": "zone_2",
      "zone_name": "zone_2",
      "window_start": "2012-03-01 08:05:00",
      "window_end": "2012-03-01 08:10:00",
      "predicted_speed_kmph": 81.03,
      "status": "Optimal",
      "status_color": "#10b981",
      "traffic_record_count": 5,
      "latitude": 34.0456,
      "longitude": -118.258,
      "predicted_pm25": null,
      "model_provenance": "LinearRegression_MLlib"
    }
  ]
  ```

### 2. `GET /api/route`
Returns optimal shortest path recommendations from Origin Node 1 (Civic Center) calculated by the Scala GraphX Pregel routing engine.

- **Request**: `GET http://127.0.0.1:8000/api/route`
- **Optional Query Parameter**: `?destination_id=15` (filters by destination intersection)
- **Response Format**: `application/json`
- **Status Codes**: `200 OK`, `404 Not Found`, `503 Service Unavailable`
- **Sample JSON Output**:
  ```json
  [
    {
      "origin_id": 1,
      "destination_id": 15,
      "path": [1, 2, 14, 16, 15],
      "path_formatted": "1 -> 2 -> 14 -> 16 -> 15",
      "total_travel_time_hours": 0.317,
      "total_travel_time_minutes": 19.02,
      "is_reachable": true,
      "computed_at": "2026-10-22T08:05:00",
      "prediction_zones_used": 5,
      "fallback_applied": false
    }
  ]
  ```

### 3. `GET /api/map`
Generates and returns an interactive HTML document compiled via Folium containing Leaflet.js map layers, OpenStreetMap tiles, zone centroid CircleMarkers colored by speed status, and click popups.

- **Request**: `GET http://127.0.0.1:8000/api/map`
- **Response Format**: `text/html; charset=utf-8`
- **Status Codes**: `200 OK`, `500 Internal Server Error`, `503 Service Unavailable`

---

## 7. Automated Verification & Testing

### Running the Pytest Suite
The dashboard includes 12 automated unit and contract tests verifying path resolution, Parquet filtering, coordinate calculation, status thresholds, REST endpoints, and mock-free compliance:

```powershell
.\dashboard\venv\Scripts\python.exe -m pytest dashboard/tests -v
```
**Expected Result**: `12 passed in < 2.50s`.

### Running the Live HTTP Smoke Test
With Uvicorn running, execute the standalone smoke verification script:

```powershell
.\dashboard\venv\Scripts\python.exe dashboard\tests\smoke_test.py
```
**Expected Result**: `ALL 6 LIVE ENDPOINT SMOKE TESTS PASSED!`.

### Manual PowerShell Smoke Test Commands
You can also inspect endpoints directly using PowerShell:

```powershell
# Test root HTML page
(Invoke-WebRequest -Uri "http://127.0.0.1:8000/" -UseBasicParsing).StatusCode

# Test predictions JSON
Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/predictions" | Format-Table zone_id, predicted_speed_kmph, status

# Test destination 15 route
Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/route?destination_id=15" | Select-Object origin_id, destination_id, total_travel_time_minutes, path_formatted
```

---

## 8. Frontend Auto-Refresh & Iframe Architecture

### Why an IFrame is Used Instead of `innerHTML`
A common mistake in Python/Folium web dashboards is attempting to inject Folium's HTML output directly into a `<div>` using `document.getElementById('map').innerHTML = responseHtml`.

**Why that fails**:
1. **W3C Script Execution Restriction**: The HTML5 specification dictates that `<script>` tags inserted into the DOM via `innerHTML` are **not executed** by the browser. Because Folium relies entirely on inline JavaScript to initialize Leaflet (`var map = L.map(...)`), the map container remains blank.
2. **DOM Pollution and Memory Leaks**: Leaflet registers global event listeners and window-level object registries. Swapping HTML via `innerHTML` fails to clean up previous Leaflet instances, causing severe memory leaks on repeated 15-second polling cycles.
3. **The Solution**: An `<iframe>` element (`<iframe id="map-frame" src="/api/map">`) isolates Leaflet scripts in their own browsing context. When refreshed via `mapFrame.src = '/api/map?t=' + Date.now()`, the browser cleanly tears down the previous window instance, runs garbage collection, and executes all Leaflet initialization scripts natively.

### Auto-Refresh Mechanics
- **Configurable Polling Interval**: `const REFRESH_INTERVAL_MS = 15000;` (15 seconds).
- **Concurrency Guard**: An `isRefreshing` boolean flag prevents overlapping fetch requests if a network request takes longer than the interval.
- **Cache Busting**: Every polling request appends `?t=Date.now()` to guarantee that browser caches do not serve stale responses.
- **Graceful Error Handling**: If the backend is temporarily unreachable, an alert banner appears without crashing the UI, and retries automatically on the next polling cycle.

---

## 9. Engineering Notes & Defense Concepts

### Zero In-Memory Caching (Academic Integrity)
Unlike toy web applications that load a DataFrame once at startup into a global variable (`df = pd.read_parquet(...)`), Urbanix adheres to strict distributed systems streaming principles:
- Every invocation of `GET /api/predictions` and `GET /api/route` scans the filesystem and reads Parquet files **afresh**.
- As the upstream Spark Structured Streaming or MLlib batch jobs publish new micro-batch partitions into `data/feature_table/`, the dashboard immediately reads and reflects the newest state on the next request.

### Polling vs. Changed Data
A successful 15-second HTTP polling request (`HTTP 200 OK`) demonstrates that the API server is healthy and Parquet data was successfully parsed. However:
- If upstream producers or streaming jobs have not emitted a new micro-batch window, the prediction values will remain identical to the previous window.
- The UI explicitly displays the **Data Window Timestamp** alongside the **Last Polled Time** so operators can distinguish between "dashboard is actively polling" and "new streaming data has arrived."

### Cross-Origin Resource Sharing (CORS)
- **Same-Origin Policy**: Because the frontend (`index.html`) is served directly by FastAPI from `http://127.0.0.1:8000/`, requests to `http://127.0.0.1:8000/api/...` share the exact same protocol (`http`), host (`127.0.0.1`), and port (`8000`). Under browser security rules, these are **same-origin** requests and do not require CORS headers.
- **Developer Flexibility**: To support potential decoupled frontend development (e.g., Live Server or local node tooling), `main.py` configures `CORSMiddleware` explicitly restricted to local origins (`localhost`, `127.0.0.1`). Wildcard (`*`) CORS is intentionally avoided to adhere to secure engineering practices.

### Parquet Filtering Architecture
Big data directory structures in Spark commonly contain non-data metadata files:
- `_SUCCESS` (zero-byte commit marker)
- `.*.crc` (checksum verification files)
- `README.md` (documentation markers)

Calling `pd.read_parquet("data/feature_table/traffic/")` directly causes PyArrow to fail with `ArrowInvalid: Parquet magic bytes not found in footer` because it attempts to parse `README.md` as Parquet. The Urbanix helper `get_parquet_files()` scans directories specifically for valid `*.parquet` files, skipping metadata and non-parquet files safely.

---

## 10. Data Provenance & Physical Constraints

1. **Replay vs. Real Hardware**: Sensor data stems from Caltrans Performance Measurement System (PeMS) METR-LA loop detectors replayed through Kafka streaming topics rather than physical field hardware deployed across the campus.
2. **Geospatial Coordinates**:
   - The road network contains 20 numbered intersections (`data/intersections.csv`) situated along major Los Angeles arteries (Interstates 10, 110, 101, and US-60).
   - Zone centroids for `zone_1` through `zone_5` are computed by averaging the physical coordinates of their respective constituent intersections.
3. **PM2.5 / Environmental Scope**: Per the Day 6 modeling decision, environmental AQI predictions were decoupled from the primary traffic speed regression workflow. `predicted_pm25` is returned honestly as `null` rather than fabricated with artificial numbers.

---

## 11. Troubleshooting & Diagnostics

| Symptom | Probable Cause | Diagnostic / Fix |
| :--- | :--- | :--- |
| **HTTP 503 "Predictions data not found"** | Feature table directory is missing or unpopulated. | Verify `data/feature_table/traffic/` exists and contains `*.parquet` files from Day 5/6. |
| **HTTP 503 "Recommended routes snapshot not found"** | GraphX routing engine has not published routes. | Verify `data/recommended_routes/` exists and contains routes from Day 8/9. |
| **Blank Map Container in Browser** | Browser blocked iframe or Leaflet JS CDN blocked. | Ensure internet access is available to fetch Leaflet CDN assets, or check browser developer console (`F12`). |
| **Port 8000 Already in Use** | Another Uvicorn or web process is occupying port 8000. | Launch on an alternative port: `--port 8080`. |
| **ImportError: No module named 'fastapi'** | Running with system Python instead of dashboard venv. | Run using `.\dashboard\venv\Scripts\python.exe` explicitly. |

---

## 12. Day 11 Handoff & Upcoming Scope

Day 10 establishes the robust geospatial backend, live heatmap, and auto-refreshing dashboard.

**Reserved for Day 11**:
1. **Interactive Route Overlays**: Rendering PolyLine paths on the Folium map directly connecting the node coordinates for chosen GraphX recommended routes (`1 -> 2 -> 14 -> 16 -> 15`).
2. **Live Congestion Reroute Demonstration**: Toggling between baseline routes and the synthetic congestion spike fixture (`data/feature_table_spike/` and `data/recommended_routes_spike/`).
3. **Fault Tolerance & Stale Data Badging**: Adding graceful UI indicators when streaming pipelines lag or Parquet files exceed maximum freshness windows.

---

*Urbanix — Intelligent Smart City Transport Resilience Platform | INT315 Cluster Computing*
