# ==============================================================================
# Urbanix — Day 10: Geospatial Dashboard Backend, Live Heatmap & APIs
# main.py — FastAPI Application Serving Live Parquet Predictions & Folium Map
# ==============================================================================
#
# 1. ARCHITECTURAL OVERVIEW:
# In Weekends 1-3 (Days 1-9), the Urbanix platform built an enterprise-grade distributed
# pipeline:
#   - Weekend 1 (Days 1-3): Kafka sensor producers -> Spark Structured Streaming -> Windowed Aggregations
#   - Weekend 2 (Days 4-6): MLlib Feature Engineering -> Linear Regression & Decision Tree Forecasting
#   - Weekend 3 (Days 7-9): Scala Spark GraphX Pregel Shortest-Path Routing Engine
#
# The output of this entire pipeline sits on disk in two decoupled Apache Parquet datasets:
#   - `data/feature_table/traffic/`: Windowed traffic observations and MLlib velocity forecasts per zone.
#   - `data/recommended_routes/`: Current optimal shortest paths and travel times computed by GraphX.
#
# Day 10 provides the visible face of Urbanix:
# A lightweight, asynchronous FastAPI web backend that reads these Parquet datasets FRESH on every
# HTTP request, builds a dynamic Folium Leaflet geospatial heatmap, and exposes clean REST APIs
# for automated browser polling.
#
# 2. WHY NO CACHING IN MEMORY (READING FRESH PARQUET):
# Evaluators and operators need to see live updates as the streaming and routing engines commit
# new micro-batches. If FastAPI cached a DataFrame at startup in a global variable, the web UI
# would remain frozen on stale data even while Spark commits new predictions.
# By executing a fast columnar Parquet scan on every request, the API reflects the genuine,
# current state of the filesystem at that millisecond.
#
# 3. UNDERSTANDING CORS (CROSS-ORIGIN RESOURCE SHARING):
# By default, modern browsers enforce the Same-Origin Policy (SOP) to protect users from malicious
# cross-site requests. An origin is defined by (Protocol + Domain + Port).
# In our architecture, the browser page (index.html) is served by FastAPI on http://localhost:8000/,
# and it fetches API endpoints on http://localhost:8000/api/*. Because the protocol, domain, and port
# are identical, these are SAME-ORIGIN requests, meaning CORS is technically not required!
# However, to support local frontend development (e.g. if a developer serves index.html via Live Server
# on port 5500 or port 3000), we configure restrictive local CORS allowing only localhost origins.
# We explicitly avoid dangerous wildcard `allow_origins=["*"]` in production code.
# ==============================================================================

import os
import glob
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
import folium
from folium.plugins import MiniMap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Urbanix-Dashboard] %(message)s",
)
logger = logging.getLogger("UrbanixDashboard")

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

# ------------------------------------------------------------------------------
# 1. Robust Path Resolution (Works from Repo Root OR dashboard/ Subdirectory)
# ------------------------------------------------------------------------------
# Resolve repository root dynamically based on this file's physical location:
# dashboard/main.py -> parent is dashboard/ -> parent.parent is urbanix repo root.
REPO_ROOT: Path = Path(__file__).resolve().parent.parent
DASHBOARD_DIR: Path = Path(__file__).resolve().parent
STATIC_DIR: Path = DASHBOARD_DIR / "static"

FEATURE_TABLE_DIR: Path = REPO_ROOT / "data" / "feature_table" / "traffic"
RECOMMENDED_ROUTES_DIR: Path = REPO_ROOT / "data" / "recommended_routes"
INTERSECTIONS_CSV: Path = REPO_ROOT / "data" / "intersections.csv"
ROADS_CSV: Path = REPO_ROOT / "data" / "roads.csv"

# ------------------------------------------------------------------------------
# 1b. Freshness Configuration Constants (Day 11 — Phase 3)
# ------------------------------------------------------------------------------
# Production data generation cadences:
# - Routes are recomputed approximately every 60 seconds by GraphX RouteRecommender.
# - Predictions are generated per 5-minute (300s) tumbling window by Spark Streaming.
# - A data source is considered STALE when its age reaches or exceeds 3x its production cadence.
ROUTES_UPDATE_INTERVAL_SECONDS: float = float(os.getenv("ROUTES_UPDATE_INTERVAL_SECONDS", "60.0"))
PREDICTIONS_UPDATE_INTERVAL_SECONDS: float = float(os.getenv("PREDICTIONS_UPDATE_INTERVAL_SECONDS", "300.0"))
STALE_THRESHOLD_MULTIPLIER: float = float(os.getenv("STALE_THRESHOLD_MULTIPLIER", "3.0"))

# Thresholds where age >= threshold is marked STALE (multiplier applied exactly once):
ROUTES_STALE_THRESHOLD_SECONDS: float = ROUTES_UPDATE_INTERVAL_SECONDS * STALE_THRESHOLD_MULTIPLIER  # 180.0s
PREDICTIONS_STALE_THRESHOLD_SECONDS: float = PREDICTIONS_UPDATE_INTERVAL_SECONDS * STALE_THRESHOLD_MULTIPLIER  # 900.0s

# ------------------------------------------------------------------------------
# 2. FastAPI Application Initialization & Middleware
# ------------------------------------------------------------------------------
app = FastAPI(
    title="Urbanix Smart City Geospatial Dashboard",
    description="Real-time traffic forecasting, zone congestion heatmaps, and GraphX shortest-path recommendations.",
    version="1.0.0"
)

# Configure restrictive local CORS (permits local dashboard testing across standard dev ports)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5500",
        "http://127.0.0.1:5500"
    ],
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------------------
# 3. Helper Functions: Data Ingestion & Transformation
# ------------------------------------------------------------------------------

def get_parquet_files(dir_path: Path) -> List[str]:
    """
    Safely discovers all real Parquet chunks in a directory while strictly ignoring
    ancillary files such as README.md, _SUCCESS markers, and checksum (.crc) files.
    
    Why this is critical:
    In distributed computing, Spark writes metadata (_SUCCESS, .crc) and documentation
    (README.md) into the output directory. Passing the directory directly to pyarrow
    without filtering triggers 'Parquet magic bytes not found' exceptions when it attempts
    to read markdown as columnar binary data.
    """
    if not dir_path.exists():
        return []
    
    # Target only *.parquet files that do not start with hidden dots or underscores
    files = [
        str(f) for f in dir_path.glob("*.parquet")
        if f.is_file() and not f.name.startswith(".") and not f.name.startswith("_")
    ]
    return sorted(files)


def load_zone_coordinates() -> Dict[str, Dict[str, float]]:
    """
    Computes honest, geographically verified zone center coordinates derived directly
    from the project's physical intersections (data/intersections.csv).
    
    Academic Rigor:
    Rather than hardcoding arbitrary coordinates, this function groups the 20 actual
    road network vertices by zone_id and computes the centroid (mean latitude, mean longitude).
    """
    if not INTERSECTIONS_CSV.exists():
        # Documented fallback if intersections.csv is absent
        return {
            "zone_1": {"lat": 34.0498, "lon": -118.2526, "name": "Downtown Core"},
            "zone_2": {"lat": 34.1820, "lon": -118.4880, "name": "San Fernando Valley"},
            "zone_3": {"lat": 34.1339, "lon": -118.0658, "name": "San Gabriel Foothills"},
            "zone_4": {"lat": 33.9601, "lon": -118.1885, "name": "South Central Corridor"},
            "zone_5": {"lat": 33.7645, "lon": -118.1832, "name": "Harbor & Outer Ring"},
        }
    
    try:
        df_int = pd.read_csv(INTERSECTIONS_CSV)
        centroids = df_int.groupby("zone_id").agg(
            center_lat=("latitude", "mean"),
            center_lon=("longitude", "mean")
        ).reset_index()
        
        coord_map = {}
        for _, row in centroids.iterrows():
            coord_map[str(row["zone_id"])] = {
                "lat": float(row["center_lat"]),
                "lon": float(row["center_lon"]),
            }
        return coord_map
    except Exception as ex:
        logger.warning(f"Failed to compute zone centroids from CSV: {ex}")
        return {}


def load_intersections_registry() -> Dict[int, Dict[str, Any]]:
    """
    Loads data/intersections.csv into an integer-keyed registry mapping each intersection ID
    to its exact physical attributes (name, zone_id, latitude, longitude).
    
    Academic Rigor:
    Rather than assuming coordinates or relying on mock locations, this function directly
    indexes the ground-truth physical intersection dataset established on Day 7.
    
    Returns:
        Dict[int, Dict[str, Any]]: Mapping of intersection ID -> {name, zone_id, lat, lon}
    """
    if not INTERSECTIONS_CSV.exists():
        logger.warning(f"Intersections CSV not found at {INTERSECTIONS_CSV}")
        return {}
    
    try:
        df_int = pd.read_csv(INTERSECTIONS_CSV)
        registry: Dict[int, Dict[str, Any]] = {}
        for _, row in df_int.iterrows():
            node_id = int(row["id"])
            registry[node_id] = {
                "name": str(row["name"]),
                "zone_id": str(row["zone_id"]),
                "lat": float(row["latitude"]),
                "lon": float(row["longitude"]),
            }
        return registry
    except Exception as ex:
        logger.error(f"Failed to load intersections registry from {INTERSECTIONS_CSV}: {ex}")
        return {}


def build_route_coordinates(
    path_nodes: Any,
    intersections_registry: Dict[int, Dict[str, Any]]
) -> List[Tuple[float, float]]:
    """
    Transforms a GraphX path (sequence of integer intersection IDs) into an ordered list
    of geographic coordinate pairs (latitude, longitude) for Folium PolyLine rendering.
    
    Safety & Data Integrity Rules (Rule D & FR-8):
    1. Rejects non-iterable, empty, or single-node paths (a valid route requires at least 2 vertices).
    2. Coerces each node ID safely to native integer (handling NumPy int64 cleanly).
    3. Validates that EVERY node in the path exists in the intersections registry.
    4. If ANY node coordinate is missing, NaN, infinite, or falls outside the valid
       geographic bounding box (32.0 <= lat <= 36.0, -120.0 <= lon <= -116.0), the function
       ABORTS and returns an empty list. It NEVER draws a truncated or misleading partial line.
    
    Returns:
        List[Tuple[float, float]]: Ordered list of (lat, lon) tuples.
    """
    if path_nodes is None:
        return []
    
    try:
        # Handles lists, tuples, and numpy.ndarray elements safely
        int_nodes = [int(n) for n in path_nodes]
    except (ValueError, TypeError):
        logger.warning(f"Malformed path elements in {path_nodes} - cannot convert to integers.")
        return []
    
    if len(int_nodes) < 2:
        return []
    
    coords: List[Tuple[float, float]] = []
    for node_id in int_nodes:
        if node_id not in intersections_registry:
            logger.warning(
                f"Intersection ID {node_id} from route path not found in intersections registry. "
                f"Aborting PolyLine generation to prevent drawing a misleading partial route."
            )
            return []
        
        info = intersections_registry[node_id]
        lat = info.get("lat")
        lon = info.get("lon")
        
        # Bounding box and numeric validity check (Greater Southern California / LA basin)
        if (
            lat is None 
            or lon is None 
            or not isinstance(lat, (int, float)) 
            or not isinstance(lon, (int, float))
            or pd.isna(lat) 
            or pd.isna(lon)
            or not (32.0 <= lat <= 36.0)
            or not (-120.0 <= lon <= -116.0)
        ):
            logger.warning(
                f"Invalid or out-of-bounds coordinates for intersection {node_id}: ({lat}, {lon}). "
                f"Aborting PolyLine generation."
            )
            return []
            
        coords.append((lat, lon))
        
    return coords


def determine_speed_status(speed_kmph: float) -> Dict[str, str]:
    """
    Maps effective traffic velocity to standard civil engineering Level of Service (LOS)
    and visual color codes:
      - Green  (> 60 km/h) : Free Flowing
      - Orange (30 - 60 km/h): Moderate Traffic / Approaching Saturation
      - Red    (< 30 km/h) : Congested / Breakdown
    """
    if speed_kmph >= 60.0:
        return {"status": "FREE_FLOWING", "color": "green", "hex": "#10B981"}
    elif speed_kmph >= 30.0:
        return {"status": "MODERATE", "color": "orange", "hex": "#F59E0B"}
    else:
        return {"status": "CONGESTED", "color": "red", "hex": "#EF4444"}


def evaluate_freshness(
    timestamp_val: Any,
    threshold_seconds: float,
    reference_time: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Evaluates whether a source data timestamp is fresh or stale against a configured threshold.
    
    Academic Engineering Contract & Timezone Rules (Day 11 — Phase 3):
    1. Reference Time: Defaults to current wall-clock UTC time (datetime.now(timezone.utc)).
       Accepts an explicit reference_time for deterministic, sleep-free unit testing.
    2. Missing Timestamps: If timestamp_val is None, empty string, or pd.isna/NaT,
       returns is_stale=True with status 'MISSING_TIMESTAMP'. Never silently marks missing data as fresh.
    3. Malformed Timestamps: If timestamp_val cannot be parsed by pd.to_datetime,
       returns is_stale=True with status 'MALFORMED_TIMESTAMP' without crashing the application.
    4. Timezone Safety:
       - Timezone-aware inputs are normalized to UTC.
       - Timezone-naive inputs are explicitly localized to UTC per documented repository convention.
    5. Future Timestamps / Clock Skew:
       - If timestamp is in the future by more than 5.0 seconds (age_seconds < -5.0),
         returns is_stale=True with status 'FUTURE_CLOCK_SKEW' to avoid negative age staying fresh indefinitely.
       - If timestamp is slightly in the future (-5.0 <= age_seconds < 0.0), treats age as 0.0s.
    6. Boundary Conditions:
       - Age strictly below threshold (age_seconds < threshold_seconds): is_stale=False, status 'FRESH'.
       - Age exactly equal to threshold (age_seconds == threshold_seconds): is_stale=True, status 'STALE'.
       - Age above threshold (age_seconds > threshold_seconds): is_stale=True, status 'STALE'.
       
    Returns:
        Dict[str, Any]: {
            "computed_at": Optional[str],  # ISO 8601 UTC string (e.g. '2012-03-01T08:10:00Z') or None
            "is_stale": bool,
            "age_seconds": Optional[float],
            "freshness_status": str  # 'FRESH', 'STALE', 'MISSING_TIMESTAMP', 'MALFORMED_TIMESTAMP', 'FUTURE_CLOCK_SKEW'
        }
    """
    if timestamp_val is None or (isinstance(timestamp_val, str) and not timestamp_val.strip()):
        return {
            "computed_at": None,
            "is_stale": True,
            "age_seconds": None,
            "freshness_status": "MISSING_TIMESTAMP"
        }
    
    try:
        if pd.isna(timestamp_val):
            return {
                "computed_at": None,
                "is_stale": True,
                "age_seconds": None,
                "freshness_status": "MISSING_TIMESTAMP"
            }
    except Exception:
        pass

    if reference_time is None:
        ref_utc = datetime.now(timezone.utc)
    else:
        if reference_time.tzinfo is None:
            ref_utc = reference_time.replace(tzinfo=timezone.utc)
        else:
            ref_utc = reference_time.astimezone(timezone.utc)

    try:
        ts = pd.to_datetime(timestamp_val)
        if pd.isna(ts):
            return {
                "computed_at": None,
                "is_stale": True,
                "age_seconds": None,
                "freshness_status": "MISSING_TIMESTAMP"
            }
    except Exception:
        return {
            "computed_at": None,
            "is_stale": True,
            "age_seconds": None,
            "freshness_status": "MALFORMED_TIMESTAMP"
        }

    try:
        if ts.tzinfo is None:
            ts_utc = ts.tz_localize("UTC")
        else:
            ts_utc = ts.tz_convert("UTC")
        dt_utc = ts_utc.to_pydatetime()
    except Exception:
        return {
            "computed_at": None,
            "is_stale": True,
            "age_seconds": None,
            "freshness_status": "MALFORMED_TIMESTAMP"
        }

    iso_str = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    diff_seconds = (ref_utc - dt_utc).total_seconds()

    if diff_seconds < -5.0:
        return {
            "computed_at": iso_str,
            "is_stale": True,
            "age_seconds": round(diff_seconds, 2),
            "freshness_status": "FUTURE_CLOCK_SKEW"
        }
    
    if diff_seconds < 0.0:
        diff_seconds = 0.0

    is_stale = bool(diff_seconds >= threshold_seconds)
    status = "STALE" if is_stale else "FRESH"

    return {
        "computed_at": iso_str,
        "is_stale": is_stale,
        "age_seconds": round(diff_seconds, 2),
        "freshness_status": status
    }


def read_latest_predictions_dataframe() -> pd.DataFrame:
    """
    Reads the production Parquet feature table afresh from disk and deterministically
    extracts the single latest prediction record per zone based on window_start descending.
    """
    parquet_files = get_parquet_files(FEATURE_TABLE_DIR)
    if not parquet_files:
        raise HTTPException(
            status_code=503,
            detail=f"Traffic feature store not found or empty at {FEATURE_TABLE_DIR}. Ensure Day 6 / Day 9 streaming job has persisted predictions."
        )

    try:
        df = pd.read_parquet(parquet_files)
    except Exception as ex:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Parquet feature store: {str(ex)}"
        )

    if df.empty or "zone_id" not in df.columns:
        raise HTTPException(
            status_code=500,
            detail="Traffic feature store is empty or missing required 'zone_id' column."
        )

    # Identify temporal ordering column (prefers window_start, falls back to window)
    time_col = "window_start" if "window_start" in df.columns else ("window" if "window" in df.columns else None)
    if time_col:
        df = df.sort_values(time_col, ascending=True)

    # Deterministically extract the single latest record per zone
    latest_df = df.groupby("zone_id", as_index=False).tail(1).copy()
    return latest_df


def read_recommended_routes_dataframe() -> pd.DataFrame:
    """
    Reads the production GraphX recommended routes snapshot afresh from disk.
    """
    parquet_files = get_parquet_files(RECOMMENDED_ROUTES_DIR)
    if not parquet_files:
        raise HTTPException(
            status_code=503,
            detail=f"Recommended routes snapshot not found at {RECOMMENDED_ROUTES_DIR}. Ensure RouteRecommender has published route recommendations."
        )

    try:
        df = pd.read_parquet(parquet_files)
    except Exception as ex:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read recommended routes Parquet snapshot: {str(ex)}"
        )

    if df.empty:
        raise HTTPException(
            status_code=500,
            detail="Recommended routes snapshot is empty."
        )

    return df.sort_values("destination_id", ascending=True)


# ------------------------------------------------------------------------------
# 4. REST API Endpoints
# ------------------------------------------------------------------------------

@app.get("/api/predictions", summary="Get Latest Zone Traffic Predictions")
async def get_predictions() -> List[Dict[str, Any]]:
    """
    Returns the latest velocity forecasts and operational metrics for all active zones.
    
    Academic Engineering Contract:
    - Reads genuine Parquet feature store on every request (zero in-memory caching).
    - Speed is extracted from 'predicted_speed_kmph', 'predicted_speed', or 'avg_speed_kmph'.
    - PM2.5 / AQI is explicitly returned as null (never fabricated; documented as deferred per Day 6).
    """
    df = read_latest_predictions_dataframe()
    centroids = load_zone_coordinates()

    # Determine which speed column exists in this feature table
    speed_col = "predicted_speed_kmph" if "predicted_speed_kmph" in df.columns else (
        "predicted_speed" if "predicted_speed" in df.columns else "avg_speed_kmph"
    )

    results = []
    for _, row in df.iterrows():
        zone_id = str(row["zone_id"])
        speed_val = float(row[speed_col]) if pd.notnull(row.get(speed_col)) else 0.0
        status_info = determine_speed_status(speed_val)

        # Coordinate resolution
        coords = centroids.get(zone_id, {"lat": 34.0200, "lon": -118.2900})
        
        # Window timestamp extraction
        win_start = str(row["window_start"]) if pd.notnull(row.get("window_start")) else None
        win_end = str(row["window_end"]) if pd.notnull(row.get("window_end")) else None
        zone_name = str(row["zone_name"]) if pd.notnull(row.get("zone_name")) else zone_id
        record_count = int(row["traffic_record_count"]) if pd.notnull(row.get("traffic_record_count")) else 0

        # Freshness evaluation based on latest completed window_end (Day 11 — Phase 3)
        win_end_val = row.get("window_end")
        freshness = evaluate_freshness(win_end_val, PREDICTIONS_STALE_THRESHOLD_SECONDS)

        results.append({
            "zone_id": zone_id,
            "zone_name": zone_name,
            "window_start": win_start,
            "window_end": win_end,
            "predicted_speed_kmph": round(speed_val, 2),
            "status": status_info["status"],
            "status_color": status_info["color"],
            "traffic_record_count": record_count,
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "predicted_pm25": None,  # Explicitly None (deferred environmental scope)
            "model_provenance": str(row.get("prediction_model", "LinearRegression_MLlib")),
            # Freshness Metadata
            "computed_at": freshness["computed_at"],
            "is_stale": freshness["is_stale"],
            "age_seconds": freshness["age_seconds"],
            "freshness_status": freshness["freshness_status"]
        })

    return sorted(results, key=lambda x: x["zone_id"])


@app.get("/api/route", summary="Get Current Recommended Routes Snapshot")
async def get_route(destination_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Returns the current optimal shortest path recommendations from Origin 1 (Civic Center)
    computed by the Scala Spark GraphX Pregel routing engine.
    
    Optional query parameter:
    - destination_id: Filter to a specific destination intersection (e.g. 8 or 15).
    """
    df = read_recommended_routes_dataframe()

    if destination_id is not None:
        df = df[df["destination_id"] == destination_id]
        if df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Destination ID {destination_id} not found in current route snapshot."
            )

    results = []
    for _, row in df.iterrows():
        # Path array conversion to native Python int (ensures clean JSON serialization)
        raw_path = row.get("path")
        path_list = [int(node) for node in raw_path] if hasattr(raw_path, "__iter__") else []

        # Freshness evaluation based on genuine route snapshot computed_at (Day 11 — Phase 3)
        route_freshness = evaluate_freshness(row.get("computed_at"), ROUTES_STALE_THRESHOLD_SECONDS)

        results.append({
            "origin_id": int(row["origin_id"]),
            "destination_id": int(row["destination_id"]),
            "path": path_list,
            "path_formatted": str(row.get("path_formatted", "")),
            "total_travel_time_hours": float(row.get("total_travel_time_hours", 0.0)),
            "total_travel_time_minutes": float(row.get("total_travel_time_minutes", 0.0)),
            "is_reachable": bool(row.get("is_reachable", True)),
            "computed_at": route_freshness["computed_at"] or str(row.get("computed_at", "")),
            "prediction_zones_used": int(row.get("prediction_zones_used", 0)),
            "fallback_applied": bool(row.get("fallback_applied", False)),
            # Freshness Metadata
            "is_stale": route_freshness["is_stale"],
            "age_seconds": route_freshness["age_seconds"],
            "freshness_status": route_freshness["freshness_status"]
        })

    return results


@app.get("/api/map", response_class=HTMLResponse, summary="Render Live Folium Heatmap with Route Overlay")
async def get_map(destination_id: Optional[int] = 15) -> str:
    """
    Generates and returns an interactive Folium Leaflet HTML map document freshly constructed
    from the latest Parquet predictions and intersection coordinates, overlaid with the
    genuine GraphX shortest path recommended route.
    
    Features:
    - Base Tile: OpenStreetMap with clean typography.
    - Centered on the greater Los Angeles metropolitan road network (34.018° N, -118.236° W).
    - 5 Zone Centroid CircleMarkers colored dynamically by live predicted velocity.
    - 20 Physical intersection markers with zone affiliation.
    - Genuine GraphX Recommended Route PolyLine connecting ordered intersection coordinates (FR-8).
    - Prominent Origin Node 1 and Destination Node markers.
    - Clean visual map legend explaining speed thresholds and route overlay.
    """
    predictions_df = read_latest_predictions_dataframe()
    centroids = load_zone_coordinates()

    speed_col = "predicted_speed_kmph" if "predicted_speed_kmph" in predictions_df.columns else (
        "predicted_speed" if "predicted_speed" in predictions_df.columns else "avg_speed_kmph"
    )

    # Initialize Folium Map centered across greater Los Angeles
    m = folium.Map(
        location=[34.0180, -118.2356],
        zoom_start=11,
        tiles="OpenStreetMap",
        control_scale=True
    )

    # 1. Plot Zone Heatmap Circles
    for _, row in predictions_df.iterrows():
        zone_id = str(row["zone_id"])
        zone_name = str(row.get("zone_name", zone_id))
        speed_val = float(row[speed_col]) if pd.notnull(row.get(speed_col)) else 0.0
        status_info = determine_speed_status(speed_val)
        win_start = str(row.get("window_start", "N/A"))

        coords = centroids.get(zone_id, {"lat": 34.0200, "lon": -118.2900})

        # Structured HTML popup
        popup_html = f"""
        <div style="font-family: Arial, sans-serif; min-width: 180px; padding: 4px;">
            <h4 style="margin: 0 0 6px 0; color: #1F2937; border-bottom: 2px solid {status_info['hex']}; padding-bottom: 4px;">
                {zone_name}
            </h4>
            <table style="width: 100%; font-size: 12px; line-height: 1.5;">
                <tr><td><b>Zone ID:</b></td><td><code>{zone_id}</code></td></tr>
                <tr><td><b>Forecast Speed:</b></td><td><b style="color: {status_info['hex']};">{speed_val:.1f} km/h</b></td></tr>
                <tr><td><b>Traffic State:</b></td><td>{status_info['status']}</td></tr>
                <tr><td><b>Window:</b></td><td><small>{win_start[:19] if win_start else 'N/A'}</small></td></tr>
            </table>
        </div>
        """

        # Outer pulsing circle representing zone influence
        folium.Circle(
            location=[coords["lat"], coords["lon"]],
            radius=4500,
            color=status_info["color"],
            fill=True,
            fill_color=status_info["color"],
            fill_opacity=0.25,
            weight=2,
            tooltip=f"{zone_name}: {speed_val:.1f} km/h ({status_info['status']})"
        ).add_to(m)

        # Core marker with click popup
        folium.CircleMarker(
            location=[coords["lat"], coords["lon"]],
            radius=12,
            color="#FFFFFF",
            weight=2,
            fill=True,
            fill_color=status_info["color"],
            fill_opacity=0.9,
            popup=folium.Popup(popup_html, max_width=250)
        ).add_to(m)

    # 2. Add Physical Intersections (Road Network Nodes)
    if INTERSECTIONS_CSV.exists():
        try:
            df_int = pd.read_csv(INTERSECTIONS_CSV)
            for _, i_row in df_int.iterrows():
                i_id = int(i_row["id"])
                i_name = str(i_row["name"])
                i_zone = str(i_row["zone_id"])
                i_lat = float(i_row["latitude"])
                i_lon = float(i_row["longitude"])

                folium.CircleMarker(
                    location=[i_lat, i_lon],
                    radius=4,
                    color="#3B82F6" if i_id == 1 else "#6B7280",
                    fill=True,
                    fill_color="#3B82F6" if i_id == 1 else "#9CA3AF",
                    fill_opacity=0.8,
                    weight=1,
                    tooltip=f"Intersection {i_id}: {i_name} ({i_zone})"
                ).add_to(m)
        except Exception as ex:
            logger.warning(f"Could not plot intersections: {ex}")

    # 3. Add Recommended Route PolyLine (FR-8: Dynamic Route Overlay)
    intersections_reg = load_intersections_registry()
    if destination_id is not None and destination_id > 0:
        try:
            routes_df = read_recommended_routes_dataframe()
            target_route = routes_df[routes_df["destination_id"] == destination_id]
            if not target_route.empty:
                r_row = target_route.iloc[0]
                is_reachable = bool(r_row.get("is_reachable", True))
                if is_reachable:
                    raw_path = r_row.get("path")
                    route_coords = build_route_coordinates(raw_path, intersections_reg)
                    if route_coords:
                        eta_min = float(r_row.get("total_travel_time_minutes", 0.0))
                        orig_id = int(r_row.get("origin_id", 1))
                        dest_id = int(r_row.get("destination_id", destination_id))
                        path_str = str(r_row.get("path_formatted", f"{raw_path}"))

                        # Freshness evaluation for recommended route (Day 11 — Phase 3)
                        route_freshness = evaluate_freshness(r_row.get("computed_at"), ROUTES_STALE_THRESHOLD_SECONDS)
                        is_route_stale = route_freshness["is_stale"]
                        route_age = route_freshness["age_seconds"]
                        route_age_str = f"{route_age:.0f}s" if route_age is not None else "Unknown"

                        if is_route_stale:
                            route_color = "#6B7280"  # Gray PolyLine for stale route
                            route_tooltip = f"STALE ROUTE: Recommended Route Origin {orig_id} &rarr; Destination {dest_id} (ETA: {eta_min:.1f} min | Age: {route_age_str})"
                            stale_badge = f"""
                            <div style="background: #FEE2E2; border: 1px solid #EF4444; color: #991B1B; padding: 4px 8px; border-radius: 4px; font-weight: 600; font-size: 11px; margin-bottom: 8px;">
                                ⚠️ STALE ROUTE: Snapshot age ({route_age_str}) &ge; {ROUTES_STALE_THRESHOLD_SECONDS:.0f}s threshold
                            </div>
                            """
                        else:
                            route_color = "#2563EB"  # Royal blue for fresh route
                            route_tooltip = f"Recommended Route: Origin {orig_id} &rarr; Destination {dest_id} (ETA: {eta_min:.1f} min | Fresh)"
                            stale_badge = f"""
                            <div style="background: #ECFDF5; border: 1px solid #10B981; color: #065F46; padding: 4px 8px; border-radius: 4px; font-weight: 600; font-size: 11px; margin-bottom: 8px;">
                                ✅ FRESH ROUTE: GraphX optimal path (Age: {route_age_str})
                            </div>
                            """

                        route_popup_html = f"""
                        <div style="font-family: Arial, sans-serif; min-width: 220px; padding: 4px;">
                            <h4 style="margin: 0 0 6px 0; color: {'#374151' if is_route_stale else '#1E40AF'}; border-bottom: 2px solid {route_color}; padding-bottom: 4px;">
                                Recommended Route (Pregel SSSP) {'&mdash; <span style="color: #DC2626;">STALE</span>' if is_route_stale else ''}
                            </h4>
                            {stale_badge}
                            <table style="width: 100%; font-size: 12px; line-height: 1.5;">
                                <tr><td><b>Origin:</b></td><td>Node {orig_id} (Civic Center)</td></tr>
                                <tr><td><b>Destination:</b></td><td>Node {dest_id}</td></tr>
                                <tr><td><b>Path:</b></td><td><code>{path_str}</code></td></tr>
                                <tr><td><b>Optimal ETA:</b></td><td><b style="color: {route_color};">{eta_min:.2f} mins</b></td></tr>
                                <tr><td><b>Freshness:</b></td><td><b>{'STALE' if is_route_stale else 'FRESH'}</b></td></tr>
                            </table>
                        </div>
                        """

                        # Draw PolyLine along ordered intersection coordinates
                        folium.PolyLine(
                            locations=route_coords,
                            color=route_color,
                            weight=5,
                            opacity=0.85,
                            tooltip=route_tooltip,
                            popup=folium.Popup(route_popup_html, max_width=290)
                        ).add_to(m)

                        # Prominent Origin Indicator (Node 1)
                        folium.CircleMarker(
                            location=route_coords[0],
                            radius=8,
                            color="#1E40AF",
                            weight=3,
                            fill=True,
                            fill_color="#3B82F6",
                            fill_opacity=1.0,
                            tooltip=f"Route Origin: Node {orig_id}"
                        ).add_to(m)

                        # Prominent Destination Pin Marker
                        folium.CircleMarker(
                            location=route_coords[-1],
                            radius=8,
                            color="#991B1B" if not is_route_stale else "#4B5563",
                            weight=3,
                            fill=True,
                            fill_color="#EF4444" if not is_route_stale else "#6B7280",
                            fill_opacity=1.0,
                            tooltip=f"Route Destination: Node {dest_id} (ETA: {eta_min:.1f}m)"
                        ).add_to(m)
                    else:
                        logger.warning(
                            f"build_route_coordinates returned empty for destination {destination_id}. "
                            f"Skipping route overlay to prevent drawing a misleading partial route."
                        )
                else:
                    logger.info(f"Destination {destination_id} is flagged as unreachable in route snapshot.")
            else:
                logger.info(f"Destination {destination_id} not found in route snapshot.")
        except Exception as ex:
            # Graceful degradation: If route snapshot cannot be loaded or is missing,
            # log diagnostic and continue rendering heatmap without crashing.
            logger.warning(f"Could not load or render route overlay for destination {destination_id}: {ex}")

    # 4. Add Custom Floating Map Legend
    legend_html = """
    <div style="
        position: fixed;
        bottom: 25px;
        right: 25px;
        z-index: 1000;
        background: rgba(255, 255, 255, 0.95);
        padding: 12px 16px;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        font-size: 12px;
        line-height: 1.6;
        border-left: 4px solid #3B82F6;
    ">
        <b style="font-size: 13px; color: #111827;">Urbanix Traffic Legend</b><br>
        <div style="display: flex; align-items: center; margin-top: 4px;">
            <span style="height: 12px; width: 12px; background-color: #10B981; border-radius: 50%; display: inline-block; margin-right: 8px;"></span>
            <span>Free Flowing (&ge; 60 km/h)</span>
        </div>
        <div style="display: flex; align-items: center; margin-top: 4px;">
            <span style="height: 12px; width: 12px; background-color: #F59E0B; border-radius: 50%; display: inline-block; margin-right: 8px;"></span>
            <span>Moderate (30 &ndash; 60 km/h)</span>
        </div>
        <div style="display: flex; align-items: center; margin-top: 4px;">
            <span style="height: 12px; width: 12px; background-color: #EF4444; border-radius: 50%; display: inline-block; margin-right: 8px;"></span>
            <span>Congested (&lt; 30 km/h)</span>
        </div>
        <div style="display: flex; align-items: center; margin-top: 4px;">
            <span style="height: 4px; width: 16px; background-color: #2563EB; display: inline-block; margin-right: 8px;"></span>
            <span>Recommended Route (Pregel SSSP) [Fresh]</span>
        </div>
        <div style="display: flex; align-items: center; margin-top: 4px;">
            <span style="height: 4px; width: 16px; background-color: #6B7280; display: inline-block; margin-right: 8px;"></span>
            <span>Recommended Route (Pregel SSSP) [STALE]</span>
        </div>
        <div style="display: flex; align-items: center; margin-top: 4px;">
            <span style="height: 8px; width: 8px; background-color: #3B82F6; border-radius: 50%; display: inline-block; margin-right: 10px; margin-left: 2px;"></span>
            <span style="color: #6B7280;">Node 1: Dispatch Origin</span>
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # Render complete HTML document
    return m.get_root().render()


# ------------------------------------------------------------------------------
# 5. Static Files & Root Dashboard Serving
# ------------------------------------------------------------------------------

@app.get("/", response_class=FileResponse, summary="Serve Browser Dashboard")
async def serve_dashboard():
    """
    Serves the primary auto-refreshing Urbanix browser dashboard.
    """
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard frontend not found at {index_file}."
        )
    return FileResponse(index_file)

# Mount static files directory for CSS/assets if needed
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ------------------------------------------------------------------------------
# 6. Local Server Execution Hook
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    print("=" * 80)
    print("  URBANIX DAY 10: GEOSPATIAL DASHBOARD BACKEND")
    print("  Starting Uvicorn web server at http://localhost:8000")
    print("=" * 80)
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
