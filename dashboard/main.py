# ==============================================================================
# 🌟 URBANIX — GEOSPATIAL DASHBOARD BACKEND & REST API SERVICE
# main.py — Asynchronous FastAPI Web Server, Real-Time Parquet Reader & Folium Map Builder
# ==============================================================================
# Welcome to the main backend engine of the Urbanix Dashboard! 👋
#
# WHAT IS THIS FILE FOR?
# This Python file is the "brain" and web server of our dashboard.
# In web development, frontends (like HTML and JavaScript in the browser) need a way
# to talk to data on the computer's hard drive. That's where this backend comes in!
# It runs a FastAPI web application that:
#   1. Reads real-time traffic forecasts from Apache Spark (stored in Parquet files).
#   2. Reads optimal emergency vehicle routes from Scala GraphX (also in Parquet files).
#   3. Determines whether data is "Fresh" or "Stale" using strict time formulas.
#   4. Renders an interactive, dynamic Folium Leaflet geospatial heatmap.
#   5. Serves clean, safe JSON REST API endpoints (/api/predictions, /api/route, /api/map).
#
# ------------------------------------------------------------------------------
# 🎓 3 CORE ARCHITECTURAL CONCEPTS EVERY BEGINNER SHOULD KNOW:
# ------------------------------------------------------------------------------
# 1. WHAT IS FASTAPI?
#    FastAPI is one of the fastest, most modern Python web frameworks in the world.
#    It lets us build REST APIs with minimal code and automatically validates inputs!
#
# 2. WHY ZERO IN-MEMORY CACHING? (READING FRESH PARQUET ON EVERY REQUEST):
#    In big data streaming, Apache Spark and Scala GraphX continuously write new
#    predictions to the disk. If we loaded data into memory once when the server started,
#    the dashboard would be frozen on old data forever! By reading the Parquet files
#    fresh on every request, operators always see the true, living state of the city!
#
# 3. WHAT IS CORS (CROSS-ORIGIN RESOURCE SHARING)?
#    Web browsers have a security rule: a web page on port 3000 cannot talk to an API
#    on port 8000 unless the API explicitly allows it! We configure CORS middleware
#    so local developer tools (like React, Vite, or Live Server) can safely communicate.
# ==============================================================================

# ==============================================================================
# 📦 STEP 1: IMPORT STANDARD PYTHON LIBRARIES
# ==============================================================================

# 'os' provides tools to interact with the Operating System (e.g. reading environment variables).
import os

# 'glob' is a pattern-matching tool for finding file paths (e.g. searching for "*.parquet").
import glob

# 'logging' is Python's tool for recording diagnostic messages, warnings, and errors.
import logging

# 'Enum' (Enumeration) allows us to define fixed sets of named symbolic constants (like Error Codes).
from enum import Enum

# 'datetime' and 'timezone' are essential for manipulating timestamps and working with UTC clocks.
from datetime import datetime, timezone

# 'pathlib.Path' is the modern, cross-platform way to handle file and folder paths in Python.
from pathlib import Path

# 'typing' adds type hints to Python functions, making code easier to read and debug for beginners!
# For example: List[str] means a list of strings; Optional[int] means an integer or None.
from typing import List, Dict, Any, Optional, Tuple

# ==============================================================================
# 📦 STEP 2: IMPORT THIRD-PARTY BIG DATA & MAPPING LIBRARIES
# ==============================================================================

# 'pandas' is Python's premier data analysis library for reading and processing tabular data.
import pandas as pd

# 'folium' is a Python library that generates interactive, draggable Leaflet.js maps in HTML!
import folium
from folium.plugins import MiniMap

# ==============================================================================
# 📦 STEP 3: CONFIGURE SYSTEM LOGGING
# ==============================================================================
# This sets up a clean, uniform logging format so messages in the terminal look professional:
# Example: "2026-10-23 22:00:00,123 [INFO] [Urbanix-Dashboard] Starting web server..."
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Urbanix-Dashboard] %(message)s",
)
# Create a dedicated logger instance for our dashboard application
logger = logging.getLogger("UrbanixDashboard")

# ==============================================================================
# 📦 STEP 4: IMPORT FASTAPI WEB SERVER MODULES
# ==============================================================================
# 'FastAPI' is the main application class.
# 'HTTPException' allows us to raise HTTP errors (like 404 Not Found or 503 Unavailable).
# 'Request' represents the incoming HTTP request from the browser.
from fastapi import FastAPI, HTTPException, Request

# 'RequestValidationError' is raised when a user passes the wrong data type (e.g. text instead of a number).
from fastapi.exceptions import RequestValidationError

# 'CORSMiddleware' handles browser security permissions across different ports.
from fastapi.middleware.cors import CORSMiddleware

# Custom response classes telling the browser what format we are returning (HTML, Files, or JSON).
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse

# 'StaticFiles' allows FastAPI to serve images, CSS stylesheets, and client JavaScript directly from a folder.
from fastapi.staticfiles import StaticFiles

# Starlette is the ultra-fast underlying ASGI toolkit that powers FastAPI.
from starlette.exceptions import HTTPException as StarletteHTTPException


# ==============================================================================
# 📂 STEP 5: ROBUST DIRECTORY PATH RESOLUTION
# ==============================================================================
# Beginner Tip: Hardcoding paths like "C:/Users/.../data" is bad practice because
# the code will break on someone else's computer.
# Instead, we find the path relative to THIS file's physical location:
#   dashboard/main.py -> .parent is 'dashboard/' -> .parent.parent is the 'urbanix' project root!
REPO_ROOT: Path = Path(__file__).resolve().parent.parent
DASHBOARD_DIR: Path = Path(__file__).resolve().parent
STATIC_DIR: Path = DASHBOARD_DIR / "static"

# Concrete paths to project datasets:
FEATURE_TABLE_DIR: Path = REPO_ROOT / "data" / "feature_table" / "traffic"
RECOMMENDED_ROUTES_DIR: Path = REPO_ROOT / "data" / "recommended_routes"
INTERSECTIONS_CSV: Path = REPO_ROOT / "data" / "intersections.csv"
ROADS_CSV: Path = REPO_ROOT / "data" / "roads.csv"


# ==============================================================================
# ⏱️ STEP 6: DATA FRESHNESS CONFIGURATION CONSTANTS
# ==============================================================================
# In smart cities, sensor data ages quickly. We establish strict freshness thresholds:
#   - Routes are recomputed by Scala GraphX every 60 seconds (1 minute).
#   - Predictions are computed by Spark Streaming every 300 seconds (5 minutes).
#   - Rule of Thumb: Data is considered STALE when it is 3 times (3x) older than its normal schedule!
ROUTES_UPDATE_INTERVAL_SECONDS: float = float(os.getenv("ROUTES_UPDATE_INTERVAL_SECONDS", "60.0"))
PREDICTIONS_UPDATE_INTERVAL_SECONDS: float = float(os.getenv("PREDICTIONS_UPDATE_INTERVAL_SECONDS", "300.0"))
STALE_THRESHOLD_MULTIPLIER: float = float(os.getenv("STALE_THRESHOLD_MULTIPLIER", "3.0"))

# Thresholds where age >= threshold is marked STALE:
# Routes threshold:      60s  * 3.0 = 180.0s (3 minutes)
# Predictions threshold: 300s * 3.0 = 900.0s (15 minutes)
ROUTES_STALE_THRESHOLD_SECONDS: float = ROUTES_UPDATE_INTERVAL_SECONDS * STALE_THRESHOLD_MULTIPLIER
PREDICTIONS_STALE_THRESHOLD_SECONDS: float = PREDICTIONS_UPDATE_INTERVAL_SECONDS * STALE_THRESHOLD_MULTIPLIER


# ==============================================================================
# 🏷️ STEP 7: ERROR TAXONOMY ENUMERATION
# ==============================================================================
# An Enum groups related symbolic names together. This prevents typos and guarantees
# that our API returns standardized machine-readable error codes to frontend clients.
class ErrorCode(str, Enum):
    DATA_SOURCE_UNAVAILABLE = "DATA_SOURCE_UNAVAILABLE"  # HTTP 503: Parquet folder missing
    ROUTES_UNAVAILABLE = "ROUTES_UNAVAILABLE"            # HTTP 503: Routes folder missing
    DATA_PARSING_ERROR = "DATA_PARSING_ERROR"            # HTTP 500: Parquet file corrupted
    INVALID_DATA_SCHEMA = "INVALID_DATA_SCHEMA"          # HTTP 500: Missing expected columns
    DESTINATION_NOT_FOUND = "DESTINATION_NOT_FOUND"      # HTTP 404: Node ID does not exist
    INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"      # HTTP 500: Unexpected internal crash
    UNPROCESSABLE_ENTITY = "UNPROCESSABLE_ENTITY"        # HTTP 422: Wrong query type (e.g. text for int)
    NOT_FOUND = "NOT_FOUND"                              # HTTP 404: Generic endpoint not found


# ==============================================================================
# 🚀 STEP 8: FASTAPI APPLICATION INITIALIZATION & MIDDLEWARE
# ==============================================================================
# Create the FastAPI app instance with metadata for Swagger documentation
app = FastAPI(
    title="Urbanix Smart City Geospatial Dashboard",
    description="Real-time traffic forecasting, zone congestion heatmaps, and GraphX shortest-path recommendations.",
    version="1.0.0"
)

# Configure CORS Middleware:
# Allows browsers connecting from localhost ports (8000, 3000, 5500) to fetch API data
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
    allow_methods=["GET", "OPTIONS"], # Only allow safe read-only HTTP methods
    allow_headers=["*"],
)


# ==============================================================================
# 🛡️ STEP 9: CENTRALIZED EXCEPTION HANDLERS (ERROR WRAPPERS)
# ==============================================================================
# These handlers intercept errors across the entire app and format them into
# clean, predictable JSON responses without leaking computer paths or secret stack traces!

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    """
    Standardizes all HTTP exceptions (like 404, 503, 500) into a clean JSON envelope:
    {
        "error": "DESTINATION_NOT_FOUND",
        "message": "Destination ID 999 not found",
        "detail": "...",
        "status_code": 404
    }
    """
    if isinstance(exc.detail, dict):
        error_code = exc.detail.get("error", "HTTP_ERROR")
        message = exc.detail.get("message", "An HTTP error occurred.")
        detail_msg = exc.detail.get("detail", message)
    else:
        detail_msg = str(exc.detail)
        if exc.status_code == 404:
            error_code = ErrorCode.DESTINATION_NOT_FOUND if "destination" in detail_msg.lower() else ErrorCode.NOT_FOUND
        elif exc.status_code == 422:
            error_code = ErrorCode.UNPROCESSABLE_ENTITY
        elif exc.status_code == 503:
            error_code = ErrorCode.DATA_SOURCE_UNAVAILABLE
        elif exc.status_code == 500:
            error_code = ErrorCode.INTERNAL_SERVER_ERROR
        else:
            error_code = "HTTP_ERROR"
        message = detail_msg

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": error_code,
            "message": message,
            "detail": detail_msg,
            "status_code": exc.status_code
        }
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Standardizes input validation errors (HTTP 422).
    For example: if a user visits /api/route?destination_id=banana instead of a number!
    """
    return JSONResponse(
        status_code=422,
        content={
            "error": ErrorCode.UNPROCESSABLE_ENTITY,
            "message": "Validation error for request parameters.",
            "detail": exc.errors(),
            "status_code": 422
        }
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """
    Ultimate Safety Net:
    If an unexpected Python bug occurs anywhere, this handler catches it.
    It logs the full error details privately to the server terminal,
    and returns a polite, sanitized HTTP 500 message to the user without exposing code secrets!
    """
    logger.critical(
        f"Unhandled internal exception during {request.method} {request.url.path}: {exc}",
        exc_info=True
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": ErrorCode.INTERNAL_SERVER_ERROR,
            "message": "An unexpected internal server error occurred.",
            "detail": "Internal server error.",
            "status_code": 500
        }
    )


# ==============================================================================
# 🛠️ STEP 10: HELPER FUNCTIONS (DATA INGESTION, PARSING & FRESHNESS)
# ==============================================================================

def get_parquet_files(dir_path: Path) -> List[str]:
    """
    Safely discovers all genuine Parquet data chunks in a directory.
    
    Why this is critical for beginners to understand:
    When Apache Spark writes Parquet files, it also creates helper files:
      - '_SUCCESS' (empty zero-byte marker file)
      - '.crc' hidden checksum validation files
      - 'README.md' notes
    If you tell pandas to read everything in the folder, it will try to read README.md
    as a Parquet file and crash with "magic bytes not found"!
    This function ignores those helper files and returns ONLY genuine *.parquet data files.
    """
    if not dir_path.exists():
        return []

    # Find files ending in .parquet that don't start with '.' (hidden) or '_' (metadata)
    files = [
        str(f) for f in dir_path.glob("*.parquet")
        if f.is_file() and not f.name.startswith(".") and not f.name.startswith("_")
    ]
    return sorted(files)


def load_zone_coordinates() -> Dict[str, Dict[str, float]]:
    """
    Computes authentic geographic center coordinates (centroids) for our 5 urban zones.
    
    Academic Rigor:
    Rather than guessing or hardcoding random map coordinates, this function reads
    'data/intersections.csv', groups all 20 physical street intersections by zone,
    and calculates the exact mathematical average latitude and longitude for each zone!
    """
    if not INTERSECTIONS_CSV.exists():
        # Fallback dictionary if the CSV file hasn't been generated yet
        return {
            "zone_1": {"lat": 34.0498, "lon": -118.2526, "name": "Downtown Core"},
            "zone_2": {"lat": 34.1820, "lon": -118.4880, "name": "San Fernando Valley"},
            "zone_3": {"lat": 34.1339, "lon": -118.0658, "name": "San Gabriel Foothills"},
            "zone_4": {"lat": 33.9601, "lon": -118.1885, "name": "South Central Corridor"},
            "zone_5": {"lat": 33.7645, "lon": -118.1832, "name": "Harbor & Outer Ring"},
        }

    try:
        # Read the intersections CSV file with pandas
        df_int = pd.read_csv(INTERSECTIONS_CSV)
        # Group by 'zone_id' and calculate the mean (average) latitude and longitude
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
    Loads 'data/intersections.csv' into a Python dictionary keyed by intersection ID (1 to 20).
    
    Returns:
        Dict[int, Dict[str, Any]]: Example: { 1: {"name": "Grand Ave & 1st St", "lat": 34.0549, "lon": -118.2498} }
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
    Transforms a sequence of intersection node IDs (like [1, 2, 14, 16, 15])
    into an ordered list of geographic coordinate pairs: [(lat1, lon1), (lat2, lon2), ...].
    These coordinates are used by Folium to draw the route vector line on the map!
    
    Data Integrity & Safety Rules:
      1. A path must contain at least 2 intersections to draw a line between them.
      2. If ANY node in the route is missing from the registry, abort immediately!
         (We NEVER want to draw a broken or misleading partial route).
      3. Verifies coordinates fall inside Greater Southern California (32°N-36°N, -120°W to -116°W).
    """
    if path_nodes is None:
        return []

    try:
        # Convert path elements safely into standard Python integers
        int_nodes = [int(n) for n in path_nodes]
    except (ValueError, TypeError):
        logger.warning(f"Malformed path elements in {path_nodes} - cannot convert to integers.")
        return []

    # Need at least two points to connect a line
    if len(int_nodes) < 2:
        return []

    coords: List[Tuple[float, float]] = []
    for node_id in int_nodes:
        # Safety Check: Node must exist in our registry
        if node_id not in intersections_registry:
            logger.warning(
                f"Intersection ID {node_id} from route path not found in intersections registry. "
                f"Aborting PolyLine generation to prevent drawing a misleading partial route."
            )
            return []

        info = intersections_registry[node_id]
        lat = info.get("lat")
        lon = info.get("lon")

        # Sanity check: Ensure latitude and longitude are valid numbers within Los Angeles bounds
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
    Level of Service (LOS) Speed Classifier:
    Translates raw speed numbers (km/h) into traffic status and colors:
      - Green  (>= 60 km/h)   : FREE_FLOWING (Traffic is moving smoothly)
      - Orange (30 to 60 km/h): MODERATE (Traffic is approaching congestion)
      - Red    (< 30 km/h)    : CONGESTED (Heavy gridlock / slowdown)
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
    Evaluates whether a dataset's timestamp is FRESH or STALE.
    
    How it works:
      1. Converts the timestamp into UTC (Coordinated Universal Time).
      2. Compares it against the current UTC clock (or an explicit reference_time).
      3. Calculates data age: diff_seconds = (Current Time - Data Time).
      4. If age >= threshold_seconds, flags as STALE; otherwise FRESH.
      5. Handles edge cases: missing timestamps, unparseable dates, and future clock skew!
    """
    # Case 1: Missing or blank timestamp
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

    # Reference Time: Default to current wall-clock UTC time
    if reference_time is None:
        ref_utc = datetime.now(timezone.utc)
    else:
        if reference_time.tzinfo is None:
            ref_utc = reference_time.replace(tzinfo=timezone.utc)
        else:
            ref_utc = reference_time.astimezone(timezone.utc)

    # Parse timestamp into a datetime object
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

    # Normalize timezone: convert local time into universal UTC ('Z')
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

    # Format into standard ISO-8601 string: "2026-10-23T12:00:00Z"
    iso_str = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Calculate age in seconds
    diff_seconds = (ref_utc - dt_utc).total_seconds()

    # Clock Skew Detection: If data timestamp is more than 5 seconds in the future
    if diff_seconds < -5.0:
        return {
            "computed_at": iso_str,
            "is_stale": True,
            "age_seconds": round(diff_seconds, 2),
            "freshness_status": "FUTURE_CLOCK_SKEW"
        }

    # If timestamp is slightly ahead due to sub-second clock drift, clamp age to 0.0s
    if diff_seconds < 0.0:
        diff_seconds = 0.0

    # Decision rule: Age >= threshold means STALE!
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
    Reads the production Parquet feature table from disk on every request.
    Extracts the latest prediction record for each zone.
    
    Error Resilience Contracts:
      - Missing folder -> raises HTTP 503 (DATA_SOURCE_UNAVAILABLE)
      - Corrupted file -> raises HTTP 500 (DATA_PARSING_ERROR)
      - Empty table    -> returns empty DataFrame
      - Missing column -> raises HTTP 500 (INVALID_DATA_SCHEMA)
    """
    if not FEATURE_TABLE_DIR.exists():
        logger.warning(f"Traffic feature store directory does not exist at {FEATURE_TABLE_DIR}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": ErrorCode.DATA_SOURCE_UNAVAILABLE,
                "message": "Traffic predictions data source is temporarily unavailable.",
                "detail": "Traffic feature store directory is missing."
            }
        )

    parquet_files = get_parquet_files(FEATURE_TABLE_DIR)
    if not parquet_files:
        logger.warning(f"No parquet files found in feature table directory {FEATURE_TABLE_DIR}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": ErrorCode.DATA_SOURCE_UNAVAILABLE,
                "message": "Traffic predictions data source is temporarily unavailable.",
                "detail": "No Parquet chunks available in traffic feature store."
            }
        )

    try:
        # Load the columnar Parquet files into a pandas DataFrame
        df = pd.read_parquet(parquet_files)
    except Exception as ex:
        logger.error(f"Failed to read Parquet feature store files: {ex}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": ErrorCode.DATA_PARSING_ERROR,
                "message": "Failed to parse traffic feature store files.",
                "detail": "Data store parsing error."
            }
        )

    # Empty store handling
    if df.empty:
        logger.info("Traffic feature store exists but contains 0 records.")
        return pd.DataFrame()

    # Schema validation: Ensure the mandatory 'zone_id' column exists
    if "zone_id" not in df.columns:
        logger.error(f"Traffic feature store is missing required 'zone_id' column. Columns: {list(df.columns)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": ErrorCode.INVALID_DATA_SCHEMA,
                "message": "Traffic feature store schema is invalid: missing required 'zone_id' column.",
                "detail": "Schema validation failed."
            }
        )

    # Sort records by time window
    time_col = "window_start" if "window_start" in df.columns else ("window" if "window" in df.columns else None)
    if time_col:
        df = df.sort_values(time_col, ascending=True)

    # Take the single latest record for each zone
    latest_df = df.groupby("zone_id", as_index=False).tail(1).copy()
    return latest_df


def read_recommended_routes_dataframe() -> pd.DataFrame:
    """
    Reads the production GraphX recommended routes snapshot from disk.
    Contains optimal shortest paths from Origin Node 1 to all destination vertices.
    """
    if not RECOMMENDED_ROUTES_DIR.exists():
        logger.warning(f"Recommended routes directory does not exist at {RECOMMENDED_ROUTES_DIR}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": ErrorCode.ROUTES_UNAVAILABLE,
                "message": "Recommended routes snapshot is temporarily unavailable.",
                "detail": "Recommended routes directory is missing."
            }
        )

    parquet_files = get_parquet_files(RECOMMENDED_ROUTES_DIR)
    if not parquet_files:
        logger.warning(f"No parquet files found in recommended routes directory {RECOMMENDED_ROUTES_DIR}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": ErrorCode.ROUTES_UNAVAILABLE,
                "message": "Recommended routes snapshot is temporarily unavailable.",
                "detail": "No Parquet chunks available in recommended routes directory."
            }
        )

    try:
        df = pd.read_parquet(parquet_files)
    except Exception as ex:
        logger.error(f"Failed to read recommended routes Parquet snapshot: {ex}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": ErrorCode.DATA_PARSING_ERROR,
                "message": "Failed to parse recommended routes snapshot.",
                "detail": "Data store parsing error."
            }
        )

    if df.empty:
        logger.info("Recommended routes snapshot exists but contains 0 records.")
        return pd.DataFrame()

    # Validate that essential route columns are present
    required_cols = {"origin_id", "destination_id", "path"}
    missing = required_cols - set(df.columns)
    if missing:
        logger.error(f"Recommended routes snapshot is missing required columns: {missing}. Columns: {list(df.columns)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": ErrorCode.INVALID_DATA_SCHEMA,
                "message": f"Recommended routes snapshot schema is invalid: missing required column(s): {', '.join(sorted(missing))}.",
                "detail": "Schema validation failed."
            }
        )

    return df.sort_values("destination_id", ascending=True)


# ==============================================================================
# 🌐 STEP 11: REST API ENDPOINTS
# ==============================================================================

@app.get("/api/predictions", summary="Get Latest Zone Traffic Predictions")
async def get_predictions() -> List[Dict[str, Any]]:
    """
    REST Endpoint: GET /api/predictions
    Returns the latest speed forecasts, Level of Service colors, and freshness
    metadata for all 5 monitored urban zones.
    """
    df = read_latest_predictions_dataframe()
    if df.empty:
        return []

    centroids = load_zone_coordinates()

    # Look for the speed column (support multiple naming conventions)
    speed_col = "predicted_speed_kmph" if "predicted_speed_kmph" in df.columns else (
        "predicted_speed" if "predicted_speed" in df.columns else "avg_speed_kmph"
    )

    results = []
    # Loop through each zone record
    for _, row in df.iterrows():
        zone_id = str(row["zone_id"])
        speed_val = float(row[speed_col]) if pd.notnull(row.get(speed_col)) else 0.0
        status_info = determine_speed_status(speed_val)

        # Coordinate lookup for this zone
        coords = centroids.get(zone_id, {"lat": 34.0200, "lon": -118.2900})

        # Extract timestamps and zone metadata
        win_start = str(row["window_start"]) if pd.notnull(row.get("window_start")) else None
        win_end = str(row["window_end"]) if pd.notnull(row.get("window_end")) else None
        zone_name = str(row["zone_name"]) if pd.notnull(row.get("zone_name")) else zone_id
        record_count = int(row["traffic_record_count"]) if pd.notnull(row.get("traffic_record_count")) else 0

        # Evaluate telemetry freshness (threshold = 900s / 15 min)
        win_end_val = row.get("window_end")
        freshness = evaluate_freshness(win_end_val, PREDICTIONS_STALE_THRESHOLD_SECONDS)

        # Assemble JSON response object
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

    # Sort alphabetically by zone_id (zone_1, zone_2, ...)
    return sorted(results, key=lambda x: x["zone_id"])


@app.get("/api/route", summary="Get Current Recommended Routes Snapshot")
async def get_route(destination_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    REST Endpoint: GET /api/route
    Returns optimal shortest paths from Origin 1 (Civic Center) calculated by Scala GraphX.
    
    Optional query parameter:
      - ?destination_id=15: Filter to only return the path to destination Node 15.
    """
    df = read_recommended_routes_dataframe()

    if df.empty:
        if destination_id is not None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": ErrorCode.DESTINATION_NOT_FOUND,
                    "message": f"Destination ID {destination_id} not found in current route snapshot.",
                    "detail": f"Destination ID {destination_id} not found."
                }
            )
        return []

    # Filter by specific destination if requested
    if destination_id is not None:
        df = df[df["destination_id"] == destination_id]
        if df.empty:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": ErrorCode.DESTINATION_NOT_FOUND,
                    "message": f"Destination ID {destination_id} not found in current route snapshot.",
                    "detail": f"Destination ID {destination_id} not found."
                }
            )

    results = []
    for _, row in df.iterrows():
        # Convert path elements into clean Python integers
        raw_path = row.get("path")
        path_list = [int(node) for node in raw_path] if hasattr(raw_path, "__iter__") else []

        # Evaluate route snapshot freshness (threshold = 180s / 3 min)
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
    HTML Map Endpoint: GET /api/map
    Generates an interactive Folium Leaflet HTML map directly from Parquet predictions.
    
    Layers rendered:
      1. OpenStreetMap dark canvas centered on Los Angeles.
      2. 5 Zone heat circles colored by live predicted speed.
      3. 20 Physical intersection markers (road network nodes).
      4. Dynamic GraphX PolyLine connecting nodes from Origin 1 to Destination (Node 15).
         - Royal Blue (#2563EB) when Fresh
         - Muted Gray (#6B7280) when Stale
      5. Floating visual legend explaining colors and thresholds.
    """
    predictions_df = read_latest_predictions_dataframe()
    centroids = load_zone_coordinates()

    speed_col = "predicted_speed_kmph" if "predicted_speed_kmph" in predictions_df.columns else (
        "predicted_speed" if "predicted_speed" in predictions_df.columns else "avg_speed_kmph"
    )

    # Initialize the Folium map centered on Los Angeles (34.018° N, -118.236° W)
    m = folium.Map(
        location=[34.0180, -118.2356],
        zoom_start=11,
        tiles="OpenStreetMap",
        control_scale=True # Adds a scale bar (km/miles) at the bottom left
    )

    # -----------------------------------------------------------------
    # LAYER 1: Zone Heatmap Circles
    # -----------------------------------------------------------------
    for _, row in predictions_df.iterrows():
        zone_id = str(row["zone_id"])
        zone_name = str(row.get("zone_name", zone_id))
        speed_val = float(row[speed_col]) if pd.notnull(row.get(speed_col)) else 0.0
        status_info = determine_speed_status(speed_val)
        win_start = str(row.get("window_start", "N/A"))

        coords = centroids.get(zone_id, {"lat": 34.0200, "lon": -118.2900})

        # Clean HTML card displayed when clicking on a zone
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

        # Semi-transparent outer circle representing the zone's geographical radius
        folium.Circle(
            location=[coords["lat"], coords["lon"]],
            radius=4500, # 4.5 kilometer radius
            color=status_info["color"],
            fill=True,
            fill_color=status_info["color"],
            fill_opacity=0.25,
            weight=2,
            tooltip=f"{zone_name}: {speed_val:.1f} km/h ({status_info['status']})"
        ).add_to(m)

        # Crisp inner dot marking the zone's exact centroid
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

    # -----------------------------------------------------------------
    # LAYER 2: Physical Intersections (Road Network Nodes)
    # -----------------------------------------------------------------
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

    # -----------------------------------------------------------------
    # LAYER 3: Recommended Route PolyLine (Pregel SSSP Route Overlay)
    # -----------------------------------------------------------------
    intersections_reg = load_intersections_registry()
    route_rendered: bool = False
    if destination_id is not None and destination_id > 0:
        try:
            routes_df = read_recommended_routes_dataframe()
            target_route = routes_df[routes_df["destination_id"] == destination_id]
            if not target_route.empty:
                r_row = target_route.iloc[0]
                is_reachable = bool(r_row.get("is_reachable", True))
                if is_reachable:
                    raw_path = r_row.get("path")
                    # Convert node sequence into ordered GPS coordinates
                    route_coords = build_route_coordinates(raw_path, intersections_reg)
                    if route_coords:
                        eta_min = float(r_row.get("total_travel_time_minutes", 0.0))
                        orig_id = int(r_row.get("origin_id", 1))
                        dest_id = int(r_row.get("destination_id", destination_id))
                        path_str = str(r_row.get("path_formatted", f"{raw_path}"))

                        # Evaluate freshness of the route calculation
                        route_freshness = evaluate_freshness(r_row.get("computed_at"), ROUTES_STALE_THRESHOLD_SECONDS)
                        is_route_stale = route_freshness["is_stale"]
                        route_age = route_freshness["age_seconds"]
                        route_age_str = f"{route_age:.0f}s" if route_age is not None else "Unknown"

                        # Visual differentiation: Fresh is Royal Blue; Stale is Muted Gray
                        if is_route_stale:
                            route_color = "#6B7280"  # Gray PolyLine
                            route_tooltip = f"STALE ROUTE: Recommended Route Origin {orig_id} &rarr; Destination {dest_id} (ETA: {eta_min:.1f} min | Age: {route_age_str})"
                            stale_badge = f"""
                            <div style="background: #FEE2E2; border: 1px solid #EF4444; color: #991B1B; padding: 4px 8px; border-radius: 4px; font-weight: 600; font-size: 11px; margin-bottom: 8px;">
                                ⚠️ STALE ROUTE: Snapshot age ({route_age_str}) &ge; {ROUTES_STALE_THRESHOLD_SECONDS:.0f}s threshold
                            </div>
                            """
                        else:
                            route_color = "#2563EB"  # Royal Blue PolyLine
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

                        # Draw the vector PolyLine on the map
                        folium.PolyLine(
                            locations=route_coords,
                            color=route_color,
                            weight=5,
                            opacity=0.85,
                            tooltip=route_tooltip,
                            popup=folium.Popup(route_popup_html, max_width=290)
                        ).add_to(m)
                        route_rendered = True

                        # Marker at Origin Node 1 (Civic Center)
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

                        # Marker at Destination Node
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
        except Exception as ex:
            logger.warning(f"Could not load or render route overlay for destination {destination_id}: {ex}")

    # -----------------------------------------------------------------
    # LAYER 4: Custom Floating Map Legend
    # -----------------------------------------------------------------
    if destination_id is not None and destination_id > 0:
        if route_rendered:
            route_legend_entries = """
            <div style="display: flex; align-items: center; margin-top: 4px;">
                <span style="height: 4px; width: 16px; background-color: #2563EB; display: inline-block; margin-right: 8px;"></span>
                <span>Recommended Route (Pregel SSSP) [Fresh]</span>
            </div>
            <div style="display: flex; align-items: center; margin-top: 4px;">
                <span style="height: 4px; width: 16px; background-color: #6B7280; display: inline-block; margin-right: 8px;"></span>
                <span>Recommended Route (Pregel SSSP) [STALE]</span>
            </div>
            """
        else:
            route_legend_entries = """
            <div style="display: flex; align-items: center; margin-top: 4px;">
                <span style="height: 4px; width: 16px; background-color: #9CA3AF; display: inline-block; margin-right: 8px;"></span>
                <span>Recommended Route (Pregel SSSP) [Route Unavailable]</span>
            </div>
            """
    else:
        route_legend_entries = """
        <div style="display: flex; align-items: center; margin-top: 4px;">
            <span style="height: 4px; width: 16px; background-color: #9CA3AF; display: inline-block; margin-right: 8px;"></span>
            <span>Recommended Route (Pregel SSSP) [No Destination]</span>
        </div>
        """

    legend_html = f"""
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
        {route_legend_entries}
        <div style="display: flex; align-items: center; margin-top: 4px;">
            <span style="height: 8px; width: 8px; background-color: #3B82F6; border-radius: 50%; display: inline-block; margin-right: 10px; margin-left: 2px;"></span>
            <span style="color: #6B7280;">Node 1: Dispatch Origin</span>
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # Render into a complete HTML string and return to browser
    return m.get_root().render()


# ==============================================================================
# 📄 STEP 12: STATIC FILE SERVING & ROOT ROUTE
# ==============================================================================

@app.get("/", response_class=FileResponse, summary="Serve Browser Dashboard")
async def serve_dashboard():
    """
    Root Endpoint: GET /
    Delivers our main 'index.html' dashboard page directly to the operator's web browser.
    """
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard frontend not found at {index_file}."
        )
    return FileResponse(index_file)

# Mount the static directory so the browser can request /static/styles.css if needed
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ==============================================================================
# 🚀 STEP 13: LOCAL SERVER ENTRYPOINT
# ==============================================================================
# This block runs when you launch the file directly from the terminal:
#   python dashboard/main.py
if __name__ == "__main__":
    # 'uvicorn' is an lightning-fast ASGI web server implementation for Python
    import uvicorn
    print("=" * 80)
    print("  URBANIX DAY 10: GEOSPATIAL DASHBOARD BACKEND")
    print("  Starting Uvicorn web server at http://localhost:8000")
    print("=" * 80)
    # Start server listening on localhost port 8000 with hot-reload enabled
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
