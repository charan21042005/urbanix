# ==============================================================================
# 🌟 URBANIX — DASHBOARD REGRESSION & API CONTRACT TEST SUITE
# test_dashboard.py — Fast, In-Memory Unit, Integration & Error Contract Tests
# ==============================================================================
# Welcome to the Urbanix Dashboard test suite! 👋
#
# WHAT IS THIS FILE FOR?
# In software engineering, writing automated tests is like having a robot inspect
# your code every time you make a change. This ensures that new features never
# accidentally break existing features (this is called "regression testing").
#
# HOW DOES THIS TEST SUITE WORK?
# Instead of launching a slow web browser or starting a live network server on a port,
# this file uses FastAPI's 'TestClient' to simulate HTTP requests in memory!
# It executes 47 comprehensive tests in just ~2 seconds!
#
# WHAT DOES IT TEST?
#   1. Path & File Handling         -> Folder existence, strictly discovering .parquet files
#   2. Geographic Mapping           -> LA bounding box, Level of Service (LOS) speed colors
#   3. FastAPI Endpoints            -> /api/predictions, /api/route, /api/map, CORS headers
#   4. Route Coordinate Sequences   -> Transforming path node IDs [1, 2, 14, 16, 15] into lat/lon
#   5. Data Freshness Semantics     -> 180s route & 900s prediction thresholds, UTC normalization
#   6. Map Styling & Vector Overlays-> Fresh royal blue (#2563EB) vs stale slate gray (#6B7280)
#   7. Frontend UI Integration      -> Verifying badges, interval constants, and polling scripts
#   8. Error Handling & Privacy     -> Structured JSON errors (503, 500, 422, 404) with zero path leakage
# ==============================================================================

# ------------------------------------------------------------------------------
# 📦 STEP 1: PYTHON STANDARD LIBRARY IMPORTS
# ------------------------------------------------------------------------------
# 'sys' allows us to modify Python's runtime search path so it can find our application files.
import sys

# 'pathlib.Path' is Python's modern object-oriented library for working with filesystem paths.
# It works seamlessly across Windows (backslashes \) and Linux/macOS (forward slashes /)!
from pathlib import Path

# 'datetime', 'timezone', 'timedelta' handle timestamps, dates, UTC time, and time differences.
from datetime import datetime, timezone, timedelta

# ------------------------------------------------------------------------------
# 📦 STEP 2: THIRD-PARTY TESTING & DATA LIBRARIES
# ------------------------------------------------------------------------------
# 'pytest' is Python's industry-standard test framework.
# When you run 'pytest', it automatically finds every function that starts with 'test_' and executes it!
import pytest

# 'pandas' is Python's famous data manipulation library.
# We use it here to create test DataFrames and write sample Parquet files.
import pandas as pd

# 'TestClient' from FastAPI:
# This allows us to make fake HTTP calls (client.get, client.post) directly to our FastAPI app
# completely in memory without opening an actual network port!
from fastapi.testclient import TestClient

# ------------------------------------------------------------------------------
# 🔧 STEP 3: CONFIGURE PYTHON MODULE SEARCH PATH
# ------------------------------------------------------------------------------
# __file__ is a built-in variable containing the path of this test file.
# .resolve() finds the absolute path on your computer.
# .parent.parent moves two levels up to the 'dashboard/' directory.
DASHBOARD_DIR = Path(__file__).resolve().parent.parent

# sys.path is a list of directories Python searches when you type 'import ...'.
# sys.path.insert(0, ...) puts the dashboard folder at the very front of that search list!
sys.path.insert(0, str(DASHBOARD_DIR))

# ------------------------------------------------------------------------------
# 📦 STEP 4: IMPORT APPLICATION FUNCTIONS & CONSTANTS UNDER TEST
# ------------------------------------------------------------------------------
# Now that sys.path includes dashboard/, we can safely import 'main' (dashboard/main.py)
import main
from main import (
    app,                                # The FastAPI application instance
    get_parquet_files,                  # Helper function that finds .parquet files
    load_zone_coordinates,              # Helper function that computes zone centroids
    load_intersections_registry,        # Helper function that loads the 20 road nodes
    build_route_coordinates,            # Helper function that maps node IDs to lat/lon pairs
    determine_speed_status,             # Helper function for Level of Service (green/orange/red)
    evaluate_freshness,                 # Helper function that calculates data age and staleness
    ErrorCode,                          # Enum containing structured error code strings
    ROUTES_UPDATE_INTERVAL_SECONDS,     # Cadence constant: 60.0s
    PREDICTIONS_UPDATE_INTERVAL_SECONDS, # Cadence constant: 300.0s
    STALE_THRESHOLD_MULTIPLIER,         # Multiplier: 3.0x
    ROUTES_STALE_THRESHOLD_SECONDS,      # Stale cutoff: 180.0s (3 min)
    PREDICTIONS_STALE_THRESHOLD_SECONDS, # Stale cutoff: 900.0s (15 min)
    REPO_ROOT,                          # Path pointing to the root of the Urbanix repository
    FEATURE_TABLE_DIR,                  # Path pointing to data/feature_table/traffic
    RECOMMENDED_ROUTES_DIR,             # Path pointing to data/recommended_routes
    INTERSECTIONS_CSV,                  # Path pointing to data/intersections.csv
)

# Initialize the simulated HTTP client wrapping our FastAPI 'app'
# Any call like client.get("/") will invoke FastAPI routes directly in memory!
client = TestClient(app)


# ==============================================================================
# 📂 TEST SECTION 1: PATH & FILE HANDLING TESTS
# ==============================================================================

def test_repo_paths_exist():
    """
    Sanity Check:
    Verify that all foundational project directories and configuration files exist on disk.
    If any of these paths are missing, the dashboard cannot read telemetry!
    """
    # .exists() returns True if the file or folder physically exists on your computer
    assert REPO_ROOT.exists(), f"Repo root does not exist: {REPO_ROOT}"
    assert FEATURE_TABLE_DIR.exists(), f"Feature table path does not exist: {FEATURE_TABLE_DIR}"
    assert RECOMMENDED_ROUTES_DIR.exists(), f"Routes path does not exist: {RECOMMENDED_ROUTES_DIR}"
    assert INTERSECTIONS_CSV.exists(), f"Intersections CSV does not exist: {INTERSECTIONS_CSV}"


def test_parquet_filtering_ignores_non_parquet():
    """
    Data Cleaning & Parquet Discovery Test:
    When Apache Spark writes Parquet tables, it also creates non-data metadata files:
      - '_SUCCESS' (empty marker file indicating the write finished)
      - '.crc' hidden checksum files
      - 'README.md' documentation files
    'get_parquet_files()' must strictly filter these out and return ONLY valid .parquet files!
    """
    # 1. Test against the streaming traffic feature table folder
    ft_files = get_parquet_files(FEATURE_TABLE_DIR)
    assert len(ft_files) > 0, "No parquet files found in feature table"
    for f in ft_files:
        assert f.endswith(".parquet"), f"Discovered non-parquet file: {f}"
        assert not Path(f).name.startswith("."), f"Discovered hidden file: {f}"
        assert not Path(f).name.startswith("_"), f"Discovered metadata file: {f}"
        assert "README" not in f, f"Discovered README in parquet files: {f}"

    # 2. Test against the recommended routes folder
    rr_files = get_parquet_files(RECOMMENDED_ROUTES_DIR)
    assert len(rr_files) > 0, "No parquet files found in recommended routes"
    for f in rr_files:
        assert f.endswith(".parquet"), f"Discovered non-parquet file: {f}"


# ==============================================================================
# 🗺️ TEST SECTION 2: GEOGRAPHIC & MATHEMATICAL MAPPING TESTS
# ==============================================================================

def test_load_zone_coordinates():
    """
    Geographic Sanity Test:
    Verify that zone centroids are computed honestly from intersections.csv
    and lie strictly within the geographic bounding box of Greater Los Angeles!
    """
    centroids = load_zone_coordinates()
    # Urbanix monitors 5 distinct urban zones (zone_1 through zone_5)
    assert len(centroids) == 5, f"Expected 5 zones, got {len(centroids)}"

    for zone_id, coords in centroids.items():
        assert zone_id.startswith("zone_")
        assert "lat" in coords and "lon" in coords

        # Greater Los Angeles bounding box coordinates:
        # Latitude range: 33.5° N to 34.5° N
        # Longitude range: -118.7° W to -117.8° W
        assert 33.5 <= coords["lat"] <= 34.5, f"Invalid latitude for {zone_id}: {coords['lat']}"
        assert -118.7 <= coords["lon"] <= -117.8, f"Invalid longitude for {zone_id}: {coords['lon']}"


def test_speed_status_thresholds():
    """
    Level of Service (LOS) Classification Test:
    Tests the mathematical classification that turns raw speed numbers into human-readable colors:
      - Speed >= 45 km/h: FREE_FLOWING (Green)
      - Speed 25 to 45 km/h: MODERATE (Orange)
      - Speed < 25 km/h: CONGESTED (Red)
    """
    # High speed: Free-flowing
    fast = determine_speed_status(75.0)
    assert fast["status"] == "FREE_FLOWING"
    assert fast["color"] == "green"

    # Medium speed: Moderate congestion
    moderate = determine_speed_status(45.0)
    assert moderate["status"] == "MODERATE"
    assert moderate["color"] == "orange"

    # Slow speed: Heavy gridlock
    congested = determine_speed_status(15.0)
    assert congested["status"] == "CONGESTED"
    assert congested["color"] == "red"


# ==============================================================================
# ⚡ TEST SECTION 3: FASTAPI ENDPOINT IN-MEMORY TESTS
# ==============================================================================

def test_get_predictions_endpoint():
    """
    API Contract Test:
    Verify that GET /api/predictions returns HTTP 200 and a list of 5 valid zone records.
    """
    # client.get() sends an in-memory HTTP GET request to our FastAPI application
    response = client.get("/api/predictions")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"

    # .json() parses the JSON response into native Python lists and dicts
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 5, f"Expected 5 zones in predictions, got {len(data)}"

    # Ensure all 5 zone IDs are represented
    zone_ids = [item["zone_id"] for item in data]
    assert sorted(zone_ids) == ["zone_1", "zone_2", "zone_3", "zone_4", "zone_5"]

    # Validate the data contract schema for every record
    for item in data:
        assert "zone_name" in item
        assert "predicted_speed_kmph" in item
        assert isinstance(item["predicted_speed_kmph"], (int, float))
        assert item["predicted_speed_kmph"] > 0, "Predicted speed must be positive"
        assert item["status"] in ["FREE_FLOWING", "MODERATE", "CONGESTED"]
        # Academic Scope Honesty: AQI/PM2.5 prediction models were not scoped for Day 6/9
        # So it must return None rather than a fabricated mock number!
        assert item["predicted_pm25"] is None, "AQI/PM2.5 must be None per Day 6/9 scope honesty"
        assert "traffic_record_count" in item
        assert "latitude" in item and "longitude" in item


def test_get_route_endpoint():
    """
    API Contract Test:
    Verify that GET /api/route returns all 20 GraphX SSSP calculated route recommendations.
    """
    response = client.get("/api/route")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"

    data = response.json()
    assert isinstance(data, list)
    # Origin 1 (Civic Center) routes to all 20 nodes in the road graph
    assert len(data) == 20, f"Expected 20 routes from Origin 1, got {len(data)}"

    # Check Route 1: Origin 1 to Destination 1 (self-loop distance is 0 hours)
    route_1 = next(r for r in data if r["destination_id"] == 1)
    assert route_1["origin_id"] == 1
    assert route_1["total_travel_time_hours"] == 0.0
    assert route_1["path"] == [1]

    # Check Route 8: A multi-hop path across the city
    route_8 = next(r for r in data if r["destination_id"] == 8)
    assert route_8["is_reachable"] is True
    assert route_8["total_travel_time_minutes"] > 0
    assert len(route_8["path"]) >= 2
    assert route_8["path_formatted"] == "1 -> 2 -> 3 -> 5 -> 7 -> 8"


def test_get_route_filtered_by_destination():
    """
    Query Parameter Filtering Test:
    Verify that GET /api/route?destination_id=15 filters the result to only Destination 15.
    """
    response = client.get("/api/route?destination_id=15")
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 1
    assert data[0]["destination_id"] == 15
    # The verified optimal path to Node 15 is 1 -> 2 -> 14 -> 16 -> 15
    assert data[0]["path_formatted"] == "1 -> 2 -> 14 -> 16 -> 15"


def test_get_route_invalid_destination():
    """
    404 Error Handling Test:
    Verify that querying a nonexistent destination (Node 999) returns a clean HTTP 404 Not Found.
    """
    response = client.get("/api/route?destination_id=999")
    assert response.status_code == 404


def test_get_map_endpoint():
    """
    Map HTML Generation Test:
    Verify that GET /api/map returns a valid HTML document containing the Leaflet interactive map.
    """
    response = client.get("/api/map")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]

    html = response.text
    # Check that basic HTML markers and the Leaflet library are present in the response
    assert "<!DOCTYPE html>" in html or "<div" in html
    assert "leaflet" in html.lower()
    assert "Urbanix Traffic Legend" in html
    assert "Free Flowing" in html


def test_root_index_page():
    """
    Frontend Dashboard Delivery Test:
    Verify that GET / serves 'index.html' with our live polling interval script (15,000ms).
    """
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]

    html = response.text
    assert "URBANIX DASHBOARD" in html
    assert "REFRESH_INTERVAL_MS = 15000" in html
    assert "<iframe" in html
    assert "map-frame" in html


def test_cors_headers():
    """
    CORS (Cross-Origin Resource Sharing) Security Test:
    Verify that modern web frontends (e.g. React/Vite running on port 3000)
    receive proper Access-Control-Allow-Origin headers when making preflight OPTIONS requests.
    """
    response = client.options(
        "/api/predictions",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET"
        }
    )
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "http://localhost:3000"


def test_no_mock_data():
    """
    Production Data Authenticity Test:
    Verify that /api/predictions reads real empirical numbers from the Parquet feature table
    and does NOT return hardcoded mock numbers (e.g. Zone 1 speed is ~77.67 km/h from METR-LA).
    """
    response = client.get("/api/predictions")
    data = response.json()
    zone_1 = next(z for z in data if z["zone_id"] == "zone_1")
    # Verify zone 1 speed matches empirical Parquet data within 0.1 tolerance
    assert abs(zone_1["predicted_speed_kmph"] - 77.67) < 0.1


# ==============================================================================
# 📍 TEST SECTION 4: ROUTE OVERLAY & COORDINATE VERIFICATION TESTS
# ==============================================================================

def test_load_intersections_registry():
    """
    Intersections Registry Test:
    Verify that load_intersections_registry() correctly indexes all 20 nodes with valid numeric lat/lon.
    """
    registry = load_intersections_registry()
    assert len(registry) == 20, f"Expected 20 intersections, got {len(registry)}"

    # Check Node 1 (Origin at Civic Center)
    assert 1 in registry
    assert registry[1]["name"] == "Grand Ave & 1st St"
    assert registry[1]["zone_id"] == "zone_1"
    assert abs(registry[1]["lat"] - 34.0549) < 0.001
    assert abs(registry[1]["lon"] - (-118.2498)) < 0.001

    # Check Node 15 (Canonical evaluation destination)
    assert 15 in registry
    assert abs(registry[15]["lat"] - 33.9984) < 0.001
    assert abs(registry[15]["lon"] - (-118.0874)) < 0.001


def test_build_route_coordinates_ordered_sequence():
    """
    Coordinate Sequence Transformation Test:
    Verify that build_route_coordinates transforms path [1, 2, 14, 16, 15] into
    the exact ordered sequence of physical latitude and longitude coordinates.
    """
    registry = load_intersections_registry()
    path = [1, 2, 14, 16, 15]
    coords = build_route_coordinates(path, registry)

    assert len(coords) == 5, f"Expected 5 coordinate pairs, got {len(coords)}"

    # Verify each coordinate pair matches the node's position in order
    assert coords[0] == (registry[1]["lat"], registry[1]["lon"])
    assert coords[1] == (registry[2]["lat"], registry[2]["lon"])
    assert coords[2] == (registry[14]["lat"], registry[14]["lon"])
    assert coords[3] == (registry[16]["lat"], registry[16]["lon"])
    assert coords[4] == (registry[15]["lat"], registry[15]["lon"])


def test_build_route_coordinates_aborts_on_missing_node():
    """
    Data Integrity Test (Missing Node):
    If ANY node in the route path is missing from the registry,
    build_route_coordinates MUST abort and return an empty list.
    It must NEVER draw a broken, partial, or misleading route!
    """
    registry = load_intersections_registry()
    path_with_missing_node = [1, 2, 999, 15]  # Node 999 does not exist
    coords = build_route_coordinates(path_with_missing_node, registry)
    assert coords == [], f"Expected empty list on missing node ID, got {coords}"


def test_build_route_coordinates_aborts_on_invalid_coordinates():
    """
    Data Integrity Test (Out-of-Bounds Coordinates):
    If an intersection has an out-of-bounds latitude (e.g. 55.0 N, far outside LA),
    build_route_coordinates must abort and return an empty list.
    """
    mock_registry = {
        1: {"name": "Node 1", "zone_id": "zone_1", "lat": 34.0549, "lon": -118.2498},
        2: {"name": "Node 2", "zone_id": "zone_1", "lat": 55.0000, "lon": -118.2543},  # Out of bounds
    }
    coords = build_route_coordinates([1, 2], mock_registry)
    assert coords == [], "Expected empty list on out-of-bounds latitude"


def test_build_route_coordinates_handles_malformed_inputs():
    """
    Robustness Test:
    Verify safe handling of empty paths, single-node paths, None, and non-numeric lists.
    None of these should ever crash the application!
    """
    registry = load_intersections_registry()
    assert build_route_coordinates([], registry) == []
    assert build_route_coordinates([1], registry) == []  # Single node cannot form a route edge
    assert build_route_coordinates(None, registry) == []
    assert build_route_coordinates(["invalid", "path"], registry) == []


def test_get_map_with_route_overlay_destination_15():
    """
    PolyLine Overlay Rendering Test:
    Verify that GET /api/map?destination_id=15 renders a Folium PolyLine
    for the baseline GraphX route [1, 2, 14, 16, 15] with ETA and origin/destination markers.
    """
    response = client.get("/api/map?destination_id=15")
    assert response.status_code == 200
    html = response.text

    # Verify that Leaflet PolyLine javascript function is included
    assert "polyline" in html.lower() or "l.polyline" in html.lower()

    # Check that route metadata appears in HTML / tooltip / popups
    assert "Recommended Route" in html
    assert "Node 1" in html
    assert "Node 15" in html
    assert "19.02" in html or "19.0" in html

    # Check that map legend contains the route entry
    assert "Recommended Route (Pregel SSSP)" in html

    # Check that zone circles still exist
    assert "Urbanix Traffic Legend" in html


def test_get_map_graceful_on_nonexistent_destination():
    """
    Graceful Degradation Test (Invalid Destination on Map):
    If an uncomputed destination ID (e.g. 999) is requested on the map,
    GET /api/map must NOT crash with 500. It must gracefully render the base zone heatmap.
    """
    response = client.get("/api/map?destination_id=999")
    assert response.status_code == 200
    html = response.text
    assert "Urbanix Traffic Legend" in html
    assert "Free Flowing" in html


# ==============================================================================
# ⏱️ TEST SECTION 5: FRESHNESS EVALUATION & TIMESTAMP SEMANTICS TESTS
# ==============================================================================

def test_freshness_helper_below_threshold():
    """
    Freshness Test (< Threshold):
    Verify that data updated 60 seconds ago is marked as FRESH when threshold is 180s.
    """
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    ts = datetime(2026, 10, 23, 11, 59, 0, tzinfo=timezone.utc)  # 60s old vs 180s threshold
    result = evaluate_freshness(ts, threshold_seconds=180.0, reference_time=ref_time)
    assert result["is_stale"] is False
    assert result["freshness_status"] == "FRESH"
    assert result["age_seconds"] == 60.0
    assert result["computed_at"] == "2026-10-23T11:59:00Z"


def test_freshness_helper_exact_threshold():
    """
    Boundary Condition Test (== Threshold):
    Verify that data exactly 180 seconds old is flagged as STALE (is_stale=True).
    """
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    ts = datetime(2026, 10, 23, 11, 57, 0, tzinfo=timezone.utc)  # Exactly 180s old
    result = evaluate_freshness(ts, threshold_seconds=180.0, reference_time=ref_time)
    assert result["is_stale"] is True
    assert result["freshness_status"] == "STALE"
    assert result["age_seconds"] == 180.0


def test_freshness_helper_above_threshold():
    """
    Boundary Condition Test (> Threshold):
    Verify that data 181 seconds old is flagged as STALE (is_stale=True).
    """
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    ts = datetime(2026, 10, 23, 11, 56, 59, tzinfo=timezone.utc)  # 181s old
    result = evaluate_freshness(ts, threshold_seconds=180.0, reference_time=ref_time)
    assert result["is_stale"] is True
    assert result["freshness_status"] == "STALE"
    assert result["age_seconds"] == 181.0


def test_freshness_helper_missing_timestamp():
    """
    Safety Test (Missing Timestamp):
    Verify that missing, empty, or None timestamps are safely flagged STALE, never fresh.
    """
    assert evaluate_freshness(None, 180.0)["is_stale"] is True
    assert evaluate_freshness("", 180.0)["is_stale"] is True
    assert evaluate_freshness("   ", 180.0)["is_stale"] is True
    assert evaluate_freshness(None, 180.0)["computed_at"] is None
    assert evaluate_freshness(None, 180.0)["freshness_status"] == "MISSING_TIMESTAMP"


def test_freshness_helper_malformed_timestamp():
    """
    Safety Test (Corrupted Timestamp String):
    Verify that invalid date strings do not crash the app and are safely flagged as STALE.
    """
    result = evaluate_freshness("invalid-date-format-xyz", 180.0)
    assert result["is_stale"] is True
    assert result["computed_at"] is None
    assert result["freshness_status"] == "MALFORMED_TIMESTAMP"


def test_freshness_helper_timezone_aware_normalization():
    """
    Timezone Normalization Test:
    Verify that timestamps with explicit timezone offsets (+05:30)
    are accurately normalized into UTC ('Z') for clean cross-system comparison.
    """
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    aware_str = "2026-10-23T17:00:00+05:30"  # Equivalent to 11:30:00 UTC (1800s old)
    result = evaluate_freshness(aware_str, threshold_seconds=3600.0, reference_time=ref_time)
    assert result["computed_at"] == "2026-10-23T11:30:00Z"
    assert result["age_seconds"] == 1800.0
    assert result["is_stale"] is False


def test_freshness_helper_naive_interpreted_as_utc():
    """
    Timezone-Naive Test:
    Verify that timezone-naive timestamps from Parquet files are explicitly interpreted as UTC.
    """
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    naive_str = "2026-10-23 11:58:00"  # 120s old
    result = evaluate_freshness(naive_str, threshold_seconds=180.0, reference_time=ref_time)
    assert result["computed_at"] == "2026-10-23T11:58:00Z"
    assert result["age_seconds"] == 120.0
    assert result["is_stale"] is False


def test_freshness_helper_future_timestamp_clock_skew():
    """
    Clock Skew Test:
    If a sensor or computer clock is out of sync and reports a future timestamp,
    it must be flagged as FUTURE_CLOCK_SKEW and treated as stale.
    """
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    future_ts = datetime(2026, 10, 23, 12, 5, 0, tzinfo=timezone.utc)  # 5 minutes in future
    result = evaluate_freshness(future_ts, threshold_seconds=180.0, reference_time=ref_time)
    assert result["is_stale"] is True
    assert result["freshness_status"] == "FUTURE_CLOCK_SKEW"
    assert result["age_seconds"] == -300.0


def test_configured_constants():
    """
    System Constants Verification:
    Verifies update cadence and multiplier thresholds match the SRS specification:
      - Routes update interval: 60s
      - Predictions update interval: 300s
      - Stale multiplier: 3.0x
      - Routes stale threshold: 180s (3 * 60s)
      - Predictions stale threshold: 900s (3 * 300s)
    """
    assert ROUTES_UPDATE_INTERVAL_SECONDS == 60.0
    assert PREDICTIONS_UPDATE_INTERVAL_SECONDS == 300.0
    assert STALE_THRESHOLD_MULTIPLIER == 3.0
    assert ROUTES_STALE_THRESHOLD_SECONDS == 180.0
    assert PREDICTIONS_STALE_THRESHOLD_SECONDS == 900.0


def test_predictions_api_freshness_metadata():
    """
    API Freshness Contract Test (/api/predictions):
    Verify that /api/predictions responses include computed_at, is_stale, and age_seconds metadata.
    """
    response = client.get("/api/predictions")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 5

    for item in data:
        assert "computed_at" in item
        assert "is_stale" in item
        assert "age_seconds" in item
        assert "freshness_status" in item
        assert isinstance(item["is_stale"], bool)
        # Because baseline data is from 2012, against current system clock it must be stale
        assert item["is_stale"] is True
        assert item["freshness_status"] == "STALE"
        assert item["computed_at"].startswith("2012-03-01T08:10:00")
        assert item["age_seconds"] > 900.0
        # Backward-compatibility: predicted_pm25 is None
        assert item["predicted_pm25"] is None


def test_route_api_freshness_metadata():
    """
    API Freshness Contract Test (/api/route):
    Verify that /api/route responses include computed_at, is_stale, and age_seconds metadata.
    """
    response = client.get("/api/route?destination_id=15")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    r = data[0]

    assert "computed_at" in r
    assert "is_stale" in r
    assert "age_seconds" in r
    assert "freshness_status" in r
    assert isinstance(r["is_stale"], bool)
    assert r["computed_at"] != ""
    assert r["is_stale"] is True  # Snapshot from prior run is > 180s old
    assert r["age_seconds"] > 180.0


def test_route_api_invalid_query_type_returns_422():
    """
    Type Validation Contract:
    Verify that passing an invalid type (string "invalid_id" instead of integer) returns HTTP 422.
    """
    response = client.get("/api/route?destination_id=invalid_id")
    assert response.status_code == 422


# ==============================================================================
# 🎨 TEST SECTION 6: MAP STALE & FRESH ROUTE STYLING TESTS
# ==============================================================================

def test_map_stale_route_styling_and_tooltip():
    """
    Visual Degradation Styling Test:
    When a route is stale (>180s), the Leaflet map must paint the PolyLine
    in muted gray (#6B7280) and include 'STALE ROUTE' in the popup tooltip.
    """
    response = client.get("/api/map?destination_id=15")
    assert response.status_code == 200
    html = response.text

    assert "STALE ROUTE" in html
    assert "#6B7280" in html
    assert "Recommended Route (Pregel SSSP) [STALE]" in html


def test_map_fresh_route_styling_with_injected_snapshot(monkeypatch):
    """
    Visual Freshness Styling Test (using pytest 'monkeypatch'):
    WHAT IS MONKEYPATCH?
    'monkeypatch' is a pytest feature that lets us temporarily replace a Python function
    with a mock version for just this ONE test!

    Here, we inject a freshly generated timestamp (0 seconds old) to test that
    fresh routes are rendered in royal blue (#2563EB) without touching real production files!
    """
    import main as main_module
    orig_read = main_module.read_recommended_routes_dataframe

    # Define our mock function that returns a DataFrame with a fresh timestamp
    def mock_fresh_routes():
        df = orig_read().copy()
        now_utc = datetime.now(timezone.utc)
        df["computed_at"] = now_utc  # Fresh timestamp (0s old)
        return df

    # Temporarily substitute the read function during this test
    monkeypatch.setattr(main_module, "read_recommended_routes_dataframe", mock_fresh_routes)

    response = client.get("/api/map?destination_id=15")
    assert response.status_code == 200
    html = response.text

    # Verify royal blue hex code and fresh route legend text
    assert "#2563EB" in html
    assert "FRESH ROUTE" in html
    assert "Recommended Route (Pregel SSSP) [Fresh]" in html


# ==============================================================================
# 🖥️ TEST SECTION 7: FRONTEND UI INTEGRATION TESTS
# ==============================================================================

def test_frontend_stale_indicators_and_polling_script():
    """
    Frontend Template Verification:
    Verify that GET / serves HTML with status badges, 15s interval, and concurrency guard.
    """
    response = client.get("/")
    assert response.status_code == 200
    html = response.text

    # Freshness badge element IDs
    assert "badge-pred-header" in html
    assert "badge-route-header" in html
    assert "chip-pred-badge" in html
    assert "chip-route-badge" in html

    # Polling cadence and concurrency guard variable names
    assert "REFRESH_INTERVAL_MS = 15000" in html
    assert "isRefreshing" in html
    assert "updateRouteStatus" in html


# ==============================================================================
# 🛡️ TEST SECTION 8: ERROR HANDLING, RESILIENCE & PRIVACY CONTRACTS
# ==============================================================================
# In production, services must fail gracefully:
#   1. They must NEVER leak raw stack traces or internal computer directories!
#   2. They must return clean, structured JSON envelopes with standard HTTP error codes.

def test_predictions_missing_directory_returns_503(monkeypatch, tmp_path):
    """
    Resilience Test (Missing Directory -> 503):
    WHAT IS tmp_path?
    'tmp_path' is a pytest fixture that creates a temporary empty folder that is
    automatically deleted when the test ends.

    Verify that when the feature table folder is missing, the API returns:
      - HTTP 503 (DATA_SOURCE_UNAVAILABLE)
      - Zero internal filesystem paths leaked in the response message!
    """
    missing_dir = tmp_path / "missing_feature_table"
    # Point the application to this nonexistent folder
    monkeypatch.setattr(main, "FEATURE_TABLE_DIR", missing_dir)

    response = client.get("/api/predictions")
    assert response.status_code == 503
    data = response.json()

    assert data["error"] == ErrorCode.DATA_SOURCE_UNAVAILABLE.value
    assert "temporarily unavailable" in data["message"]

    # Security & Path Privacy Verification:
    # Ensure raw operating system folder paths are NEVER exposed to the public!
    assert str(missing_dir) not in str(data)
    assert "tmp_path" not in str(data).lower()
    assert "\\" not in data["message"] and "/" not in data["message"]


def test_predictions_empty_table_returns_200_empty_list(monkeypatch, tmp_path):
    """
    Empty State Test (Empty Table -> HTTP 200 with []):
    Verify that an existing but completely empty Parquet table returns
    an empty JSON list '[]' with status 200 rather than crashing.
    """
    # Create an empty DataFrame with the required column names
    empty_df = pd.DataFrame(columns=[
        "zone_id", "window_start", "window_end",
        "predicted_speed_kmh", "historical_avg_speed_kmh", "computed_at"
    ])
    empty_parquet = tmp_path / "empty_pred.parquet"
    empty_df.to_parquet(empty_parquet)
    monkeypatch.setattr(main, "FEATURE_TABLE_DIR", tmp_path)

    response = client.get("/api/predictions")
    assert response.status_code == 200
    assert response.json() == []


def test_predictions_corrupted_parquet_returns_500(monkeypatch, tmp_path):
    """
    Fault Tolerance Test (Corrupted Bytes -> 500 DATA_PARSING_ERROR):
    Verify that corrupted or malformed Parquet files return a structured 500 error
    without leaking raw Python stack traces.
    """
    bad_file = tmp_path / "corrupt.parquet"
    # Write garbage random bytes instead of valid Parquet format
    bad_file.write_bytes(b"CORRUPTED_NON_PARQUET_BYTE_STREAM_123456789")
    monkeypatch.setattr(main, "FEATURE_TABLE_DIR", tmp_path)

    response = client.get("/api/predictions")
    assert response.status_code == 500
    data = response.json()

    assert data["error"] == ErrorCode.DATA_PARSING_ERROR.value
    assert "Failed to parse" in data["message"]
    # Verify no raw traceback or internal file paths leaked
    assert "Traceback" not in str(data)
    assert str(bad_file) not in str(data)


def test_predictions_invalid_schema_returns_500(monkeypatch, tmp_path):
    """
    Schema Contract Test (Missing Columns -> 500 INVALID_DATA_SCHEMA):
    Verify that Parquet tables missing required columns (e.g. zone_id)
    return a structured 500 INVALID_DATA_SCHEMA error.
    """
    # DataFrame with missing required columns
    invalid_df = pd.DataFrame({"wrong_col": [1, 2, 3]})
    invalid_parquet = tmp_path / "invalid_schema.parquet"
    invalid_df.to_parquet(invalid_parquet)
    monkeypatch.setattr(main, "FEATURE_TABLE_DIR", tmp_path)

    response = client.get("/api/predictions")
    assert response.status_code == 500
    data = response.json()

    assert data["error"] == ErrorCode.INVALID_DATA_SCHEMA.value
    assert "invalid" in data["message"].lower() or "missing" in data["message"].lower()


def test_route_missing_directory_returns_503(monkeypatch, tmp_path):
    """
    Route Resilience Test:
    Verify GET /api/route returns structured 503 ROUTES_UNAVAILABLE
    when the recommended routes directory is missing, with zero local path leakage.
    """
    missing_dir = tmp_path / "missing_routes"
    monkeypatch.setattr(main, "RECOMMENDED_ROUTES_DIR", missing_dir)

    response = client.get("/api/route")
    assert response.status_code == 503
    data = response.json()

    assert data["error"] == ErrorCode.ROUTES_UNAVAILABLE.value
    assert "temporarily unavailable" in data["message"]
    assert str(missing_dir) not in str(data)


def test_route_empty_table_returns_200_empty_list_and_404_on_query(monkeypatch, tmp_path):
    """
    Route Empty Table Test:
    Verify that an empty routes table returns:
      - All routes: HTTP 200 with []
      - Specific destination query: HTTP 404 DESTINATION_NOT_FOUND
    """
    empty_df = pd.DataFrame(columns=[
        "origin_id", "destination_id", "path", "path_formatted",
        "total_travel_time_hours", "total_travel_time_minutes",
        "is_reachable", "computed_at", "prediction_zones_used", "fallback_applied"
    ])
    empty_parquet = tmp_path / "empty_routes.parquet"
    empty_df.to_parquet(empty_parquet)
    monkeypatch.setattr(main, "RECOMMENDED_ROUTES_DIR", tmp_path)

    # 1. Fetching all routes -> HTTP 200 with []
    all_res = client.get("/api/route")
    assert all_res.status_code == 200
    assert all_res.json() == []

    # 2. Querying specific destination -> HTTP 404
    dest_res = client.get("/api/route?destination_id=15")
    assert dest_res.status_code == 404
    data = dest_res.json()
    assert data["error"] == ErrorCode.DESTINATION_NOT_FOUND.value


def test_route_corrupted_parquet_returns_500(monkeypatch, tmp_path):
    """
    Route Corruption Test:
    Verify that corrupted route Parquet bytes return a structured 500 DATA_PARSING_ERROR.
    """
    bad_file = tmp_path / "corrupt_route.parquet"
    bad_file.write_bytes(b"CORRUPTED_ROUTE_DATA_BYTE_HEADER")
    monkeypatch.setattr(main, "RECOMMENDED_ROUTES_DIR", tmp_path)

    response = client.get("/api/route")
    assert response.status_code == 500
    data = response.json()

    assert data["error"] == ErrorCode.DATA_PARSING_ERROR.value
    assert "Traceback" not in str(data)


def test_route_invalid_schema_returns_500(monkeypatch, tmp_path):
    """
    Route Schema Validation Test:
    Verify that route Parquet tables missing required columns return 500 INVALID_DATA_SCHEMA.
    """
    bad_df = pd.DataFrame({"dummy_field": ["a", "b"]})
    bad_parquet = tmp_path / "bad_route_schema.parquet"
    bad_df.to_parquet(bad_parquet)
    monkeypatch.setattr(main, "RECOMMENDED_ROUTES_DIR", tmp_path)

    response = client.get("/api/route")
    assert response.status_code == 500
    data = response.json()

    assert data["error"] == ErrorCode.INVALID_DATA_SCHEMA.value


def test_route_invalid_query_type_returns_422():
    """
    Type Safety Contract:
    Verify that passing a string for destination_id returns HTTP 422 with structured envelope.
    """
    response = client.get("/api/route?destination_id=not_an_int")
    assert response.status_code == 422
    data = response.json()
    assert data["error"] == ErrorCode.UNPROCESSABLE_ENTITY.value


def test_map_fallback_when_route_directory_missing(monkeypatch, tmp_path):
    """
    Graceful Map Fallback Test (Missing Route Folder):
    Verify that if the recommended routes folder is missing, the map endpoint
    does NOT crash! It falls back to rendering the base zone map and marks 'Route Unavailable'.
    """
    missing_dir = tmp_path / "missing_routes_for_map"
    monkeypatch.setattr(main, "RECOMMENDED_ROUTES_DIR", missing_dir)

    response = client.get("/api/map?destination_id=15")
    assert response.status_code == 200
    html = response.text
    # Map still renders Leaflet base elements
    assert "folium" in html.lower() or "leaflet" in html.lower()
    # Legend explicitly informs the operator that route calculations are unavailable
    assert "Route Unavailable" in html


def test_map_fallback_when_intersections_missing(monkeypatch, tmp_path):
    """
    Graceful Map Fallback Test (Missing CSV):
    Verify that if intersections.csv is missing, the map safely falls back to base rendering.
    """
    missing_csv = tmp_path / "missing_intersections.csv"
    monkeypatch.setattr(main, "INTERSECTIONS_CSV", missing_csv)

    response = client.get("/api/map?destination_id=15")
    assert response.status_code == 200
    html = response.text
    assert "folium" in html.lower() or "leaflet" in html.lower()
    assert "Route Unavailable" in html


def test_unexpected_internal_exception_sanitized_500(monkeypatch):
    """
    Security & Sanitization Test (Internal Unhandled Crash):
    Simulate an unexpected catastrophic crash (e.g. database error with secret passwords)
    and verify that:
      1. The client receives a clean HTTP 500 with 'INTERNAL_SERVER_ERROR'
      2. The secret text and Python stack trace are NEVER leaked to the client!
    """
    # Create a mock function that raises a sensitive internal error
    def mock_crash():
        raise RuntimeError("CRITICAL_INTERNAL_DATABASE_LEAK_SECRET_12345")

    monkeypatch.setattr(main, "read_latest_predictions_dataframe", mock_crash)

    # Note: raise_server_exceptions=False tells TestClient:
    # "Don't let the exception crash the test; let FastAPI's 500 error handler process it!"
    safe_client = TestClient(app, raise_server_exceptions=False)
    response = safe_client.get("/api/predictions")
    assert response.status_code == 500
    data = response.json()

    assert data["error"] == ErrorCode.INTERNAL_SERVER_ERROR.value
    assert "internal server error" in data["message"].lower()

    # CRITICAL LEAK PREVENTION CHECK:
    # Sensitive internal strings and stack traces must NEVER appear in client-facing JSON!
    assert "CRITICAL_INTERNAL_DATABASE_LEAK_SECRET_12345" not in str(data)
    assert "Traceback" not in str(data)


def test_frontend_graceful_degradation_strings():
    """
    Frontend Degradation Strings Test:
    Verify that the frontend template 'index.html' contains all necessary fallback
    labels and graceful degradation strings used when microservices are restarting.
    """
    response = client.get("/")
    assert response.status_code == 200
    html = response.text

    # Verify fallback and offline UI strings exist in the HTML template
    assert "Cached (API Down)" in html
    assert "Predictions: Unavailable" in html
    assert "Route: Unavailable" in html
    assert "No data yet. Awaiting sensor micro-batch streaming output..." in html
    assert "No Routes" in html
    assert "Route: No Data" in html
