# ==============================================================================
# Urbanix — Day 10: Dashboard Regression Test Suite
# test_dashboard.py — Fast, Read-Only Unit & API Contract Verification Tests
# ==============================================================================

import sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

# Add dashboard directory to sys.path
DASHBOARD_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(DASHBOARD_DIR))

from datetime import datetime, timezone, timedelta
import main
from main import (
    app,
    get_parquet_files,
    load_zone_coordinates,
    load_intersections_registry,
    build_route_coordinates,
    determine_speed_status,
    evaluate_freshness,
    ROUTES_UPDATE_INTERVAL_SECONDS,
    PREDICTIONS_UPDATE_INTERVAL_SECONDS,
    STALE_THRESHOLD_MULTIPLIER,
    ROUTES_STALE_THRESHOLD_SECONDS,
    PREDICTIONS_STALE_THRESHOLD_SECONDS,
    REPO_ROOT,
    FEATURE_TABLE_DIR,
    RECOMMENDED_ROUTES_DIR,
    INTERSECTIONS_CSV,
)

client = TestClient(app)


# ------------------------------------------------------------------------------
# 1. Path & File Handling Tests
# ------------------------------------------------------------------------------

def test_repo_paths_exist():
    """Verify that all core project paths resolve correctly."""
    assert REPO_ROOT.exists(), f"Repo root does not exist: {REPO_ROOT}"
    assert FEATURE_TABLE_DIR.exists(), f"Feature table path does not exist: {FEATURE_TABLE_DIR}"
    assert RECOMMENDED_ROUTES_DIR.exists(), f"Routes path does not exist: {RECOMMENDED_ROUTES_DIR}"
    assert INTERSECTIONS_CSV.exists(), f"Intersections CSV does not exist: {INTERSECTIONS_CSV}"


def test_parquet_filtering_ignores_non_parquet():
    """
    Verify that get_parquet_files strictly discovers only .parquet files
    and ignores README.md, _SUCCESS, and hidden .crc files.
    """
    ft_files = get_parquet_files(FEATURE_TABLE_DIR)
    assert len(ft_files) > 0, "No parquet files found in feature table"
    for f in ft_files:
        assert f.endswith(".parquet"), f"Discovered non-parquet file: {f}"
        assert not Path(f).name.startswith("."), f"Discovered hidden file: {f}"
        assert not Path(f).name.startswith("_"), f"Discovered metadata file: {f}"
        assert "README" not in f, f"Discovered README in parquet files: {f}"

    rr_files = get_parquet_files(RECOMMENDED_ROUTES_DIR)
    assert len(rr_files) > 0, "No parquet files found in recommended routes"
    for f in rr_files:
        assert f.endswith(".parquet"), f"Discovered non-parquet file: {f}"


# ------------------------------------------------------------------------------
# 2. Geographic & Mathematical Mapping Tests
# ------------------------------------------------------------------------------

def test_load_zone_coordinates():
    """
    Verify that zone centroids are computed honestly from intersections.csv
    and lie within the Los Angeles geographic bounding box.
    """
    centroids = load_zone_coordinates()
    assert len(centroids) == 5, f"Expected 5 zones, got {len(centroids)}"

    for zone_id, coords in centroids.items():
        assert zone_id.startswith("zone_")
        assert "lat" in coords and "lon" in coords
        # Greater Los Angeles bounding box sanity check:
        # Lat: 33.5 to 34.5 N, Lon: -118.7 to -117.8 W
        assert 33.5 <= coords["lat"] <= 34.5, f"Invalid latitude for {zone_id}: {coords['lat']}"
        assert -118.7 <= coords["lon"] <= -117.8, f"Invalid longitude for {zone_id}: {coords['lon']}"


def test_speed_status_thresholds():
    """Verify Level of Service (LOS) color classifications."""
    fast = determine_speed_status(75.0)
    assert fast["status"] == "FREE_FLOWING"
    assert fast["color"] == "green"

    moderate = determine_speed_status(45.0)
    assert moderate["status"] == "MODERATE"
    assert moderate["color"] == "orange"

    congested = determine_speed_status(15.0)
    assert congested["status"] == "CONGESTED"
    assert congested["color"] == "red"


# ------------------------------------------------------------------------------
# 3. FastAPI Endpoint Tests
# ------------------------------------------------------------------------------

def test_get_predictions_endpoint():
    """Verify GET /api/predictions returns valid schema and real data for all 5 zones."""
    response = client.get("/api/predictions")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"

    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 5, f"Expected 5 zones in predictions, got {len(data)}"

    zone_ids = [item["zone_id"] for item in data]
    assert sorted(zone_ids) == ["zone_1", "zone_2", "zone_3", "zone_4", "zone_5"]

    for item in data:
        assert "zone_name" in item
        assert "predicted_speed_kmph" in item
        assert isinstance(item["predicted_speed_kmph"], (int, float))
        assert item["predicted_speed_kmph"] > 0, "Predicted speed must be positive"
        assert item["status"] in ["FREE_FLOWING", "MODERATE", "CONGESTED"]
        assert item["predicted_pm25"] is None, "AQI/PM2.5 must be None per Day 6/9 scope honesty"
        assert "traffic_record_count" in item
        assert "latitude" in item and "longitude" in item


def test_get_route_endpoint():
    """Verify GET /api/route returns GraphX SSSP recommendations."""
    response = client.get("/api/route")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"

    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 20, f"Expected 20 routes from Origin 1, got {len(data)}"

    # Check structure of first route (Origin 1 to Destination 1)
    route_1 = next(r for r in data if r["destination_id"] == 1)
    assert route_1["origin_id"] == 1
    assert route_1["total_travel_time_hours"] == 0.0
    assert route_1["path"] == [1]

    # Check reachable route to Destination 8
    route_8 = next(r for r in data if r["destination_id"] == 8)
    assert route_8["is_reachable"] is True
    assert route_8["total_travel_time_minutes"] > 0
    assert len(route_8["path"]) >= 2
    assert route_8["path_formatted"] == "1 -> 2 -> 3 -> 5 -> 7 -> 8"


def test_get_route_filtered_by_destination():
    """Verify GET /api/route?destination_id=15 filters correctly."""
    response = client.get("/api/route?destination_id=15")
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 1
    assert data[0]["destination_id"] == 15
    assert data[0]["path_formatted"] == "1 -> 2 -> 14 -> 16 -> 15"


def test_get_route_invalid_destination():
    """Verify GET /api/route with nonexistent destination returns 404."""
    response = client.get("/api/route?destination_id=999")
    assert response.status_code == 404


def test_get_map_endpoint():
    """Verify GET /api/map returns valid HTML with embedded Leaflet map."""
    response = client.get("/api/map")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]

    html = response.text
    assert "<!DOCTYPE html>" in html or "<div" in html
    assert "leaflet" in html.lower()
    assert "Urbanix Traffic Legend" in html
    assert "Free Flowing" in html


def test_root_index_page():
    """Verify GET / serves index.html with live polling script."""
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]

    html = response.text
    assert "URBANIX DASHBOARD" in html
    assert "REFRESH_INTERVAL_MS = 15000" in html
    assert "<iframe" in html
    assert "map-frame" in html


def test_cors_headers():
    """Verify that local development origins receive proper CORS headers."""
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
    Verify that /api/predictions returns actual values from data/feature_table/traffic
    and does NOT return hardcoded mock numbers.
    """
    response = client.get("/api/predictions")
    data = response.json()
    zone_1 = next(z for z in data if z["zone_id"] == "zone_1")
    # Zone 1 speed in baseline feature table is 77.67 km/h
    assert abs(zone_1["predicted_speed_kmph"] - 77.67) < 0.1


# ------------------------------------------------------------------------------
# 4. Route Overlay & Coordinate Verification Tests (Day 11 — FR-8)
# ------------------------------------------------------------------------------

def test_load_intersections_registry():
    """Verify that intersections registry indexes all 20 nodes with valid numeric lat/lon."""
    registry = load_intersections_registry()
    assert len(registry) == 20, f"Expected 20 intersections, got {len(registry)}"
    
    # Check Node 1 (Origin)
    assert 1 in registry
    assert registry[1]["name"] == "Grand Ave & 1st St"
    assert registry[1]["zone_id"] == "zone_1"
    assert abs(registry[1]["lat"] - 34.0549) < 0.001
    assert abs(registry[1]["lon"] - (-118.2498)) < 0.001
    
    # Check Node 15 (Canonical destination)
    assert 15 in registry
    assert abs(registry[15]["lat"] - 33.9984) < 0.001
    assert abs(registry[15]["lon"] - (-118.0874)) < 0.001


def test_build_route_coordinates_ordered_sequence():
    """
    Verify that build_route_coordinates transforms path [1, 2, 14, 16, 15] into
    the exact ordered sequence of physical latitude and longitude coordinates.
    """
    registry = load_intersections_registry()
    path = [1, 2, 14, 16, 15]
    coords = build_route_coordinates(path, registry)
    
    assert len(coords) == 5, f"Expected 5 coordinate pairs, got {len(coords)}"
    
    # Verify exact coordinate sequence matches the path node order
    assert coords[0] == (registry[1]["lat"], registry[1]["lon"])
    assert coords[1] == (registry[2]["lat"], registry[2]["lon"])
    assert coords[2] == (registry[14]["lat"], registry[14]["lon"])
    assert coords[3] == (registry[16]["lat"], registry[16]["lon"])
    assert coords[4] == (registry[15]["lat"], registry[15]["lon"])


def test_build_route_coordinates_aborts_on_missing_node():
    """
    Data Integrity Test: If ANY node in the route path is missing from the registry,
    build_route_coordinates MUST abort and return an empty list. It must NEVER draw
    a partial or misleading route.
    """
    registry = load_intersections_registry()
    path_with_missing_node = [1, 2, 999, 15]  # Node 999 does not exist
    coords = build_route_coordinates(path_with_missing_node, registry)
    assert coords == [], f"Expected empty list on missing node ID, got {coords}"


def test_build_route_coordinates_aborts_on_invalid_coordinates():
    """
    Data Integrity Test: If an intersection has out-of-bounds latitude (e.g. 55.0 N)
    or NaN, build_route_coordinates must abort and return an empty list.
    """
    mock_registry = {
        1: {"name": "Node 1", "zone_id": "zone_1", "lat": 34.0549, "lon": -118.2498},
        2: {"name": "Node 2", "zone_id": "zone_1", "lat": 55.0000, "lon": -118.2543},  # Out of bounds
    }
    coords = build_route_coordinates([1, 2], mock_registry)
    assert coords == [], "Expected empty list on out-of-bounds latitude"


def test_build_route_coordinates_handles_malformed_inputs():
    """Verify safe handling of empty paths, single-node paths, None, and non-numeric lists."""
    registry = load_intersections_registry()
    assert build_route_coordinates([], registry) == []
    assert build_route_coordinates([1], registry) == []  # Single node cannot form an edge
    assert build_route_coordinates(None, registry) == []
    assert build_route_coordinates(["invalid", "path"], registry) == []


def test_get_map_with_route_overlay_destination_15():
    """
    Contract Test: Verify GET /api/map?destination_id=15 renders Folium PolyLine
    for the baseline GraphX route [1, 2, 14, 16, 15] with ETA and origin/destination markers.
    """
    response = client.get("/api/map?destination_id=15")
    assert response.status_code == 200
    html = response.text
    
    # Check that PolyLine is rendered
    assert "polyline" in html.lower() or "l.polyline" in html.lower()
    
    # Check that route metadata appears in HTML / tooltip / popups
    assert "Recommended Route" in html
    assert "Node 1" in html
    assert "Node 15" in html
    assert "19.02" in html or "19.0" in html
    
    # Check that legend contains route entry
    assert "Recommended Route (Pregel SSSP)" in html
    
    # Check that zone circles still exist
    assert "Urbanix Traffic Legend" in html


def test_get_map_graceful_on_nonexistent_destination():
    """
    Graceful Degradation Test: If an invalid or uncomputed destination ID (e.g. 999) is requested,
    GET /api/map must NOT crash with 500. It must gracefully render the zone heatmap.
    """
    response = client.get("/api/map?destination_id=999")
    assert response.status_code == 200
    html = response.text
    assert "Urbanix Traffic Legend" in html
    assert "Free Flowing" in html


# ------------------------------------------------------------------------------
# 5. Freshness Evaluation & Timestamp Semantics Tests (Day 11 — Phase 3)
# ------------------------------------------------------------------------------

def test_freshness_helper_below_threshold():
    """Verify age strictly below threshold is marked fresh (is_stale=False)."""
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    ts = datetime(2026, 10, 23, 11, 59, 0, tzinfo=timezone.utc)  # 60s old vs 180s threshold
    result = evaluate_freshness(ts, threshold_seconds=180.0, reference_time=ref_time)
    assert result["is_stale"] is False
    assert result["freshness_status"] == "FRESH"
    assert result["age_seconds"] == 60.0
    assert result["computed_at"] == "2026-10-23T11:59:00Z"


def test_freshness_helper_exact_threshold():
    """Boundary Condition: Age exactly equal to threshold is marked stale (is_stale=True)."""
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    ts = datetime(2026, 10, 23, 11, 57, 0, tzinfo=timezone.utc)  # Exactly 180s old
    result = evaluate_freshness(ts, threshold_seconds=180.0, reference_time=ref_time)
    assert result["is_stale"] is True
    assert result["freshness_status"] == "STALE"
    assert result["age_seconds"] == 180.0


def test_freshness_helper_above_threshold():
    """Boundary Condition: Age strictly greater than threshold is marked stale (is_stale=True)."""
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    ts = datetime(2026, 10, 23, 11, 56, 59, tzinfo=timezone.utc)  # 181s old
    result = evaluate_freshness(ts, threshold_seconds=180.0, reference_time=ref_time)
    assert result["is_stale"] is True
    assert result["freshness_status"] == "STALE"
    assert result["age_seconds"] == 181.0


def test_freshness_helper_missing_timestamp():
    """Verify missing, empty, or NaT timestamps are safely flagged stale, not fresh."""
    assert evaluate_freshness(None, 180.0)["is_stale"] is True
    assert evaluate_freshness("", 180.0)["is_stale"] is True
    assert evaluate_freshness("   ", 180.0)["is_stale"] is True
    assert evaluate_freshness(None, 180.0)["computed_at"] is None
    assert evaluate_freshness(None, 180.0)["freshness_status"] == "MISSING_TIMESTAMP"


def test_freshness_helper_malformed_timestamp():
    """Verify malformed timestamps do not crash and are flagged stale."""
    result = evaluate_freshness("invalid-date-format-xyz", 180.0)
    assert result["is_stale"] is True
    assert result["computed_at"] is None
    assert result["freshness_status"] == "MALFORMED_TIMESTAMP"


def test_freshness_helper_timezone_aware_normalization():
    """Verify timezone-aware timestamps (+05:30) are correctly normalized to UTC."""
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    aware_str = "2026-10-23T17:00:00+05:30"  # 11:30:00 UTC (1800s old)
    result = evaluate_freshness(aware_str, threshold_seconds=3600.0, reference_time=ref_time)
    assert result["computed_at"] == "2026-10-23T11:30:00Z"
    assert result["age_seconds"] == 1800.0
    assert result["is_stale"] is False


def test_freshness_helper_naive_interpreted_as_utc():
    """Verify timezone-naive timestamps from Parquet are explicitly localized to UTC."""
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    naive_str = "2026-10-23 11:58:00"  # 120s old
    result = evaluate_freshness(naive_str, threshold_seconds=180.0, reference_time=ref_time)
    assert result["computed_at"] == "2026-10-23T11:58:00Z"
    assert result["age_seconds"] == 120.0
    assert result["is_stale"] is False


def test_freshness_helper_future_timestamp_clock_skew():
    """Verify future timestamps beyond skew tolerance (-5.0s) are flagged as FUTURE_CLOCK_SKEW and stale."""
    ref_time = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc)
    future_ts = datetime(2026, 10, 23, 12, 5, 0, tzinfo=timezone.utc)  # 5 minutes in future
    result = evaluate_freshness(future_ts, threshold_seconds=180.0, reference_time=ref_time)
    assert result["is_stale"] is True
    assert result["freshness_status"] == "FUTURE_CLOCK_SKEW"
    assert result["age_seconds"] == -300.0


def test_configured_constants():
    """Verify configured update intervals and threshold multiplier."""
    assert ROUTES_UPDATE_INTERVAL_SECONDS == 60.0
    assert PREDICTIONS_UPDATE_INTERVAL_SECONDS == 300.0
    assert STALE_THRESHOLD_MULTIPLIER == 3.0
    assert ROUTES_STALE_THRESHOLD_SECONDS == 180.0
    assert PREDICTIONS_STALE_THRESHOLD_SECONDS == 900.0


def test_predictions_api_freshness_metadata():
    """Verify /api/predictions includes computed_at, is_stale, and age_seconds derived from window_end."""
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
        # Check backward-compatibility: predicted_pm25 is still None
        assert item["predicted_pm25"] is None


def test_route_api_freshness_metadata():
    """Verify /api/route includes computed_at, is_stale, and age_seconds derived from route snapshot."""
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
    assert r["is_stale"] is True  # Snapshot from 2026-10-22 is > 180s old
    assert r["age_seconds"] > 180.0


def test_route_api_invalid_query_type_returns_422():
    """Verify invalid query parameter type (e.g. string for int destination_id) returns HTTP 422."""
    response = client.get("/api/route?destination_id=invalid_id")
    assert response.status_code == 422


def test_map_stale_route_styling_and_tooltip():
    """Verify GET /api/map?destination_id=15 renders gray PolyLine (#6B7280) and STALE ROUTE indicators."""
    response = client.get("/api/map?destination_id=15")
    assert response.status_code == 200
    html = response.text

    assert "STALE ROUTE" in html
    assert "#6B7280" in html
    assert "Recommended Route (Pregel SSSP) [STALE]" in html


def test_map_fresh_route_styling_with_injected_snapshot(monkeypatch):
    """
    Verify GET /api/map renders royal blue PolyLine (#2563EB) when route snapshot is fresh.
    Uses monkeypatch to inject fresh computed_at without mutating production datasets.
    """
    import main as main_module
    orig_read = main_module.read_recommended_routes_dataframe
    
    def mock_fresh_routes():
        df = orig_read().copy()
        now_utc = datetime.now(timezone.utc)
        df["computed_at"] = now_utc  # Fresh timestamp (0s old)
        return df

    monkeypatch.setattr(main_module, "read_recommended_routes_dataframe", mock_fresh_routes)

    response = client.get("/api/map?destination_id=15")
    assert response.status_code == 200
    html = response.text

    assert "#2563EB" in html
    assert "FRESH ROUTE" in html
    assert "Recommended Route (Pregel SSSP) [Fresh]" in html


def test_frontend_stale_indicators_and_polling_script():
    """Verify GET / serves HTML with status badges, 15s interval, and concurrency guard."""
    response = client.get("/")
    assert response.status_code == 200
    html = response.text

    # Freshness badges
    assert "badge-pred-header" in html
    assert "badge-route-header" in html
    assert "chip-pred-badge" in html
    assert "chip-route-badge" in html

    # Polling cadence and concurrency guard
    assert "REFRESH_INTERVAL_MS = 15000" in html
    assert "isRefreshing" in html
    assert "updateRouteStatus" in html


