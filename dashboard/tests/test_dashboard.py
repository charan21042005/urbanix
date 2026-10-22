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

import main
from main import (
    app,
    get_parquet_files,
    load_zone_coordinates,
    determine_speed_status,
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
