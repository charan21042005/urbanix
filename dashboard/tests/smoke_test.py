"""
URBANIX Day 10 — Live HTTP Smoke Test
Tests all endpoints against a live running Uvicorn server on http://127.0.0.1:8000.
"""

import sys
import requests

BASE_URL = "http://127.0.0.1:8000"

def run_smoke_test():
    print("=" * 60)
    print("URBANIX DAY 10: LIVE HTTP ENDPOINT VERIFICATION")
    print(f"Target: {BASE_URL}")
    print("=" * 60)

    # 1. Root /
    try:
        r_root = requests.get(f"{BASE_URL}/", timeout=5)
        print(f"[PASS] GET / -> HTTP {r_root.status_code}")
        print(f"       Content-Type: {r_root.headers.get('content-type')}")
        print(f"       Payload size: {len(r_root.text)} bytes")
        assert r_root.status_code == 200
        assert "URBANIX" in r_root.text
    except Exception as e:
        print(f"[FAIL] GET / failed: {e}")
        return False

    # 2. Docs /docs
    try:
        r_docs = requests.get(f"{BASE_URL}/docs", timeout=5)
        print(f"[PASS] GET /docs -> HTTP {r_docs.status_code}")
        assert r_docs.status_code == 200
    except Exception as e:
        print(f"[FAIL] GET /docs failed: {e}")
        return False

    # 3. GET /api/predictions
    try:
        r_pred = requests.get(f"{BASE_URL}/api/predictions", timeout=5)
        print(f"[PASS] GET /api/predictions -> HTTP {r_pred.status_code}")
        data = r_pred.json()
        print(f"       Records returned: {len(data)}")
        for rec in data:
            print(f"       Zone: {rec['zone_id']}, Speed: {rec['predicted_speed_kmph']} km/h, Computed: {rec.get('computed_at')}, Stale: {rec.get('is_stale')}")
            assert "is_stale" in rec, "Missing is_stale in prediction"
            assert "computed_at" in rec, "Missing computed_at in prediction"
        assert r_pred.status_code == 200
        assert len(data) == 5
    except Exception as e:
        print(f"[FAIL] GET /api/predictions failed: {e}")
        return False

    # 4. GET /api/route
    try:
        r_routes = requests.get(f"{BASE_URL}/api/route", timeout=5)
        print(f"[PASS] GET /api/route -> HTTP {r_routes.status_code}")
        routes = r_routes.json()
        print(f"       Total routes returned: {len(routes)}")
        sample = routes[0]
        print(f"       Sample route: Origin={sample['origin_id']} -> Dest={sample['destination_id']}, ETA={sample['total_travel_time_minutes']:.2f} min, Stale={sample.get('is_stale')}")
        assert "is_stale" in sample, "Missing is_stale in route"
        assert "computed_at" in sample, "Missing computed_at in route"
        assert r_routes.status_code == 200
        assert len(routes) == 20
    except Exception as e:
        print(f"[FAIL] GET /api/route failed: {e}")
        return False

    # 5. GET /api/route?destination_id=15
    try:
        r_dest = requests.get(f"{BASE_URL}/api/route?destination_id=15", timeout=5)
        print(f"[PASS] GET /api/route?destination_id=15 -> HTTP {r_dest.status_code}")
        dest_routes = r_dest.json()
        assert len(dest_routes) == 1
        d15 = dest_routes[0]
        print(f"       Destination 15 route: Origin={d15['origin_id']} -> Dest={d15['destination_id']}, ETA={d15['total_travel_time_minutes']:.2f} min, Stale={d15.get('is_stale')}")
        assert r_dest.status_code == 200
        assert d15["destination_id"] == 15
        assert d15["origin_id"] == 1
    except Exception as e:
        print(f"[FAIL] GET /api/route?destination_id=15 failed: {e}")
        return False

    # 6. GET /api/map
    try:
        r_map = requests.get(f"{BASE_URL}/api/map", timeout=5)
        print(f"[PASS] GET /api/map -> HTTP {r_map.status_code}")
        print(f"       Content-Type: {r_map.headers.get('content-type')}")
        print(f"       HTML length: {len(r_map.text)} bytes")
        assert r_map.status_code == 200
        assert "leaflet" in r_map.text.lower()
        assert "polyline" in r_map.text.lower() or "l.polyline" in r_map.text.lower()
        assert "Recommended Route" in r_map.text
    except Exception as e:
        print(f"[FAIL] GET /api/map failed: {e}")
        return False

    # 7. GET /api/map?destination_id=15
    try:
        r_map_15 = requests.get(f"{BASE_URL}/api/map?destination_id=15", timeout=5)
        print(f"[PASS] GET /api/map?destination_id=15 -> HTTP {r_map_15.status_code}")
        assert r_map_15.status_code == 200
        assert "Node 15" in r_map_15.text
        assert "19.02" in r_map_15.text or "19.0" in r_map_15.text
        assert "STALE ROUTE" in r_map_15.text or "FRESH ROUTE" in r_map_15.text
    except Exception as e:
        print(f"[FAIL] GET /api/map?destination_id=15 failed: {e}")
        return False

    # 8. GET /api/route?destination_id=invalid_id (Type Validation Contract)
    try:
        r_inv = requests.get(f"{BASE_URL}/api/route?destination_id=invalid_id", timeout=5)
        print(f"[PASS] GET /api/route?destination_id=invalid_id -> HTTP {r_inv.status_code} (Validation)")
        assert r_inv.status_code == 422
    except Exception as e:
        print(f"[FAIL] GET /api/route?destination_id=invalid_id failed: {e}")
        return False

    print("=" * 60)
    print("ALL 8 LIVE ENDPOINT SMOKE TESTS PASSED!")
    print("=" * 60)
    return True

if __name__ == "__main__":
    success = run_smoke_test()
    sys.exit(0 if success else 1)
