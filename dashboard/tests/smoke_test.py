"""
========================================================================================
🔥 URBANIX DASHBOARD — LIVE HTTP ENDPOINT SMOKE TEST SUITE
========================================================================================
Welcome to the Urbanix Smoke Test script! 👋

WHAT IS A "SMOKE TEST"?
In software engineering, the term "smoke test" comes from electronics hardware:
When engineers built a new circuit board, they turned it on for the very first time.
If smoke came out of the board, they knew something was critically broken immediately!

In modern web development, a smoke test is a fast, non-destructive test suite that sends
real HTTP network requests to a live running web server. It verifies that the server
is alive, listening on its assigned port, and responding with valid data across all
its primary web and API endpoints before deeper automated or manual testing begins.

WHAT DOES THIS SCRIPT VERIFY?
This script tests 9 core HTTP contracts against our live running FastAPI / Uvicorn server:
  1. GET /                                      -> Main HTML web dashboard (HTTP 200)
  2. GET /docs                                  -> Interactive OpenAPI documentation (HTTP 200)
  3. GET /api/predictions                       -> 5-zone speed forecasting telemetry (HTTP 200)
  4. GET /api/route                             -> All 20 Pregel GraphX shortest paths (HTTP 200)
  5. GET /api/route?destination_id=15           -> Specific route recommendation query (HTTP 200)
  6. GET /api/map                               -> Dynamic Leaflet Folium HTML canvas (HTTP 200)
  7. GET /api/map?destination_id=15             -> Map with route vector overlay (HTTP 200)
  8. GET /api/route?destination_id=invalid_id   -> Input validation error contract (HTTP 422)
  9. GET /api/route?destination_id=999          -> Nonexistent destination 404 error (HTTP 404)
========================================================================================
"""

# =====================================================================
# 📦 STEP 1: IMPORT REQUIRED STANDARD & THIRD-PARTY LIBRARIES
# =====================================================================

# 'sys' is a built-in Python module (System-specific parameters and functions).
# We use it here for two things:
#   1. Reading command-line arguments (sys.argv)
#   2. Exiting the script with a status code (sys.exit(0) for success, sys.exit(1) for failure)
import sys

# 'os' is Python's built-in Operating System interaction module.
# We use it to read environment variables (like URBANIX_BASE_URL) if configured.
import os

# 'requests' is Python's most popular third-party HTTP library.
# Just like a web browser sends requests when you type a website URL,
# 'requests.get()' allows Python code to send HTTP GET requests over the network!
import requests


# =====================================================================
# ⚙️ STEP 2: DEFINE SERVER CONFIGURATION & TARGET URL
# =====================================================================

# By convention in Python, variables written in ALL_CAPS are constants (values that don't change).
# We allow the user to override the target URL via:
#   1. A command-line argument: python smoke_test.py http://127.0.0.1:8085
#   2. An environment variable: $env:URBANIX_BASE_URL="http://127.0.0.1:8085"
#   3. Default fallback: http://127.0.0.1:8000 (standard Uvicorn port)
if len(sys.argv) > 1:
    # If the user passed an argument in the terminal (e.g. 'python smoke_test.py http://127.0.0.1:8085')
    BASE_URL = sys.argv[1].rstrip("/")
else:
    # Otherwise, check environment variable, or fall back to default localhost:8000
    BASE_URL = os.getenv("URBANIX_BASE_URL", "http://127.0.0.1:8000").rstrip("/")


# =====================================================================
# 🧪 STEP 3: MAIN TEST RUNNER FUNCTION
# =====================================================================

def run_smoke_test() -> bool:
    """
    Executes all 9 live HTTP endpoint checks sequentially.

    Returns:
        bool: True if every single test passes without error; False if any test fails.
    """
    # Visual banner in the terminal: string multiplication ("=" * 60) prints 60 equal signs in a row!
    print("=" * 60)
    print("URBANIX DAY 10: LIVE HTTP ENDPOINT VERIFICATION")
    # An f-string (f"...") allows us to insert variable values directly inside curly braces {BASE_URL}
    print(f"Target: {BASE_URL}")
    print("=" * 60)

    # -----------------------------------------------------------------
    # TEST 1: Root Endpoint (GET /)
    # Purpose: Verifies the main dashboard HTML page loads successfully.
    # -----------------------------------------------------------------
    try:
        # requests.get() makes a network GET call.
        # 'timeout=5' is a crucial best practice! It tells Python:
        # "If the server doesn't respond within 5 seconds, stop waiting and raise an error."
        # This prevents the test script from freezing forever if the server is hung.
        r_root = requests.get(f"{BASE_URL}/", timeout=5)

        # Print the HTTP response status code (e.g. 200 = Success, 404 = Not Found)
        print(f"[PASS] GET / -> HTTP {r_root.status_code}")
        # Headers contain metadata about the response. 'content-type' tells us if it's HTML, JSON, etc.
        print(f"       Content-Type: {r_root.headers.get('content-type')}")
        # len(r_root.text) counts how many text characters were returned in the HTML document
        print(f"       Payload size: {len(r_root.text)} bytes")

        # 'assert' is Python's way of testing expectations.
        # If the condition is True, nothing happens and code continues.
        # If the condition is False, Python immediately raises an 'AssertionError' exception!
        assert r_root.status_code == 200, f"Expected HTTP 200, got {r_root.status_code}"
        # Verify that the word "URBANIX" is actually inside the returned HTML content
        assert "URBANIX" in r_root.text, "Keyword 'URBANIX' not found in root HTML page"

    except Exception as e:
        # 'except Exception as e:' catches any error (connection failed, timeout, or assertion failed)
        # Printing the error allows the developer to immediately see what went wrong!
        print(f"[FAIL] GET / failed: {e}")
        return False  # Early exit: test suite fails!

    # -----------------------------------------------------------------
    # TEST 2: API Documentation (GET /docs)
    # Purpose: Verifies FastAPI's automated Swagger UI documentation is available.
    # -----------------------------------------------------------------
    try:
        r_docs = requests.get(f"{BASE_URL}/docs", timeout=5)
        print(f"[PASS] GET /docs -> HTTP {r_docs.status_code}")
        assert r_docs.status_code == 200, f"Expected HTTP 200, got {r_docs.status_code}"

    except Exception as e:
        print(f"[FAIL] GET /docs failed: {e}")
        return False

    # -----------------------------------------------------------------
    # TEST 3: Traffic Speed Predictions (GET /api/predictions)
    # Purpose: Verifies telemetry predictions for all 5 monitored urban zones.
    # -----------------------------------------------------------------
    try:
        r_pred = requests.get(f"{BASE_URL}/api/predictions", timeout=5)
        print(f"[PASS] GET /api/predictions -> HTTP {r_pred.status_code}")

        # '.json()' automatically parses the raw JSON text string into native Python dictionaries and lists!
        data = r_pred.json()
        print(f"       Records returned: {len(data)}")

        # Loop through each prediction dictionary in the returned list
        for rec in data:
            # We use dict['key'] to read values, and .get('key') to read optional values safely
            print(f"       Zone: {rec['zone_id']}, Speed: {rec['predicted_speed_kmph']} km/h, Computed: {rec.get('computed_at')}, Stale: {rec.get('is_stale')}")
            # Ensure required telemetry metadata fields are present in every record
            assert "is_stale" in rec, "Missing 'is_stale' boolean field in prediction record"
            assert "computed_at" in rec, "Missing 'computed_at' timestamp field in prediction record"

        # Assert status code is 200 OK and exactly 5 active zones were returned
        assert r_pred.status_code == 200, f"Expected HTTP 200, got {r_pred.status_code}"
        assert len(data) == 5, f"Expected 5 zone predictions, received {len(data)}"

    except Exception as e:
        print(f"[FAIL] GET /api/predictions failed: {e}")
        return False

    # -----------------------------------------------------------------
    # TEST 4: GraphX Optimal Routes List (GET /api/route)
    # Purpose: Verifies the complete shortest-path routing table calculated by Scala GraphX.
    # -----------------------------------------------------------------
    try:
        r_routes = requests.get(f"{BASE_URL}/api/route", timeout=5)
        print(f"[PASS] GET /api/route -> HTTP {r_routes.status_code}")

        routes = r_routes.json()
        print(f"       Total routes returned: {len(routes)}")

        # Inspect the very first route recommendation in the list as a sample check
        sample = routes[0]
        # ':.2f' formats a floating-point decimal number to exactly 2 decimal places (e.g. 19.02)
        print(f"       Sample route: Origin={sample['origin_id']} -> Dest={sample['destination_id']}, ETA={sample['total_travel_time_minutes']:.2f} min, Stale={sample.get('is_stale')}")

        # Check required schema fields
        assert "is_stale" in sample, "Missing 'is_stale' field in route record"
        assert "computed_at" in sample, "Missing 'computed_at' field in route record"
        assert r_routes.status_code == 200, f"Expected HTTP 200, got {r_routes.status_code}"
        # The city road network has 20 destination vertices, so there must be 20 calculated routes!
        assert len(routes) == 20, f"Expected 20 destination routes, received {len(routes)}"

    except Exception as e:
        print(f"[FAIL] GET /api/route failed: {e}")
        return False

    # -----------------------------------------------------------------
    # TEST 5: Specific Destination Route Query (GET /api/route?destination_id=15)
    # Purpose: Tests query parameter filtering for emergency dispatch to Node 15.
    # -----------------------------------------------------------------
    try:
        # The query parameter '?destination_id=15' asks FastAPI to filter the results
        r_dest = requests.get(f"{BASE_URL}/api/route?destination_id=15", timeout=5)
        print(f"[PASS] GET /api/route?destination_id=15 -> HTTP {r_dest.status_code}")

        dest_routes = r_dest.json()
        # Filtering by a single destination should return a list with EXACTLY 1 route item
        assert len(dest_routes) == 1, f"Expected 1 filtered route, received {len(dest_routes)}"

        d15 = dest_routes[0]
        print(f"       Destination 15 route: Origin={d15['origin_id']} -> Dest={d15['destination_id']}, ETA={d15['total_travel_time_minutes']:.2f} min, Stale={d15.get('is_stale')}")

        # Assert contract guarantees
        assert r_dest.status_code == 200, f"Expected HTTP 200, got {r_dest.status_code}"
        assert d15["destination_id"] == 15, f"Expected destination_id=15, got {d15['destination_id']}"
        assert d15["origin_id"] == 1, f"Expected origin_id=1 (Civic Center), got {d15['origin_id']}"

    except Exception as e:
        print(f"[FAIL] GET /api/route?destination_id=15 failed: {e}")
        return False

    # -----------------------------------------------------------------
    # TEST 6: Base Folium Leaflet Map (GET /api/map)
    # Purpose: Verifies the Folium HTML map generator produces a valid Leaflet canvas.
    # -----------------------------------------------------------------
    try:
        r_map = requests.get(f"{BASE_URL}/api/map", timeout=5)
        print(f"[PASS] GET /api/map -> HTTP {r_map.status_code}")
        print(f"       Content-Type: {r_map.headers.get('content-type')}")
        print(f"       HTML length: {len(r_map.text)} bytes")

        assert r_map.status_code == 200, f"Expected HTTP 200, got {r_map.status_code}"
        # '.lower()' converts all characters to lowercase so comparisons are case-insensitive
        assert "leaflet" in r_map.text.lower(), "Leaflet library not detected in rendered map HTML"
        # Verify PolyLine vector route styling is present in the rendered HTML script
        assert "polyline" in r_map.text.lower() or "l.polyline" in r_map.text.lower(), "PolyLine route overlay not found in map HTML"
        assert "Recommended Route" in r_map.text, "Legend 'Recommended Route' missing from map HTML"

    except Exception as e:
        print(f"[FAIL] GET /api/map failed: {e}")
        return False

    # -----------------------------------------------------------------
    # TEST 7: Map With Specific Route Vector (GET /api/map?destination_id=15)
    # Purpose: Verifies map rendering when focused on a specific destination route.
    # -----------------------------------------------------------------
    try:
        r_map_15 = requests.get(f"{BASE_URL}/api/map?destination_id=15", timeout=5)
        print(f"[PASS] GET /api/map?destination_id=15 -> HTTP {r_map_15.status_code}")

        assert r_map_15.status_code == 200, f"Expected HTTP 200, got {r_map_15.status_code}"
        # Verify Node 15 is explicitly highlighted in the tooltip/popup text
        assert "Node 15" in r_map_15.text, "Destination 'Node 15' missing in map popup text"
        # Verify the verified travel time (~19.02 min) is stamped on the map
        assert "19.02" in r_map_15.text or "19.0" in r_map_15.text, "Expected ETA (~19.02 min) not found in map HTML"
        # Verify freshness indicator is visible in the route tooltip
        assert "STALE ROUTE" in r_map_15.text or "FRESH ROUTE" in r_map_15.text, "Freshness tag missing from map route tooltip"

    except Exception as e:
        print(f"[FAIL] GET /api/map?destination_id=15 failed: {e}")
        return False

    # -----------------------------------------------------------------
    # TEST 8: Input Validation Contract (GET /api/route?destination_id=invalid_id)
    # Purpose: Verifies FastAPI returns structured HTTP 422 when given bad query types.
    # -----------------------------------------------------------------
    try:
        # 'invalid_id' is a string of letters, but destination_id MUST be an integer!
        r_inv = requests.get(f"{BASE_URL}/api/route?destination_id=invalid_id", timeout=5)
        print(f"[PASS] GET /api/route?destination_id=invalid_id -> HTTP {r_inv.status_code} (Validation)")

        # In HTTP standards, 422 Unprocessable Entity means the input format was invalid
        assert r_inv.status_code == 422, f"Expected HTTP 422 validation error, got {r_inv.status_code}"

        data_422 = r_inv.json()
        # Verify that our custom structured error handler returned an 'UNPROCESSABLE_ENTITY' code
        assert data_422.get("error") == "UNPROCESSABLE_ENTITY", f"Expected error 'UNPROCESSABLE_ENTITY', got {data_422.get('error')}"
        print(f"       Structured 422 Error: {data_422.get('error')}")

    except Exception as e:
        print(f"[FAIL] GET /api/route?destination_id=invalid_id failed: {e}")
        return False

    # -----------------------------------------------------------------
    # TEST 9: Nonexistent Resource Contract (GET /api/route?destination_id=999)
    # Purpose: Verifies FastAPI returns structured HTTP 404 when a destination doesn't exist.
    # -----------------------------------------------------------------
    try:
        # Destination 999 does not exist in our 20-node city network
        r_404 = requests.get(f"{BASE_URL}/api/route?destination_id=999", timeout=5)
        print(f"[PASS] GET /api/route?destination_id=999 -> HTTP {r_404.status_code} (Not Found)")

        # In HTTP standards, 404 Not Found means the requested resource does not exist
        assert r_404.status_code == 404, f"Expected HTTP 404, got {r_404.status_code}"

        data_404 = r_404.json()
        # Verify that our custom structured error handler returned a 'DESTINATION_NOT_FOUND' code
        assert data_404.get("error") == "DESTINATION_NOT_FOUND", f"Expected error 'DESTINATION_NOT_FOUND', got {data_404.get('error')}"
        print(f"       Structured 404 Error: {data_404.get('error')}")

    except Exception as e:
        print(f"[FAIL] GET /api/route?destination_id=999 failed: {e}")
        return False

    # -----------------------------------------------------------------
    # SUMMARY: All 9 checks passed with zero exceptions!
    # -----------------------------------------------------------------
    print("=" * 60)
    print("ALL 9 LIVE ENDPOINT SMOKE TESTS PASSED!")
    print("=" * 60)
    return True


# =====================================================================
# 🚀 STEP 4: SCRIPT EXECUTION ENTRYPOINT
# =====================================================================

# In Python, '__name__' is a special built-in variable.
# When you run this file directly from the terminal (e.g. 'python smoke_test.py'),
# Python automatically assigns the string "__main__" to '__name__'.
# However, if another Python script imports this file ('import smoke_test'),
# '__name__' will be "smoke_test", so the code inside this block will NOT run!
if __name__ == "__main__":
    # Run the complete smoke test suite and store the True/False outcome
    success = run_smoke_test()

    # 'sys.exit(code)' tells the Operating System (Windows / Linux) whether the program succeeded:
    #   Exit Code 0: Clean success! Everything passed.
    #   Exit Code 1: Failure! Something crashed or an assertion failed.
    # CI/CD systems (like GitHub Actions) check this exact number to mark builds green or red!
    sys.exit(0 if success else 1)
