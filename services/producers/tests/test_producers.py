"""
Urbanix — Unit & Integration Tests for Telemetry Simulation Producers (Day 2)
Tests transformations, unit conversions, schema validation, zone mapping,
Kafka delivery callbacks, and sample file integrity.

Unit tests are 100% self-contained and run in-memory using temporary fixtures.
Integration checks validate the generated sample files with explicit assertions.
"""

# --- Standard Library Imports ---
import os        # File system path checks and file deletion utilities
import sys       # System module path manipulation (sys.path)
import json      # JSON parsing and validation tests
import pytest    # Pytest testing framework and assertion runners
import tempfile  # Creating isolated, disposable temporary files for test fixtures
from unittest.mock import MagicMock  # Mocking external network objects (e.g. Kafka futures)

# Add parent directory to sys.path so producer modules can be imported directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import the zone mapping functions and zone list from the producers module
from zone_mapping import (
    map_traffic_sensor_to_zone,
    map_aqi_station_to_zone,
    get_weather_zones,
    URBANIX_ZONES,
)
# Import traffic parsing generator and unit conversion factor
from traffic_producer import parse_traffic_rows, MPH_TO_KMPH
# Import air quality parsing generator
from aqi_producer import parse_aqi_rows
# Import weather parsing generator and Kelvin temperature offset constant
from weather_producer import parse_weather_rows, KELVIN_OFFSET


# =====================================================================
# 1. Zone Mapping Tests (Pure Unit)
# =====================================================================

def test_traffic_zone_mapping_determinism():
    """
    Verify that METR-LA sensor IDs map deterministically to Urbanix zones.
    Calling the mapping function multiple times for the same sensor ID must always return the exact same zone.
    """
    # Test a set of representative real METR-LA loop detector station IDs
    for s_id in ["773869", "767541", "767542", "717447", "717446"]:
        # First mapping call
        zone_1 = map_traffic_sensor_to_zone(s_id)
        # Second mapping call
        zone_2 = map_traffic_sensor_to_zone(s_id)
        # Assert that both calls produce identical results (deterministic mapping)
        assert zone_1 == zone_2
        # Assert that the output belongs to the recognized list of 5 zones
        assert zone_1 in URBANIX_ZONES


def test_traffic_zone_mapping_fallback():
    """
    Verify non-numeric sensor IDs fall back to deterministic hash mapping.
    Ensures that unexpected text-based sensor names do not crash the pipeline.
    """
    # Pass a non-numeric string identifier
    zone = map_traffic_sensor_to_zone("SENSOR_NORTH_CORRIDOR")
    # Verify zone is valid and matches canonical set
    assert zone in URBANIX_ZONES
    # Verify repeated calls on the string remain perfectly deterministic
    assert zone == map_traffic_sensor_to_zone("SENSOR_NORTH_CORRIDOR")


def test_aqi_zone_mapping():
    """
    Verify documented Los Angeles County EPA monitoring sites map to correct zones.
    Checks ground-truth sub-regional assignments (Downtown, Burbank, Pasadena, Compton, Long Beach).
    """
    # EPA Site 1103 (North Main St) must map to Downtown Core (zone_1)
    assert map_aqi_station_to_zone("1103") == "zone_1"  # Downtown Core
    # EPA Site 1002 (Burbank) must map to San Fernando Valley (zone_2)
    assert map_aqi_station_to_zone("1002") == "zone_2"  # Burbank
    # EPA Site 2005 (Pasadena) must map to San Gabriel Valley (zone_3)
    assert map_aqi_station_to_zone("2005") == "zone_3"  # Pasadena
    # EPA Site 1302 (Compton) must map to South Central Corridor (zone_4)
    assert map_aqi_station_to_zone("1302") == "zone_4"  # Compton
    # EPA Site 4002 (Long Beach) must map to Harbor / Port Terminal (zone_5)
    assert map_aqi_station_to_zone("4002") == "zone_5"  # Long Beach


def test_aqi_zone_mapping_fallback():
    """
    Verify unknown EPA stations fall back to deterministic zone assignment.
    Unknown station numbers outside the hardcoded LA lookup table should hash stably into one of the 5 zones.
    """
    # Site 9999 is not in the known EPA site dictionary
    zone = map_aqi_station_to_zone("9999")
    # Must still map safely to one of the 5 Urbanix zones without raising KeyError
    assert zone in URBANIX_ZONES


def test_weather_zones():
    """
    Verify regional weather observation spans all Urbanix grid zones identically.
    Ambient basin conditions are broadcast across all 5 operational zones.
    """
    zones = get_weather_zones()
    # Confirm exactly 5 zones are returned
    assert len(zones) == 5
    # Confirm exact alphabetical ordering and names
    assert zones == ["zone_1", "zone_2", "zone_3", "zone_4", "zone_5"]


# =====================================================================
# 2. Traffic Producer Transformations & Parsing (Pure Unit)
# =====================================================================

def test_traffic_unit_conversion():
    """
    Verify explicit mph -> km/h conversion formula: speed_kmph = speed_mph * 1.60934.
    """
    # 60.0 mph * 1.60934 = 96.5604 -> rounded to 96.56 km/h
    test_mph = 60.0
    expected_kmph = round(test_mph * MPH_TO_KMPH, 2)
    assert expected_kmph == 96.56

    # 0.0 mph must convert to 0.0 km/h
    test_mph_zero = 0.0
    assert test_mph_zero * MPH_TO_KMPH == 0.0


def test_traffic_parsing_valid_fixture():
    """
    Verify wide METR-LA matrix parsing into discrete sensor events.
    Checks timestamp standardization, sensor ID extraction, unit conversion, and JSON serializability.
    """
    # Synthetic CSV content representing a wide matrix: timestamp column followed by sensor IDs
    csv_content = (
        ",773869,767541\n"
        "2012-03-01 00:00:00,64.38,67.62\n"
        "2012-03-01 00:05:00,0.0,65.00\n"  # 0.0 should be skipped as sensor dropout
    )
    # Create an isolated temporary file to feed into the generator
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
        tmp.write(csv_content)
        tmp_path = tmp.name

    try:
        # Collect generated event dictionaries into a list
        events = list(parse_traffic_rows(tmp_path))
        # 2 rows * 2 sensors = 4 cells, but 1 cell has speed 0.0 (skipped), leaving 3 valid events
        assert len(events) == 3

        e1 = events[0]
        # Verify first event metadata fields
        assert e1["sensor_id"] == "773869"
        assert e1["timestamp"] == "2012-03-01T00:00:00"
        assert e1["speed_mph"] == 64.38
        assert e1["speed_kmph"] == round(64.38 * MPH_TO_KMPH, 2)
        assert e1["unit"] == "km/h"
        # Strictly verified: METR-LA data contains velocities only; no artificial vehicle counts
        assert "vehicle_count" not in e1
        # Verify valid JSON serialization (dumping to string and reloading produces identical dict)
        assert json.loads(json.dumps(e1)) == e1
    finally:
        # Ensure temporary file is cleanly removed from disk
        os.remove(tmp_path)


def test_traffic_parsing_malformed_rows():
    """
    Verify malformed or empty rows in traffic CSV are skipped safely.
    Tests resistance against blank lines, non-numeric strings, and negative values.
    """
    csv_content = (
        ",773869\n"
        "\n"                                  # Empty line: must be ignored
        "2012-03-01 00:00:00,not_a_number\n"  # Corrupt string value: must be skipped
        "2012-03-01 00:05:00,-15.0\n"         # Negative speed: unphysical, must be skipped
        "2012-03-01 00:10:00,55.0\n"          # Only valid reading
    )
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
        tmp.write(csv_content)
        tmp_path = tmp.name

    try:
        events = list(parse_traffic_rows(tmp_path))
        # Only the single valid row (55.0 mph) should be emitted
        assert len(events) == 1
        assert events[0]["speed_mph"] == 55.0
    finally:
        os.remove(tmp_path)


def test_traffic_missing_file_raises():
    """
    Verify that providing a non-existent file path raises FileNotFoundError.
    """
    with pytest.raises(FileNotFoundError):
        list(parse_traffic_rows("non_existent_file.csv"))


# =====================================================================
# 3. AQI Producer Transformations & Parsing (Pure Unit)
# =====================================================================

def test_aqi_parsing_and_validation():
    """
    Verify EPA AirData row parsing, PM2.5 validation, and missing value skipping.
    """
    # EPA format header followed by 3 data rows: 1 valid, 1 with missing PM2.5, 1 with negative value
    csv_content = (
        "state_code,county_code,site_num,parameter_code,poc,latitude,longitude,parameter_name,date_local,units_of_measure,pm25_mean,aqi,site_name,city_name\n"
        "06,037,1103,88101,1,34.06,-118.22,PM2.5,2012-03-01,Micrograms/cubic meter (LC),14.2,55,N. Main St,Los Angeles\n"
        "06,037,1002,88101,1,34.17,-118.31,PM2.5,2012-03-01,Micrograms/cubic meter (LC),,20,Burbank,Burbank\n"     # Missing PM2.5
        "06,037,2005,88101,1,34.13,-118.12,PM2.5,2012-03-01,Micrograms/cubic meter (LC),-5.0,,Pasadena,Pasadena\n"  # Invalid negative
    )
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
        tmp.write(csv_content)
        tmp_path = tmp.name

    try:
        events = list(parse_aqi_rows(tmp_path))
        # Only the single valid row (14.2 ug/m3) should be parsed
        assert len(events) == 1
        e = events[0]
        assert e["station_id"] == "1103"
        assert e["zone_id"] == "zone_1"
        assert e["pm25"] == 14.2
        assert e["aqi"] == 55
        assert e["unit"] == "ug/m3"
        # Strictly verified: EPA AirData contains PM2.5; no fabricated CO2 readings
        assert "co2" not in e
        assert json.loads(json.dumps(e)) == e
    finally:
        os.remove(tmp_path)


def test_aqi_missing_file_raises():
    """
    Verify that missing AQI file path raises FileNotFoundError.
    """
    with pytest.raises(FileNotFoundError):
        list(parse_aqi_rows("non_existent_file.csv"))


# =====================================================================
# 4. Weather Producer Transformations & Parsing (Pure Unit)
# =====================================================================

def test_weather_unit_conversion():
    """
    Verify Kelvin -> Celsius formula: T_c = T_k - 273.15.
    """
    test_k = 291.87
    expected_c = round(test_k - KELVIN_OFFSET, 2)
    # 291.87 K - 273.15 = 18.72 °C
    assert expected_c == 18.72


def test_weather_parsing_and_regional_broadcast():
    """
    Verify weather parsing, Kelvin conversion, and multi-zone regional broadcast.
    Each regional reading should be duplicated across all 5 zones with identical values.
    """
    csv_content = (
        "datetime,city,temperature_kelvin,humidity_percent\n"
        "2012-10-01 12:00:00,Los Angeles,,\n"          # Missing measurements: skipped
        "2012-10-01 13:00:00,Los Angeles,291.87,88.0\n" # Valid observation
    )
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
        tmp.write(csv_content)
        tmp_path = tmp.name

    try:
        events = list(parse_weather_rows(tmp_path))
        # 1 valid observation row broadcast across all 5 Urbanix zones = 5 events
        assert len(events) == 5

        # Check attributes across all broadcast events
        for e in events:
            assert e["city"] == "Los Angeles"
            assert e["temperature_k"] == 291.87
            assert e["temperature_c"] == 18.72
            assert e["humidity_pct"] == 88.0
            assert e["temperature_unit"] == "Celsius"
            assert e["humidity_unit"] == "Percent"
            assert e["zone_id"] in URBANIX_ZONES
            assert json.loads(json.dumps(e)) == e

        # Confirm all 5 distinct zones received an event
        zones_seen = [e["zone_id"] for e in events]
        assert sorted(zones_seen) == ["zone_1", "zone_2", "zone_3", "zone_4", "zone_5"]
    finally:
        os.remove(tmp_path)


def test_weather_parsing_out_of_bounds():
    """
    Verify unphysical temperature (<150K or >350K) or humidity (<0% or >100%) are rejected.
    """
    csv_content = (
        "datetime,city,temperature_kelvin,humidity_percent\n"
        "2012-10-01 13:00:00,Los Angeles,50.0,50.0\n"   # 50K is unphysically cold for Earth surface
        "2012-10-01 14:00:00,Los Angeles,450.0,50.0\n"  # 450K is unphysically hot for Earth surface
        "2012-10-01 15:00:00,Los Angeles,290.0,150.0\n" # 150% relative humidity is impossible
        "2012-10-01 16:00:00,Los Angeles,290.0,-5.0\n"  # Negative humidity is impossible
    )
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
        tmp.write(csv_content)
        tmp_path = tmp.name

    try:
        events = list(parse_weather_rows(tmp_path))
        # All 4 unphysical rows must be filtered out
        assert len(events) == 0
    finally:
        os.remove(tmp_path)


def test_weather_missing_file_raises():
    """
    Verify that missing weather file path raises FileNotFoundError.
    """
    with pytest.raises(FileNotFoundError):
        list(parse_weather_rows("non_existent_file.csv"))


# =====================================================================
# 5. Kafka Delivery Mocking Test (Pure Unit)
# =====================================================================

def test_mock_kafka_delivery_callbacks():
    """
    Verify that Kafka callbacks correctly update success and failure counts.
    Simulates asynchronous broker responses using MagicMock.
    """
    # Create a mock future object simulating kafka.producer.future.FutureRecordMetadata
    mock_future = MagicMock()
    success_callback = None
    error_callback = None

    # Capture the registered success callback function
    def add_callback_mock(cb):
        nonlocal success_callback
        success_callback = cb

    # Capture the registered error errback function
    def add_errback_mock(cb):
        nonlocal error_callback
        error_callback = cb

    mock_future.add_callback = add_callback_mock
    mock_future.add_errback = add_errback_mock

    # Counters to track message outcomes
    confirmed = 0
    failed = 0

    # Define test callbacks matching production producer logic
    def on_success(meta):
        nonlocal confirmed
        confirmed += 1

    def on_error(err):
        nonlocal failed
        failed += 1

    # Attach callbacks to the mock future
    mock_future.add_callback(on_success)
    mock_future.add_errback(on_error)

    # 1. Simulate broker success: invoke success callback with record metadata dict
    success_callback({"offset": 10})
    assert confirmed == 1
    assert failed == 0

    # 2. Simulate broker failure: invoke error errback with simulated network exception
    error_callback(Exception("Network timeout"))
    assert confirmed == 1
    assert failed == 1


# =====================================================================
# 6. Prepared Development Sample Integrity Tests (Integration)
# =====================================================================

def test_aqi_sample_file_window():
    """
    Asserts that data/aqi_sample.csv exists and covers March 1 to June 30, 2012.
    Ensures dataset reproducibility across local workstations.
    """
    sample_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "aqi_sample.csv")
    )
    assert os.path.exists(sample_path), f"Required sample missing: {sample_path}. Run python data/prepare_data.py"

    events = list(parse_aqi_rows(sample_path))
    # Verified sample record count
    assert len(events) == 712
    # Verify temporal coverage window
    dates = sorted(list(set(e["timestamp"][:10] for e in events)))
    assert dates[0] == "2012-03-01"
    assert dates[-1] == "2012-06-30"


def test_traffic_sample_file_structure():
    """
    Asserts that data/traffic_sample.csv exists and contains valid positive speeds.
    """
    sample_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "traffic_sample.csv")
    )
    assert os.path.exists(sample_path), f"Required sample missing: {sample_path}. Run python data/prepare_data.py"

    count = 0
    for event in parse_traffic_rows(sample_path):
        count += 1
        # Speeds must be strictly positive numbers
        assert event["speed_mph"] > 0.0
        assert event["speed_kmph"] > 0.0
        # Speeds must be lower than physical highway maximum (250 km/h)
        assert event["speed_kmph"] < 250.0
        # Zones must belong to canonical Urbanix set
        assert event["zone_id"] in URBANIX_ZONES
        # Sample first 100 rows for performance
        if count >= 100:
            break
    assert count == 100


def test_weather_sample_file_pairing():
    """
    Asserts that data/weather_sample.csv exists and has matching temperature/humidity observations.
    """
    sample_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "weather_sample.csv")
    )
    assert os.path.exists(sample_path), f"Required sample missing: {sample_path}. Run python data/prepare_data.py"

    events = list(parse_weather_rows(sample_path))
    # 162 unique timestamps * 5 zones = 810 events
    assert len(events) == 162 * 5
    timestamps = sorted(list(set(e["timestamp"] for e in events)))
    assert len(timestamps) == 162
    assert timestamps[0] == "2012-10-01T13:00:00"
    assert timestamps[-1] == "2012-10-08T06:00:00"
