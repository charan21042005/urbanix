"""
Urbanix — Unit Tests for Telemetry Simulation Producers (Day 2)
Tests transformations, unit conversions, schema validation, zone mapping,
and error handling without requiring a live Kafka broker.
"""

import os
import sys
import json
import pytest
import tempfile

# Add parent directory to path so producers can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from zone_mapping import (
    map_traffic_sensor_to_zone,
    map_aqi_station_to_zone,
    get_weather_zones,
    URBANIX_ZONES,
)
from traffic_producer import parse_traffic_rows, MPH_TO_KMPH
from aqi_producer import parse_aqi_rows
from weather_producer import parse_weather_rows, KELVIN_OFFSET


# ---------------------------------------------------------
# 1. Zone Mapping Tests
# ---------------------------------------------------------
def test_traffic_zone_mapping_determinism():
    """Verify that METR-LA sensor IDs map deterministically to Urbanix zones."""
    for s_id in ["773869", "767541", "767542", "717447", "717446"]:
        zone_1 = map_traffic_sensor_to_zone(s_id)
        zone_2 = map_traffic_sensor_to_zone(s_id)
        assert zone_1 == zone_2
        assert zone_1 in URBANIX_ZONES


def test_aqi_zone_mapping():
    """Verify documented Los Angeles County EPA monitoring sites map to correct zones."""
    assert map_aqi_station_to_zone("1103") == "zone_1"  # Downtown Core
    assert map_aqi_station_to_zone("1002") == "zone_2"  # Burbank
    assert map_aqi_station_to_zone("2005") == "zone_3"  # Pasadena
    assert map_aqi_station_to_zone("1302") == "zone_4"  # Compton
    assert map_aqi_station_to_zone("4002") == "zone_5"  # Long Beach


def test_weather_zones():
    """Verify regional weather observation spans all Urbanix grid zones identically."""
    zones = get_weather_zones()
    assert len(zones) == 5
    assert zones == ["zone_1", "zone_2", "zone_3", "zone_4", "zone_5"]


# ---------------------------------------------------------
# 2. Traffic Producer Transformations & Parsing
# ---------------------------------------------------------
def test_traffic_unit_conversion():
    """Verify explicit mph -> km/h conversion formula: speed_kmph = speed_mph * 1.60934."""
    test_mph = 60.0
    expected_kmph = round(test_mph * MPH_TO_KMPH, 2)
    assert expected_kmph == 96.56

    test_mph_zero = 0.0
    assert test_mph_zero * MPH_TO_KMPH == 0.0


def test_traffic_parsing_valid_fixture():
    """Verify wide METR-LA matrix parsing into discrete sensor events."""
    csv_content = (
        ",773869,767541\n"
        "2012-03-01 00:00:00,64.38,67.62\n"
        "2012-03-01 00:05:00,0.0,65.00\n"  # 0.0 should be skipped as sensor dropout
    )
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
        tmp.write(csv_content)
        tmp_path = tmp.name

    try:
        events = list(parse_traffic_rows(tmp_path))
        # Expected: 2 events from first row, 1 event from second row (0.0 dropped)
        assert len(events) == 3

        # First event check
        e1 = events[0]
        assert e1["sensor_id"] == "773869"
        assert e1["timestamp"] == "2012-03-01T00:00:00"
        assert e1["speed_mph"] == 64.38
        assert e1["speed_kmph"] == round(64.38 * MPH_TO_KMPH, 2)
        assert e1["unit"] == "km/h"
        assert "vehicle_count" not in e1  # Strictly verified: METR-LA has no count
        assert json.loads(json.dumps(e1)) == e1  # Valid JSON serialization
    finally:
        os.remove(tmp_path)


def test_traffic_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        list(parse_traffic_rows("non_existent_file.csv"))


# ---------------------------------------------------------
# 3. AQI Producer Transformations & Parsing
# ---------------------------------------------------------
def test_aqi_parsing_and_validation():
    """Verify EPA AirData row parsing, PM2.5 validation, and missing value skipping."""
    csv_content = (
        "state_code,county_code,site_num,parameter_code,poc,latitude,longitude,parameter_name,date_local,units_of_measure,pm25_mean,aqi,site_name,city_name\n"
        "06,037,1103,88101,1,34.06,-118.22,PM2.5,2012-03-01,Micrograms/cubic meter (LC),14.2,55,N. Main St,Los Angeles\n"
        "06,037,1002,88101,1,34.17,-118.31,PM2.5,2012-03-01,Micrograms/cubic meter (LC),,20,Burbank,Burbank\n"  # Missing PM2.5
        "06,037,2005,88101,1,34.13,-118.12,PM2.5,2012-03-01,Micrograms/cubic meter (LC),-5.0,,Pasadena,Pasadena\n"  # Invalid negative
    )
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
        tmp.write(csv_content)
        tmp_path = tmp.name

    try:
        events = list(parse_aqi_rows(tmp_path))
        # Only the first row is valid
        assert len(events) == 1
        e = events[0]
        assert e["station_id"] == "1103"
        assert e["zone_id"] == "zone_1"
        assert e["pm25"] == 14.2
        assert e["aqi"] == 55
        assert e["unit"] == "ug/m3"
        assert "co2" not in e  # Strictly verified: no fake CO2
        assert json.loads(json.dumps(e)) == e
    finally:
        os.remove(tmp_path)


# ---------------------------------------------------------
# 4. Weather Producer Transformations & Parsing
# ---------------------------------------------------------
def test_weather_unit_conversion():
    """Verify Kelvin -> Celsius formula: T_c = T_k - 273.15."""
    test_k = 291.87
    expected_c = round(test_k - KELVIN_OFFSET, 2)
    assert expected_c == 18.72


def test_weather_parsing_and_regional_broadcast():
    """Verify weather parsing, Kelvin conversion, and multi-zone regional broadcast."""
    csv_content = (
        "datetime,city,temperature_kelvin,humidity_percent\n"
        "2012-10-01 12:00:00,Los Angeles,,\n"  # Missing values: skipped
        "2012-10-01 13:00:00,Los Angeles,291.87,88.0\n"
    )
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
        tmp.write(csv_content)
        tmp_path = tmp.name

    try:
        events = list(parse_weather_rows(tmp_path))
        # The single valid observation broadcasts across all 5 zones
        assert len(events) == 5

        for e in events:
            assert e["city"] == "Los Angeles"
            assert e["temperature_k"] == 291.87
            assert e["temperature_c"] == 18.72
            assert e["humidity_pct"] == 88.0
            assert e["temperature_unit"] == "Celsius"
            assert e["humidity_unit"] == "Percent"
            assert e["zone_id"] in URBANIX_ZONES
            assert json.loads(json.dumps(e)) == e

        # Verify all 5 zones received identical atmospheric observation without artificial alteration
        zones_seen = [e["zone_id"] for e in events]
        assert sorted(zones_seen) == ["zone_1", "zone_2", "zone_3", "zone_4", "zone_5"]
    finally:
        os.remove(tmp_path)


def test_weather_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        list(parse_weather_rows("non_existent_file.csv"))


# ---------------------------------------------------------
# 5. Prepared Sample Integrity Tests
# ---------------------------------------------------------
def test_aqi_prepared_sample_window():
    """Verify that the generated aqi_sample.csv falls strictly within the METR-LA window (2012-03-01 to 2012-06-30)."""
    sample_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "aqi_sample.csv")
    )
    if os.path.exists(sample_path):
        events = list(parse_aqi_rows(sample_path))
        assert len(events) == 712
        dates = sorted(list(set(e["timestamp"][:10] for e in events)))
        assert dates[0] == "2012-03-01"
        assert dates[-1] == "2012-06-30"


def test_traffic_prepared_sample_structure():
    """Verify that traffic_sample.csv contains valid speed records in positive ranges."""
    sample_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "traffic_sample.csv")
    )
    if os.path.exists(sample_path):
        # Inspect first 100 events
        count = 0
        for event in parse_traffic_rows(sample_path):
            count += 1
            assert event["speed_mph"] > 0.0
            assert event["speed_kmph"] > 0.0
            assert event["speed_kmph"] < 250.0  # Physical highway maximum
            assert event["zone_id"] in URBANIX_ZONES
            if count >= 100:
                break
        assert count == 100

