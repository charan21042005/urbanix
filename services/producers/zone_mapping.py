"""
Urbanix — Zone Mapping & Spatial Assignment Module
Provides deterministic mapping of real physical sensor and station identifiers
to Urbanix smart city zones (zone_1 through zone_5).

Important Engineering Notes:
1. Provisional Traffic Zone Mapping:
   METR-LA contains 207 highway loop detectors on Los Angeles County freeways
   (I-5, I-10, I-405, etc.). For Day 2 simulation balancing, sensors are assigned
   to zones using a deterministic modulo partition: zone = (sensor_id % 5) + 1.
   This mapping is provisional for multi-zone stream simulation and does NOT
   claim to represent official administrative municipal boundaries.

2. Air Quality Station Assignment:
   EPA AQS monitoring stations are mapped according to their named sub-regional
   locations within the Los Angeles County air basin (Downtown, Valley, Foothills,
   South Central, Harbor).

3. Regional Weather Assumption:
   A single regional meteorological observation represents the ambient basin
   atmosphere across the 5 grid zones. The genuine observation is preserved and
   broadcast identically without manufacturing artificial per-zone variations.
"""

# Import the List type hint from Python's typing module for static type annotations
from typing import List

# Deterministic dictionary mapping for Los Angeles County EPA Air Quality Monitoring Sites.
# The keys represent the physical EPA site numbers (padded to 4 digits), and values
# represent the corresponding Urbanix operational sub-regions.
EPA_SITE_TO_ZONE = {
    "1103": "zone_1",  # Los Angeles - North Main Street (Downtown Core)
    "1002": "zone_2",  # Burbank (San Fernando Valley Corridor)
    "1201": "zone_2",  # Reseda (West Valley)
    "2005": "zone_3",  # Pasadena (San Gabriel Valley)
    "0002": "zone_3",  # Azusa (East Foothills)
    "1302": "zone_4",  # Compton (South Central Corridor)
    "1602": "zone_4",  # Pico Rivera (Southeast Transit Arterial)
    "4002": "zone_5",  # Long Beach (Harbor / Port Terminal)
    "4004": "zone_5",  # Long Beach South
    "9033": "zone_5",  # Lancaster (High Desert Outer Ring)
}

# Canonical list of all 5 Urbanix smart city operational zones
URBANIX_ZONES: List[str] = ["zone_1", "zone_2", "zone_3", "zone_4", "zone_5"]


def map_traffic_sensor_to_zone(sensor_id: str) -> str:
    """
    Deterministically maps a METR-LA loop detector station ID to one of 5 Urbanix zones.

    NOTE: This mapping is provisional for simulation load balancing across the 5
    Urbanix platform zones. It distributes sensors evenly using positive modulo arithmetic
    and does not represent official municipal administrative boundaries.

    Parameters:
        sensor_id (str): The unique string identifier of the traffic loop detector.

    Returns:
        str: A zone identifier string, e.g., 'zone_1', 'zone_2', ..., 'zone_5'.
    """
    try:
        # Convert the sensor ID to an integer after stripping any whitespace
        val = int(str(sensor_id).strip())
        # Modulo arithmetic distributes IDs evenly across 5 zones (0 to 4), then +1 shifts to 1 to 5
        zone_num = (val % len(URBANIX_ZONES)) + 1
        # Format and return the standard zone string
        return f"zone_{zone_num}"
    except (ValueError, TypeError):
        # Fallback for non-numeric sensor IDs (e.g. test strings or alpha sensor tags):
        # Compute the sum of ASCII character codes to maintain deterministic hashing
        val = sum(ord(c) for c in str(sensor_id))
        # Compute zone number from character sum
        zone_num = (val % len(URBANIX_ZONES)) + 1
        return f"zone_{zone_num}"


def map_aqi_station_to_zone(site_num: str) -> str:
    """
    Maps an EPA AQS monitoring site number to an Urbanix zone based on its
    sub-regional location in Los Angeles County.

    Parameters:
        site_num (str): The EPA monitoring station site identifier.

    Returns:
        str: The assigned Urbanix zone string ('zone_1' through 'zone_5').
    """
    # Clean up whitespace and pad site number with leading zeros to 4 digits (e.g., '2' -> '0002')
    clean_site = str(site_num).strip().zfill(4)

    # Check if the site number exists in our Los Angeles County EPA lookup table
    if clean_site in EPA_SITE_TO_ZONE:
        return EPA_SITE_TO_ZONE[clean_site]

    # Deterministic fallback for unknown stations:
    # Hash the characters by summing their ASCII codes so unknown stations are stably assigned
    val = sum(ord(c) for c in clean_site)
    zone_num = (val % len(URBANIX_ZONES)) + 1
    return f"zone_{zone_num}"


def get_weather_zones() -> List[str]:
    """
    Returns the list of Urbanix zones governed by regional metropolitan weather observations.
    Regional weather observations represent ambient basin conditions and apply across
    all zones without artificial per-zone variations.

    Returns:
        List[str]: A shallow copy of the canonical URBANIX_ZONES list.
    """
    # Return a new list copy to prevent callers from accidentally modifying the module-level constant
    return list(URBANIX_ZONES)
