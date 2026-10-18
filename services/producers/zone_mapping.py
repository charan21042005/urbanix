"""
Urbanix — Zone Mapping & Spatial Assignment Module
Provides deterministic mapping of real physical sensor and station identifiers
to Urbanix smart city zones (zone_1 through zone_5).

Preserves the reality that atmospheric regional weather spans across zones
without manufacturing artificial differences.
"""

# Deterministic mapping for Los Angeles County EPA Air Quality Monitoring Sites
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

URBANIX_ZONES = ["zone_1", "zone_2", "zone_3", "zone_4", "zone_5"]


def map_traffic_sensor_to_zone(sensor_id: str) -> str:
    """
    Deterministically maps a METR-LA loop detector station ID to one of 5 Urbanix zones.
    Uses positive integer modulo mapping for deterministic, balanced corridor distribution.
    """
    try:
        val = int(str(sensor_id).strip())
        zone_num = (val % len(URBANIX_ZONES)) + 1
        return f"zone_{zone_num}"
    except (ValueError, TypeError):
        # Fallback for non-numeric sensor IDs: deterministic string hash
        val = sum(ord(c) for c in str(sensor_id))
        zone_num = (val % len(URBANIX_ZONES)) + 1
        return f"zone_{zone_num}"


def map_aqi_station_to_zone(site_num: str) -> str:
    """
    Maps an EPA AQS monitoring site number to an Urbanix zone.
    Uses documented Los Angeles County site table with deterministic fallback.
    """
    clean_site = str(site_num).strip().zfill(4)
    if clean_site in EPA_SITE_TO_ZONE:
        return EPA_SITE_TO_ZONE[clean_site]
    
    # Deterministic fallback for unknown stations
    val = sum(ord(c) for c in clean_site)
    zone_num = (val % len(URBANIX_ZONES)) + 1
    return f"zone_{zone_num}"


def get_weather_zones() -> list[str]:
    """
    Returns the list of Urbanix zones governed by regional metropolitan weather observations.
    In accordance with physical reality, regional weather applies across all grid zones
    without injecting artificial per-zone variations.
    """
    return list(URBANIX_ZONES)
