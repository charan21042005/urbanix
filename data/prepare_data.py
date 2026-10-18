"""
Urbanix — Data Preparation & Preprocessing Script (Day 2)
Extracts and prepares verified, reproducible development sample datasets for:
1. Traffic: METR-LA (Zenodo Record 5724362 / Caltrans PeMS loop detectors)
2. Air Quality: U.S. EPA AirData (AQS PM2.5 Los Angeles County, filtered to March-June 2012)
3. Weather: Historical Hourly Weather (OpenWeatherMap / Kaggle selfishgene collection, Los Angeles)
"""

import os
import csv
import zipfile
import io

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRATCH_DIR = os.path.join(
    os.path.expanduser("~"),
    ".gemini",
    "antigravity-ide",
    "brain",
    "fe6199e7-2a4c-4d93-aaf2-9c9ca0cf397b",
    "scratch",
)
RAW_DATA_DIR = os.path.join(BASE_DIR, "raw")


def get_source_path(filename: str) -> str:
    """Finds raw file in scratch directory or local raw data directory."""
    scratch_path = os.path.join(SCRATCH_DIR, filename)
    if os.path.exists(scratch_path):
        return scratch_path
    local_path = os.path.join(RAW_DATA_DIR, filename)
    if os.path.exists(local_path):
        return local_path
    return scratch_path


def prepare_traffic_sample():
    raw_path = get_source_path("metr_la_sample_raw.csv")
    out_path = os.path.join(BASE_DIR, "traffic_sample.csv")

    if not os.path.exists(raw_path):
        print(f"[-] Raw METR-LA file not found at {raw_path}")
        return

    with open(raw_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Drop trailing incomplete line from range download if any
    valid_lines = [l for l in lines if l.count(",") >= 200]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        f.writelines(valid_lines)

    first_ts = valid_lines[1].split(",")[0]
    last_ts = valid_lines[-1].split(",")[0]
    print(
        f"[+] Prepared Traffic Sample: {out_path} ({len(valid_lines)-1} timestamps across 207 sensors)"
    )
    print(f"    - First observation: {first_ts}")
    print(f"    - Last observation:  {last_ts}")


def prepare_aqi_sample():
    zip_path = get_source_path("daily_88101_2012.zip")
    out_path = os.path.join(BASE_DIR, "aqi_sample.csv")

    if not os.path.exists(zip_path):
        print(f"[-] EPA zip not found at {zip_path}")
        return

    with zipfile.ZipFile(zip_path) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            reader = csv.reader(io.TextIOWrapper(f, "utf-8"))
            header = next(reader)

            # Filter for California (State Code 06), Los Angeles County (County Code 037),
            # strictly constrained to the METR-LA calendar window: March 1, 2012 to June 30, 2012.
            la_window_rows = []
            for row in reader:
                if len(row) > 25 and row[0] == "06" and row[1] == "037":
                    date_local = row[11].strip()
                    if "2012-03-01" <= date_local <= "2012-06-30":
                        la_window_rows.append(row)

    clean_header = [
        "state_code",
        "county_code",
        "site_num",
        "parameter_code",
        "poc",
        "latitude",
        "longitude",
        "parameter_name",
        "date_local",
        "units_of_measure",
        "pm25_mean",
        "aqi",
        "site_name",
        "city_name",
    ]

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(clean_header)
        for r in la_window_rows:
            # Indices: 0: State, 1: County, 2: Site, 3: Param, 4: POC, 5: Lat, 6: Lon,
            # 8: Param Name, 11: Date Local, 12: Units, 16: Arithmetic Mean, 19: AQI, 22: Site Name, 26: City
            writer.writerow([
                r[0],
                r[1],
                r[2],
                r[3],
                r[4],
                r[5],
                r[6],
                r[8],
                r[11],
                r[12],
                r[16],
                r[19],
                r[22],
                r[26],
            ])

    dates = sorted(list(set(r[11] for r in la_window_rows)))
    sites = sorted(list(set(r[2] for r in la_window_rows)))
    print(
        f"[+] Prepared AQI Sample: {out_path} ({len(la_window_rows)} observations across {len(sites)} stations)"
    )
    print(f"    - Calendar Window: {dates[0]} to {dates[-1]} ({len(dates)} days)")
    print(f"    - Active Stations in LA County: {sites}")


def prepare_weather_sample():
    temp_path = get_source_path("weather_temp_sample.csv")
    hum_path = get_source_path("weather_humidity_sample.csv")
    out_path = os.path.join(BASE_DIR, "weather_sample.csv")

    if not os.path.exists(temp_path) or not os.path.exists(hum_path):
        print(f"[-] Raw weather files not found in scratch or raw directories")
        return

    with open(temp_path, "r", encoding="utf-8") as f:
        temp_reader = csv.reader(f)
        temp_header = next(temp_reader)
        la_temp_idx = temp_header.index("Los Angeles")
        temp_dict = {}
        for r in temp_reader:
            if len(r) > la_temp_idx and r[0] and r[la_temp_idx]:
                temp_dict[r[0]] = r[la_temp_idx]

    with open(hum_path, "r", encoding="utf-8") as f:
        hum_reader = csv.reader(f)
        hum_header = next(hum_reader)
        la_hum_idx = hum_header.index("Los Angeles")
        hum_dict = {}
        for r in hum_reader:
            if len(r) > la_hum_idx and r[0] and r[la_hum_idx]:
                hum_dict[r[0]] = r[la_hum_idx]

    # Only include timestamps where both temperature and humidity observations exist
    common_dates = sorted(
        list(set(temp_dict.keys()).intersection(set(hum_dict.keys())))
    )

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["datetime", "city", "temperature_kelvin", "humidity_percent"]
        )
        for dt in common_dates:
            writer.writerow([dt, "Los Angeles", temp_dict[dt], hum_dict[dt]])

    print(
        f"[+] Prepared Weather Sample: {out_path} ({len(common_dates)} hourly observations for Los Angeles)"
    )
    print(f"    - First observation: {common_dates[0]}")
    print(f"    - Last observation:  {common_dates[-1]}")
    print(
        f"    - Note: Sourced from Kaggle 2012-2017 collection starting October 1, 2012."
    )


if __name__ == "__main__":
    prepare_traffic_sample()
    prepare_aqi_sample()
    prepare_weather_sample()
