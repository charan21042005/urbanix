"""
========================================================================================
🌟 URBANIX — DATA PREPARATION & SENSOR TELEMETRY PREPROCESSING SCRIPT
========================================================================================
Welcome to the Urbanix Data Preparation script! 👋

WHAT DOES THIS SCRIPT DO?
Before a big data pipeline can process real-time streams, it needs clean, authentic data!
This script extracts, cleans, filters, and formats real-world empirical datasets from
three major government and scientific data sources:

1. TRAFFIC SENSOR DATA (METR-LA):
   - Origin: Caltrans Performance Measurement System (PeMS) via Zenodo Record 5724362.
   - What it contains: Vehicle speed readings from 207 highway loop detectors across Los Angeles.
   - Output file: 'data/traffic_sample.csv'

2. AIR QUALITY SENSOR DATA (EPA AirData):
   - Origin: United States Environmental Protection Agency (U.S. EPA) AQS database.
   - What it contains: Ambient PM2.5 particulate matter readings across Los Angeles County.
   - Output file: 'data/aqi_sample.csv' (filtered strictly to March-June 2012)

3. WEATHER OBSERVATION DATA (OpenWeather / Kaggle):
   - Origin: Historical hourly weather station archives for Los Angeles.
   - What it contains: Hourly temperature (Kelvin) and relative humidity percentages.
   - Output file: 'data/weather_sample.csv'

WHY DO WE PREPROCESS DATA?
Raw datasets from the wild are often messy: they come inside compressed zip files, contain
thousands of extra cities we don't care about, or have incomplete trailing lines from downloads.
This script cleans everything up so our Kafka producers can read clean, standardized CSV files!
========================================================================================
"""

# ==============================================================================
# 📦 STEP 1: IMPORT PYTHON STANDARD LIBRARIES
# ==============================================================================

# 'os' is Python's built-in Operating System module.
# It gives us functions to navigate folders, check if files exist, and build file paths.
import os

# 'csv' is Python's built-in Comma-Separated Values module.
# It provides special 'reader' and 'writer' tools to parse and generate CSV spreadsheets easily!
import csv

# 'zipfile' is Python's built-in tool for opening and reading compressed .zip archive files
# directly in memory without needing third-party software like WinZip or 7-Zip!
import zipfile

# 'io' handles in-memory input/output byte streams.
# We use 'io.TextIOWrapper' to translate raw binary bytes from a zip file into readable text strings!
import io


# ==============================================================================
# 📂 STEP 2: CONFIGURE DIRECTORY PATHS
# ==============================================================================

# __file__ is a special variable that holds the path to this specific script file.
# os.path.abspath(__file__) turns that path into an absolute full path on your computer.
# os.path.dirname(...) finds the directory containing this script (the 'data/' folder).
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# os.path.expanduser("~") finds the logged-in user's home folder (e.g. 'C:/Users/Username' on Windows).
# We build a path to the scratch storage folder where large downloaded files are temporarily saved.
SCRATCH_DIR = os.path.join(
    os.path.expanduser("~"),
    ".gemini",
    "antigravity-ide",
    "brain",
    "fe6199e7-2a4c-4d93-aaf2-9c9ca0cf397b",
    "scratch",
)

# RAW_DATA_DIR points to an optional local 'data/raw/' folder inside the repository.
RAW_DATA_DIR = os.path.join(BASE_DIR, "raw")


# ==============================================================================
# 🔍 STEP 3: HELPER FUNCTION: FIND SOURCE FILES ACROSS FOLDERS
# ==============================================================================

def get_source_path(filename: str) -> str:
    """
    Locates a raw data file by checking multiple potential directories:
      1. First, checks the scratch directory (where raw zip and CSV files were downloaded).
      2. Second, checks the local 'data/raw/' directory.
      3. If not found in either, returns the scratch path as the default target.

    Args:
        filename (str): Name of the file to search for (e.g. 'daily_88101_2012.zip')

    Returns:
        str: The full filesystem path to where the file exists.
    """
    # Build full path inside scratch directory
    scratch_path = os.path.join(SCRATCH_DIR, filename)
    # os.path.exists() returns True if the file physically exists on the hard drive
    if os.path.exists(scratch_path):
        return scratch_path

    # Build full path inside local raw data directory
    local_path = os.path.join(RAW_DATA_DIR, filename)
    if os.path.exists(local_path):
        return local_path

    # Fallback to scratch path if not found yet
    return scratch_path


# ==============================================================================
# 🚗 STEP 4: PREPARE TRAFFIC SENSOR DATA (METR-LA HIGHWAY LOOP DETECTORS)
# ==============================================================================

def prepare_traffic_sample():
    """
    Cleans raw METR-LA highway loop detector velocity data.

    How it works:
      1. Reads raw CSV rows from 'metr_la_sample_raw.csv'.
      2. Inspects each line: incomplete lines from partial downloads are discarded.
         (A valid row must contain at least 200 commas for the 207 highway sensors).
      3. Saves clean rows into 'data/traffic_sample.csv'.
      4. Prints summary information showing the first and last observation timestamps.
    """
    # Find where the raw file is located
    raw_path = get_source_path("metr_la_sample_raw.csv")
    # Define where the cleaned output file will be written
    out_path = os.path.join(BASE_DIR, "traffic_sample.csv")

    # Safety check: If the raw source file doesn't exist, warn the user and stop
    if not os.path.exists(raw_path):
        print(f"[-] Raw METR-LA file not found at {raw_path}")
        return

    # 'with open(...) as f:' is Python's context manager for reading files.
    # It automatically opens the file and guarantees it is safely closed when done,
    # even if an unexpected error occurs!
    # 'encoding="utf-8"' ensures standard character decoding.
    with open(raw_path, "r", encoding="utf-8") as f:
        # f.readlines() reads the entire text file into a Python list of strings (one per line)
        lines = f.readlines()

    # List Comprehension (A clean, fast Python way to filter lists):
    # Here, we keep only lines where l.count(",") >= 200.
    # Why? Each row contains readings for 207 sensors separated by commas.
    # If a line has fewer than 200 commas, it was cut off mid-download and is corrupted!
    valid_lines = [l for l in lines if l.count(",") >= 200]

    # Open the output file in write mode ("w")
    # 'newline=""' is recommended by Python when writing CSV files on Windows to prevent extra blank lines
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        # Write all clean lines into the new file
        f.writelines(valid_lines)

    # Extract the first timestamp (line 1, because line 0 is the column header)
    # .split(",")[0] splits the comma-separated line and grabs the very first element (the timestamp)
    first_ts = valid_lines[1].split(",")[0]
    # Grab the timestamp from the very last line in the file
    last_ts = valid_lines[-1].split(",")[0]

    # Print friendly confirmation with observation counts and time range
    print(
        f"[+] Prepared Traffic Sample: {out_path} ({len(valid_lines)-1} timestamps across 207 sensors)"
    )
    print(f"    - First observation: {first_ts}")
    print(f"    - Last observation:  {last_ts}")


# ==============================================================================
# 🌫️ STEP 5: PREPARE AIR QUALITY DATA (EPA AIRDATA PM2.5 SENSORS)
# ==============================================================================

def prepare_aqi_sample():
    """
    Extracts and filters official EPA ambient air pollution data directly from a ZIP file.

    Academic Rigor & Filtering Rules:
      1. Reads directly from compressed 'daily_88101_2012.zip' without unzipping to disk.
      2. Filters for California (State Code '06') and Los Angeles County (County Code '037').
      3. Strictly constrains records to the METR-LA study window: March 1, 2012 to June 30, 2012.
      4. Selects only essential research columns (latitude, longitude, PM2.5 mean, AQI, station name).
      5. Saves the clean records into 'data/aqi_sample.csv'.
    """
    zip_path = get_source_path("daily_88101_2012.zip")
    out_path = os.path.join(BASE_DIR, "aqi_sample.csv")

    if not os.path.exists(zip_path):
        print(f"[-] EPA zip not found at {zip_path}")
        return

    # 'zipfile.ZipFile(zip_path)' opens the compressed archive
    with zipfile.ZipFile(zip_path) as z:
        # z.namelist()[0] gets the name of the first file stored inside the zip archive
        name = z.namelist()[0]
        # z.open(name) opens a streaming read pipe to that file inside the zip
        with z.open(name) as f:
            # io.TextIOWrapper converts binary bytes from the zip into UTF-8 text characters.
            # csv.reader parses that text into lists of columns separated by commas.
            reader = csv.reader(io.TextIOWrapper(f, "utf-8"))
            # next(reader) reads the very first line (the header row) and advances to line 2
            header = next(reader)

            # Filter for Los Angeles County records during March-June 2012
            la_window_rows = []
            for row in reader:
                # Validation checks:
                # len(row) > 25: ensure the row has enough columns
                # row[0] == "06": U.S. Federal FIPS State Code for California
                # row[1] == "037": FIPS County Code for Los Angeles County
                if len(row) > 25 and row[0] == "06" and row[1] == "037":
                    # row[11] is the local date string (e.g. '2012-03-15')
                    date_local = row[11].strip()
                    # Check if the date falls inside the 4-month METR-LA study window
                    if "2012-03-01" <= date_local <= "2012-06-30":
                        la_window_rows.append(row)

    # Define clean, simplified column names for our final dataset
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

    # Write the filtered, clean dataset into 'data/aqi_sample.csv'
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        # csv.writer formats Python lists into properly quoted CSV rows
        writer = csv.writer(f)
        # Write our clean header row first
        writer.writerow(clean_header)

        # Loop through each matching row and write only the desired columns
        for r in la_window_rows:
            # Column index mapping from raw EPA schema:
            # Index  0: State Code ("06")
            # Index  1: County Code ("037")
            # Index  2: Site Number (Station ID)
            # Index  3: Parameter Code ("88101" = PM2.5)
            # Index  4: Parameter Occurrence Code (POC)
            # Index  5: Latitude coordinate
            # Index  6: Longitude coordinate
            # Index  8: Parameter Name ("PM2.5 - Local Conditions")
            # Index 11: Date Local ("YYYY-MM-DD")
            # Index 12: Units of Measure ("Micrograms/cubic meter")
            # Index 16: Arithmetic Mean (Daily average PM2.5 concentration)
            # Index 19: Official Air Quality Index (AQI) number
            # Index 22: Monitoring Site Name
            # Index 26: City Name
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

    # Find the unique dates and unique stations using Python sets:
    # 'set()' automatically removes duplicate entries, keeping only unique values!
    dates = sorted(list(set(r[11] for r in la_window_rows)))
    sites = sorted(list(set(r[2] for r in la_window_rows)))

    print(
        f"[+] Prepared AQI Sample: {out_path} ({len(la_window_rows)} observations across {len(sites)} stations)"
    )
    print(f"    - Calendar Window: {dates[0]} to {dates[-1]} ({len(dates)} days)")
    print(f"    - Active Stations in LA County: {sites}")


# ==============================================================================
# 🌦️ STEP 6: PREPARE WEATHER OBSERVATION DATA (TEMPERATURE & HUMIDITY)
# ==============================================================================

def prepare_weather_sample():
    """
    Pairs historical hourly temperature and humidity observations for Los Angeles.

    How it works:
      1. Reads raw temperature table and finds the column index for "Los Angeles".
      2. Reads raw humidity table and finds the column index for "Los Angeles".
      3. Finds common timestamps where BOTH temperature and humidity were recorded.
      4. Writes the synchronized hourly weather dataset into 'data/weather_sample.csv'.
    """
    temp_path = get_source_path("weather_temp_sample.csv")
    hum_path = get_source_path("weather_humidity_sample.csv")
    out_path = os.path.join(BASE_DIR, "weather_sample.csv")

    # Safety check: Ensure both raw weather files exist before proceeding
    if not os.path.exists(temp_path) or not os.path.exists(hum_path):
        print(f"[-] Raw weather files not found in scratch or raw directories")
        return

    # 1. Parse Temperature Data:
    with open(temp_path, "r", encoding="utf-8") as f:
        temp_reader = csv.reader(f)
        # Read the header row containing city names: ['datetime', 'Vancouver', 'Portland', 'Los Angeles', ...]
        temp_header = next(temp_reader)
        # Find which column number corresponds to "Los Angeles"
        la_temp_idx = temp_header.index("Los Angeles")

        # Store temperature values in a dictionary mapping: timestamp -> temperature
        temp_dict = {}
        for r in temp_reader:
            # Check that the row has enough columns and neither timestamp nor temperature is empty
            if len(r) > la_temp_idx and r[0] and r[la_temp_idx]:
                temp_dict[r[0]] = r[la_temp_idx]

    # 2. Parse Humidity Data:
    with open(hum_path, "r", encoding="utf-8") as f:
        hum_reader = csv.reader(f)
        hum_header = next(hum_reader)
        la_hum_idx = hum_header.index("Los Angeles")

        # Store humidity values in a dictionary mapping: timestamp -> humidity
        hum_dict = {}
        for r in hum_reader:
            if len(r) > la_hum_idx and r[0] and r[la_hum_idx]:
                hum_dict[r[0]] = r[la_hum_idx]

    # 3. Synchronize Timestamps (Mathematical Set Intersection):
    # In mathematics, the intersection of set A and set B contains only items in BOTH sets.
    # Here, we only keep timestamps where we have BOTH temperature AND humidity!
    common_dates = sorted(
        list(set(temp_dict.keys()).intersection(set(hum_dict.keys())))
    )

    # 4. Write Clean Merged Weather File:
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        # Header row
        writer.writerow(
            ["datetime", "city", "temperature_kelvin", "humidity_percent"]
        )
        # Write rows with paired measurements
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


# ==============================================================================
# 🚀 STEP 7: SCRIPT EXECUTION ENTRYPOINT
# ==============================================================================

# When this file is run directly from the terminal (e.g. 'python data/prepare_data.py'),
# Python automatically executes all three preparation functions in order!
if __name__ == "__main__":
    print("=" * 70)
    print("  URBANIX: PREPARING MULTI-SENSOR REPRODUCIBLE SAMPLES (DAY 2)")
    print("=" * 70)
    # Clean and prepare traffic, air quality, and weather samples
    prepare_traffic_sample()
    prepare_aqi_sample()
    prepare_weather_sample()
    print("=" * 70)
    print("  ALL SAMPLES PREPARED SUCCESSFULLY!")
    print("=" * 70)
