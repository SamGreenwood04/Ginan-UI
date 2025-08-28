import os
from pathlib import Path
from datetime import datetime, timedelta
from app.utils.gn_functions import GPSDate
import numpy as np
import requests
from bs4 import BeautifulSoup
import netrc

BASE_URL = "https://cddis.nasa.gov/archive"

def validate_netrc(machine="urs.earthdata.nasa.gov") -> bool:
    """
    Validates that the .netrc file exists and contains valid credentials for the given machine.

    :param machine: The remote machine entry to check in .netrc (default: Earthdata login).
    :return: True if valid entry exists, False otherwise.
    """
    netrc_path = Path.home() / ".netrc"

    if not netrc_path.exists():
        print(f"❌ No .netrc file found at {netrc_path}")
        print(f"EarthData registration: https://urs.earthdata.nasa.gov/users/new")
        print(f"Instructions for creating .netrc file: https://cddis.nasa.gov/Data_and_Derived_Products/CreateNetrcFile.html")
        return False

    try:
        credentials = netrc.netrc(netrc_path).authenticators(machine)
        if credentials is None:
            print(f"❌ No credentials found for machine '{machine}' in .netrc")
            return False
        login, _, password = credentials
        if not login or not password:
            print(f"❌ Incomplete credentials for '{machine}' in .netrc")
            return False
        print(f"✅ .netrc contains valid entry for '{machine}'")
        return True

    except (netrc.NetrcParseError, FileNotFoundError) as e:
        print(f"❌ Error parsing .netrc: {e}")
        return False


def retrieve_all_cddis_types(gps_week: int) -> list[str]:
    """
    Retrieve CDDIS file list for a specific GPS week.
    """
    url = f"https://cddis.nasa.gov/archive/gnss/products/{gps_week}/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch files for GPS week {gps_week}: {e}")
        return []
    
    # Parse the HTML links for file names
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    files = [a['href'] for a in soup.find_all('a', href=True) if not a['href'].endswith('/')]
    return files


def create_cddis_file(filepath: Path, start: GPSDate, end: GPSDate) -> None:
    """
    Create a file named "CDDIS.list" with all CDDIS product files across a GPS week range.
    """
    seen_files = set()
    output_path = filepath / "../models/CDDIS.list"

    with open(output_path, "w") as f:
        for gpswk in gps_week_range(start, end):
            print(f"🔍 Processing GPS Week: {gpswk}")
            data = retrieve_all_cddis_types(gpswk)
            for d in data:
                if d in seen_files:
                    continue
                seen_files.add(d)
                try:
                    # Example filename pattern: igs_20231950000.sp3
                    time = datetime.strptime(d.split("_")[1], "%Y%j%H%M")
                    f.write(f"{d} {time}\n")
                except Exception:
                    pass  # Skip if the filename doesn't match expected format


def gps_week_range(start: GPSDate, end: GPSDate) -> list[int]:
    """
    Generate a list of GPS weeks between two GPSDate objects.
    """
    return list(range(int(start.gpswk), int(end.gpswk) + 1))


if __name__ == "__main__":
    if not validate_netrc():
        print("Aborting due to invalid or missing .netrc credentials.")
        exit(1)

# Example input range
    start_time = GPSDate(np.datetime64(datetime(2023, 9, 16)))
    end_time   = GPSDate(np.datetime64(datetime(2023, 9, 17)))

    create_cddis_file(Path(__file__).parent, start_time, end_time)

