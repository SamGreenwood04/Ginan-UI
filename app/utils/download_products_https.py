import json
import threading
from datetime import datetime
from PyQt6.QtCore import QThread
from bs4 import BeautifulSoup

from app.utils.common_dirs import INPUT_PRODUCTS_PATH
from app.utils.gn_functions import GPSDate
from app.utils.yaml import load_yaml
import numpy as np
import netrc
import requests
from pathlib import Path
from app.utils.auto_download_PPP import (
    download_atx,
    download_ocean_loading_model,
    download_atmosphere_loading_model,
    download_geomagnetic_model,
    download_ocean_pole_tide_file,
    download_ocean_tide_potential_model,
    download_planetary_ephemerides_file,
    download_trop_model,
    download_satellite_metadata_snx,
    download_yaw_files
)

BASE_URL = "https://cddis.nasa.gov/archive"

def download_file(file_url, download_dir: Path=INPUT_PRODUCTS_PATH, overwrite_file: bool=False) -> Path | None:
    output_path = Path(download_dir / file_url.split("/")[-1])
    if output_path.exists() and not overwrite_file:
        print(f"❌ File already downloaded: {output_path}")
        return output_path
    else:
        try:
            response = requests.get(file_url, timeout=10)
            response.raise_for_status()
            with open(output_path, "wb") as file:
                file.write(response.content)
            print(f"✅ File downloaded: {output_path}")
            return output_path

        except requests.RequestException:
            print(f"❌ Failed to download: {file_url}")
            return None

def download_metadata(terminal_callback=None):
    """
    Download required PPP auxiliary metadata files using the existing auto-download functions.
    :param terminal_callback: Optional function to redirect print output (e.g., to GUI terminal)
    """
    def log(msg):
        if terminal_callback:
            terminal_callback(msg)
        else:
            print(msg)

    target_dir = INPUT_PRODUCTS_PATH
    tables_dir = INPUT_PRODUCTS_PATH / "tables"
    trop_dir = INPUT_PRODUCTS_PATH / "tables"
    trop_model = "gpt2"
    long_filename = False
    if_file_present = "dont_replace"

    log("🌐 Starting auxiliary metadata download...")
    log(f"📁 Download path: {target_dir}")

    try:
        download_atx(download_dir=target_dir, long_filename=True, if_file_present=if_file_present)
        download_satellite_metadata_snx(download_dir=target_dir, if_file_present=if_file_present)
        download_ocean_loading_model(download_dir=tables_dir, if_file_present=if_file_present)
        download_atmosphere_loading_model(download_dir=tables_dir, if_file_present=if_file_present)
        download_geomagnetic_model(download_dir=tables_dir, if_file_present=if_file_present)
        download_ocean_pole_tide_file(download_dir=tables_dir, if_file_present=if_file_present)
        download_ocean_tide_potential_model(download_dir=tables_dir, if_file_present=if_file_present)
        download_planetary_ephemerides_file(download_dir=tables_dir, if_file_present=if_file_present)
        download_trop_model(download_dir=trop_dir, model=trop_model, if_file_present=if_file_present)
        download_yaw_files(download_dir=tables_dir, if_file_present=if_file_present)
    except Exception as e:
        log(f"❌ Metadata download failed: {e}")
        return

    log("✅ All required metadata files downloaded (or already present).")

def start_metadata_download_thread(terminal_callback=None):
    """
    Start metadata download in a background thread.
    :param terminal_callback: Optional function to redirect print output (e.g., to GUI terminal)
    """
    thread = threading.Thread(target=download_metadata, args=(terminal_callback,), daemon=True)
    thread.start()

def download_metadata2(terminal_callback=None):
    """
    Download required PPP auxiliary metadata files using the existing auto-download functions.

    :param terminal_callback: Optional function to redirect print output (e.g., to GUI terminal)
    """
    def log(msg):
        if terminal_callback:
            terminal_callback(msg)
        else:
            print(msg)

    target_dir = INPUT_PRODUCTS_PATH
    tables_dir = INPUT_PRODUCTS_PATH / "tables"
    trop_dir = INPUT_PRODUCTS_PATH / "tables"

    log("🌐 Starting auxiliary metadata download...")

    files_to_download = [
        ("https://files.igs.org/pub/station/general/igs20.atx", target_dir),
        ("https://files.igs.org/pub/station/general/igs_satellite_metadata.snx", target_dir),
        ("https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/OLOAD_GO.BLQ.gz", tables_dir),
        ("https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/ALOAD_GO.BLQ.gz", tables_dir),
        ("https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/igrf14coeffs.txt.gz", tables_dir),
        ("https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/opoleloadcoefcmcor.txt.gz", tables_dir),
        ("https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/fes2014b_Cnm-Snm.dat.gz", tables_dir),
        ("https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/DE436.1950.2050.gz", tables_dir),
        ("https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/gpt_25.grd.gz", tables_dir),
        ("https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/bds_yaw_modes.snx.gz", tables_dir),
        ("https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/qzss_yaw_modes.snx.gz", trop_dir),
        ("https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/sat_yaw_bias_rate.snx.gz", tables_dir)
    ]
    for file, dest in files_to_download:
        try:
            download_file(file, dest)
            log("✅ Downloaded metadata!")
        except Exception as e:
            log(f"❌ Metadata download failed: {e}")

    log("✅ All required metadata files downloaded (or already present).")

def download_pea_auxiliary_products(start_epoch: datetime, end_epoch: datetime, log_callback=None):
    """
    Download auxiliary files required for Ginan PEA:
    - GNSS broadcast navigation files (BRDC)
    - Earth orientation parameters (IAU2000)

    :param start_epoch: Start time for processing window
    :param end_epoch: End time for processing window
    :param log_callback: Optional callback to emit log messages (e.g., to GUI)
    """
    msg = "🔽 Starting download of auxiliary PEA metadata..."
    print(msg)
    if log_callback:
        log_callback(msg)

    params = {
        "metadataStatus": "valid",
        "stationId": "BRDC",
        "fileType": "obs",
        "rinexVersion": "3",
        "filePeriod": "01D",
        "decompress": "true",
        "startDate": start_epoch.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endDate": end_epoch.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tenantId": "default"
    }
    query = requests.get("https://data.gnss.ga.gov.au/api/rinexFiles", params=params, headers={})
    query.raise_for_status()
    for response in json.loads(query.content):
        print(response)
        if log_callback:
            log_callback(response)


def reload_config(self):
    """
    Force reload of the YAML config from disk into memory.
    This allows any manual edits to be picked up before GUI changes are applied.
    """
    try:
        self.config = load_yaml(self.config_path)
        print(f"[Execution] 🔁 Reloaded config from disk: {self.config_path}")
    except Exception as e:
        raise RuntimeError(f"❌ Failed to reload config from {self.config_path}: {e}")

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

