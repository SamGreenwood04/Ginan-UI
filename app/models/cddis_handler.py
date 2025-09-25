import gzip
import shutil
import urllib
from datetime import datetime, timedelta
from http.client import HTTPException
from pathlib import Path
from typing import Optional

import unlzw3
import pandas as pd
import numpy as np
from requests import HTTPError

from app.utils.cddis_email import get_netrc_auth
from app.utils.common_dirs import INPUT_PRODUCTS_PATH
from app.utils.gn_functions import GPSDate, download_url
import requests
from bs4 import BeautifulSoup, SoupStrainer

BASE_URL = "https://cddis.nasa.gov/archive"
GPS_ORIGIN = np.datetime64("1980-01-06 00:00:00") # Magic date from gn_functions
MAX_RETRIES = 3

METADATA = [
    "https://files.igs.org/pub/station/general/igs_satellite_metadata.snx",
    "https://files.igs.org/pub/station/general/igs20.atx",
    "https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/OLOAD_GO.BLQ.gz",
    "https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/ALOAD_GO.BLQ.gz",
    "https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/igrf14coeffs.txt.gz",
    "https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/opoleloadcoefcmcor.txt.gz",
    "https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/fes2014b_Cnm-Snm.dat.gz",
    "https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/DE436.1950.2050.gz",
    "https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/gpt_25.grd.gz",
    "https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/bds_yaw_modes.snx.gz",
    "https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/qzss_yaw_modes.snx.gz",
    "https://peanpod.s3.ap-southeast-2.amazonaws.com/aux/products/tables/sat_yaw_bias_rate.snx.gz",
    "https://datacenter.iers.org/data/latestVersion/finals.data.iau2000.txt"
]

def date_to_gpswk(date: datetime) -> int:
    return int(GPSDate(np.datetime64(date)).gpswk)

def gpswk_to_date(gps_week: int, gps_day: int=0) -> datetime:
    return GPSDate(GPS_ORIGIN + np.timedelta64(gps_week, "W") + np.timedelta64(gps_day, "D")).as_datetime

def str_to_datetime(date_time_str):
    """
    :param date_time_str: YYYY-MM-DD_HH:mm:ss
    :returns datetime: datetime.strptime()
    """
    # Note can shift over to YYYY-dddHHmm format if needed through datetime.strptime(date_time,"%Y%j%H%M")
    try:
        return datetime.strptime(date_time_str, "%Y-%m-%d_%H:%M:%S")
    except ValueError:
        raise ValueError("Invalid datetime format. Use YYYY-MM-DDTHH:MM (e.g. 2025-05-01_00:00:00)")

def get_product_dataframe(start_time: datetime, end_time: datetime, target_files=None) -> pd.DataFrame:
    """
    Retrieves a DataFrame of available products for given time window and target files
    :param start_time: the start of the time window (use str_to_datetime helper function)
    :param end_time: the start of the time window (use str_to_datetime helper function)
    :param target_files: list of target files to filter for, defaulted to ["CLK","BIA","SP3"]
    :returns: set of valid analysis centers
    """
    if target_files is None:
        target_files = ["CLK", "BIA", "SP3"]
    else:
        target_files = [file.upper() for file in target_files]

    products = pd.DataFrame(columns=["analysis_center", "project", "date", "solution_type", "period", "resolution", "content", "format"])

    # 1. Retrieve available options
    gps_weeks = range(date_to_gpswk(start_time), date_to_gpswk(end_time) + 1)
    for gps_week in gps_weeks:
        print(f"[Handler] Retrieving week: {gps_week} ")
        url = f"https://cddis.nasa.gov/archive/gnss/products/{gps_week}/"
        try:
            week_files = requests.get(url, timeout=10)
            week_files.raise_for_status()
        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to fetch files for GPS week {gps_week}: {e}")

    # 2. Extract data from available options
        # Only relevant datafile containers are stored in memory
        soup = BeautifulSoup(week_files.content, "html.parser", parse_only=SoupStrainer("div", class_="archiveItemTextContainer"))
        for div in soup:
            filename = div.get_text().split(" ")[0]
            try:
                if gps_week < 2237:
                    # Format convention changed in week 2237
                    # AAAWWWWD.TYP.Z
                    center = filename[0:3].upper() # e.g. "COD"
                    _type = "FIN"  # pre-2237 were probably always final solutions :shrug:
                    day = int(filename[7])  # e.g. "0", 0-indexed, 7 indicates weekly
                    _format = filename[9:12].upper() # e.g. "snx", "ssc", "sum", "erp"
                    project = "OPS"
                    sampling_resolution = None
                    content = None
                    date = gpswk_to_date(gps_week)
                    if 0 < day < 7:
                        date += timedelta(days=day)
                        period = timedelta(days=1)
                    else:
                        period = timedelta(days=7)

                else:
                    # e.g. GRG0OPSFIN_20232620000_01D_01D_SOL.SNX.gz
                    # AAA0OPSSNX_YYYYDDDHHMM_LEN_SMP_CNT.FMT.gz
                    center = filename[0:3] # e.g. "COD"
                    project = filename[4:7] # e.g. "OPS" or "RNN" unused
                    _type = filename[7:10] # e.g. "FIN"
                    year = int(filename[11:15])  # e.g. "2023"
                    day_of_year = int(filename[15:18]) # e.g. "262"
                    hour = int(filename[18:20]) # e.g. "00"
                    minute = int(filename[20:22]) # e.g. "00"
                    intended_period = filename[23:26]  # eg "01D"
                    sampling_resolution = filename[27:30] # eg "01D"
                    content = filename[31:34] # e.g. "SOL"
                    _format = filename[35:38] # e.g. "SNX"

                    date = datetime(year, 1, 1, hour, minute) + timedelta(day_of_year - 1)
                    period = timedelta(days=int(intended_period[:-1])) # Assuming all periods are in days :shrug:

                if _format in target_files and start_time <= date <= end_time:
                    products.loc[len(products)] = {
                        "analysis_center": center,
                        "project": project,
                        "date": date,
                        "solution_type": _type,
                        "period": period,
                        "resolution": sampling_resolution,
                        "content": content,
                        "format": _format
                    }
            except (ValueError, IndexError) as e:
                print(f"Skipping irrelevant file: {filename} ({e})")
                continue
    products = products.drop_duplicates(inplace=False) # resets indexes too
    return products

def get_valid_analysis_centers(data: pd.DataFrame) -> set[str]:
    """
    Analyzes dataframe for the valid analysis_centers that provide continuous coverage

    :param data: dataframe to analyze (use get_product_dataframe to filter for time and target files)
    :returns: set of valid analysis centers
    """
    for (center, _type, _format), group in data.groupby(["analysis_center", "solution_type", "format"]):
        # We only included files within the time window, now just check they're contiguous
        group = group.sort_values("date").reset_index(drop=True)
        for i in range(len(group)-1):
            if group.loc[i]["date"] + group.loc[i]["period"] < group.loc[i+1]["date"]:
                print(f"Gap detected for {center} { _type} {_format} between {group.loc[i, 'date']} and {group.loc[i+1, 'date']}")
                data = data[data["analysis_center"] != center and data["solution_type"] != _type and data["format"] != _format]
                break

    # 4. Report results
    centers = set()
    for analysis_center in data["analysis_center"].unique():
        centers.add(analysis_center)
        center_products = data.loc[data["analysis_center"] == analysis_center]
        center_products = center_products.drop_duplicates(subset=["solution_type", "format"], inplace=False)
        offerings = ""
        for _format in center_products["format"].unique():
            types = center_products.loc[center_products["format"] == _format, "solution_type"].unique()
            offerings += f"{_format}:({'/'.join(types)}) "
        print(f"[Handler] {analysis_center} offers: {offerings}")

    return centers

def extract_file(filename: str, compressed: Path, decompressed: Path) -> Path:
    if filename.endswith((".gz", ".gzip")):
        with gzip.open(compressed, "rb") as f_in, open(decompressed, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    elif filename.endswith(".Z"):
        decompressed_data = unlzw3.unlzw(compressed)
        with open(decompressed, "wb") as f_out:
            f_out.write(decompressed_data)
    compressed.unlink()
    return decompressed

def download_file(url: str, session: requests.Session, download_dir: Path=INPUT_PRODUCTS_PATH,
                  log_callback=None) -> Optional[Path]:
    def log(msg: str):
        log_callback(msg) if log_callback else print(msg)
    log(f"Attempting to download: {url}")

    filename = url.split("/")[-1]
    if filename.endswith((".gz", ".gzip", ".Z")):
        compressed = Path(download_dir / filename)
        decompressed = Path(download_dir / ".".join(filename.split(".")[:-1]))
    else:
        compressed = None
        decompressed = Path(download_dir / filename)

    # 1. Ensure the file is not already downloaded
    if decompressed.exists():
        log(f"{decompressed} already exists, skipping download")
        return decompressed

    # 2. Try extract from a compressed version
    if compressed and compressed.exists():
        log(f"Found {compressed}, extracting to {decompressed}")
        try:
            return extract_file(filename, compressed, decompressed)
        except Exception as e:
            log(f"Failed to extract {filename}: {e}")

    # 3. Download then extract
    for i in range(MAX_RETRIES):
        try:
            if url.startswith(BASE_URL): # don't use session with creds elsewhere
                resp = session.get(url, timeout=30)
            else:
                resp = requests.get(url, timeout=30)
            resp.raise_for_status()

            if compressed:
                with open(compressed, "wb") as f_out:
                    f_out.write(resp.content)
                log(f"{compressed} downloaded, extracting to {decompressed}")
                return extract_file(filename, compressed, decompressed)
            else:
                with open(decompressed, "wb") as f_out:
                    f_out.write(resp.content)
                log(f"{decompressed} downloaded.")
                return decompressed
        except (HTTPException, HTTPError) as e:
            log(f"Session failed on attempt {i} to download {filename}: {e}")
            try:
                download_url(url, decompressed)
            except Exception as e:
                log(f"Failed attempt {1} to download {url}: {e}")
                return None

def download_metadata(download_dir: Path=INPUT_PRODUCTS_PATH, log_callback=None):
    download_products(pd.DataFrame(), download_dir, log_callback, metadata=True)

def download_products(products: pd.DataFrame, download_dir: Path=INPUT_PRODUCTS_PATH, log_callback=None,
                      metadata=True, start_time=None, end_time=None):
    """
    Downloads all products in the provided DataFrame to the specified directory.

    :param products : DataFrame (from get_product_dataframe) of all products to download
    :param download_dir: Directory to save downloaded files
    :param log_callback: Optional callback function for log messages (message)
    :param metadata: Optional boolean flag to determine if products should be downloaded
    :param start_time: NECESSARY FOR BRDC DOWNLOAD
    :param end_time: NECESSARY FOR BRDC DOWNLOAD
    :returns: None if not in generator mode; otherwise yields (filename, percent) tuples
    """
    def log(msg: str):
        log_callback(msg) if log_callback else print(msg)

    # 1. Retrieve filenames from the DataFrame
    log(f"📦 Found {len(products)} files in DataFrame")
    downloads = []
    for _, row in products.iterrows():
        gps_week = date_to_gpswk(row.date)
        if gps_week < 2237:
            # AAAWWWWD.TYP.Z
            # e.g. COD22360.FIN.SNX.gz
            if row.period == timedelta(days=7):
                day = 7
            else:
                day = int((row.date - gpswk_to_date(gps_week)).days)
            filename = f"{row.analysis_center.lower()}{gps_week}{day}.{row.format.lower()}.Z"
        else:
            # e.g. GRG0OPSFIN_20232620000_01D_01D_SOL.SNX.gz
            # AAA0OPSSNX_YYYYDDDHHMM_LEN_SMP_CNT.FMT.gz
            filename = f"{row.analysis_center}0{row.project}{row.solution_type}_{row.date.strftime('%Y%j%H%M')}_{row.period.days:02d}D_{row.resolution}_{row.content}.{row.format}.gz"

        url = f"{BASE_URL}/gnss/products/{gps_week}/{filename}"
        downloads.append(url)

    # 2. Add in metadata urls
    if metadata:
        for url in METADATA:
            downloads.append(url)
        if start_time and end_time:
            reference_dt = start_time - timedelta(days=1)
            while (end_time - reference_dt).total_seconds() > 0:
                day = reference_dt.strftime("%j")
                filename = f"BRDC00IGS_R_{reference_dt.year}{day}0000_01D_MN.rnx.gz"
                url = f"{BASE_URL}/gnss/data/daily/{reference_dt.year}/brdc/{filename}"
                downloads.append(url)
                reference_dt += timedelta(days=1)

    download_dir.mkdir(parents=True, exist_ok=True)
    (download_dir / "tables").mkdir(parents=True, exist_ok=True)
    sesh = requests.Session()
    sesh.auth = get_netrc_auth()
    for url in downloads:
        x = url.split("/")
        if len(x) < 2:
            fin_dir = download_dir
        else:
            fin_dir = download_dir / "tables" if x[-2]=="tables" else download_dir

        download_file(url, sesh, fin_dir, log_callback)
