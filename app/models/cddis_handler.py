import gzip
import shutil
import time
from datetime import datetime, timedelta
from netrc import netrc
from pathlib import Path

import pandas as pd
import numpy as np

from app.utils.cddis_email import get_netrc_auth
from app.utils.gn_functions import GPSDate
import requests
from bs4 import BeautifulSoup, SoupStrainer

BASE_URL = "https://cddis.nasa.gov/archive"
MAX_RETRIES = 3

def date_to_gpswk(date: datetime) -> int:
    return int(GPSDate(np.datetime64(date)).gpswk)

def gpswk_to_date(gps_week: int) -> datetime:
    return GPSDate(gps_week).as_datetime

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
                    center = filename[0:3] # e.g. "COD"
                    _type = "FIN"  # pre-2237 were probably always final solutions :shrug:
                    day = int(filename[7])  # e.g. "0", 0-indexed, 7 indicates weekly
                    _format = filename[9:12] # e.g. "snx", "ssc", "sum", "erp"
                    project = None
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
    :param start_time: the start of the time window (use str_to_datetime helper function)
    :param end_time: the start of the time window (use str_to_datetime helper function)
    :param target_files: list of target files to filter for, defaulted to ["CLK","BIA","SP3"]
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

def download_products(products: pd.DataFrame, download_dir: Path = Path("./downloads"), progress_callback=None,
                      log_callback=None, yield_progress: bool = False,
):
    """
    Downloads all products in the provided DataFrame to the specified directory.

    :param products : DataFrame containing product metadata to download
    :param download_dir: Directory to save downloaded files
    :param progress_callback: Optional callback function for progress updates (filename, percent)
    :param log_callback: Optional callback function for log messages (message)
    :param yield_progress: If True, function acts as a generator yielding (filename, percent) tuples
    :returns: None if not in generator mode; otherwise yields (filename, percent) tuples
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.auth = get_netrc_auth()

    if log_callback:
        log_callback(f"📦 Found {len(products)} files to download")
    else:
        print(f"📦 Found {len(products)} files to download")

    for _, row in products.iterrows():
        gps_week = date_to_gpswk(row.date)
        if gps_week < 2237:
            # AAAWWWWD.TYP.Z
            # e.g. COD22360.FIN.SNX.gz
            if row.period == timedelta(days=7):
                day = 7
            else:
                day = int((row.date - gpswk_to_date(gps_week)).days)
            filename = f"{row.analysis_center}{gps_week}{day}.{row.format}.gz"
        else:
            # e.g. GRG0OPSFIN_20232620000_01D_01D_SOL.SNX.gz
            # AAA0OPSSNX_YYYYDDDHHMM_LEN_SMP_CNT.FMT.gz
            filename = f"{row.analysis_center}0{row.project}{row.solution_type}_{row.date.strftime('%Y%j%H%M')}_{row.period.days:02d}D_{row.resolution}_{row.content}.{row.format}.gz"

        url = f"{BASE_URL}/gnss/products/{gps_week}/{filename}"
        if log_callback:
            log_callback(f"[Handler] Preparing to download: {url}")
        else:
            print(f"[Handler] Preparing to download: {url}")

        final_file = download_dir / filename
        partial_file = final_file.with_suffix(final_file.suffix + ".part")
        decompressed_path = final_file.with_suffix('') if final_file.suffix == ".gz" else final_file

        # Already exists?
        if decompressed_path.exists():
            msg = f"✅ Skipping (already exists): {decompressed_path.name}"
            log_callback(msg) if log_callback else print(msg)
            continue

        # Existing .gz but not decompressed
        if final_file.exists() and final_file.suffix == ".gz":
            try:
                with gzip.open(final_file, 'rb') as f_in, open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                final_file.unlink()
                msg = f"🗜️ Decompressed existing: {decompressed_path.name}"
                log_callback(msg) if log_callback else print(msg)
                continue
            except Exception as e:
                msg = f"❌ Failed to decompress existing {final_file.name}: {e}"
                log_callback(msg) if log_callback else print(msg)

        # --- Retry & resume loop ---
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                headers = {}
                mode = "wb"
                existing_size = 0

                if partial_file.exists():
                    existing_size = partial_file.stat().st_size
                    if existing_size > 0:
                        headers = {"Range": f"bytes={existing_size}-"}
                        mode = "ab"
                        msg = f"↪️ Resuming {filename} from byte {existing_size}"
                        log_callback(msg) if log_callback else print(msg)

                msg = f"⬇️ Downloading: {filename} (attempt {attempt}/{MAX_RETRIES})"
                log_callback(msg) if log_callback else print(msg)

                response = session.get(url, headers=headers, stream=True, timeout=30)
                with response as r:
                    if r.status_code == 416:  # Range beyond EOF → treat as complete
                        msg = f"⚠️ Server says {filename} already complete."
                        log_callback(msg) if log_callback else print(msg)
                        break
                    r.raise_for_status()

                    total_size = int(r.headers.get("Content-Length", 0)) + existing_size
                    downloaded = existing_size

                    with open(partial_file, mode) as f:
                        for chunk in r.iter_content(512 * 512):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)

                                percent = int(downloaded * 100 / total_size) if total_size > 0 else 100
                                if progress_callback:
                                    progress_callback(filename, percent)
                                if yield_progress:
                                    yield filename, percent

                # Rename partial → final
                partial_file.rename(final_file)

                # Decompress if needed
                if final_file.suffix == ".gz":
                    with gzip.open(final_file, 'rb') as f_in, open(decompressed_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                    final_file.unlink()
                    msg = f"🗜️ Decompressed to: {decompressed_path.name}"
                    log_callback(msg) if log_callback else print(msg)

                # ✅ per-file completion log
                msg = f"✅ Finished downloading {decompressed_path.name}"
                log_callback(msg) if log_callback else print(msg)

                break  # success, exit retry loop

            except Exception as e:
                if attempt == MAX_RETRIES:
                    msg = f"❌ Failed to download {filename} after {MAX_RETRIES} attempts: {e}"
                    log_callback(msg) if log_callback else print(msg)
                    if partial_file.exists():
                        partial_file.unlink()
                else:
                    wait = 5 * attempt
                    msg = f"⚠️ Download failed ({e}), retrying in {wait}s..."
                    log_callback(msg) if log_callback else print(msg)
                    time.sleep(wait)
                    continue  # retry same file

    # In generator mode, function is already exhausted by the caller
    if not yield_progress:
        return None

