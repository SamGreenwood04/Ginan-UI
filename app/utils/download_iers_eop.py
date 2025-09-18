# app/utils/download_iers_eop.py

import requests
from pathlib import Path

def download_iau2000_eop_from_url(download_dir: Path, if_file_present: str = "dont_repalce"):
    """
    Downloads the IAU2000 EOP file directly from IERS datacenter latest version URL,
    unless it already exists and skipping is requested.

    :param download_dir: The directory where the file will be saved
    :param if_file_present: "skip", "replace", or "error"
    :return: Path to the downloaded file or None if skipped/failed
    """
    url = "https://datacenter.iers.org/data/latestVersion/finals.data.iau2000.txt"
    output_path = download_dir / "finals.data.iau2000.txt"

    if output_path.exists():
        if if_file_present == "dont_replace":
            print(f"ℹ️ IERS EOP file already exists, skipping download: {output_path}")
            return output_path
        elif if_file_present == "error":
            print(f"❌ IERS EOP file already exists and 'error' policy set: {output_path}")
            return None
        elif if_file_present == "replace":
            print(f"🔁 Replacing existing IERS EOP file: {output_path}")
            output_path.unlink()

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        with open(output_path, 'wb') as f:
            f.write(response.content)

        print(f"✅ IERS IAU2000 EOP file downloaded to {output_path}")
        return output_path

    except requests.RequestException as e:
        print(f"❌ Failed to download IERS IAU2000 EOP file: {e}")
        return None
