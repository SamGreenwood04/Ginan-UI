import threading
from app.models.execution import INPUT_PRODUCTS_PATH
from app.utils.auto_download_PPP import (
    download_atx,
    download_ocean_loading_model,
    download_atmosphere_loading_model,
    download_geomagnetic_model,
    download_geopotential_model,
    download_ocean_pole_tide_file,
    download_ocean_tide_potential_model,
    download_planetary_ephemerides_file,
    download_trop_model,
    download_satellite_metadata_snx,
    download_yaw_files
)

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
