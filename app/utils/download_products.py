from pathlib import Path

from app.models.cddis_handler import CDDIS_Handler
import subprocess
from app.models.execution import INPUT_PRODUCTS_PATH
from app.utils.auto_download_PPP import auto_download, auto_download_main


def download_ppp_products(inputs) -> bool:
    """Download PPP products using the CDDIS_handler and auto_download_PPP script"""
    start_datetime  = inputs.start_epoch.replace(" ", "_")
    end_datetime    = inputs.end_epoch.replace(" ", "_")

    cddis = CDDIS_Handler(start_datetime,end_datetime)

    analysis_center = inputs.ppp_provider.upper()
    project_type, solution_type = cddis.get_optimal_project_solution_tuple(analysis_center)

    if not project_type or not solution_type:
        print(f"No valid products available for {analysis_center}")
        return False

    try:
        download_static_products(start_datetime, end_datetime)
        download_dynamic_products(start_datetime, end_datetime, analysis_center, project_type, solution_type)
        return True
    except Exception as e:
        print(f"Error downloading PPP products: {e}")
        return False


def download_static_products(start_datetime: str, end_datetime: str) -> None:
    """Download static PPP products that don't change often"""

    print(f"Downloading static PPP products for {start_datetime} to {end_datetime}...")
    auto_download(most_recent=True, dont_replace=True,
                  target_dir=INPUT_PRODUCTS_PATH, start_datetime=start_datetime, end_datetime=end_datetime,
                  preset="real-time", atx=True, aload=True, igrf=True, oload=True, opole=True, planet=True,
                  sat_meta=True, yaw=True, gpt2=True, data_source="cddis", verbose=True)

    print("Static products downloaded successfully")

def download_dynamic_products(
        start_datetime: str, end_datetime: str,
        analysis_center: str, project_type: str, solution_type: str) -> None:
    """Download dynamic PPP products that change based on analysis center"""

    print(f"Downloading dynamic PPP products for {analysis_center}, {project_type}, {solution_type} for {start_datetime} to {end_datetime}...")
    auto_download(dont_replace=True, target_dir=INPUT_PRODUCTS_PATH,
                  start_datetime=start_datetime, end_datetime=end_datetime,
                  analysis_center=analysis_center, project_type=project_type,
                  solution_type=solution_type, preset="manual",
                  clk=True, sp3=True, bia=True, nav=True, iau2000=True,
                  data_source="cddis", bia_ac=analysis_center, verbose=True)

    print("Dynamic products downloaded successfully")
