# app/utils/workers.py
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from PySide6.QtCore import QObject, Signal, Slot

from app.models.dl_products import get_product_dataframe, download_products, get_brdc_urls, METADATA, download_metadata
from app.utils.common_dirs import INPUT_PRODUCTS_PATH


class PeaExecutionWorker(QObject):
    """
    Executes execute_config() method of a given PEAExecution instance.
    The 'execution' object is expected to implement:
      - execute_config()
      - stop_all()  (optional but recommended: terminate underlying process)
    """
    finished = Signal(object)
    error = Signal(str)
    log = Signal(str)

    def __init__(self, execution):
        super().__init__()
        self.execution = execution

    @Slot()
    def stop(self):
        try:
            self.log.emit("[PeaExecutionWorker] Stop requested — terminating PEA...")
            # recommended to implement stop_all() in Execution to terminate child processes
            if hasattr(self.execution, "stop_all"):
                self.execution.stop_all()
            self.finished.emit("[PeaExecutionWorker] Stopped.")
        except Exception:
            tb = traceback.format_exc()
            self.error.emit(f"[PeaExecutionWorker] Exception during stop:\n{tb}")

    @Slot()
    def run(self):
        try:
            self.log.emit("[PeaExecutionWorker] Starting PEA execution...")
            self.execution.execute_config()
            self.finished.emit("[PeaExecutionWorker] Execution finished successfully.")
        except Exception:
            tb = traceback.format_exc()
            self.error.emit(f"[PeaExecutionWorker] Exception:\n{tb}")


class DownloadWorker(QObject):
    """
    Downloads PPP and BRDC products for a specified date range or retrieves valid analysis centers.

    :param products: DataFrame of products to download. (See get_product_dataframe())
    :param download_dir: Directory to save downloaded products.
    :param start_epoch: Start datetime for BRDC files.
    :param end_epoch: End datetime for BRDC files.
    :param analysis_centers: Set to true to retrieve valid analysis centers, ensure start and end date specified
    """
    finished = Signal(object)
    error = Signal(str)
    progress = Signal(str, int)
    log = Signal(str)
    atx_downloaded = Signal(str)

    def __init__(self, start_epoch: Optional[datetime]=None, end_epoch: Optional[datetime]=None,
                 download_dir: Path=INPUT_PRODUCTS_PATH, products: pd.DataFrame=pd.DataFrame(), analysis_centers=False):
        super().__init__()
        self.products = products
        self.download_dir = download_dir
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch
        self.analysis_centers = analysis_centers
        self._stop = False

    @Slot()
    def stop(self):
        self._stop = True

    @Slot()
    def run(self):
        def _log_cb(msg: str):
            self.log.emit(msg)

        # 1. Get valid products
        if self.analysis_centers:
            if not self.start_epoch and not self.end_epoch:
                self.log.emit("[PPPDownloadWorker] No start and/or end date, can't check valid analysis centers")
                return
            self.log.emit("[PPPDownloadWorker] Retrieving valid products")
            try:
                valid_products = get_product_dataframe(self.start_epoch, self.end_epoch)
                self.finished.emit(valid_products)
            except Exception as e:
                tb = traceback.format_exc()
                self.log.emit(f"[PPPDownloadWorker] Error whilst retrieving valid products:\n{tb}")
                self.error.emit(str(e))
            return

        # 2. Install metadata
        elif self.products.empty:
            self.log.emit("[PPPDownloadWorker] Checking pre-processing metadata installed")
            try:
                download_metadata(self.download_dir, _log_cb, self.progress.emit, self.start_epoch, self.end_epoch,
                                  self.atx_downloaded.emit)
            except Exception as e:
                tb = traceback.format_exc()
                self.log.emit(f"[PPPDownloadWorker] Error whilst downloading metadata:\n{tb}")
                self.error.emit(str(e))
                return


        # 3. Install products
        else:
            self.log.emit("[PPPDownloadWorker] Downloading specified products")
            try:
                def check_stop():
                    return self._stop
                # Disregard generator output
                for _ in download_products(self.products, download_dir=self.download_dir, log_callback=_log_cb,
                                  dl_urls=get_brdc_urls(self.start_epoch, self.end_epoch),
                                  progress_callback=self.progress.emit, stop_requested=check_stop):
                    pass
            except RuntimeError as e:
                self.error.emit(str(e))
                return
            except Exception as e:
                tb = traceback.format_exc()
                self.log.emit(f"[PPPDownloadWorker] Error whilst downloading products:\n{tb}")
                self.error.emit(str(e))
                return

        self.finished.emit("[PPPDownloadWorker] Downloaded all products successfully.")