# app/utils/workers.py
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from PySide6.QtCore import QObject, Signal, Slot, QRunnable
from PySide6.QtWidgets import QTextEdit

from app.models.dl_products import get_product_dataframe, download_products, get_brdc_urls, download_metadata
from app.utils.common_dirs import INPUT_PRODUCTS_PATH


class DownloadMetadataWorker(QObject):
    """
    Downloads metadata that doesn't require a specific date range.
    """
    finished = Signal(object)
    error = Signal(str)
    log = Signal(str)

    def __init__(self):
        super().__init__()

    @Slot()
    def run(self):
        try:
            self.log.emit("[DownloadMetadataWorker] Starting metadata download...")
            download_metadata(log_callback=self.log.emit)
            self.finished.emit("[DownloadMetadataWorker] Metadata downloaded.")
        except Exception:
            tb = traceback.format_exc()
            self.log.emit(f"[CDDISWorker] Exception:\n{tb}")
            self.error.emit(tb)


class PeaExecutionWorker(QObject):
    """
    Executes execute_config() method of a given PEAExecution instance.

    :param execution: An instance of PEAExecution.
    """
    finished = Signal(object)
    error = Signal(str)
    log = Signal(str)

    def __init__(self, execution):
        super().__init__()
        self.execution = execution

    @Slot()
    def run(self):
        try:
            self.log.emit("[PeaExecutionWorker] Starting PEA execution...")
            self.execution.execute_config()
            self.finished.emit("[PeaExecutionWorker] Execution finished successfully.")
        except Exception:
            tb = traceback.format_exc()
            self.error.emit(f"[PeaExecutionWorker] Exception:\n{tb}")

class PPPWorker(QObject):
    """
    Downloads PPP and BRDC products for a specified date range or retrieves valid analysis centers.

    LEAVE PRODUCTS EMPTY TO RETURN VALID ANALYSIS CENTERS.

    :param products: DataFrame of products to download. (See get_product_dataframe())
    :param download_dir: Directory to save downloaded products.
    :param start_epoch: Start datetime for BRDC files.
    :param end_epoch: End datetime for BRDC files.
    """
    finished = Signal(object)
    error = Signal(str)
    progress = Signal(str, int)
    log = Signal(str)

    def __init__(self, start_epoch: Optional[datetime]=None, end_epoch: Optional[datetime]=None,
                 download_dir: Path=INPUT_PRODUCTS_PATH, products: pd.DataFrame=pd.DataFrame()):
        super().__init__()
        self.products = products
        self.download_dir = download_dir
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch

    @Slot()
    def run(self):
        if self.products.empty and self.start_epoch and self.end_epoch:
            self.log.emit("[PPPDownloadWorker] No products specified, start and end epochs specified, returning valid analysis centers")
            try:
                valid_acs = get_product_dataframe(self.start_epoch, self.end_epoch)
                self.finished.emit(valid_acs)
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                self.log.emit(f"[PPPDownloadWorker] Exception during run:\n{tb}")
                self.error.emit(str(e))
            return

        # If products are specified, proceed to download
        self.log.emit("[PPPDownloadWorker] Starting products download...")

        try:
            if self.products.empty and not self.start_epoch and not self.end_epoch:
                self.log.emit("[PPPDownloadWorker] No products specified, start and end epochs not specified, downloading metadata")
                # Make sure metadata downloaded (archiver is buggy atm)
                download_metadata(download_dir=self.download_dir, log_callback=self.log.emit, progress_callback=self.progress.emit)
            else:
                self.log.emit("[PPPDownloadWorker] Products specified, downloading products")
                download_products(self.products, download_dir=self.download_dir, log_callback=self.log.emit,
                          dl_urls=get_brdc_urls(self.start_epoch, self.end_epoch),
                          progress_callback=self.progress.emit)

            self.finished.emit("[PPPDownloadWorker] Downloaded all products successfully.")

        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self.log.emit(f"[PPPDownloadWorker] Exception during run:\n{tb}")
            self.error.emit(str(e))