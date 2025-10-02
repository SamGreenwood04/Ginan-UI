# app/utils/workers.py
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from PySide6.QtCore import QObject, Signal, Slot
from PySide6.QtWidgets import QTextEdit

from app.models.dl_products import (
    get_product_dataframe,
    download_products,
    get_brdc_urls,
    download_metadata,
)
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
        self._stop = False

    @Slot()
    def stop(self):
        self._stop = True
        self.log.emit("[DownloadMetadataWorker] Stop requested.")

    @Slot()
    def run(self):
        try:
            if self._stop:
                self.finished.emit("[DownloadMetadataWorker] Cancelled before start.")
                return

            def _log_cb(msg: str):
                self.log.emit(msg)
                if self._stop:
                    raise RuntimeError("Cancelled")

            self.log.emit("[DownloadMetadataWorker] Starting metadata download...")
            download_metadata(log_callback=_log_cb)

            if self._stop:
                self.finished.emit("[DownloadMetadataWorker] Cancelled.")
                return

            self.finished.emit("[DownloadMetadataWorker] Metadata downloaded.")
        except Exception:
            tb = traceback.format_exc()
            self.log.emit(f"[DownloadMetadataWorker] Exception:\n{tb}")
            self.error.emit(tb)


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
            # 推荐在 Execution 里实现 stop_all()，用于终止子进程
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
    progress = Signal(int)
    log = Signal(str)

    def __init__(
        self,
        start_epoch: datetime,
        end_epoch: datetime,
        download_dir: Path = INPUT_PRODUCTS_PATH,
        products: pd.DataFrame = pd.DataFrame(),
    ):
        super().__init__()
        self.products = products
        self.download_dir = download_dir
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch
        self._stop = False

    @Slot()
    def stop(self):
        self._stop = True
        self.log.emit("[PPPDownloadWorker] Stop requested.")

    @Slot()
    def run(self):
        def _log_cb(msg: str):
            self.log.emit(msg)
            if self._stop:
                raise RuntimeError("Cancelled")

        if self.products.empty:
            self.log.emit("[PPPDownloadWorker] No products specified, returning valid analysis centers...")
            try:
                if self._stop:
                    self.finished.emit("[PPPDownloadWorker] Cancelled before start.")
                    return
                valid_acs = get_product_dataframe(self.start_epoch, self.end_epoch)
                self.finished.emit(valid_acs)
            except Exception as e:
                tb = traceback.format_exc()
                self.log.emit(f"[PPPDownloadWorker] Exception during run:\n{tb}")
                self.error.emit(str(e))
            return

        self.log.emit("[PPPDownloadWorker] Starting products download...")

        try:
            if self._stop:
                self.finished.emit("[PPPDownloadWorker] Cancelled before metadata.")
                return

            # Ensure metadata present
            download_metadata(download_dir=self.download_dir, log_callback=_log_cb)

            if self._stop:
                self.finished.emit("[PPPDownloadWorker] Cancelled before products.")
                return

            urls = get_brdc_urls(self.start_epoch, self.end_epoch)

            if self._stop:
                self.finished.emit("[PPPDownloadWorker] Cancelled before download.")
                return

            download_products(
                self.products,
                download_dir=self.download_dir,
                log_callback=_log_cb,
                dl_urls=urls,
            )

            if self._stop:
                self.finished.emit("[PPPDownloadWorker] Cancelled.")
                return

            self.finished.emit("[PPPDownloadWorker] Downloaded all products successfully.")
        except Exception as e:
            tb = traceback.format_exc()
            self.log.emit(f"[PPPDownloadWorker] Exception during run:\n{tb}")
            self.error.emit(str(e))
