# app/utils/workers.py

import traceback
from datetime import datetime
import pandas as pd
from PySide6.QtCore import QObject, Signal, Slot, QRunnable
from app.models.cddis_handler import get_product_dataframe, download_products
from app.utils.download_products_https import download_pea_auxiliary_products


# ==========================================================
# CDDIS Worker
# ==========================================================
class CDDISWorkerSignals(QObject):
    """
    Signals for CDDISWorker:
      - finished: emits (handler, result)
      - error: emits error message as str
    """
    finished = Signal(set)  # (handler, result)
    error = Signal(str)


class CDDISWorker(QRunnable):
    """
    Runs a CDDIS_Handler in a background thread.
    Used to fetch available PPP product providers without blocking the UI.
    """
    def __init__(self, start_epoch: datetime, end_epoch: datetime):
        super().__init__()
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch
        self.signals = CDDISWorkerSignals()

    @Slot()
    def run(self):
        try:
            print(f"[CDDISWorker] Running handler from {self.start_epoch} to {self.end_epoch}")
            data = get_product_dataframe(self.start_epoch, self.end_epoch)
            print(f"[CDDISWorker] Finished fetching data")
            self.signals.finished.emit(data)
        except Exception:
            tb = traceback.format_exc()
            print(f"[CDDISWorker] Exception:\n{tb}")
            self.signals.error.emit(tb)

# ==========================================================
# PEA execution Worker 
# ==========================================================
class PeaExecutionWorker(QObject):
    """
    Runs execution.execute_config() in a background thread.
    Ensures GUI remains responsive while PEA is running.
    """
    finished = Signal()
    error = Signal(str)

    def __init__(self, execution):
        super().__init__()
        self.execution = execution

    @Slot()
    def run(self):
        try:
            print("[PeaExecutionWorker] Starting PEA execution...")
            self.execution.execute_config()
            print("[PeaExecutionWorker] Execution finished successfully.")
            self.finished.emit()
        except Exception as e:
            tb = traceback.format_exc()
            print(f"[PeaExecutionWorker] Exception:\n{tb}")
            self.error.emit(str(e))

# ==========================================================
# PPP Download Worker (PPP + Aux products)
# ==========================================================
class PPPDownloadWorker(QObject):
    finished = Signal(bool, str)      # success, message
    error = Signal(str)
    progress = Signal(str, int)       # filename, percent
    log = Signal(str)                 # signal for log messages

    def __init__(self, products: pd.DataFrame, download_dir, execution, start_epoch, end_epoch):
        super().__init__()
        self.products = products
        self.download_dir = download_dir
        self.execution = execution
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch

    @Slot()
    def run(self):
        self.log.emit("🔍 PPPDownloadWorker: entering run()")
        self.log.emit(f"Downloading {self.products.to_string()}")
        try:
            # --- Force consumption of generator if returned ---
            result = download_products(self.products, download_dir=self.download_dir,
                                       progress_callback=lambda fn, p: self.progress.emit(fn, p))

            # If a generator was returned, exhaust it
            if result is not None:
                for _ in result:
                    pass

            self.log.emit("✅ PPP product downloads completed.")

            # --- Auxiliary product downloads ---
            download_pea_auxiliary_products(self.start_epoch, self.end_epoch)

            self.finished.emit(True, "✅ PPP + auxiliary products downloaded successfully.")

        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self.log.emit(f"❌ Exception in PPPDownloadWorker.run:\n{tb}")
            self.error.emit(str(e))
