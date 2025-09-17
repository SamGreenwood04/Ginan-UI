# app/utils/workers.py

import traceback
from PySide6.QtCore import QObject, Signal, Slot, QRunnable
from app.models.cddis_handler import CDDIS_Handler


# ==========================================================
# CDDIS Worker
# ==========================================================
class CDDISWorkerSignals(QObject):
    """
    Signals for CDDISWorker:
      - finished: emits (handler, result)
      - error: emits error message as str
    """
    finished = Signal(object, dict)  # (handler, result)
    error = Signal(str)


class CDDISWorker(QRunnable):
    """
    Runs a CDDIS_Handler in a background thread.
    Used to fetch available PPP product providers without blocking the UI.
    """
    def __init__(self, start_epoch, end_epoch, result):
        super().__init__()
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch
        self.result = result
        self.signals = CDDISWorkerSignals()

    @Slot()
    def run(self):
        try:
            print(f"[CDDISWorker] Running handler from {self.start_epoch} to {self.end_epoch}")
            handler = CDDIS_Handler(self.start_epoch, self.end_epoch)
            centers = handler.get_list_of_valid_analysis_centers()
            print(f"[CDDISWorker] Finished. Analysis Centers: {centers}")
            self.signals.finished.emit(handler, self.result)
        except Exception:
            tb = traceback.format_exc()
            print(f"[CDDISWorker] Exception:\n{tb}")
            self.signals.error.emit(tb)


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
            print(f"[PeaExecutionWorker] Exception:\n{tb}")
            self.error.emit(str(e))

