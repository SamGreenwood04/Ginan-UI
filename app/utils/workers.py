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

    def __init__(self, handler, analysis_center, project_type, solution_type,
                 start_time, end_time, target_files, download_dir, execution):
        super().__init__()
        self.handler = handler
        self.analysis_center = analysis_center
        self.project_type = project_type
        self.solution_type = solution_type
        self.start_time = start_time
        self.end_time = end_time
        self.target_files = target_files
        self.download_dir = download_dir
        self.execution = execution

    @Slot()
    def run(self):
        try:
            self.log.emit("🔍 PPPDownloadWorker: entering run()")
            self.log.emit(
                f"  analysis_center={self.analysis_center}, "
                f"project_type={self.project_type}, "
                f"solution_type={self.solution_type}, "
                f"start={self.start_time}, end={self.end_time}"
            )

            # --- Force consumption of generator if returned ---
            result = self.handler.download_products(
                analysis_center=self.analysis_center,
                project_type=self.project_type,
                solution_type=self.solution_type,
                start_time=self.start_time,
                end_time=self.end_time,
                target_files=self.target_files,
                download_dir=self.download_dir,
                progress_callback=lambda fn, p: self.progress.emit(fn, p),
            )

            # If a generator was returned, exhaust it
            if result is not None:
                for _ in result:
                    pass

            self.log.emit("✅ PPP product downloads completed.")

            # --- Auxiliary product downloads ---
            self.execution.download_pea_auxiliary_products(
                self.start_time,
                self.end_time,
                log_callback=self.log.emit
            )

            self.finished.emit(True, "✅ PPP + auxiliary products downloaded successfully.")

        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self.log.emit(f"❌ Exception in PPPDownloadWorker.run:\n{tb}")
            self.error.emit(str(e))
