from pathlib import Path
from typing import Optional

from PySide6.QtCore import QUrl, Signal, QThread, Slot, Qt, QRegularExpression
from PySide6.QtWidgets import QMainWindow, QDialog, QVBoxLayout, QPushButton, QComboBox
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtGui import QTextCursor, QTextDocument

from app.utils.cddis_credentials import validate_netrc as gui_validate_netrc
from app.models.execution import Execution
from app.utils.ui_compilation import compile_ui
from app.controllers.input_controller import InputController
from app.controllers.visualisation_controller import VisualisationController
from app.utils.cddis_email import get_username_from_netrc, write_email, test_cddis_connection
from app.utils.workers import PeaExecutionWorker, DownloadWorker
from app.models.archive_manager import archive_products_if_selection_changed, archive_products, archive_old_outputs
from app.models.execution import INPUT_PRODUCTS_PATH

# Optional toggle for development visualization testing
test_visualisation = False


def setup_main_window():
    # compile_ui()  # Always recompile .ui files during development
    from app.views.main_window_ui import Ui_MainWindow
    return Ui_MainWindow()


class FullHtmlDialog(QDialog):
    def __init__(self, file_path: str):
        super().__init__()
        self.setWindowTitle("Full HTML View")
        layout = QVBoxLayout(self)
        webview = QWebEngineView(self)
        webview.setUrl(QUrl.fromLocalFile(file_path))
        layout.addWidget(webview)
        self.resize(800, 600)


class MainWindow(QMainWindow):
    log_signal = Signal(str)

    def __init__(self):
        super().__init__()

        # Setup UI
        self.ui = setup_main_window()
        self.ui.setupUi(self)

        # Unified logger
        self.log_signal.connect(self.log_message)

        # Controllers
        self.execution = Execution()
        self.inputCtrl = InputController(self.ui, self, self.execution)
        self.visCtrl = VisualisationController(self.ui, self)

        # Connect ready signals
        self.inputCtrl.ready.connect(self.on_files_ready)
        self.inputCtrl.pea_ready.connect(self._on_process_clicked)

        # State
        self.rnx_file: Optional[str] = None
        self.output_dir: Optional[str] = None
        self.download_progress: dict[str, int] = {}  # track per-file progress
        self.is_processing = False
        self.atx_required_for_rnx_extraction = False # File required to extract info from RINEX
        self.metadata_downloaded = False

        # Visualisation widgets
        self.openInBrowserBtn = QPushButton("Open in Browser", self)
        self.ui.rightLayout.addWidget(self.openInBrowserBtn)
        self.visCtrl.bind_open_button(self.openInBrowserBtn)

        self.visSelector = QComboBox(self)
        self.ui.rightLayout.addWidget(self.visSelector)
        self.visCtrl.bind_selector(self.visSelector)

        archive_products(INPUT_PRODUCTS_PATH, "startup_archival", True)

        # Validate connection then start metadata download in a separate thread
        self._validate_cddis_credentials_once()

        self.metadata_thread = QThread()
        self.metadata_worker = DownloadWorker()
        self.metadata_worker.moveToThread(self.metadata_thread)

        # Signals
        self.metadata_thread.started.connect(self.metadata_worker.run)
        self.metadata_worker.progress.connect(self._on_download_progress)
        self.metadata_worker.log.connect(self.log_message)
        self.metadata_worker.error.connect(self._on_download_error)
        self.metadata_worker.finished.connect(self._on_metadata_download_finished)
        self.metadata_worker.atx_downloaded.connect(self._on_atx_downloaded)

        # Cleanup
        self.metadata_worker.finished.connect(self.metadata_thread.quit)
        self.metadata_worker.finished.connect(self.metadata_worker.deleteLater)
        self.metadata_thread.finished.connect(self.metadata_thread.deleteLater)
        self.metadata_thread.start()

        # Added: wire an optional stop-all button if present in the UI
        if hasattr(self.ui, "stopAllButton") and self.ui.stopAllButton:
            self.ui.stopAllButton.clicked.connect(self.on_stopAllClicked)
        elif hasattr(self.ui, "btnStopAll") and self.ui.btnStopAll:
            self.ui.btnStopAll.clicked.connect(self.on_stopAllClicked)

    def log_message(self, msg: str):
        """Append a log line normally """
        self.ui.terminalTextEdit.append(msg)

    def _set_processing_state(self, processing: bool):
        """Enable/disable UI elements during processing"""
        self.is_processing = processing

        # Disable/enable the process button
        self.ui.processButton.setEnabled(not processing)

        # Optionally disable other critical UI elements during processing
        self.ui.observationsButton.setEnabled(not processing)
        self.ui.outputButton.setEnabled(not processing)
        self.ui.showConfigButton.setEnabled(not processing)

        # Update button text to show processing state
        if processing:
            self.ui.processButton.setText("Processing...")
            # Set cursor to waiting cursor for visual feedback
            self.setCursor(Qt.CursorShape.WaitCursor)
        else:
            self.ui.processButton.setText("Process")
            self.setCursor(Qt.CursorShape.ArrowCursor)

    def on_files_ready(self, rnx_path: str, out_path: str):
        # self.log_message(f"[DEBUG] on_files_ready: rnx={rnx_path}, out={out_path}")
        self.rnx_file = rnx_path
        self.output_dir = out_path

    def _on_process_clicked(self):
        if not self.rnx_file or not self.output_dir:
            self.log_message("⚠️ Please select RINEX and output directory first.")
            return

        # Prevent multiple simultaneous processing
        if self.is_processing:
            self.log_message("⚠️ Processing already in progress. Please wait...")
            return

        # Lock the "Process" button and set processing state
        self._set_processing_state(True)

        # Get PPP params from UI
        ac = self.ui.PPP_provider.currentText()
        project = self.ui.PPP_project.currentText()
        series = self.ui.PPP_series.currentText()

        # Archive old products if needed
        current_selection = {"ppp_provider": ac, "ppp_project": project, "ppp_series": series}
        archive_dir = archive_products_if_selection_changed(
            current_selection, getattr(self, "last_ppp_selection", None), INPUT_PRODUCTS_PATH
        )
        self.last_ppp_selection = current_selection
        if archive_dir:
           self.log_message(f"📦 Archived old PPP products → {archive_dir}")

        output_archive = archive_old_outputs(Path(self.output_dir), archive_dir)
        if output_archive:
            self.log_message(f" Archived old outputs → {output_archive}")

        # List products to be downloaded
        x = self.inputCtrl.products_df
        products = x.loc[(x["analysis_center"] == ac) & (x["project"] == project) & (x["solution_type"] == series)].drop_duplicates()

        # Reset progress
        self.download_progress.clear()

        # Start download in background
        self.download_thread = QThread()
        self.download_worker = DownloadWorker(products=products, start_epoch=self.inputCtrl.start_time, end_epoch=self.inputCtrl.end_time)
        self.download_worker.moveToThread(self.download_thread)

        # Signals
        self.download_thread.started.connect(self.download_worker.run)
        self.download_worker.progress.connect(self._on_download_progress)
        self.download_worker.log.connect(self.log_message)
        self.download_worker.finished.connect(self._on_download_finished)
        self.download_worker.error.connect(self._on_download_error)

        # Cleanup
        self.download_worker.finished.connect(self.download_thread.quit)
        self.download_worker.finished.connect(self.download_worker.deleteLater)
        self.download_thread.finished.connect(self.download_thread.deleteLater)

        self.download_worker.error.connect(self.download_thread.quit)
        self.download_worker.error.connect(self.download_worker.deleteLater)

        self.log_message("📡 Starting PPP product downloads...")
        self.download_thread.start()

    @Slot(str, int)
    def _on_download_progress(self, filename: str, percent: int):
        """Update progress display in-place at the bottom of the UI terminal."""
        self.download_progress[filename] = percent

        total_length = 20
        filled_length = int(percent/100 * total_length)
        bar = '[' + "█" * filled_length + "░" * (total_length - filled_length) + ']'
        output = f"{filename[:30]} {bar} {percent:3d}%"
        search_pattern = QRegularExpression(f"^{filename[:30]}.+%$")

        # Work with cursor & doc
        cursor = self.ui.terminalTextEdit.textCursor()
        cursor.movePosition(QTextCursor.End)
        flags = QTextDocument.FindFlag.FindBackward
        found_cursor = self.ui.terminalTextEdit.document().find(search_pattern, cursor, flags)

        on_latest_5_lines = self.ui.terminalTextEdit.document().blockCount() - found_cursor.blockNumber() <= 5
        if found_cursor.hasSelection() and on_latest_5_lines:
            found_cursor.movePosition(QTextCursor.EndOfLine) # Replaces final percent symbol too
            found_cursor.movePosition(QTextCursor.StartOfLine, QTextCursor.KeepAnchor)
            found_cursor.removeSelectedText()
            found_cursor.insertText(output)
        else: # Make new progress bar
            self.ui.terminalTextEdit.setTextCursor(cursor)
            cursor.insertText("\n" + output)
            cursor.movePosition(QTextCursor.End)
            self.ui.terminalTextEdit.setTextCursor(cursor)

    def _on_atx_downloaded(self, filename: str):
        self.atx_required_for_rnx_extraction = True
        self.log_message(f"✅ ATX file {filename} installed - ready for RINEX parsing.")

    def _on_metadata_download_finished(self, message):
        self.log_message(message)
        self.metadata_downloaded = True
        self.inputCtrl.try_enable_process_button()

    def _on_download_finished(self, message):
        self.log_message(message)
        self._start_pea_execution()

    def _on_download_error(self, msg):
        self.log_message(f"⚠️ PPP download error: {msg}")
        self._set_processing_state(False)

    def _start_pea_execution(self):
        self.log_message("⚙️ Starting PEA execution in background...")

        self.thread = QThread()
        self.worker = PeaExecutionWorker(self.execution)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self._on_pea_finished)
        self.worker.error.connect(self._on_pea_error)

        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    def _on_pea_finished(self):
        self.log_message("✅ PEA processing completed.")
        self._run_visualisation()
        self._set_processing_state(False)

    def _on_pea_error(self, msg: str):
        self.log_message(f"⚠️ PEA execution failed: {msg}")
        self._set_processing_state(False)

    def _run_visualisation(self):
        try:
            self.log_message("📊 Generating plots from PEA output...")
            html_files = self.execution.build_pos_plots()
            if html_files:
                self.log_message(f"✅ {len(html_files)} plots generated.")
                self.visCtrl.set_html_files(html_files)
            else:
                self.log_message("⚠️ No plots found.")
        except Exception as err:
            self.log_message(f"⚠️ Plot generation failed: {err}")

        if test_visualisation:
            try:
                self.log_message("[Dev] Testing static visualisation...")
                test_output_dir = Path(__file__).resolve().parents[1] / "tests" / "resources" / "outputData"
                test_visual_dir = test_output_dir / "visual"
                test_visual_dir.mkdir(parents=True, exist_ok=True)
                self.visCtrl.build_from_execution()
                self.log_message("[Dev] Static plot generation complete.")
            except Exception as err:
                self.log_message(f"[Dev] Test plot generation failed: {err}")

    def _validate_cddis_credentials_once(self):
        ok, where = gui_validate_netrc()
        if not ok and hasattr(self.ui, "cddisCredentialsButton"):
            self.log_message("⚠️  No Earthdata credentials. Opening CDDIS Credentials dialog…")
            self.ui.cddisCredentialsButton.click()
            ok, where = gui_validate_netrc()
        if not ok:
            self.log_message(f"❌ Credentials invalid: {where}")
            return
        self.log_message(f"✅ Earthdata Credentials found: {where}")

        ok_user, email_candidate = get_username_from_netrc()
        if not ok_user:
            self.log_message(f"❌ Cannot read username from .netrc: {email_candidate}")
            return

        ok_conn, why = test_cddis_connection()
        if not ok_conn:
            self.log_message(
                f"❌ CDDIS connectivity check failed: {why}. Please verify Earthdata credentials via the CDDIS Credentials dialog."
            )
            return
        self.log_message(f"✅ CDDIS connectivity check passed in {why.split(' ')[-2]} seconds.")

        write_email(email_candidate)
        self.log_message(f"✉️ EMAIL set to: {email_candidate}")

    # Added: unified stop entry, wired to an optional UI button
    @Slot()
    def on_stopAllClicked(self):
        self.log_message("🛑 Stop requested — stopping all running tasks...")

        # Stop the metadata worker in InputController, if present
        try:
            if hasattr(self, "inputCtrl") and hasattr(self.inputCtrl, "stop_all"):
                self.inputCtrl.stop_all()
        except Exception:
            pass

        # Stop PPP downloads, if running
        try:
            if hasattr(self, "download_worker") and self.download_worker is not None and hasattr(self.download_worker, "stop"):
                # self.log_message("[UI] Stop → PPP downloads")
                self.download_worker.stop()
        except Exception:
            pass

        # Stop PEA execution, if running
        try:
            if hasattr(self, "worker") and self.worker is not None and hasattr(self.worker, "stop"):
                # self.log_message("[UI] Stop → PEA worker")
                self.worker.stop()
        except Exception:
            pass

        # Best-effort: ask Execution to stop any external process if supported
        try:
            if hasattr(self, "execution") and self.execution is not None and hasattr(self.execution, "stop_all"):
                self.execution.stop_all()
        except Exception:
            pass

        # Restore UI state immediately
        try:
            self._set_processing_state(False)
        except Exception:
            pass

        # self.log_message("🛑 Stop signals sent.")
