import os
from pathlib import Path
from PySide6.QtCore import QUrl, Signal, QObject, QThread, Slot, Qt
from PySide6.QtWidgets import QMainWindow, QDialog, QVBoxLayout, QPushButton, QComboBox
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtGui import QTextCursor
from app.utils.cddis_credentials import validate_netrc as gui_validate_netrc
from app.models.execution import Execution
from app.utils.find_executable import get_pea_exec
from app.utils.ui_compilation import compile_ui
from app.controllers.input_controller import InputController
from app.controllers.visualisation_controller import VisualisationController
from app.utils.cddis_email import get_username_from_netrc, write_email, test_cddis_connection
from app.utils.download_products_https import start_metadata_download_thread
from app.utils.workers import PeaExecutionWorker, PPPDownloadWorker
from app.utils.archive_manager import archive_products_if_selection_changed
from app.models.execution import INPUT_PRODUCTS_PATH

# Optional toggle for development visualization testing
test_visualisation = False


def setup_main_window():
    compile_ui()  # Always recompile .ui files during development
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
        self.execution = Execution(executable=get_pea_exec())
        self.inputCtrl = InputController(self.ui, self, self.execution)
        self.visCtrl = VisualisationController(self.ui, self)

        # Connect ready signals
        self.inputCtrl.ready.connect(self.on_files_ready)
        self.inputCtrl.pea_ready.connect(self._on_process_clicked)

        # State
        self.rnx_file: str | None = None
        self.output_dir: str | None = None
        self.download_progress: dict[str, int] = {}  # track per-file progress
        self.is_processing = False

        # Visualisation widgets
        self.openInBrowserBtn = QPushButton("Open in Browser", self)
        self.ui.rightLayout.addWidget(self.openInBrowserBtn)
        self.visCtrl.bind_open_button(self.openInBrowserBtn)

        self.visSelector = QComboBox(self)
        self.ui.rightLayout.addWidget(self.visSelector)
        self.visCtrl.bind_selector(self.visSelector)

        # Validate CDDIS credentials
        self._validate_cddis_credentials_once()

        # Start validation and metadata download in a separate thread
        start_metadata_download_thread(self.log_message)

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

        # Time window comes from InputController
        start_time = self.inputCtrl.start_time
        end_time = self.inputCtrl.end_time

        # Archive old products if needed
        current_selection = {"ppp_provider": ac, "ppp_project": project, "ppp_series": series}
        archive_dir = archive_products_if_selection_changed(
            current_selection, getattr(self, "last_ppp_selection", None), INPUT_PRODUCTS_PATH
        )
        self.last_ppp_selection = current_selection
        if archive_dir:
            self.log_message(f"📦 Archived old PPP products → {archive_dir}")

        # Reset progress
        self.download_progress.clear()

        # Start download in background
        self.download_thread = QThread()
        self.download_worker = PPPDownloadWorker(
            handler=self.inputCtrl.cddis_handler,
            analysis_center=ac,
            project_type=project,
            solution_type=series,
            start_time=start_time,
            end_time=end_time,
            target_files=["SP3", "CLK", "BIA"],
            download_dir=INPUT_PRODUCTS_PATH,
            execution=self.execution,
        )
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

        self.log_message("📡 Starting PPP product downloads...")
        self.download_thread.start()

    @Slot(str, int)
    def _on_download_progress(self, filename: str, percent: int):
        """Update progress display in-place at the bottom of the UI terminal."""
        self.download_progress[filename] = percent

        # Build progress summary
        lines = []
        for f, p in self.download_progress.items():
            bar_len = 20
            filled = int(bar_len * p / 100)
            bar = "█" * filled + "-" * (bar_len - filled)
            lines.append(f"{f:30} [{bar}] {p:3d}%")
        text = "\n".join(lines)

        # Work with cursor & doc
        cursor = self.ui.terminalTextEdit.textCursor()
        doc = self.ui.terminalTextEdit.document()
        cursor.movePosition(QTextCursor.End)

        # Progress block = last N lines, where N = number of tracked files
        block = doc.findBlockByNumber(doc.blockCount() - len(self.download_progress))
        if block.isValid():
            # Replace old progress block
            cursor.setPosition(block.position())
            cursor.movePosition(QTextCursor.End, QTextCursor.KeepAnchor)
            cursor.removeSelectedText()
            cursor.insertText(text)
        else:
            # First time → just append bars
            cursor.insertText("\n" + text)

        self.ui.terminalTextEdit.setTextCursor(cursor)

    def _on_download_finished(self, success, message):
        self.log_message(message)
        if success:
            self._start_pea_execution()
        else:
            self._set_processing_state(False)

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