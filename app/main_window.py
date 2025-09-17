import os
from pathlib import Path
from PySide6.QtCore import QUrl, Signal, QObject, QThread, Slot
from PySide6.QtWidgets import QMainWindow, QDialog, QVBoxLayout, QPushButton, QComboBox
from PySide6.QtWebEngineWidgets import QWebEngineView


from app.models.execution import Execution
from app.utils.find_executable import get_pea_exec
from app.utils.ui_compilation import compile_ui
from app.controllers.input_controller import InputController
from app.controllers.visualisation_controller import VisualisationController
from app.utils.cddis_email import get_username_from_netrc, write_email, test_cddis_connection
from app.utils.metadata_download import start_metadata_download_thread
from app.utils.workers import PeaExecutionWorker

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
    log_signal = Signal(str)  # Declare log_signal here

    def __init__(self):
        super().__init__()

        # Setup UI
        self.ui = setup_main_window()
        self.ui.setupUi(self)
    
        # UI Terminal Logging
        self.log_signal.connect(self.ui.terminalTextEdit.append)

        # Controllers
        self.execution = Execution(executable=get_pea_exec())
        self.inputCtrl = InputController(self.ui, self, self.execution)
        self.visCtrl = VisualisationController(self.ui, self)

        # Connect ready signals
        self.inputCtrl.ready.connect(self.on_files_ready)
        self.inputCtrl.pea_ready.connect(self._on_process_clicked)

        # Store file paths for PEA
        self.rnx_file: str | None = None
        self.output_dir: str | None = None

        # Visualisation widgets
        self.openInBrowserBtn = QPushButton("Open in Browser", self)
        self.ui.rightLayout.addWidget(self.openInBrowserBtn)
        self.visCtrl.bind_open_button(self.openInBrowserBtn)

        self.visSelector = QComboBox(self)
        self.ui.rightLayout.addWidget(self.visSelector)
        self.visCtrl.bind_selector(self.visSelector)

        #Validate CDDIS credentials 
        self._validate_cddis_credentials_once()

        #Start validtion and download of Ginan metadata files in a separate thread
        start_metadata_download_thread(self.ui.terminalTextEdit.append)

    def on_files_ready(self, rnx_path: str, out_path: str):
        print(f"[DEBUG MainWindow] on_files_ready called with rnx_path={rnx_path}, out_path={out_path}")
        self.ui.terminalTextEdit.append(f"[DEBUG] on_files_ready: rnx={rnx_path}, out={out_path}")

        self.rnx_file = rnx_path
        self.output_dir = out_path

        print(f"[DEBUG MainWindow] State updated: self.rnx_file={self.rnx_file}, self.output_dir={self.output_dir}")
    
    def _on_process_clicked(self):
        if not self.rnx_file or not self.output_dir:
            self.ui.terminalTextEdit.append("⚠️ Please select RINEX and output directory first.")
            return

        # Create thread + worker for PEA
        self.thread = QThread()
        self.worker = PeaExecutionWorker(self.execution)
        self.worker.moveToThread(self.thread)

        # Connect signals
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self._on_pea_finished)
        self.worker.error.connect(self._on_pea_error)

        # Cleanup: quit + delete thread/worker when done
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)

        # Cleanup for error path
        self.worker.error.connect(self.thread.quit)
        self.worker.error.connect(self.worker.deleteLater)

        # Start background thread
        self.ui.terminalTextEdit.append("⚙️ Starting PEA execution in background...")
        self.thread.start()

    def _on_pea_finished(self):
        self.ui.terminalTextEdit.append("✅ PEA processing completed.")
        # Trigger visualisation after PEA finishes
        self._run_visualisation()

    def _on_pea_error(self, msg: str):
        self.ui.terminalTextEdit.append(f"⚠️ PEA execution failed: {msg}")

    def _run_visualisation(self):
        try:
            self.ui.terminalTextEdit.append("📊 Generating plots from PEA output...")
            html_files = self.execution.build_pos_plots()
            if html_files:
                self.ui.terminalTextEdit.append(f"✅ {len(html_files)} plots generated.")
                self.visCtrl.set_html_files(html_files)
            else:
                self.ui.terminalTextEdit.append("⚠️ No plots found.")
        except Exception as err:
            self.ui.terminalTextEdit.append(f"⚠️ Plot generation failed: {err}")
        
        # Optional: test visualisation path
        if test_visualisation:
            try:
                self.ui.terminalTextEdit.append("[Dev] Testing static visualisation...")
                test_output_dir = Path(__file__).resolve().parents[1] / "tests" / "resources" / "outputData"
                test_visual_dir = test_output_dir / "visual"
                test_visual_dir.mkdir(parents=True, exist_ok=True)

                self.visCtrl.build_from_execution()

                self.ui.terminalTextEdit.append("[Dev] Static plot generation complete.")
            except Exception as err:
                self.ui.terminalTextEdit.append(f"[Dev] Test plot generation failed: {err}")

    def _validate_cddis_credentials_once(self):
        from app.utils.cddis_credentials import validate_netrc as gui_validate_netrc

        #ok, where = gui_validate_netrc()
        #if not ok and hasattr(self.ui, "cddisCredentialsButton"):
        #    self.ui.terminalTextEdit.append("⚠️ No Earthdata credentials found. Please set them using the CDDIS Credentials button.")
        #else:
        #    self.ui.terminalTextEdit.append(f"✅ Earthdata credentials found: {where}")

        # === CDDIS (HTTPS) pre-check — terminate immediately on failure; proceed with the legacy flow only on success ===
        # 1) Earthdata credential validation; if missing, prompt with the existing Credentials dialog
        ok, where = gui_validate_netrc()
        if not ok and hasattr(self.ui, "cddisCredentialsButton"):
            self.ui.terminalTextEdit.append("⚠️  No Earthdata credentials. Opening CDDIS Credentials dialog…")
            self.ui.cddisCredentialsButton.click()
            ok, where = gui_validate_netrc()
        if not ok:
            self.ui.terminalTextEdit.append(f"❌ Credentials invalid: {where}")
            return
        self.ui.terminalTextEdit.append(f"✅ Earthdata Credentials found: {where}")

        # 2) Read the username from .netrc (team convention: username == email; no file write at this stage)）
        ok_user, email_candidate = get_username_from_netrc()
        if not ok_user:
            self.ui.terminalTextEdit.append(f"❌ Cannot read username from .netrc: {email_candidate}")
            return

        # 3) Connectivity + authentication test (two-phase with requests.Session）
        ok_conn, why = test_cddis_connection()
        if not ok_conn:
            self.ui.terminalTextEdit.append(
                f"❌ CDDIS connectivity check failed: {why}. Please verify Earthdata credentials via the CDDIS Credentials dialog."
            )
            return
        self.ui.terminalTextEdit.append("✅ CDDIS connectivity check passed.")

        # Accept/write EMAIL only after passing the test
        write_email(email_candidate)
        self.ui.terminalTextEdit.append(f"✉️ EMAIL set to: {email_candidate}")

