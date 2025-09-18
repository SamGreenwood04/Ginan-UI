# app/controllers/visualisation_controller.py
"""Controller responsible for everything inside the visualisation panel.

Responsibilities
----------------
1. Embed one of the generated HTML files into the QTextEdit area.
2. Maintain a list (indexed) of available HTML visualisations.
3. Provide a double-click handler and an explicit *Open* action that open the
   current html in the user's default browser.

NOTE:  UI widgets for selecting visualisation (e.g. a ComboBox or QListWidget)
       and an *Open* button are **not** yet present in the .ui file.  This
       controller exposes stub `bind_open_button()` / `bind_selector()` helpers
       which can be called once those widgets are added.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Sequence, Optional
from PySide6.QtCore import QRect, QUrl, QObject, QEvent
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import QTextEdit, QPushButton, QComboBox
from PySide6.QtWebEngineWidgets import QWebEngineView

HERE = Path(__file__).resolve()
ROOT = HERE.parents[2]
DEFAULT_OUT_DIR = ROOT / "tests" / "resources" / "outputData" / "visual"

class VisualisationController(QObject):
    """Manage visualisation panel interactions."""

    def __init__(self, ui, parent_window):
        super().__init__(parent_window)
        self.ui = ui  # Ui_MainWindow instance
        self.parent = parent_window
        self.html_files: List[str] = []  # paths of available visualisations
        self.current_index: Optional[int] = None
        self.external_base_url: Optional[str] = None
        self._selector: Optional[QComboBox] = None

        # Install event filter on the container to catch double-clicks
        self.ui.visualisationTextEdit.installEventFilter(self)

    # ---------------------------------------------------------------------
    # Public API (to be called from MainWindow / other controllers)
    # ---------------------------------------------------------------------
    def set_html_files(self, paths: Sequence[str]):
        """Register the list of generated html files, refresh selector, and default to #0."""
        self.html_files = list(paths)
        # Refresh selector if bound
        if self._selector:
            self._refresh_selector()
        if self.html_files:
            self.display_html(0)

    def display_html(self, index: int):
        """Embed the *index*-th html into the QTextEdit panel."""
        if not isinstance(index, int) or not (0 <= index < len(self.html_files)):
            return
        file_path = self.html_files[index]
        self.current_index = index
        self._embed_html(file_path)

    def open_current_external(self):
        """Open the currently displayed html in the system web browser."""
        if self.current_index is None:
            return
        path = self.html_files[self.current_index]
        if self.external_base_url:
            import pathlib
            try:
                project_root = pathlib.Path(__file__).resolve().parents[2]
                rel_path = pathlib.Path(path).resolve().relative_to(project_root)
                url = self.external_base_url + str(rel_path).replace(os.sep, '/')
                QDesktopServices.openUrl(QUrl(url))
                return
            except Exception:
                pass
        QDesktopServices.openUrl(QUrl.fromLocalFile(path))

    # ------------------------------------------------------------------
    # Helpers for wiring additional UI elements
    # ------------------------------------------------------------------
    def bind_open_button(self, button: QPushButton):
        """Wire an *Open* button to open the current html externally."""
        button.clicked.connect(self.open_current_external)

    def bind_selector(self, combo: QComboBox):
        """Wire a selector combo to this controller and populate on every update."""
        self._selector = combo

        def safe_display():
            data = combo.currentData()
            if isinstance(data, int):  # Only proceed if it's a valid index
                self.display_html(data)

        combo.currentIndexChanged.connect(lambda _: safe_display())
        self._refresh_selector()

    def _refresh_selector(self):
        """Helper to (re)fill the bound QComboBox with current html_files."""
        if not self._selector:
            return
        self._selector.clear()
        for idx, path in enumerate(self.html_files):
            self._selector.addItem(f"#{idx} – {os.path.basename(path)}", userData=idx)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def eventFilter(self, obj: QObject, event: QEvent) -> bool:
        """Catch double-clicks to open external view."""
        if event.type() == QEvent.MouseButtonDblClick:
            self.open_current_external()
            return True
        return super().eventFilter(obj, event)

    def _embed_html(self, file_path: str):
        container: QTextEdit = self.ui.visualisationTextEdit
        # Clean previous webviews
        for child in container.findChildren(QWebEngineView):
            child.setParent(None)
            child.deleteLater()

        webview = QWebEngineView(container)
        webview.setUrl(QUrl.fromLocalFile(file_path))

        rect: QRect = container.rect()
        webview.setGeometry(rect)
        webview.show()
        webview.setZoomFactor(0.8)

        # Also install event filter on the webview
        webview.installEventFilter(self)

        # keep reference to avoid GC
        self._webview = webview

    # ------------------------------------------------------------------
    # Optional configuration
    # ------------------------------------------------------------------
    def set_external_base_url(self, url: str):
        """Set a base HTTP URL; when provided external open uses this instead of file:// paths."""
        if not url.endswith('/'):
            url += '/'
        self.external_base_url = url

    def build_from_execution(self):
        """
        let model.execution batch generate .html from .pos,
        then merge the generated html list with the existing html files and pass it to the existing set_html_files() to display.
        """
        try:
            exec_obj = getattr(self.parent, "execution", None)
            if exec_obj is None:
                from PySide6.QtWidgets import QMessageBox
                QMessageBox.warning(self.ui, "Plot", "execution object is not set")
                return

            new_html_paths = exec_obj.build_pos_plots()  # default output to tests/resources/outputData/visual
            
            existing_html_paths = self._find_existing_html_files()

            all_html_paths = list(set(new_html_paths + existing_html_paths))
            
            all_html_paths.sort(key=lambda x: os.path.basename(x))
            
            self.set_html_files(all_html_paths)
            
        except Exception as e:
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.critical(self.ui, "Plot Error", str(e))
    
    def _find_existing_html_files(self):
        existing_files = []

        default_visual_dir = DEFAULT_OUT_DIR
        if default_visual_dir.exists():
            for html_file in default_visual_dir.glob("*.html"):
                existing_files.append(str(html_file))

        if self.external_base_url:
            pass
            
        return existing_files


    # # display pos plot
    # def show_pos_plot(self, pos_file):
    #     html_path = run_plot_pos(pos_file, "app/resources/pos_plot.html")
    #     self._embed_html(html_path)