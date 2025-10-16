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
    """
    Function:
      Manage interactions and rendering inside the visualisation panel.

    Example:
      >>> controller = VisualisationController(ui, parent)
      >>> controller.set_html_files(["plot.html"])
    """
    
    def __init__(self, ui, parent_window):
        """
        Function:
          Initialize the visualisation controller.

        Arguments:
          ui: The main window UI instance.
          parent_window: The parent QMainWindow or controller.

        Example:
          >>> ctrl = VisualisationController(ui, main_window)
        """
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
        """
        Function:
          Register available HTML visualisation files and display the first one.

        Arguments:
          paths (Sequence[str]): List of file paths to HTML visualisations.

        Example:
          >>> controller.set_html_files(["plot1.html", "plot2.html"])
        """
        self.html_files = list(paths)
        # Refresh selector if bound
        if self._selector:
            self._refresh_selector()
        if self.html_files:
            self.display_html(0)

    def display_html(self, index: int):
        """
        Function:
          Display the specified HTML file within the QTextEdit area.

        Arguments:
          index (int): Index of the file to embed from the html_files list.

        Example:
          >>> controller.display_html(0)
        """
        if not isinstance(index, int) or not (0 <= index < len(self.html_files)):
            return
        file_path = self.html_files[index]
        self.current_index = index
        self._embed_html(file_path)

    def open_current_external(self):
        """
        Function:
          Open the currently displayed HTML in the system’s default web browser.

        Example:
          >>> controller.open_current_external()
        """
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
        """
        Function:
          Connect an *Open* button to open the current visualisation externally.

        Arguments:
          button (QPushButton): The button to connect.

        Example:
          >>> controller.bind_open_button(ui.openButton)
        """
        button.clicked.connect(self.open_current_external)

    def bind_selector(self, combo: QComboBox):
        """
        Function:
          Bind a QComboBox selector to manage and display HTML visualisations.

        Arguments:
          combo (QComboBox): The combo box used as selector.

        Example:
          >>> controller.bind_selector(ui.comboBox)
        """
        self._selector = combo

        def safe_display():
            data = combo.currentData()
            if isinstance(data, int):  # Only proceed if it's a valid index
                self.display_html(data)

        combo.currentIndexChanged.connect(lambda _: safe_display())
        self._refresh_selector()

    def _refresh_selector(self):
        """
        Function:
          Populate the selector combo box with available HTML files.
        """
        if not self._selector:
            return
        self._selector.clear()
        for idx, path in enumerate(self.html_files):
            self._selector.addItem(f"#{idx} – {os.path.basename(path)}", userData=idx)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def eventFilter(self, obj: QObject, event: QEvent) -> bool:
        """
        Function:
          Handle double-click events to open the visualisation externally.

        Arguments:
          obj (QObject): The source object.
          event (QEvent): The event being filtered.

        Returns:
          bool: True if handled, False otherwise.
        """
        if event.type() == QEvent.MouseButtonDblClick:
            self.open_current_external()
            return True
        return super().eventFilter(obj, event)

    def _embed_html(self, file_path: str):
        """
        Function:
          Embed an HTML file inside the QTextEdit container using QWebEngineView.

        Arguments:
          file_path (str): Local path to the HTML file.

        Example:
          >>> controller._embed_html("visual.html")
        """
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
        """
        Function:
          Define a base HTTP URL for opening HTML files externally via web links.

        Arguments:
          url (str): The base URL (must end with '/').

        Example:
          >>> controller.set_external_base_url("http://localhost:8000/")
        """
        if not url.endswith('/'):
            url += '/'
        self.external_base_url = url

    def build_from_execution(self):
        """
        Function:
          Generate and load visualisation HTMLs from the execution model.

        Raises:
          Exception: If generation or file merging fails.

        Example:
          >>> controller.build_from_execution()
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
        """
        Function:
          Locate and return paths of existing visualisation HTML files.

        Returns:
          list[str]: List of existing HTML file paths.

        Example:
          >>> controller._find_existing_html_files()
        """
        existing_files = []

        default_visual_dir = DEFAULT_OUT_DIR
        if default_visual_dir.exists():
            for html_file in default_visual_dir.glob("*.html"):
                existing_files.append(str(html_file))

        if self.external_base_url:
            pass
            
        return existing_files

