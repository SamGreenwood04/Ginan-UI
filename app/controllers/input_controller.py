# app/controllers/input_controller.py

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, List
from decimal import Decimal, InvalidOperation

import pandas as pd

from app.models.cddis_handler import get_valid_analysis_centers, str_to_datetime
from PySide6.QtCore import QObject, Signal, Qt, QDateTime, QRunnable, Slot, QThreadPool
from PySide6.QtGui import QStandardItemModel, QStandardItem
from PySide6.QtWidgets import (
    QFileDialog,
    QDialog,
    QFormLayout,
    QDoubleSpinBox,
    QHBoxLayout,
    QPushButton,
    QVBoxLayout,
    QDateTimeEdit,
    QInputDialog,
    QMessageBox,
    QComboBox,
    QLineEdit,
    QPushButton,
    QLabel
)

from app.models.execution import Execution, GENERATED_YAML, TEMPLATE_PATH, INPUT_PRODUCTS_PATH
from app.models.rinex_extractor import RinexExtractor
from app.utils.cddis_credentials import save_earthdata_credentials
from app.utils.archive_manager import (archive_products_if_rinex_changed, archive_products_if_selection_changed)
from app.utils.archive_manager import archive_old_outputs
from app.utils.workers import CDDISWorker

class InputController(QObject):
    """
    Front-end controller

    Owns all UI input flows:
        - Select RNX file and Output directory
        - Extract RINEX metadata and apply to UI
        - Populate / handle config widgets (Mode, Constellations, etc.)
        - Open small dialogs for selecting some values
        - Show config file and run PEA processing

    Emits:
        ready(rnx_path: str, output_path: str)

        when both RNX and output dir are set.
    """

    ready = Signal(str, str) # rnx_path, output_path
    pea_ready = Signal() # emitted when PEA processing should start

    def __init__(self, ui, parent_window, execution: Execution):
        super().__init__()
        self.ui = ui
        self.parent = parent_window
        self.execution = execution

        self.rnx_file: str = ""
        self.output_dir: str = ""
        self.products_df: pd.DataFrame = pd.DataFrame() # CDDIS replaces with a populated dataframe

        # Config file path
        self.config_path = GENERATED_YAML

        ### Wire: file selection buttons ###
        self.ui.observationsButton.clicked.connect(self.load_rnx_file)
        self.ui.outputButton.clicked.connect(self.load_output_dir)

        # Initial states
        self.ui.outputButton.setEnabled(False)
        self.ui.showConfigButton.setEnabled(False)
        self.ui.processButton.setEnabled(False)

        ### Bind: configuration drop-downs / UIs ###

        self._bind_combo(self.ui.Mode, self._get_mode_items)

        # PPP_provider, project and series
        self.ui.PPP_provider.currentTextChanged.connect(self._on_ppp_provider_changed)
        self.ui.PPP_project.currentTextChanged.connect(self._on_ppp_project_changed)
        self.ui.PPP_series.currentTextChanged.connect(self._on_ppp_series_changed)

        # Constellations
        self._bind_multiselect_combo(
            self.ui.Constellations_2,
            self._get_constellations_items,
            self.ui.constellationsValue,
            placeholder="Select one or more",
        )

        # Receiver/Antenna types: free-text input
        self._enable_free_text_for_receiver_and_antenna()

        # Antenna offset
        self.ui.antennaOffsetButton.clicked.connect(self._open_antenna_offset_dialog)
        self.ui.antennaOffsetButton.setCursor(Qt.CursorShape.PointingHandCursor)
        self.ui.antennaOffsetValue.setText("0.0, 0.0, 0.0")

        # Time window and data interval
        self.ui.timeWindowButton.clicked.connect(self._open_time_window_dialog)
        self.ui.timeWindowButton.setCursor(Qt.CursorShape.PointingHandCursor)
        self.ui.dataIntervalButton.clicked.connect(self._open_data_interval_dialog)
        self.ui.dataIntervalButton.setCursor(Qt.CursorShape.PointingHandCursor)

        # Run buttons
        self.ui.showConfigButton.clicked.connect(self.on_show_config)
        self.ui.showConfigButton.setCursor(Qt.CursorShape.PointingHandCursor)
        self.ui.processButton.clicked.connect(self.on_run_pea)

        # CDDIS credentials dialog
        self.ui.cddisCredentialsButton.clicked.connect(self._open_cddis_credentials_dialog)


    def _open_cddis_credentials_dialog(self):
        """ Open the CDDIS Credential Input Dialog Box """
        dialog = CredentialsDialog(self.parent)
        dialog.exec()

    #region File Selection + Metadata Extraction + PPP product selection
    def load_rnx_file(self) -> ExtractedInputs | None:
        """Pick an RNX file, extract metadata, apply to UI, and enable next steps."""

        path = self._select_rnx_file(self.parent)
        if not path:
            return None

        current_rinex_path = Path(path).resolve()
        archive_products_if_rinex_changed(
            current_rinex=current_rinex_path,
            last_rinex=getattr(self, "last_rinex_path", None),
            products_dir=INPUT_PRODUCTS_PATH
        )
        self.last_rinex_path = current_rinex_path
        self.rnx_file = str(current_rinex_path)

        self.ui.terminalTextEdit.append(f"📄 RINEX file selected: {self.rnx_file}")

        try:
            extractor = RinexExtractor(self.rnx_file)
            result = extractor.extract_rinex_data(self.rnx_file)

            self.ui.terminalTextEdit.append("🔍 Scanning CDDIS archive for PPP products. Please wait...")

            # Synchronous CDDIS Products list downloader (for testing only)
            #try:
            #    self.ui.terminalTextEdit.append("⚠️ Running synchronous CDDIS test...")
            #    self.cddis_handler = CDDIS_Handler(result['start_epoch'], result['end_epoch'])
            #    self.valid_analysis_centers = self.cddis_handler.get_list_of_valid_analysis_centers()
            #    print("[SYNC TEST] Centers:", self.valid_analysis_centers)
            #except Exception as e:
            #    print("[SYNC TEST ERROR]", e)

            # Kick off CDDIS PPP products query in background thread
            worker = CDDISWorker(str_to_datetime(result['start_epoch']), str_to_datetime(result['end_epoch']))
            worker.signals.finished.connect(self._on_cddis_ready)
            worker.signals.error.connect(self._on_cddis_error)
            QThreadPool.globalInstance().start(worker)

            # Populate extracted metadata immediately
            self.ui.constellationsValue.setText(result["constellations"])
            self.ui.timeWindowValue.setText(f"{result['start_epoch']} to {result['end_epoch']}")
            self.ui.timeWindowButton.setText(f"{result['start_epoch']} to {result['end_epoch']}")
            self.ui.dataIntervalButton.setText(f"{result['epoch_interval']} s")
            self.ui.receiverTypeValue.setText(result["receiver_type"])
            self.ui.antennaTypeValue.setText(result["antenna_type"])
            self.ui.antennaOffsetValue.setText(", ".join(map(str, result["antenna_offset"])))
            self.ui.antennaOffsetButton.setText(", ".join(map(str, result["antenna_offset"])))

            self.ui.Receiver_type.clear()
            self.ui.Receiver_type.addItem(result["receiver_type"])
            self.ui.Receiver_type.setCurrentIndex(0)
            self.ui.Receiver_type.lineEdit().setText(result["receiver_type"])

            self.ui.Antenna_type.clear()
            self.ui.Antenna_type.addItem(result["antenna_type"])
            self.ui.Antenna_type.setCurrentIndex(0)
            self.ui.Antenna_type.lineEdit().setText(result["antenna_type"])

            self._update_constellations_multiselect(result["constellations"])

            self.ui.outputButton.setEnabled(True)
            self.ui.showConfigButton.setEnabled(True)

            self.ui.terminalTextEdit.append("⚒️ RINEX file metadata extracted and applied to UI fields")
            self.ui.outputButton.setEnabled(True)
            self.ui.showConfigButton.setEnabled(True)

        except Exception as e:
            self.ui.terminalTextEdit.append(f"Error extracting RNX metadata: {e}")
            print(f"[Error] RNX metadata extraction failed: {e}")
            return None

        # Always update MainWindow's state
        self.parent.rnx_file = self.rnx_file
        print(f"[DEBUG InputCtrl] load_rnx_file set parent.rnx_file={self.parent.rnx_file}")

        if self.output_dir:
            print(f"[DEBUG InputCtrl] Emitting ready with rnx={self.rnx_file}, out={self.output_dir}")
            self.ready.emit(str(self.rnx_file), str(self.output_dir))

        return result

    def _update_constellations_multiselect(self, constellation_str: str):
        """
        Populate the multi-select combo for constellations using checkboxes.
        """
        from PySide6.QtGui import QStandardItemModel, QStandardItem

        constellations = [c.strip() for c in constellation_str.split(",") if c.strip()]
        combo = self.ui.Constellations_2

        # Remove previous bindings
        if hasattr(combo, '_old_showPopup'):
            delattr(combo, '_old_showPopup')

        combo.clear()
        combo.setEditable(True)
        combo.lineEdit().setReadOnly(True)
        combo.setInsertPolicy(QComboBox.NoInsert)

        # Build the item model
        model = QStandardItemModel(combo)
        for txt in constellations:
            item = QStandardItem(txt)
            item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked)
            model.appendRow(item)

        def on_item_changed(_item):
            selected = [
                model.item(i).text()
                for i in range(model.rowCount())
                if model.item(i).checkState() == Qt.Checked
            ]
            label = ", ".join(selected) if selected else "Select one or more"
            combo.lineEdit().setText(label)
            self.ui.constellationsValue.setText(label)

        model.itemChanged.connect(on_item_changed)
        combo.setModel(model)
        combo.setCurrentIndex(-1)

        # Custom showPopup function to keep things reset
        def show_popup_constellation():
            if combo.model() != model:
                combo.setModel(model)
            combo.setCurrentIndex(-1)
            QComboBox.showPopup(combo)

        combo.showPopup = show_popup_constellation

        # Store for access and event consistency
        combo._constellation_model = model
        combo._constellation_on_item_changed = on_item_changed

        # Set initial label text
        combo.lineEdit().setText(", ".join(constellations))
        self.ui.constellationsValue.setText(", ".join(constellations))

    def _on_cddis_ready(self, data: pd.DataFrame):
        self.products_df = data
        self.valid_analysis_centers = list(get_valid_analysis_centers(self.products_df))

        if len(self.valid_analysis_centers) == 0:
            self.ui.terminalTextEdit.append("⚠️ No valid PPP providers found.")
            self.ui.PPP_provider.clear()
            self.ui.PPP_provider.addItem("None")
            self.ui.PPP_series.clear()
            self.ui.PPP_series.addItem("None")
            return

        self.ui.PPP_provider.blockSignals(True)
        self.ui.PPP_provider.clear()
        self.ui.PPP_provider.addItems(self.valid_analysis_centers)
        self.ui.PPP_provider.setCurrentIndex(0)
        self.ui.PPP_provider.blockSignals(False)

        # Update PPP_series based on default PPP_provider
        self._on_ppp_provider_changed(self.valid_analysis_centers[0])
        self.ui.terminalTextEdit.append(f"✅ CDDIS archive scan complete. Found PPP product providers: {', '.join(self.valid_analysis_centers)}")

    def _on_cddis_error(self, msg):
        self.ui.terminalTextEdit.append(f"Error loading CDDIS data: {msg}")
        self.ui.PPP_provider.clear()
        self.ui.PPP_provider.addItem("None")

    def _on_ppp_provider_changed(self, provider_name: str):
        if not provider_name or provider_name.strip() == "":
            print("[Warning] No PPP provider selected — isgnoring update.")
            return
        try:
            # Get DataFrame of valid (project, series) pairs
            df = self.products_df.loc[self.products_df["analysis_center"] == provider_name, ["project", "solution_type"]]

            if df.empty:
                raise ValueError(f"No valid project–series combinations for provider: {provider_name}")

            # Store for future filtering if needed
            self._valid_project_series_df = df

            project_options = sorted(df['project'].unique())
            series_options = sorted(df['solution_type'].unique())

            self.ui.PPP_project.clear()
            self.ui.PPP_series.clear()

            self.ui.PPP_project.addItems(project_options)
            self.ui.PPP_series.addItems(series_options)

            print(f"[CDDIS] Updated PPP_project for {provider_name}: {project_options}")
            print(f"[CDDIS] Updated PPP_series for {provider_name}: {series_options}")

            # Optionally set default selections here:
            self.ui.PPP_project.setCurrentIndex(0)
            self.ui.PPP_series.setCurrentIndex(0)

        except Exception as e:
            print(f"[Error] Failed to update PPP options for provider {provider_name}: {e}")
            self.ui.PPP_series.clear()
            self.ui.PPP_series.addItem("None")
            self.ui.PPP_project.clear()
            self.ui.PPP_project.addItem("None")

    def _on_ppp_series_changed(self, selected_series: str):
        if not hasattr(self, "_valid_project_series_df"):
            return

        df = self._valid_project_series_df
        filtered_df = df[df["solution_type"] == selected_series]
        valid_projects = sorted(filtered_df["project"].unique())

        self.ui.PPP_project.blockSignals(True)
        self.ui.PPP_project.clear()
        self.ui.PPP_project.addItems(valid_projects)
        self.ui.PPP_project.setCurrentIndex(0)
        self.ui.PPP_project.blockSignals(False)

        print(f"[UI] Filtered PPP_project for series '{selected_series}': {valid_projects}")
        
    def _on_ppp_project_changed(self, selected_project: str):
        if not hasattr(self, "_valid_project_series_df"):
            return

        df = self._valid_project_series_df
        filtered_df = df[df["project"] == selected_project]
        valid_series = sorted(filtered_df["solution_type"].unique())

        self.ui.PPP_series.blockSignals(True)
        self.ui.PPP_series.clear()
        self.ui.PPP_series.addItems(valid_series)
        self.ui.PPP_series.setCurrentIndex(0)
        self.ui.PPP_series.blockSignals(False)

        print(f"[UI] Filtered PPP_series for project '{selected_project}': {valid_series}")

    def load_output_dir(self):
        """Pick an output directory; if RNX is also set, emit ready."""
        path = self._select_output_dir(self.parent)
        if not path:
            return

        # Ensure output_dir is a Path object
        self.output_dir = Path(path).resolve()
        self.ui.terminalTextEdit.append(f"📂 Output directory selected: {self.output_dir}")

        # Archive existing/old outputs
        visual_dir = self.output_dir / "visual"
        archive_old_outputs(self.output_dir, visual_dir)

        # Enable process button
        # MainWindow owns when to enable processButton. This controller exposes a helper if needed.
        self.enable_process_button()

        # Always update MainWindow's state
        self.parent.output_dir = self.output_dir
        print(f"[DEBUG InputCtrl] load_output_dir set parent.output_dir={self.parent.output_dir}")

        if self.rnx_file:
            print(f"[DEBUG InputCtrl] Emitting ready with rnx={self.rnx_file}, out={self.output_dir}")
            self.ready.emit(str(self.rnx_file), str(self.output_dir))

    def enable_process_button(self):
        """Public helper so other components can enable the Process button without knowing UI internals."""
        self.ui.processButton.setEnabled(True)

    #endregion

    #region Multi-Selectors Assigning (A.K.A. Combo Plumbing)

    def _on_select(self, combo: QComboBox, label, title: str, index: int):
        """Mirror combo selection to label and reset combo's placeholder text."""
        value = combo.itemText(index)
        label.setText(value)

        combo.clear()
        combo.addItem(title)

    def _bind_combo(self, combo: QComboBox, items_func: Callable[[], List[str]]):
        """
        Populate a single-choice QComboBox each time it opens.
        Keeps the left combo visually clean while moving the chosen value to the right label.
        """
        combo._old_showPopup = combo.showPopup

        def new_showPopup():
            combo.clear()
            combo.setEditable(True)
            combo.lineEdit().setAlignment(Qt.AlignCenter)
            for item in items_func():
                combo.addItem(item)
            combo.setEditable(False)
            combo._old_showPopup()

        combo.showPopup = new_showPopup

    def _bind_multiselect_combo(
            self,
            combo: QComboBox,
            items_func: Callable[[], List[str]],
            mirror_label,
            placeholder: str,
    ):
        """
        On open, replace the combo's model with checkbox items and mirror all checked items
        as comma-separated text to mirror_label.
        """
        combo.setEditable(True)
        combo.lineEdit().setReadOnly(True)
        combo.lineEdit().setPlaceholderText(placeholder)
        combo.setInsertPolicy(QComboBox.NoInsert)

        combo._old_showPopup = combo.showPopup

        def show_popup():
            model = QStandardItemModel(combo)
            for txt in items_func():
                it = QStandardItem(txt)
                it.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable)
                it.setData(Qt.Unchecked, Qt.CheckStateRole)
                model.appendRow(it)

            def on_item_changed(_item: QStandardItem):
                # Collect all checked items
                selected = [
                    model.item(r).text()
                    for r in range(model.rowCount())
                    if model.item(r).checkState() == Qt.Checked
                ]
                text = ", ".join(selected) if selected else placeholder
                combo.lineEdit().setText(text)
                mirror_label.setText(text)

            model.itemChanged.connect(on_item_changed)
            combo.setModel(model)
            combo._old_showPopup()

        combo.showPopup = show_popup
        combo.clear()
        combo.lineEdit().clear()
        combo.lineEdit().setPlaceholderText(placeholder)        

    # ==========================================================
    # Receiver / Antenna free text popups
    # ==========================================================
    def _enable_free_text_for_receiver_and_antenna(self):
        """Allow entering custom Receiver/Antenna types via popup prompt."""
        self.ui.Receiver_type.setEditable(True)
        self.ui.Receiver_type.lineEdit().setReadOnly(True)
        self.ui.Antenna_type.setEditable(True)
        self.ui.Antenna_type.lineEdit().setReadOnly(True)

        # Receiver type free text
        def _ask_receiver_type():
            current_text = self.ui.Receiver_type.currentText().strip()
            text, ok = QInputDialog.getText(
                self.ui.Receiver_type,
                "Receiver Type",
                "Enter receiver type:",
                text=current_text  # prefill with current
            )
            if ok and text:
                self.ui.Receiver_type.clear()
                self.ui.Receiver_type.addItem(text)
                self.ui.Receiver_type.lineEdit().setText(text)
                self.ui.receiverTypeValue.setText(text)

        self.ui.Receiver_type.showPopup = _ask_receiver_type

        # Antenna type free text
        def _ask_antenna_type():
            current_text = self.ui.Antenna_type.currentText().strip()
            text, ok = QInputDialog.getText(
                self.ui.Antenna_type,
                "Antenna Type",
                "Enter antenna type:",
                text=current_text  # prefill with current
            )
            if ok and text:
                self.ui.Antenna_type.clear()
                self.ui.Antenna_type.addItem(text)
                self.ui.Antenna_type.lineEdit().setText(text)
                self.ui.antennaTypeValue.setText(text)

        self.ui.Antenna_type.showPopup = _ask_antenna_type


    # ==========================================================
    # Antenna offset popup
    # ==========================================================
    def _open_antenna_offset_dialog(self):
        dlg = QDialog(self.ui.antennaOffsetButton)
        dlg.setWindowTitle("Antenna Offset")

        # Parse existing "E, N, U"
        try:
            e0, n0, u0 = [float(x.strip()) for x in self.ui.antennaOffsetValue.text().split(",")]
        except Exception:
            e0 = n0 = u0 = 0.0 

        form = QFormLayout(dlg)
        
        class DecimalSpinBox(QDoubleSpinBox):
            def __init__(self, parent=None, top=1e100, bottom=-1e100, precision = 15,step_size = 0.1):
                super().__init__(parent)
                self.setRange(bottom, top)
                
                self.setDecimals(precision)  # fallback precision
                # up down arrow Step size
                # note there is some float point inaccuracy when useing steps
                self.setSingleStep(step_size)

            def textFromValue(self, value: float) -> str:
                """Format value dynamically with Decimal for more precision"""
                # Convert through Decimal to avoid scientific notation
                d = Decimal(str(value))
                return str(d.normalize())  # trims trailing zeros

            def valueFromText(self, text: str) -> float:
                """Parse text back into a float"""
                try:
                    return float(Decimal(text))
                except InvalidOperation:
                    raise ValueError(f"Failed to convert Antenna offset to float: {text}")
       
       
        sb_e = DecimalSpinBox(dlg)
        sb_e.setValue(e0)
        
        sb_n = DecimalSpinBox(dlg)
        sb_n.setValue(n0)
            
        sb_u = DecimalSpinBox(dlg)
        sb_u.setValue(u0)

        form.addRow("E:", sb_e)
        form.addRow("N:", sb_n)
        form.addRow("U:", sb_u)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("OK", dlg)
        cancel_btn = QPushButton("Cancel", dlg)
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        form.addRow(btn_row)

        ok_btn.clicked.connect(lambda: self._set_antenna_offset(sb_e, sb_n, sb_u, dlg))
        cancel_btn.clicked.connect(dlg.reject)

        dlg.exec()

    def _set_antenna_offset(self, sb_e, sb_n, sb_u, dlg: QDialog):
        e, n, u = sb_e.value(), sb_n.value(), sb_u.value()
        text = f"{e}, {n}, {u}"
        self.ui.antennaOffsetButton.setText(text)
        self.ui.antennaOffsetValue.setText(text)
        dlg.accept()


    # ==========================================================
    # Time window popup
    # ==========================================================
    def _open_time_window_dialog(self):
        dlg = QDialog(self.ui.timeWindowValue)
        dlg.setWindowTitle("Select start / end time")

        # Parse existing "yyyy-MM-dd_HH:mm:ss to yyyy-MM-dd_HH:mm:ss"
        current_text = self.ui.timeWindowButton.text()
        try:
            s_text, e_text = current_text.split(" to ")
            s_dt = QDateTime.fromString(s_text, "yyyy-MM-dd_HH:mm:ss")
            e_dt = QDateTime.fromString(e_text, "yyyy-MM-dd_HH:mm:ss")
            if not s_dt.isValid():
                s_dt = QDateTime.fromString(s_text, "yyyy-MM-dd HH:mm:ss")
            if not e_dt.isValid():
                e_dt = QDateTime.fromString(e_text, "yyyy-MM-dd HH:mm:ss")
        except Exception:
            s_dt = e_dt = QDateTime.currentDateTime()

        vbox = QVBoxLayout(dlg)
        start_edit = QDateTimeEdit(s_dt, dlg)
        end_edit = QDateTimeEdit(e_dt, dlg)

        start_edit.setCalendarPopup(True)
        end_edit.setCalendarPopup(True)
        start_edit.setDisplayFormat("yyyy-MM-dd_HH:mm:ss")
        end_edit.setDisplayFormat("yyyy-MM-dd_HH:mm:ss")

        vbox.addWidget(start_edit)
        vbox.addWidget(end_edit)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("OK", dlg)
        cancel_btn = QPushButton("Cancel", dlg)
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        vbox.addLayout(btn_row)

        ok_btn.clicked.connect(lambda: self._set_time_window(start_edit, end_edit, dlg))
        cancel_btn.clicked.connect(dlg.reject)

        dlg.exec()

    def _set_time_window(self, start_edit, end_edit, dlg: QDialog):
        if end_edit.dateTime() < start_edit.dateTime():
            QMessageBox.warning(dlg, "Time error",
                                "End time cannot be earlier than start time.\nPlease select again.")
            return

        s = start_edit.dateTime().toString("yyyy-MM-dd_HH:mm:ss")
        e = end_edit.dateTime().toString("yyyy-MM-dd_HH:mm:ss")
        self.ui.timeWindowButton.setText(f"{s} to {e}")
        self.ui.timeWindowValue.setText(f"{s} to {e}")
        dlg.accept()


    # ==========================================================
    # Data interval popup
    # ==========================================================
    def _open_data_interval_dialog(self):
        # Extract current value from button text ("30 s" → 30)
        current_text = self.ui.dataIntervalButton.text().replace(" s", "").strip()
        try:
            current_val = int(current_text)
        except ValueError:
            current_val = 1  # fallback if parsing fails

        val, ok = QInputDialog.getInt(
            self.ui.dataIntervalButton,
            "Data interval",
            "Input interval (seconds):",
            current_val,   # prefill with current value
            1,
            999_999,
        )
        if ok:
            text = f"{val} s"
            self.ui.dataIntervalButton.setText(text)
            self.ui.dataIntervalValue.setText(text)

    #endregion

    #region Config and PEA Processing

    def _generate_modified_config_yaml(self, config_parameters):
        """
        Args:
            config_parameters (dict): modified config parameters directory
                example: {
                    'setting1': 'value1',
                    'setting2': 'value2',
                    'nested_config': {
                        'subsetting1': 'subvalue1'
                    }
                }
        
        Returns:
            str: generated YAML file path, should return the path in the format of /resources/Yaml/xxxx.yaml
        
        TODO: backend please implement the following functions:
        1. receive config_parameters parameter
        2. convert the parameters to YAML format
        3. save to /resources/Yaml/ directory
        4. file name format can be: timestamp.yaml, config_v1.yaml, etc.
        5. return the complete file path
        
        Note: the current UI version uses the hardcode path /resources/Yaml/default_config.yaml
        """
        # TODO: backend please implement functions here.
        return self.default_config_path

    def extract_ui_values(self, rnx_path):
        # Extract user input from the UI and assign it to class variables.
        mode_raw           = self.ui.Mode.currentText() if self.ui.Mode.currentText() != "Select one" else "Static"
        
        # Get constellations from the actual dropdown selections, not the label
        constellations_raw = ""
        combo = self.ui.Constellations_2
        if hasattr(combo, '_constellation_model') and combo._constellation_model:
            model = combo._constellation_model
            selected = [model.item(i).text() for i in range(model.rowCount()) if model.item(i).checkState() == Qt.Checked]
            constellations_raw = ", ".join(selected)
        else:
            # Fallback to the label text if no custom model exists
            constellations_raw = self.ui.constellationsValue.text()
        print("*****", constellations_raw)
        time_window_raw    = self.ui.timeWindowValue.text()  # Get from button, not value label
        epoch_interval_raw = self.ui.dataIntervalButton.text()  # Get from button, not value label
        receiver_type      = self.ui.receiverTypeValue.text()
        antenna_type       = self.ui.antennaTypeValue.text()
        antenna_offset_raw = self.ui.antennaOffsetButton.text()  # Get from button, not value label
        ppp_provider       = self.ui.PPP_provider.currentText() if self.ui.PPP_provider.currentText() != "Select one" else ""
        ppp_series         = self.ui.PPP_series.currentText()   if self.ui.PPP_series.currentText()   != "Select one" else ""
        ppp_project        = self.ui.PPP_project.currentText()  if self.ui.PPP_project.currentText()  != "Select one" else ""

        # Parsed values
        start_epoch, end_epoch = self.parse_time_window(time_window_raw)
        antenna_offset         = self.parse_antenna_offset(antenna_offset_raw)
        epoch_interval         = int(epoch_interval_raw.replace("s", "").strip())
        marker_name            = self.extract_marker_name(rnx_path)
        mode                   = self.determine_mode_value(mode_raw)

        # Print verification
        print("InputExtractController Extraction Completed：")
        print("mode =", mode)
        print("constellation =", constellations_raw)
        print("start_epoch =", start_epoch)
        print("end_epoch =", end_epoch)
        print("epoch_interval =", epoch_interval)
        print("receiver_type =", receiver_type)
        print("antenna_type =", antenna_type)
        print("antenna_offset =", antenna_offset)
        print("PPP_provider =", ppp_provider)
        print("PPP_series =", ppp_series)
        print("PPP_project =", ppp_project)
        print("marker = ", marker_name)

        # Returned the values found as a dataclass for easier access
        return self.ExtractedInputs(
            marker_name=marker_name,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            epoch_interval=epoch_interval,
            antenna_offset=antenna_offset,
            mode=mode,
            constellations_raw=constellations_raw,
            receiver_type=receiver_type,
            antenna_type=antenna_type,
            ppp_provider=ppp_provider,
            ppp_series=ppp_series,
            ppp_project=ppp_project,
            rnx_path=rnx_path,
            output_path=self.output_dir,
        )

    def on_show_config(self):
        """
        Show config file
        Open the fixed path YAML config file: /resources/Yaml/default_config.yaml
        No longer need to manually select files
        """
        print("opening config file...")
        print("[DEBUG] on_show_config: rnx_file =", self.rnx_file)
        # Reload disk version before overwriting with GUI changes
        self.execution.reload_config()
        inputs = self.extract_ui_values(self.rnx_file)
        self.execution.apply_ui_config(inputs)
        self.execution.write_cached_changes()

        # Execution class will throw error when instantiated if the file doesn't exist and it can't create it
        # This code is run after Execution class is instantiated within this file, thus never will occur
        if not os.path.exists(GENERATED_YAML):
            QMessageBox.warning(
                None,
                "File not found",
                f"The file {GENERATED_YAML} does not exist."
            )
            return

        self.on_open_config_in_editor(self.config_path)

    def on_open_config_in_editor(self, file_path):
        """
        Open the config file in an external editor
        
        Args:
            file_path (str): the complete path of the YAML config file
        """
        import subprocess
        import platform
        
        try:
            abs_path = os.path.abspath(file_path)
            
            # Open the file with the appropriate method for the operating system
            if platform.system() == "Windows":
                os.startfile(abs_path)
                print("Opened with default Windows application")
                
            elif platform.system() == "Darwin":  # macOS
                subprocess.run(["open", abs_path])
                print("Opened with default macOS application")
                
            else:  # Linux and other Unix-like systems
                subprocess.run(["xdg-open", abs_path])
                print("Opened with default Linux application")
                
        except Exception as e:
            error_message = f"Cannot open config file:\n{file_path}\n\nError: {str(e)}"
            print(f"Error: {error_message}")
            QMessageBox.critical(
                None,
                "Error Opening File",
                error_message
            )

    def on_run_pea(self):
        """Triggered when 'Process' is clicked: validates input, parses time window,
        applies config, then signals MainWindow to continue with PPP downloads + PEA execution.
        """
        raw = self.ui.timeWindowValue.text()
        print(f"[UI] Time window raw input: {raw}")
        print("[DEBUG] on_run_pea: rnx_file =", self.rnx_file)

        # --- Parse time window ---
        try:
            start_str, end_str = raw.split("to")
            start_time = datetime.strptime(start_str.strip(), "%Y-%m-%d_%H:%M:%S")
            end_time = datetime.strptime(end_str.strip(), "%Y-%m-%d_%H:%M:%S")
        except ValueError:
            QMessageBox.warning(
                None,
                "Format error",
                "Time window must be in the format:\n"
                "YYYY-MM-DD_HH:MM:SS to YYYY-MM-DD_HH:MM:SS"
            )
            return

        if start_time > end_time:
            QMessageBox.warning(None, "Time error", "Start time cannot be later than end time.")
            return

        if not getattr(self, "config_path", None):
            QMessageBox.warning(
                None,
                "No config file",
                "Please click Show config and select a YAML file first."
            )
            return

        # Store time window so MainWindow can use it later
        self.start_time = start_time
        self.end_time = end_time

        # --- Write updated config ---
        try:
            self.execution.reload_config()
            inputs = self.extract_ui_values(self.rnx_file)
            self.execution.apply_ui_config(inputs)  # config only, no product archiving here
            self.execution.write_cached_changes()
        except Exception as e:
            self.ui.terminalTextEdit.append(f"⚠️ Failed to apply config: {e}")
            return

        # --- Emit signal for MainWindow ---
        self.pea_ready.emit()

    #endregion

    #region Utility Functions

    @staticmethod
    def _set_combobox_by_value(combo: QComboBox, value: str):
        """Find 'value' in a combo and set it if present."""
        if value is None:
            return
        idx = combo.findText(value)
        if idx != -1:
            combo.setCurrentIndex(idx)

    @staticmethod
    def _select_rnx_file(parent) -> str:
        """Select RINEX file using file dialog"""
        path, _ = QFileDialog.getOpenFileName(
            parent, 
            "Select RINEX Observation File", 
            "", 
            "RINEX Observation Files (*.rnx *.rnx.gz);;All Files (*.*)"
        )
        return path or ""

    @staticmethod
    def _select_output_dir(parent) -> str:
        """Select output directory using file dialog"""
        path = QFileDialog.getExistingDirectory(parent, "Select Output Directory")
        return path or ""

    @staticmethod
    def determine_mode_value(mode_raw: str) -> int:
        if mode_raw == "Static":
            return 0
        elif mode_raw == "Kinematic":
            return 30
        elif mode_raw == "Dynamic":
            return 100
        else:
            raise ValueError(f"Unknown mode: {mode_raw!r}")

    @staticmethod
    def extract_marker_name(rnx_path: str) -> str:
        """
        Extracts the 4-char site code from the RNX file name.
        Falls back to "TEST" if one cannot be found.
        E.g.: ALIC00AUS_R_20250190000_01D_30S_MO.rnx.gz -> ALBY
        """
        if not rnx_path:
            return "TEST"
        stem = Path(rnx_path).stem  # drops .gz/.rnx
        m = re.match(r"([A-Za-z]{4})", stem)
        return m.group(1).upper() if m else "TEST"

    @staticmethod
    def parse_time_window(time_window_raw: str):
        """Convert 'start_time to end_time' into (start_epoch, end_epoch)."""
        try:
            start, end = map(str.strip, time_window_raw.split("to"))

            # Replace underscores with spaces in datetime strings
            start = start.replace("_", " ")
            end = end.replace("_", " ")
            return start, end
        except ValueError:
            raise ValueError("Invalid time_window format. Expected: 'start_time to end_time'")

    @staticmethod
    def parse_antenna_offset(antenna_offset_raw: str):
        """Convert 'e, n, u' into [e, n, u] floats."""
        try:
            e, n, u = map(str.strip, antenna_offset_raw.split(","))
            return [float(e), float(n), float(u)]
        except ValueError:
            raise ValueError("Invalid antenna offset format. Expected: 'e, n, u'")

    @dataclass
    class ExtractedInputs:
        # Parsed / derived values
        marker_name: str
        start_epoch: str
        end_epoch: str
        epoch_interval: int
        antenna_offset: list[float]
        mode: int

        # Raw strings / controls that are needed downstream
        constellations_raw: str
        receiver_type: str
        antenna_type: str
        ppp_provider: str
        ppp_series: str
        ppp_project: str

        # File paths associated to this run
        rnx_path: str
        output_path: str

    #endregion

    #region Statics

    @staticmethod
    def _get_mode_items() -> List[str]:
        return ["Static", "Kinematic", "Dynamic"]

    @staticmethod
    def _get_constellations_items() -> List[str]:
        return ["GPS", "GAL", "GLO", "BDS", "QZS"]

    def _get_ppp_provider_items(self) -> List[str]:
        if hasattr(self, "valid_analysis_centers") and self.valid_analysis_centers:
            return self.valid_analysis_centers
        return ["None (select RNX file first)"]

    @staticmethod
    def _get_ppp_series_items() -> List[str]:
        return ["RAP", "ULT", "FIN"]

    #endregion


class CredentialsDialog(QDialog):
    """ Credentials, pop-up window """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("CDDIS Credentials")

        layout = QVBoxLayout()

        # Username
        layout.addWidget(QLabel("Username:"))
        self.username_input = QLineEdit()
        layout.addWidget(self.username_input)

        # Password
        layout.addWidget(QLabel("Password:"))
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.Password)
        layout.addWidget(self.password_input)

        # Confirm button
        self.confirm_button = QPushButton("Save")
        self.confirm_button.clicked.connect(self.save_credentials)
        layout.addWidget(self.confirm_button)

        self.setLayout(layout)

    def save_credentials(self):
        username = self.username_input.text().strip()
        password = self.password_input.text().strip()

        if not username or not password:
            QMessageBox.warning(self, "Error", "Username and password cannot be empty")
            return

        # ✅ Save correctly in one go (Windows will write both %USERPROFILE%\\.netrc and %USERPROFILE%\\_netrc;
        #    macOS/Linux will write ~/.netrc and automatically chmod 600; both URS and CDDIS entries are written)
        try:
            paths = save_earthdata_credentials(username, password)
        except Exception as e:
            QMessageBox.critical(self, "Save failed", f"❌ Failed to save credentials:\n{e}")
            return

        QMessageBox.information(self, "Success",
                                "✅ Credentials saved to:\n" + "\n".join(str(p) for p in paths))
        self.accept()



