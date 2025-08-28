# app/controllers/input_controller.py

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, List
from app.utils.download_products import download_ppp_products

from PySide6.QtCore import QObject, Signal, Qt, QDateTime
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

from app.models.execution import Execution, GENERATED_YAML, TEMPLATE_PATH
from app.models.rinex_extractor import RinexExtractor
from app.utils.cddis_credentials import save_earthdata_credentials

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
        
        # Config file path
        self.config_path = GENERATED_YAML

        ### Wire: file selection buttons ###
        self.ui.observationsButton.clicked.connect(self.load_rnx_file)
        self.ui.outputButton.clicked.connect(self.load_output_dir)

        # Initial states
        self.ui.outputButton.setEnabled(False) # output disabled until RNX chosen
        self.ui.showConfigButton.setEnabled(False)  # show config disabled until RNX chosen
        self.ui.processButton.setEnabled(False) # process enabled later by MainWindow

        ### Bind: configuration drop-downs / UIs ###

        # Single-choice combos (populated on open)
        self._bind_combo(self.ui.Mode, self._get_mode_items)
        self._bind_combo(self.ui.PPP_provider, self._get_ppp_provider_items)
        self._bind_combo(self.ui.PPP_series, self._get_ppp_series_items)

        # Constellations: multi-select with checkboxes, mirror to constellationsValue label
        self._bind_multiselect_combo(
            self.ui.Constellations_2,
            self._get_constellations_items,
            self.ui.constellationsValue,
            placeholder="Select one or more",
        )


        # Receiver/Antenna types: allow free-text via popup prompts
        self._enable_free_text_for_receiver_and_antenna()

        # Antenna offset dialog
        self.ui.antennaOffsetButton.clicked.connect(self._open_antenna_offset_dialog)
        self.ui.antennaOffsetButton.setCursor(Qt.CursorShape.PointingHandCursor)
        self.ui.antennaOffsetValue.setText("0.0, 0.0, 0.0")

        # Time window & Data interval dialogs
        self.ui.timeWindowButton.clicked.connect(self._open_time_window_dialog)
        self.ui.timeWindowButton.setCursor(Qt.CursorShape.PointingHandCursor)

        self.ui.dataIntervalButton.clicked.connect(self._open_data_interval_dialog)
        self.ui.dataIntervalButton.setCursor(Qt.CursorShape.PointingHandCursor)

        # Show config and Run PEA buttons
        self.ui.showConfigButton.clicked.connect(self.on_show_config)
        self.ui.showConfigButton.setCursor(Qt.CursorShape.PointingHandCursor)
        self.ui.processButton.clicked.connect(self.on_run_pea)

        # CDDIS Credentials button
        self.ui.cddisCredentialsButton.clicked.connect(self._open_cddis_credentials_dialog)



    def _open_cddis_credentials_dialog(self):
        """ Open the CDDIS Credential Input Dialog Box """
        dialog = CredentialsDialog(self.parent)
        dialog.exec()

    #region File Selection + Metadata Extraction

    def load_rnx_file(self) -> ExtractedInputs or None:
        """Pick an RNX file, extract metadata, apply to UI, and enable next steps."""
        path = self._select_rnx_file(self.parent)
        if not path:
            return None

        self.rnx_file = path
        self.ui.terminalTextEdit.append(f"RNX selected: {path}")


        # Extract information from submitted .RNX file and reflect it in the UI
        try:
            extractor = RinexExtractor(path)
            result = extractor.extract_rinex_data(path)

            # Update UI fields directly
            self.ui.constellationsValue.setText(result["constellations"])
            self.ui.timeWindowValue.setText(f"{result['start_epoch']} to {result['end_epoch']}")
            self.ui.timeWindowButton.setText(f"{result['start_epoch']} to {result['end_epoch']}")
            self.ui.dataIntervalButton.setText(f"{result['epoch_interval']} s")
            self.ui.receiverTypeValue.setText(result["receiver_type"])
            self.ui.antennaTypeValue.setText(result["antenna_type"])
            self.ui.antennaOffsetButton.setText(", ".join(map(str, result["antenna_offset"])))

            # Set left-side combos to extracted values
            # Receiver type
            self.ui.Receiver_type.clear()
            self.ui.Receiver_type.addItem(result["receiver_type"])
            self.ui.Receiver_type.setCurrentIndex(0)
            self.ui.Receiver_type.lineEdit().setText(result["receiver_type"])

            # Antenna type
            self.ui.Antenna_type.clear()
            self.ui.Antenna_type.addItem(result["antenna_type"])
            self.ui.Antenna_type.setCurrentIndex(0)
            self.ui.Antenna_type.lineEdit().setText(result["antenna_type"])

            # Constellations (multi-select combo)
            # Completely replace the constellation dropdown behavior with file-specific constellations
            constellations = [c.strip() for c in result["constellations"].split(",") if c.strip()]
            combo = self.ui.Constellations_2
            
            # Clear any existing bindings and reset combo
            if hasattr(combo, '_old_showPopup'):
                delattr(combo, '_old_showPopup')
            combo.clear()
            combo.setEditable(True)
            combo.lineEdit().setReadOnly(True)
            combo.setInsertPolicy(QComboBox.NoInsert)
            
            from PySide6.QtGui import QStandardItemModel, QStandardItem
            model = QStandardItemModel(combo)
            for txt in constellations:
                it = QStandardItem(txt)
                it.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable)
                it.setCheckState(Qt.Checked)
                model.appendRow(it)
            
            def on_item_changed(_item):
                current_model = combo.model()
                if current_model:
                    selected = [current_model.item(i).text() for i in range(current_model.rowCount()) if current_model.item(i).checkState() == Qt.Checked]
                    text = ", ".join(selected) if selected else "Select one or more"
                    combo.lineEdit().setText(text)
                    self.ui.constellationsValue.setText(text)
            
            model.itemChanged.connect(on_item_changed)
            combo.setModel(model)
            
            # Set current index to -1 to avoid the first item being "selected"
            combo.setCurrentIndex(-1)
            
            # Store references and override showPopup completely
            combo._constellation_model = model
            combo._constellation_on_item_changed = on_item_changed
            
            def show_popup_constellation():
                # Ensure model is still connected and items are checked
                if combo.model() != combo._constellation_model:
                    combo.setModel(combo._constellation_model)
                # Make sure no item is currently selected
                combo.setCurrentIndex(-1)
                # Call the original showPopup without any custom logic
                QComboBox.showPopup(combo)
            
            combo.showPopup = show_popup_constellation
            
            # Set initial text
            combo.lineEdit().setText(", ".join(constellations))
            self.ui.constellationsValue.setText(", ".join(constellations))

            self.ui.terminalTextEdit.append(".RNX file metadata extracted and applied to UI fields")

            self.ui.outputButton.setEnabled(True)  # allow choosing output dir next
            self.ui.showConfigButton.setEnabled(True)  # allow showing config
        except Exception as e:
            self.ui.terminalTextEdit.append(f"Error extracting RNX metadata: {e}")
            print(f"Error extracting RNX metadata: {e}")
            return None

        # If both are available already (user might have chosen output first), emit
        if self.rnx_file and self.output_dir:
            self.ready.emit(self.rnx_file, self.output_dir)

        return result

    def load_output_dir(self):
        """Pick an output directory; if RNX is also set, emit ready."""
        path = self._select_output_dir(self.parent)
        if not path:
            return

        self.output_dir = path
        self.ui.terminalTextEdit.append(f"Output directory selected: {path}")

        # MainWindow owns when to enable processButton. This controller exposes a helper if needed.
        self.enable_process_button()

        if self.rnx_file:
            self.ready.emit(self.rnx_file, self.output_dir)

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

    def _enable_free_text_for_receiver_and_antenna(self):
        """Allow entering custom Receiver/Antenna types via popup prompt."""
        self.ui.Receiver_type.setEditable(True)
        self.ui.Receiver_type.lineEdit().setReadOnly(True)
        self.ui.Antenna_type.setEditable(True)
        self.ui.Antenna_type.lineEdit().setReadOnly(True)

        # Receiver type free text
        def _ask_receiver_type():
            text, ok = QInputDialog.getText(self.ui.Receiver_type, "Receiver Type", "Enter receiver type:")
            if ok and text:
                self.ui.Receiver_type.clear()
                self.ui.Receiver_type.addItem(text)
                # Let ComboBox display the selected text itself
                self.ui.Receiver_type.lineEdit().setText(text)
                self.ui.receiverTypeValue.setText(text)

        self.ui.Receiver_type.showPopup = _ask_receiver_type
        self.ui.receiverTypeValue.setText("")

        # Antenna type free text
        def _ask_antenna_type():
            text, ok = QInputDialog.getText(self.ui.Antenna_type, "Antenna Type", "Enter antenna type:")
            if ok and text:
                self.ui.Antenna_type.clear()
                self.ui.Antenna_type.addItem(text)
                self.ui.Antenna_type.lineEdit().setText(text)
                self.ui.antennaTypeValue.setText(text)

        self.ui.Antenna_type.showPopup = _ask_antenna_type
        self.ui.antennaTypeValue.setText("")

    #endregion

    #region Small Dialogs

    def _open_antenna_offset_dialog(self):
        dlg = QDialog(self.ui.antennaOffsetButton)
        dlg.setWindowTitle("Antenna Offset")

        # Try to parse existing U, N, E values
        try:
            u0, n0, e0 = [float(x.strip()) for x in self.ui.antennaOffsetValue.text().split(",")]
        except Exception:
            u0 = n0 = e0 = 0.0

        form = QFormLayout(dlg)

        sb_u = QDoubleSpinBox(dlg)
        sb_u.setRange(-9999, 9999)
        sb_u.setDecimals(3)
        sb_u.setValue(u0)
        sb_n = QDoubleSpinBox(dlg)
        sb_n.setRange(-9999, 9999)
        sb_n.setDecimals(3)
        sb_n.setValue(n0)
        sb_e = QDoubleSpinBox(dlg)
        sb_e.setRange(-9999, 9999)
        sb_e.setDecimals(3)
        sb_e.setValue(e0)

        form.addRow("U:", sb_u)
        form.addRow("N:", sb_n)
        form.addRow("E:", sb_e)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("OK", dlg)
        cancel_btn = QPushButton("Cancel", dlg)
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        form.addRow(btn_row)

        ok_btn.clicked.connect(lambda: self._set_antenna_offset(sb_u, sb_n, sb_e, dlg))
        cancel_btn.clicked.connect(dlg.reject)

        dlg.exec()

    def _set_antenna_offset(self, sb_u: QDoubleSpinBox, sb_n: QDoubleSpinBox, sb_e: QDoubleSpinBox, dlg: QDialog):
        u, n, e = sb_u.value(), sb_n.value(), sb_e.value()
        text = f"{u}, {n}, {e}"
        self.ui.antennaOffsetButton.setText(text)
        self.ui.antennaOffsetValue.setText(text)
        dlg.accept()

    def _open_time_window_dialog(self):
        dlg = QDialog(self.ui.timeWindowValue)
        dlg.setWindowTitle("Select start / end time")

        vbox = QVBoxLayout(dlg)
        start_edit = QDateTimeEdit(QDateTime.currentDateTime(), dlg)
        end_edit = QDateTimeEdit(QDateTime.currentDateTime(), dlg)

        start_edit.setCalendarPopup(True)
        end_edit.setCalendarPopup(True)
        start_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        end_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")

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

    def _set_time_window(self, start_edit: QDateTimeEdit, end_edit: QDateTimeEdit, dlg: QDialog):
        if end_edit.dateTime() < start_edit.dateTime():
            QMessageBox.warning(dlg, "Time error", "End time cannot be earlier than start time.\nPlease select again.")
            return

        s = start_edit.dateTime().toString("yyyy-MM-dd_HH:mm:ss")
        e = end_edit.dateTime().toString("yyyy-MM-dd_HH:mm:ss")
        self.ui.timeWindowButton.setText(f"{s} to {e}")
        self.ui.timeWindowValue.setText(f"{s} to {e}")
        dlg.accept()

    def _open_data_interval_dialog(self):
        val, ok = QInputDialog.getInt(
            self.ui.dataIntervalButton,
            "Data interval",
            "Input interval (seconds):",
            1,
            1,
            999_999,
        )
        if ok:
            # Keep "X s" to match RNX metadata format and existing parsing in MainController
            text = f"{val} s"
            self.ui.dataIntervalButton.setText(text)
            self.ui.dataIntervalValue.setText(f"{val} s")

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
        ppp_series         = self.ui.PPP_series.currentText() if self.ui.PPP_series.currentText() != "Select one" else ""

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
        """Run PEA processing with validation"""
        raw = self.ui.timeWindowValue.text()
        print(raw)
        try:
            start_str, end_str = raw.split("to")
            start = datetime.strptime(start_str.strip(), "%Y-%m-%d_%H:%M:%S")
            end = datetime.strptime(end_str.strip(), "%Y-%m-%d_%H:%M:%S")
        except ValueError:
            QMessageBox.warning(
                None,
                "Format error",
                "Time window must be in the format:\n"
                "YYYY-MM-DD_HH:MM:SS to YYYY-MM-DD_HH:MM:SS"
            )
            return

        if start > end:
            QMessageBox.warning(
                None,
                "Time error",
                "Start time cannot be later than end time."
            )
            return

        if not getattr(self, "config_path", None):
            QMessageBox.warning(
                None,
                "No config file",
                "Please click Show config and select a YAML file first."
            )
            return
        
        # just for sprint 4 exhibition
        # self.ui.terminalTextEdit.clear()
        # self.ui.terminalTextEdit.append("Basic validation passed, starting PEA execution...")

        inputs = self.extract_ui_values(self.rnx_file)
        # Ignore PPP downloading, TODO: need backend to repair CDDIS connection
        try:
            download_ppp_products(inputs)
        except Exception as e:
            self.ui.terminalTextEdit.append(f"⚠️ PPP products download failed: {e}")
            self.ui.terminalTextEdit.append("Continuing without PPP products...")

        self.pea_ready.emit()

        # Ignore PEA execution, TODO: need backend to repair configuration problems
        try:
            self.execution.execute_config()
        except Exception as e:
            self.ui.terminalTextEdit.append(f"⚠️ PEA execution failed: {e}")
            self.ui.terminalTextEdit.append("Continuing to plot generation...")

        # download_ppp_products(inputs)
        # self.pea_ready.emit()
        # self.execution.execute_config()

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
        """Convert 'u, n, e' into [u, n, e] floats."""
        try:
            u, n, e = map(str.strip, antenna_offset_raw.split(","))
            return [float(u), float(n), float(e)]
        except ValueError:
            raise ValueError("Invalid antenna offset format. Expected: 'u, n, e'")

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

    @staticmethod
    def _get_ppp_provider_items() -> List[str]:
        return ["COD", "GFZ", "JPL", "ESA", "IGS", "WUH"]

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


