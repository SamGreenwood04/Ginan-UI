import os
import shutil
import subprocess
from ruamel.yaml.scalarstring import PlainScalarString
from importlib.resources import files
from pathlib import Path
import pandas as pd

from app.utils.yaml import load_yaml, write_yaml
from app.utils.plot_pos import plot_pos_files

TEMPLATE_PATH = Path(__file__).parent.parent / "resources" / "Yaml" / "default_config.yaml"
GENERATED_YAML = Path(__file__).parent.parent / "resources" / "ppp_generated.yaml"
INPUT_DATA_PATH = Path(__file__).parent.parent / "resources" / "inputData" / "data"
INPUT_PRODUCTS_PATH = Path(__file__).parent.parent / "resources" / "inputData" / "products"
TEST_PRODUCTS_PATH = Path(__file__).parent.parent.parent / "tests" / "resources" / "inputData" / "products"

class Execution:
    def __init__(self, executable, config_path: str=GENERATED_YAML):
        self.config_path = config_path
        self.executable = executable # the PEA executable
        self.changes = False # Flag to track if config has been changed

        try:
            self.config = load_yaml(config_path)
        except FileNotFoundError:
            shutil.copy(TEMPLATE_PATH, config_path)
            self.config = load_yaml(config_path)
        if not self.config:
            raise ValueError(f"Failed to load configuration from {config_path}. Please check the file format.")


    def edit_config(self, key_path: str, value, add_field=False):
        """
        Edits the cached config

        :param key_path: Yaml key e.g. "outputs.outputs_root"
        :param value: new Yaml value e.g. "/my/path/to/outputs"
        :param add_field: Adds field if it doesn't exist
        :raises KeyError if key not found
        """
        self.changes = True # Mark config as changed
        keys = key_path.split(".")

        node = self.config
        for key in keys[:-1]: # Ensure key path validity
            if key not in node:
                if add_field:
                    node[key] = {}
                else:
                    raise KeyError(f"Key '{key}' not found in {node}")
            node = node[key]
        if not add_field:
            if keys[-1] not in node:
                # raise KeyError(f"Key '{keys[-1]}' not found in {node}")
                pass
        node[keys[-1]] = value

    def apply_ui_config(self, inputs):
        self.changes = True
        # 1. Set core inputs / outputs
        self.edit_config("inputs.inputs_root", str(INPUT_PRODUCTS_PATH) + "/", False)
        #self.edit_config("inputs.gnss_observations.gnss_observations_root", str(INPUT_PRODUCTS_PATH), True)
        self.edit_config("inputs.gnss_observations.rnx_inputs", [inputs.rnx_path], False)
        self.edit_config("outputs.outputs_root", inputs.output_path, False)

        # 2. Modify the config file to use the right receiver acronym
        if "TEST" in self.config.get("receiver_options", {}):
            self.config["receiver_options"][inputs.marker_name] = self.config["receiver_options"].pop("TEST")

        # 3. Modify the file to include the UI extraction values
        self.edit_config("processing_options.epoch_control.start_epoch", PlainScalarString(inputs.start_epoch), False)
        self.edit_config("processing_options.epoch_control.end_epoch", PlainScalarString(inputs.end_epoch), False)
        self.edit_config("processing_options.epoch_control.epoch_interval", inputs.epoch_interval, False)
        self.edit_config(f"receiver_options.{inputs.marker_name}.receiver_type", inputs.receiver_type, True)
        self.edit_config(f"receiver_options.{inputs.marker_name}.antenna_type", inputs.antenna_type, True)
        self.edit_config(f"receiver_options.{inputs.marker_name}.models.eccentricity.offset",
                              inputs.antenna_offset, True)
        self.edit_config("estimation_parameters.receivers.global.pos.process_noise", inputs.mode, False)

        # 4. Set constellation processing based on user selection
        # First, disable all possible constellations
        all_constellations = ["gps", "gal", "glo", "bds", "qzs"]
        for const in all_constellations:
            self.edit_config(f"processing_options.gnss_general.sys_options.{const}.process", False, False)
        
        # Then enable only the selected constellations
        if inputs.constellations_raw:
            selected_constellations = [const.strip().lower() for const in inputs.constellations_raw.split(",") if const.strip()]
            for const_key in selected_constellations:
                if const_key in all_constellations:
                    self.edit_config(f"processing_options.gnss_general.sys_options.{const_key}.process", True, False)

    def write_cached_changes(self):
        write_yaml(self.config_path, self.config)
        self.changes = False

    def execute_config(self):
        if self.changes:
            self.write_cached_changes()
            self.changes = False

        command = [self.executable, "--config", self.config_path]
        try:
            # Run PEA using a subprocess at the directory "config_path"
            subprocess.run(command, check=True, text=True,cwd=os.path.dirname(self.config_path))
        except subprocess.CalledProcessError as e:
            e.add_note("Error executing PEA command")
            raise e

    def build_pos_plots(self, out_dir=None):
        """
        Search for .pos and .POS files in outputs_root (or default tests/resources/outputData),
        and generate .html files in tests/resources/outputData/visual.
        Return a list of generated html paths (str).
        """
        try:
            outputs_root = self.config["outputs"]["outputs_root"]
            root = Path(outputs_root).expanduser().resolve()
        except Exception:
            # if the outputs_root is not set, use the default tests/resources/outputData
            root = Path(__file__).resolve().parents[2] / "tests" / "resources" / "outputData"
            root = root.resolve()

        # if the out_dir is not set, use the default tests/resources/outputData/visual
        if out_dir is None:
            out_dir = root / "visual"
        else:
            out_dir = Path(out_dir).expanduser().resolve()
        out_dir.mkdir(parents=True, exist_ok=True)

        htmls = []
        pos_files = list(root.rglob("*.pos")) + list(root.rglob("*.POS"))
        if pos_files:
            try:
                htmls = plot_pos_files(
                    input_files=[str(pos_path) for pos_path in pos_files],
                    save_prefix=str(out_dir / "plot")
                )
            except Exception as e:
                print("[plot_pos] Failed to generate plots:", e)
                for pos_path in pos_files:
                    try:
                        htmls.extend(plot_pos_files(
                            input_files=[str(pos_path)],
                            save_prefix=str(out_dir / "plot")
                        ))
                    except Exception as e:
                        print("[plot_pos] Failed:", pos_path, e)

        return htmls