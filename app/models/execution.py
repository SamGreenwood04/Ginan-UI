import os
import platform
import shutil
import subprocess
import signal
import threading
import time
from importlib.resources import files

from ruamel.yaml.scalarstring import PlainScalarString
from ruamel.yaml.comments import CommentedSeq, CommentedMap
from pathlib import Path
from app.utils.yaml import load_yaml, write_yaml, normalise_yaml_value
from app.utils.plot_pos import plot_pos_files
from app.utils.common_dirs import GENERATED_YAML, TEMPLATE_PATH, INPUT_PRODUCTS_PATH

def get_pea_exec():
    # AppImage works natively
    if platform.system().lower() == "linux":
        executable = files('app.resources').joinpath('ginan.AppImage')

    # PEA available on PATH
    elif shutil.which("pea"):
        executable = "pea"

    elif platform.system().lower() == "darwin":
        executable = files('app.resources.osx_arm64.bin').joinpath('pea')

    elif platform.system().lower() == "windows":
        raise RuntimeError("No binary for windows available")

    # Unknown system
    else:
        raise RuntimeError("Unsupported platform: " + platform.system())

    return executable

class Execution:
    def __init__(self, config_path: Path=GENERATED_YAML):
        self.config_path = config_path
        self.executable = get_pea_exec() # the PEA executable
        self.changes = False # Flag to track if config has been changed
        self._procs = []  
        self._stop_event = threading.Event()

        template_file = Path(TEMPLATE_PATH)

        if config_path.exists():
            print(f"[Execution] Using existing config file: {config_path}")
        else:
            print(f"[Execution] Existing config not found, copying default template: {template_file} → {config_path}")
            try:
                config_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(template_file, config_path)
            except Exception as e:
                raise RuntimeError(f"❌ Failed to copy default config: {e}")
        self.config = load_yaml(config_path)

    def reload_config(self):
        """
        Force reload of the YAML config from disk into memory.
        This allows any manual edits to be picked up before GUI changes are applied.
        """
        try:
            self.config = load_yaml(self.config_path)
            print(f"[Execution] 🔁 Reloaded config from disk: {self.config_path}")
        except Exception as e:
            raise RuntimeError(f"❌ Failed to reload config from {self.config_path}: {e}")

    def edit_config(self, key_path: str, value, add_field=False):
        """
        Edits the cached config while preserving YAML formatting and comments.

        :param key_path: Dot-separated YAML key path (e.g., "inputs.gnss_observations.rnx_inputs")
        :param value: New value to assign (will be converted to ruamel-safe types)
        :param add_field: Whether to add the field if it doesn't exist
        :raises KeyError if path doesn't exist and add_field is False
        """
        self.changes = True # Mark config as changed
        keys = key_path.split(".")
        node = self.config

        for key in keys[:-1]:
            if key not in node:
                if add_field:
                    node[key] = CommentedMap()
                else:
                    raise KeyError(f"Key '{key}' not found in {node}")
            node = node[key]

        final_key = keys[-1]
        value = normalise_yaml_value(value)

        # Preserve any existing comment on the final_key
        if final_key in node:
            old_value = node[final_key]
            if hasattr(old_value, 'ca') and not hasattr(value, 'ca'):
                value.ca = old_value.ca

        if not add_field and final_key not in node:
            raise KeyError(f"Key '{final_key}' not found in {key_path}")

        node[final_key] = value

    def apply_ui_config(self, inputs):
        print("✅ apply_ui_config was called")
        print("[DEBUG] apply_ui_config: rnx_inputs =", inputs.rnx_path, "| type =", type(inputs.rnx_path))
        self.changes = True

        # 1. Set core inputs / outputs
        self.edit_config("inputs.inputs_root", str(INPUT_PRODUCTS_PATH) + "/", False)
        self.edit_config("inputs.gnss_observations.gnss_observations_root", str(INPUT_PRODUCTS_PATH), False)

        # Normalise RNX path
        rnx_val = normalise_yaml_value(inputs.rnx_path)

        # 1a. Set rnx_inputs safely, preserving formatting
        try:
            existing = self.config["inputs"]["gnss_observations"].get("rnx_inputs")
            if isinstance(existing, CommentedSeq):
                existing.clear()
                existing.append(rnx_val)
                existing.fa.set_block_style()
            else:
                new_seq = CommentedSeq([rnx_val])
                new_seq.fa.set_block_style()
                self.config["inputs"]["gnss_observations"]["rnx_inputs"] = new_seq
        except Exception as e:
            print(f"[apply_ui_config] Error setting rnx_inputs: {e}")

        # Normalise outputs_root
        out_val = normalise_yaml_value(inputs.output_path)
        self.edit_config("outputs.outputs_root", out_val, False)

        # 2. Replace 'TEST' receiver block with real marker name
        if "TEST" in self.config.get("receiver_options", {}):
            self.config["receiver_options"][inputs.marker_name] = self.config["receiver_options"].pop("TEST")

        # 3. Include UI-extracted values
        self.edit_config("processing_options.epoch_control.start_epoch", PlainScalarString(inputs.start_epoch), False)
        self.edit_config("processing_options.epoch_control.end_epoch", PlainScalarString(inputs.end_epoch), False)
        self.edit_config("processing_options.epoch_control.epoch_interval", inputs.epoch_interval, False)
        self.edit_config(f"receiver_options.{inputs.marker_name}.receiver_type", inputs.receiver_type, True)
        self.edit_config(f"receiver_options.{inputs.marker_name}.antenna_type", inputs.antenna_type, True)
        self.edit_config(f"receiver_options.{inputs.marker_name}.models.eccentricity.offset", inputs.antenna_offset, True)

        # Always format process_noise as a list
        self.edit_config("estimation_parameters.receivers.global.pos.process_noise", [inputs.mode], False)

        # 4. GNSS constellation toggles
        all_constellations = ["gps", "gal", "glo", "bds", "qzs"]
        for const in all_constellations:
            self.edit_config(f"processing_options.gnss_general.sys_options.{const}.process", False, False)
        
        # Then enable only the selected constellations
        if inputs.constellations_raw:
            selected = [c.strip().lower() for c in inputs.constellations_raw.split(",") if c.strip()]
            for const in selected:
                if const in all_constellations:
                    self.edit_config(f"processing_options.gnss_general.sys_options.{const}.process", True, False)

    def write_cached_changes(self):
        write_yaml(self.config_path, self.config)
        self.changes = False

    def execute_config(self):
        # clear stop flag before each run
        self.reset_stop_flag()

        if self.changes:
            self.write_cached_changes()
            self.changes = False

        command = [self.executable, "--config", str(self.config_path)]
        workdir = str(Path(self.config_path).parent)

        try:
            # spawn process with process group
            p = self.spawn_process(command, cwd=workdir)

            # forward stdout/stderr line by line, can be stopped at any time
            assert p.stdout is not None and p.stderr is not None
            while True:
                if self._stop_event.is_set():
                    # UI clicked "stop", exit loop, cleanup handled by stop_all()
                    break

                line = p.stdout.readline()
                if line:
                    print(line.rstrip())
                else:
                    # no new output, check if process has ended
                    if p.poll() is not None:
                        # print remaining stderr for debugging
                        rest_err = p.stderr.read() or ""
                        if rest_err:
                            print(rest_err.rstrip())
                        if p.returncode != 0:
                            e = subprocess.CalledProcessError(p.returncode, command)
                            e.add_note("Error executing PEA command")
                            raise e
                        break

                # slight sleep to avoid busy polling
                time.sleep(0.01)

        finally:
            # after execution, clean up finished processes
            self._procs = [proc for proc in self._procs if proc.poll() is None]

        # unified process spawning: use independent process groups, for easy kill (macOS/Linux)
    def spawn_process(self, args, cwd=None, env=None) -> subprocess.Popen:
        p = subprocess.Popen(
            args,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,   # critical: new session = new process group
        )
        self._procs.append(p)
        return p

    # one-click stop: set stop flag + terminate all child process groups
    def stop_all(self):
        self._stop_event.set()

        # try graceful termination first
        for p in list(self._procs):
            try:
                if p.poll() is None:
                    os.killpg(p.pid, signal.SIGTERM)
            except Exception:
                pass

        time.sleep(0.5)  # give it a little time

        # if still not exited, force kill
        for p in list(self._procs):
            try:
                if p.poll() is None:
                    os.killpg(p.pid, signal.SIGKILL)
            except Exception:
                pass

    def reset_stop_flag(self):
        self._stop_event.clear()


    def build_pos_plots(self, out_dir=None):
        """
        Search for .pos and .POS files directly under outputs_root (not in archive/visual),
        and generate one .html per file in outputs_root/visual.
        Return a list of generated html paths (str).
        """
        try:
            outputs_root = self.config["outputs"]["outputs_root"]
            root = Path(outputs_root).expanduser().resolve()
        except Exception:
            # Fallback to default
            root = Path(__file__).resolve().parents[2] / "tests" / "resources" / "outputData"
            root = root.resolve()

        # Set output dir for HTML plots
        if out_dir is None:
            out_dir = root / "visual"
        else:
            out_dir = Path(out_dir).expanduser().resolve()
        out_dir.mkdir(parents=True, exist_ok=True)

        # Only look in the top-level of outputs_root
        pos_files = list(root.glob("*.pos")) + list(root.glob("*.POS"))

        if pos_files:
            print(f"📂 Found {len(pos_files)} .pos files in {root}:")
            for f in pos_files:
                print(f"   • {f.name}")
        else:
            print(f"⚠️ No .pos files found in {root}")

        htmls = []
        for pos_path in pos_files:
            try:
                base_name = pos_path.stem
                save_prefix = out_dir / f"plot_{base_name}"

                html_files = plot_pos_files(
                    input_files=[str(pos_path)],
                    save_prefix=str(save_prefix)
                )
                htmls.extend(html_files)
            except Exception as e:
                print(f"[plot_pos] ❌ Failed for {pos_path.name}: {e}")

        # Final summary
        if htmls:
            print(f"✅ Generated {len(htmls)} plot(s) → saved in {out_dir}")
        else:
            print("⚠️ No plots were generated.")

        return htmls