import subprocess, shutil
from pathlib import Path


def compile_ui():
    ui_file = Path(__file__).parent.parent / "views" / "main_window.ui"
    output_file = Path(__file__).parent.parent / "views" / "main_window_ui.py"
    if shutil.which("pyside6-uic"):
        with open(output_file, 'w') as f:
            f.write("# This file is auto-generated. Do not edit.\n")
        result = subprocess.run(["pyside6-uic", ui_file, "-o", output_file], capture_output=True)
        if result.returncode != 0:
            print(f"Error compiling UI: {result.stderr.decode()}")
        else:
            print("UI compiled successfully.")
            print(result.stdout.decode())
    else:
        raise ImportError("Ensure pyside6-uic is installed and available on PATH.")

    # Manually fix the file path to the logo resource
    with open(output_file, 'r') as f:
        lines = f.readlines()
    lines[21] = "from app.resources import ginan_logo_rc"
    with open(output_file, 'w') as f:
        f.writelines(lines)

if __name__ == "__main__":
    compile_ui()