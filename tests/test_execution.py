import os
import shutil
import subprocess
import unittest
from importlib.resources import files
from pathlib import Path

from app.models.execution import Execution


class TestExecution(unittest.TestCase):
    def test_load_sample_config(self):
        execution = Execution(config_path=Path(files("tests.resources").joinpath("ppp_example.yaml")))
        self.assertFalse(execution.config.values() == {}, "Caches ppp_example config from tests/resources/ppp_example.yaml")

    def test_copies_template_config(self):
        test_config_path = files("tests.resources").joinpath("non_existent.yaml")
        if os.path.isfile(str(test_config_path)):
            os.remove(str(test_config_path))
        self.assertFalse(os.path.isfile(str(test_config_path)), "tests/resources/non_existent.yaml shouldn't exist prior to test")
        Execution(config_path=Path(files("tests.resources").joinpath("non_existent.yaml")))
        self.assertTrue(os.path.isfile(str(test_config_path)), "tests/resources/non_existent.yaml should be created by execution")


    def test_execute_ppp_example_config(self):
        # common paths
        data_folder = str(files("tests.resources.inputData").joinpath("data"))
        product_folder = str(files("tests.resources.inputData").joinpath("products"))
        output_folder = str(files("tests.resources").joinpath("output"))
        sample_config = str(files("tests.resources").joinpath("ppp_example.yaml"))

        # Ensure data downloaded
        if not len(os.listdir(data_folder)) > 3:
            subprocess.call("./getData.sh", shell=True, text=True, cwd=data_folder)

        if not len(os.listdir(product_folder)) > 3:
            subprocess.call("./getProducts.sh", shell=True, text=True, cwd=product_folder)

        execution = Execution(Path(sample_config))

        # Clears output folder
        for filename in os.listdir(output_folder):
            file_path = os.path.join(output_folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")

        # Apply test settings
        alterations = {"outputs.outputs_root": output_folder + "/",
                       "inputs.inputs_root": product_folder + "/"}
        for key, value in alterations.items():
            execution.edit_config(key, value)

        # Executes
        execution.execute_config()

        # Only checks if output is created successfully, not accuracy of output
        self.assertTrue(os.listdir(output_folder), "Output folder should not be empty")
