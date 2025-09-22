import unittest
from pathlib import Path
from app.models.execution import get_pea_exec
from importlib.resources import files
import subprocess

class PEAExecutable(unittest.TestCase):
    def test_executable_exists(self):
        app_image = files('app.resources').joinpath('ginan.AppImage').is_file()
        mac_binary = files('app.resources.osx_arm64.bin').joinpath('pea').is_file()
        self.assertTrue(app_image and mac_binary, "Executable files should exist")

    def test_finds_executable(self):
        self.assertIsNotNone(get_pea_exec(), "Executable should be found")

    def test_executable(self):
        result = subprocess.call(get_pea_exec(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.assertEqual(result, 0, "Executable should run without error")
