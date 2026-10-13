"""Isola o runtime Streamlit dos imports e variaveis dos testes de banco."""
from pathlib import Path
import subprocess
import sys
import unittest


class Interface(unittest.TestCase):
    def test_quatro_paginas_sem_consultas_externas(self):
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("smoke_interface.py"))],
                                capture_output=True, text=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
