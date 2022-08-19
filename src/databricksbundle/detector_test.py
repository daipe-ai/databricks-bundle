import os
import unittest
from unittest.mock import patch
from databricksbundle.detector import is_databricks_repo, is_databricks_workspace


os.environ["DATABRICKS_RUNTIME_VERSION"] = "10.4"


class TestDetector(unittest.TestCase):
    @patch("os.getcwd", return_value="/Workspace/Repos/dir/repo/notebook")
    @patch("sys.argv", new=["/databricks/python_shell/scripts/PythonShell.py"])
    def test_databricks_repos_detect(self, *_):
        self.assertTrue(is_databricks_repo())
        self.assertFalse(is_databricks_workspace())

    @patch("os.getcwd", return_value="/databricks/driver")
    @patch("sys.argv", new=["/databricks/python_shell/scripts/PythonShell.py"])
    def test_databricks_workspace_detect(self, *_):
        self.assertTrue(is_databricks_workspace())
        self.assertFalse(is_databricks_repo())

    @patch("os.getcwd", return_value="/databricks/driver")
    @patch("sys.argv", new=["/dbfs/some_script.py"])
    def test_databricks_script_detect(self, *_):
        self.assertFalse(is_databricks_workspace())
        self.assertFalse(is_databricks_repo())


if __name__ == "__main__":
    unittest.main()
