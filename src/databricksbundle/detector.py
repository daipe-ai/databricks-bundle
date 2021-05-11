import os
import sys
from pathlib import Path


def is_databricks():
    """
    Check, if scripts are running on databricks cluster or locally.

    DATABRICKS_RUNTIME_VERSION available for all version and only on databricks cluster
    :return: true, if running on databricks
    """
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None


def is_jupyter_server_running():
    return "ipykernel" in sys.modules


def set_jupyter_cwd():
    from jupyter_server import serverapp

    running_servers = list(serverapp.list_running_servers())
    for item in running_servers:
        if Path(item["root_dir"]).resolve() in Path.cwd().parents:
            return os.chdir(item["root_dir"])
