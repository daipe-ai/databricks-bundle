import os


def is_databricks():
    """
    Check, if scripts are running on databricks cluster or locally.

    DATABRICKS_RUNTIME_VERSION available for all version and only on databricks cluster
    :return: true, if running on databricks
    """
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None


def is_databricks_repo():
    return is_databricks() and os.getcwd().startswith("/Workspace/Repos")
