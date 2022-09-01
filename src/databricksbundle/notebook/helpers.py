import sys
from databricksbundle.dbutils.IPythonDbUtilsResolver import resolve_dbutils


def get_notebook_context():
    return resolve_dbutils().notebook.entry_point.getDbutils().notebook().getContext()


def get_user_email():
    return get_notebook_context().tags().get("user").get()


def get_notebook_path():
    return get_notebook_context().notebookPath().get()


def is_notebook_environment():
    python_shell_launcher = "/databricks/python_shell/scripts/PythonShell.py"  # DBR < 11.0
    ipykernel_launcher = "/databricks/python_shell/scripts/db_ipykernel_launcher.py"  # DBR >= 11.0

    return sys.argv and (sys.argv[0] == python_shell_launcher or sys.argv[0] == ipykernel_launcher)


def ipython_display(obj):
    # pylint: disable=import-outside-toplevel
    import IPython

    IPython.get_ipython().user_ns["display"](obj)
