import sys
from databricksbundle.dbutils.IPythonDbUtilsResolver import resolve_dbutils


def get_notebook_context():
    return resolve_dbutils().notebook.entry_point.getDbutils().notebook().getContext()


def get_user_email():
    return get_notebook_context().tags().get("user").get()


def get_notebook_path():
    return get_notebook_context().notebookPath().get()


def is_notebook_environment():
    return sys.argv and sys.argv[0][-15:] == "/PythonShell.py"


def ipython_display(obj):
    import IPython

    IPython.get_ipython().user_ns["display"](obj)
