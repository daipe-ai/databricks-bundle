def resolve_dbutils():
    import IPython

    ipython = IPython.get_ipython()

    if not hasattr(ipython, "user_ns") or "dbutils" not in ipython.user_ns:
        raise Exception("dbutils cannot be resolved")

    return ipython.user_ns["dbutils"]
