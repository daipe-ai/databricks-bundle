from databricksbundle.dbutils.IPythonDbUtilsResolver import resolveDbUtils

def getNotebookContext():
    return resolveDbUtils().notebook.entry_point.getDbutils().notebook().getContext()

def getUserEmail():
    return getNotebookContext().tags().get('user').get()

def getNotebookPath():
    return getNotebookContext().notebookPath().get()
