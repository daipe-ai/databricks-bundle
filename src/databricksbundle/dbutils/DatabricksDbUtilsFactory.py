from databricksbundle.dbutils.DbUtilsWrapper import DbUtilsWrapper
from databricksbundle.dbutils.IPythonDbUtilsResolver import resolveDbUtils

class DatabricksDbUtilsFactory:

    def create(self) -> DbUtilsWrapper:
        return DbUtilsWrapper(resolveDbUtils)
