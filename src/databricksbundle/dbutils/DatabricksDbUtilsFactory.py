from databricksbundle.dbutils.DbUtilsWrapper import DbUtilsWrapper
from databricksbundle.dbutils.IPythonDbUtilsResolver import resolve_dbutils


class DatabricksDbUtilsFactory:
    def create(self) -> DbUtilsWrapper:
        return DbUtilsWrapper(resolve_dbutils)
