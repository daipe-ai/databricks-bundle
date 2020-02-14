from databricksbundle.dbutils.DatabricksConnectDbUtilsFactory import DatabricksConnectDbUtilsFactory
from databricksbundle.dbutils.DatabricksDbUtilsFactory import DatabricksDbUtilsFactory
from databricksbundle.dbutils.DbUtilsWrapper import DbUtilsWrapper
from databricksbundle.detector import isDatabricks

class AutodetectionDbUtilsFactory:

    def __init__(
        self,
        databricksConnectDbUtilsFactory: DatabricksConnectDbUtilsFactory,
        databricksDbUtilsFactory: DatabricksDbUtilsFactory,
    ):
        self.__databricksConnectDbUtilsFactory = databricksConnectDbUtilsFactory
        self.__databricksDbUtilsFactory = databricksDbUtilsFactory

    def create(self) -> DbUtilsWrapper:
        if isDatabricks():
            return self.__databricksDbUtilsFactory.create()

        return self.__databricksConnectDbUtilsFactory.create()
