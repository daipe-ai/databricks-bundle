from databricksbundle.spark.DatabricksConnectSessionFactory import DatabricksConnectSessionFactory
from databricksbundle.spark.DatabricksSessionFactory import DatabricksSessionFactory
from databricksbundle.detector import isDatabricks
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy

class AutodetectionSessionFactory:

    def __init__(
        self,
        databricksConnectSessionFactory: DatabricksConnectSessionFactory,
        databricksSessionFactory: DatabricksSessionFactory,
    ):
        self.__databricksConnectSessionFactory = databricksConnectSessionFactory
        self.__databricksSessionFactory = databricksSessionFactory


    def create(self) -> SparkSessionLazy:
        if isDatabricks():
            return self.__databricksSessionFactory.create()

        return self.__databricksConnectSessionFactory.create()
