from pyspark.sql import SparkSession
from databricksbundle.dbutils.DbUtilsWrapper import DbUtilsWrapper

class DatabricksConnectDbUtilsFactory:

    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def create(self) -> DbUtilsWrapper:
        def createLazy():
            from pyspark.dbutils import DBUtils # pylint: disable = import-outside-toplevel

            return DBUtils(self.__spark)

        return DbUtilsWrapper(createLazy)
