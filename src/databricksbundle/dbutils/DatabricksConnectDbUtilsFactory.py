from pyspark.sql import SparkSession
from databricksbundle.dbutils.DbUtilsWrapper import DbUtilsWrapper


class DatabricksConnectDbUtilsFactory:
    def __init__(self, spark: SparkSession):
        self.__spark = spark

    def create(self) -> DbUtilsWrapper:
        def create_lazy():
            from pyspark.dbutils import DBUtils

            return DBUtils(self.__spark)

        return DbUtilsWrapper(create_lazy)
