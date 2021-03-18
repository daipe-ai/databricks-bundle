from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from databricksbundle.jdbc.OptionsFactoryInterface import OptionsFactoryInterface


class TableReader:
    def __init__(
        self,
        options_factory: OptionsFactoryInterface,
        spark: SparkSession,
    ):
        self.__options_factory = options_factory
        self.__spark = spark

    def read(self, table_name: str, **kwargs) -> DataFrame:
        options = {**self.__options_factory.create(), **kwargs}

        return self.__spark.read.format("jdbc").options(**options).option("dbtable", table_name).load()
