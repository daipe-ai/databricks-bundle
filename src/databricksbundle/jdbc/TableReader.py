from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from databricksbundle.jdbc.OptionsFactoryInterface import OptionsFactoryInterface

class TableReader:

    def __init__(
        self,
        optionsFactory: OptionsFactoryInterface,
        spark: SparkSession,
    ):
        self.__optionsFactory = optionsFactory
        self.__spark = spark

    def read(self, tableName: str, **kwargs) -> DataFrame:
        options = {**self.__optionsFactory.create(), **kwargs}

        return (
            self.__spark
                .read
                .format('jdbc')
                .options(**options)
                .option('dbtable', tableName)
                .load()
        )
