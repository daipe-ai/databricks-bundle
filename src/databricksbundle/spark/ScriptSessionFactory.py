from typing import List
from pyspark.sql import SparkSession
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy
from databricksbundle.spark.config.ConfiguratorInterface import ConfiguratorInterface


class ScriptSessionFactory:
    def __init__(
        self,
        configurators: List[ConfiguratorInterface] = None,
    ):
        self.__configurators = configurators or []

    def create(self) -> SparkSessionLazy:
        def create_lazy():
            spark = SparkSession.builder.getOrCreate()

            for configurator in self.__configurators:
                configurator.configure(spark)

            return spark

        return SparkSessionLazy(create_lazy)
