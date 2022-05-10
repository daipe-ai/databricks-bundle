from typing import List
from pyspark.sql import SparkSession
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy
from databricksbundle.spark.config.ConfiguratorInterface import ConfiguratorInterface


class NotebookSessionFactory:
    def __init__(
        self,
        configurators: List[ConfiguratorInterface],
    ):
        self.__configurators = configurators

    def create(self) -> SparkSessionLazy:
        # pylint: disable=import-outside-toplevel
        import IPython

        spark: SparkSession = IPython.get_ipython().user_ns["spark"]

        for configurator in self.__configurators:
            configurator.configure(spark)

        IPython.get_ipython().user_ns["spark"] = spark

        return SparkSessionLazy(lambda: spark)
