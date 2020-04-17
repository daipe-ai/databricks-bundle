from typing import List
from pyspark.sql import SparkSession # pylint: disable = unused-import
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy
import IPython
from databricksbundle.spark.config.ConfiguratorInterface import ConfiguratorInterface

class DatabricksSessionFactory:

    def __init__(
        self,
        configurators: List[ConfiguratorInterface],
    ):
        self.__configurators = configurators

    def create(self) -> SparkSessionLazy:
        spark = IPython.get_ipython().user_ns['spark'] # type: SparkSession

        for configurator in self.__configurators:
            configurator.configure(spark)

        IPython.get_ipython().user_ns['spark'] = spark

        return SparkSessionLazy(lambda: spark)
