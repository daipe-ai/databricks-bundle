from pyspark.sql import SparkSession # pylint: disable = unused-import
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy
import IPython

class DatabricksSessionFactory:

    def __init__(
        self,
        sparkConfig: dict,
    ):
        self.__sparkConfig = sparkConfig

    def create(self) -> SparkSessionLazy:
        spark = IPython.get_ipython().user_ns['spark'] # type: SparkSession

        def createLazy():
            for k, v in self.__sparkConfig.items():
                spark.conf.set(k, v)

            return spark

        return SparkSessionLazy(createLazy)
