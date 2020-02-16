from pyspark.sql import SparkSession # pylint: disable = unused-import
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy
import IPython

class DatabricksSessionFactory:

    def __init__(
        self,
        extraConfig: dict,
    ):
        self.__extraConfig = extraConfig

    def create(self) -> SparkSessionLazy:
        spark = IPython.get_ipython().user_ns['spark'] # type: SparkSession

        def createLazy():
            for k, v in self.__extraConfig.items():
                spark.conf.set(k, v)

            return spark

        return SparkSessionLazy(createLazy)
