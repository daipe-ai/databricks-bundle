from pyspark.sql import SparkSession # pylint: disable = unused-import
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy
import IPython

class DatabricksSessionFactory:

    def __init__(
        self,
        extraConfig: dict = None,
    ):
        self.__extraConfig = extraConfig or dict()

    def create(self) -> SparkSessionLazy:
        spark = IPython.get_ipython().user_ns['spark'] # type: SparkSession

        for k, v in self.__extraConfig.items():
            spark.conf.set(k, v)

        IPython.get_ipython().user_ns['spark'] = spark

        return SparkSessionLazy(lambda: spark)
