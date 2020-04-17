from pyspark.sql.session import SparkSession
from databricksbundle.spark.config.ConfiguratorInterface import ConfiguratorInterface

class DictConfigConfigurator(ConfiguratorInterface):

    def __init__(
        self,
        dictConfig: dict = None,
    ):
        self.__dictConfig = dictConfig or dict()

    def configure(self, spark: SparkSession):
        for k, v in self.__dictConfig.items():
            spark.conf.set(k, v)
