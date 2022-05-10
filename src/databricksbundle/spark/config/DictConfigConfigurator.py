from typing import Optional

from pyspark.sql.session import SparkSession
from databricksbundle.spark.config.ConfiguratorInterface import ConfiguratorInterface


class DictConfigConfigurator(ConfiguratorInterface):
    def __init__(
        self,
        dict_config: Optional[dict] = None,
    ):
        self.__dict_config = dict_config or {}

    def configure(self, spark: SparkSession):
        for k, v in self.__dict_config.items():
            spark.conf.set(k, v)
