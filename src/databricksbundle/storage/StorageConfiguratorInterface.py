from abc import ABC
from box import Box
from pyspark.sql.session import SparkSession


class StorageConfiguratorInterface(ABC):
    def configure(self, spark: SparkSession, config: Box):
        pass

    def get_type(self) -> str:
        pass
