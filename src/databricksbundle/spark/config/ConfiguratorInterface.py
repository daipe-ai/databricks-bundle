from abc import ABC, abstractmethod
from pyspark.sql.session import SparkSession


class ConfiguratorInterface(ABC):
    @abstractmethod
    def configure(self, spark: SparkSession):
        pass
