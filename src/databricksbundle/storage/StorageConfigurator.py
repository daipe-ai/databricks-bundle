from box import Box
from typing import List, Dict
from pyspark.sql.session import SparkSession
from databricksbundle.storage.StorageConfiguratorInterface import StorageConfiguratorInterface
from databricksbundle.spark.config.ConfiguratorInterface import ConfiguratorInterface


class StorageConfigurator(ConfiguratorInterface):

    __storage_configurators: Dict[str, StorageConfiguratorInterface]

    def __init__(self, storages: Box, storage_configurators: List[StorageConfiguratorInterface]):
        self.__storages = storages or Box({})
        self.__storage_configurators = {
            storage_configurator.get_type(): storage_configurator for storage_configurator in storage_configurators
        }

    def configure(self, spark: SparkSession):
        for key, conf in self.__storages.items():
            if conf.type not in self.__storage_configurators:
                raise Exception(f"No configurator for storage: {conf.type}")

            storage_configurator = self.__storage_configurators[conf.type]

            storage_configurator.configure(spark, conf)
