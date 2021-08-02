from typing import List
from box import Box
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy
from databricksbundle.spark.config.ConfiguratorInterface import ConfiguratorInterface


class DatabricksConnectSessionFactory:
    def __init__(
        self,
        config: Box,
        configurators: List[ConfiguratorInterface],
    ):
        self.__config = config
        self.__configurators = configurators

    def create(self) -> SparkSessionLazy:
        def create_lazy():
            if not self.__config.address:
                raise Exception("Databricks workspace address not set")

            if not self.__config.token:
                raise Exception("Databricks workspace token not set")

            if not self.__config.cluster_id:
                raise Exception("Databricks cluster not set")

            if not self.__config.port:
                raise Exception("Databricks connect port not set")

            # Databricks Connect configuration must be set before calling getOrCreate()
            conf = SparkConf()
            conf.set("spark.databricks.service.address", self.__config.address)
            conf.set("spark.databricks.service.token", self.__config.token)
            conf.set("spark.databricks.service.clusterId", self.__config.cluster_id)

            if self.__config.org_id is not None:
                conf.set("spark.databricks.service.orgId", self.__config.org_id)

            conf.set("spark.databricks.service.port", self.__config.port)

            if self.__config.driver_bind_address is not None:
                conf.set("spark.driver.bindAddress", self.__config.driver_bind_address)

            spark = SparkSession.builder.config(conf=conf).getOrCreate()

            for configurator in self.__configurators:
                configurator.configure(spark)

            return spark

        return SparkSessionLazy(create_lazy)
