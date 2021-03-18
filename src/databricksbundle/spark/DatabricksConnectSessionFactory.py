from typing import Optional, List
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy
from databricksbundle.spark.config.ConfiguratorInterface import ConfiguratorInterface


class DatabricksConnectSessionFactory:
    def __init__(
        self,
        address: str,
        token: str,
        cluster_id: str,
        org_id: Optional[str],
        port: int,
        bind_address: Optional[str],
        configurators: List[ConfiguratorInterface],
    ):
        self.__address = address
        self.__token = token
        self.__cluster_id = cluster_id
        self.__org_id = org_id
        self.__port = port
        self.__bind_address = bind_address
        self.__configurators = configurators

    def create(self) -> SparkSessionLazy:
        if not self.__address:
            raise Exception("Databricks workspace address not set")

        if not self.__token:
            raise Exception("Databricks workspace token not set")

        if not self.__cluster_id:
            raise Exception("Databricks cluster not set")

        if not self.__port:
            raise Exception("Databricks connect port not set")

        def create_lazy():
            # Databricks Connect configuration must be set before calling getOrCreate()
            conf = SparkConf()
            conf.set("spark.databricks.service.address", self.__address)
            conf.set("spark.databricks.service.token", self.__token)
            conf.set("spark.databricks.service.clusterId", self.__cluster_id)

            if self.__org_id is not None:
                conf.set("spark.databricks.service.orgId", self.__org_id)

            conf.set("spark.databricks.service.port", self.__port)

            if self.__bind_address is not None:
                conf.set("spark.driver.bindAddress", self.__bind_address)

            spark = SparkSession.builder.config(conf=conf).getOrCreate()

            for configurator in self.__configurators:
                configurator.configure(spark)

            return spark

        return SparkSessionLazy(create_lazy)
