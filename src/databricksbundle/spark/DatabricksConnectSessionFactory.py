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
        clusterId: str,
        orgId: Optional[str],
        port: int,
        bindAddress: Optional[str],
        configurators: List[ConfiguratorInterface],
    ):
        self.__address = address
        self.__token = token
        self.__clusterId = clusterId
        self.__orgId = orgId
        self.__port = port
        self.__bindAddress = bindAddress
        self.__configurators = configurators

    def create(self) -> SparkSessionLazy:
        if not self.__address:
            raise Exception('Databricks workspace address not set')

        if not self.__token:
            raise Exception('Databricks workspace token not set')

        if not self.__clusterId:
            raise Exception('Databricks cluster not set')

        if not self.__port:
            raise Exception('Databricks connect port not set')

        def createLazy():
            # Databricks Connect configuration must be set before calling getOrCreate()
            conf = SparkConf()
            conf.set('spark.databricks.service.address', self.__address)
            conf.set('spark.databricks.service.token', self.__token)
            conf.set('spark.databricks.service.clusterId', self.__clusterId)

            if self.__orgId is not None:
                conf.set('spark.databricks.service.orgId', self.__orgId)

            conf.set('spark.databricks.service.port', self.__port)

            if self.__bindAddress is not None:
                conf.set('spark.driver.bindAddress', self.__bindAddress)

            spark = SparkSession.builder.config(conf=conf).getOrCreate()

            for configurator in self.__configurators:
                configurator.configure(spark)

            return spark

        return SparkSessionLazy(createLazy)
