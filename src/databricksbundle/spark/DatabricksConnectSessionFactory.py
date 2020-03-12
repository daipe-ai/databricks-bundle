from typing import Optional
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy

class DatabricksConnectSessionFactory:

    def __init__(
        self,
        address: str,
        token: str,
        clusterId: str,
        orgId: Optional[str],
        port: int,
        bindAddress: Optional[str],
        extraConfig: dict = None,
    ):
        self.__address = address
        self.__token = token
        self.__clusterId = clusterId
        self.__orgId = orgId
        self.__port = port
        self.__bindAddress = bindAddress
        self.__extraConfig = extraConfig or dict()

    def create(self) -> SparkSessionLazy:
        def createLazy():
            conf = SparkConf()
            conf.set('spark.databricks.service.address', self.__address)
            conf.set('spark.databricks.service.token', self.__token)
            conf.set('spark.databricks.service.clusterId', self.__clusterId)

            if self.__orgId is not None:
                conf.set('spark.databricks.service.orgId', self.__orgId)

            conf.set('spark.databricks.service.port', self.__port)

            if self.__bindAddress is not None:
                conf.set('spark.driver.bindAddress', self.__bindAddress)

            for k, v in self.__extraConfig.items():
                conf.set(k, v)

            return SparkSession.builder.config(conf=conf).getOrCreate()

        return SparkSessionLazy(createLazy)
