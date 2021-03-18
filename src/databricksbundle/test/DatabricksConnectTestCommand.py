from logging import Logger
from pyspark.sql import SparkSession
from argparse import Namespace
from consolebundle.ConsoleCommand import ConsoleCommand


class DatabricksConnectTestCommand(ConsoleCommand):
    def __init__(self, logger: Logger, spark: SparkSession):
        self.__logger = logger
        self.__spark = spark

    def get_command(self) -> str:
        return "dbx:test-connection"

    def get_description(self):
        return "Test databricks-connect connection to Databricks instance"

    def run(self, input_args: Namespace):
        self.__logger.info("Testing the connectivity")
        a = [1, 2, 3, 4]
        b = [2, 3, 4, 8]
        df = self.__spark.createDataFrame([a, b], schema=["a", "b"])
        self.__logger.info("Creating sample DataFrame")
        df.show()
        self.__logger.info("Connection successful")
