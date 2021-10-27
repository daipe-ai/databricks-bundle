import re
from typing import List
from box import Box
from pyspark.sql.session import SparkSession
from consolebundle.detector import is_running_in_console
from injecta.container.ContainerInterface import ContainerInterface
from injecta.dtype.DType import DType
from injecta.service.Service import Service
from injecta.service.ServiceAlias import ServiceAlias
from injecta.service.argument.ServiceArgument import ServiceArgument
from pyfonybundles.Bundle import Bundle
from databricksbundle.notebook.NotebookErrorHandler import set_notebook_error_handler
from databricksbundle.detector import is_databricks
from databricksbundle.notebook.helpers import get_notebook_path, is_notebook_environment
from databricksbundle.notebook.logger.NotebookLoggerFactory import NotebookLoggerFactory


class DatabricksBundle(Bundle):

    DATABRICKS_NOTEBOOK = "databricks_notebook.yaml"
    DATABRICKS_SCRIPT = "databricks_script.yaml"
    DATABRICKS_CONNECT = "databricks_connect.yaml"

    @staticmethod
    def autodetect():
        if is_databricks():
            if is_notebook_environment():
                return DatabricksBundle(DatabricksBundle.DATABRICKS_NOTEBOOK)

            return DatabricksBundle(DatabricksBundle.DATABRICKS_SCRIPT)

        return DatabricksBundle(DatabricksBundle.DATABRICKS_CONNECT)

    def __init__(self, databricks_config: str):
        self.__databricks_config = databricks_config

    def get_config_files(self):
        return ["config.yaml", "databricks/" + self.__databricks_config]

    def modify_services(self, services: List[Service], aliases: List[ServiceAlias], parameters: Box):
        if is_running_in_console():
            aliases.append(ServiceAlias("databricksbundle.logger", "consolebundle.logger"))
        else:
            service = Service("databricksbundle.logger", DType("logging", "Logger"))
            service.set_factory(ServiceArgument(NotebookLoggerFactory.__module__), "create")

            services.append(service)

        return services, aliases

    def modify_parameters(self, parameters: Box) -> Box:
        if parameters.daipecore.logger.type == "default":
            parameters.daipecore.logger.type = "databricks"

        if is_databricks():
            parameters.pysparkbundle.dataframe.show_method = "databricks_display"
            parameters.daipecore.pandas.dataframe.show_method = "databricks_display"

        if parameters.pysparkbundle.filesystem is not None:
            raise Exception(
                "pysparkbundle.filesystem parameter must not be explicitly set as dbutils.fs must be used for Databricks-based projects"
            )

        parameters.pysparkbundle.filesystem = "dbutils.fs"

        return parameters

    def boot(self, container: ContainerInterface):
        parameters = container.get_parameters()

        if (
            is_databricks()
            and is_notebook_environment()
            and parameters.databricksbundle.enable_notebook_error_handler is True
            and not re.match("^/Users/", get_notebook_path())
        ):
            logger = container.get("databricksbundle.logger")

            set_notebook_error_handler(logger)

            multiple_results_enabled = "spark.databricks.workspace.multipleResults.enabled"

            spark = container.get(SparkSession)

            if spark.conf.get(multiple_results_enabled) == "false":
                logger.warning(f"{multiple_results_enabled} is set to false!")
                logger.warning("Error messages will not show properly!")
