import re
from typing import List
from box import Box
from databricksbundle.bootstrap import bootstrap_config_reader
from consolebundle.detector import is_running_in_console
from injecta.container.ContainerInterface import ContainerInterface
from injecta.dtype.DType import DType
from injecta.package.path_resolver import resolve_path
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

    def modify_raw_config(self, raw_config: dict) -> dict:
        bootstrap_config = bootstrap_config_reader.read()

        if "daipe" in raw_config["parameters"]:
            raise Exception("parameters.daipe must not be explicitly defined")

        raw_config["parameters"]["daipe"] = {
            "root_module": {
                "name": bootstrap_config.root_module_name,
                "path": resolve_path(bootstrap_config.root_module_name).replace("\\", "/"),
            }
        }

        return raw_config

    def modify_services(self, services: List[Service], aliases: List[ServiceAlias], parameters: Box):
        if is_running_in_console():
            aliases.append(ServiceAlias("databricksbundle.logger", "consolebundle.logger"))
        else:
            service = Service("databricksbundle.logger", DType("logging", "Logger"))
            service.set_factory(ServiceArgument(NotebookLoggerFactory.__module__), "create")

            services.append(service)

        return services, aliases

    def boot(self, container: ContainerInterface):
        parameters = container.get_parameters()

        if (
            is_databricks()
            and is_notebook_environment()
            and parameters.databricksbundle.enable_notebook_error_handler is True
            and not re.match("^/Users/", get_notebook_path())
        ):
            set_notebook_error_handler()
