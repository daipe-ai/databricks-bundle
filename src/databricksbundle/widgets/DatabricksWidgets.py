from daipecore.widgets.Widgets import Widgets
from databricksbundle.detector import is_databricks
from pyspark.dbutils import DBUtils


class DatabricksWidgets(Widgets):
    def __init__(self, dbutils: DBUtils):
        self.__dbutils = dbutils
        self.__multiselect_fields = []

    def add_text(self, name: str, default_value: str = "", label: str = None):
        self.__dbutils.widgets.text(name, default_value if default_value is not None else "", label)

    def add_select(self, name: str, choices: list, default_value: str, label: str = None):
        if None in choices:
            raise Exception("Value None cannot be used as choice, use empty string instead")

        if default_value not in choices:
            raise Exception(f'Default value "{default_value}" not among choices')

        self.__dbutils.widgets.dropdown(name, default_value if default_value is not None else "", choices, label)

    def add_multiselect(self, name: str, choices: list, default_values: list, label: str = None):
        self.__multiselect_fields.append(name)
        self.__dbutils.widgets.multiselect(name, default_values, choices, label)

    def remove(self, name: str):
        self.__dbutils.widgets.remove(name)

    def remove_all(self):
        self.__dbutils.widgets.removeAll()

    def get_value(self, name: str):
        value = self.__dbutils.widgets.get(name)

        if name in self.__multiselect_fields:
            if value == "":
                return []

            return value.split(",")

        return value

    def should_be_resolved(self):
        return is_databricks()
