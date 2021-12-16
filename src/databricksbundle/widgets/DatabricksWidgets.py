from daipecore.widgets.Widgets import Widgets
from databricksbundle.detector import is_databricks
from databricksbundle.widgets.DatabricksWidgetsLabelGenerator import DatabricksWidgetsLabelGenerator
from pyspark.dbutils import DBUtils


class DatabricksWidgets(Widgets):
    def __init__(self, dbutils: DBUtils):
        self.__dbutils = dbutils
        self.__multiselect_fields = []
        self.__label_generator = DatabricksWidgetsLabelGenerator()

    def add_text(self, name: str, default_value: str = "", label: str = None):
        default_value = default_value if default_value is not None else ""
        label = self.__label_generator.generate_widget_label(name) if label is None else label

        self.__dbutils.widgets.text(name, default_value, label)

    def add_select(self, name: str, choices: list, default_value: str, label: str = None):
        if None in choices:
            raise Exception("Value None cannot be used as choice, use empty string instead")

        if default_value not in choices:
            raise Exception(f'Default value "{default_value}" not among choices')

        label = self.__label_generator.generate_widget_label(name) if label is None else label

        self.__dbutils.widgets.dropdown(name, default_value, choices, label)

    def add_multiselect(self, name: str, choices: list, default_values: list, label: str = None):
        if None in choices:
            raise Exception("Value None cannot be used as choice, use empty string instead")

        if type(default_values) != list:
            raise Exception("You must provide a list of length 1 if you want to specify default value")

        if len(default_values) != 1:
            raise Exception("Default values must contain exactly 1 element")

        label = self.__label_generator.generate_widget_label(name) if label is None else label
        default_value = str(default_values[0])

        self.__multiselect_fields.append(name)
        self.__dbutils.widgets.multiselect(name, default_value, choices, label)

    def remove(self, name: str):
        self.__dbutils.widgets.remove(name)
        self.__label_generator.remove(name)

    def remove_all(self):
        self.__dbutils.widgets.removeAll()
        self.__label_generator.remove_all()

    def get_value(self, name: str):
        value = self.__dbutils.widgets.get(name)

        if name in self.__multiselect_fields:
            if value == "":
                return []

            return value.split(",")

        return value

    def should_be_resolved(self):
        return is_databricks()
