from abc import ABC, abstractmethod
from typing import Dict

from pyspark.dbutils import DBUtils


class TestingWidget(ABC):
    @abstractmethod
    def get_default(self):
        pass

    @abstractmethod
    def get_value(self, raw_value):
        pass


class TextField(TestingWidget):
    def __init__(self, name: str, default_value: str = None, label: str = None):
        self.__name = name
        self.__default_value = default_value
        self.__label = label

    @property
    def name(self):
        return self.__name

    @property
    def default_value(self):
        return self.__default_value

    @property
    def label(self):
        return self.__label

    def get_default(self):
        return self.__default_value

    def get_value(self, raw_value):
        return raw_value


class Dropdown(TestingWidget):
    def __init__(self, name: str, choices: list, default_value: str = None, label: str = None):
        self.__name = name
        self.__choices = choices
        self.__default_value = default_value
        self.__label = label

    @property
    def name(self):
        return self.__name

    @property
    def choices(self):
        return self.__choices

    @property
    def default_value(self):
        return self.__default_value

    @property
    def label(self):
        return self.__label

    def get_default(self):
        return self.__default_value

    def get_value(self, raw_value):
        if raw_value not in self.__choices:
            choices_str = "'" + "', '".join(self.__choices) + "'"
            raise Exception(f"argument --{self.__name}: invalid choice: '{raw_value}' (choose from {choices_str})")

        return raw_value


class Multiselect(TestingWidget):
    def __init__(self, name: str, choices: list, default_values: list = None, label: str = None):
        self.__name = name
        self.__choices = choices
        self.__default_values = default_values or []
        self.__label = label

    @property
    def name(self):
        return self.__name

    @property
    def choices(self):
        return self.__choices

    @property
    def default_values(self):
        return self.__default_values

    @property
    def label(self):
        return self.__label

    def get_default(self):
        return ",".join(self.__default_values)

    def get_value(self, raw_value):
        diff = set(raw_value) - set(self.__choices)

        if diff != set():
            choices_str = "'" + "', '".join(self.__choices) + "'"
            invalid_choices_str = "'" + "', '".join(diff) + "'"
            raise Exception(f"argument --{self.__name}: invalid choice: {invalid_choices_str} (choose from {choices_str})")

        return ",".join(raw_value)


class TestingDbUtilsWidgets:
    __fields = []
    _raw_values = dict()

    @classmethod
    def set_raw_values(cls, values: dict):
        cls._raw_values = values

    def text(self, name, default_value: str = "", label: str = None):
        if default_value is None:
            raise Exception("Default value cannot be None")

        self.__fields.append(TextField(name, default_value, label))

    def dropdown(self, name, default_value: str, choices: list, label: str = None):
        if default_value is None:
            raise Exception("Default value cannot be None")

        self.__fields.append(Dropdown(name, choices, default_value, label))

    def multiselect(self, name, default_values: list, choices: list, label: str = None):
        self.__fields.append(Multiselect(name, choices, default_values, label))

    def get(self, name):
        mapped_fields: Dict[str, TestingWidget] = {field.name: field for field in self.__fields}

        if name not in mapped_fields:
            raise Exception('No widget defined for name "undefined_field"')

        testing_widget = mapped_fields[name]

        if name not in TestingDbUtilsWidgets._raw_values:
            return testing_widget.get_default()

        return testing_widget.get_value(TestingDbUtilsWidgets._raw_values[name])


class DbUtilsTesting(DBUtils):
    def __init__(self):
        self.widgets = TestingDbUtilsWidgets()
