import re


class DatabricksWidgetsLabelGenerator(object):
    def __init__(self):
        self.__name_validator = re.compile("^[a-z][a-z_0-9]+$")
        self.__widget_labels = dict()
        self.__widget_index = 1

    def generate_widget_label(self, name):
        if self.__name_validator.match(name) is None:
            raise Exception("The name you provided is incorrect, please provide name containing only alpha-numeric letters and _")

        if name not in self.__widget_labels:
            self.__widget_labels[name] = self.__widget_index
            self.__widget_index += 1

        return f"{self.__widget_labels[name]:02d}. " + name.replace("_", " ")

    def remove(self, name: str):
        del self.__widget_labels[name]

    def remove_all(self):
        self.__widget_labels = dict()
        self.__widget_index = 1
