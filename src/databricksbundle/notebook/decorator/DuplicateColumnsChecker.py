import collections
from logging import Logger
from typing import Tuple
from pyspark.sql.dataframe import DataFrame
from databricksbundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator

class DuplicateColumnsChecker:

    def __init__(
        self,
        logger: Logger,
    ):
        self.__logger = logger

    def check(self, df: DataFrame, resultDecorators: Tuple[DataFrameReturningDecorator]):
        fieldNames = [field.name.lower() for field in df.schema.fields]
        duplicateFields = dict()

        for fieldName, count in collections.Counter(fieldNames).items():
            if count > 1:
                duplicateFields[fieldName] = []

        if duplicateFields == dict():
            return

        fields2Tables = dict()

        for resultDecorator in resultDecorators:
            sourceDf = resultDecorator.result
            for field in sourceDf.schema.fields:
                fieldName = field.name.lower()

                if fieldName not in fields2Tables:
                    fields2Tables[fieldName] = []

                fields2Tables[fieldName].append(resultDecorator.function.__name__)

        for duplicateField in duplicateFields:
            self.__logger.error(f'Duplicate field {duplicateField}', extra={'source_dataframes': fields2Tables[duplicateField]})

        fieldsString = ', '.join(duplicateFields)
        raise Exception(f'Duplicate output column(s): {fieldsString}. Disable by setting @transformation(checkDuplicateColumns=False)')
