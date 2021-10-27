from pyspark.sql import DataFrame
from pysparkbundle.dataframe.DataFrameShowMethodInterface import DataFrameShowMethodInterface
from databricksbundle.notebook.helpers import ipython_display


class DataFrameDisplay(DataFrameShowMethodInterface):
    def show(self, df: DataFrame):
        ipython_display(df)
