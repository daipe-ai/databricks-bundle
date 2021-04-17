from pyspark.sql import DataFrame
from databricksbundle.detector import is_databricks
from pysparkbundle.dataframe.DataFrameShowMethodInterface import DataFrameShowMethodInterface


class DataFrameDisplay(DataFrameShowMethodInterface):
    def show(self, df: DataFrame):
        if is_databricks():
            import IPython

            IPython.get_ipython().user_ns["display"](df)
        else:
            df.show()
