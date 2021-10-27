import pandas as pd
from daipecore.pandas.dataframe.PandasDataFrameShowMethodInterface import PandasDataFrameShowMethodInterface
from databricksbundle.notebook.helpers import ipython_display


class PandasDataFrameDisplay(PandasDataFrameShowMethodInterface):
    def show(self, df: pd.DataFrame):
        ipython_display(df)
