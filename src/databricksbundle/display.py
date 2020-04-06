import IPython
from databricksbundle.detector import isDatabricks

def createDisplay():
    if isDatabricks():
        return IPython.get_ipython().user_ns['display']

    from pyspark.sql.dataframe import DataFrame # pylint: disable = import-outside-toplevel
    import pandas as pd # pylint: disable = import-outside-toplevel

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    def displayLocal(df: DataFrame):
        print(df.limit(100).toPandas())

    return displayLocal

display = createDisplay()
