# pylint: disable = import-outside-toplevel
from databricksbundle.detector import isDatabricks

def createDisplay():
    if isDatabricks():
        import IPython  # pylint: disable = import-error
        return IPython.get_ipython().user_ns['display']

    from pyspark.sql.dataframe import DataFrame

    def displayLocal(df: DataFrame):
        df.show()

    return displayLocal

display = createDisplay()
