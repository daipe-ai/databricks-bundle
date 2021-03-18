from databricksbundle.detector import is_databricks


def create_display():
    if is_databricks():
        import IPython

        return IPython.get_ipython().user_ns["display"]

    from pyspark.sql.dataframe import DataFrame

    def display_local(df: DataFrame):
        df.show()

    return display_local


display = create_display()
