from pyspark.dbutils import FileInfo


class FSHandlerMock:

    def ls(self, path: str): # pylint: disable = unused-argument
        return [
            FileInfo('/foo/bar', 'something.txt', 123),
            FileInfo('/foo/bar', 'something_else.txt', 789),
        ]

class DbUtilsMock:

    def __init__(self):
        self.fs = FSHandlerMock()
