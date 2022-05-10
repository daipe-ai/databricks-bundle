from pyspark.dbutils import FileInfo


class FSHandlerMock:
    # pylint: disable=invalid-name,unused-argument
    def ls(self, path: str):
        return [
            FileInfo("/foo/bar", "something.txt", 123),
            FileInfo("/foo/bar", "something_else.txt", 789),
        ]


class DbUtilsMock:
    def __init__(self):
        # pylint: disable=invalid-name
        self.fs = FSHandlerMock()
