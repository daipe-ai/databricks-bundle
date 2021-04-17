from pyspark.dbutils import DBUtils
from pysparkbundle.filesystem.FilesystemInterface import FilesystemInterface


class DbUtilsFilesystem(FilesystemInterface):
    def __init__(
        self,
        dbutils: DBUtils,
    ):
        self.__dbutils = dbutils

    def exists(self, path: str):
        try:
            self.__dbutils.fs.head(path)

            return True
        except Exception as e:
            if "Cannot head a directory:" in str(e):
                return True

            if "java.io.FileNotFoundException" in str(e):
                return False

            raise

    def put(self, path: str, content: str, overwrite: bool = False):
        self.__dbutils.fs.put(path, content, overwrite)

    def makedirs(self, path: str):
        self.__dbutils.fs.mkdirs(path)

    def copy(self, source: str, destination: str, recursive: bool = False):
        self.__dbutils.fs.cp(source, destination, recursive)

    def move(self, source: str, destination: str, recursive: bool = False):
        self.__dbutils.fs.mv(source, destination, recursive)

    def delete(self, path: str, recursive: bool = False):
        self.__dbutils.fs.rm(path, recursive)
