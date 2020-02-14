import unittest
from databricksbundle.dbutils.DbUtilsWrapper import DbUtilsWrapper

class DbUtilsWrapperTest(unittest.TestCase):

    def test_objectAttributeDelegation(self):
        def createLazy():
            from pyspark.dbutils import DBUtils
            return DBUtils()

        dbUtilsWrapper = DbUtilsWrapper(createLazy)
        result = dbUtilsWrapper.fs.ls('/')

        self.assertIsInstance(result, list)

    def test_scalarAttributeDelegation(self):
        def createLazy():
            from pyspark.dbutils import DBUtils
            dbUtils = DBUtils()
            dbUtils.a = 5
            return dbUtils

        dbUtilsWrapper = DbUtilsWrapper(createLazy)
        result = dbUtilsWrapper.a

        self.assertEqual(5, result)

    def test_methodWithArgument(self):
        def createLazy():
            from pyspark.dbutils import FSHandler
            return FSHandler('foo')

        dbUtilsWrapper = DbUtilsWrapper(createLazy)
        result = dbUtilsWrapper.print_return(12345)

        self.assertEqual(12345, result)

    def test_delegationNonExistentAttribute(self):
        def createLazy():
            from pyspark.dbutils import DBUtils
            return DBUtils()

        dbUtilsWrapper = DbUtilsWrapper(createLazy)

        with self.assertRaises(AttributeError):
            dbUtilsWrapper.nonExistentMethod()

if __name__ == '__main__':
    unittest.main()
