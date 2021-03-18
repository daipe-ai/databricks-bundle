import unittest
from databricksbundle.dbutils.DbUtilsMock import DbUtilsMock
from databricksbundle.dbutils.DbUtilsWrapper import DbUtilsWrapper


class DbUtilsWrapperTest(unittest.TestCase):
    def test_object_attribute_delegation(self):
        def create_lazy():
            return DbUtilsMock()

        db_utils_wrapper = DbUtilsWrapper(create_lazy)
        result = db_utils_wrapper.fs.ls("/")

        self.assertIsInstance(result, list)

    def test_method_with_argument(self):
        def create_lazy():
            from pyspark.dbutils import FSHandler

            return FSHandler("foo")

        db_utils_wrapper = DbUtilsWrapper(create_lazy)
        result = db_utils_wrapper.print_return(12345)

        self.assertEqual(12345, result)

    def test_delegation_non_existent_attribute(self):
        def create_lazy():
            return DbUtilsMock()

        db_utils_wrapper = DbUtilsWrapper(create_lazy)

        with self.assertRaises(AttributeError):
            db_utils_wrapper.non_existent_method()


if __name__ == "__main__":
    unittest.main()
