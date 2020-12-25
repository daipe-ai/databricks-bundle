import unittest

class EtlFunctionsTest(unittest.TestCase):

    def test_basic(self):
        from databricksbundle.notebook.decorator.tests.etl_functions_test import load_data

        result = load_data()

        self.assertEqual(155, result)

    def test_error(self):
        with self.assertRaises(Exception) as error:
            from databricksbundle.notebook.decorator.tests.etl_functions_fixture import load_data3

        self.assertEqual('Use @dataFrameLoader() instead of @dataFrameLoader please', str(error.exception))

if __name__ == '__main__':
    unittest.main()
