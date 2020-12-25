import unittest

class notebookFunctionTest(unittest.TestCase):

    def test_basic(self):
        from databricksbundle.notebook.decorator.tests.notebookFunction_test import load_data

        result = load_data()

        self.assertEqual(155, result)

    def test_error(self):
        with self.assertRaises(Exception) as error:
            from databricksbundle.notebook.decorator.tests.notebookFunction_fixture import load_data

        self.assertEqual('Use @notebookFunction() instead of @notebookFunction please', str(error.exception))

if __name__ == '__main__':
    unittest.main()
