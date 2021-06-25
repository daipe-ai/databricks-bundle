import unittest
from databricksbundle.widgets.DatabricksWidgets import DatabricksWidgets
from databricksbundle.widgets.testing import DbUtilsTesting, TestingDbUtilsWidgets


def set_input(**kwargs):
    TestingDbUtilsWidgets.set_raw_values(kwargs)


class DatabricksWidgetsTest(unittest.TestCase):
    def setUp(self):
        self.__widgets = DatabricksWidgets(DbUtilsTesting())

    # widgets.add_text() -----------------------

    def test_text(self):
        self.__widgets.add_text("mytext")
        set_input(mytext="January, February")
        self.assertEqual("January, February", self.__widgets.get_value("mytext"))

    def test_undefined_field(self):
        self.__widgets.add_text("mytext")
        set_input(mytext="January, February")

        with self.assertRaises(Exception) as error:
            self.__widgets.get_value("undefined_field")

        self.assertEqual('No widget defined for name "undefined_field"', str(error.exception))

    def test_text_default(self):
        self.__widgets.add_text("mytext", "my_default_value")
        set_input()
        self.assertEqual("my_default_value", self.__widgets.get_value("mytext"))

    def test_text_default_empty_string(self):
        self.__widgets.add_text("mytext", "")
        set_input()
        self.assertEqual("", self.__widgets.get_value("mytext"))

    def test_text_default_none(self):
        self.__widgets.add_text("mytext")
        set_input()

        self.assertEqual("", self.__widgets.get_value("mytext"))

    def test_text_default_override(self):
        self.__widgets.add_text("mytext", "my_default_value")
        set_input(mytext="January, February")
        self.assertEqual("January, February", self.__widgets.get_value("mytext"))

    # widgets.add_select() -----------------------

    def test_select_with_default(self):
        self.__widgets.add_select("myselect", ["month", "year"], "year")
        set_input()
        self.assertEqual("year", self.__widgets.get_value("myselect"))

    def test_select_default_override(self):
        self.__widgets.add_select("myselect", ["month", "year"], "year")
        set_input(myselect="month")
        self.assertEqual("month", self.__widgets.get_value("myselect"))

    def test_select_no_input_default_empty_string(self):
        self.__widgets.add_select("myselect", ["month", "year", ""], "")
        set_input()
        self.assertEqual("", self.__widgets.get_value("myselect"))

    def test_select_with_none(self):
        with self.assertRaises(Exception) as error:
            self.__widgets.add_select("myselect", ["month", "year", None], "")

        self.assertEqual("Value None cannot be used as choice, use empty string instead", str(error.exception))

    def test_select_default_not_in_choices(self):
        with self.assertRaises(Exception) as error:
            self.__widgets.add_select("myselect", ["month", "year"], "")

        self.assertEqual('Default value "" not among choices', str(error.exception))

    def test_select_invalid_value(self):
        self.__widgets.add_select("myselect", ["month", "year"], "month")
        set_input(myselect="my_invalid_value")

        with self.assertRaises(Exception) as error:
            self.__widgets.get_value("myselect")

        self.assertEqual("argument --myselect: invalid choice: 'my_invalid_value' (choose from 'month', 'year')", str(error.exception))

    # widgets.add_multiselect() -----------------------

    def test_multiselect_defaults(self):
        self.__widgets.add_multiselect("mymulti", ["January", "February", "March"], ["January", "February"])
        set_input()
        self.assertEqual(["January", "February"], self.__widgets.get_value("mymulti"))

    def test_multiselect_defaults_overridden(self):
        self.__widgets.add_multiselect("mymulti", ["January", "February", "March"], ["January", "February"])
        set_input(mymulti=["January", "March"])
        self.assertEqual(["January", "March"], self.__widgets.get_value("mymulti"))

    def test_multiselect_defaults_empty(self):
        self.__widgets.add_multiselect("mymulti", ["January", "February", "March"], [])
        set_input(mymulti=["January"])
        self.assertEqual(["January"], self.__widgets.get_value("mymulti"))

    def test_multiselect_multi_values_one_invalid(self):
        self.__widgets.add_multiselect("mymulti", ["January", "February", "March"], [])
        set_input(mymulti=["January", "April"])

        with self.assertRaises(Exception) as error:
            self.__widgets.get_value("mymulti")

        self.assertEqual("argument --mymulti: invalid choice: 'April' (choose from 'January', 'February', 'March')", str(error.exception))

    def test_multiselect_single_value_invalid(self):
        self.__widgets.add_multiselect("mymulti", ["January", "February", "March"], [])
        set_input(mymulti=["April"])

        with self.assertRaises(Exception) as error:
            self.__widgets.get_value("mymulti")

        self.assertEqual(
            "argument --mymulti: invalid choice: 'April' (choose from 'January', 'February', 'March')",
            str(error.exception),
        )


if __name__ == "__main__":
    unittest.main()
