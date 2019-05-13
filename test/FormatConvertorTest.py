from sqlalchemy_drill.sadrill import DrillIdentifierPreparer,DrillDialect_sadrill
import unittest

#
class FormatConvertorTest(unittest.TestCase):

    def test_file_without_extension(self):
        expected_result = DrillIdentifierPreparer.format_drill_table("s3.root.test_table_csv")
        self.assertEqual("""`s3`.`root`.`test_table_csv`""" , expected_result)

    def test_file_with_extension(self):
        expected_result = DrillIdentifierPreparer.format_drill_table("s3.root.test_table.csv")
        self.assertEqual("""`s3`.`root`.`test_table.csv`""" , expected_result)

    def test_non_file_type_extension(self):
        expected_result = DrillIdentifierPreparer.format_drill_table("customers.orders",False)
        self.assertEqual("""`customers`.`orders`""" , expected_result)

    def test_wildcard_file_extension(self):
        expected_result = DrillIdentifierPreparer.format_drill_table("select * from namespace.schema.folder/*.csv",True)
        self.assertEqual("""`select * from namespace`.`schema`.`folder/*.csv`""",expected_result)

