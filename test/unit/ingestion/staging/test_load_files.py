import unittest
import os
import sys
import pyspark.sql.dataframe
sys.path.insert(0, 'D:\\PySpark\\Projects\\PySpark Project\\src')
os.chdir('D:\\PySpark\\Projects\\PySpark Project\\src')
from ingestion.staging.load_files import load_files
from spark.SparkSetup import get_spark_object


class LoadFilesTest(unittest.TestCase):
    def test_load_files(self):
        spark = get_spark_object('TEST', 'UnitTests')
        path = os.getcwd() + '\\..\\test\\resources\\staging\\dimension_city'
        dfTest = load_files(spark, path)

        # Validate the dataframe is created correctly
        self.assertEqual(type(dfTest), pyspark.sql.dataframe.DataFrame)
        # Validate that the dataframe is not empty
        self.assertGreater(dfTest.count(), 0)


if __name__ == '__main__':
    unittest.main()
