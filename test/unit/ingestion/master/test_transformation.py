import unittest
import os
import sys
import pyspark.sql.functions as f
sys.path.insert(0, 'D:\\PySpark\\Projects\\PySpark Project\\src')
os.chdir('D:\\PySpark\\Projects\\PySpark Project\\src')
from src.spark.SparkSetup import get_spark_object
from src.ingestion.staging.load_files import load_files
from src.ingestion.raw.preprocessing import preprocess_dimension, preprocess_fact
from src.ingestion.master.transformation import city_report, prescriber_report


class TestTransformation(unittest.TestCase):

    __spark = get_spark_object('TEST', 'Unit Test')

    def test_city_report(self):

        select_columns = {"city", "state_name", "county_name", "population", "zip_counts", "trx_counts", "presc_counts"}
        path_city = os.getcwd() + '\\..\\test\\resources\\staging\\dimension_city'
        path_fact = os.getcwd() + '\\..\\test\\resources\\staging\\fact'
        dfTestFact = load_files(self.__spark, path_fact)
        dfTestCity = load_files(self.__spark, path_city)
        dfTestFactSelected = preprocess_fact(dfTestFact)
        dfTestCitySelected = preprocess_dimension(dfTestCity)
        dfTestCityReport = city_report(dfTestCitySelected, dfTestFactSelected)

        # Validate that the dataframe is not empty
        self.assertGreater(dfTestCityReport.count(), 0)
        # Validate that the required column has been selected
        self.assertEqual(set(dfTestCityReport.columns), select_columns)
        # Validate that zip_counts fields is an int
        self.assertEqual(dict(dfTestCityReport.dtypes)['zip_counts'], 'int')

    def test_prescriber_report(self):

        path_fact = os.getcwd() + '\\..\\test\\resources\\staging\\fact'
        dfTestFact = load_files(self.__spark, path_fact)
        dfTestFactSelected = preprocess_fact(dfTestFact)
        dfTestPrescriberReport = prescriber_report(dfTestFactSelected)

        # Validate that the dataframe is not empty
        self.assertEqual(dfTestPrescriberReport.count(), 0)
        # Validate that filter is working correctly
        self.assertEqual(dfTestPrescriberReport.filter((f.col('years_of_exp') < 20) &
                                                       (f.col('years_of_exp') > 50)).count(), 0)
        self.assertEqual(dfTestPrescriberReport.filter(f.col('rank') > 5).count(), 0)


if __name__ == '__main__':
    unittest.main()
