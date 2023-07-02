import unittest
import os
import sys
import pyspark.sql.functions as f
sys.path.insert(0, 'D:\\PySpark\\Projects\\PySpark Project\\src')
os.chdir('D:\\PySpark\\Projects\\PySpark Project\\src')
from src.spark.SparkSetup import get_spark_object
from src.ingestion.staging.load_files import load_files
from src.ingestion.raw.preprocessing import select_columns, renamed_columns, preprocess_dimension, preprocess_fact


class TestPreprocessing(unittest.TestCase):

    __spark = get_spark_object('TEST', 'Unit Test')

    def test_select_columns(self):

        columns_dict = {
            'city': {
                'type': str
            },
            'state_id': {
                'type': str
            },
            'zips': {
                'type': int
            }
        }
        path = os.getcwd() + '\\..\\test\\resources\\staging\\dimension_city'
        dfTest = load_files(self.__spark, path)
        dfTestSelected = select_columns(dfTest, columns_dict)

        # Validate the dataframe has the columns required
        self.assertEqual(set(dfTestSelected.columns), set(columns_dict.keys()))
        # Validate that the dataframe is not empty
        self.assertGreater(dfTestSelected.count(), 0)

    def test_renamed_columns(self):

        columns_dict = {
            'city': {
                'type': str,
                'alias': 'new_city'
            },
            'state_id': {
                'type': str,
                'alias': 'new_state'
            },
            'zips': {
                'type': int,
                'alias': 'new_alias'
            }
        }
        new_columns = set(att['alias'] for att in columns_dict.values())
        path = os.getcwd() + '\\..\\test\\resources\\staging\\dimension_city'
        dfTest = load_files(self.__spark, path)
        dfTestSelected = select_columns(dfTest, columns_dict)
        dfTestRenamed = renamed_columns(dfTestSelected, columns_dict)

        # Validate that columns have been renamed correctly
        self.assertEqual(set(dfTestRenamed.columns), new_columns)
        # Validate that the dataframe is not empty
        self.assertGreater(dfTestRenamed.count(), 0)

    def test_preprocess_dimension(self):

        path = os.getcwd() + '\\..\\test\\resources\\staging\\dimension_city'
        dfTest = load_files(self.__spark, path)
        dfTestDimensionPreprocessed = preprocess_dimension(dfTest)

        # Validate that the dataframe is not empty
        self.assertGreater(dfTestDimensionPreprocessed.count(), 0)

    def test_preprocess_fact(self):

        path = os.getcwd() + '\\..\\test\\resources\\staging\\fact'
        dfTestFact = load_files(self.__spark, path)
        dfTestFactPreprocessed = preprocess_fact(dfTestFact)

        # Validate that the dataframe is not empty
        self.assertGreater(dfTestFactPreprocessed.count(), 0)
        # Validate that there are not null values in certain columns
        self.assertEqual(dfTestFactPreprocessed.where(f.col('presc_id').isNull() | f.isnan('presc_id')).count(), 0)
        self.assertEqual(dfTestFactPreprocessed.where(f.col('presc_id').isNull() | f.isnan('trx_cnt')).count(), 0)


if __name__ == '__main__':
    unittest.main()
