import os
import sys
os.chdir('D:\\PySpark\\Projects\\PySpark Project\\src')
sys.path.insert(0, 'D:\\PySpark\\Projects\\PySpark Project\\src')
import unittest
from utils.Utilities import get_format_file


class UtilitiesTest(unittest.TestCase):

    def test_get_format_file(self):
        path = os.getcwd() + "\\..\\test\\resources\\staging\\dimension_city"
        file_format, file_dir, header, inferSchema = get_format_file(path)

        # Validate file_format is a string
        self.assertIsInstance(file_format, str)
        # Validate file_dir is a string
        self.assertIsInstance(file_dir, str)
        # Validate header between possible values
        self.assertTrue(header in ('True', 'NA'))
        # Validate inferSchema between possible values
        self.assertTrue(header in ('True', 'NA'))


if __name__ == '__main__':
    unittest.main()
