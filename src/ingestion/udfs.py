from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf


@udf(returnType=IntegerType())
def count_zips(column):
    return len(column.split(' '))
