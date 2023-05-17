import pyspark.sql.dataframe
from pyspark.sql import functions as f
from ..tables import dimensionsTable
import logging.config

logging.config.fileConfig('../configs/loggingFile.conf')
logger = logging.getLogger('preprocessing')


def select_columns(df: pyspark.sql.dataframe.DataFrame):

    try:
        logger.info('select_columns() object has been initialized...')
        dfSelected = df.select(*list(dimensionsTable.keys()))
        logger.info(f'{df} has been selected successfully!')
        logger.info('\n \t New Schema: ' + str(df.dtypes))
        return dfSelected
    except Exception as ex:
        logger.error(f'select_columns() method has failed. Please check the Stack Trace: {str(ex)}', exc_info=True)
        raise ex


def preprocess_dimension(df: pyspark.sql.dataframe.DataFrame):

    try:
        logger.info('preprocess_dimension() method has been initialized...')
        dfSelected = select_columns(df)
        dfPreprocessed = dfSelected.\
            withColumn('city', f.upper(f.col('city'))). \
            withColumn('state_name', f.upper(f.col('state_name'))). \
            withColumn('country_name', f.upper(f.col('county_name')))
        logger.info(f'{df} has been processed successfully!')
        return dfPreprocessed
    except Exception as ex:
        logger.error(f"preprocess_dimension() method has failed. Please check the Stack Trace: {str(ex)}", exc_info=True)
        raise ex


def preprocess_fact(df: pyspark.sql.dataframe.DataFrame):

    try:
        logger.info('preprocess_fact() method has been initialized...')
        dfSelected = select_columns(df)
    except Exception as ex:
        logger.error(f"preprocess_fact() method has failed. Please check the Stack Trace: {str(ex)}", exc_info=True)
        raise ex
