import pyspark.sql.dataframe
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from ..tables import dimensionsTable, factTable
import logging.config

logging.config.fileConfig('./configs/loggingFile.conf')
logger = logging.getLogger('preprocessing')


def select_columns(df: pyspark.sql.dataframe.DataFrame, table: dict):

    try:
        logger.info('select_columns() object has been initialized...')
        dfSelected = df.select(*list(table.keys()))
        logger.info(f'{df} has been selected successfully!')
        logger.info('\n \t New Schema: ' + str(df.dtypes))
        return dfSelected
    except Exception as ex:
        logger.error(f'select_columns() method has failed. Please check the Stack Trace: {str(ex)}', exc_info=True)
        raise ex


def renamed_columns(df: pyspark.sql.dataframe.DataFrame, table: dict):

    try:
        logger.info('renamed_columns() method has been initialized...')
        oldColsName = list(table.keys())
        newColsName = [att['alias'] for att in table.values()]
        dictRenamed = dict(zip(oldColsName, newColsName))
        dfRenamed = df.select([f.col(name).alias(dictRenamed.get(name)) for name in dictRenamed])
        logger.info(f'{df} has been processed successfully!')
        logger.info('\n \t New schema' + str(dfRenamed.dtypes))
        return dfRenamed
    except Exception as ex:
        logger.error(f"renamed_columns() method has failed. Please check the Stack Trace: {str(ex)}", exc_info=True)
        raise ex


def preprocess_dimension(df: pyspark.sql.dataframe.DataFrame):

    try:
        logger.info('preprocess_dimension() method has been initialized...')
        dfSelected = select_columns(df, dimensionsTable)
        dfPreprocessed = dfSelected.\
            withColumn('city', f.upper(f.col('city'))). \
            withColumn('state_name', f.upper(f.col('state_name'))). \
            withColumn('country_name', f.upper(f.col('county_name')))
        logger.info(f'{dfPreprocessed} has been processed successfully!')
        logger.info("\n \t" + dfPreprocessed.limit(10).toPandas().to_string(index=False))
        return dfPreprocessed
    except Exception as ex:
        logger.error(f"preprocess_dimension() method has failed. Please check the Stack Trace: {str(ex)}", exc_info=True)
        raise ex


def preprocess_fact(df: pyspark.sql.dataframe.DataFrame):

    try:
        logger.info('preprocess_fact() method has been initialized...')
        dfSelected = select_columns(df, factTable)
        dfRenamed = renamed_columns(dfSelected, factTable)
        # Add column country_name
        dfFact = dfRenamed.withColumn('country_name', f.lit('USA'))
        logger.info('Column country_name added')
        # Get number of years_of_exp column
        dfFact = dfFact.withColumn('years_of_exp', f.regexp_extract('years_of_exp', r'\d+', 0).cast('int'))
        logger.info("regex extract applied to years_of_exp column")
        # Get drug full name
        dfFact = dfFact.withColumn('full_name', f.concat_ws(' ', 'presc_fname', 'presc_lname')).\
            drop('presc_fname', 'presc_lname')
        logger.info("Column full name added")
        # Drop null values
        dfFact = dfFact.dropna(subset=['presc_id', 'drug_name'])
        logger.info("Null values from presc_id and drug_name dropped")
        # Fill na values for trx_cnt
        windowsSpec = Window.partitionBy('presc_id')
        dfFact = dfFact.withColumn('trx_cnt', f.coalesce('trx_cnt', f.round(f.avg('trx_cnt').over(windowsSpec))))
        dfNullCheck = dfFact.select([f.count(f.when(f.isnan(col) | f.col(col).isNull(), col)).alias(col)\
                                     for col in dfFact.columns])
        logger.info("Null values from trx_cnt has been replaced ")
        logger.info(f'{dfFact} has been processed successfully!')
        logger.info("\n \t Checking if DF have null values per column" + dfNullCheck.limit(10).toPandas().\
                    to_string(index=False))
        logger.info("\n \t" + dfFact.limit(10).toPandas().to_string(index=False))
        return dfFact
    except Exception as ex:
        logger.error(f"preprocess_fact() method has failed. Please check the Stack Trace: {str(ex)}", exc_info=True)
        raise ex
