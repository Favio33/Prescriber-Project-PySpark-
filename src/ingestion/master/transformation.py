import pyspark.sql.dataframe
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from ..udfs import count_zips
from ..tables import reportPrescriber

import logging.config

logging.config.fileConfig('./configs/loggingFile.conf')
logger = logging.getLogger('transformation')


def city_report(df_city, df_fact):

    try:
        logger.info('city_transformation() function has been initialized...')
        dfCityCount = df_city.withColumn('zip_counts', count_zips(f.col('zips')))
        logger.info('UDF count_zips() executed successfully')
        dfFactGroupCity = df_fact.groupBy('presc_state', 'presc_city').agg(
            f.countDistinct('presc_id').alias('presc_counts'),
            f.sum('trx_cnt').alias('trx_counts'))
        logger.info('Successfully get counts and sum')
        dfJoinCityFact = dfCityCount.join(dfFactGroupCity, (dfCityCount.state_id == dfFactGroupCity.presc_state)\
                                          & (dfCityCount.city == dfFactGroupCity.presc_city), 'inner')
        logger.info('Successfully joined')
        dfJoinCityFactSelected = dfJoinCityFact.select("city", "state_name", "county_name", "population", "zip_counts",
                                                       "trx_counts", "presc_counts")
        logger.info('city_transformation() function has finished successfully...')
        return dfJoinCityFactSelected
    except Exception as ex:
        logger.error(f'city_transformation() function has failed! Check the Stack Trace {str(ex)}', exc_info=True)
        raise ex


def prescriber_report(df_fact):
    try:
        logger.info('prescriber_report() function has been initialized...')
        windowsSpec = Window.partitionBy('presc_state').orderBy(f.col('trx_cnt').desc())
        df_fact_selected = df_fact.select(*reportPrescriber)
        df_presc_report = df_fact_selected.\
            filter((f.col('years_of_exp') >= 20) & (f.col('years_of_exp') <= 50)).\
            withColumn('rank', f.dense_rank().over(windowsSpec)).\
            filter(f.col('rank') <= 5)
        logger.info('prescriber_report() function has finished successfully...')
        return df_presc_report
    except Exception as ex:
        logger.error(f'prescriber_report() function has failed! Check the Stack Trace {str(ex)}', exc_info=True)
        raise ex
