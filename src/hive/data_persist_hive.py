import datetime as date
from pyspark.sql.functions import lit

import logging.config

logging.config.fileConfig('./configs/loggingFile.conf')
logger = logging.getLogger('data_persist_hive')


def data_persist_hive(spark, df, df_name, partition_by, mode):

    try:
        logger.info('data_persist_hive() function has been initialized...')
        df = df.withColumn(partition_by, lit(date.datetime.now().strftime("%Y-%m-%d")))
        spark.sql("""create database if not exists prescpipeline location 
        'hdfs://localhost:9000/user/hive/warehouse/prescpipeline.db'""")
        spark.sql('use prescpipeline')
        df.write.saveAsTable(df_name, mode=mode, partitionBy=partition_by)

    except Exception as ex:
        logger.error(f'data_persist_hive() function has failed. Check Stack Drive {str(ex)}', exc_info=True)
        raise ex
