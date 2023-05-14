from pyspark.sql import SparkSession
import logging
import logging.config

# Load the Logging config file
logging.config.fileConfig('../configs/loggingFile.conf')
logger = logging.getLogger(__name__.split('.')[-1])


def get_spark_object(env: str, appname: str):

    try:
        logger.info(f'get_spark_object() is started in {env} stage')
        if env == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession.builder.master(master).appName(appname).getOrCreate()
        logger.info('SparkSession has been created!')
        validate_spark_config(spark)

        return spark

    except Exception as ex:
        logger.error('Error in get_spark_object() - SparkSession was not created: ' + str(ex), exc_info=True)
        raise ex


def validate_spark_config(spark):

    try:
        validationdf = spark.sql("select current_timestamp")
        logger.info("Spark Object set up correctly at " + str(validationdf.collect()[0][0]))
    except Exception as ex:
        logger.error('Check out get_spark_object() method: ' + str(ex), exc_info=True)
        raise ex
