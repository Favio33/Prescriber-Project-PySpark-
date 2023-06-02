import logging.config

logging.config.fileConfig('./configs/loggingFile.conf')
logger = logging.getLogger('data_persist_pg')


def data_persist_pg(df, url, driver, dbtable, mode, user, password):

    try:
        logger.info('data_persist_pg() function has been initialized...')
        df.write.format("jdbc") \
            .option("url", url) \
            .option("driver", driver) \
            .option("dbtable", dbtable) \
            .mode(mode) \
            .option("user", user) \
            .option("password", password) \
            .save()
    except Exception as ex:
        logger.error(f'data_persist_pg() function has failed. Check Stack Drive {str(ex)}', exc_info=True)
        raise ex
