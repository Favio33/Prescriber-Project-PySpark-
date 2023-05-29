import os
from src import variables
import logging.config

logging.config.fileConfig('./configs/loggingFile.conf')
logger = logging.getLogger('Utilities')


def get_format_file(path: str):

    logger.info('get_format_file() method has been initialized...')
    try:
        for file in os.listdir(path):

            file_dir = path + '\\' + file
            format_file = file.split('.')[-1]
            if format_file == 'csv':
                file_format = 'csv'
                header = variables.header
                inferSchema = variables.inferSchema
                logger.info(f'File {file} found it')
                return file_format, file_dir, header, inferSchema

            elif format_file == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
                logger.info(f'File {file} found it')
                return file_format, file_dir, header, inferSchema

    except Exception as ex:
        logger.error(f'Error in get_format_file() method. Check Stack Trace: {str(ex)}', exc_info=True)
        raise ex
