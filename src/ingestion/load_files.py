import logging

from src.utils.Utilities import get_format_file
import logging.config

logging.config.fileConfig('../configs/loggingFile.conf')
logger = logging.getLogger('load_files')


def load_files(spark, file_path):

    logger.info('load_files() method has been initialized...')
    try:
        file_format, file_dir, header, inferSchema = get_format_file(file_path)
        if file_format == 'parquet':
            df = spark.\
                read.\
                format(file_format).\
                load(file_dir)
        elif file_format == 'csv':
            df = spark.\
                read.\
                format(file_format).\
                options(header=header).\
                options(inferSchema=inferSchema).\
                load(file_dir)
    except Exception as ex:
        logger.error('Error in method load_files(). Please check the Stack Trace: ' + str(ex), exc_info=True)
        raise ex
    else:
        logger.info(f'The input file {file_dir} is loaded to an dataframe')
        return df

