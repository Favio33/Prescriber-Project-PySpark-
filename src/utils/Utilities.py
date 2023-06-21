import os
import variables
from subprocess import Popen, PIPE
import logging.config

logging.config.fileConfig('./configs/loggingFile.conf')
logger = logging.getLogger('Utilities')


def get_format_file(path: str):

    logger.info('get_format_file() method has been initialized...')
    try:
        if variables.env == "TEST":
            logger.info(f"Working in {variables.env} environment!!")
            list_files = os.listdir(path)
        elif variables.env == "PROD":
            logger.info(f"Working in {variables.env} environment!!")
            hdfs_subprocess = Popen(['hdfs', 'dfs', '-ls', '-C', path], stdout = PIPE, stderr = PIPE)
            out, err = hdfs_subprocess.communicate()
            list_files = out.decode("utf-8").split('\n')[:-1]
        else:
            logger.error(f"Work environment is not valid: {variables.env}")
            raise f"Work environment is not valid: {variables.env}"

        for file in list_files:
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


def extract_files(df, file_format, filepath, split_no, header_req, compression_type):
    try:
        logger.info(f"extract_files() has been initialized...")
        df.coalesce(split_no).\
            write.\
            mode('overwrite').\
            format(file_format).\
            save(filepath, header=header_req, compressionType=compression_type)
        logger.info(f"extract_files() finalized successfully!!\n")
    except Exception as ex:
        logger.error(f"extract_files() function has failed. Check Stack Trace: {str(ex)}", exc_info=True)
        raise ex
