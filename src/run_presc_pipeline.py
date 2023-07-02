# Import all required modules
import sys

# Import variables
import variables

# Import Utils Ingestion
from ingestion.staging.load_files import load_files
from ingestion.raw.preprocessing import preprocess_dimension, preprocess_fact
from ingestion.master.transformation import city_report, prescriber_report
from utils.Utilities import extract_files
from hive.data_persist_hive import data_persist_hive
from database.data_persist_pg import data_persist_pg

# Import Spark Utilities
from spark.SparkSetup import get_spark_object

# Logging
import logging
import logging.config

logging.config.fileConfig(fname='./configs/loggingFile.conf')


def main():

    try:

        logging.info('main() is started ...')
        spark = get_spark_object(variables.env, variables.appName)
        # Load Columns per DataFrame
        dfCity = load_files(spark, variables.pathStagingDimensionCity)
        dfFact = load_files(spark, variables.pathStagingFact)
        # Preprocessed DataFrames
        dfCitySelected = preprocess_dimension(dfCity)
        dfFactSelected = preprocess_fact(dfFact)
        # Transform DataFrames
        dfCityReport = city_report(dfCitySelected, dfFactSelected)
        dfPrescriberReport = prescriber_report(dfFactSelected)

        # Initiate run_data_extraction script
        extract_files(dfCityReport, 'json', variables.output_city, 1, False, 'bzip2')
        extract_files(dfPrescriberReport, 'orc', variables.output_fact, 2, False, 'snappy')

        # Persist data hive
        data_persist_hive(spark, dfCityReport, df_name='df_city_report', partition_by='delivery_date', mode='append')
        data_persist_hive(spark, dfPrescriberReport, df_name='df_prescriber_report', partition_by='delivery_date',
                          mode='append')
        # Persist data pg
        data_persist_pg(dfCityReport, 'jdbc:postgresql://localhost:6432/prescpipeline', 'org.postgresql.Driver',
                        'df_city_report', 'append', 'sparkuser1', 'pepo1612')
        data_persist_pg(dfPrescriberReport, 'jdbc:postgresql://localhost:6432/prescpipeline', 'org.postgresql.Driver',
                        'df_prescriber_report', 'append', 'sparkuser1', 'pepo1612')

        logging.info('presc_run_pipeline.py is completed!')

    except Exception as ex:
        logging.error('Error occurred in main(): ' + str(ex), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    logging.info('run_presc_pipeline.py is started!')
    main()
