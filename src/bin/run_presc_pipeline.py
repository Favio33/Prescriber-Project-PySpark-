# Import all required modules
import sys

# Import variables
import variables

# Import Utils Ingestion
from src.ingestion.load_files import load_files

# Import Spark Utilities
from src.spark.SparkSetup import get_spark_object

# Logging
import logging
import logging.config

logging.config.fileConfig(fname='../configs/loggingFile.conf')


def main():

    try:

        logging.info('main() is started ...')
        spark = get_spark_object(variables.env, variables.appName)

        dfCity = load_files(spark, variables.pathStagingDimensionCity)

        # Load the Prescriber Fact File
        # Validate
        # Set Up Logging Configuration Mechanism
        # Set Up Error Handling

        # Initiate run_presc_data_processing script
        # Perform data cleaning
        # Validate
        # Set Up Logging Configuration Mechanism
        # Set Up Error Handling

        # Initiate run_presc_data_transform script
        # Apply all transformations
        # Validate
        # Set Up Logging Configuration Mechanism
        # Set Up Error Handling

        # Initiate run_data_extraction script
        # Validate
        # Set Up Logging Configuration Mechanism
        # Set Up Error Handling

        logging.info('presc_run_pipeline.py is completed!')

    except Exception as ex:
        logging.error('Error occurred in main(): ' + str(ex), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    logging.info('run_presc_pipeline.py is started!')
    main()
