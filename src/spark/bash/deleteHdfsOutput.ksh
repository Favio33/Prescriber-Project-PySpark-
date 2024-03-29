############################################################
# Developed By:                                            #
# Developed Date:                                          # 
# Script NAME:                                             #
# PURPOSE: Delete HDFS Output paths so that Spark 
#          extraction will be smooth.                      #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="deleteHdfsOutput.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define a Log File where logs would be generated
LOGFILE="/home/pepo_alberto_ramos33/pyspk_project/src/logs/${JOBNAME}_${date}.log"

###########################################################################
### COMMENTS: From this point on, all standard output and standard error will
###           be logged in the log file.
###########################################################################
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

CITY_PATH=PrescPipeline/output/dimension_city
hdfs dfs -test -d $CITY_PATH
status=$?
if [ $status == 0 ]
  then
  echo "The HDFS output directory $CITY_PATH is available. Proceed to delete."
  hdfs dfs -rm -r -f $CITY_PATH
  echo "The HDFS Output directory $CITY_PATH is deleted before extraction."
fi

FACT_PATH=PrescPipeline/output/presc
hdfs dfs -test -d $FACT_PATH
status=$?
if [ $status == 0 ]
  then
  echo "The HDFS output directory $FACT_PATH is available. Proceed to delete."
  hdfs dfs -rm -r -f $FACT_PATH
  echo "The HDFS Output directory $FACT_PATH is deleted before extraction."
fi

HIVE_CITY_PATH=/user/hive/warehouse/prescpipeline.db/df_city_report
hdfs dfs -test -d $HIVE_CITY_PATH
status=$?
if [ $status == 0]
    then
    echo "The HDFS output directory $HIVE_CITY_PATH is available. Proceed to delete."
    hdfs dfs -rm -r -f $HIVE_CITY_PATH
    printf "The HDFS Output directory $HIVE_CITY_PATH is deleted for Hive !!! \n\n "
fi

HIVE_FACT_PATH=/user/hive/warehouse/prescpipeline.db/df_prescriber_report
hdfs dfs -test -d $HIVE_FACT_PATH
status=$?
if [ $status == 0]
    then
    echo "The HDFS output directory $HIVE_FACT_PATH is available. Proceed to delete."
    hdfs dfs -rm -r -f $HIVE_FACT_PATH
    printf "The HDFS Output directory $HIVE_FACT_PATH is deleted for Hive !!! \n\n "
fi

echo "${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.

