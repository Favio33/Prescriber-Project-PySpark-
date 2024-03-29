### Part1 ###
### Copy source files from origin to hadoop
echo "Calling copyFilesToLocal.ksh ..."
${PROJ_FOLDER}/src/pyspk_project/src/spark/bash/copyFilesToLocal.ksh
echo "Executing copyFilesToLocal.ksh is completed.\n"

### Call below wrapper to delete HDFS Paths.
echo "Calling delete_hdfs_output_paths.ksh ..."
${PROJ_FOLDER}/src/pyspk_project/src/spark/bash/deleteHdfsOutput.ksh
echo "Executing deleteHdfsOutput.ksh is completed.\n"

### Call below Spark Job to extract Fact and City Files
echo "Calling run_presc_pipeline.py ..."
spark3-submit --master yarn --num-executors 28 --jars ${PROJ_FOLDER}/lib/postgresql-42.3.5.jar run_presc_pipeline.py
echo "Executing run_presc_pipeline.py is completed.\n"

### Part2 ###
### Call the below script to copy files from HDFS to local.
printf "Calling copytFilesToLocal.ksh ..."
${PROJ_FOLDER}/src/pyspk_project/src/spark/bash/copyFilesToLocal.ksh
printf "Executing copyFilesToLocal.ksh is completed.\n"

### Call the below script to copy files from local to S3.
printf "Calling copyFilesToS3.ksh ..."
${PROJ_FOLDER}/src/pyspk_project/src/spark/bash/copyFilesToS3.ksh
printf "Executing copyFilesToS3.ksh is completed.\n"



