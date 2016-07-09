#! /bin/bash

NUM_EXECUTORS=350

source config.sh
source ${DIG_CRF_SCRIPT}/checkMemexConnection.sh
source ${DIG_CRF_SCRIPT}/limitMemexExecutors.sh

hadoop fs -copyFromLocal -f ${DIG_CRF_DATA_CONFIG_DIR}/${QUIETER_LOG4J_PROPERTIES_FILE} \
                            ${HDFS_WORK_DIR}/${QUIETER_LOG4J_PROPERTIES_FILE}

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --conf spark.yarn.appMasterEnv.SPARK_HOME=/usr/lib/spark \
    --conf spark.executorEnvEnv.SPARK_HOME=/usr/lib/spark \
    --master 'yarn' \
    --deploy-mode 'cluster' \
    --num-executors ${NUM_EXECUTORS} \
    --driver-java-options -Dlog4j.configuration=${HDFS_WORK_DIR}/${QUIETER_LOG4J_PROPERTIES_FILE} \
    --py-files ${ARGPARSE_PY_PATH} \
    ${DIG_CRF_COUNT}/countGoodKeysByTarget.py \
    --byUrl \
    --input ${HDFS_INPUT_DATA_DIR}
