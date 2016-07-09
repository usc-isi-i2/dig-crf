#! /bin/bash

NUM_EXECUTORS=350

source config.sh
source ${DIG_CRF_SCRIPT}/checkMemexConnection.sh
source ${DIG_CRF_SCRIPT}/limitMemexExecutors.sh

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --conf spark.yarn.appMasterEnv.SPARK_HOME=/usr/lib/spark \
    --conf spark.executorEnvEnv.SPARK_HOME=/usr/lib/spark \
    --master 'yarn' \
    --deploy-mode 'cluster' \
    --num-executors ${NUM_EXECUTORS} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_COUNT}/countGoodKeysByTarget.py \
    -- \
    --byUrl \
    --input ${HDFS_INPUT_DATA_DIR}
