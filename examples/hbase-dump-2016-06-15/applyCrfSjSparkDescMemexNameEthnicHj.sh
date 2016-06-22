#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

NUM_EXECUTORS=350
NUM_PARTITIONS=350

source config.sh
source ${DIG_CRF_SCRIPT}/checkMemexConnection.sh
${DIG_CRF_SCRIPT}/buildPythonFiles.sh
source ${DIG_CRF_SCRIPT}/limitMemexExecutors.sh

# Dangerous!
echo "Clearing the output folder: ${WORKING_NAME_ETHNIC_HJ_FILE}"
hadoop fs -rm -r -f ${WORKING_NAME_ETHNIC_HJ_FILE}

echo "Copying the feature control file and CRF model to Hadoop."
hadoop fs -copyFromLocal -f ${DIG_CRF_DATA_CONFIG_DIR}/${NAME_ETHNIC_FEATURES_CONFIG_FILE} \
                            ${HDFS_WORK_DIR}/${NAME_ETHNIC_FEATURES_CONFIG_FILE}
hadoop fs -copyFromLocal -f ${DIG_CRF_DATA_CONFIG_DIR}/${NAME_ETHNIC_CRF_MODEL_FILE} \
                            ${HDFS_WORK_DIR}/${NAME_ETHNIC_CRF_MODEL_FILE}

echo "Creating the Python Egg cache folder: $PYTHON_EGG_CACHE"
hadoop fs -mkdir -p $PYTHON_EGG_CACHE

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    --coalesceOutput $NUM_PARTITIONS} \
    --py-files ${DIG_EGG_FILE},${DIG_CRF_PYTHON_ZIP_FILE} \
    --conf "spark.executorEnv.PYTHON_EGG_CACHE=${PYTHON_EGG_CACHE}" \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_APPLY}/applyCrfSparkTest.py \
    -- \
    --featlist ${HDFS_WORK_DIR}/${NAME_ETHNIC_FEATURES_CONFIG_FILE} \
    --model ${HDFS_WORK_DIR}/${NAME_ETHNIC_CRF_MODEL_FILE} \
    --hybridJaccardConfig ${DIG_CRF_DATA_CONFIG_DIR}/${HYBRID_JACCARD_CONFIG_FILE} \
    --download \
    --input ${WORKING_TITLE_AND_TEXT_TOKENS_FILE} --inputSeq --justTokens \
    --output ${WORKING_NAME_ETHNIC_HJ_FILE} --outputSeq --embedKey url \
    --verbose --statistics


