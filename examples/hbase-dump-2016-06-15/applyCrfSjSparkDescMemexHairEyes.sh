#! /bin/bash

# Apply CRF for hairType and eyeColor extraction.
# Work in the working area, for later release to production.

# This script assumes that "spark-submit" is available on $PATH.

NUM_EXECUTORS=350
NUM_PARTITIONS=350

source config.sh
source ${DIG_CRF_SCRIPT}/checkMemexConnection.sh
source ${DIG_CRF_SCRIPT}/limitMemexExecutors.sh
${DIG_CRF_SCRIPT}/buildPythonFiles.sh

INPUTFILE=${WORKING_TITLE_AND_TEXT_TOKENS_FILE}
OUTPUTFILE=${WORKING_HAIR_EYES_FILE}

echo "Clearing the output folder: ${OUTPUTFILE}"
if [ "x${OUTPUTFILE}" == "x" ]
  then
    echo "OUTPUTFILE is not set, exiting"
    exit 1
fi
hadoop fs -rm -r -f ${OUTPUTFILE}

echo "Copying the feature control file and CRF model to Hadoop."
hadoop fs -copyFromLocal -f ${DIG_CRF_DATA_CONFIG_DIR}/${HAIR_EYE_FEATURES_CONFIG_FILE} \
                            ${HDFS_WORK_DIR}/${HAIR_EYE_FEATURES_CONFIG_FILE}
hadoop fs -copyFromLocal -f ${DIG_CRF_DATA_CONFIG_DIR}/${HAIR_EYE_CRF_MODEL_FILE} \
                            ${HDFS_WORK_DIR}/${HAIR_EYE_CRF_MODEL_FILE}

echo "Creating the Python Egg cache folder: $PYTHON_EGG_CACHE"
hadoop fs -mkdir -p $PYTHON_EGG_CACHE

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    --py-files ${DIG_CRF_EGG_FILE},${DIG_CRF_PYTHON_ZIP_FILE} \
    --conf "spark.executorEnv.PYTHON_EGG_CACHE=${PYTHON_EGG_CACHE}" \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_APPLY}/applyCrfSparkTest.py \
    -- \
    --coalesceOutput ${NUM_PARTITIONS} \
    --featlist ${HDFS_WORK_DIR}/${HAIR_EYE_FEATURES_CONFIG_FILE} \
    --model ${HDFS_WORK_DIR}/${HAIR_EYE_CRF_MODEL_FILE} \
    --download \
    --input ${INPUTFILE} --inputSeq --justTokens \
    --output ${OUTPUTFILE} --outputSeq --embedKey url \
    --fusedPhrases \
    --cache --count \
    --verbose --statistics
