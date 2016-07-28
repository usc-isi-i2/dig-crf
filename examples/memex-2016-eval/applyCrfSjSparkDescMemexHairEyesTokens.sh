#! /bin/bash

# Apply CRF for hairType and eyeColor extraction.
# Work in the working area, for later release to production.

# This script assumes that "spark-submit" is available on $PATH.

NUM_EXECUTORS=32
NUM_PARTITIONS=3200

source config.sh
${DIG_CRF_SCRIPT}/buildPythonFiles.sh

INPUTFILE=${WORKING_TITLE_AND_TEXT_TOKENS_FILE}
OUTPUTFILE=${WORKING_HAIR_EYES_TOKENS_FILE}

echo "Clearing the output folder: ${OUTPUTFILE}"
if [ "x${OUTPUTFILE}" == "x" ]
  then
    echo "OUTPUTFILE is not set, exiting"
    exit 1
fi
rm -r -f ${OUTPUTFILE}

echo "Creating the Python Egg cache folder: $PYTHON_EGG_CACHE"
mkdir -p $PYTHON_EGG_CACHE
chmod go-rwx $PYTHON_EGG_CACHE

echo "Running the job locally."
time spark-submit \
    --master 'local[32]' \
    --num-executors ${NUM_EXECUTORS} \
    --py-files ${DIG_CRF_EGG_FILE},${DIG_CRF_PYTHON_ZIP_FILE} \
    --conf "spark.executorEnv.PYTHON_EGG_CACHE=${PYTHON_EGG_CACHE}" \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_APPLY}/applyCrfSparkTest.py \
    --coalesceOutput ${NUM_PARTITIONS} \
    --featlist ${HDFS_WORK_DIR}/${HAIR_EYE_FEATURES_CONFIG_FILE} \
    --model ${HDFS_WORK_DIR}/${HAIR_EYE_CRF_MODEL_FILE} \
    --download \
    --input ${INPUTFILE} --inputTuples --justTokens \
    --output ${OUTPUTFILE} --outputTuples --embedKey _id \
    --verbose --statistics
