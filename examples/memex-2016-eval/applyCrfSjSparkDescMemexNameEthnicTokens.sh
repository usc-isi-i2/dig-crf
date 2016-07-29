#! /bin/bash

# Apply CRF for workingName and ethnicityType extraction.
# Work in the working area, for later release to production.

# This script assumes that "spark-submit" is available on $PATH.

NUM_EXECUTORS=32
NUM_PARTITIONS=3200

source config.sh
${DIG_CRF_SCRIPT}/buildPythonFiles.sh

INPUTFILE=${WORKING_TITLE_AND_TEXT_TOKENS_FILE}
OUTPUTFILE=${WORKING_NAME_ETHNIC_TOKENS_FILE}

echo "Clearing the output folder: ${OUTPUTFILE}"
if [ "x${OUTPUTFILE}" == "x" ]
  then
    echo "OUTPUTFILE is not set, exiting"
    exit 1
fi
rm -r -f ${OUTPUTFILE}

echo "Creating the Python Egg cache folder: $PYTHON_EGG_CACHE"
hadoop fs -mkdir -p $PYTHON_EGG_CACHE
chmod go-rwx $PYTHON_EGG_CACHE 

echo "Running the job locally."
time spark-submit \
    --master ${SPARK_MASTER_MODE} \
    --num-executors ${NUM_EXECUTORS} \
    --py-files ${DIG_CRF_EGG_FILE},${DIG_CRF_PYTHON_ZIP_FILE} \
    --conf "spark.executorEnv.PYTHON_EGG_CACHE=${PYTHON_EGG_CACHE}" \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_APPLY}/applyCrfSparkTest.py \
    --coalesceOutput ${NUM_PARTITIONS} \
    --featlist ${DIG_CRF_DATA_CONFIG_DIR}/${NAME_ETHNIC_FEATURES_CONFIG_FILE} \
    --model ${DIG_CRF_DATA_CONFIG_DIR}/${NAME_ETHNIC_CRF_MODEL_FILE} \
    --hybridJaccardConfig ${DIG_CRF_DATA_CONFIG_DIR}/${HYBRID_JACCARD_CONFIG_FILE} \
    --tags B_ethnic:ethnicityType,I_ethnic:ethnicityType,B_workingname:,I_workingname: \
    --download \
    --input ${INPUTFILE} --inputTuples --justTokens \
    --output ${OUTPUTFILE} --outputTuples --embedKey ${RECORD_ID_PROPERTY} \
    --verbose --statistics
