#! /bin/bash

NUM_EXECUTORS=32

source config.sh

echo "Running the job locally."
time spark-submit \
    --master ${SPARK_MASTER_MODE} \
    --num-executors ${NUM_EXECUTORS} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_COUNT}/countCrfResultTokens.py \
    --input ${WORKING_HAIR_EYES_FILE} \
    --inputTuples \
    --printToLog \
    --excludeTags ${RECORD_ID_PROPERTY}
