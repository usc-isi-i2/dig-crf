#! /bin/bash

NUM_EXECUTORS=32

source config.sh

echo "Running the job locally."
time spark-submit \
    --master ${SPARK_MASTER_MODE} \
    --num-executors ${NUM_EXECUTORS} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_COUNT}/countCrfResultTokensFancy.py \
    --input ${WORKING_NAME_ETHNIC_HJ_FILE} \
    --inputTuples \
    --excludeTags ${RECORD_ID_PROPERTY}
