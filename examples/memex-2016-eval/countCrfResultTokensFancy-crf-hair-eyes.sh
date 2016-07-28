#! /bin/bash

NUM_EXECUTORS=32

source config.sh

echo "Running the job locally."
time spark-submit \
    --master 'local[32]' \
    --num-executors ${NUM_EXECUTORS} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_COUNT}/countCrfResultTokensFancy.py \
    --input ${WORKING_HAIR_EYES_FILE} \
    --inputTuples \
    --excludeTags url
