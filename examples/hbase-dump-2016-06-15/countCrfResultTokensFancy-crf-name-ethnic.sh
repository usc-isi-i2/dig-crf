#! /bin/bash

NUM_EXECUTORS=50

source config.sh
source ${DIG_CRF_SCRIPT}/checkMemexConnection.sh
source ${DIG_CRF_SCRIPT}/limitMemexExecutors.sh

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_COUNT}/countCrfResultTokensFancy.py \
    -- \
    --input ${WORKING_NAME_ETHNIC_TOKENS_FILE} \
    --excludeTags url
