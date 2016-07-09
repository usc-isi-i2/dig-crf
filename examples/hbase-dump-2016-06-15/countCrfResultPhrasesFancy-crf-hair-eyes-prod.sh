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
    ${DIG_CRF_COUNT}/countCrfResultPhrasesFancy.py \
    -- \
    --input ${PRODUCTION_HAIR_EYES_FILE} \
    --excludeTags url
