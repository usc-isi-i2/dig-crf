#! /bin/bash                                                                                           

DUMP6FILE=/user/worker/hbase-dump-6-months-18feb/webpage
KEY_TO_EXTRACT=description
NUM_EXECUTORS=50

${DIG_CRF_HOME}/checkMemexConnection.sh
source ${DIG_CRF_HOME}/limitMemexExecutors.sh

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    ${DIG_CRF_HOME}/src/extract/extractField.py \
    -- \
    --input ${DUMP6FILE} \
    --key ${KEY_TO_EXTRACT} \
    --count \
    --sample 10


