#! /bin/bash

NUM_EXECUTORS=32

source config.sh

echo "Running the job locally."
time spark-submit \
    --master 'local[32]' \
    --num-executors ${NUM_EXECUTORS} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_COUNT}/countTextRecords.py \
    --input ${HDFS_INPUT_DATA_DIR}
