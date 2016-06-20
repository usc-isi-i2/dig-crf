#! /bin/bash                                                                                           

DUMP6FILE=/user/worker/hbase-dump-6-months-18feb/webpage

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 50 \
    ${DIG_CRF_HOME}/src/count/countSeqRecords.py \
    -- \
    --input ${DUMP6FILE}
