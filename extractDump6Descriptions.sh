#! /bin/bash                                                                                           

DUMP6FILE=/user/worker/hbase-dump-6-months-18feb/webpage
KEY_TO_EXTRACT=description

echo "Submitting the job to the Memex cluster."
spark-submit \
    --master 'yarn-client' \
    --num-executors 50 \
    ./extractField.py \
    -- \
    --input ${DUMP6FILE} \
    --key ${KEY_TO_EXTRACT} \
    --count \
    --sample 10


