#! /bin/bash                                                                                           

DUMP6FILE=/user/worker/hbase-dump-6-months-18feb/webpage

echo "Submitting the job to the Memex cluster."
spark-submit \
    --master 'yarn-client' \
    --num-executors 50 \
    ./countGoodKeys.py \
    -- \
    --input ${DUMP6FILE}
