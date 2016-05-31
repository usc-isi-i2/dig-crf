#! /bin/bash                                                                                           

DUMP6FILE=/user/worker/hbase-dump-6-months-18feb/webpage
KEY_TO_EXTRACT=description

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 50 \
    --py-files crf_tokenizer.py \
    ./extractAndTokenizeField.py \
    -- \
    --input ${DUMP6FILE} \
    --take 10 \
    --key ${KEY_TO_EXTRACT} \
    --count \
    --show


