#! /bin/bash                                                                                           

DUMP6FILE=/user/worker/hbase-dump-6-months-18feb/webpage
KEY_TO_EXTRACT=description
OUTFILE=/user/crogers/hbase-dump-6-months-18feb-webpage-first1k-descriptions.kjsonl

# Dangerous!
echo "Clearing the output folder: ${OUTFILE}"
hadoop fs -rm -r -f ${OUTFILE}

echo "Submitting the job to the Memex cluster."
spark-submit \
    --master 'yarn-client' \
    --num-executors 5 \
    --py-files cmrTokenizer.py \
    ./extractAndTokenizeField.py \
    -- \
    --input ${DUMP6FILE} \
    --take 1000 \
    --key ${KEY_TO_EXTRACT} \
    --prune --repartition 5 --cache \
    --count \
    --output ${OUTFILE}


