#! /bin/bash                                                                                           

DUMP6FILE=/user/worker/hbase-dump-6-months-18feb/webpage
KEY_TO_EXTRACT=description
OUTFILE=/user/crogers/hbase-dump-6-months-18feb-webpage-descriptions-tokens.kjsonl

# Dangerous!
echo "Clearing the output folder: ${OUTFILE}"
hadoop fs -rm -r -f ${OUTFILE}

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 100 \
    --py-files ${DIG_CRF_HOME}/crf_tokenizer \
    ${DIG_CRF_HOME}/src/extract/extractAndTokenizeField.py \
    -- \
    --input ${DUMP6FILE} \
    --key ${KEY_TO_EXTRACT} \
    --prune --repartition 500 --cache \
    --count \
    --output ${OUTFILE}


