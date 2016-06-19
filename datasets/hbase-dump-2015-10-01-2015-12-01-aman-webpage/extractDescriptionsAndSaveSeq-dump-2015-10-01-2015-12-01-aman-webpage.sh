#! /bin/bash                                                                                           

INPUTFILE=/user/worker/hbase-dump-2015-10-01-2015-12-01-aman/webpage
KEY_TO_EXTRACT=description
NEW_RDD_KEY_KEY=url
OUTFILE=/user/crogers/hbase-dump-2015-10-01-2015-12-01-aman-webpage-descriptions.seq

# Dangerous!
echo "Clearing the output folder: ${OUTFILE}"
hadoop fs -rm -r -f ${OUTFILE}

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 350 \
    --py-files ${DIG_CRF_HOME}/crf_tokenizer \
    --driver-java-options -Dlog4j.configuration=file:${DIG_CRF_HOME}/quieter-log4j.properties \
    ${DIG_CRF_HOME}/src/extract/extractAndTokenizeField.py \
    -- \
    --input ${INPUTFILE} \
    --key ${KEY_TO_EXTRACT} \
    --newRddKeyKey ${NEW_RDD_KEY_KEY} \
    --notokenize --prune --repartition 350 --cache \
    --count \
    --output ${OUTFILE} \
    --outputSeq
