#! /bin/bash                                                                                           

INPUTFILE=/user/worker/hbase-dump-2015-10-01-2015-12-01-aman/webpage
KEY_TO_EXTRACT=description
NEW_RDD_KEY_KEY=url
OUTFILE=/user/crogers/hbase-dump-2015-10-01-2015-12-01-aman-webpage-descriptions-tokens.seq
NUM_EXECUTORS=350

source ${DIG_CRF_HOME}/checkMemexConnection.sh
${DIG_CRF_HOME}/buildPythonFiles.sh
source ${DIG_CRF_HOME}/limitMemexExecutors.sh

# Dangerous!
echo "Clearing the output folder: ${OUTFILE}"
hadoop fs -rm -r -f ${OUTFILE}

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    --py-files ${DIG_CRF_HOME}/crf_tokenizer \
    --driver-java-options -Dlog4j.configuration=file:${DIG_CRF_HOME}/data/config/quieter-log4j.properties \
    ${DIG_CRF_HOME}/src/extract/extractAndTokenizeField.py \
    -- \
    --input ${INPUTFILE} \
    --key ${KEY_TO_EXTRACT} \
    --newRddKeyKey ${NEW_RDD_KEY_KEY} \
    --prune --repartition 350 --cache \
    --count \
    --output ${OUTFILE} \
    --outputSeq
