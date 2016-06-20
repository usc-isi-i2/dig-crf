#! /bin/bash                                                                                           

DUMP6FILE=/user/worker/hbase-dump-6-months-18feb/webpage
KEY_TO_EXTRACT=description
OUTFILE=/user/crogers/hbase-dump-6-months-18feb-webpage-descriptions.seq
NUM_EXECUTORS=100

${DIG_CRF_HOME}/checkMemexConnection.sh
${DIG_CRF_HOME}/buildPythonFiles.sh
source ${DIG_CRF_HOME}/limitMemexExecutors.sh

# Dangerous!
echo "Clearing the output folder: ${OUTFILE}"
hadoop fs -rm -r -f ${OUTFILE}

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    --py-files ${DIG_CRF_HOME}/pythnFiles.zip \
    ${DIG_CRF_HOME}/src/extract/extractAndTokenizeField.py \
    -- \
    --input ${DUMP6FILE} \
    --key ${KEY_TO_EXTRACT} \
    --notokenize --prune --repartition 600 --cache \
    --count \
    --output ${OUTFILE} \
    --outputSeq
