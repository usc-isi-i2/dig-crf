#! /bin/bash                                                                                           

INPUTFILE='/user/worker/hbase-dump-06-15-2016-aman/201*'
KEYS_TO_EXTRACT=extractions:title:results,extractions:text:results
NEW_RDD_KEY_KEY=url
OUTFILE=/user/crogers/hbase-dump-06-15-2016-aman-title-and-text-tokens.seq
NUM_EXECUTORS=350
NUM_PARTITIONS=1400

source ${DIG_CRF_HOME}/checkMemexConnection.sh
${DIG_CRF_HOME}/buildPythonFiles.sh
source ${DIG_CRF_HOME}/limitMemexExecutors.sh

# Dangerous!
echo "Clearing the output folder: ${OUTFILE}"
hadoop fs -rm -r -f ${OUTFILE}

echo "Submitting the job to the Memex cluster."
#    --conf "spark.executor.memory=4g" \
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    --py-files ${DIG_CRF_HOME}/pythonFiles.zip \
    --driver-java-options -Dlog4j.configuration=file:${DIG_CRF_HOME}/data/config/quieter-log4j.properties \
    ${DIG_CRF_HOME}/src/extract/extractAndTokenizeField.py \
    -- \
    --input ${INPUTFILE} \
    --key ${KEYS_TO_EXTRACT} \
    --newRddKeyKey ${NEW_RDD_KEY_KEY} \
    --prune --repartition ${NUM_PARTITIONS} --cache \
    --skipHtmlTags \
    --count \
    --output ${OUTFILE} \
    --outputSeq
