#! /bin/bash                                                                                           

source config.sh

KEYS_TO_EXTRACT=extractions:title:results,extractions:text:results
NEW_RDD_KEY_KEY=url
NUM_EXECUTORS=350
NUM_PARTITIONS=1400

source ${DIG_CRF_SCRIPT}/checkMemexConnection.sh
${DIG_CRF_SCRIPT}/buildPythonFiles.sh
source ${DIG_CRF_SCRIPT}/limitMemexExecutors.sh

# Dangerous!
echo "Clearing the output folder: $WORKING_TITLE_AND_TEXT_TOKENS_FILE}"
hadoop fs -rm -r -f ${WORKING_TITLE_AND_TEXT_TOKENS_FILE}

echo "Submitting the job to the Memex cluster."
#    --conf "spark.executor.memory=4g" \
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    --py-files ${DIG_CRF_PYTHON_ZIP_FILE} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_EXTRACT}/extractAndTokenizeField.py \
    -- \
    --input ${HDFS_INPUT_DATA_DIR} \
    --key ${KEYS_TO_EXTRACT} \
    --newRddKeyKey ${NEW_RDD_KEY_KEY} \
    --prune --repartition ${NUM_PARTITIONS} --cache \
    --skipHtmlTags \
    --count \
    --output ${WORKING_TITLE_AND_TEXT_TOKENS_FILE} \
    --outputSeq
