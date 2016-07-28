#! /bin/bash

# Extract title and test results, tokenize, and save as a
# SequenceFile.
#
# Note the "--count" option.  This causes an input record count action
# to take place before the data extraction operations. Since the data
# extraction operations also report a total count of input records, you
# may wonder, isn't the pre-extraction record count an unnecessary
# activity that will slow down processing?
#
# Here are some elapsed times, with and without the record count.
# These runs took place using the same dataset,
# "hbase-dump-2016-06-15", with 350 executors, 1393 output partitions
# (1400 requested), and 2G of java memory per executor.
#
# With pre-extraction record count:
# 22m32
# 22m22
# phases: 1) load data and count records, 2) extract and tokenize,
# 3) save to HDFS.
#
# Without pre-extraction record count:
# 25m02
# 24m23
# phases: 1) load data, extract and tokenize, 2) save to HDFS.
#
# As you can see, performing the pre-extraction record count results
# in a roughly 10% performance improvement in processing speed.  This
# is counterintuitive. My tentative explanation is that the
# pre-extraction record count isolates dataloading activity from the
# extraction activity, resulting in hotter caches.  In other words,
# perhaps reading in the data poisons the cache used by the data
# extraction program.

source config.sh

KEYS_TO_EXTRACT=description,readability_text
NUM_EXECUTORS=32
NUM_PARTITIONS=3200

${DIG_CRF_SCRIPT}/buildPythonFiles.sh

# Dangerous!
echo "Clearing the output folder: ${WORKING_TITLE_AND_TEXT_TOKENS_FILE}"
rm -rf ${WORKING_TITLE_AND_TEXT_TOKENS_FILE}

echo "Running the job locally."
#    --conf "spark.executor.memory=4g" \
time spark-submit \
    --master 'local[32]' \
    --num-executors ${NUM_EXECUTORS} \
    --py-files ${DIG_CRF_PYTHON_ZIP_FILE} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_EXTRACT}/extractAndTokenizeField.py \
    --input ${HDFS_INPUT_DATA_DIR} \
    --inputTuples \
    --key ${KEYS_TO_EXTRACT} \
    --skipHtmlTags \
    --prune --repartition ${NUM_PARTITIONS} \
    --output ${WORKING_TITLE_AND_TEXT_TOKENS_FILE} \
    --outputTuples
