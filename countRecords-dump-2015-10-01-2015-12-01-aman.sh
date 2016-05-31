#! /bin/bash

# This script counts the records in the dump of Web pages for the 2-month
# period of Oct and Nov of 2015.

DUMP6FILE=/user/worker/hbase-dump-2015-10-01-2015-12-01-aman/webpage

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 50 \
    ./countSeqRecords.py \
    -- \
    --input ${DUMP6FILE}
