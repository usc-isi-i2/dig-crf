#! /bin/bash

# This script counts the records in the dump of Web pages for the 2-month
# period of Oct and Nov of 2015.

INPUTFILE=/user/worker/hbase-dump-2015-10-01-2015-12-01-aman/hbase

FOUND=`fgrep tun0: /proc/net/dev`
if  [ -n "$FOUND" ] ; then
  echo "A tunnel is present, assuming it leads to the Memex cluster."
else
  echo "No tunnel found, exiting"
  exit 1
fi

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 350 \
    --driver-java-options -Dlog4j.configuration=file:${DIG_CRF_HOME}-log4j.properties \
    ${DIG_CRF_HOME}/src/count/countSeqRecords.py \
    -- \
    --input ${INPUTFILE}
