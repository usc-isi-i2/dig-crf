#! /bin/bash                                                                                           

INPUTFILE=/user/crogers/hbase-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.seq
NUM_EXECUTORS=50

source ${DIG_CRF_HOME}/checkMemexConnection.sh
source ${DIG_CRF_HOME}/limitMemexExecutors.sh

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    --driver-java-options -Dlog4j.configuration=file:${DIG_CRF_HOME}/data/config/quieter-log4j.properties \
    ${DIG_CRF_HOME}/src/count/countGoodKeys.py \
    -- \
    --input ${INPUTFILE}
