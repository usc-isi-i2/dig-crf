#! /bin/bash                                                                                           

MYHOME=hdfs:///user/crogers

INPUTFILE=${MYHOME}/hbase-dump-06-15-2016-aman-crf-hair-eyes-hj.seq
NUM_EXECUTORS=50

source ${DIG_CRF_HOME}/checkMemexConnection.sh
source ${DIG_CRF_HOME}/limitMemexExecutors.sh

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    --driver-java-options -Dlog4j.configuration=file:${DIG_CRF_HOME}/data/config/quieter-log4j.properties \
    ${DIG_CRF_HOME}/src/count/countCrfResultTokensFancy.py \
    -- \
    --input ${INPUTFILE} \
    --excludeTags url
