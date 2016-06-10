#! /bin/bash                                                                                           

INPUTFILE=/user/worker/hbase-dump-2015-10-01-2015-12-01-aman/hbase

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 300\
    --driver-java-options -Dlog4j.configuration=file:quieter-log4j.properties \
    ./countGoodKeysByPublisher.py \
    -- \
    --input ${INPUTFILE}
