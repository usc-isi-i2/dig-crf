#! /bin/bash                                                                                           

INPUTFILE=/user/crogers/results-hair-eyes-dump-2015-10-01-2015-12-01-aman.seq
OUTPUTFILE=results-hair-eyes-dump-2015-10-01-2015-12-01-aman-token-counts.txt

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 50 \
    --driver-java-options -Dlog4j.configuration=file:quieter-log4j.properties \
    ./countCrfResultTokens.py \
    -- \
    --input ${INPUTFILE} \
    --output ${OUTPUTFILE} \
    --printToLog
