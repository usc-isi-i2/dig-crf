#! /bin/bash                                                                                           

INPUTFILE=/user/worker/hbase-dump-2015-10-01-2015-12-01-aman/hbase
KEYS_TO_EXTRACT=extractions:title:results,extractions:text:results
NEW_RDD_KEY_KEY=url
OUTFILE=/user/crogers/hbase-dump-2015-10-01-2015-12-01-aman-hbase-title-and-text-tokens.seq

FOUND=`fgrep tun0: /proc/net/dev`
if  [ -n "$FOUND" ] ; then
  echo "A tunnel is present, assuming it leads to the Memex cluster."
else
  echo "No tunnel found, exiting"
  exit 1
fi

# Dangerous!
echo "Clearing the output folder: ${OUTFILE}"
hadoop fs -rm -r -f ${OUTFILE}

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 350 \
    --py-files ${DIG_CRF_HOME}/crf_tokenizer \
    --driver-java-options -Dlog4j.configuration=file:${DIG_CRF_HOME}/data/config/quieter--log4j.properties \
    ${DIG_CRF_HOME}/src/extract/extractAndTokenizeField.py \
    -- \
    --input ${INPUTFILE} \
    --key ${KEYS_TO_EXTRACT} \
    --newRddKeyKey ${NEW_RDD_KEY_KEY} \
    --prune --repartition 350 --cache \
    --skipHtmlTags \
    --count \
    --output ${OUTFILE} \
    --outputSeq
