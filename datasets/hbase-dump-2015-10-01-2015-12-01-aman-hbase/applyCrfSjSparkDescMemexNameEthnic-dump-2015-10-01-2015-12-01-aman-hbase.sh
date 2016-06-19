#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

MYHOME=hdfs:///user/crogers

INFILE=${MYHOME}/hbase-dump-2015-10-01-2015-12-01-aman-hbase-title-and-text-tokens.seq
FEATURES=features.name-ethnic
MODEL=dig-name-ethnic-train.model
OUTDIR=hbase-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.seq
NUM_EXECUTORS=350

# Use the envar MEMEX_MAX_EXECUTORS to limit the number of executors.
if [ ${NUM_EXECUTORS} -gt ${MEMEX_MAX_EXECUTORS:-${NUM_EXECUTORS}} ]
  then
    NUM_EXECUTORS=${MEMEX_MAX_EXECUTORS}
fi
echo "Requesting ${NUM_EXECUTORS} executors."

PYTHON_EGG_CACHE=./python-eggs
export PYTHON_EGG_CACHE

FOUND=`fgrep tun0: /proc/net/dev`
if  [ -n "$FOUND" ] ; then
  echo "A tunnel is present, assuming it leads to the Memex cluster."
else
  echo "No tunnel found, exiting"
  exit 1
fi

# Create a zip file of all the Python files.
rm -f ${DIG_CRF_HOME}/pythonFiles.zip
(cd ${DIG_CRF_HOME}/src/applyCrf && zip -r ${DIG_CRF_HOME}/pythonFiles.zip \
     crf_features.py crf_sentences.py crf_tokenizer.py \
     applyCrf.py applyCrfSpark.py \
     hybridJaccard)

# Dangerous!
echo "Clearing the output folder: ${OUTDIR}"
hadoop fs -rm -r -f ${OUTDIR}

echo "Copying the feature control file and CRF model to Hadoop."
hadoop fs -copyFromLocal -f ${DIG_CRF_HOME}/data/config/$FEATURES $MYHOME/$FEATURES
hadoop fs -copyFromLocal -f ${DIG_CRF_HOME}/data/config/$MODEL $MYHOME/$MODEL

echo "Creating the Python Egg cache folder: $PYTHON_EGG_CACHE"
hadoop fs -mkdir -p $PYTHON_EGG_CACHE

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    --py-files ${DIG_CRF_HOME}/CRF++-0.58/python/dist/mecab_python-0.0.0-py2.7-linux-x86_64.egg,${DIG_CRF_HOME}/pythonFiles.zip \
    --conf "spark.executorEnv.PYTHON_EGG_CACHE=${PYTHON_EGG_CACHE}" \
    --driver-java-options -Dlog4j.configuration=file:${DIG_CRF_HOME}-log4j.properties \
    ${DIG_CRF_HOME}/src/applyCrf/applyCrfSparkTest.py \
    -- \
    --featlist ${MYHOME}/${FEATURES} \
    --model ${MYHOME}/${MODEL} \
    --download \
    --input ${INFILE} --inputSeq --justTokens \
    --output ${MYHOME}/${OUTDIR} --outputSeq --embedKey url \
    --verbose


