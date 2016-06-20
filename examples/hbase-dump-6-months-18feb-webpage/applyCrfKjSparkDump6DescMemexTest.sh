#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

MYHOME=hdfs:///user/crogers

INFILE=${MYHOME}/hbase-dump-6-months-18feb-webpage-descriptions.kjsonl
FEATURES=features.hair-eye
MODEL=dig-hair-eye-train.model
OUTDIR=results-dump6-desc.kjsonl

NUM_EXECUTORS=270

# Use the envar MEMEX_MAX_EXECUTORS to limit the number of executors.
if [ ${NUM_EXECUTORS} -gt ${MEMEX_MAX_EXECUTORS:-${NUM_EXECUTORS}} ]
  then
    NUM_EXECUTORS=${MEMEX_MAX_EXECUTORS}
fi
echo "Requesting ${NUM_EXECUTORS} executors."


PYTHON_EGG_CACHE=./python-eggs
export PYTHON_EGG_CACHE

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
    ${DIG_CRF_HOME}/src/applyCrf/applyCrfSparkTest.py \
    -- \
    --keyed \
    --featlist ${MYHOME}/${FEATURES} \
    --model ${MYHOME}/${MODEL} \
    --download \
    --input ${INFILE} \
    --justTokens \
    --output ${MYHOME}/${OUTDIR} \
    --verbose


