#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.  The
# envar MEMEX_MAX_EXECUTORS may be used to limit the number of
# executors used in this script.

MYHOME=hdfs:///user/crogers

INFILE=${MYHOME}/hbase-dump-2015-10-01-2015-12-01-aman-hbase-title-and-text-tokens.seq
FEATURES=features.name-ethnic
MODEL=dig-name-ethnic-train.model
HYBRID_JACCARD=${DIG_CRF_HOME}/data/config/hybrid_jaccard_config.json
OUTDIR=hbase-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic-hj.seq
NUM_EXECUTORS=350

source ${DIG_CRF_HOME}/checkMemexConnection.sh
${DIG_CRF_HOME}/buildPythonFiles.sh
source ${DIG_CRF_HOME}/limitMemexExecutors.sh

PYTHON_EGG_CACHE=./python-eggs
export PYTHON_EGG_CACHE

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
    --driver-java-options -Dlog4j.configuration=file:${DIG_CRF_HOME}/data/config/quieter-log4j.properties \
    ${DIG_CRF_HOME}/src/applyCrf/applyCrfSparkTest.py \
    -- \
    --featlist ${MYHOME}/${FEATURES} \
    --model ${MYHOME}/${MODEL} \
    --hybridJaccardConfig ${HYBRID_JACCARD} \
    --tags B_ethnic:ethnicityType,I_ethnic:ethnicityType,B_workingname:workingname,I_workingname:workingname \
    --download \
    --input ${INFILE} --inputSeq --justTokens \
    --output ${MYHOME}/${OUTDIR} --outputSeq --embedKey url \
    --verbose --statistics


