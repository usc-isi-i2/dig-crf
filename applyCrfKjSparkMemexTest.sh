#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

MYHOME=hdfs:///user/crogers

INFILE=adjudicated_modeled_live_eyehair_100_03.kjsonl
FEATURES=features.hair-eye
MODEL=dig-hair-eye-train.model
OUTDIR=results

PYTHON_EGG_CACHE=./python-eggs
export PYTHON_EGG_CACHE

# Dangerous!
echo "Clearing the output folder: ${OUTDIR}"
hadoop fs -rm -r -f ${OUTDIR}

echo "Copying the input data, feature control file, and CRF model to Hadoop."
hadoop fs -copyFromLocal -f data/sample/$INFILE $INFILE
hadoop fs -copyFromLocal -f data/config/$FEATURES $MYHOME/$FEATURES
hadoop fs -copyFromLocal -f data/config/$MODEL $MYHOME/$MODEL

echo "Creating the Python Egg cache folder: $PYTHON_EGG_CACHE"
hadoop fs -mkdir -p $PYTHON_EGG_CACHE

echo "Submitting the job to the Memex cluster."
spark-submit \
    --master 'yarn-client' \
    --py-files CRF++-0.58/python/dist/mecab_python-0.0.0-py2.7-linux-x86_64.egg,crf_features.py,crf_sentences.py,cmrTokenizer.py,applyCrf.py \
    --conf "spark.executorEnv.PYTHON_EGG_CACHE=${PYTHON_EGG_CACHE}" \
    ./applyCrfSparkTest.py \
    -- \
    --keyed \
    --input ${MYHOME}/${INFILE} \
    --output ${MYHOME}/${OUTDIR} \
    --featlist ${MYHOME}/${FEATURES} \
    --model ${MYHOME}/${MODEL} \
    --download \
    --partitions 8 \
    --verbose
