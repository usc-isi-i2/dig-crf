#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

MYHOME=hdfs:///user/crogers

INFILE=adjudicated_modeled_live_eyehair_100_03-x100.kjsonl
FEATURES=features.hair-eye
MODEL=dig-hair-eye-train.model
OUTDIR=results-x100

PYTHON_EGG_CACHE=./python-eggs
export PYTHON_EGG_CACHE

# PYTHONPATH=/home/crogers/effect/dig-crf/dig-crf/crf/lib/python2.7/site-packages/
# export PYTHONPATH

# Dangerous!
hadoop fs -rm -r -f ${OUTDIR}
# rm -rf ${OUTDIR}

#    --input file:///home/crogers/effect/dig-crf/dig-crf/d_modeled_live_eyehair_100_03.kjsonl

hadoop fs -copyFromLocal -f data/sample/$INFILE $INFILE
hadoop fs -copyFromLocal -f data/config/$FEATURES $MYHOME/$FEATURES
hadoop fs -copyFromLocal -f data/config/$MODEL $MYHOME/$MODEL

hadoop fs -mkdir -p $PYTHON_EGG_CACHE

spark-submit \
    --master 'yarn-client' \
    --num-executors 100 \
    --py-files CRF++-0.58/python/dist/mecab_python-0.0.0-py2.7-linux-x86_64.egg,crf_features.py,crf_sentences.py,applyCrf.py \
    --conf "spark.executorEnv.PYTHON_EGG_CACHE=${PYTHON_EGG_CACHE}" \
    ./applyCrfKjSparkTest.py \
    -- \
    --input ${MYHOME}/${INFILE} \
    --output ${MYHOME}/${OUTDIR} \
    --featlist ${MYHOME}/${FEATURES} \
    --model ${MYHOME}/${MODEL} \
    --download \
    --partitions 800 \
    --statistics
