#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

OUTDIR=${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-tags.spark

# Dangerous!
rm -rf ${OUTDIR}

time spark-submit \
    --master 'local[8]' \
    ${DIG_CRF_HOME}/src/applyCrf/applyCrfSparkTest.py --keyed --justTokens \
    --input ${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-justTokens.kjsonl \
    --output ${OUTDIR} \
    --featlist ${DIG_CRF_HOME}/data/config/features.hair-eye \
    --model ${DIG_CRF_HOME}/data/config/dig-hair-eye-train.model \
    --partitions 8 \
    --verbose --statistics
