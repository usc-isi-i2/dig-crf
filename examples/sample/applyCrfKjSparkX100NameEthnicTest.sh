#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

OUTDIR=${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-x100-name-ethnic-tags.spark

# Dangerous!
rm -rf ${OUTDIR}

time spark-submit \
    --master 'local[8]' \
    ${DIG_CRF_HOME}/src/applyCrf/applyCrfSparkTest.py --keyed \
    --input ${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-x100.kjsonl \
    --output ${OUTDIR} \
    --featlist ${DIG_CRF_HOME}/data/config/features.name-ethnic \
    --model ${DIG_CRF_HOME}/data/config/dig-name-ethnic-train.model \
    --download \
    --partitions 8 \
    --verbose --statistics
