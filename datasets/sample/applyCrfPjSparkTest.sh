#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

OUTDIR=data/sample/adjudicated_modeled_live_eyehair_100_03-tags.spark

# Dangerous!
rm -rf ${OUTDIR}

time spark-submit \
    --master 'local[8]' \
    ./applyCrfSparkTest.py --pairs \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03.kjsonl \
    --output ${OUTDIR} \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --partitions 8 \
    --statistics
