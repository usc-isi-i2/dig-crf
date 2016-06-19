#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

OUTDIR=data/sample/adjudicated_modeled_live_eyehair_100_03-x100-tags.spark

# Dangerous!
rm -rf ${OUTDIR}

time spark-submit \
    --master 'local[8]' \
    ./applyCrfSparkTest.py --keyed \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-x100.kjsonl \
    --output ${OUTDIR} \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --download \
    --partitions 8 \
    --verbose --statistics
