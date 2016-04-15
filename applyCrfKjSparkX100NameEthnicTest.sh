#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

OUTDIR=data/sample/adjudicated_modeled_live_eyehair_100_03-x100-name-ethnic-tags.spark

# Dangerous!
rm -rf ${OUTDIR}

spark-submit \
    --master 'local[8]' \
    ./applyCrfSparkTest.py --keyed \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-x100.kjsonl \
    --output ${OUTDIR} \
    --featlist data/config/features.name-ethnic \
    --model data/config/dig-name-ethnic-train.model \
    --download \
    --partitions 8 \
    --verbose --statistics
