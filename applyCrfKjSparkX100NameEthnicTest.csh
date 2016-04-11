#! /bin/tcsh

# This script assumes that "spark-submit" is available on $PATH.

set OUTDIR=data/sample/adjudicated_modeled_live_eyehair_100_03-x100-name-ethnic-tags.spark

# Dangerous!
rm -rf ${OUTDIR}

env spark-submit \
    --master 'local[8]' \
    ./applyCrfKjSparkTest.py \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-x100.kjsonl \
    --output ${OUTDIR} \
    --featlist data/config/features.name-ethnic \
    --model data/config/dig-name-ethnic-train.model \
    --download \
    --partitions 8 \
    --statistics \



