#! /bin/tcsh

# This script assumes that "spark-submit" is available on $PATH.

set OUTDIR=data/sample/adjudicated_modeled_live_eyehair_100_03-x100-tags.spark

# Dangerous!
rm -rf ${OUTDIR}

env spark-submit \
    --master 'local[8]' \
    ./applyCrfPjSparkTest.py \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-x100.kjsonl \
    --output ${OUTDIR} \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --download \
    --partitions 8 \
    --statistics
