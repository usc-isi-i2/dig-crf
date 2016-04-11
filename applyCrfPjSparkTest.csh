#! /bin/tcsh

# This script assumes that "spark-submit" is available on $PATH.

set OUTDIR=data/sample/adjudicated_modeled_live_eyehair_100_03-tags.spark

# Dangerous!
rm -rf ${OUTDIR}

spark-submit \
    --master 'local[8]' \
    ./applyCrfPjSparkTest.py \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03.kjsonl \
    --output ${OUTDIR} \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --partitions 8 \
    --statistics
