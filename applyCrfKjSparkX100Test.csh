#! /bin/tcsh

set OUTDIR=data/sample/adjudicated_modeled_live_eyehair_100_03-x100-tags.spark

# Dangerous!
rm -rf ${OUTDIR}

/home/rogers/src/apache/spark/spark-1.6.1/bin/spark-submit \
    --master 'local[8]' \
    ./applyCrfKjSparkTest.py \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-x100.kjsonl \
    --output ${OUTDIR} \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --partitions 8 \
    --statistics \



