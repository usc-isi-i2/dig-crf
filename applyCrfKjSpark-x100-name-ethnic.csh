#! /bin/tcsh

set OUTDIR=data/sample/adjudicated_modeled_live_eyehair_100_03-x100-name-ethnic-tags.spark

# Dangerous!
rm -rf ${OUTDIR}

/home/rogers/src/apache/spark/spark-1.6.1/bin/spark-submit \
    --master 'local[8]' \
    ./applyCrfKjSpark.py \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-x100.kjsonl \
    --output ${OUTDIR} \
    --featlist data/config/features.name-ethnic \
    --model data/config/dig-name-ethnic-train.model \
    --partitions 8 \
    --statistics \



