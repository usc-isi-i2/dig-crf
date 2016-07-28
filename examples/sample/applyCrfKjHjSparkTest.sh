#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

OUTDIR=${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-tags-hj.spark

# Build the python zip file:
../../buildPythonFiles.sh

# Dangerous!
rm -rf ${OUTDIR}

spark-submit \
    --master 'local[8]' \
    --py-files ../../pythonFiles.zip \
    ${DIG_CRF_HOME}/src/applyCrf/applyCrfSparkTest.py --keyed \
    --input ${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03.kjsonl \
    --output ${OUTDIR} \
    --featlist ${DIG_CRF_HOME}/data/config/features.hair-eye \
    --model ${DIG_CRF_HOME}/data/config/dig-hair-eye-train.model \
    --hybridJaccardConfig ${DIG_CRF_HOME}/data/config/hybrid_jaccard_config.json \
    --partitions 8 \
    --verbose --statistics
