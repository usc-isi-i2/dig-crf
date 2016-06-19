#! /bin/bash

INFILE=part-00000
FEATURES=${DIG_CRF_HOME}/data/config/features.hair-eye
MODEL=${DIG_CRF_HOME}/data/config/dig-hair-eye-train.model
OUTFILE=${INFILE}.results

python ${DIG_CRF_HOME}/src/applyCrf/applyCrfTest.py --keyed \
    --featlist ${FEATURES} \
    --model ${MODEL} \
    --input ${INFILE} --justTokens \
    --output ${OUTFILE} \
    --statistics


