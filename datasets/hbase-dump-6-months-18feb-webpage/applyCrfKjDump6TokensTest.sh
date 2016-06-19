#! /bin/bash

INFILE=part-00000
FEATURES=data/config/features.hair-eye
MODEL=data/config/dig-hair-eye-train.model
OUTFILE=${INFILE}.results

python ./applyCrfTest.py --keyed \
    --featlist ${FEATURES} \
    --model ${MODEL} \
    --input ${INFILE} --justTokens \
    --output ${OUTFILE} \
    --statistics


