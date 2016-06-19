#! /bin/bash

time python ${DIG_CRF_HOME}/src/extract/extractFeatures.py \
    --input ${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-001.json \
    --featlist ${DIG_CRF_HOME}/data/config/features.hair-eye

