#! /bin/bash

time python ${DIG_CRF_HOME}/src/applyCrf/applyCrfTest.py --keyed \
    --input ${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03.kjsonl \
    --output ${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-tags.kjsonl \
    --featlist ${DIG_CRF_HOME}/data/config/features.hair-eye \
    --model ${DIG_CRF_HOME}/data/config/dig-hair-eye-train.model \
    --statistics



