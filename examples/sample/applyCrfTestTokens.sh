#! /bin/bash

time python ${DIG_CRF_HOME}/src/applyCrf/applyCrfTestTokens.py \
    --input ${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-justTokens.jsonl \
    --output ${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-justTokens-token-tags.jsonl \
    --featlist ${DIG_CRF_HOME}/data/config/features.hair-eye \
    --model ${DIG_CRF_HOME}/data/config/dig-hair-eye-train.model \
    --fusePhrases \
    --statistics



