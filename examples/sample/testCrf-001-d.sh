#! /bin/bash

python ./testCrf.py \
    --input ${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-001.json \
    --featlist ${DIG_CRF_HOME}/data/config/features.hair-eye \
    --model ${DIG_CRF_HOME}/data/config/dig-hair-eye-train.model \
    --debug


