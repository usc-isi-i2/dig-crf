#! /bin/bash

python ./testCrf.py \
    --input ${DIG_CRF_HOME}/data/sample/adjudicated_modeled_live_eyehair_100_03-001.json \
    --featlist ${DIG_CRF_HOME}/data/config/features.name-ethnic \
    --model ${DIG_CRF_HOME}/data/config/dig-name-ethnic-train.model \
    --debug


