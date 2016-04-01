#! /bin/tcsh

python ./testCrf.py \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-001.json \
    --featlist data/config/features.name-ethnic \
    --model data/config/dig-name-ethnic-train.model \
    --debug


