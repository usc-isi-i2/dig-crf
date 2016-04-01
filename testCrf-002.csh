#! /bin/tcsh

python ./applyCrf.py \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-002.json \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model


