#! /bin/bash

python ./applyCrfPjTest.py \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03.kjsonl \
    --output data/sample/adjudicated_modeled_live_eyehair_100_03-tags.kjsonl \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --statistics



