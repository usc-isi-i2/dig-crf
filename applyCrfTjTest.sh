#! /bin/bash

python ./applyCrfTest.py --keyed --justTokens \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-justTokens.kjsonl \
    --output data/sample/adjudicated_modeled_live_eyehair_100_03-justTokens-tags.kjsonl \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --statistics



