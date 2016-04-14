#! /bin/bash

python ./applyCrfTest.py --keyed --justTokens \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-justTokens-x100.kjsonl \
    --output data/sample/adjudicated_modeled_live_eyehair_100_03-justTokens-x100-tags.kjsonl \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --statistics
