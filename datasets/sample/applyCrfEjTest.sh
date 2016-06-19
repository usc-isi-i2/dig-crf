#! /bin/bash

time python ./applyCrfTest.py --keyed --justTokens --embedKey url \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-justTokens.kjsonl \
    --output data/sample/adjudicated_modeled_live_eyehair_100_03-justTokens-tags.jsonl \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --statistics



