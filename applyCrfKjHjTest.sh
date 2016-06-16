#! /bin/bash

time python ./applyCrfTest.py --keyed \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03.kjsonl \
    --output data/sample/adjudicated_modeled_live_eyehair_100_03-tags-hj.kjsonl \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --hybridJaccardConfig hybrid_jaccard_config.json \
    --verbose --statistics



