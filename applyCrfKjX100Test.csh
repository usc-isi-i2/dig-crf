#! /bin/tcsh

python ./applyCrfKjTest.py \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03-x100.kjsonl \
    --output data/sample/adjudicated_modeled_live_eyehair_100_03-x100-tags.kjsonl \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
    --statistics
