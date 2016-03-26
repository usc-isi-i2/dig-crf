#! /bin/tcsh

python ./applyCrfKj.py \
    --input data/sample/adjudicated_modeled_live_eyehair_100_03.kjsonl \
    --featlist data/config/features.hair-eye \
    --model data/config/dig-hair-eye-train.model \
| tee data/sample/adjudicated_modeled_live_eyehair_100_03-tags.kjsonl



