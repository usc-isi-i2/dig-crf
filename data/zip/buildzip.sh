#!/bin/sh

cp ../../../dig-mturk/mturk/src/main/python/htmltoken.py .
zip driver.zip htmltoken.py
rm htmltoken.py

cp ../../../dig-mturk/mturk/src/main/python/util.py .
zip driver.zip util.py
rm util.py

cp ../../../dig-mturk/mturk/src/main/python/crf_features.py .
zip driver.zip crf_features.py
rm crf_features.py

cp ../../../dig-mturk/mturk/src/main/python/harvestspans.py .
zip driver.zip harvestspans.py
rm harvestspans.py

cp ../../../hybrid-jaccard/jaro.py .
zip driver.zip jaro.py
rm jaro.py

cp ../../../hybrid-jaccard/munkres.py .
zip driver.zip munkres.py
rm munkres.py

cp ../../../hybrid-jaccard/typo_tables.py .
zip driver.zip typo_tables.py
rm typo_tables.py

cp ../../../hybrid-jaccard/Stringmatcher.py .
zip driver.zip Stringmatcher.py
rm Stringmatcher.py

cp ../../../hybrid-jaccard/hybridJaccard.py .
zip driver.zip hybridJaccard.py
rm hybridJaccard.py

cp ../../bin/crf_test .
zip driver.zip crf_test
rm crf_test

cp ../../bin/crf_test_filter.sh .
zip driver.zip crf_test_filter.sh
rm crf_test_filter.sh

cp ../config/dig-hair-eye-train.model .
zip driver.zip dig-hair-eye-train.model
rm dig-hair-eye-train.model
