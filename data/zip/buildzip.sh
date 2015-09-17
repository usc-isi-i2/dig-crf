#!/bin/sh

cp ../../../dig-mturk/mturk/src/main/python/htmltoken.py .
zip crfprocess.zip htmltoken.py
rm htmltoken.py

cp ../../../dig-mturk/mturk/src/main/python/util.py .
zip crfprocess.zip util.py
rm util.py

cp ../../crf_features.py .
zip crfprocess.zip crf_features.py
rm crf_features.py

cp ../../../hybrid-jaccard/jaro.py .
zip crfprocess.zip jaro.py
rm jaro.py

cp ../../../hybrid-jaccard/munkres.py .
zip crfprocess.zip munkres.py
rm munkres.py

cp ../../../hybrid-jaccard/typo_tables.py .
zip crfprocess.zip typo_tables.py
rm typo_tables.py

cp ../../../hybrid-jaccard/Stringmatcher.py .
zip crfprocess.zip Stringmatcher.py
rm Stringmatcher.py

cp ../../../hybrid-jaccard/hybridJaccard.py .
zip crfprocess.zip hybridJaccard.py
rm hybridJaccard.py

cp ../../bin/crf_test .
zip crfprocess.zip crf_test
rm crf_test

cp ../../bin/crf_test_filter.sh .
zip crfprocess.zip crf_test_filter.sh
rm crf_test_filter.sh
