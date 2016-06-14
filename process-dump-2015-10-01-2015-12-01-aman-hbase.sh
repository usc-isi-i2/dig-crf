#! /bin/bash

# Perform preliminary examinations of the data.  These steps may
# be omitted from a production workflow.
echo ./countRecords-dump-2015-10-01-2015-12-01-aman-hbase.sh
./countRecords-dump-2015-10-01-2015-12-01-aman-hbase.sh \
| tee countRecords-dump-2015-10-01-2015-12-01-aman-hbase.log

echo ./countGoodJson-dump-2015-10-01-2015-12-01-aman-hbase.sh
./countGoodJson-dump-2015-10-01-2015-12-01-aman-hbase.sh \
| tee countGoodJson-dump-2015-10-01-2015-12-01-aman-hbase.log

echo ./countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase.sh
./countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase.sh \
| tee countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase.log

# TODO: Fix this script and its program, they don't work on this dataset.
# echo ./countGoodKeysByPublisher-dump-2015-10-01-2015-12-01-aman-hbase.sh
# ./countGoodKeysByPublisher-dump-2015-10-01-2015-12-01-aman-hbase.sh \
# | tee countGoodKeysByPublisher-dump-2015-10-01-2015-12-01-aman-hbase.log

echo ./extractTitleAndTextAndSaveSeq-dump-2015-10-01-2015-12-01-aman-hbase.sh
./extractTitleAndTextAndSaveSeq-dump-2015-10-01-2015-12-01-aman-hbase.sh \
| tee extractTitleAndTextAndSaveSeq-dump-2015-10-01-2015-12-01-aman-hbase.log

# Perform the critical analysis steps that should be part of the
# production workflow.
#
# TODO: Choose -seq or -text as the desired output.
echo ./tokenizeTitleAndTextAndSaveSeq-dump-2015-10-01-2015-12-01-aman-hbase.sh
./tokenizeTitleAndTextAndSaveSeq-dump-2015-10-01-2015-12-01-aman-hbase.sh \
| tee tokenizeTitleAndTextAndSaveSeq-dump-2015-10-01-2015-12-01-aman-hbase.log

echo ./applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-seq.sh
./applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-seq.sh \
| tee applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-seq.log

echo ./applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-text.sh
./applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-text.sh \
| tee applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-text.log

echo ./applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-seq.sh
./applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-seq.sh \
| tee applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-seq.log

echo ./applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-text.sh
./applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-text.sh \
| tee applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-text.log

# Examine the output of the CRF analyses.  These steps may be omitted
# from a production workflow.
echo ./applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-nourl.sh
./applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-nourl.sh \
| tee applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-nourl.log

echo ./applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-nourl.sh
./applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-nourl.sh \
| tee applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-nourl.log

echo ./countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.sh
./countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.sh \
| tee countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.log

echo ./countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.sh
./countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.sh \
| tee countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.log

echo ./countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.sh
./countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.sh \
| tee countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.log

echo ./countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.sh
./countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.sh \
| tee countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.log

echo ./countCrfResultTokensFancy-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.sh
./countCrfResultTokensFancy-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.sh \
| tee countCrfResultTokensFancy-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.log

echo ./countCrfResultTokensFancy-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.sh
./countCrfResultTokensFancy-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.sh \
| tee countCrfResultTokensFancy-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.log
