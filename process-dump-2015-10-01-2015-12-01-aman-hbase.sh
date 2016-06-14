#! /bin/bash

# Perform preliminary examinations of the data.  These steps may
# be omitted from a production workflow.
./countRecords-dump-2015-10-01-2015-12-01-aman-hbase.sh \
| tee countRecords-dump-2015-10-01-2015-12-01-aman-hbase.log

./countGoodJson-dump-2015-10-01-2015-12-01-aman-hbase.sh \
| tee countGoodJson-dump-2015-10-01-2015-12-01-aman-hbase.log

./countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase.sh \
| tee countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase.log

# TODO: Fix this script and its program, they don't work on this dataset.
# ./countGoodKeysByPublisher-dump-2015-10-01-2015-12-01-aman-hbase.sh \
# | tee countGoodKeysByPublisher-dump-2015-10-01-2015-12-01-aman-hbase.log

./extractTitleAndTextAndSaveSeq-dump-2015-10-01-2015-12-01-aman-hbase.sh \
| tee extractTitleAndTextAndSaveSeq-dump-2015-10-01-2015-12-01-aman-hbase.log

# Perform the critical analysis steps that should be part of the
# production workflow.
#
# TODO: Choose -seq or -text as the desired output.
./tokenizeTitleAndTextAndSaveSeq-dump-2015-10-01-2015-12-01-aman-hbase.sh \
| tee tokenizeTitleAndTextAndSaveSeq-dump-2015-10-01-2015-12-01-aman-hbase.log

./applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-seq.sh \
| tee applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-seq.log

./applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-text.sh \
| tee applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-text.log

./applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-seq.sh \
| tee applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-seq.log

./applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-text.sh \
| tee applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-text.log

# Examine the output of the CRF analyses.  These steps may be omitted
# from a production workflow.
./applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-nourl.sh \
| tee applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman-hbase-nourl.log

./applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-seq.sh \
| tee applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman-hbase-seq.log

./countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.sh \
| tee countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.log

./countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.sh \
| tee countGoodKeys-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.log

./countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.sh \
| tee countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.log

./countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.sh \
| tee countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.log

./countCrfResultTokensFancy-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.sh \
| tee countCrfResultTokensFancy-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.log

./countCrfResultTokensFancy-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.sh \
| tee countCrfResultTokensFancy-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.log
