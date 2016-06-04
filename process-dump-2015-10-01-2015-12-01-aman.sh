#! /bin/bash

# Perform preliminary examinations of the data.  These steps may
# be omitted from a production workflow.
countRecords-dump-2015-10-01-2015-12-01-aman.sh \
| tee countRecords-dump-2015-10-01-2015-12-01-aman.log

countGoodJson-dump-2015-10-01-2015-12-01-aman.sh \
| tee countGoodJson-dump-2015-10-01-2015-12-01-aman.log

countGoodKeys-dump-2015-10-01-2015-12-01-aman.sh \
| tee countGoodKeys-dump-2015-10-01-2015-12-01-aman.log

countGoodKeysByPublisher-dump-2015-10-01-2015-12-01-aman.sh \
| tee countGoodKeysByPublisher-dump-2015-10-01-2015-12-01-aman.log

extractDescriptionsAndSaveSeq-dump-2015-10-01-2015-12-01-aman.sh \
| tee extractDescriptionsAndSaveSeq-dump-2015-10-01-2015-12-01-aman.log

# Perform the critical analysis steps that should be part of the
# production workflow:
tokenizeDescriptionsAndSaveSeq-dump-2015-10-01-2015-12-01-aman.sh \
| tee tokenizeDescriptionsAndSaveSeq-dump-2015-10-01-2015-12-01-aman.log

applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman.sh \
| tee applyCrfSjSparkDescMemexHairEyes-dump-2015-10-01-2015-12-01-aman.log

applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman.sh \
| tee applyCrfSjSparkDescMemexNameEthnic-dump-2015-10-01-2015-12-01-aman.log

# Examine the output of the CRF analyses.  These steps may be omitted
# from a production workflow.
countGoodKeys-dump-2015-10-01-2015-12-01-aman-results-hair-eyes.sh \
| tee countGoodKeys-dump-2015-10-01-2015-12-01-aman-results-hair-eyes.log

countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-results-hair-eyes.sh \
| tee countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-results-hair-eyes.log

countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-results-name-ethnic.sh \
| tee countCrfResultTokens-dump-2015-10-01-2015-12-01-aman-results-name-ethnic.log

countGoodKeys-dump-2015-10-01-2015-12-01-aman-results-name-ethnic.sh \
| tee countGoodKeys-dump-2015-10-01-2015-12-01-aman-results-name-ethnic.log

