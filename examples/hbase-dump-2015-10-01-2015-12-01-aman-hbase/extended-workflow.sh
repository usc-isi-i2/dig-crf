#! /bin/bash

# |& is available in bash version 4.  It copies both STDERR and STDOUT to a
# pipe, as |& does in csh.
#
# The advantages of including STDERR in the logs are:
# 1) it captures error messages for later analysis
# 2) it captures execution time reports from bash
#
# The disadvantage is:
# 1) it captures the Spark completion bars.

# Extended pre-extraction analysis.  These steps have been subsumed by
# "countGoodKeysByTarget.sh", but I'm keeping them around in case
# "countGoodKeysByTarget.sh" fails.
#
{ time ./countRecords.sh; } \
|& tee countRecords.log

{ time ./countGoodJson.sh; } \
|& tee countGoodJson.log

{ time ./countGoodKeys.sh; } \
|& tee countGoodKeys.log

# Extractions and analyses without hybrid Jaccard processing.
{ time ./applyCrfSjSparkDescMemexHairEyes.sh; } \
|& tee applyCrfSjSparkDescMemexHairEyes.log

# Analyze the results of the extraction:
{ time ./countCrfResultPhrases-crf-hair-eyes.sh; } \
|& tee countCrfResultPhrases-crf-hair-eyes.log

{ time ./countCrfResultPhrasesFancy-crf-hair-eyes.sh; } \
|& tee countCrfResultPhrasesFancy-crf-hair-eyes.log

{ time ./countCrfResultTokensFancy-crf-hair-eyes.sh; } \
|& tee countCrfResultTokensFancy-crf-hair-eyes.log

# Extractions and analyses without hybrid Jaccard processing.
{ time ./applyCrfSjSparkDescMemexNameEthnic.sh; } \
|& tee applyCrfSjSparkDescMemexNameEthnic.log

# Analyze the results of the extraction:
{ time ./countCrfResultPhrases-crf-name-ethnic.sh; } \
|& tee countCrfResultPhrases-crf-name-ethnic.log

{ time ./countCrfResultPhrasesFancy-crf-name-ethnic.sh; } \
|& tee countCrfResultPhrasesFancy-crf-name-ethnic.log

{ time ./countCrfResultTokensFancy-crf-name-ethnic.sh; } \
|& tee countCrfResultTokensFancy-crf-name-ethnic.log
