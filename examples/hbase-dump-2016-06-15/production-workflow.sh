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

# Pre-extraction analysis.

{ time countGoodKeysByTarget.sh; } \
|& tee countGoodKeysByTarget.log

# Extract title and text results, tokenize, and save.  This is a critical step
# in the production workflow.
{ time tokenizeTitleAndTextAndSaveSeq.sh; } \
|& tee tokenizeTitleAndTextAndSaveSeq.log

# Apply the hair and eyes CRF extraction, with hybrid Jaccard filtering. Run
# some analyses on the results.  This is a critical step in the production
# workflow.
{ time ./applyCrfSjSparkDescMemexHairEyesHj.sh; } \
|& tee applyCrfSjSparkDescMemexHairEyesHj.log

# Analyze the results of the extraction:
{ time ./countCrfResultPhrases-crf-hair-eyes-hj.sh; } \
|& tee countCrfResultPhrases-crf-hair-eyes-hj.log

{ time ./countCrfResultPhrasesFancy-crf-hair-eyes-hj.sh; } \
|& tee countCrfResultPhrasesFancy-crf-hair-eyes-hj.log

{ time ./countCrfResultTokensFancy-crf-hair-eyes-hj.sh; } \
|& tee countCrfResultTokensFancy-crf-hair-eyes-hj.log

# Apply the ethnicity CRF extraction, with hybrid Jaccard filtering. Run some
# analyses on the results.  This is a critical step in the production
# workflow.

# For the moment, workingname extraction has been disabled, but I left the
# script names the same.  To re-enable the workingname analysys, edit the
# extraction script ("applyCrfSjSparkDescMemexNameEthnicHj.sh") and change the
# file names in "config.sh".
{ time ./applyCrfSjSparkDescMemexNameEthnicHj.sh; } \
|& tee applyCrfSjSparkDescMemexNameEthnicHj.log

# Analyze the results of the extraction:
{ time ./countCrfResultPhrases-crf-name-ethnic-hj.sh; } \
|& tee countCrfResultPhrases-crf-name-ethnic-hj.log

{ time ./countCrfResultPhrasesFancy-crf-name-ethnic-hj.sh; } \
|& tee countCrfResultPhrasesFancy-crf-name-ethnic-hj.log

{ time ./countCrfResultTokensFancy-crf-name-ethnic-hj.sh; } \
|& tee countCrfResultTokensFancy-crf-name-ethnic-hj.log
