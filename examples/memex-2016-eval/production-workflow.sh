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

# The next five steps are the critical steps in the workflow:

# Extract title and text results, tokenize, and save.
{ time ./tokenizeTitleAndTextAndSaveSeq.sh; } \
|& tee tokenizeTitleAndTextAndSaveSeq.log

# Apply the hair and eyes CRF extraction without hybrid Jaccard filtering,
# but with the output phrases fused for Karma.
{ time ./applyCrfSjSparkDescMemexHairEyes.sh; } \
|& tee applyCrfSjSparkDescMemexHairEyes.log

# Apply the hair and eyes CRF extraction with hybrid Jaccard filtering and
# with the output phrases fused for Karma.
{ time ./applyCrfSjSparkDescMemexHairEyesHj.sh; } \
|& tee applyCrfSjSparkDescMemexHairEyesHj.log

# Apply the ethnicity CRF extraction without hybrid Jaccard filtering,
# but with the result phrases fused.

# For the moment, workingname extraction has been disabled, but I left the
# script names the same.  To re-enable the workingname analysys, edit the
# extraction script ("applyCrfSjSparkDescMemexNameEthnic.sh") and change the
# file names in "config.sh".
{ time ./applyCrfSjSparkDescMemexNameEthnic.sh; } \
|& tee applyCrfSjSparkDescMemexNameEthnic.log

# Apply the ethnicity CRF extraction with hybrid Jaccard filtering and with
# the output phrases fused for Karma.

# For the moment, workingname extraction has been disabled, but I left the
# script names the same.  To re-enable the workingname analysys, edit the
# extraction script ("applyCrfSjSparkDescMemexNameEthnicHj.sh") and change the
# file names in "config.sh".
{ time ./applyCrfSjSparkDescMemexNameEthnicHj.sh; } \
|& tee applyCrfSjSparkDescMemexNameEthnicHj.log

# Analyze the results of the hair and eyes extractions without hybrid Jaccard
# processing:
{ time ./countCrfResultTokens-crf-hair-eyes.sh; } \
|& tee countCrfResultTokens-crf-hair-eyes.log

{ time ./countCrfResultTokensFancy-crf-hair-eyes.sh; } \
|& tee countCrfResultTokensFancy-crf-hair-eyes.log

# Analyze the results of the hair and eyes extractions with hybrid Jaccard
# processing:
{ time ./countCrfResultTokens-crf-hair-eyes-hj.sh; } \
|& tee countCrfResultTokens-crf-hair-eyes-hj.log

{ time ./countCrfResultTokensFancy-crf-hair-eyes-hj.sh; } \
|& tee countCrfResultTokensFancy-crf-hair-eyes-hj.log

# Analyze the results of the name and ethnicity extraction without hybrid Jaccard
# processing:

{ time ./countCrfResultTokens-crf-name-ethnic.sh; } \
|& tee countCrfResultTokens-crf-name-ethnic.log

{ time ./countCrfResultTokensFancy-crf-name-ethnic.sh; } \
|& tee countCrfResultTokensFancy-crf-name-ethnic.log

# Analyze the results of the name and ethnicity extraction with hybrid Jaccard
# processing:
{ time ./countCrfResultTokens-crf-name-ethnic-hj.sh; } \
|& tee countCrfResultTokens-crf-name-ethnic-hj.log

{ time ./countCrfResultTokensFancy-crf-name-ethnic-hj.sh; } \
|& tee countCrfResultTokensFancy-crf-name-ethnic-hj.log
