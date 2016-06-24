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

# { time ./countGoodKeysByTarget.sh; } \
# |& tee countGoodKeysByTarget.log

# Extract title and text results, tokenize, and save.  This is a critical step
# in the production workflow.
# { time ./tokenizeTitleAndTextAndSaveSeqProd.sh; } \
# |& tee tokenizeTitleAndTextAndSaveSeqProd.log

# Apply the hair and eyes CRF extraction, with hybrid Jaccard filtering. Run
# some analyses on the results.  This is a critical step in the production
# workflow.
{ time ./applyCrfSjSparkDescMemexHairEyesHjProd.sh; } \
|& tee applyCrfSjSparkDescMemexHairEyesHjProd.log

# Apply the ethnicity CRF extraction, with hybrid Jaccard filtering. Run some
# analyses on the results.  This is a critical step in the production
# workflow.

# For the moment, workingname extraction has been disabled, but I left the
# script names the same.  To re-enable the workingname analysys, edit the
# extraction script ("applyCrfSjSparkDescMemexNameEthnicHj.sh") and change the
# file names in "config.sh".
{ time ./applyCrfSjSparkDescMemexNameEthnicHjProd.sh; } \
|& tee applyCrfSjSparkDescMemexNameEthnicHjProd.log
