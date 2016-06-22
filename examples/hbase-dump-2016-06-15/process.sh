#! /bin/bash

# (time countGoodKeysByTarget.sh) \
# | tee countGoodKeysByTarget.log

# (time tokenizeTitleAndTextAndSaveSeq.sh) \
# | tee tokenizeTitleAndTextAndSaveSeq.log

(time ./applyCrfSjSparkDescMemexHairEyesHj.sh) \
| tee applyCrfSjSparkDescMemexHairEyesHj.log

(time ./applyCrfSjSparkDescMemexNameEthnicHj.sh) \
| tee applyCrfSjSparkDescMemexNameEthnicHj.log

(time ./countCrfResultPhrases-crf-hair-eyes-hj.sh) \
| tee countCrfResultPhrases-crf-hair-eyes-hj.log

(time ./countCrfResultPhrases-crf-name-ethnic-hj.sh) \
| tee countCrfResultPhrases-crf-name-ethnic-hj.log

(time ./countCrfResultPhrasesFancy-crf-hair-eyes-hj.sh) \
| tee countCrfResultPhrasesFancy-crf-hair-eyes-hj.log

(time ./countCrfResultPhrasesFancy-crf-name-ethnic-hj.sh) \
| tee countCrfResultPhrasesFancy-crf-name-ethnic-hj.log

(time ./countCrfResultTokensFancy-crf-hair-eyes-hj.sh) \
| tee countCrfResultTokensFancy-crf-hair-eyes-hj.log

(time ./countCrfResultTokensFancy-crf-name-ethnic-hj.sh) \
| tee countCrfResultTokensFancy-crf-name-ethnic-hj.log
