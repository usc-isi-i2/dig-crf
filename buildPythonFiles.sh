#! /bin/bash

# Create a zip file of all the Python files we might use.
#
# Note: running this in parallel is not a good idea.

if [ "x${DIG_CRF_HOME}" == "x" ]
  then
    echo "Please set the DIG_CRF_HOME envar"
    exit 1
fi

rm -f ${DIG_CRF_HOME}/pythonFiles.zip
cd ${DIG_CRF_HOME}/src/applyCrf
zip -r ${DIG_CRF_HOME}/pythonFiles.zip \
    crf_features.py crf_sentences.py crf_tokenizer.py \
    applyCrf.py applyCrfSpark.py \
    hybridJaccard

