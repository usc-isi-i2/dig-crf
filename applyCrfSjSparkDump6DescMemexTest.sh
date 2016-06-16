#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

MYHOME=hdfs:///user/crogers

INFILE=${MYHOME}/hbase-dump-6-months-18feb-webpage-descriptions.seq
FEATURES=features.hair-eye
MODEL=dig-hair-eye-train.model
OUTDIR=results-dump6-desc.seq

PYTHON_EGG_CACHE=./python-eggs
export PYTHON_EGG_CACHE

FOUND=`fgrep tun0: /proc/net/dev`
if  [ -n "$FOUND" ] ; then
  echo "A tunnel is present, assuming it leads to the Memex cluster."
else
  echo "No tunnel found, exiting"
  exit 1
fi

# Create a zip file of all the Python files.
rm -f pythonFiles.zip
zip -r pythonFiles.zip \
     crf_features.py crf_sentences.py crf_tokenizer.py \
     applyCrf.py applyCrfSpark.py \
     hybridJaccard

# Dangerous!
echo "Clearing the output folder: ${OUTDIR}"
hadoop fs -rm -r -f ${OUTDIR}

echo "Copying the feature control file and CRF model to Hadoop."
hadoop fs -copyFromLocal -f data/config/$FEATURES $MYHOME/$FEATURES
hadoop fs -copyFromLocal -f data/config/$MODEL $MYHOME/$MODEL

echo "Creating the Python Egg cache folder: $PYTHON_EGG_CACHE"
hadoop fs -mkdir -p $PYTHON_EGG_CACHE

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 270 \
    --py-files CRF++-0.58/python/dist/mecab_python-0.0.0-py2.7-linux-x86_64.egg,pythonFiles.zip \
    --conf "spark.executorEnv.PYTHON_EGG_CACHE=${PYTHON_EGG_CACHE}" \
    ./applyCrfSparkTest.py \
    -- \
    --featlist ${MYHOME}/${FEATURES} \
    --model ${MYHOME}/${MODEL} \
    --download \
    --input ${INFILE} --inputSeq --justTokens \
    --output ${MYHOME}/${OUTDIR} --outputSeq \
    --verbose


