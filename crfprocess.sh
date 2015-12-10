#!/bin/sh

if [[ "${HOSTNAME}" == *avatar* ]]
then
    CFG=data/config
    ZIP=data/zip
    SPARKSUBMIT=/opt/spark/bin/spark-submit
    $SPARKSUBMIT --py-files $ZIP/crfprocess.zip \
--files $CFG/eyeColor_config.txt,$CFG/eyeColor_reference_wiki.txt,$CFG/hairColor_config.txt,$CFG/hairColor_reference_wiki.txt,$CFG/hairTexture_config.txt,$CFG/hairTexture_reference_wiki.txt,$CFG/hairLength_config.txt,$CFG/hairLength_reference_wiki.txt \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  crfprocess.py \
  $@
else
    SPARKSUBMIT=/usr/lib/spark/bin/spark-submit
    $SPARKSUBMIT --py-files crfprocess.zip \
--files eyeColor_config.txt,eyeColor_reference_wiki.txt,hairColor_config.txt,hairColor_reference_wiki.txt,hairTexture_config.txt,hairTexture_reference_wiki.txt,hairLength_config.txt,hairLength_reference_wiki.txt,crf_test_filter.sh,crf_test \
--master yarn-client \
--driver-memory 8g \
--executor-memory 80G  --executor-cores 5 \
--num-executors 20 \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  crfprocess.py \
  $@
fi


