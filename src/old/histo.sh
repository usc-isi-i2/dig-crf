#!/bin/sh

if [[ "${HOSTNAME}" == *avatar* ]]
then
    SPARKSUBMIT=/opt/spark/bin/spark-submit
    $SPARKSUBMIT --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  histo.py \
  $@
else
    SPARKSUBMIT=/usr/lib/spark/bin/spark-submit
    $SPARKSUBMIT --master yarn-client \
--driver-memory 8g \
--executor-memory 80G  --executor-cores 5 \
--num-executors 20 \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  histo.py \
  $@
fi


