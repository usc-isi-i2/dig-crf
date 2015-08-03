#!/bin/sh

month=$1
day=$2
hour=$3

shift 3
rest=$*

# hdfs dfs -rm -r /tmp/dig/spark/mturk
#hdfs dfs -rm -r /user/worker/process/incremental/pilot/refactor/ads_attrs_crff/2015-${month}-${day}-${hour}-00
# time spark-submit --num-executors 10 --py-files data/zip/driver.zip --driver-library-path /user/lib/hadoop/lib/native $PROJ/dig-crf/mergedriver.py prep incremental $month $day $hour $rest

time spark-submit --py-files data/zip/driver.zip --driver-library-path /user/lib/hadoop/lib/native $PROJ/dig-crf/crfprep.py crff incremental $month $day $hour $rest
