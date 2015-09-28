#!/bin/sh

partnum=$1

shift 1
rest=$*

hdfs dfs -rm -r /tmp/dig/spark/mturk
hdfs dfs -rm -r /user/worker/process/istr58m/pilot01/ads_attrs_crfinput/from-part-r-${partnum}
time spark-submit --py-files data/zip/driver.zip --driver-library-path /user/lib/hadoop/lib/native $PROJ/dig-crf/mergedriver.py prep istr58m 00 00 00 $partnum $rest