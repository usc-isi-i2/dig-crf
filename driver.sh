#!/bin/sh

SPARKDIR=/tmp/spark-1.4.0-bin-hadoop2.4
HERE=/Users/philpot/Documents/project/dig-mturk/spark
CONFIG=$HERE/data/config

rm -rf $HERE/data/output
cd $SPARKDIR

./bin/spark-submit --master local[*] --py-files $HERE/data/zip/driver.zip  $HERE/driver.py $HERE/data/input/part-r-00000.seq $HERE/data/output $CONFIG/features.hair-eye $CONFIG/dig-hair-eye-train.model $CONFIG/eye_reference_wiki.txt $CONFIG/eye_config.txt $CONFIG/hair_reference_wiki.txt $CONFIG/hair_config.txt 
