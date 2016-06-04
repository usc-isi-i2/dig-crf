#! /bin/bash

# This script assumes that "spark-submit" is available on $PATH.

echo "Submitting the job to the Memex cluster."
time spark-submit \
    --master 'yarn-client' \
    --num-executors 2 \
    ./showEnv.py


