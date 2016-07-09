#! /bin/bash

# This file must be sourced.

if [ "x${DIG_CRF_HOME}" == "x" ]
  then
    echo "Please set the DIG_CRF_HOME envar"
    exit 1
fi

DIG_CRF_APPLY=${DIG_CRF_HOME}/src/applyCrf
DIG_CRF_COUNT=${DIG_CRF_HOME}/src/count
DIG_CRF_EXTRACT=${DIG_CRF_HOME}/src/extract
DIG_CRF_SCRIPT=${DIG_CRF_HOME}
DIG_CRF_UTIL=${DIG_CRF_HOME}/src/util
DIG_CRF_DATA_CONFIG_DIR=${DIG_CRF_HOME}/data/config

QUIETER_LOG4J_PROPERTIES_FILE=quieter-log4j.properties
DRIVER_JAVA_OPTIONS="--driver-java-options -Dlog4j.configuration=file:${DIG_CRF_DATA_CONFIG_DIR}/${QUIETER_LOG4J_PROPERTIES_FILE}"

HDFS_WORK_DIR=hdfs:///user/crogers
