#! /bin/bash

# This file must be sourced.

DATASET_NAME=hbase-dump-2016-06-15

DIG_CRF_APPLY=${DIG_CRF_HOME}/src/applyCrf
DIG_CRF_COUNT=${DIG_CRF_HOME}/src/count
DIG_CRF_EXTRACT=${DIG_CRF_HOME}/src/extract
DIG_CRF_SCRIPT=${DIG_CRF_HOME}
DIG_CRF_DATA_CONFIG_DIR=${DIG_CRF_HOME}/data/config

QUIETER_LOG4J_PROPERTIES_FILE=quieter-log4j.properties
DRIVER_JAVA_OPTIONS="--driver-java-options -Dlog4j.configuration=file:${DIG_CRF_DATA_CONFIG_DIR}/${QUIETER_LOG4J_PROPERTIES_FILE}"

HYBRID_JACCARD_CONFIG_FILE=hybrid_jaccard_config.json
HAIR_EYE_FEATURES_CONFIG_FILE=features.hair-eye
HAIR_EYE_CRF_MODEL_FILE=dig-hair-eye-train.model

NAME_ETHNIC_FEATURES_CONFIG_FILE=features.name-ethnic
NAME_ETHNIC_CRF_MODEL_FILE=dig-name-ethnic-train.model

HDFS_WORK_DIR=hdfs:///user/crogers
HDFS_PRODUCTION_DIR=hdfs:///user/worker/${DATASET_NAME}
HDFS_INPUT_DATA_DIR=${HDFS_PRODUCTION_DIR}/data/'*'
HDFS_CRF_DATA_DIR=${HDFS_PRODUCTION_DIR}/crf

PYTHON_EGG_CACHE=./python-eggs
export PYTHON_EGG_CACHE

DIG_CRF_EGG_FILE=${DIG_CRF_HOME}/CRF++-0.58/python/dist/mecab_python-0.0.0-py2.7-linux-x86_64.egg

DIG_CRF_PYTHON_ZIP_FILE=${DIG_CRF_HOME}/pythonFiles.zip

DATASET_WORK_DIR=${HDFS_WORK_DIR}/${DATASET_NAME}

WORKING_TITLE_AND_TEXT_TOKENS_FILE=${DATASET_WORK_DIR}/title-and-text-tokens.seq

HAIR_EYES_FILE=hair-eyes.seq
WORKING_HAIR_EYES_FILE=${DATASET_WORK_DIR}/${HAIR_EYES_FILE}

NAME_ETHNIC_FILE=name-ethnic.seq
WORKING_NAME_ETHNIC_FILE=${DATASET_WORK_DIR}/${NAME_ETHNIC_FILE}

# These are the files we'll deliver:
HAIR_EYES_HJ_FILE=hair-eyes-hj.seq
WORKING_HAIR_EYES_HJ_FILE=${DATASET_WORK_DIR}/${HAIR_EYES_HJ_FILE}
# 22-Jun-2016:  Workingname extraction has been disabled.
# The old definition was: NAME_ETHNIC_HJ_FILE=name-ethnic-hj.seq
NAME_ETHNIC_HJ_FILE=ethnic-hj.seq
WORKING_NAME_ETHNIC_HJ_FILE=${DATASET_WORK_DIR}/${NAME_ETHNIC_HJ_FILE}
