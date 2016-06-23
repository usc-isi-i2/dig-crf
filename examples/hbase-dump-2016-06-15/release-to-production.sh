#! /bin/bash

# Copy the CRF/hybridJaccard output files from the work area to the
# production area ("/user/worker"). The script tried to do the right
# thing by copying the files, but that took a very long time.  SO, we
# will copy to a temporary area, then do quick renames.

NUM_EXECUTORS=50

source config.sh
source ${DIG_CRF_SCRIPT}/checkMemexConnection.sh
source ${DIG_CRF_SCRIPT}/limitMemexExecutors.sh

HAIR_EYES_INPUTFILE=${WORKING_HAIR_EYES_HJ_FILE}
NAME_ETHNIC_INPUTFILE=${WORKING_NAME_ETHNIC_HJ_FILE}

HAIR_EYES_OUTPUTFILE=${PRODUCTION_HAIR_EYES_FILE}
NAME_ETHNIC_OUTPUTFILE=${PRODUCTION_NAME_ETHNIC_FILE}
TEMPSUFFIX=.TEMP
HAIR_EYES_TEMPFILE=${HAIR_EYES_OUTPUTFILE}${TEMPSUFFIX}
NAME_ETHNIC_TEMPFILE=${NAME_ETHNIC_OUTPUTFILE}${TEMPSUFFIX}

# Check for the presence of the input files:
hdfs dfs -test -e ${HAIR_EYES_INPUTFILE}
if [ $? -ne 0 ]
  then
    echo "${HAIR_EYES_INPUTFILE} is missing, giving up."
    exit 1
fi

hdfs dfs -test -e ${NAME_ETHNIC_INPUTFILE}
if [ $? -ne 0 ]
  then
    echo "${NAME_ETHNIC_INPUTFILE} is missing, giving up."
    exit 1
fi

echo "New data files:"
hdfs dfs -ls -d ${HAIR_EYES_INPUTFILE}
hdfs dfs -ls -d ${NAME_ETHNIC_INPUTFILE}

echo "Checking for existing temporary files, and removing any found."
hdfs dfs -test -e ${HAIR_EYES_TEMPFILE}
if [ $? -eq 0 ]
  then
    hdfs dfs -rm -r ${HAIR_EYES_TEMPFILE}
    hdfs dfs -test -e ${HAIR_EYES_TEMPFILE}
    if [ $? -eq 0 ]
      then
        echo "Unable to remove ${HAIR_EYES_TEMPFILE}"
        exit 1
    fi
fi
hdfs dfs -test -e ${NAME_ETHNIC_TEMPFILE}
if [ $? -eq 0 ]
  then
    hdfs dfs -rm -r ${NAME_ETHNIC_TEMPFILE}
    hdfs dfs -test -e ${NAME_ETHNIC_TEMPFILE}
    if [ $? -eq 0 ]
      then
        echo "Unable to remove ${NAME_ETHNIC_TEMPFILE}"
        exit 1
    fi
fi

echo Copying ${HAIR_EYES_INPUTFILE} to ${HAIR_EYES_TEMPFILE}
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_UTIL}/copySeqFileSpark.py \
    -- \
    --input ${HAIR_EYES_INPUTFILE} \
    --output ${HAIR_EYES_TEMPFILE} \
    --cache --count --coalesce 50 \
    --showPartitions --time --verbose

echo Copying ${NAME_ETHNIC_INPUTFILE} to ${NAME_ETHNIC_TEMPFILE}
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_UTIL}/copySeqFileSpark.py \
    -- \
    --input ${NAME_ETHNIC_INPUTFILE} \
    --output ${NAME_ETHNIC_TEMPFILE} \
    --cache --count --coalesce 50 \
    --showPartitions --time --verbose

echo "Checking for new temporary files."
hdfs dfs -test -e ${HAIR_EYES_TEMPFILE}
if [ $? -ne 0 ]
  then
    echo "Copy failed to create ${HAIR_EYES_TEMPFILE}."
    exit 1
fi
hdfs dfs -test -e ${NAME_ETHNIC_TEMPFILE}
if [ $? -ne 0 ]
  then
    echo "Copy failed to create ${NAME_ETHNIC_TEMPFILE}."
    exit 1
fi

echo "Checking for existing production files, and removing any found."
hdfs dfs -test -e ${HAIR_EYES_OUTPUTFILE}
if [ $? -eq 0 ]
  then
    hdfs dfs -rm -r ${HAIR_EYES_OUTPUTFILE}
    hdfs dfs -test -e ${HAIR_EYES_OUTPUTFILE}
    if [ $? -eq 0 ]
      then
        echo "Unable to remove ${HAIR_EYES_OUTPUTFILE}"
        exit 1
    fi
fi
hdfs dfs -test -e ${NAME_ETHNIC_OUTPUTFILE}
if [ $? -eq 0 ]
  then
    hdfs dfs -rm -r ${NAME_ETHNIC_OUTPUTFILE}
    hdfs dfs -test -e ${NAME_ETHNIC_OUTPUTFILE}
    if [ $? -eq 0 ]
      then
        echo "Unable to remove ${NAME_ETHNIC_OUTPUTFILE}"
        exit 1
    fi
fi

echo "Renaming temporary files to new production files."
hdfs dfs -mv ${HAIR_EYES_TEMPFILE}   ${HAIR_EYES_OUTPUTFILE}
hdfs dfs -mv ${NAME_ETHNIC_TEMPFILE} ${NAME_ETHNIC_OUTPUTFILE}

echo "Checking for new production files."
hdfs dfs -test -e ${HAIR_EYES_OUTPUTFILE}
if [ $? -ne 0 ]
  then
    echo "Failed to install ${HAIR_EYES_OUTPUTFILE}."
    exit 1
fi
hdfs dfs -test -e ${NAME_ETHNIC_OUTPUTFILE}
if [ $? -ne 0 ]
  then
    echo "Failed to install ${NAME_ETHNIC_OUTPUTFILE}."
    exit 1
fi

echo "New production data files:"
hdfs dfs -ls -d ${HAIR_EYES_OUTPUTFILE}
hdfs dfs -ls -d ${NAME_ETHNIC_OUTPUTFILE}
