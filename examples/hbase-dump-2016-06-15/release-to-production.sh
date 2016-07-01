#! /bin/bash

# Copy the CRF/hybridJaccard output files from the work area to the
# production area ("/user/worker"). The script tried to do the right
# thing by copying the files, but that took a very long time.  SO, we
# will copy to a temporary area, then do quick renames.

NUM_EXECUTORS=50

source config.sh
source ${DIG_CRF_SCRIPT}/checkMemexConnection.sh
source ${DIG_CRF_SCRIPT}/limitMemexExecutors.sh

TOKENIZED_INPUTFILE=${WORKING_TITLE_AND_TEXT_TOKENS_FILE}
HAIR_EYES_INPUTFILE=${WORKING_HAIR_EYES_HJ_FILE}
NAME_ETHNIC_INPUTFILE=${WORKING_NAME_ETHNIC_HJ_FILE}

TOKENIZED_OUTPUTFILE=${PRODUCTION_TITLE_AND_TEXT_TOKENS_FILE}
HAIR_EYES_OUTPUTFILE=${PRODUCTION_HAIR_EYES_FILE}
NAME_ETHNIC_OUTPUTFILE=${PRODUCTION_NAME_ETHNIC_FILE}
TEMPSUFFIX=.TEMP
TOKENIZED_TEMPFILE=${TOKENIZED_OUTPUTFILE}${TEMPSUFFIX}
HAIR_EYES_TEMPFILE=${HAIR_EYES_OUTPUTFILE}${TEMPSUFFIX}
NAME_ETHNIC_TEMPFILE=${NAME_ETHNIC_OUTPUTFILE}${TEMPSUFFIX}

echo "Checking for the presence of the input files."
for INPUTFILE in ${TOKENIZED_INPUTFILE} ${HAIR_EYES_INPUTFILE} ${NAME_ETHNIC_INPUTFILE}; do
  echo "  Checking for ${INPUTFILE}"
  hdfs dfs -test -e ${INPUTFILE}
  if [ $? -ne 0 ]
    then
      echo "${INPUTFILE} is missing, giving up."
      exit 1
  fi
done

echo "New data files:"
hdfs dfs -ls -d ${TOKENIZED_INPUTFILE} ${HAIR_EYES_INPUTFILE} ${NAME_ETHNIC_INPUTFILE}

echo "Checking for existing temporary files, and removing any found."
for TEMPFILE in ${TOKENIZED_TEMPFILE} ${HAIR_EYES_TEMPFILE} ${NAME_ETHNIC_TEMPFILE}; do
  echo "  Checking for ${TEMPFILE}"
  hdfs dfs -test -e ${TEMPFILE}
  if [ $? -eq 0 ]
    then
      echo "    Removing ${TEMPFILE}"
      hdfs dfs -rm -r ${TEMPFILE}
      hdfs dfs -test -e ${TEMPFILE}
      if [ $? -eq 0 ]
        then
          echo "Unable to remove ${TEMPFILE}"
          exit 1
      fi
  fi
done

echo Copying ${TOKENIZED_INPUTFILE} to ${TOKENIZED_TEMPFILE}
time spark-submit \
    --master 'yarn-client' \
    --num-executors ${NUM_EXECUTORS} \
    ${DRIVER_JAVA_OPTIONS} \
    ${DIG_CRF_UTIL}/copySeqFileSpark.py \
    -- \
    --input ${TOKENIZED_INPUTFILE} \
    --output ${TOKENIZED_TEMPFILE} \
    --cache --count \
    --showPartitions --time --verbose

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
for TEMPFILE in ${TOKENIZED_TEMPFILE} ${HAIR_EYES_TEMPFILE} ${NAME_ETHNIC_TEMPFILE}; do
  echo "  Checking for ${TEMPFILE}"
  hdfs dfs -test -e ${TEMPFILE}
  if [ $? -ne 0 ]
    then
      echo "Copy failed to create ${TEMPFILE}."
      exit 1
  fi
done

echo "Checking for existing production files, and removing any found."
for OUTPUTFILE in ${TOKENIZED_OUTPUTFILE} ${HAIR_EYES_OUTPUTFILE} ${NAME_ETHNIC_OUTPUTFILE}; do
 echo "  Checking for ${OUTPUTFILE}"
 hdfs dfs -test -e ${OUTPUTFILE}
  if [ $? -eq 0 ]
    then
      echo "    Removing ${OUTPUTFILE}"
      hdfs dfs -rm -r ${OUTPUTFILE}
      hdfs dfs -test -e ${OUTPUTFILE}
      if [ $? -eq 0 ]
        then
          echo "Unable to remove ${OUTPUTFILE}"
          exit 1
      fi
  fi
done

echo "Renaming temporary files to new production files."
hdfs dfs -mv ${TOKENIZED_TEMPFILE}   ${TOKENIZED_OUTPUTFILE}
hdfs dfs -mv ${HAIR_EYES_TEMPFILE}   ${HAIR_EYES_OUTPUTFILE}
hdfs dfs -mv ${NAME_ETHNIC_TEMPFILE} ${NAME_ETHNIC_OUTPUTFILE}

echo "Checking for new production files."
for OUTPUTFILE in ${TOKENIZED_OUTPUTFILE} ${HAIR_EYES_OUTPUTFILE} ${NAME_ETHNIC_OUTPUTFILE}; do
  echo "  Checking for ${OUTPUTFILE}"
  hdfs dfs -test -e ${OUTPUTFILE}
  if [ $? -ne 0 ]
    then
      echo "Failed to install ${OUTPUTFILE}."
      exit 1
  fi
done

echo "New production data files:"
hdfs dfs -ls -d ${TOKENIZED_OUTPUTFILE} ${HAIR_EYES_OUTPUTFILE} ${NAME_ETHNIC_OUTPUTFILE}
