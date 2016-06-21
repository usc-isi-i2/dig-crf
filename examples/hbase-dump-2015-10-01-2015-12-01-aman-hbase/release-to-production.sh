#! /bin/bash

# Copy the CRF/hybridJaccard output files from the work area
# to the production area ("/user/worker"). The script tries
# to do the right thing by copying the files, but that takes
# a very long time.
#
# TODO: Copy to a temporary area, then do quick renames.
#
# TODO: Use a simple program to parallel load and write
# the data.

INPUTFOLDER=/user/crogers
HAIR_EYES_INPUTFILE=${INPUTFOLDER}/hbase-dump-2015-10-01-2015-12-01-aman-hbase-crf-hair-eyes.seq
NAME_ETHNIC_INPUTFILE=${INPUTFOLDER}/hbase-dump-2015-10-01-2015-12-01-aman-hbase-crf-name-ethnic.seq

OUTPUTFOLDER=/user/worker/hbase-dump-2015-10-01-2015-12-01-aman/crf
HAIR_EYES_OUTPUTFILE=${OUTPUTFOLDER}/hair-eyes
NAME_ETHNIC_OUTPUTFILE=${OUTPUTFOLDER}/name-ethnic

source ${DIG_CRF_HOME}/checkMemexConnection.sh

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

echo "Installing the new data files."
hdfs dfs -test -d ${HAIR_EYES_INPUTFILE}
if [ $? -eq 0 ]
  then
    echo hdfs dfs -mkdir ${HAIR_EYES_OUTPUTFILE}
    hdfs dfs -mkdir ${HAIR_EYES_OUTPUTFILE}
    echo hdfs dfs -cp ${HAIR_EYES_INPUTFILE}/'*' ${HAIR_EYES_OUTPUTFILE}/
    hdfs dfs -cp ${HAIR_EYES_INPUTFILE}/'*' ${HAIR_EYES_OUTPUTFILE}/
  else
    hdfs dfs -cp ${HAIR_EYES_INPUTFILE} ${HAIR_EYES_OUTPUTFILE}
fi
hdfs dfs -test -d ${NAME_ETHNIC_INPUTFILE}
if [ $? -eq 0 ]
  then
    echo hdfs dfs -mkdir ${NAME_ETHNIC_OUTPUTFILE}
    hdfs dfs -mkdir ${NAME_ETHNIC_OUTPUTFILE}
    echo hdfs dfs -cp ${NAME_ETHNIC_INPUTFILE}/'*' ${NAME_ETHNIC_OUTPUTFILE}
    hdfs dfs -cp ${NAME_ETHNIC_INPUTFILE}/'*' ${NAME_ETHNIC_OUTPUTFILE}
  else
    hdfs dfs -cp ${NAME_ETHNIC_INPUTFILE} ${NAME_ETHNIC_OUTPUTFILE}/
fi

echo "Checking for new production files."
hdfs dfs -test -e ${HAIR_EYES_OUTPUTFILE}
if [ $? -ne 0 ]
  then
    echo "Copy failed for ${HAIR_EYES_OUTPUTFILE}."
    exit 1
fi
hdfs dfs -test -e ${NAME_ETHNIC_OUTPUTFILE}
if [ $? -ne 0 ]
  then
    echo "Copy failed for ${NAME_ETHNIC_OUTPUTFILE}."
    exit 1
fi

echo "Copied data files:"
hdfs dfs -ls -d ${HAIR_EYES_OUTPUTFILE}
hdfs dfs -ls -d ${NAME_ETHNIC_OUTPUTFILE}
