#! /bin/bash

# Copy the CRF/hybridJaccard output files from the work area to the
# production area ("/user/worker"). The script tried to do the right
# thing by copying the files, but that took a very long time.  SO, we
# will copy to a temporary area, then do quick renames.
#
# TODO: Use a simple program to parallel load and write
# the data.

source config.sh

HAIR_EYES_INPUTFILE=${WORKING_HAIR_EYES_HJ_FILE}
NAME_ETHNIC_INPUTFILE=${WORKING_NAME_ETHNIC_HJ_FILE}

OUTPUTFOLDER=${HDFS_CRF_DATA_DIR}
HAIR_EYES_OUTPUTFILE=${OUTPUTFOLDER}/${HAIR_EYES_FILE}
NAME_ETHNIC_OUTPUTFILE=${OUTPUTFOLDER}/${NAME_ETHNIC_FILE}
TEMPSUFFIX=.TEMP
HAIR_EYES_TEMPFILE=${HAIR_EYES_OUTPUTFILE}${TEMPSUFFIX}
NAME_ETHNIC_TEMPFILE=${NAME_ETHNIC_OUTPUTFILE}${TEMPSUFFIX}

source ${DIG_CRF_SCRIPT}/checkMemexConnection.sh

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

echo "Copying new data files to temporary files."
hdfs dfs -test -d ${HAIR_EYES_INPUTFILE}
if [ $? -eq 0 ]
  then
    echo hdfs dfs -mkdir ${HAIR_EYES_TEMPFILE}
    hdfs dfs -mkdir ${HAIR_EYES_TEMPFILE}
    echo hdfs dfs -cp ${HAIR_EYES_INPUTFILE}/'*' ${HAIR_EYES_TEMPFILE}/
    hdfs dfs -cp ${HAIR_EYES_INPUTFILE}/'*' ${HAIR_EYES_TEMPFILE}/
  else
    hdfs dfs -cp ${HAIR_EYES_INPUTFILE} ${HAIR_EYES_TEMPFILE}
fi
hdfs dfs -test -d ${NAME_ETHNIC_INPUTFILE}
if [ $? -eq 0 ]
  then
    echo hdfs dfs -mkdir ${NAME_ETHNIC_TEMPFILE}
    hdfs dfs -mkdir ${NAME_ETHNIC_TEMPFILE}
    echo hdfs dfs -cp ${NAME_ETHNIC_INPUTFILE}/'*' ${NAME_ETHNIC_TEMPFILE}
    hdfs dfs -cp ${NAME_ETHNIC_INPUTFILE}/'*' ${NAME_ETHNIC_TEMPFILE}
  else
    hdfs dfs -cp ${NAME_ETHNIC_INPUTFILE} ${NAME_ETHNIC_TEMPFILE}/
fi

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
