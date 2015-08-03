#!/usr/bin/env python

try:
    from pyspark import SparkContext, SparkFiles
except:
    print "### NO PYSPARK"
import sys
import os
import platform
import socket

LIMIT = None

def crfexec(sc, inputFilename, outputDirectory, 
            limit=LIMIT, location='hdfs', outputFormat="text", partitions=None):
    crfConfigDir = os.path.join(os.path.dirname(__file__), "data/config")
    crfExecutable = "/usr/local/bin/crf_test"
    crfModelFilename = os.path.join(crfConfigDir, "dig-hair-eye-train.model")

    rdd_pipeinput = sc.textFile(inputFilename)
    rdd_pipeinput.setName('rdd_pipeinput')
    # rdd_pipeinput.persist()

    # DON'T USE SparkFiles.get to fetch the crf_test or model
    # This only works with local Spark (--master local[*])
    if location == 'hdfs':
        cmd = "%s -m %s" % (os.path.basename(crfExecutable), os.path.basename(crfModelFilename))
    elif location == 'local':
        cmd = "%s -m %s" % (SparkFiles.get(os.path.basename(crfExecutable)), SparkFiles.get(os.path.basename(crfModelFilename)))
    print "### %s" % cmd
    rdd_crf = rdd_pipeinput.pipe(cmd)
    
    rdd_final = rdd_crf
    if outputFormat == "sequence":
        rdd_final.saveAsSequenceFile(outputDirectory)
    elif outputFormat == "text":
        rdd_final.saveAsTextFile(outputDirectory)
    else:
        raise RuntimeError("Unrecognized output format: %s" % outputFormat)

def input(year=2015, month=07, day=01, hour=01, location='hdfs', tag='incremental', partNum=0):
    """##Assumes only one part-00000 per incremental"""
    if location == 'hdfs':
        if tag == 'incremental':
            return ("/user/worker/process/incremental/pilot/refactor/ads_main/%04d-%02d-%02d-%02d-00/part-r-%05d"
                    % (year, month, day, hour, int(partNum)))
        elif tag == 'istr58m':
            return ("/user/worker/process/istr58m/pilot01/ads_main/part-r-%05d" % int(partNum))
        else:
            raise RuntimeError("Unknown tag %r" % tag)
    elif location == 'local':
        return ("/Users/philpot/Documents/project/dig-mturk/spark/data/input/%s/%04d-%02d-%02d-%02d-00/part-r-%05d"
                % (tag, year, month, day, hour, int(partNum)))
    else:
        raise RuntimeError("Unknown location: %s" % location)

def output(year=2015, month=07, day=01, hour=01, location='hdfs', tag='incremental', partNum=0):
    if location == 'hdfs':
        if tag == 'incremental':
            # For HDFS incrementals, there is always a single part-r-00000
            # So we can safely drop this when creating an output directory
            return ("/user/worker/process/incremental/pilot/refactor/ads_attrs_crfoutput/%04d-%02d-%02d-%02d-00"
                    % (year, month, day, hour))
        elif tag == 'istr58m':
            # An HDFS batch file such as istr58m will generally generate multiple output files
            # We key them to a directory using the input file part index
            # /user/worker/process/istr58m/pilot01/ads_main
            # /user/worker/process/istr58m/pilot01/ads_addrs_crfoutput
            return ("/user/worker/process/istr58m/pilot01/ads_attrs_crfoutput/from-part-r-%05d"
                    % int(partNum))
        else:
            raise RuntimeError("Unknown tag %r" % tag)
    elif location == 'local':
        if tag == 'incremental':
            # Let's mirror the HDFS convention for local as well
            return ("/Users/philpot/Documents/project/dig-mturk/spark/data/output/incremental/pilot/refactor/ads_attrs_crfoutput/%04d-%02d-%02d-%02d-00"
                    % (year, month, day, hour))
        elif tag == 'istr58m':
            return ("/Users/philpot/Documents/project/dig-mturk/spark/data/output/istr58m/pilot/ads_attrs_crfoutput/from-part-r-%05d"
                    % int(partNum))
        else:
            raise RuntimeError("Unknown tag %r" % tag)
    else:
        raise RuntimeError("Unknown location: %s" % location)

if __name__ == "__main__":

    location = "hdfs"
    try:
        if "avatar" in platform.node():
            location = "local"
    except:
        pass
    try:
        if "avatar" in socket.gethostname():
            location = "local"
    except:
        pass
    print "### location %s" % location

    sc = SparkContext(appName="crfexec")
    year = 2015
    mode = sys.argv[1]
    tag = sys.argv[2]
    month = int(sys.argv[3])
    day = int(sys.argv[4])
    hour = int(sys.argv[5])
    partNum = None
    try:
        partNum = int(sys.argv[6])
    except:
        pass
    limit = None
    try:
        limit = int(sys.argv[7])
    except:
        pass
    partitions = None
    try:
        partitions = int(sys.argv[8])
    except:
        pass
    inputFilename = input(year=year, month=month, day=day, hour=hour, location=location, tag=tag, partNum=partNum)
    outputDirectory = output(year=year, month=month, day=day, hour=hour, location=location, tag=tag, partNum=partNum)
    docu =("year %s, mode %s, tag %s, month %s, day %s, hour %s, partNum %s, limit %s, partitions %s" %
           (year, mode, tag, month, day, hour, partNum, limit, partitions))
    print docu
    sc = SparkContext(appName="crfexec %s % % % % % % % %" % (year, mode, tag, month, day, hour, partNum, limit, partitions))
    crfexec(sc, inputFilename, outputDirectory, 
            limit=limit, location=location, outputFormat="text", partitions=partitions)
