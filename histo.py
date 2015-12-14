#!/usr/bin/env python

try:
    from pyspark import SparkContext, SparkFiles
except:
    print "### NO PYSPARK"
import sys
import os
import platform
import socket
import argparse
import json
from random import randint
import time
from datetime import timedelta
import pprint

### from trollchar.py

def asList(x):
    if isinstance(x, list):
        return x
    else:
        return [x]

# Sniff for execution environment

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


configDir = os.getcwd() if location=="hdfs" else os.path.join(os.path.dirname(__file__), "data/config")
def configPath(n):
    return os.path.join(configDir, n)

binDir = os.getcwd() if location=="hdfs" else os.path.join(os.path.dirname(__file__), "bin")
def binPath(n):
    return os.path.join(binDir, n)

def histo(sc, input, output, 
          # specify uriClass=None to mean default it from inputType
          # specify uriClass=False to mean there is no uriClass filtering
          # specify uriClass=class name (e.g., 'Offer') to fully specify it
          limit=None, 
          debug=0, 
          location='hdfs', 
          outputFormat="text",
          verbose=False,

          sampleSeed=1234):

    show = True if debug>=1 else False
    def showPartitioning(rdd):
        """Seems to be significantly more expensive on cluster than locally"""
        if show:
            partitionCount = rdd.getNumPartitions()
            try:
                valueCount = rdd.countApprox(1000, confidence=0.50)
            except:
                valueCount = -1
            print "At %s, there are %d partitions with on average %s values" % (rdd.name(), partitionCount, int(valueCount/float(partitionCount)))
            if valueCount == 0:
                showSizeAndExit(rdd)

    debugOutput = output + '_debug'
    def debugDump(rdd,keys=True,listElements=False):
        showPartitioning(rdd)
        keys=False
        if debug >= 2:
            startTime = time.time()
            outdir = os.path.join(debugOutput, rdd.name() or "anonymous-%d" % randint(10000,99999))
            keyCount = None
            try:
                keyCount = rdd.keys().count() if keys else None
            except:
                pass
            rowCount = None
            try:
                rowCount = rdd.count()
            except:
                pass
            elementCount = None
            try:
                elementCount = rdd.mapValues(lambda x: len(x) if isinstance(x, (list, tuple)) else 0).values().sum() if listElements else None
            except:
                pass
            rdd.saveAsTextFile(outdir)
            endTime = time.time()
            elapsedTime = endTime - startTime
            print "wrote [%s] to outdir %r: [%s, %s, %s]" % (str(timedelta(seconds=elapsedTime)), outdir, keyCount, rowCount, elementCount)

    def showSizeAndExit(rdd):
        try:
            k = rdd.count()
        except:
            k = None
        print "Just finished %s with size %s" % (rdd.name(), k)
        exit(0)

    # LOADING DATA
    rdd_ingest = sc.sequenceFile(input)
    rdd_ingest.setName('rdd_ingest_input')
    showPartitioning(rdd_ingest)

    # LIMIT/SAMPLE (OPTIONAL)
    if limit==0:
        limit = None
    if limit:
        # Because take/takeSample collects back to master, can create "task too large" condition
        # rdd_ingest = sc.parallelize(rdd_ingest.take(limit))
        # Instead, generate approximately 'limit' rows
        ratio = float(limit) / rdd_ingest.count()
        rdd_ingest = rdd_ingest.sample(False, ratio, seed=sampleSeed)
        
    # layout: pageUri -> content serialized JSON string
    rdd_ingest.setName('rdd_ingest_net')
    debugDump(rdd_ingest)

    # layout: pageUri -> dict (from json)
    rdd_json = rdd_ingest.mapValues(lambda x: json.loads(x))
    rdd_json.setName('rdd_json')
    debugDump(rdd_json)

    # filter only ethnicities
    # input: uri -> json
    # output: uri -> json
    rdd_ethnic = rdd_json.filter(lambda (u,j): j["featureName"] ==  "person_ethnicity")
    rdd_ethnic.setName('rdd_ethnic')
    debugDump(rdd_ethnic)

    # generate point count of feature
    # input: uri -> json
    # output: json['ethnic'] => 1
    rdd_counts = rdd_ethnic.map(lambda (u,j): (j["featureValue"], 1))
    rdd_counts.setName('rdd_counts')
    debugDump(rdd_counts)

    # add them up
    from operator import add
    rdd_histo = rdd_counts.reduceByKey(add)
    rdd_histo.setName('rdd_histo')
    # repartition for total
    # rdd_histo.coalesce(1, shuffle=True)
    rdd_histo.repartition(1)
    print("We have {} partitions at end".format(    rdd_histo.getNumPartitions() ))
    debugDump(rdd_histo)
    print(rdd_histo.collect())

    # dump as is to text
    rdd_histo.saveAsTextFile(output)

def main(argv=None):
    '''this is called if run from command line'''
    # pprint.pprint(sorted(os.listdir(os.getcwd())))
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    parser.add_argument('-o','--output', required=True)
    parser.add_argument('-l','--limit', required=False, default=None, type=int)
    parser.add_argument('-v','--verbose', required=False, help='verbose', action='store_true')
    parser.add_argument('-z','--debug', required=False, help='debug', type=int)
    args=parser.parse_args()

    sparkName = "histo"

    sc = SparkContext(appName=sparkName)
    histo(sc, args.input, args.output, 
          limit=args.limit,
          location=location,               
          debug=args.debug,
          verbose=args.verbose)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
