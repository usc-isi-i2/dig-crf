#!/usr/bin/env python

try:
    from pyspark import SparkContext
except:
    print "### NO PYSPARK"
import sys
import os
import platform
import socket
from harvestspans import computeSpans
from hybridJaccard import HybridJaccard

# import snakebite
from snakebite.client import Client
from snakebite.errors import FileNotFoundException

from util import echo
LIMIT = None

### The URI + index mechanism isn't good enough to recover data when there are multiple sentences per URI
### which can occur from (a) title + body as separate documents (b) sentence breaking of body
### The intra-sentence space rows won't be maintained by reconstructTuple + groupBy
### Proposed future workarounds:
### (a) sentence-specific temporary URIs ending /processed/title or /processed/1
### (b) and/or sentence-specific indexing 1.1, 1.2, ... 1.N, 2.1, etc. could work

def reconstructTuple(tabsep):
    """Works only for old multi-line invocation of crf_test, not crf_test_b64"""
    fields = tabsep.split('\t')
    try:
        uri = fields[-3]
        idx = fields[-2]
    except:
        uri = "endOfDocument"
        idx = "0"
    return (uri, [idx] + fields)

def fakeFindBestMatch(words):
    if 'blue' in words:
        return 'blue'
    else:
        return 'NONE'

def alignToControlledVocab(harvested, vocabs):
    """harvested is a dict matching category 'eyeColor', 'hairType' to a function"""
    try:
        category = harvested['category']
        f = vocabs[category]
        words = harvested['words']
        try:
            harvested['bestMatch'] = f(words)
        except Exception as e:
            return "sm.findBestMatch error:" + str(e) + ":" + str(words)
        return harvested
    except Exception as e:
        return str(e)
    return None

def crfalign(sc, inputFilename, outputDirectory, 
            limit=LIMIT, location='hdfs', outputFormat="text", partitions=None, deleteFirst=True):

    crfConfigDir = os.path.join(os.path.dirname(__file__), "data/config")
    def cpath(n):
        return os.path.join(crfConfigDir, n)

    smEyeColor = HybridJaccard(ref_path=cpath("eyeColor_reference_wiki.txt"),
                               config_path=cpath("eyeColor_config.txt"))
    smHairColor = HybridJaccard(ref_path=cpath("hairColor_reference_wiki.txt"),
                                config_path=cpath("hairColor_config.txt"))
    print smEyeColor, smHairColor

    if location == "hdfs":
        if deleteFirst:
            namenode = "memex-nn1"
            port = 8020
            client = Client(namenode, 8020, use_trash=True)
            try:
                for deleted in client.delete([outputDirectory], recurse=True):
                    print deleted
            except FileNotFoundException as e:
                pass

    # hypothesis1: data fetched this way prompts the lzo compression error
    # hypothesis2: but it doesn't matter, error is just a warning
    if partitions:
        if limit:
            rdd_crfl = sc.parallelize(rdd_crfl.take(limit))
            rdd_crfl = rdd_crfl.repartition(partitions)
        else:
            print inputFilename
            rdd_crfl = sc.textFile(inputFilename, minPartitions=partitions)
    else:
        rdd_crfl = sc.textFile(inputFilename)
    rdd_crfl.setName('rdd_crfl')
    # rdd_crfl.persist()
    print "beginning: %s partitions" % rdd_crfl.getNumPartitions()

    # "value-only" RDD, not a pair RDD
    # but we have the URI in the -3 position
    # and the index in the -2 position
    rdd_withuri = rdd_crfl.map(lambda x: reconstructTuple(x))

    # Note: groupByKey returns iterable, not data; so no point in printing
    rdd_grouped = rdd_withuri.groupByKey()
    # sort the vectors by index (within key groups)
    rdd_sorted = rdd_grouped.mapValues(lambda x: [l[1:] for l in sorted(x, key=lambda r: int(r[0]))])
    # find all contiguous spans of marked-up tokens
    # returns 0 or more dicts per URI key
    rdd_spans = rdd_sorted.mapValues(lambda x: computeSpans(x, indexed=True))
    # flatten to (URI, single dict) on each line
    rdd_flat = rdd_spans.flatMapValues(lambda x: list(x))
    # rdd_flat = rdd_flat.coalesce(rdd_flat.getNumPartitions() / 3)
    # # map any eyeColor spans using smEyeColor, hairType spans using smHairColor
    # rdd_aligned = rdd_flat.mapValues(lambda x: alignToControlledVocab(x, {"eyeColor": smEyeColor, "hairType": smHairColor}))
    rdd_aligned = rdd_flat.mapValues(lambda x: alignToControlledVocab(x, {"eyeColor": smEyeColor.findBestMatch, "hairType": smHairColor.findBestMatch}))
    # rdd_aligned = rdd_flat.mapValues(lambda x: alignToControlledVocab(x, {"eyeColor": fakeFindBestMatch, "hairType": fakeFindBestMatch}))
    # rdd_aligned = rdd_flat.mapValues(lambda x: alignToControlledVocab(x, {}))
    # rdd_aligned = rdd_spans

    # rdd_final = rdd_crfl
    rdd_final = rdd_aligned
    print outputFormat
    if outputFormat == "sequence":
        rdd_final.saveAsSequenceFile(outputDirectory)
    elif outputFormat == "text":
        print "saving to %s" % outputDirectory
        rdd_final.saveAsTextFile(outputDirectory)
    else:
        raise RuntimeError("Unrecognized output format: %s" % outputFormat)

def intOrStar(x):
    if x == "*":
        return "*"
    else:
        return int(x)

def input(year=2015, month=07, day=01, hour=01, location='hdfs', tag='incremental', fromPartNum=0, partNum=0):
    """##Assumes only one part-00000 per incremental"""
    if location == 'hdfs':
        if tag == 'incremental':
            return ("TBD/user/worker/process/incremental/pilot/refactor/ads_main/%04d-%02d-%02d-%02d-00/part-r-%05d.crf.out"
                    % (year, month, day, hour, int(partNum)))
        elif tag == 'istr58m':
            partNum = intOrStar(partNum)
            if partNum != '*':
                partNum = "%05d" % partNum
            return ("/user/worker/process/istr58m/pilot01/ads_attrs_crfl/from-part-r-%05d-part-%s.crf.out" % (int(fromPartNum),partNum))
        else:
            raise RuntimeError("Unknown tag %r" % tag)
    elif location == 'local':
        return ("TBD/Users/philpot/Documents/project/dig-mturk/spark/data/input/%s/%04d-%02d-%02d-%02d-00/part-r-%05d"
                % (tag, year, month, day, hour, int(partNum)))
    else:
        raise RuntimeError("Unknown location: %s" % location)

def output(year=2015, month=07, day=01, hour=01, location='hdfs', tag='incremental', fromPartNum=0, partNum=0):
    if location == 'hdfs':
        if tag == 'incremental':
            # For HDFS incrementals, there is always a single part-r-00000
            # So we can safely drop this when creating an output directory
            return ("TBD/user/worker/process/incremental/pilot/refactor/ads_attrs_crfa/%04d-%02d-%02d-%02d-00"
                    % (year, month, day, hour))
        elif tag == 'istr58m':
            # An HDFS batch file such as istr58m will generally generate multiple output files
            # We key them to a directory using the input file part index
            # /user/worker/process/istr58m/pilot01/ads_main
            # /user/worker/process/istr58m/pilot01/ads_addrs_crff
            partNum == intOrStar(partNum)
            if partNum == '*':
                partNum = 'all'
            else:
                partNum = "%05d" % partNum
            return ("/user/worker/process/istr58m/pilot01/ads_attrs_crfa/from-part-r-%05d-part-%s" % (int(fromPartNum), partNum))
        else:
            raise RuntimeError("Unknown tag %r" % tag)
    elif location == 'local':
        if tag == 'incremental':
            # Let's mirror the HDFS convention for local as well
            return ("TBD/Users/philpot/Documents/project/dig-mturk/spark/data/output/incremental/pilot/refactor/ads_attrs_crfa/%04d-%02d-%02d-%02d-00"
                    % (year, month, day, hour))
        elif tag == 'istr58m':
            return ("TBD/Users/philpot/Documents/project/dig-mturk/spark/data/output/istr58m/pilot/ads_attrs_crfa/from-part-r-%05d"
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

    year = 2015
    mode = sys.argv[1]
    tag = sys.argv[2]
    month = int(sys.argv[3])
    day = int(sys.argv[4])
    hour = int(sys.argv[5])
    fromPartNum = None
    try:
        fromPartNum = int(sys.argv[6])
    except:
        pass
    partNum = None
    try:
        partNum = sys.argv[7]
    except:
        pass
    limit = None
    try:
        limit = int(sys.argv[8])
    except:
        pass
    partitions = None
    try:
        partitions = int(sys.argv[9])
    except:
        pass
    inputFilename = input(year=year, month=month, day=day, hour=hour, location=location, tag=tag, fromPartNum=fromPartNum, partNum=partNum)
    outputDirectory = output(year=year, month=month, day=day, hour=hour, location=location, tag=tag, fromPartNum=fromPartNum, partNum=partNum)
    print inputFilename, outputDirectory
    docu =("year %s, mode %s, tag %s, month %s, day %s, hour %s, fromPartNum %s, partNum %s, limit %s, partitions %s" %
           (year, mode, tag, month, day, hour, fromPartNum, partNum, limit, partitions))
    print docu
    sc = SparkContext(appName="crfalign %s %s %s %s %s %s %s %s %s %s" % (year, mode, tag, month, day, hour, fromPartNum, partNum, limit, partitions))
    crfalign(sc, inputFilename, outputDirectory, 
             limit=limit, location=location, outputFormat="text", partitions=partitions)
