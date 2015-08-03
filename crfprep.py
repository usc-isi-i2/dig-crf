#!/usr/bin/env python

try:
    from pyspark import SparkContext
except:
    print "### NO PYSPARK"
import sys
import os
import json
import cgi
import platform
import socket
import codecs
from datetime import datetime

from htmltoken import tokenize
import crf_features

def extract_body(main_json):
    try:
        return main_json["hasBodyPart"]["text"]
    except:
        pass

def extract_title(main_json):
    try:
        return main_json["hasTitlePart"]["text"]
    except:
        pass

def textTokens(texts):
    # Turn None into empty text 
    texts = texts or ""
    # Allow for multiple texts
    texts = texts if isinstance(texts, list) else [texts]
    v = []
    for text in texts:
        try:
            for tok in genescaped(text):
                v.append([tok])
        except TypeError as e:
            print >> sys.stderr, "Error computing textTokens of %s: %s" % (text, e)
        v.append("")
    return v

def genescaped(text, maxTokenLength=40):
    """All tokens in TEXT with any odd characters (such as <>&) encoded using HTML escaping"""
    for tok in tokenize(text, interpret=cgi.escape, keepTags=False):
        # Some ads have odd tokens like 1000 As in a row
        if len(tok) <= maxTokenLength:
            # yield tok
            yield tok.replace('\t', ' ')

def rowToString(r):
    if isinstance(r, list):
        return "\t".join(r)
    else:
        return str(r)

def vectorToString(v, debug=False):
    """Too baroque, too defensive"""
    rows = []
    try:
        if v[-1] == "":
            pass
        else:
            # print "appending1"
            v.append("")
    except:
        # print "appending2"
        v.append("")
    for r in v:
        try:
            row = rowToString(r).encode('utf-8')
            # # rows.append(rowToString(r).encode('ascii', 'ignore'))
            # row = rowToString(r).encode('ascii', 'ignore')
            if row:
                rows.append(row)
            else:
                # encode('ascii', 'ignore') may yield empty string
                # pass
                rows.append("")
        except:
            try:
                rows.append(rowToString(r).encode('ascii', 'ignore'))
                if debug:
                    try:
                        dt = datetime.now()
                        temp = '/tmp/encoding_error_' + str(dt).replace(' ', '_')
                        with codecs.open(temp, 'w', encoding='utf-8') as f:
                            print >> f, type(r), r
                    except:
                        pass
            except:
                pass
    return "\n".join(rows)

LIMIT = None

def crfprep(sc, inputFilename, outputDirectory, 
            limit=LIMIT, location='hdfs', outputFormat="text", partitions=None):
    crfConfigDir = os.path.join(os.path.dirname(__file__), "data/config")
    featureListFilename = os.path.join(crfConfigDir, "features.hair-eye")

    rdd_sequence_file_input = sc.sequenceFile(inputFilename)
    rdd_sequence_file_input.setName('rdd_sequence_file_input')
    # rdd_sequence_file_input.persist()
    
    if limit:
        rdd_sequence_file_input = sc.parallelize(rdd_sequence_file_input.take(limit))
    if partitions:
        rdd_sequence_file_input = rdd_sequence_file_input.repartition(partitions)

    rdd_json = rdd_sequence_file_input.mapValues(lambda x: json.loads(x))
    rdd_json.setName('rdd_json')
    # rdd_json.persist()

    rdd_texts = rdd_json.mapValues(lambda x: (textTokens(extract_body(x)), textTokens(extract_title(x))))
    rdd_texts.setName('rdd_texts')

    # This separator could have appeared in original text, and should serve to cleanly delimit the body from the title
    # Not perfect, it could have appeared between real tokens
    c = crf_features.CrfFeatures(featureListFilename)
    SEPARATOR = '&amp;nbsp;',
    rdd_features = rdd_texts.map(lambda x: (x[0], 
                                            c.computeFeatMatrix(list(x[1][0]) + [SEPARATOR] + list(x[1][1]),
                                                                False,
                                                                addLabels=[x[0]],
                                                                addIndex=True)))
    rdd_features.setName('rdd_features')
    # rdd_features.persist()

    rdd_pipeinput = rdd_features.mapValues(lambda x: vectorToString(x)).values()
    rdd_pipeinput.setName('rdd_pipeinput')
    # rdd_features.persist()
    
    rdd_final = rdd_pipeinput
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
        return ("/Users/philpot/Documents/project/dig-crf/data/input/%s/%04d-%02d-%02d-%02d-00/part-r-%05d"
                % (tag, year, month, day, hour, int(partNum)))
    else:
        raise RuntimeError("Unknown location: %s" % location)

def output(year=2015, month=07, day=01, hour=01, location='hdfs', tag='incremental', partNum=0):
    if location == 'hdfs':
        if tag == 'incremental':
            # For HDFS incrementals, there is always a single part-r-00000
            # So we can safely drop this when creating an output directory
            return ("/user/worker/process/incremental/pilot/refactor/ads_attrs_crff/%04d-%02d-%02d-%02d-00"
                    % (year, month, day, hour))
        elif tag == 'istr58m':
            # An HDFS batch file such as istr58m will generally generate multiple output files
            # We key them to a directory using the input file part index
            # /user/worker/process/istr58m/pilot01/ads_main
            # /user/worker/process/istr58m/pilot01/ads_addrs_crff
            return ("/user/worker/process/istr58m/pilot01/ads_attrs_crff/from-part-r-%05d"
                    % int(partNum))
        else:
            raise RuntimeError("Unknown tag %r" % tag)
    elif location == 'local':
        if tag == 'incremental':
            # Let's mirror the HDFS convention for local as well
            return ("/Users/philpot/Documents/project/dig-crf/data/output/incremental/pilot/refactor/ads_attrs_crff/%04d-%02d-%02d-%02d-00"
                    % (year, month, day, hour))
        elif tag == 'istr58m':
            return ("/Users/philpot/Documents/project/dig-crf/data/output/istr58m/pilot/ads_attrs_crff/from-part-r-%05d"
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
    sc = SparkContext(appName="crfprep %s %s %s %s %s %s %s %s %s" % (year, mode, tag, month, day, hour, partNum, limit, partitions))
    crfprep(sc, inputFilename, outputDirectory, 
            limit=limit, location=location, outputFormat="text", partitions=partitions)
